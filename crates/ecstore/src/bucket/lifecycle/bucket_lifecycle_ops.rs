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

use super::{metadata_boundary, object_lock_boundary, runtime_boundary as runtime_sources};
use crate::bucket::lifecycle::bucket_lifecycle_audit::{
    LcAuditEvent, LcEventSrc, emit_non_transitioned_expiration_event, emit_transition_complete_event,
    emit_transition_failed_event, emit_transitioned_expiration_event,
};
use crate::bucket::lifecycle::evaluator::Evaluator;
use crate::bucket::lifecycle::lifecycle::{
    self, Lifecycle, ObjectOpts, TransitionOptions, abort_incomplete_multipart_upload_due,
};
use crate::bucket::lifecycle::manual_transition_job::{
    MANUAL_TRANSITION_JOB_RECORD_PREFIX, ManualTransitionJobRecord, ManualTransitionJobState, ManualTransitionScopeAdmission,
    ManualTransitionScopeAdmissionClaim, ManualTransitionTaskRecord, ManualTransitionWorkerFailureReason,
    ManualTransitionWorkerResult, claim_manual_transition_scope_admission, delete_manual_transition_scope_admission_if_current,
    load_manual_transition_job_record, load_manual_transition_job_record_with_etag, load_manual_transition_pending_task_records,
    manual_transition_job_id_from_record_object_name, manual_transition_job_lease_expired,
    manual_transition_worker_result_task_key, persist_manual_transition_job_progress_if_owned,
    reconcile_manual_transition_worker_results_if_owned, record_manual_transition_worker_result,
    record_manual_transition_worker_result_with_reason, renew_manual_transition_job_lease_if_owned,
    save_manual_transition_job_record_if_current, save_manual_transition_task_if_absent, update_manual_transition_job_record,
};
use crate::bucket::lifecycle::replication_sink;
use crate::bucket::lifecycle::replication_sink::{
    DeleteReplicationConfigSnapshot, ReplicationObjectBridge, ReplicationStatusType, replication_state_to_filemeta,
};
use crate::bucket::lifecycle::tier_delete_journal::{process_tier_delete_journal_entry, run_tier_delete_journal_recovery_loop};
use crate::bucket::lifecycle::tier_free_version_recovery::{
    DEFAULT_FREE_VERSION_RECOVERY_LIMIT, FreeVersionRecoveryStats, recover_tier_free_versions_with_cancel,
};
use crate::bucket::lifecycle::tier_last_day_stats::{DailyAllTierStats, LastDayTierStats};
use crate::bucket::lifecycle::tier_sweeper::{Jentry, delete_object_from_remote_tier_idempotent_with_manager_and_identity};
use crate::bucket::lifecycle::transition_transaction::run_transition_transaction_recovery_loop;
use crate::bucket::object_lock::ObjectLockApi;
use crate::bucket::versioning::VersioningApi as _;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::disk::error::DiskError;
use crate::disk::{DeleteOptions, Disk, DiskAPI, RUSTFS_META_BUCKET, RUSTFS_META_MULTIPART_BUCKET, STORAGE_FORMAT_FILE};
use crate::error::Error;
use crate::error::StorageError;
use crate::error::{is_err_object_not_found, is_err_read_quorum, is_err_version_not_found, is_network_or_host_down};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions};
use crate::object_api::{ObjectEncryptionResolver, ReadPlan};
use crate::services::tier::{
    tier::{TierConfigMgr, TierOperationLease, tier_destination_id_from_metadata},
    warm_backend::WarmBackendGetOpts,
};
use crate::set_disk::{
    MAX_PARTS_COUNT, RUSTFS_MULTIPART_BUCKET_KEY, RUSTFS_MULTIPART_OBJECT_KEY, SetDisks, get_lock_acquire_timeout,
};
use crate::storage_api_contracts::{
    lifecycle::ExpirationOptions,
    list::ListOperations as _,
    multipart::MultipartOperations as _,
    namespace::NamespaceLocking as _,
    object::{DeletedObject, ObjectOperations as _, ObjectToDelete},
    range::HTTPRangeSpec,
};
use crate::store::ECStore;
use async_channel::{Receiver as A_Receiver, Sender as A_Sender, bounded};
use http::HeaderMap;
use rand::RngExt as _;
use rustfs_common::metrics::{
    IlmAction, Metrics, ScannerLifecycleExpiryStateUpdate, ScannerLifecycleTransitionStateUpdate, global_metrics,
};
use rustfs_config::{
    DEFAULT_TRANSITION_QUEUE_CAPACITY, DEFAULT_TRANSITION_QUEUE_SEND_TIMEOUT_MS, DEFAULT_TRANSITION_WORKERS_ABSOLUTE_MAX,
    DEFAULT_TRANSITION_WORKERS_CAP, ENV_MAX_EXPIRY_WORKERS, ENV_TRANSITION_QUEUE_CAPACITY, ENV_TRANSITION_QUEUE_SEND_TIMEOUT_MS,
    ENV_TRANSITION_WORKERS, ENV_TRANSITION_WORKERS_ABSOLUTE_MAX,
};
use rustfs_data_usage::TierStats;
use rustfs_filemeta::{
    FileInfo, FileInfoOpts, NULL_VERSION_ID, RestoreStatusOps, TRANSITION_COMPLETE, get_file_info, is_restored_object_on_disk,
};
use rustfs_utils::{
    get_env_i64, get_env_usize,
    path::encode_dir_object,
    string::{parse_bool, strings_has_prefix_fold},
};
use s3s::dto::{
    BucketLifecycleConfiguration, ExpirationStatus, ObjectLockConfiguration, RestoreRequest, RestoreRequestType, RestoreStatus,
    Timestamp,
};
use s3s::header::{X_AMZ_RESTORE, X_AMZ_SERVER_SIDE_ENCRYPTION};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll};
use std::time::Duration as StdDuration;
use time::OffsetDateTime;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Notify, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use uuid::Uuid;
use xxhash_rust::xxh64;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_LIFECYCLE_WORKER_STATE: &str = "lifecycle_worker_state";
const EVENT_LIFECYCLE_TRANSITION_COMPENSATION: &str = "lifecycle_transition_compensation";
const EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP: &str = "lifecycle_stale_multipart_cleanup";
const EVENT_LIFECYCLE_SCAN_SKIPPED: &str = "lifecycle_scan_skipped";
const EVENT_LIFECYCLE_EVALUATION_FAILED: &str = "lifecycle_evaluation_failed";
const EVENT_LIFECYCLE_EXPIRED_DETECTED: &str = "lifecycle_expired_detected";
const EVENT_LIFECYCLE_NOT_ENQUEUED: &str = "lifecycle_not_enqueued";
const EVENT_LIFECYCLE_DELETE_DISPATCHED: &str = "lifecycle_delete_dispatched";
const EVENT_LIFECYCLE_DELETE_COMPLETED: &str = "lifecycle_delete_completed";
#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
const EVENT_LIFECYCLE_TIER_AUDIT: &str = "lifecycle_tier_audit";
const EVENT_LIFECYCLE_TIER_OPERATION_FAILED: &str = "lifecycle_tier_operation_failed";
const EVENT_LIFECYCLE_DELETE_FAILED: &str = "lifecycle_delete_failed";

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub type TimeFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub type TraceFn =
    Arc<dyn Fn(String, HashMap<String, String>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
pub type ExpiryOpType = Box<dyn ExpiryOp + Send + Sync + 'static>;

static XXHASH_SEED: u64 = 0;
static TIER_FREE_VERSION_RECOVERY_STARTED: OnceLock<()> = OnceLock::new();
static MANUAL_TRANSITION_JOB_RECOVERY_STARTED: OnceLock<()> = OnceLock::new();

pub const AMZ_OBJECT_TAGGING: &str = "X-Amz-Tagging";
#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub const AMZ_TAG_COUNT: &str = "x-amz-tagging-count";
#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub const AMZ_TAG_DIRECTIVE: &str = "X-Amz-Tagging-Directive";
pub const AMZ_ENCRYPTION_AES: &str = "AES256";
#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub const AMZ_ENCRYPTION_KMS: &str = "aws:kms";

pub const ERR_INVALID_STORAGECLASS: &str = "invalid tier.";
const ENV_STALE_UPLOADS_EXPIRY: &str = "RUSTFS_API_STALE_UPLOADS_EXPIRY";
const ENV_STALE_UPLOADS_CLEANUP_INTERVAL: &str = "RUSTFS_API_STALE_UPLOADS_CLEANUP_INTERVAL";
const ENV_TIER_FREE_VERSION_RECOVERY_ENABLED: &str = "RUSTFS_TIER_FREE_VERSION_RECOVERY_ENABLED";
const DEFAULT_STALE_UPLOADS_EXPIRY: StdDuration = StdDuration::from_secs(24 * 60 * 60);
const DEFAULT_STALE_UPLOADS_CLEANUP_INTERVAL: StdDuration = StdDuration::from_secs(6 * 60 * 60);
const TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL: StdDuration = StdDuration::from_secs(60);
// Recovery notifications are process-local, so bound the additional idle gap
// before another full sweep can discover work persisted by another node.
const TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL: StdDuration = StdDuration::from_secs(10 * 60);
const TIER_FREE_VERSION_RECOVERY_JITTER_PERCENT: u64 = 10;
const DATE_EXPIRY_EXISTING_OBJECTS_GRACE_SECS: i64 = 5;
const EXPIRY_WORKER_QUEUE_CAPACITY: usize = 1000;
const DEFAULT_MANUAL_TRANSITION_JOB_RECOVERY_LIMIT: usize = 100;

// Phase 5 (backlog#939): lifecycle expiry/transition state moved into the
// per-instance `InstanceContext`; these owner helpers forward to the current
// instance's context (lazily materialized, shared for single-instance).
pub fn get_global_expiry_state() -> Arc<RwLock<ExpiryState>> {
    crate::runtime::global::current_ctx().expiry_state()
}

pub fn get_global_transition_state() -> Arc<TransitionState> {
    crate::runtime::global::current_ctx().transition_state()
}

fn resolve_transition_worker_count() -> (i64, i64, i64) {
    let fallback = std::cmp::min(num_cpus::get() as i64, DEFAULT_TRANSITION_WORKERS_CAP);
    let configured = env::var(ENV_TRANSITION_WORKERS)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback);
    let mut effective = configured;
    let absolute_max = resolve_transition_workers_absolute_max();
    effective = std::cmp::min(effective, absolute_max);
    (configured, absolute_max, effective)
}

fn resolve_transition_workers_absolute_max() -> i64 {
    let absolute_max = get_env_i64(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX, DEFAULT_TRANSITION_WORKERS_ABSOLUTE_MAX);
    if absolute_max > 0 {
        absolute_max
    } else {
        DEFAULT_TRANSITION_WORKERS_ABSOLUTE_MAX
    }
}

fn resolve_transition_queue_capacity() -> usize {
    get_env_usize(ENV_TRANSITION_QUEUE_CAPACITY, DEFAULT_TRANSITION_QUEUE_CAPACITY).max(1)
}

fn resolve_transition_queue_send_timeout() -> StdDuration {
    StdDuration::from_millis(
        get_env_usize(ENV_TRANSITION_QUEUE_SEND_TIMEOUT_MS, DEFAULT_TRANSITION_QUEUE_SEND_TIMEOUT_MS).max(1) as u64,
    )
}

fn is_immediate_transition_source(src: &LcEventSrc) -> bool {
    matches!(
        src,
        LcEventSrc::S3PutObject | LcEventSrc::S3CopyObject | LcEventSrc::S3CompleteMultipartUpload
    )
}

fn record_scanner_lifecycle_enqueue_result(src: &LcEventSrc, count: u64, queued: bool) {
    if matches!(src, LcEventSrc::Scanner) {
        global_metrics().record_scanner_expiry_enqueue_result(count, queued);
    }
}

fn record_scanner_lifecycle_expiry_blocked(src: &LcEventSrc, count: u64) {
    if matches!(src, LcEventSrc::Scanner) {
        global_metrics().record_scanner_expiry_blocked(count);
    }
}

fn record_scanner_lifecycle_expiry_delete_failed(src: &LcEventSrc, count: u64) {
    if matches!(src, LcEventSrc::Scanner) {
        global_metrics().record_scanner_expiry_delete_failed(count);
    }
}

fn record_scanner_transition_enqueue_result(src: &LcEventSrc, count: u64, queued: bool) {
    if matches!(src, LcEventSrc::Scanner) {
        global_metrics().record_scanner_transition_enqueue_result(count, queued);
    }
}

fn nonnegative_i64_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(any(test, debug_assertions))]
fn should_force_immediate_transition_enqueue_timeout() -> bool {
    env::var(rustfs_config::ENV_TEST_FORCE_IMMEDIATE_TRANSITION_ENQUEUE_TIMEOUT)
        .ok()
        .is_some_and(|value| value == "1")
}

#[cfg(not(any(test, debug_assertions)))]
fn should_force_immediate_transition_enqueue_timeout() -> bool {
    false
}

pub struct LifecycleSys;

impl LifecycleSys {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }

    pub async fn get(&self, bucket: &str) -> Option<BucketLifecycleConfiguration> {
        match metadata_boundary::get_lifecycle_config(bucket).await {
            Ok((lc, _)) => Some(lc),
            Err(Error::ConfigNotFound) => None,
            Err(err) => {
                debug!(
                    event = EVENT_LIFECYCLE_SCAN_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket,
                    error = ?err,
                    reason = "lifecycle_config_unavailable",
                    "Skipped lifecycle config lookup"
                );
                None
            }
        }
    }

    #[allow(
        dead_code,
        reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
    )]
    pub fn trace(oi: &ObjectInfo) -> TraceFn {
        let bucket = oi.bucket.clone();
        let name = oi.name.clone();
        let version_id = oi.version_id.map(|v| v.to_string()).unwrap_or_default();
        Arc::new(move |_action: String, _ctx: HashMap<String, String>| {
            let bucket = bucket.clone();
            let name = name.clone();
            let version_id = version_id.clone();
            Box::pin(async move {
                debug!(
                    bucket = %bucket,
                    object = %name,
                    version_id = %version_id,
                    action = %_action,
                    event = EVENT_LIFECYCLE_WORKER_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    state = "trace",
                    "Lifecycle trace event"
                );
            })
        })
    }
}

struct ExpiryTask {
    obj_info: ObjectInfo,
    event: lifecycle::Event,
    src: LcEventSrc,
    bucket_incarnation_id: Uuid,
}

impl ExpiryOp for ExpiryTask {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(self.obj_info.bucket.as_bytes());
        hasher.update(self.obj_info.name.as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct ExpiryStats {
    missed_expiry_tasks: AtomicI64,
    missed_freevers_tasks: AtomicI64,
    missed_tier_journal_tasks: AtomicI64,
    pending_tasks: AtomicI64,
    active_tasks: AtomicI64,
    workers: AtomicI64,
}

#[cfg(test)]
type LifecycleObservabilityObserver = Box<dyn Fn(&'static str, &'static str, Option<&'static str>) + Send + Sync + 'static>;

#[cfg(test)]
static LIFECYCLE_OBSERVABILITY_OBSERVER: Mutex<Option<LifecycleObservabilityObserver>> = Mutex::new(None);

#[cfg(test)]
struct LifecycleObservabilityObserverGuard;

#[cfg(test)]
impl Drop for LifecycleObservabilityObserverGuard {
    fn drop(&mut self) {
        let mut observer = LIFECYCLE_OBSERVABILITY_OBSERVER
            .lock()
            .expect("lifecycle observability observer lock should not poison");
        *observer = None;
    }
}

#[cfg(test)]
fn set_lifecycle_observability_observer(
    observer_fn: impl Fn(&'static str, &'static str, Option<&'static str>) + Send + Sync + 'static,
) -> LifecycleObservabilityObserverGuard {
    let mut observer = LIFECYCLE_OBSERVABILITY_OBSERVER
        .lock()
        .expect("lifecycle observability observer lock should not poison");
    *observer = Some(Box::new(observer_fn));
    LifecycleObservabilityObserverGuard
}

fn observe_lifecycle_observability_event(_event: &'static str, _state: &'static str, _reason: Option<&'static str>) {
    #[cfg(test)]
    if let Some(observer) = LIFECYCLE_OBSERVABILITY_OBSERVER
        .lock()
        .expect("lifecycle observability observer lock should not poison")
        .as_ref()
    {
        observer(_event, _state, _reason);
    }
}

struct LifecycleExpiryTrace<'a> {
    bucket: &'a str,
    object: Option<&'a str>,
    version_id: Option<Uuid>,
    event: &'a lifecycle::Event,
    src: &'a LcEventSrc,
    version_count: u64,
}

impl<'a> LifecycleExpiryTrace<'a> {
    fn for_object(oi: &'a ObjectInfo, event: &'a lifecycle::Event, src: &'a LcEventSrc, version_count: u64) -> Self {
        Self {
            bucket: &oi.bucket,
            object: Some(&oi.name),
            version_id: oi.version_id,
            event,
            src,
            version_count,
        }
    }

    fn for_batch(bucket: &'a str, event: &'a lifecycle::Event, src: &'a LcEventSrc, version_count: u64) -> Self {
        Self {
            bucket,
            object: None,
            version_id: None,
            event,
            src,
            version_count,
        }
    }

    fn emit(&self, event_name: &'static str, state: &'static str, reason: Option<&'static str>) {
        observe_lifecycle_observability_event(event_name, state, reason);
        debug!(
            event = event_name,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            bucket = %self.bucket,
            object = self.object.unwrap_or_default(),
            version_id = ?self.version_id,
            action = ?self.event.action,
            rule_id = %self.event.rule_id,
            due = ?self.event.due,
            source = ?self.src,
            version_count = self.version_count,
            state,
            reason = reason.unwrap_or_default(),
            "Lifecycle expiry observability event"
        );
    }
}

impl ExpiryStats {
    pub fn missed_tasks(&self) -> i64 {
        self.missed_expiry_tasks.load(Ordering::SeqCst)
    }

    #[allow(
        dead_code,
        reason = "asserted by this file's tests; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    fn missed_free_vers_tasks(&self) -> i64 {
        self.missed_freevers_tasks.load(Ordering::SeqCst)
    }

    #[allow(
        dead_code,
        reason = "asserted by this file's tests; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    fn missed_tier_journal_tasks(&self) -> i64 {
        self.missed_tier_journal_tasks.load(Ordering::SeqCst)
    }

    pub fn pending_tasks(&self) -> i64 {
        self.pending_tasks.load(Ordering::SeqCst)
    }

    pub fn active_tasks(&self) -> i64 {
        self.active_tasks.load(Ordering::SeqCst)
    }

    fn num_workers(&self) -> i64 {
        self.workers.load(Ordering::SeqCst)
    }

    fn add_nonnegative(counter: &AtomicI64, delta: i64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_add(delta).max(0)));
    }

    fn increment_missed_expiry_tasks(&self) {
        Self::add_nonnegative(&self.missed_expiry_tasks, 1);
    }

    fn increment_missed_freevers_tasks(&self) {
        Self::add_nonnegative(&self.missed_freevers_tasks, 1);
    }

    fn increment_missed_tier_journal_tasks(&self) {
        Self::add_nonnegative(&self.missed_tier_journal_tasks, 1);
    }

    fn increment_pending_tasks(&self) {
        Self::add_nonnegative(&self.pending_tasks, 1);
    }

    fn decrement_pending_tasks(&self) {
        Self::add_nonnegative(&self.pending_tasks, -1);
    }

    fn increment_active_tasks(&self) {
        Self::add_nonnegative(&self.active_tasks, 1);
    }

    fn decrement_active_tasks(&self) {
        Self::add_nonnegative(&self.active_tasks, -1);
    }

    fn increment_workers(&self) {
        Self::add_nonnegative(&self.workers, 1);
    }

    fn decrement_workers(&self) {
        Self::add_nonnegative(&self.workers, -1);
    }

    fn scanner_expiry_state_update(&self) -> ScannerLifecycleExpiryStateUpdate {
        let workers = nonnegative_i64_to_u64(self.num_workers());
        ScannerLifecycleExpiryStateUpdate {
            queue_capacity: workers.saturating_mul(usize_to_u64_saturated(EXPIRY_WORKER_QUEUE_CAPACITY)),
            queued: nonnegative_i64_to_u64(self.pending_tasks()),
            active: nonnegative_i64_to_u64(self.active_tasks()),
            workers,
            queue_missed: nonnegative_i64_to_u64(self.missed_tasks()),
        }
    }

    fn record_scanner_expiry_state(&self) {
        global_metrics().record_scanner_lifecycle_expiry_state(self.scanner_expiry_state_update());
    }
}

struct ExpiryActiveTask {
    stats: Arc<ExpiryStats>,
}

impl ExpiryActiveTask {
    fn begin(stats: Arc<ExpiryStats>) -> Self {
        stats.increment_active_tasks();
        stats.record_scanner_expiry_state();
        Self { stats }
    }
}

impl Drop for ExpiryActiveTask {
    fn drop(&mut self) {
        self.stats.decrement_active_tasks();
        self.stats.record_scanner_expiry_state();
    }
}

pub trait ExpiryOp: 'static {
    fn op_hash(&self) -> u64;
    fn as_any(&self) -> &dyn Any;
}

pub use crate::storage_api_contracts::lifecycle::TransitionedObject;

struct FreeVersionTask(ObjectInfo);

impl ExpiryOp for FreeVersionTask {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(self.0.transitioned_object.tier.as_bytes());
        hasher.update(self.0.transitioned_object.name.as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

async fn delete_free_version_remote_object(
    oi: &ObjectInfo,
    tier_config_mgr: &Arc<RwLock<TierConfigMgr>>,
) -> Result<(), std::io::Error> {
    let version_id_exact = validate_transition_remote_version(oi)?;
    let identity = tier_destination_id_from_metadata(&oi.user_defined)?
        .ok_or_else(|| std::io::Error::other("tier free-version has no durable backend identity"))?;
    delete_object_from_remote_tier_idempotent_with_manager_and_identity(
        &oi.transitioned_object.name,
        &oi.transitioned_object.version_id,
        &oi.transitioned_object.tier,
        identity,
        tier_config_mgr,
        version_id_exact,
    )
    .await?;
    Ok(())
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
async fn delete_free_version_remote_object_then<T, F, Fut>(
    oi: &ObjectInfo,
    tier_config_mgr: &Arc<RwLock<TierConfigMgr>>,
    delete_local: F,
) -> Result<T, std::io::Error>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    delete_free_version_remote_object(oi, tier_config_mgr).await?;
    Ok(delete_local().await)
}

struct NewerNoncurrentTask {
    bucket: String,
    versions: Vec<ObjectToDelete>,
    event: lifecycle::Event,
    src: LcEventSrc,
    bucket_incarnation_id: Uuid,
}

impl ExpiryOp for NewerNoncurrentTask {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(self.bucket.as_bytes());
        hasher.update(self.versions[0].object_name.as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct ExpiryState {
    tasks_tx: Vec<Sender<Option<ExpiryOpType>>>,
    tasks_rx: Vec<Arc<tokio::sync::Mutex<Receiver<Option<ExpiryOpType>>>>>,
    stats: Arc<ExpiryStats>,
    recovery_notify: Arc<Notify>,
}

impl ExpiryState {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            tasks_tx: vec![],
            tasks_rx: vec![],
            recovery_notify: Arc::new(Notify::new()),
            stats: Arc::new(ExpiryStats {
                missed_expiry_tasks: AtomicI64::new(0),
                missed_freevers_tasks: AtomicI64::new(0),
                missed_tier_journal_tasks: AtomicI64::new(0),
                pending_tasks: AtomicI64::new(0),
                active_tasks: AtomicI64::new(0),
                workers: AtomicI64::new(0),
            }),
        }))
    }

    #[cfg(test)]
    fn new_with_unconsumed_worker_channel(capacity: usize) -> Arc<RwLock<Self>> {
        let (tx, rx) = mpsc::channel(capacity);
        Arc::new(RwLock::new(Self {
            tasks_tx: vec![tx],
            tasks_rx: vec![Arc::new(tokio::sync::Mutex::new(rx))],
            recovery_notify: Arc::new(Notify::new()),
            stats: Arc::new(ExpiryStats {
                missed_expiry_tasks: AtomicI64::new(0),
                missed_freevers_tasks: AtomicI64::new(0),
                missed_tier_journal_tasks: AtomicI64::new(0),
                pending_tasks: AtomicI64::new(0),
                active_tasks: AtomicI64::new(0),
                workers: AtomicI64::new(1),
            }),
        }))
    }

    pub fn pending_tasks(&self) -> usize {
        usize::try_from(self.stats.pending_tasks().max(0)).unwrap_or(usize::MAX)
    }

    fn send_expiry_task(&self, wrkr: Sender<Option<ExpiryOpType>>, task: ExpiryOpType) -> bool {
        let queued = wrkr.try_send(Some(task)).is_ok();
        if queued {
            self.stats.increment_pending_tasks();
        }
        queued
    }

    pub fn enqueue_tier_journal_entry(&mut self, je: &Jentry) -> Result<(), std::io::Error> {
        let wrkr = self.get_worker_ch(je.op_hash());
        if wrkr.is_none() {
            self.stats.increment_missed_tier_journal_tasks();
            self.stats.record_scanner_expiry_state();
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "lifecycle expiry worker unavailable for tier journal task",
            ));
        }
        let wrkr = wrkr.expect("worker channel should exist after None check");
        let queued = self.send_expiry_task(wrkr, Box::new(je.clone()));
        if !queued {
            self.stats.increment_missed_tier_journal_tasks();
            self.stats.record_scanner_expiry_state();
            return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "failed to enqueue tier journal task"));
        }
        self.stats.record_scanner_expiry_state();
        Ok(())
    }

    pub fn enqueue_free_version(&mut self, oi: ObjectInfo) -> bool {
        let task = FreeVersionTask(oi);
        let wrkr = self.get_worker_ch(task.op_hash());
        if wrkr.is_none() {
            self.stats.increment_missed_freevers_tasks();
            self.stats.record_scanner_expiry_state();
            self.recovery_notify.notify_one();
            return false;
        }
        let wrkr = wrkr.expect("worker channel should exist after None check");
        let queued = self.send_expiry_task(wrkr, Box::new(task));
        if !queued {
            self.stats.increment_missed_freevers_tasks();
            self.recovery_notify.notify_one();
        }
        self.stats.record_scanner_expiry_state();
        queued
    }

    pub fn enqueue_by_days(
        &mut self,
        oi: &ObjectInfo,
        event: &lifecycle::Event,
        src: &LcEventSrc,
        bucket_incarnation_id: Uuid,
    ) -> bool {
        let trace = LifecycleExpiryTrace::for_object(oi, event, src, 1);
        trace.emit(EVENT_LIFECYCLE_EXPIRED_DETECTED, "detected", None);
        let task = ExpiryTask {
            obj_info: oi.clone(),
            event: event.clone(),
            src: src.clone(),
            bucket_incarnation_id,
        };
        let wrkr = self.get_worker_ch(task.op_hash());
        if wrkr.is_none() {
            self.stats.increment_missed_expiry_tasks();
            record_scanner_lifecycle_enqueue_result(src, 1, false);
            self.stats.record_scanner_expiry_state();
            trace.emit(EVENT_LIFECYCLE_NOT_ENQUEUED, "not_enqueued", Some("worker_unavailable"));
            return false;
        }
        let wrkr = wrkr.expect("worker channel should exist after None check");
        let queued = self.send_expiry_task(wrkr, Box::new(task));
        if !queued {
            self.stats.increment_missed_expiry_tasks();
            trace.emit(EVENT_LIFECYCLE_NOT_ENQUEUED, "not_enqueued", Some("queue_full"));
        }
        record_scanner_lifecycle_enqueue_result(src, 1, queued);
        self.stats.record_scanner_expiry_state();
        queued
    }

    pub fn enqueue_by_newer_noncurrent(
        &mut self,
        bucket: &str,
        versions: Vec<ObjectToDelete>,
        lc_event: lifecycle::Event,
        src: &LcEventSrc,
        bucket_incarnation_id: Uuid,
    ) -> bool {
        if versions.is_empty() {
            return true;
        }
        let version_count = u64::try_from(versions.len()).unwrap_or(u64::MAX);
        let trace = LifecycleExpiryTrace::for_batch(bucket, &lc_event, src, version_count);
        trace.emit(EVENT_LIFECYCLE_EXPIRED_DETECTED, "detected", None);

        let task = NewerNoncurrentTask {
            bucket: String::from(bucket),
            versions,
            event: lc_event.clone(),
            src: src.clone(),
            bucket_incarnation_id,
        };
        let wrkr = self.get_worker_ch(task.op_hash());
        if wrkr.is_none() {
            self.stats.increment_missed_expiry_tasks();
            record_scanner_lifecycle_enqueue_result(src, version_count, false);
            self.stats.record_scanner_expiry_state();
            trace.emit(EVENT_LIFECYCLE_NOT_ENQUEUED, "not_enqueued", Some("worker_unavailable"));
            return false;
        }
        let wrkr = wrkr.expect("worker channel should exist after None check");
        let queued = self.send_expiry_task(wrkr, Box::new(task));
        if !queued {
            self.stats.increment_missed_expiry_tasks();
            trace.emit(EVENT_LIFECYCLE_NOT_ENQUEUED, "not_enqueued", Some("queue_full"));
        }
        record_scanner_lifecycle_enqueue_result(src, version_count, queued);
        self.stats.record_scanner_expiry_state();
        queued
    }

    pub fn get_worker_ch(&self, h: u64) -> Option<Sender<Option<ExpiryOpType>>> {
        if self.tasks_tx.is_empty() {
            return None;
        }
        Some(self.tasks_tx[h as usize % self.tasks_tx.len()].clone())
    }

    pub fn increment_missed_tier_journal_tasks(&mut self) {
        self.stats.increment_missed_tier_journal_tasks();
        self.stats.record_scanner_expiry_state();
    }

    pub async fn resize_workers(n: usize, api: Arc<ECStore>) {
        let expiry_state = runtime_sources::expiry_state_handle();
        if n == expiry_state.read().await.tasks_tx.len() || n < 1 {
            return;
        }

        let mut state = expiry_state.write().await;

        while state.tasks_tx.len() < n {
            let (tx, rx) = mpsc::channel(EXPIRY_WORKER_QUEUE_CAPACITY);
            let api = api.clone();
            let rx = Arc::new(tokio::sync::Mutex::new(rx));
            let stats = Arc::clone(&state.stats);
            let recovery_notify = Arc::clone(&state.recovery_notify);
            state.tasks_tx.push(tx);
            state.tasks_rx.push(rx.clone());
            state.stats.increment_workers();
            tokio::spawn(async move {
                let mut rx = rx.lock().await;
                //let mut expiry_state = runtime_sources::expiry_state_handle().read().await;
                ExpiryState::worker(&mut rx, api, stats, recovery_notify).await;
            });
        }

        let mut l = state.tasks_tx.len();
        while l > n {
            let worker = state.tasks_tx[l - 1].clone();
            let _ = worker.try_send(None);
            state.tasks_tx.remove(l - 1);
            state.tasks_rx.remove(l - 1);
            state.stats.decrement_workers();
            l -= 1;
        }
        state.stats.record_scanner_expiry_state();
    }

    async fn worker(
        rx: &mut Receiver<Option<ExpiryOpType>>,
        api: Arc<ECStore>,
        stats: Arc<ExpiryStats>,
        recovery_notify: Arc<Notify>,
    ) {
        let cancel_token = runtime_sources::background_services_cancel_token().unwrap_or_else(|| {
            static FALLBACK: std::sync::OnceLock<tokio_util::sync::CancellationToken> = std::sync::OnceLock::new();
            FALLBACK.get_or_init(tokio_util::sync::CancellationToken::new).clone()
        });

        loop {
            select! {
                _ = cancel_token.cancelled() => {
                    debug!(
                        event = EVENT_LIFECYCLE_WORKER_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                        state = "stopped",
                        reason = "shutdown_signal",
                        "Lifecycle expiry worker stopped"
                    );
                    break;
                }
                v = rx.recv() => {
                    if v.is_none() {
                        break;
                    }
                    let v = v.expect("channel closed unexpectedly");
                    if v.is_none() {
                        //rx.close();
                        //drop(rx);
                        let _ = rx;
                        return;
                    }
                    let v = v.expect("received None after None check");
                    stats.decrement_pending_tasks();
                    let _active_task = ExpiryActiveTask::begin(Arc::clone(&stats));
                    if v.as_any().is::<ExpiryTask>() {
                        let v = v.as_any().downcast_ref::<ExpiryTask>().expect("ExpiryTask downcast failed");
                        //debug!("lifecycle expiry worker received task: {:?}", v.obj_info);
                        let trace = LifecycleExpiryTrace::for_object(&v.obj_info, &v.event, &v.src, 1);
                        trace.emit(EVENT_LIFECYCLE_DELETE_DISPATCHED, "delete_dispatched", None);
                        let deleted = if !v.obj_info.transitioned_object.status.is_empty() {
                            apply_expiry_on_transitioned_object(
                                api.clone(),
                                &v.obj_info,
                                &v.event,
                                &v.src,
                                v.bucket_incarnation_id,
                            )
                            .await
                        } else {
                            apply_expiry_on_non_transitioned_objects(
                                api.clone(),
                                &v.obj_info,
                                &v.event,
                                &v.src,
                                v.bucket_incarnation_id,
                            )
                            .await
                        };
                        if deleted {
                            trace.emit(EVENT_LIFECYCLE_DELETE_COMPLETED, "delete_completed", None);
                        } else {
                            record_scanner_lifecycle_expiry_delete_failed(&v.src, 1);
                            trace.emit(
                                EVENT_LIFECYCLE_DELETE_FAILED,
                                "delete_failed",
                                Some("delete_operation_failed"),
                            );
                        }
                    }
                    else if v.as_any().is::<NewerNoncurrentTask>() {
                        let v = v.as_any().downcast_ref::<NewerNoncurrentTask>().expect("NewerNoncurrentTask downcast failed");
                        let version_count = u64::try_from(v.versions.len()).unwrap_or(u64::MAX);
                        let trace = LifecycleExpiryTrace::for_batch(&v.bucket, &v.event, &v.src, version_count);
                        trace.emit(EVENT_LIFECYCLE_DELETE_DISPATCHED, "delete_dispatched", None);
                        crate::client::object_handlers_common::delete_object_versions(
                            &api,
                            &v.bucket,
                            &v.versions,
                            v.event.clone(),
                            v.bucket_incarnation_id,
                        )
                        .await;
                        trace.emit(EVENT_LIFECYCLE_DELETE_COMPLETED, "delete_completed", None);
                    }
                    else if v.as_any().is::<Jentry>() {
                        let v = v.as_any().downcast_ref::<Jentry>().expect("Jentry downcast failed");
                        if let Err(err) = process_tier_delete_journal_entry(api.clone(), v).await {
                            debug!(
                                event = EVENT_LIFECYCLE_WORKER_STATE,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                object = %v.obj_name,
                                version_id = %v.version_id,
                                tier = %v.tier_name,
                                error = ?err,
                                reason = "remote_tier_delete_failed",
                                "Lifecycle worker skipped remote tier delete"
                            );
                        }
                    }
                    else if v.as_any().is::<FreeVersionTask>() {
                        let v = v.as_any().downcast_ref::<FreeVersionTask>().expect("FreeVersionTask downcast failed");
                        let oi = v.0.clone();
                        if let Err(err) = delete_free_version_remote_object(&oi, &api.tier_config_mgr()).await {
                            recovery_notify.notify_one();
                            debug!(
                                bucket = %oi.bucket,
                                object = %oi.name,
                                remote_object = %oi.transitioned_object.name,
                                remote_version_id = %oi.transitioned_object.version_id,
                                tier = %oi.transitioned_object.tier,
                                error = ?err,
                                event = EVENT_LIFECYCLE_WORKER_STATE,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                reason = "remote_tier_delete_failed",
                                "Lifecycle worker skipped remote tier delete"
                            );
                            continue;
                        }

                        let local_object = encode_dir_object(&oi.name);
                        let mut fi = FileInfo {
                            name: local_object.clone(),
                            version_id: oi.version_id,
                            ..Default::default()
                        };
                        // This removes an existing internal cleanup marker. Keeping
                        // `deleted` false makes duplicate tasks return not-found
                        // instead of creating an ordinary delete marker.
                        fi.set_tier_free_version();

                        let mut deleted_locally = false;
                        for pool in &api.pools {
                            let set = pool.get_disks_by_key(&local_object);
                            let ns_lock = match set.new_ns_lock(&oi.bucket, &local_object).await {
                                Ok(lock) => lock,
                                Err(err) => {
                                    recovery_notify.notify_one();
                                    debug!(
                                        event = EVENT_LIFECYCLE_WORKER_STATE,
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                        bucket = %oi.bucket,
                                        object = %oi.name,
                                        pool_index = pool.pool_idx,
                                        set_index = set.set_index,
                                        error = ?err,
                                        reason = "local_free_version_lock_failed",
                                        "Lifecycle worker failed to create local free-version cleanup lock"
                                    );
                                    continue;
                                }
                            };
                            let _object_lock_guard =
                                match ns_lock.get_write_lock_quiet(get_lock_acquire_timeout()).await {
                                    Ok(guard) => guard,
                                    Err(err) => {
                                        recovery_notify.notify_one();
                                        debug!(
                                            event = EVENT_LIFECYCLE_WORKER_STATE,
                                            component = LOG_COMPONENT_ECSTORE,
                                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                            bucket = %oi.bucket,
                                            object = %oi.name,
                                            pool_index = pool.pool_idx,
                                            set_index = set.set_index,
                                            error = ?err,
                                            reason = "local_free_version_lock_failed",
                                            "Lifecycle worker failed to acquire local free-version cleanup lock"
                                        );
                                        continue;
                                    }
                                };
                            match set
                                .delete_object_version(&oi.bucket, &local_object, &fi, false)
                                .await
                            {
                                Ok(()) => {
                                    deleted_locally = true;
                                    break;
                                }
                                Err(err) if is_err_version_not_found(&err) || is_err_object_not_found(&err) => continue,
                                Err(err) => {
                                    recovery_notify.notify_one();
                                    debug!(
                                        event = EVENT_LIFECYCLE_WORKER_STATE,
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                        bucket = %oi.bucket,
                                        object = %oi.name,
                                        remote_object = %oi.transitioned_object.name,
                                        remote_version_id = %oi.transitioned_object.version_id,
                                        tier = %oi.transitioned_object.tier,
                                        error = ?err,
                                        reason = "local_free_version_delete_failed",
                                        "Lifecycle worker failed local free-version cleanup"
                                    );
                                    break;
                                }
                            }
                        }

                        if !deleted_locally {
                            debug!(
                                event = EVENT_LIFECYCLE_WORKER_STATE,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                bucket = %oi.bucket,
                                object = %oi.name,
                                remote_object = %oi.transitioned_object.name,
                                remote_version_id = %oi.transitioned_object.version_id,
                                tier = %oi.transitioned_object.tier,
                                reason = "local_free_version_missing",
                                "Lifecycle worker could not find transitioned free version locally"
                            );
                        }
                    }
                    else {
                        //info!("Invalid work type - {:?}", v);
                        debug!(
                            event = EVENT_LIFECYCLE_WORKER_STATE,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            state = "unsupported_task",
                            "Lifecycle worker received unsupported operation type"
                        );
                    }
                }
            }
        }
    }
}

async fn enqueue_recovered_free_version_with_state(state: &Arc<RwLock<ExpiryState>>, oi: ObjectInfo) -> bool {
    let task = FreeVersionTask(oi);
    let hash = task.op_hash();
    let (wrkr, stats) = {
        let state = state.read().await;
        (state.get_worker_ch(hash), Arc::clone(&state.stats))
    };
    let Some(wrkr) = wrkr else {
        stats.increment_missed_freevers_tasks();
        stats.record_scanner_expiry_state();
        return false;
    };

    let queued = wrkr.try_send(Some(Box::new(task))).is_ok();
    if !queued {
        stats.increment_missed_freevers_tasks();
    } else {
        stats.increment_pending_tasks();
    }
    stats.record_scanner_expiry_state();
    queued
}

#[cfg(test)]
type RecoveredFreeVersionEnqueueObserver = Box<dyn Fn(bool) + Send + Sync>;

#[cfg(test)]
static RECOVERED_FREE_VERSION_ENQUEUE_OBSERVER: Mutex<Option<RecoveredFreeVersionEnqueueObserver>> = Mutex::new(None);

#[cfg(test)]
struct RecoveredFreeVersionEnqueueObserverGuard;

#[cfg(test)]
impl Drop for RecoveredFreeVersionEnqueueObserverGuard {
    fn drop(&mut self) {
        let mut observer = RECOVERED_FREE_VERSION_ENQUEUE_OBSERVER
            .lock()
            .expect("recovered free-version enqueue observer lock should not poison");
        *observer = None;
    }
}

#[cfg(test)]
fn set_recovered_free_version_enqueue_observer(
    observer_fn: impl Fn(bool) + Send + Sync + 'static,
) -> RecoveredFreeVersionEnqueueObserverGuard {
    let mut observer = RECOVERED_FREE_VERSION_ENQUEUE_OBSERVER
        .lock()
        .expect("recovered free-version enqueue observer lock should not poison");
    *observer = Some(Box::new(observer_fn));
    RecoveredFreeVersionEnqueueObserverGuard
}

pub async fn enqueue_recovered_free_version(oi: ObjectInfo) -> bool {
    let expiry_state = runtime_sources::expiry_state_handle();
    let queued = enqueue_recovered_free_version_with_state(&expiry_state, oi).await;

    #[cfg(test)]
    if let Some(observer) = RECOVERED_FREE_VERSION_ENQUEUE_OBSERVER
        .lock()
        .expect("recovered free-version enqueue observer lock should not poison")
        .as_ref()
    {
        observer(queued);
    }

    queued
}

struct TransitionTask {
    obj_info: ObjectInfo,
    src: LcEventSrc,
    event: lifecycle::Event,
    manual_job_id: Option<Uuid>,
    manual_result_key: Option<String>,
}

impl ExpiryOp for TransitionTask {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(self.obj_info.bucket.as_bytes());
        // hasher.update(format!("{}", self.obj_info.versions[0].object_name).as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct TransitionWorker {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

pub struct TransitionState {
    transition_tx: A_Sender<Option<TransitionTask>>,
    transition_rx: A_Receiver<Option<TransitionTask>>,
    pub num_workers: AtomicI64,
    workers: Mutex<Vec<TransitionWorker>>,
    transition_queue_capacity: usize,
    transition_queue_send_timeout: StdDuration,
    active_tasks: AtomicI64,
    missed_immediate_tasks: AtomicI64,
    queue_full_tasks: AtomicI64,
    queue_send_timeout_tasks: AtomicI64,
    compensation_scheduled_tasks: AtomicI64,
    compensation_running_tasks: AtomicI64,
    compensation_buckets: Arc<Mutex<HashSet<String>>>,
    // (bucket, object, version) currently queued or being transitioned. A single
    // PUT can enqueue the same object twice (immediate transition + startup
    // compensation backfill); without this guard the duplicates run concurrently,
    // and the loser races the winner's source cleanup — reading data the winner
    // already removed and logging a spurious NotFound failure (rustfs/backlog#1268).
    in_flight_transitions: Arc<Mutex<HashSet<(String, String, String)>>>,
    last_day_stats: Arc<Mutex<HashMap<String, LastDayTierStats>>>,
}

enum ImmediateEnqueueFailure {
    ForcedTimeout,
    QueueClosed { timeout_ms: Option<u64> },
    QueueSendTimedOut { timeout_ms: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransitionEnqueueOutcome {
    Queued,
    AlreadyInFlight,
    QueueFull,
    QueueClosed,
    QueueSendTimedOut,
    TaskJournalFailed,
}

impl TransitionEnqueueOutcome {
    fn is_handled(self) -> bool {
        matches!(self, Self::Queued | Self::AlreadyInFlight)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManualTransitionPendingTaskReplay {
    Empty,
    Queued,
    Deferred,
}

impl TransitionState {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Arc<Self> {
        Self::new_with_capacity(resolve_transition_queue_capacity())
    }

    fn new_with_capacity(capacity: usize) -> Arc<Self> {
        let queue_send_timeout = resolve_transition_queue_send_timeout();
        Self::new_with_capacity_and_timeout(capacity, queue_send_timeout)
    }

    fn new_with_capacity_and_timeout(capacity: usize, queue_send_timeout: StdDuration) -> Arc<Self> {
        let capacity = capacity.max(1);
        let (tx1, rx1) = bounded(capacity);
        Arc::new(Self {
            transition_tx: tx1,
            transition_rx: rx1,
            num_workers: AtomicI64::new(0),
            workers: Mutex::new(Vec::new()),
            transition_queue_capacity: capacity,
            transition_queue_send_timeout: queue_send_timeout,
            active_tasks: AtomicI64::new(0),
            missed_immediate_tasks: AtomicI64::new(0),
            queue_full_tasks: AtomicI64::new(0),
            queue_send_timeout_tasks: AtomicI64::new(0),
            compensation_scheduled_tasks: AtomicI64::new(0),
            compensation_running_tasks: AtomicI64::new(0),
            compensation_buckets: Arc::new(Mutex::new(HashSet::new())),
            in_flight_transitions: Arc::new(Mutex::new(HashSet::new())),
            last_day_stats: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn transition_key(oi: &ObjectInfo) -> (String, String, String) {
        (
            oi.bucket.clone(),
            oi.name.clone(),
            oi.version_id.map(|v| v.to_string()).unwrap_or_default(),
        )
    }

    /// Try to claim a transition for this exact (bucket, object, version). Returns
    /// `false` when one is already queued or running, so the caller can skip the
    /// duplicate enqueue (rustfs/backlog#1268). The claim is released in the
    /// worker once the transition finishes, or here if the enqueue fails.
    fn reserve_transition(&self, oi: &ObjectInfo) -> bool {
        let key = Self::transition_key(oi);
        match self.in_flight_transitions.lock() {
            Ok(mut set) => set.insert(key),
            Err(poisoned) => poisoned.into_inner().insert(key),
        }
    }

    fn release_transition(&self, oi: &ObjectInfo) {
        let key = Self::transition_key(oi);
        match self.in_flight_transitions.lock() {
            Ok(mut set) => set.remove(&key),
            Err(poisoned) => poisoned.into_inner().remove(&key),
        };
    }

    fn reserve_bucket_compensation(&self, bucket: &str) -> bool {
        let inserted = match self.compensation_buckets.lock() {
            Ok(mut scheduled) => scheduled.insert(bucket.to_string()),
            Err(poisoned) => poisoned.into_inner().insert(bucket.to_string()),
        };
        if !inserted {
            return false;
        }
        Self::inc_counter(&self.compensation_scheduled_tasks);
        self.record_scanner_transition_state();
        true
    }

    fn schedule_bucket_compensation(self: &Arc<Self>, bucket: &str) -> bool {
        if !self.reserve_bucket_compensation(bucket) {
            return false;
        }
        let bucket = bucket.to_string();
        let state = Arc::clone(self);
        tokio::spawn(async move {
            Self::inc_counter(&state.compensation_running_tasks);
            state.record_scanner_transition_state();
            let Some(api) = runtime_sources::object_store_handle() else {
                state.finish_bucket_compensation(&bucket);
                Self::add_counter(&state.compensation_running_tasks, -1);
                state.record_scanner_transition_state();
                debug!(
                    event = EVENT_LIFECYCLE_TRANSITION_COMPENSATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket = %bucket,
                    state = "skipped",
                    reason = "object_layer_unavailable",
                    "Skipped transition compensation"
                );
                return;
            };

            if let Err(err) = enqueue_transition_for_existing_objects(api, &bucket).await {
                warn!(
                    event = EVENT_LIFECYCLE_TRANSITION_COMPENSATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket = %bucket,
                    state = "failed",
                    error = ?err,
                    "Transition compensation backfill failed"
                );
            } else {
                debug!(
                    event = EVENT_LIFECYCLE_TRANSITION_COMPENSATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket = %bucket,
                    state = "completed",
                    "Transition compensation completed"
                );
            }

            state.finish_bucket_compensation(&bucket);
            Self::add_counter(&state.compensation_running_tasks, -1);
            state.record_scanner_transition_state();
        });
        true
    }

    fn finish_bucket_compensation(&self, bucket: &str) {
        match self.compensation_buckets.lock() {
            Ok(mut scheduled) => {
                scheduled.remove(bucket);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(bucket);
                self.compensation_buckets.clear_poison();
            }
        }
    }

    #[inline]
    fn inc_counter(counter: &AtomicI64) {
        Self::add_counter(counter, 1);
    }

    #[inline]
    fn add_counter(counter: &AtomicI64, delta: i64) {
        counter.fetch_add(delta, Ordering::Relaxed);
    }

    #[inline]
    fn counter_value(counter: &AtomicI64) -> i64 {
        counter.load(Ordering::Relaxed)
    }

    fn scanner_transition_state_update(&self) -> ScannerLifecycleTransitionStateUpdate {
        ScannerLifecycleTransitionStateUpdate {
            queue_capacity: usize_to_u64_saturated(self.transition_queue_capacity),
            queued: usize_to_u64_saturated(self.transition_rx.len()),
            active: nonnegative_i64_to_u64(Self::counter_value(&self.active_tasks)),
            workers: nonnegative_i64_to_u64(Self::counter_value(&self.num_workers)),
            queue_full: nonnegative_i64_to_u64(Self::counter_value(&self.queue_full_tasks)),
            queue_send_timeout: nonnegative_i64_to_u64(Self::counter_value(&self.queue_send_timeout_tasks)),
            compensation_scheduled: nonnegative_i64_to_u64(Self::counter_value(&self.compensation_scheduled_tasks)),
            compensation_pending: self.compensation_pending_tasks(),
            compensation_running: nonnegative_i64_to_u64(Self::counter_value(&self.compensation_running_tasks)),
        }
    }

    fn record_scanner_transition_state(&self) {
        global_metrics().record_scanner_lifecycle_transition_state(self.scanner_transition_state_update());
    }

    pub fn manual_transition_queue_snapshot(&self) -> ManualTransitionQueueSnapshot {
        let state = self.scanner_transition_state_update();
        ManualTransitionQueueSnapshot {
            queue_capacity: state.queue_capacity,
            queued: state.queued,
            active: state.active,
            workers: state.workers,
            queue_full: state.queue_full,
            queue_send_timeout: state.queue_send_timeout,
            compensation_pending: state.compensation_pending,
            compensation_running: state.compensation_running,
        }
    }

    fn handle_immediate_enqueue_failure(self: &Arc<Self>, oi: &ObjectInfo, src: &LcEventSrc, failure: ImmediateEnqueueFailure) {
        Self::inc_counter(&self.missed_immediate_tasks);
        let scheduled = self.schedule_bucket_compensation(&oi.bucket);
        match failure {
            ImmediateEnqueueFailure::ForcedTimeout => {
                Self::inc_counter(&self.queue_send_timeout_tasks);
                debug!(
                    bucket = %oi.bucket,
                    object = %oi.name,
                    source = ?src,
                    compensation_scheduled = scheduled,
                    event = EVENT_LIFECYCLE_TRANSITION_COMPENSATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    state = "queue_timeout_forced",
                    "transition enqueue forced into timeout path for test fault injection"
                );
            }
            ImmediateEnqueueFailure::QueueClosed { timeout_ms } => match timeout_ms {
                Some(timeout_ms) => {
                    debug!(
                        bucket = %oi.bucket,
                        object = %oi.name,
                        source = ?src,
                        timeout_ms,
                        compensation_scheduled = scheduled,
                        event = EVENT_LIFECYCLE_TRANSITION_COMPENSATION,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                        state = "queue_closed",
                        "transition enqueue failed because the queue is closed"
                    );
                }
                None => {
                    debug!(
                        bucket = %oi.bucket,
                        object = %oi.name,
                        source = ?src,
                        compensation_scheduled = scheduled,
                        event = EVENT_LIFECYCLE_TRANSITION_COMPENSATION,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                        state = "queue_closed",
                        "transition enqueue failed because the queue is closed"
                    );
                }
            },
            ImmediateEnqueueFailure::QueueSendTimedOut { timeout_ms } => {
                Self::inc_counter(&self.queue_send_timeout_tasks);
                debug!(
                    bucket = %oi.bucket,
                    object = %oi.name,
                    source = ?src,
                    timeout_ms,
                    compensation_scheduled = scheduled,
                    event = EVENT_LIFECYCLE_TRANSITION_COMPENSATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    state = "queue_send_timed_out",
                    "transition enqueue timed out under backpressure"
                );
            }
        }
    }

    async fn queue_transition_task_outcome(
        self: &Arc<Self>,
        api: Option<Arc<ECStore>>,
        oi: &ObjectInfo,
        event: &lifecycle::Event,
        src: &LcEventSrc,
        manual_job_id: Option<Uuid>,
    ) -> TransitionEnqueueOutcome {
        if is_immediate_transition_source(src) && should_force_immediate_transition_enqueue_timeout() {
            self.handle_immediate_enqueue_failure(oi, src, ImmediateEnqueueFailure::ForcedTimeout);
            record_scanner_transition_enqueue_result(src, 1, false);
            self.record_scanner_transition_state();
            return TransitionEnqueueOutcome::QueueSendTimedOut;
        }

        // Deduplicate concurrent enqueues of the same object version. The claim is
        // released by the worker after the transition finishes (rustfs/backlog#1268).
        // This is a no-op, not a new enqueue, so it must not touch the enqueue
        // counters (the first enqueue already counted this object).
        if !self.reserve_transition(oi) {
            self.record_scanner_transition_state();
            return TransitionEnqueueOutcome::AlreadyInFlight;
        }

        let manual_result_key =
            manual_job_id.map(|_| manual_transition_worker_result_task_key(&oi.bucket, &oi.name, oi.version_id));
        if let (Some(job_id), Some(result_key)) = (manual_job_id, manual_result_key.as_deref()) {
            let Some(api) = api else {
                self.release_transition(oi);
                warn!(
                    event = EVENT_LIFECYCLE_WORKER_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket = %oi.bucket,
                    object = %oi.name,
                    version_id = %oi.version_id.map(|v| v.to_string()).unwrap_or_default(),
                    job_id = %job_id,
                    state = "manual_transition_task_journal_missing_store",
                    "Manual transition task was not enqueued because no task journal store was available"
                );
                self.record_scanner_transition_state();
                return TransitionEnqueueOutcome::TaskJournalFailed;
            };
            let task_record = ManualTransitionTaskRecord::new(
                job_id,
                result_key,
                oi.bucket.clone(),
                oi.name.clone(),
                oi.version_id,
                event.storage_class.clone(),
            )
            .with_object_metadata(oi.etag.clone(), oi.mod_time, oi.size, oi.is_latest);
            if let Err(err) = save_manual_transition_task_if_absent(api, &task_record).await {
                self.release_transition(oi);
                warn!(
                    event = EVENT_LIFECYCLE_WORKER_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket = %oi.bucket,
                    object = %oi.name,
                    version_id = %oi.version_id.map(|v| v.to_string()).unwrap_or_default(),
                    job_id = %job_id,
                    error = %err,
                    state = "manual_transition_task_journal_failed",
                    "Manual transition task was not enqueued because its task journal marker could not be persisted"
                );
                self.record_scanner_transition_state();
                return TransitionEnqueueOutcome::TaskJournalFailed;
            }
        }

        let task = TransitionTask {
            obj_info: oi.clone(),
            src: src.clone(),
            event: event.clone(),
            manual_job_id,
            manual_result_key,
        };
        if is_immediate_transition_source(src) {
            let outcome = match self.transition_tx.try_send(Some(task)) {
                Ok(()) => TransitionEnqueueOutcome::Queued,
                Err(async_channel::TrySendError::Full(task)) => {
                    Self::inc_counter(&self.queue_full_tasks);
                    let send_timeout = self.transition_queue_send_timeout;
                    match tokio::time::timeout(send_timeout, self.transition_tx.send(task)).await {
                        Ok(Ok(())) => TransitionEnqueueOutcome::Queued,
                        Ok(Err(_)) => {
                            self.handle_immediate_enqueue_failure(
                                oi,
                                src,
                                ImmediateEnqueueFailure::QueueClosed {
                                    timeout_ms: Some(send_timeout.as_millis() as u64),
                                },
                            );
                            TransitionEnqueueOutcome::QueueClosed
                        }
                        Err(_) => {
                            self.handle_immediate_enqueue_failure(
                                oi,
                                src,
                                ImmediateEnqueueFailure::QueueSendTimedOut {
                                    timeout_ms: send_timeout.as_millis() as u64,
                                },
                            );
                            TransitionEnqueueOutcome::QueueSendTimedOut
                        }
                    }
                }
                Err(async_channel::TrySendError::Closed(_task)) => {
                    self.handle_immediate_enqueue_failure(oi, src, ImmediateEnqueueFailure::QueueClosed { timeout_ms: None });
                    TransitionEnqueueOutcome::QueueClosed
                }
            };
            let queued = outcome == TransitionEnqueueOutcome::Queued;
            if !queued {
                self.release_transition(oi);
            }
            record_scanner_transition_enqueue_result(src, 1, queued);
            self.record_scanner_transition_state();
            return outcome;
        }

        let outcome = match self.transition_tx.try_send(Some(task)) {
            Ok(()) => TransitionEnqueueOutcome::Queued,
            Err(async_channel::TrySendError::Full(task)) => {
                Self::inc_counter(&self.queue_full_tasks);
                let send_timeout = self.transition_queue_send_timeout;
                match tokio::time::timeout(send_timeout, self.transition_tx.send(task)).await {
                    Ok(Ok(())) => TransitionEnqueueOutcome::Queued,
                    Ok(Err(_)) => {
                        self.schedule_bucket_compensation(&oi.bucket);
                        TransitionEnqueueOutcome::QueueClosed
                    }
                    Err(_) => {
                        Self::inc_counter(&self.queue_send_timeout_tasks);
                        self.schedule_bucket_compensation(&oi.bucket);
                        debug!(
                            bucket = %oi.bucket,
                            object = %oi.name,
                            source = ?src,
                            timeout_ms = send_timeout.as_millis() as u64,
                            event = EVENT_LIFECYCLE_TRANSITION_COMPENSATION,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            state = "queue_send_timed_out",
                            "Scanner transition enqueue timed out; scheduled bucket compensation"
                        );
                        TransitionEnqueueOutcome::QueueFull
                    }
                }
            }
            Err(async_channel::TrySendError::Closed(_)) => {
                self.schedule_bucket_compensation(&oi.bucket);
                debug!(
                    bucket = %oi.bucket,
                    object = %oi.name,
                    source = ?src,
                    event = EVENT_LIFECYCLE_TRANSITION_COMPENSATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    state = "queue_closed",
                    "transition enqueue failed because the queue is closed"
                );
                TransitionEnqueueOutcome::QueueClosed
            }
        };
        let queued = outcome == TransitionEnqueueOutcome::Queued;
        if !queued {
            self.release_transition(oi);
        }
        record_scanner_transition_enqueue_result(src, 1, queued);
        self.record_scanner_transition_state();
        outcome
    }

    pub async fn queue_transition_task(self: &Arc<Self>, oi: &ObjectInfo, event: &lifecycle::Event, src: &LcEventSrc) -> bool {
        self.queue_transition_task_outcome(None, oi, event, src, None)
            .await
            .is_handled()
    }

    pub async fn init(api: Arc<ECStore>) {
        let (configured, absolute_max, n) = resolve_transition_worker_count();
        let transition_state = runtime_sources::transition_state_handle();
        debug!(
            event = EVENT_LIFECYCLE_WORKER_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            configured_transition_workers = configured,
            absolute_max_workers = absolute_max,
            effective_transition_workers = n,
            transition_queue_capacity = transition_state.transition_queue_capacity,
            transition_queue_send_timeout_ms = transition_state.transition_queue_send_timeout.as_millis() as u64,
            state = "configured",
            "Lifecycle worker configuration resolved"
        );

        //self.objAPI = objAPI
        Self::update_workers(api, n).await;
    }

    pub fn pending_tasks(&self) -> usize {
        //let transition_rx = runtime_sources::transition_state_handle().transition_rx.lock().unwrap();
        self.transition_rx.len()
    }

    pub fn active_tasks(&self) -> i64 {
        Self::counter_value(&self.active_tasks)
    }

    pub fn missed_immediate_tasks(&self) -> i64 {
        Self::counter_value(&self.missed_immediate_tasks)
    }

    pub fn queue_full_tasks(&self) -> i64 {
        Self::counter_value(&self.queue_full_tasks)
    }

    pub fn queue_send_timeout_tasks(&self) -> i64 {
        Self::counter_value(&self.queue_send_timeout_tasks)
    }

    pub fn compensation_scheduled_tasks(&self) -> i64 {
        Self::counter_value(&self.compensation_scheduled_tasks)
    }

    pub fn compensation_pending_tasks(&self) -> u64 {
        match self.compensation_buckets.lock() {
            Ok(scheduled) => usize_to_u64_saturated(scheduled.len()),
            Err(poisoned) => usize_to_u64_saturated(poisoned.into_inner().len()),
        }
    }

    pub fn compensation_running_tasks(&self) -> i64 {
        Self::counter_value(&self.compensation_running_tasks)
    }

    async fn worker_with_cancel(api: Arc<ECStore>, cancel_token: CancellationToken) {
        let transition_state = runtime_sources::transition_state_handle();
        loop {
            select! {
                biased;

                _ = cancel_token.cancelled() => {
                    return;
                }
                task = transition_state.transition_rx.recv() => {
                    if task.is_err() {
                        break;
                    }
                    let task = task.expect("channel recv should succeed after error check");
                    if task.is_none() {
                        //self.transition_rx.close();
                        //drop(self.transition_rx);
                        return;
                    }
                    let task = task.expect("received None after None check");
                    if task.as_any().is::<TransitionTask>() {
                        let task = task.as_any().downcast_ref::<TransitionTask>().expect("TransitionTask downcast failed");

                        TransitionState::inc_counter(&transition_state.active_tasks);
                        transition_state.record_scanner_transition_state();

                        let obj_info_for_event = ObjectInfo {
                            bucket: task.obj_info.bucket.clone(),
                            name: task.obj_info.name.clone(),
                            size: task.obj_info.size,
                            version_id: task.obj_info.version_id,
                            ..Default::default()
                        };

                        if let Err(err) =
                            transition_object(api.clone(), &task.obj_info, LcAuditEvent::new(task.event.clone(), task.src.clone()))
                                .await
                        {
                            if let (Some(job_id), Some(result_key)) = (task.manual_job_id, task.manual_result_key.as_deref()) {
                                record_manual_transition_worker_result_for_task_with_reason(
                                    api.clone(),
                                    job_id,
                                    result_key,
                                    ManualTransitionWorkerResult::TierFailure,
                                    manual_transition_worker_failure_reason(&err),
                                )
                                .await;
                            }
                            global_metrics().record_scanner_transition_failed(1);
                            if !is_err_version_not_found(&err) && !is_err_object_not_found(&err) && !is_network_or_host_down(&err.to_string(), false) {
                                error!(
                                    event = EVENT_LIFECYCLE_TIER_OPERATION_FAILED,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                    bucket = %task.obj_info.bucket,
                                    object = %task.obj_info.name,
                                    version_id = %task.obj_info.version_id.map(|v| v.to_string()).unwrap_or_default(),
                                    tier = %task.event.storage_class,
                                    operation = "transition_object",
                                    error = %err,
                                    "Lifecycle tier operation failed"
                                );
                            }
                            emit_transition_failed_event(obj_info_for_event);
                        } else {
                            if let (Some(job_id), Some(result_key)) = (task.manual_job_id, task.manual_result_key.as_deref()) {
                                record_manual_transition_worker_result_for_task(
                                    api.clone(),
                                    job_id,
                                    result_key,
                                    ManualTransitionWorkerResult::Completed,
                                )
                                .await;
                            }
                            global_metrics().record_scanner_transition_completed(1);
                            let mut ts = TierStats {
                                total_size: task.obj_info.size as u64,
                                num_versions: 1,
                                ..Default::default()
                            };
                            if task.obj_info.is_latest {
                                ts.num_objects = 1;
                            }
                            transition_state.add_lastday_stats(&task.event.storage_class, ts);

                            emit_transition_complete_event(obj_info_for_event);
                        }
                        // Release the dedup claim so a later lifecycle pass can
                        // re-transition this object if needed (rustfs/backlog#1268).
                        transition_state.release_transition(&task.obj_info);
                        TransitionState::add_counter(&transition_state.active_tasks, -1);
                        transition_state.record_scanner_transition_state();
                    }
                }
                else => ()
            }
        }
    }

    pub fn add_lastday_stats(&self, tier: &str, ts: TierStats) {
        let mut tier_stats = self.lock_last_day_stats();
        tier_stats.entry(tier.to_string()).or_default().add_stats(ts);
    }

    pub fn get_daily_all_tier_stats(&self) -> DailyAllTierStats {
        let tier_stats = self.lock_last_day_stats();
        let mut res = DailyAllTierStats::with_capacity(tier_stats.len());
        for (tier, st) in tier_stats.iter() {
            res.insert(tier.clone(), st.clone());
        }
        res
    }

    fn lock_last_day_stats(&self) -> std::sync::MutexGuard<'_, HashMap<String, LastDayTierStats>> {
        match self.last_day_stats.lock() {
            Ok(stats) => stats,
            Err(poisoned) => {
                let mut stats = poisoned.into_inner();
                stats.clear();
                self.last_day_stats.clear_poison();
                stats
            }
        }
    }

    pub async fn update_workers(api: Arc<ECStore>, n: i64) {
        Self::update_workers_inner(api, n).await;
    }

    pub async fn update_workers_inner(api: Arc<ECStore>, n: i64) {
        let mut n = n;
        let requested = n;
        if n == 0 {
            let (_, _, effective) = resolve_transition_worker_count();
            n = effective;
        }
        // Allow environment override of maximum workers
        let absolute_max = resolve_transition_workers_absolute_max();
        n = n.clamp(0, absolute_max);

        Self::resize_workers_to(api, n, requested, absolute_max);
    }

    fn resize_workers_to(api: Arc<ECStore>, n: i64, requested: i64, absolute_max: i64) {
        let target = n as usize;
        let transition_state = runtime_sources::transition_state_handle();
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(err) => {
                warn!(
                    event = EVENT_LIFECYCLE_WORKER_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    error = %err,
                    state = "resize_failed",
                    "Lifecycle worker pool requires a Tokio runtime"
                );
                return;
            }
        };
        // Runtime lookup happens before locking, and the guard is dropped before
        // metrics/logging callbacks. Poison therefore means a Vec mutation may
        // have unwound and worker tracking cannot be reconstructed safely.
        let mut workers = transition_state
            .workers
            .lock()
            .expect("transition worker tracking mutex poisoned");
        let tracked_workers = workers.len();
        workers.retain(|worker| !worker.handle.is_finished());
        let pruned_finished_workers = tracked_workers.saturating_sub(workers.len());
        let previous_num_workers = workers.len() as i64;

        while workers.len() < target {
            let clone_api = api.clone();
            let cancel = CancellationToken::new();
            let worker_cancel = cancel.clone();
            let handle = runtime.spawn(async move {
                TransitionState::worker_with_cancel(clone_api, worker_cancel).await;
            });
            workers.push(TransitionWorker { cancel, handle });
        }

        while workers.len() > target {
            if let Some(worker) = workers.pop() {
                worker.cancel.cancel();
            }
        }

        let current_workers = workers.len() as i64;
        transition_state.num_workers.store(current_workers, Ordering::SeqCst);
        drop(workers);
        transition_state.record_scanner_transition_state();

        debug!(
            event = EVENT_LIFECYCLE_WORKER_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            requested_transition_workers = requested,
            effective_transition_workers = n,
            absolute_max_workers = absolute_max,
            previous_transition_workers = previous_num_workers,
            current_transition_workers = current_workers,
            pruned_finished_transition_workers = pruned_finished_workers,
            state = "resized",
            "Lifecycle worker pool resized"
        );
    }
}

async fn record_manual_transition_worker_result_for_task(
    api: Arc<ECStore>,
    job_id: Uuid,
    result_key: &str,
    result: ManualTransitionWorkerResult,
) {
    if let Err(err) =
        record_manual_transition_worker_result(api, job_id, result_key, result, manual_transition_queue_snapshot()).await
    {
        warn!(
            event = EVENT_LIFECYCLE_WORKER_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            job_id = %job_id,
            error = %err,
            state = "manual_transition_worker_result_failed",
            "Manual transition worker failed to persist job result"
        );
    }
}

async fn record_manual_transition_worker_result_for_task_with_reason(
    api: Arc<ECStore>,
    job_id: Uuid,
    result_key: &str,
    result: ManualTransitionWorkerResult,
    reason: ManualTransitionWorkerFailureReason,
) {
    if let Err(err) = record_manual_transition_worker_result_with_reason(
        api,
        job_id,
        result_key,
        result,
        manual_transition_queue_snapshot(),
        Some(reason),
    )
    .await
    {
        warn!(
            event = EVENT_LIFECYCLE_WORKER_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            job_id = %job_id,
            error = %err,
            reason = ?reason,
            state = "manual_transition_worker_result_failed",
            "Manual transition worker failed to persist job result"
        );
    }
}

fn manual_transition_worker_failure_reason(err: &Error) -> ManualTransitionWorkerFailureReason {
    if is_err_object_not_found(err) || is_err_version_not_found(err) {
        return ManualTransitionWorkerFailureReason::NotFound;
    }
    if is_err_permission_denied(err) {
        return ManualTransitionWorkerFailureReason::PermissionDenied;
    }
    if is_err_read_quorum(err) {
        return ManualTransitionWorkerFailureReason::Quorum;
    }
    if is_timeout(err) {
        return ManualTransitionWorkerFailureReason::Timeout;
    }
    if is_slow_down(err) {
        return ManualTransitionWorkerFailureReason::SlowDown;
    }
    let message = err.to_string();
    if is_remote_tier_permission_denied_error(&message) {
        return ManualTransitionWorkerFailureReason::PermissionDenied;
    }
    if is_remote_tier_network_error(&message) {
        return ManualTransitionWorkerFailureReason::Network;
    }
    ManualTransitionWorkerFailureReason::Unknown
}

fn is_remote_tier_permission_denied_error(message: &str) -> bool {
    message.contains("remote tier request failed with status 401")
        || message.contains("remote tier request failed with status 403")
        || message.contains("InvalidAccessKeyId")
        || message.contains("AccessDenied")
        || message.contains("SignatureDoesNotMatch")
}

fn is_remote_tier_network_error(message: &str) -> bool {
    message.contains("client error (SendRequest)")
        || message.contains("dispatch failure")
        || is_network_or_host_down(message, false)
}

fn is_err_permission_denied(err: &Error) -> bool {
    match err {
        Error::VolumeAccessDenied | Error::FileAccessDenied | Error::PrefixAccessDenied(_, _) | Error::DiskAccessDenied => true,
        Error::Io(io) => io.kind() == std::io::ErrorKind::PermissionDenied,
        _ => false,
    }
}

fn is_timeout(err: &Error) -> bool {
    match err {
        Error::Timeout => true,
        Error::Io(io) => io.kind() == std::io::ErrorKind::TimedOut,
        _ => false,
    }
}

fn is_slow_down(err: &Error) -> bool {
    matches!(err, Error::SlowDown)
}

/// Resolves the expiry worker count from the single documented knob,
/// `RUSTFS_MAX_EXPIRY_WORKERS`: a set, parsable, non-zero value wins;
/// anything else falls back to `min(cpus, 16)`. The historical
/// `_RUSTFS_ILM_EXPIRATION_WORKERS` silent override and the
/// `RUSTFS_DEFAULT_EXPIRY_WORKERS` zero-fallback were undocumented, unset in
/// every known deployment, and are removed (backlog#1832).
fn expiry_worker_count() -> usize {
    let default = std::cmp::min(num_cpus::get(), 16);
    match env::var(ENV_MAX_EXPIRY_WORKERS) {
        Ok(value) => match value.parse::<usize>() {
            Ok(workers) if workers > 0 => workers,
            _ => default,
        },
        Err(_) => default,
    }
}

pub async fn init_background_expiry(api: Arc<ECStore>) {
    let workers = expiry_worker_count();

    ExpiryState::resize_workers(workers, api.clone()).await;
    let _ = spawn_tier_free_version_recovery_once(api.clone(), &TIER_FREE_VERSION_RECOVERY_STARTED);
    spawn_tier_delete_journal_recovery_once(api.clone());
    spawn_transition_transaction_recovery_once(api.clone());
    spawn_manual_transition_job_recovery_once(api);
}

fn spawn_manual_transition_job_recovery_once(api: Arc<ECStore>) -> Option<JoinHandle<()>> {
    if MANUAL_TRANSITION_JOB_RECOVERY_STARTED.set(()).is_err() {
        return None;
    }

    Some(tokio::spawn(async move {
        let cancel_token = runtime_sources::background_services_cancel_token().unwrap_or_default();
        select! {
            _ = cancel_token.cancelled() => {}
            result = recover_manual_transition_jobs(api, DEFAULT_MANUAL_TRANSITION_JOB_RECOVERY_LIMIT) => {
                match result {
                    Ok(stats) => {
                        debug!(
                            event = EVENT_LIFECYCLE_WORKER_STATE,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            scanned = stats.scanned,
                            resumed = stats.resumed,
                            cancelled = stats.cancelled,
                            unknown = stats.unknown,
                            skipped = stats.skipped,
                            failed = stats.failed,
                            truncated = stats.truncated,
                            next_marker = ?stats.next_marker,
                            state = "manual_transition_recovery_completed",
                            "Manual transition job recovery completed"
                        );
                    }
                    Err(err) => {
                        warn!(
                            event = EVENT_LIFECYCLE_WORKER_STATE,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            error = %err,
                            state = "manual_transition_recovery_failed",
                            "Manual transition job recovery failed"
                        );
                    }
                }
            }
        }
    }))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManualTransitionJobRecoveryStats {
    pub scanned: u64,
    pub resumed: u64,
    pub cancelled: u64,
    pub unknown: u64,
    pub skipped: u64,
    pub failed: u64,
    pub next_marker: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManualTransitionJobRecoveryOutcome {
    Resumed,
    Cancelled,
    Unknown,
    Skipped,
}

async fn recover_manual_transition_jobs(api: Arc<ECStore>, limit: usize) -> Result<ManualTransitionJobRecoveryStats, Error> {
    let mut marker = None;
    let mut total = ManualTransitionJobRecoveryStats::default();

    loop {
        let stats = recover_manual_transition_jobs_once(api.clone(), limit, marker).await?;
        total.scanned = total.scanned.saturating_add(stats.scanned);
        total.resumed = total.resumed.saturating_add(stats.resumed);
        total.cancelled = total.cancelled.saturating_add(stats.cancelled);
        total.unknown = total.unknown.saturating_add(stats.unknown);
        total.skipped = total.skipped.saturating_add(stats.skipped);
        total.failed = total.failed.saturating_add(stats.failed);

        if !stats.truncated {
            total.truncated = false;
            total.next_marker = None;
            return Ok(total);
        }
        let Some(next_marker) = stats.next_marker else {
            return Err(Error::other("manual transition job recovery page is truncated without a next marker"));
        };
        total.truncated = true;
        total.next_marker = Some(next_marker.clone());
        marker = Some(next_marker);
    }
}

pub async fn recover_manual_transition_jobs_once(
    api: Arc<ECStore>,
    limit: usize,
    marker: Option<String>,
) -> Result<ManualTransitionJobRecoveryStats, Error> {
    if limit == 0 {
        return Err(Error::other("manual transition job recovery limit must be greater than zero"));
    }
    let list_limit = i32::try_from(limit).unwrap_or(i32::MAX);
    let page = api
        .clone()
        .list_objects_v2(
            RUSTFS_META_BUCKET,
            MANUAL_TRANSITION_JOB_RECORD_PREFIX,
            marker,
            None,
            list_limit,
            false,
            None,
            false,
        )
        .await?;
    let mut stats = ManualTransitionJobRecoveryStats {
        next_marker: page.next_continuation_token,
        truncated: page.is_truncated,
        ..Default::default()
    };

    for object in page.objects {
        stats.scanned = stats.scanned.saturating_add(1);
        let job_id = match manual_transition_job_id_from_record_object_name(&object.name) {
            Ok(job_id) => job_id,
            Err(err) => {
                stats.failed = stats.failed.saturating_add(1);
                warn!(
                    event = EVENT_LIFECYCLE_WORKER_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    object = %object.name,
                    error = %err,
                    state = "manual_transition_recovery_skipped",
                    "Manual transition recovery skipped a corrupt job record path"
                );
                continue;
            }
        };
        match recover_manual_transition_job(api.clone(), job_id, manual_transition_queue_snapshot()).await {
            Ok(ManualTransitionJobRecoveryOutcome::Resumed) => stats.resumed = stats.resumed.saturating_add(1),
            Ok(ManualTransitionJobRecoveryOutcome::Cancelled) => stats.cancelled = stats.cancelled.saturating_add(1),
            Ok(ManualTransitionJobRecoveryOutcome::Unknown) => stats.unknown = stats.unknown.saturating_add(1),
            Ok(ManualTransitionJobRecoveryOutcome::Skipped) => stats.skipped = stats.skipped.saturating_add(1),
            Err(err) => {
                stats.failed = stats.failed.saturating_add(1);
                warn!(
                    event = EVENT_LIFECYCLE_WORKER_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    job_id = %job_id,
                    error = %err,
                    state = "manual_transition_recovery_failed",
                    "Manual transition recovery failed a job"
                );
            }
        }
    }

    Ok(stats)
}

async fn recover_manual_transition_job(
    api: Arc<ECStore>,
    job_id: Uuid,
    queue_snapshot: ManualTransitionQueueSnapshot,
) -> Result<ManualTransitionJobRecoveryOutcome, Error> {
    let (mut record, mut etag) = match load_manual_transition_job_record_with_etag(api.clone(), job_id).await {
        Ok(record) => record,
        Err(Error::ConfigNotFound) => return Ok(ManualTransitionJobRecoveryOutcome::Skipped),
        Err(err) => return Err(err),
    };
    if record.is_terminal() || !manual_transition_job_lease_expired(&record) {
        return Ok(ManualTransitionJobRecoveryOutcome::Skipped);
    }

    let recovery_unknown_snapshot = ManualTransitionQueueSnapshot::default();
    if record.scan_completed {
        let reconciled = match reconcile_manual_transition_worker_results_if_owned(
            api.clone(),
            job_id,
            record.lease_id,
            recovery_unknown_snapshot,
        )
        .await
        {
            Ok(record) => record,
            Err(Error::PreconditionFailed) => return Ok(ManualTransitionJobRecoveryOutcome::Skipped),
            Err(err) => return Err(err),
        };
        if reconciled.is_terminal() {
            release_manual_transition_recovery_admission(api, &reconciled).await;
            return match reconciled.state {
                ManualTransitionJobState::Cancelled => Ok(ManualTransitionJobRecoveryOutcome::Cancelled),
                ManualTransitionJobState::Unknown => Ok(ManualTransitionJobRecoveryOutcome::Unknown),
                _ => Ok(ManualTransitionJobRecoveryOutcome::Resumed),
            };
        }
        if reconciled != record {
            (record, etag) = load_manual_transition_job_record_with_etag(api.clone(), job_id).await?;
        }
    }

    if record.cancel_requested {
        record.cancel_after_recovery(queue_snapshot);
        return match save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await {
            Ok(()) => {
                release_manual_transition_recovery_admission(api, &record).await;
                Ok(ManualTransitionJobRecoveryOutcome::Cancelled)
            }
            Err(Error::PreconditionFailed) => Ok(ManualTransitionJobRecoveryOutcome::Skipped),
            Err(err) => Err(err),
        };
    }

    let previous_lease_id = record.lease_id;
    record.claim_recovery_lease(manual_transition_recovery_owner_id(), queue_snapshot);
    let recovery_lease_id = record.lease_id;
    match save_manual_transition_job_record_if_current(api.clone(), &record, &etag).await {
        Ok(()) => {}
        Err(Error::PreconditionFailed) => return Ok(ManualTransitionJobRecoveryOutcome::Skipped),
        Err(err) => return Err(err),
    }
    delete_manual_transition_scope_admission_if_current(api.clone(), &record.scope_key, record.job_id, previous_lease_id).await?;

    match claim_manual_transition_scope_admission(api.clone(), &ManualTransitionScopeAdmission::from_job(&record)).await {
        Ok(ManualTransitionScopeAdmissionClaim::Claimed) => {}
        Ok(ManualTransitionScopeAdmissionClaim::Conflict(_)) => {
            abandon_manual_transition_recovery_lease(api, job_id, recovery_lease_id).await?;
            return Ok(ManualTransitionJobRecoveryOutcome::Skipped);
        }
        Err(err) => {
            abandon_manual_transition_recovery_lease(api, job_id, recovery_lease_id).await?;
            return Err(err);
        }
    }

    let replay = replay_manual_transition_pending_tasks(api.clone(), job_id).await?;
    if matches!(
        replay,
        ManualTransitionPendingTaskReplay::Queued | ManualTransitionPendingTaskReplay::Deferred
    ) {
        spawn_manual_transition_recovery_heartbeat(api, job_id, recovery_lease_id);
        return Ok(ManualTransitionJobRecoveryOutcome::Resumed);
    }

    let mut marked_unknown = false;
    let record = match update_manual_transition_job_record(api.clone(), job_id, Some(recovery_lease_id), |record| {
        marked_unknown = record.mark_unknown_if_worker_results_lost(recovery_unknown_snapshot)
            || record.mark_unknown_if_recovery_would_skip_pending_page(recovery_unknown_snapshot);
        marked_unknown
    })
    .await
    {
        Ok(record) => record,
        Err(Error::PreconditionFailed) => return Ok(ManualTransitionJobRecoveryOutcome::Skipped),
        Err(err) => return Err(err),
    };
    if marked_unknown {
        release_manual_transition_recovery_admission(api, &record).await;
        return Ok(ManualTransitionJobRecoveryOutcome::Unknown);
    }

    let mut options = record.resume_options();
    options.job_id = Some(job_id);
    options.cancel_check = Some(manual_transition_recovery_cancel_check(api.clone(), job_id));
    options.progress_sink = Some(manual_transition_recovery_progress_sink(api.clone(), job_id, recovery_lease_id));
    let result = enqueue_transition_for_existing_objects_scoped(api.clone(), &record.bucket, options).await;
    let final_record = match finalize_recovered_manual_transition_job(api.clone(), job_id, recovery_lease_id, result).await {
        Ok(record) => record,
        Err(Error::PreconditionFailed) => return Ok(ManualTransitionJobRecoveryOutcome::Skipped),
        Err(err) => return Err(err),
    };
    if final_record.is_terminal() {
        release_manual_transition_recovery_admission(api, &final_record).await;
    } else {
        spawn_manual_transition_recovery_heartbeat(api, job_id, recovery_lease_id);
    }
    Ok(ManualTransitionJobRecoveryOutcome::Resumed)
}

async fn replay_manual_transition_pending_tasks(
    api: Arc<ECStore>,
    job_id: Uuid,
) -> Result<ManualTransitionPendingTaskReplay, Error> {
    let transition_state = runtime_sources::transition_state_handle();
    let limit = transition_state.transition_queue_capacity.max(1);
    let pending = load_manual_transition_pending_task_records(api.clone(), job_id, limit).await?;
    if pending.is_empty() {
        return Ok(ManualTransitionPendingTaskReplay::Empty);
    }

    let mut queued = 0usize;
    for task in pending {
        let mod_time = match task.mod_time_unix_nanos {
            Some(nanos) => Some(
                OffsetDateTime::from_unix_timestamp_nanos(nanos)
                    .map_err(|_| Error::other("manual transition task journal mod_time is invalid"))?,
            ),
            None => None,
        };
        let object = ObjectInfo {
            bucket: task.bucket,
            name: task.object,
            version_id: task.version_id,
            etag: task.etag,
            mod_time,
            size: task.size.unwrap_or(0),
            is_latest: task.is_latest.unwrap_or(false),
            ..Default::default()
        };
        let event = lifecycle::Event {
            action: if object.version_id.is_some() {
                IlmAction::TransitionVersionAction
            } else {
                IlmAction::TransitionAction
            },
            storage_class: task.storage_class,
            ..Default::default()
        };

        match transition_state
            .queue_transition_task_outcome(Some(api.clone()), &object, &event, &LcEventSrc::Scanner, Some(job_id))
            .await
        {
            TransitionEnqueueOutcome::Queued | TransitionEnqueueOutcome::AlreadyInFlight => {
                queued = queued.saturating_add(1);
            }
            TransitionEnqueueOutcome::QueueFull
            | TransitionEnqueueOutcome::QueueClosed
            | TransitionEnqueueOutcome::QueueSendTimedOut
            | TransitionEnqueueOutcome::TaskJournalFailed => {
                break;
            }
        }
    }

    if queued > 0 {
        Ok(ManualTransitionPendingTaskReplay::Queued)
    } else {
        Ok(ManualTransitionPendingTaskReplay::Deferred)
    }
}

fn manual_transition_recovery_owner_id() -> &'static str {
    "ecstore-manual-transition-recovery"
}

fn manual_transition_recovery_cancel_check(api: Arc<ECStore>, job_id: Uuid) -> ManualTransitionCancelCheck {
    Arc::new(move || {
        let api = api.clone();
        Box::pin(async move {
            match load_manual_transition_job_record(api, job_id).await {
                Ok(record) => record.cancel_requested || record.is_terminal(),
                Err(_) => true,
            }
        })
    })
}

fn manual_transition_recovery_progress_sink(api: Arc<ECStore>, job_id: Uuid, lease_id: Uuid) -> ManualTransitionProgressSink {
    Arc::new(move |report| {
        let api = api.clone();
        Box::pin(async move {
            persist_manual_transition_job_progress_if_owned(api, job_id, lease_id, &report, manual_transition_queue_snapshot())
                .await
                .map(|_| ())
        })
    })
}

async fn finalize_recovered_manual_transition_job(
    api: Arc<ECStore>,
    job_id: Uuid,
    expected_lease_id: Uuid,
    result: Result<ManualTransitionRunReport, Error>,
) -> Result<ManualTransitionJobRecord, Error> {
    update_manual_transition_job_record(api, job_id, Some(expected_lease_id), |record| {
        if record.is_terminal() {
            return false;
        }
        match &result {
            Ok(report) => record.complete(report.clone(), manual_transition_queue_snapshot()),
            Err(err) => record.fail(format!("manual transition recovery failed: {err}")),
        }
        true
    })
    .await
}

async fn release_manual_transition_recovery_admission(api: Arc<ECStore>, record: &ManualTransitionJobRecord) {
    if let Err(err) =
        delete_manual_transition_scope_admission_if_current(api, &record.scope_key, record.job_id, record.lease_id).await
    {
        debug!(
            event = EVENT_LIFECYCLE_WORKER_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            job_id = %record.job_id,
            error = %err,
            state = "manual_transition_recovery_admission_release_failed",
            "Manual transition recovery failed to release admission"
        );
    }
}

fn spawn_manual_transition_recovery_heartbeat(api: Arc<ECStore>, job_id: Uuid, lease_id: Uuid) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            match renew_manual_transition_job_lease_if_owned(api.clone(), job_id, lease_id, manual_transition_queue_snapshot())
                .await
            {
                Ok(record) if record.is_terminal() => {
                    release_manual_transition_recovery_admission(api, &record).await;
                    return;
                }
                Ok(_) => {}
                Err(Error::ConfigNotFound | Error::PreconditionFailed) => return,
                Err(err) => {
                    warn!(
                        event = EVENT_LIFECYCLE_WORKER_STATE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                        job_id = %job_id,
                        error = %err,
                        state = "manual_transition_recovery_heartbeat_failed",
                        "Manual transition recovery failed to renew job lease"
                    );
                }
            }
        }
    });
}

async fn abandon_manual_transition_recovery_lease(api: Arc<ECStore>, job_id: Uuid, lease_id: Uuid) -> Result<(), Error> {
    match update_manual_transition_job_record(api, job_id, Some(lease_id), |record| {
        if record.is_terminal() {
            return false;
        }
        record.abandon_recovery_lease(lease_id);
        true
    })
    .await
    {
        Ok(_) | Err(Error::ConfigNotFound | Error::PreconditionFailed) => Ok(()),
        Err(err) => Err(err),
    }
}

fn tier_free_version_recovery_enabled() -> bool {
    resolve_tier_free_version_recovery_enabled(env::var(ENV_TIER_FREE_VERSION_RECOVERY_ENABLED))
}

fn resolve_tier_free_version_recovery_enabled(value: Result<String, env::VarError>) -> bool {
    match value {
        Ok(value) => match parse_bool(value.trim()) {
            Ok(enabled) => enabled,
            Err(_) => {
                warn!(
                    event = EVENT_LIFECYCLE_WORKER_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    env = ENV_TIER_FREE_VERSION_RECOVERY_ENABLED,
                    reason = "invalid_boolean",
                    "Invalid tier free-version recovery setting; using enabled default"
                );
                true
            }
        },
        Err(env::VarError::NotPresent) => true,
        Err(env::VarError::NotUnicode(_)) => {
            warn!(
                event = EVENT_LIFECYCLE_WORKER_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                env = ENV_TIER_FREE_VERSION_RECOVERY_ENABLED,
                "Non-Unicode tier free-version recovery setting; using enabled default"
            );
            true
        }
    }
}

fn spawn_tier_free_version_recovery_once(api: Arc<ECStore>, started: &OnceLock<()>) -> Option<JoinHandle<()>> {
    if !tier_free_version_recovery_enabled() {
        warn!(
            event = EVENT_LIFECYCLE_WORKER_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            env = ENV_TIER_FREE_VERSION_RECOVERY_ENABLED,
            "Tier free-version recovery disabled by configuration"
        );
        return None;
    }
    if started.set(()).is_err() {
        return None;
    }

    Some(tokio::spawn(async move {
        let cancel_token = runtime_sources::background_services_cancel_token().unwrap_or_default();
        let expiry_state = runtime_sources::expiry_state_handle();
        run_tier_free_version_recovery_loop(
            cancel_token,
            expiry_state,
            jitter_tier_free_version_recovery_delay,
            move |bucket_marker, object_marker, recovery_cancel| {
                let api = Arc::clone(&api);
                async move {
                    recover_tier_free_versions_with_cancel(
                        api,
                        DEFAULT_FREE_VERSION_RECOVERY_LIMIT,
                        bucket_marker,
                        object_marker,
                        recovery_cancel,
                    )
                    .await
                }
            },
        )
        .await;
    }))
}

async fn run_tier_free_version_recovery_loop<F, Fut>(
    cancel_token: CancellationToken,
    expiry_state: Arc<RwLock<ExpiryState>>,
    jitter_delay: fn(StdDuration) -> StdDuration,
    mut recover: F,
) where
    F: FnMut(Option<String>, Option<String>, CancellationToken) -> Fut,
    Fut: Future<Output = crate::error::Result<FreeVersionRecoveryStats>>,
{
    let recovery_notify = Arc::clone(&expiry_state.read().await.recovery_notify);
    let mut schedule = TierFreeVersionRecoverySchedule::default();

    loop {
        if !wait_for_tier_free_version_recovery(&cancel_token, recovery_notify.as_ref(), &mut schedule, jitter_delay).await {
            return;
        }

        let started_at = tokio::time::Instant::now();
        let recovery_cancel = cancel_token.child_token();
        let recovery_result =
            recover(schedule.bucket_marker.clone(), schedule.object_marker.clone(), recovery_cancel.clone()).await;
        recovery_cancel.cancel();
        if cancel_token.is_cancelled() {
            return;
        }

        match recovery_result {
            Ok(stats) => {
                let elapsed = started_at.elapsed();
                schedule.record_success(&stats, elapsed);
                let (pending_tasks, active_tasks) = {
                    let state = expiry_state.read().await;
                    (state.pending_tasks(), state.stats.active_tasks())
                };
                debug!(
                    event = EVENT_LIFECYCLE_WORKER_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    duration_ms = elapsed.as_millis(),
                    scanned = stats.scanned,
                    scanned_entries = stats.scanned_entries,
                    buckets_scanned = stats.buckets_scanned,
                    enqueued = stats.enqueued,
                    failed = stats.failed,
                    truncated = stats.truncated,
                    next_bucket_marker = ?schedule.bucket_marker,
                    next_object_marker = ?schedule.object_marker,
                    pending_tasks,
                    active_tasks,
                    nominal_next_recovery_delay_secs = schedule.next_delay.as_secs(),
                    idle_backoff_multiplier =
                        schedule.next_delay.as_secs() / TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL.as_secs(),
                    follow_up_sweep = schedule.follow_up_sweep,
                    "Recovered tier free-version cleanup tasks"
                );
            }
            Err(err) => {
                let elapsed = started_at.elapsed();
                schedule.record_failure(elapsed);
                rustfs_io_metrics::record_stage_duration(
                    "lifecycle_free_version_recovery_failed",
                    elapsed.as_secs_f64() * 1000.0,
                );
                warn!(
                    event = EVENT_LIFECYCLE_WORKER_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    duration_ms = elapsed.as_millis(),
                    next_bucket_marker = ?schedule.bucket_marker,
                    next_object_marker = ?schedule.object_marker,
                    nominal_next_recovery_delay_secs = schedule.next_delay.as_secs(),
                    error = ?err,
                    "Failed to recover tier free-version cleanup tasks"
                );
            }
        }
    }
}

async fn wait_for_tier_free_version_recovery(
    cancel_token: &CancellationToken,
    recovery_notify: &Notify,
    schedule: &mut TierFreeVersionRecoverySchedule,
    jitter_delay: fn(StdDuration) -> StdDuration,
) -> bool {
    let next_delay = if schedule.jitter_next_delay {
        jitter_delay(schedule.next_delay)
    } else {
        schedule.next_delay
    };
    let sleep_delay = next_delay.saturating_sub(schedule.previous_run_duration);
    schedule.previous_run_duration = StdDuration::ZERO;
    schedule.jitter_next_delay = false;
    let sleep = tokio::time::sleep(sleep_delay);
    tokio::pin!(sleep);
    let mut recovery_request_consumed = false;

    loop {
        select! {
            biased;
            _ = cancel_token.cancelled() => return false,
            _ = &mut sleep => return true,
            _ = recovery_notify.notified(), if !recovery_request_consumed => {
                schedule.request_retry();
                recovery_request_consumed = true;
                let requested_deadline = tokio::time::Instant::now() + schedule.next_delay;
                if requested_deadline < sleep.deadline() {
                    sleep.as_mut().reset(requested_deadline);
                }
            }
        }
    }
}

fn jitter_tier_free_version_recovery_delay(delay: StdDuration) -> StdDuration {
    if delay.is_zero() {
        return delay;
    }
    let delay = delay.clamp(TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL, TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL);

    let delay_millis = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
    let jitter_window_millis = delay_millis.saturating_mul(TIER_FREE_VERSION_RECOVERY_JITTER_PERCENT) / 100;
    if jitter_window_millis == 0 {
        return delay;
    }

    let lower_bound = delay
        .saturating_sub(StdDuration::from_millis(jitter_window_millis))
        .max(TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
    let upper_bound = delay
        .saturating_add(StdDuration::from_millis(jitter_window_millis))
        .min(TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL);
    let lower_millis = u64::try_from(lower_bound.as_millis()).unwrap_or(u64::MAX);
    let upper_millis = u64::try_from(upper_bound.as_millis()).unwrap_or(u64::MAX);

    StdDuration::from_millis(rand::rng().random_range(lower_millis..=upper_millis))
}

#[derive(Debug, Clone)]
struct TierFreeVersionRecoverySchedule {
    next_delay: StdDuration,
    idle_interval: StdDuration,
    failure_interval: StdDuration,
    previous_run_duration: StdDuration,
    jitter_next_delay: bool,
    bucket_marker: Option<String>,
    object_marker: Option<String>,
    follow_up_sweep: bool,
}

impl Default for TierFreeVersionRecoverySchedule {
    fn default() -> Self {
        Self {
            next_delay: StdDuration::ZERO,
            idle_interval: TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL,
            failure_interval: TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL,
            previous_run_duration: StdDuration::ZERO,
            jitter_next_delay: false,
            bucket_marker: None,
            object_marker: None,
            follow_up_sweep: false,
        }
    }
}

impl TierFreeVersionRecoverySchedule {
    fn reset_idle_interval(&mut self) {
        self.idle_interval = TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL;
        self.next_delay = TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL;
        self.previous_run_duration = StdDuration::ZERO;
        self.jitter_next_delay = false;
    }

    fn request_retry(&mut self) {
        if self.bucket_marker.is_some() || self.object_marker.is_some() {
            self.follow_up_sweep = true;
        }
        self.reset_idle_interval();
    }

    fn record_failure(&mut self, _run_duration: StdDuration) {
        self.idle_interval = TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL;
        self.next_delay = self.failure_interval;
        self.failure_interval =
            std::cmp::min(self.failure_interval.saturating_mul(2), TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL);
        // Keep the full backoff even after a long failed run, so a run whose
        // duration exceeds the interval cannot restart immediately.
        self.previous_run_duration = StdDuration::ZERO;
        self.jitter_next_delay = false;
    }

    fn record_success(&mut self, stats: &FreeVersionRecoveryStats, run_duration: StdDuration) {
        self.failure_interval = TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL;
        if stats.enqueued > 0 || stats.failed > 0 {
            self.follow_up_sweep = true;
        }

        self.bucket_marker = stats.next_bucket_marker.clone();
        self.object_marker = stats.next_object_marker.clone();
        if stats.truncated {
            self.next_delay = TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL;
            self.previous_run_duration = run_duration;
            self.jitter_next_delay = false;
            return;
        }

        self.bucket_marker = None;
        self.object_marker = None;
        if self.follow_up_sweep {
            self.follow_up_sweep = false;
            self.idle_interval = TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL;
            self.next_delay = TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL;
            self.previous_run_duration = run_duration;
            self.jitter_next_delay = false;
            return;
        }

        self.next_delay = self.idle_interval;
        self.previous_run_duration = run_duration;
        self.jitter_next_delay = true;
        self.idle_interval = std::cmp::min(self.idle_interval.saturating_mul(2), TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL);
    }
}

fn spawn_tier_delete_journal_recovery_once(api: Arc<ECStore>) {
    let Some(cancel_token) = api.ctx.background_cancel_token() else {
        error!(
            event = EVENT_LIFECYCLE_WORKER_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            store_id = %api.id,
            "Tier delete journal recovery was not started because the store shutdown token is unavailable"
        );
        return;
    };
    if !api.ctx.mark_tier_delete_journal_recovery_started(api.id) {
        return;
    }
    tokio::spawn(async move {
        run_tier_delete_journal_recovery_loop(api, cancel_token).await;
    });
}

fn spawn_transition_transaction_recovery_once(api: Arc<ECStore>) {
    let Some(cancel_token) = api.ctx.background_cancel_token() else {
        error!(
            event = EVENT_LIFECYCLE_WORKER_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            store_id = %api.id,
            "Transition transaction recovery was not started because the store shutdown token is unavailable"
        );
        return;
    };
    if !api.ctx.mark_transition_transaction_recovery_started(api.id) {
        return;
    }
    tokio::spawn(async move {
        run_transition_transaction_recovery_loop(api, cancel_token).await;
    });
}

#[derive(Debug, Clone)]
struct StaleMultipartUploadCandidate {
    path: String,
    initiated: OffsetDateTime,
    metadata: Option<HashMap<String, String>>,
}

fn parse_stale_uploads_duration(env_key: &str, default: StdDuration) -> StdDuration {
    env::var(env_key)
        .ok()
        .and_then(|value| rustfs_madmin::utils::parse_duration(&value).ok())
        .filter(|duration| !duration.is_zero())
        .unwrap_or(default)
}

fn stale_uploads_expiry() -> StdDuration {
    parse_stale_uploads_duration(ENV_STALE_UPLOADS_EXPIRY, DEFAULT_STALE_UPLOADS_EXPIRY)
}

fn stale_uploads_cleanup_interval() -> StdDuration {
    parse_stale_uploads_duration(ENV_STALE_UPLOADS_CLEANUP_INTERVAL, DEFAULT_STALE_UPLOADS_CLEANUP_INTERVAL)
}

fn encode_stale_upload_id(upload_uuid: &str) -> String {
    base64_simd::URL_SAFE_NO_PAD
        .encode_to_string(format!("{}.{}", runtime_sources::deployment_id().unwrap_or_default(), upload_uuid).as_bytes())
}

fn initiated_from_upload_dir(upload_dir: &str, fallback: Option<OffsetDateTime>) -> OffsetDateTime {
    upload_dir
        .split_once('x')
        .and_then(|(_, nanos)| nanos.parse::<i128>().ok())
        .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
        .or(fallback)
        .unwrap_or_else(OffsetDateTime::now_utc)
}

fn stale_upload_default_due(initiated: OffsetDateTime, default_expiry: StdDuration) -> OffsetDateTime {
    initiated + time::Duration::seconds(default_expiry.as_secs() as i64)
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
async fn stale_upload_current_size(set: &Arc<SetDisks>, metadata: &HashMap<String, String>, upload_dir: &str) -> Option<usize> {
    stale_upload_current_size_with_opts(set, metadata, upload_dir, false).await
}

async fn stale_upload_current_size_with_opts(
    set: &Arc<SetDisks>,
    metadata: &HashMap<String, String>,
    upload_dir: &str,
    no_lock: bool,
) -> Option<usize> {
    let bucket = metadata.get(RUSTFS_MULTIPART_BUCKET_KEY)?;
    let object = metadata.get(RUSTFS_MULTIPART_OBJECT_KEY)?;
    let upload_id = encode_stale_upload_id(upload_dir);
    let data_movement = rustfs_utils::http::contains_key_str(metadata, rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD);
    let parts = set
        .list_object_parts(
            bucket,
            object,
            &upload_id,
            None,
            MAX_PARTS_COUNT,
            &ObjectOptions {
                data_movement,
                no_lock,
                ..Default::default()
            },
        )
        .await
        .ok()?;

    Some(
        parts
            .parts
            .iter()
            .map(|part| part.actual_size.max(part.size as i64).max(0) as usize)
            .sum(),
    )
}

async fn stale_upload_lifecycle_due(
    set: &Arc<SetDisks>,
    metadata: &HashMap<String, String>,
    initiated: OffsetDateTime,
    upload_dir: &str,
    no_lock: bool,
) -> Option<OffsetDateTime> {
    if rustfs_utils::http::contains_key_str(metadata, rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD) {
        return None;
    }

    let bucket = metadata.get(RUSTFS_MULTIPART_BUCKET_KEY)?;
    let object = metadata.get(RUSTFS_MULTIPART_OBJECT_KEY)?;

    let lifecycle = match metadata_boundary::get_lifecycle_config(bucket).await {
        Ok((lifecycle, _)) => lifecycle,
        Err(_) => return None,
    };

    let object_opts = ObjectOpts {
        name: object.clone(),
        user_tags: metadata.get(AMZ_OBJECT_TAGGING).cloned().unwrap_or_default(),
        mod_time: Some(initiated),
        size: stale_upload_current_size_with_opts(set, metadata, upload_dir, no_lock)
            .await
            .unwrap_or_default(),
        is_latest: true,
        delete_marker: false,
        user_defined: metadata.clone(),
        ..Default::default()
    };

    abort_incomplete_multipart_upload_due(&lifecycle, &object_opts)
        .await
        .map(|(due, _)| due)
}

async fn read_stale_multipart_candidate(
    disk: &Disk,
    sha_dir: &str,
    upload_dir: &str,
) -> Result<StaleMultipartUploadCandidate, DiskError> {
    let metadata_path = format!("{sha_dir}/{upload_dir}/{STORAGE_FORMAT_FILE}");
    let metadata_bytes = disk.read_metadata(RUSTFS_META_MULTIPART_BUCKET, &metadata_path).await?;

    let (metadata, mod_time) = match get_file_info(
        &metadata_bytes,
        RUSTFS_META_MULTIPART_BUCKET,
        &metadata_path,
        "",
        FileInfoOpts {
            data: false,
            include_free_versions: false,
            include_part_checksums: false,
        },
    ) {
        Ok(file_info) => (Some(file_info.metadata), file_info.mod_time),
        Err(err) => {
            debug!(
                event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                path = %metadata_path,
                error = ?err,
                reason = "multipart_metadata_parse_failed",
                "Skipped multipart metadata parse during stale cleanup"
            );
            (None, None)
        }
    };

    let initiated = initiated_from_upload_dir(upload_dir, mod_time);

    Ok(StaleMultipartUploadCandidate {
        path: format!("{sha_dir}/{upload_dir}"),
        initiated,
        metadata,
    })
}

fn merge_stale_multipart_candidate(
    candidates: &mut HashMap<String, StaleMultipartUploadCandidate>,
    candidate: StaleMultipartUploadCandidate,
) {
    match candidates.get(&candidate.path) {
        Some(existing) if existing.metadata.is_some() => {}
        Some(existing) if existing.metadata.is_none() && candidate.metadata.is_none() => {}
        _ => {
            candidates.insert(candidate.path.clone(), candidate);
        }
    }
}

fn is_multipart_sha_dir(path: &str) -> bool {
    path.len() == 64 && path.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn multipart_sha_path(root: &str, entry: &str) -> Option<String> {
    let sha_dir = entry.trim_end_matches('/');
    is_multipart_sha_dir(sha_dir).then(|| {
        if root.is_empty() {
            sha_dir.to_string()
        } else {
            format!("{root}/{sha_dir}")
        }
    })
}

async fn cleanup_empty_multipart_sha_dirs_on_local_disks(set: &Arc<SetDisks>) {
    for disk in set.get_local_disks().await.into_iter().flatten() {
        if !disk.is_online().await {
            continue;
        }

        for root in ["", crate::set_disk::DATA_MOVEMENT_MULTIPART_PREFIX] {
            let sha_dirs = match disk
                .list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, root, -1)
                .await
            {
                Ok(entries) => entries,
                Err(err) => {
                    if err != DiskError::FileNotFound && err != DiskError::VolumeNotFound {
                        debug!(
                            event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            error = ?err,
                            reason = "multipart_root_list_failed",
                            "Skipped empty multipart sha cleanup"
                        );
                    }
                    continue;
                }
            };

            for sha_dir in sha_dirs.into_iter().filter_map(|entry| multipart_sha_path(root, &entry)) {
                let upload_dirs = match disk
                    .list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, &sha_dir, -1)
                    .await
                {
                    Ok(entries) => entries,
                    Err(err) => {
                        if err != DiskError::FileNotFound && err != DiskError::VolumeNotFound {
                            debug!(
                                event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                sha_dir = %sha_dir,
                                error = ?err,
                                reason = "multipart_sha_dir_list_failed",
                                "Skipped empty multipart sha cleanup"
                            );
                        }
                        continue;
                    }
                };

                if !upload_dirs.is_empty() {
                    continue;
                }

                if let Err(err) = disk
                    .delete(RUSTFS_META_MULTIPART_BUCKET, &sha_dir, DeleteOptions::default())
                    .await
                    && err != DiskError::FileNotFound
                    && err != DiskError::VolumeNotFound
                {
                    debug!(
                        event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                        sha_dir = %sha_dir,
                        error = ?err,
                        reason = "multipart_sha_dir_remove_failed",
                        "Failed to remove empty multipart sha dir"
                    );
                }
            }
        }
    }
}

async fn cleanup_stale_multipart_uploads_in_set(set: &Arc<SetDisks>, now: OffsetDateTime, default_expiry: StdDuration) -> usize {
    let mut deleted = 0usize;
    let mut candidates = HashMap::new();

    // Discovery is intentionally local-owner based: each server lists the disks
    // it owns locally. Once a stale upload path is found, delete_all fans out
    // idempotently across the set to remove matching shards on every disk.
    for disk in set.get_local_disks().await.into_iter().flatten() {
        if !disk.is_online().await {
            continue;
        }

        for root in ["", crate::set_disk::DATA_MOVEMENT_MULTIPART_PREFIX] {
            let sha_dirs = match disk
                .list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, root, -1)
                .await
            {
                Ok(entries) => entries,
                Err(err) => {
                    if err != DiskError::FileNotFound && err != DiskError::VolumeNotFound {
                        debug!(
                            event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                            error = ?err,
                            reason = "multipart_root_list_failed",
                            "Skipped stale multipart cleanup"
                        );
                    }
                    continue;
                }
            };

            for sha_dir in sha_dirs.into_iter().filter_map(|entry| multipart_sha_path(root, &entry)) {
                let upload_dirs = match disk
                    .list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, &sha_dir, -1)
                    .await
                {
                    Ok(entries) => entries,
                    Err(err) => {
                        if err != DiskError::FileNotFound && err != DiskError::VolumeNotFound {
                            debug!(
                                event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                sha_dir = %sha_dir,
                                error = ?err,
                                reason = "multipart_sha_dir_list_failed",
                                "Skipped stale multipart cleanup"
                            );
                        }
                        continue;
                    }
                };

                for upload_dir in upload_dirs {
                    let upload_dir = upload_dir.trim_end_matches('/').to_string();
                    let candidate_path = format!("{sha_dir}/{upload_dir}");
                    if candidates
                        .get(&candidate_path)
                        .is_some_and(|existing: &StaleMultipartUploadCandidate| existing.metadata.is_some())
                    {
                        continue;
                    }

                    let candidate = match read_stale_multipart_candidate(disk.as_ref(), &sha_dir, &upload_dir).await {
                        Ok(candidate) => candidate,
                        Err(err) => {
                            if err != DiskError::FileNotFound {
                                debug!(
                                    event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                                    path = %candidate_path,
                                    error = ?err,
                                    reason = "multipart_metadata_read_failed",
                                    "Multipart metadata unavailable during stale cleanup"
                                );
                            }
                            let initiated = initiated_from_upload_dir(&upload_dir, None);
                            StaleMultipartUploadCandidate {
                                path: candidate_path,
                                initiated,
                                metadata: None,
                            }
                        }
                    };
                    merge_stale_multipart_candidate(&mut candidates, candidate);
                }
            }
        }
    }

    for candidate in candidates.into_values() {
        let upload_dir = candidate.path.rsplit('/').next().unwrap_or_default().to_string();
        let mut due = stale_upload_default_due(candidate.initiated, default_expiry);
        if let Some(metadata) = candidate.metadata.as_ref()
            && let Some(lifecycle_due) = stale_upload_lifecycle_due(set, metadata, candidate.initiated, &upload_dir, false).await
            && lifecycle_due < due
        {
            due = lifecycle_due;
        }

        if now < due {
            continue;
        }

        let cleanup_guard = match set.lock_stale_multipart_cleanup(&candidate.path).await {
            Ok(guard) => guard,
            Err(err) => {
                debug!(
                    event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    path = %candidate.path,
                    error = ?err,
                    reason = "multipart_cleanup_lock_or_recheck_failed",
                    "Skipped stale multipart cleanup"
                );
                continue;
            }
        };
        let current_metadata = cleanup_guard.file_info().metadata.clone();
        let current_initiated = initiated_from_upload_dir(&upload_dir, cleanup_guard.file_info().mod_time);
        let mut current_due = stale_upload_default_due(current_initiated, default_expiry);
        if let Some(lifecycle_due) =
            stale_upload_lifecycle_due(set, &current_metadata, current_initiated, &upload_dir, true).await
            && lifecycle_due < current_due
        {
            current_due = lifecycle_due;
        }
        if now < current_due || cleanup_guard.is_lock_lost() {
            continue;
        }

        match cleanup_guard.delete(set).await {
            Ok(()) => {
                deleted += 1;
                let upload_id = encode_stale_upload_id(&upload_dir);
                debug!(
                    bucket = current_metadata.get(RUSTFS_MULTIPART_BUCKET_KEY).cloned().unwrap_or_default(),
                    object = current_metadata.get(RUSTFS_MULTIPART_OBJECT_KEY).cloned().unwrap_or_default(),
                    upload_id = %upload_id,
                    due = ?current_due,
                    event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    state = "removed",
                    "Removed stale multipart upload"
                );
            }
            Err(err) => debug!(
                event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                path = %candidate.path,
                error = ?err,
                reason = "multipart_remove_failed",
                "Failed to remove stale multipart upload"
            ),
        }
    }

    cleanup_empty_multipart_sha_dirs_on_local_disks(set).await;

    deleted
}

async fn cleanup_stale_multipart_uploads_once_at(api: Arc<ECStore>, now: OffsetDateTime, default_expiry: StdDuration) -> usize {
    let mut deleted = 0usize;
    for pool in &api.pools {
        for set in &pool.disk_set {
            deleted += cleanup_stale_multipart_uploads_in_set(set, now, default_expiry).await;
        }
    }
    deleted
}

pub async fn run_stale_multipart_upload_cleanup_once(api: Arc<ECStore>) -> usize {
    cleanup_stale_multipart_uploads_once_at(api, OffsetDateTime::now_utc(), stale_uploads_expiry()).await
}

pub fn schedule_stale_multipart_upload_cleanup_once(api: Arc<ECStore>) {
    tokio::spawn(async move {
        let deleted = run_stale_multipart_upload_cleanup_once(api).await;
        if deleted > 0 {
            debug!(
                event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                deleted,
                trigger = "on_demand",
                "Completed stale multipart cleanup pass"
            );
        }
    });
}

pub fn init_background_stale_multipart_upload_cleanup(api: Arc<ECStore>) {
    let cleanup_interval = stale_uploads_cleanup_interval();
    let default_expiry = stale_uploads_expiry();
    let api = Arc::downgrade(&api);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);

        loop {
            interval.tick().await;

            let Some(api) = Weak::upgrade(&api) else {
                return;
            };

            let deleted = cleanup_stale_multipart_uploads_once_at(api, OffsetDateTime::now_utc(), default_expiry).await;
            if deleted > 0 {
                debug!(
                    event = EVENT_LIFECYCLE_STALE_MULTIPART_CLEANUP,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    deleted,
                    "Completed stale multipart cleanup pass"
                );
            }
        }
    });
}

pub async fn validate_transition_tier(lc: &BucketLifecycleConfiguration) -> Result<(), std::io::Error> {
    for rule in &lc.rules {
        if let Some(transitions) = &rule.transitions {
            for transition in transitions {
                if let Some(storage_class) = &transition.storage_class
                    && storage_class.as_str() != ""
                {
                    let valid = runtime_sources::tier_config_mgr_handle()
                        .read()
                        .await
                        .is_tier_valid(storage_class.as_str());
                    if !valid {
                        return Err(std::io::Error::other(ERR_INVALID_STORAGECLASS));
                    }
                }
            }
        }
        if let Some(noncurrent_version_transitions) = &rule.noncurrent_version_transitions {
            for noncurrent_version_transition in noncurrent_version_transitions {
                if let Some(storage_class) = &noncurrent_version_transition.storage_class
                    && storage_class.as_str() != ""
                {
                    let valid = runtime_sources::tier_config_mgr_handle()
                        .read()
                        .await
                        .is_tier_valid(storage_class.as_str());
                    if !valid {
                        return Err(std::io::Error::other(ERR_INVALID_STORAGECLASS));
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
fn mark_delete_opts_skip_decommissioned_on_remote_success(opts: &mut ObjectOptions, remote_delete_succeeded: bool) {
    if remote_delete_succeeded {
        opts.skip_decommissioned = true;
    }
}

fn transitioned_cleanup_tuple(oi: &ObjectInfo) -> Result<(&str, &str, &str), std::io::Error> {
    let transitioned = &oi.transitioned_object;
    if transitioned.status != lifecycle::TRANSITION_COMPLETE {
        return Err(std::io::Error::other("transitioned object cleanup tuple is not complete"));
    }
    if transitioned.name.is_empty() || transitioned.tier.is_empty() {
        return Err(std::io::Error::other("transitioned object cleanup tuple is incomplete"));
    }
    Ok((&transitioned.name, &transitioned.version_id, &transitioned.tier))
}

pub async fn enqueue_transition_immediate(oi: &ObjectInfo, src: LcEventSrc) {
    if let Some(lc) = runtime_sources::bucket_lifecycle_config(&oi.bucket).await {
        enqueue_transition_with_lifecycle(oi, &lc, &src).await;
    }
}

pub async fn enqueue_immediate_expiry(oi: &ObjectInfo, src: LcEventSrc) {
    let Some(api) = runtime_sources::object_store_handle() else {
        return;
    };
    let configs = match metadata_boundary::get_expiry_configs(&api, &oi.bucket).await {
        Ok(configs) => configs,
        Err(err) => {
            observe_lifecycle_observability_event(EVENT_LIFECYCLE_EVALUATION_FAILED, "failed", Some("metadata_unavailable"));
            warn!(
                event = EVENT_LIFECYCLE_EVALUATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket = %oi.bucket,
                object = %oi.name,
                error = %err,
                reason = "metadata_unavailable",
                "Failed to load authoritative lifecycle metadata"
            );
            return;
        }
    };
    if configs.table_bucket_enabled {
        return;
    }
    let Some(lifecycle) = configs.lifecycle else {
        return;
    };

    let mut marker = None;
    let mut version_marker = None;
    let mut object_infos = Vec::new();

    loop {
        let page = match api
            .clone()
            .list_object_versions_for_lifecycle(&oi.bucket, &oi.name, marker.clone(), version_marker.clone(), None, 1000)
            .await
        {
            Ok(page) => page,
            Err(err) => {
                warn!(
                    event = EVENT_LIFECYCLE_EVALUATION_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                    bucket = %oi.bucket,
                    object = %oi.name,
                    error = %err,
                    reason = "list_versions_failed",
                    "Failed to load lifecycle version group"
                );
                return;
            }
        };

        object_infos.extend(page.objects.into_iter().filter(|object| object.name == oi.name));

        if !page.is_truncated {
            break;
        }

        marker = page.next_marker;
        version_marker = page.next_version_idmarker;
    }

    if object_infos.is_empty() {
        object_infos.push(oi.clone());
    }

    let object_opts = object_infos
        .iter()
        .map(lifecycle::object_opts_from_object_info)
        .collect::<Vec<ObjectOpts>>();
    let lock_config = configs.object_lock;
    let events = match Evaluator::new(lifecycle)
        .with_lock_retention(lock_config.clone())
        .eval(&object_opts)
        .await
    {
        Ok(events) => events,
        Err(err) => {
            warn!(
                event = EVENT_LIFECYCLE_EVALUATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket = %oi.bucket,
                object = %oi.name,
                expected_version_count = oi.num_versions,
                observed_version_count = object_infos.len(),
                error = %err,
                reason = "version_group_evaluation_failed",
                "Failed to evaluate lifecycle version group"
            );
            return;
        }
    };

    let mut to_delete_objs = Vec::new();
    let mut noncurrent_event = None;

    for (object, event) in object_infos.iter().zip(events.iter()) {
        if event.due != Some(OffsetDateTime::UNIX_EPOCH) {
            continue;
        }
        if matches!(
            event.action,
            IlmAction::DeleteAction
                | IlmAction::DeleteVersionAction
                | IlmAction::DeleteAllVersionsAction
                | IlmAction::DelMarkerDeleteAllVersionsAction
        ) && !matches!(
            object_lock_boundary::check_object_lock_for_deletion_with_config(lock_config.as_deref(), object, false),
            Ok(None)
        ) {
            record_scanner_lifecycle_expiry_blocked(&src, 1);
            continue;
        }

        match event.action {
            IlmAction::DeleteAction
            | IlmAction::DeleteRestoredAction
            | IlmAction::DeleteRestoredVersionAction
            | IlmAction::DeleteAllVersionsAction
            | IlmAction::DelMarkerDeleteAllVersionsAction => {
                enqueue_expiry_rule_with_incarnation(event, &src, object, configs.bucket_incarnation_id).await;
            }
            IlmAction::DeleteVersionAction => {
                to_delete_objs.push(ObjectToDelete {
                    object_name: object.name.clone(),
                    version_id: object.version_id,
                    ..Default::default()
                });
                if noncurrent_event.is_none() {
                    noncurrent_event = Some(event.clone());
                }
            }
            _ => {}
        }
    }

    if !to_delete_objs.is_empty()
        && let Some(event) = noncurrent_event
    {
        let expiry_state = runtime_sources::expiry_state_handle();
        expiry_state.write().await.enqueue_by_newer_noncurrent(
            &oi.bucket,
            to_delete_objs,
            event,
            &src,
            configs.bucket_incarnation_id,
        );
    }
}

pub type ManualTransitionCancelCheck = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync + 'static>;
pub type ManualTransitionProgressSink =
    Arc<dyn Fn(ManualTransitionRunReport) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send + Sync + 'static>;

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ManualTransitionRunOptions {
    pub prefix: String,
    pub marker: Option<String>,
    pub version_marker: Option<String>,
    pub continuation_token: Option<String>,
    pub tier: Option<String>,
    pub dry_run: bool,
    pub max_objects: Option<u64>,
    pub max_duration: Option<std::time::Duration>,
    #[serde(skip)]
    pub job_id: Option<Uuid>,
    #[serde(skip)]
    pub cancel_token: Option<CancellationToken>,
    #[serde(skip)]
    pub cancel_check: Option<ManualTransitionCancelCheck>,
    #[serde(skip)]
    pub progress_sink: Option<ManualTransitionProgressSink>,
}

impl std::fmt::Debug for ManualTransitionRunOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManualTransitionRunOptions")
            .field("prefix", &self.prefix)
            .field("marker", &self.marker)
            .field("version_marker", &self.version_marker)
            .field("continuation_token", &self.continuation_token)
            .field("tier", &self.tier)
            .field("dry_run", &self.dry_run)
            .field("max_objects", &self.max_objects)
            .field("max_duration", &self.max_duration)
            .field("job_id", &self.job_id)
            .field("cancel_token", &self.cancel_token.is_some())
            .field("cancel_check", &self.cancel_check.is_some())
            .field("progress_sink", &self.progress_sink.is_some())
            .finish()
    }
}

impl PartialEq for ManualTransitionRunOptions {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix
            && self.marker == other.marker
            && self.version_marker == other.version_marker
            && self.continuation_token == other.continuation_token
            && self.tier == other.tier
            && self.dry_run == other.dry_run
            && self.max_objects == other.max_objects
            && self.max_duration == other.max_duration
    }
}

impl Eq for ManualTransitionRunOptions {}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManualTransitionRunReport {
    pub bucket: String,
    pub prefix: String,
    pub tier: Option<String>,
    pub dry_run: bool,
    pub lifecycle_config_found: bool,
    pub scanned: u64,
    pub eligible: u64,
    pub enqueued: u64,
    pub dry_run_eligible: u64,
    pub skipped_not_transition: u64,
    pub skipped_tier: u64,
    pub skipped_delete_marker: u64,
    pub skipped_directory: u64,
    pub skipped_replication: u64,
    pub skipped_already_transitioned: u64,
    pub skipped_already_in_flight: u64,
    pub skipped_queue_full: u64,
    pub skipped_queue_closed: u64,
    pub skipped_queue_timeout: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub transition_completed: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub transition_failed: u64,
    pub tier_failure: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tier_failure_by_reason: BTreeMap<ManualTransitionWorkerFailureReason, u64>,
    pub truncated_by_limit: bool,
    pub truncated_by_duration: bool,
    pub cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(skip)]
    pub next_marker: Option<String>,
    #[serde(skip)]
    pub next_version_idmarker: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ManualTransitionQueueSnapshot {
    pub queue_capacity: u64,
    pub queued: u64,
    pub active: u64,
    pub workers: u64,
    pub queue_full: u64,
    pub queue_send_timeout: u64,
    pub compensation_pending: u64,
    pub compensation_running: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualTransitionRunExecution {
    pub report: ManualTransitionRunReport,
    pub cancelled: bool,
}

impl ManualTransitionRunReport {
    fn new(bucket: &str, options: &ManualTransitionRunOptions) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: options.prefix.clone(),
            tier: options.tier.clone(),
            dry_run: options.dry_run,
            ..Default::default()
        }
    }

    pub fn has_partial_enqueue(&self) -> bool {
        self.skipped_already_in_flight > 0
            || self.skipped_queue_full > 0
            || self.skipped_queue_closed > 0
            || self.skipped_queue_timeout > 0
    }

    pub fn was_truncated(&self) -> bool {
        self.truncated_by_limit || self.truncated_by_duration || self.cancelled
    }

    fn record_enqueue_outcome(&mut self, outcome: TransitionEnqueueOutcome) {
        match outcome {
            TransitionEnqueueOutcome::Queued => self.enqueued = self.enqueued.saturating_add(1),
            TransitionEnqueueOutcome::AlreadyInFlight => {
                self.skipped_already_in_flight = self.skipped_already_in_flight.saturating_add(1);
            }
            TransitionEnqueueOutcome::QueueFull => {
                self.skipped_queue_full = self.skipped_queue_full.saturating_add(1);
            }
            TransitionEnqueueOutcome::QueueClosed => {
                self.skipped_queue_closed = self.skipped_queue_closed.saturating_add(1);
            }
            TransitionEnqueueOutcome::QueueSendTimedOut => {
                self.skipped_queue_timeout = self.skipped_queue_timeout.saturating_add(1);
            }
            TransitionEnqueueOutcome::TaskJournalFailed => {
                self.skipped_queue_closed = self.skipped_queue_closed.saturating_add(1);
            }
        }
    }

    pub fn merge_scan_report_preserving_worker(&mut self, scan_report: &ManualTransitionRunReport) {
        let previous = self.clone();
        let resumed_after_checkpoint = previous.continuation_token.is_some() && scan_report.scanned < previous.scanned;
        let mut tier_failure_by_reason = self.tier_failure_by_reason.clone();
        for (reason, count) in &scan_report.tier_failure_by_reason {
            let current = tier_failure_by_reason.get(reason).copied().unwrap_or_default();
            let merged = if resumed_after_checkpoint {
                current.saturating_add(*count)
            } else {
                current.max(*count)
            };
            tier_failure_by_reason.insert(*reason, merged);
        }
        let transition_completed = self.transition_completed;
        let transition_failed = self.transition_failed;
        *self = scan_report.clone();
        if resumed_after_checkpoint {
            self.scanned = previous.scanned.saturating_add(scan_report.scanned);
            self.eligible = previous.eligible.saturating_add(scan_report.eligible);
            self.enqueued = previous.enqueued.saturating_add(scan_report.enqueued);
            self.dry_run_eligible = previous.dry_run_eligible.saturating_add(scan_report.dry_run_eligible);
            self.skipped_not_transition = previous
                .skipped_not_transition
                .saturating_add(scan_report.skipped_not_transition);
            self.skipped_tier = previous.skipped_tier.saturating_add(scan_report.skipped_tier);
            self.skipped_delete_marker = previous
                .skipped_delete_marker
                .saturating_add(scan_report.skipped_delete_marker);
            self.skipped_directory = previous.skipped_directory.saturating_add(scan_report.skipped_directory);
            self.skipped_replication = previous.skipped_replication.saturating_add(scan_report.skipped_replication);
            self.skipped_already_transitioned = previous
                .skipped_already_transitioned
                .saturating_add(scan_report.skipped_already_transitioned);
            self.skipped_already_in_flight = previous
                .skipped_already_in_flight
                .saturating_add(scan_report.skipped_already_in_flight);
            self.skipped_queue_full = previous.skipped_queue_full.saturating_add(scan_report.skipped_queue_full);
            self.skipped_queue_closed = previous.skipped_queue_closed.saturating_add(scan_report.skipped_queue_closed);
            self.skipped_queue_timeout = previous
                .skipped_queue_timeout
                .saturating_add(scan_report.skipped_queue_timeout);
            self.tier_failure = previous.tier_failure.saturating_add(scan_report.tier_failure);
        }
        self.lifecycle_config_found = previous.lifecycle_config_found || scan_report.lifecycle_config_found;
        self.truncated_by_limit = previous.truncated_by_limit || scan_report.truncated_by_limit;
        self.truncated_by_duration = previous.truncated_by_duration || scan_report.truncated_by_duration;
        self.cancelled = previous.cancelled || scan_report.cancelled;
        self.transition_completed = transition_completed;
        self.transition_failed = transition_failed;
        self.tier_failure = self.tier_failure.saturating_add(transition_failed);
        self.tier_failure_by_reason = tier_failure_by_reason;
    }

    pub fn worker_transition_pending(&self) -> bool {
        self.transition_completed.saturating_add(self.transition_failed) < self.enqueued
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ManualTransitionContinuationToken {
    marker: Option<String>,
    version_marker: Option<String>,
}

pub(super) fn encode_manual_transition_continuation_token(
    marker: Option<String>,
    version_marker: Option<String>,
) -> Option<String> {
    if marker.is_none() && version_marker.is_none() {
        return None;
    }
    let token = ManualTransitionContinuationToken { marker, version_marker };
    serde_json::to_vec(&token)
        .ok()
        .map(|encoded| base64_simd::URL_SAFE_NO_PAD.encode_to_string(&encoded))
}

pub fn decode_manual_transition_continuation_token(token: &str) -> Result<(Option<String>, Option<String>), Error> {
    if token.trim().is_empty() {
        return Err(Error::other("manual transition continuation token is empty"));
    }
    let decoded = base64_simd::URL_SAFE_NO_PAD
        .decode_to_vec(token.as_bytes())
        .map_err(|err| Error::other(format!("decode manual transition continuation token failed: {err}")))?;
    let token: ManualTransitionContinuationToken = serde_json::from_slice(&decoded)
        .map_err(|err| Error::other(format!("parse manual transition continuation token failed: {err}")))?;
    Ok((
        token.marker.filter(|marker| !marker.is_empty()),
        token.version_marker.filter(|marker| !marker.is_empty()),
    ))
}

async fn persist_manual_transition_progress(
    options: &ManualTransitionRunOptions,
    report: &ManualTransitionRunReport,
) -> Result<(), Error> {
    if let Some(progress_sink) = &options.progress_sink {
        progress_sink(report.clone()).await?;
    }
    Ok(())
}

async fn persist_manual_transition_page_checkpoint(
    options: &ManualTransitionRunOptions,
    report: &ManualTransitionRunReport,
    marker: Option<String>,
    version_marker: Option<String>,
) -> Result<(), Error> {
    let mut checkpoint = report.clone();
    checkpoint.next_marker.clone_from(&marker);
    checkpoint.next_version_idmarker.clone_from(&version_marker);
    checkpoint.continuation_token = encode_manual_transition_continuation_token(marker, version_marker);
    persist_manual_transition_progress(options, &checkpoint).await
}

pub async fn enqueue_transition_for_existing_objects(api: Arc<ECStore>, bucket: &str) -> Result<(), Error> {
    let _ = enqueue_transition_for_existing_objects_scoped(api, bucket, ManualTransitionRunOptions::default()).await?;
    Ok(())
}

pub fn manual_transition_queue_snapshot() -> ManualTransitionQueueSnapshot {
    runtime_sources::transition_state_handle().manual_transition_queue_snapshot()
}

pub async fn enqueue_transition_for_existing_objects_scoped_with_cancel(
    api: Arc<ECStore>,
    bucket: &str,
    mut options: ManualTransitionRunOptions,
    cancel_token: Option<CancellationToken>,
) -> Result<ManualTransitionRunExecution, Error> {
    if cancel_token.is_some() {
        options.cancel_token = cancel_token;
    }
    let report = enqueue_transition_for_existing_objects_scoped(api, bucket, options).await?;
    Ok(ManualTransitionRunExecution {
        cancelled: report.cancelled,
        report,
    })
}

pub async fn enqueue_transition_for_existing_objects_scoped(
    api: Arc<ECStore>,
    bucket: &str,
    options: ManualTransitionRunOptions,
) -> Result<ManualTransitionRunReport, Error> {
    const LIST_PAGE_SIZE: i32 = 1000;

    let mut report = ManualTransitionRunReport::new(bucket, &options);
    let (mut marker, mut version_marker) = if let Some(token) = options.continuation_token.as_deref() {
        decode_manual_transition_continuation_token(token)?
    } else {
        (options.marker.clone(), options.version_marker.clone())
    };
    let Some(lc) = runtime_sources::bucket_lifecycle_config(bucket).await else {
        return Ok(report);
    };
    report.lifecycle_config_found = true;
    let mut previous_marker = marker.clone();
    let mut previous_version_marker = version_marker.clone();
    let src = LcEventSrc::Scanner;
    let deadline = options.max_duration.map(|duration| tokio::time::Instant::now() + duration);

    loop {
        let page = api
            .clone()
            .list_object_versions(bucket, &options.prefix, marker.clone(), version_marker.clone(), None, LIST_PAGE_SIZE)
            .await?;

        for (index, object) in page.objects.iter().enumerate() {
            if manual_transition_cancel_requested(&options).await {
                report.cancelled = true;
                report.next_marker.clone_from(&previous_marker);
                report.next_version_idmarker.clone_from(&previous_version_marker);
                report.continuation_token =
                    encode_manual_transition_continuation_token(report.next_marker.clone(), report.next_version_idmarker.clone());
                persist_manual_transition_progress(&options, &report).await?;
                return Ok(report);
            }
            if manual_transition_duration_elapsed(deadline) {
                report.truncated_by_duration = true;
                report.next_marker.clone_from(&previous_marker);
                report.next_version_idmarker.clone_from(&previous_version_marker);
                report.continuation_token =
                    encode_manual_transition_continuation_token(report.next_marker.clone(), report.next_version_idmarker.clone());
                persist_manual_transition_progress(&options, &report).await?;
                return Ok(report);
            }
            report.scanned = report.scanned.saturating_add(1);
            enqueue_transition_with_lifecycle_report(Some(api.clone()), object, &lc, &src, &options, &mut report).await;
            if report.has_partial_enqueue() {
                report.next_marker.clone_from(&previous_marker);
                report.next_version_idmarker.clone_from(&previous_version_marker);
                report.continuation_token =
                    encode_manual_transition_continuation_token(report.next_marker.clone(), report.next_version_idmarker.clone());
                persist_manual_transition_progress(&options, &report).await?;
                return Ok(report);
            }
            if options.max_objects.is_some_and(|max_objects| report.scanned >= max_objects) {
                if manual_transition_has_more_after_limit(index, page.objects.len(), page.is_truncated) {
                    report.truncated_by_limit = true;
                    report.next_marker = Some(object.name.clone());
                    report.next_version_idmarker = Some(manual_transition_version_marker(object));
                    report.continuation_token = encode_manual_transition_continuation_token(
                        report.next_marker.clone(),
                        report.next_version_idmarker.clone(),
                    );
                }
                persist_manual_transition_progress(&options, &report).await?;
                return Ok(report);
            }
            previous_marker = Some(object.name.clone());
            previous_version_marker = Some(manual_transition_version_marker(object));
        }

        if !page.is_truncated {
            return Ok(report);
        }
        if manual_transition_duration_elapsed(deadline) {
            report.truncated_by_duration = true;
            report.next_marker.clone_from(&previous_marker);
            report.next_version_idmarker.clone_from(&previous_version_marker);
            report.continuation_token =
                encode_manual_transition_continuation_token(report.next_marker.clone(), report.next_version_idmarker.clone());
            persist_manual_transition_progress(&options, &report).await?;
            return Ok(report);
        }

        marker = page.next_marker;
        version_marker = page.next_version_idmarker;
        previous_marker = marker.clone();
        previous_version_marker = version_marker.clone();
        persist_manual_transition_page_checkpoint(&options, &report, marker.clone(), version_marker.clone()).await?;
    }
}

fn lifecycle_rule_has_date_expiration(lc: &BucketLifecycleConfiguration, rule_id: &str) -> bool {
    lc.rules.iter().any(|rule| {
        rule.status == ExpirationStatus::from_static(ExpirationStatus::ENABLED)
            && rule.id.as_deref() == Some(rule_id)
            && rule.expiration.as_ref().is_some_and(|expiration| expiration.date.is_some())
    })
}

fn should_defer_date_expiry_for_recent_config_update(lc: &BucketLifecycleConfiguration, now: OffsetDateTime) -> bool {
    lc.expiry_updated_at.as_ref().is_some_and(|updated_at| {
        let updated_at = OffsetDateTime::from(updated_at.clone());
        now.unix_timestamp().saturating_sub(updated_at.unix_timestamp()) < DATE_EXPIRY_EXISTING_OBJECTS_GRACE_SECS
    })
}

async fn apply_existing_object_expiry(
    api: Arc<ECStore>,
    object: &ObjectInfo,
    event: &lifecycle::Event,
    src: &LcEventSrc,
    bucket_incarnation_id: Uuid,
) {
    if object.is_remote() {
        apply_expiry_on_transitioned_object(api, object, event, src, bucket_incarnation_id).await;
    } else {
        apply_expiry_on_non_transitioned_objects(api, object, event, src, bucket_incarnation_id).await;
    }
}

struct ExistingObjectExpiryContext<'a> {
    api: Arc<ECStore>,
    bucket: &'a str,
    lc: Arc<BucketLifecycleConfiguration>,
    lock_config: Option<Arc<ObjectLockConfiguration>>,
    bucket_incarnation_id: Uuid,
    src: &'a LcEventSrc,
    defer_date_expiry_once: bool,
}

async fn enqueue_expiry_for_existing_object_group(
    context: &ExistingObjectExpiryContext<'_>,
    object_infos: &[ObjectInfo],
    date_expiry_deferred_once: &mut bool,
) {
    if object_infos.is_empty() {
        return;
    }

    let object_opts = object_infos
        .iter()
        .map(lifecycle::object_opts_from_object_info)
        .collect::<Vec<ObjectOpts>>();
    let events = match Evaluator::new(context.lc.clone())
        .with_lock_retention(context.lock_config.clone())
        .eval(&object_opts)
        .await
    {
        Ok(events) => events,
        Err(err) => {
            warn!(
                event = EVENT_LIFECYCLE_EVALUATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket = context.bucket,
                object = %object_infos[0].name,
                expected_version_count = object_infos[0].num_versions,
                observed_version_count = object_infos.len(),
                error = %err,
                reason = "version_group_evaluation_failed",
                "Failed to evaluate lifecycle version group"
            );
            return;
        }
    };

    let mut to_delete_objs = Vec::new();
    let mut noncurrent_event = None;

    for (object, event) in object_infos.iter().zip(events.iter()) {
        match event.action {
            IlmAction::DeleteAction
            | IlmAction::DeleteVersionAction
            | IlmAction::DeleteRestoredAction
            | IlmAction::DeleteRestoredVersionAction
            | IlmAction::DeleteAllVersionsAction
            | IlmAction::DelMarkerDeleteAllVersionsAction => {
                if !event.action.delete_restored() {
                    let object_lock_result = object_lock_boundary::check_object_lock_for_deletion_with_config(
                        context.lock_config.as_deref(),
                        object,
                        false,
                    );
                    if !matches!(object_lock_result, Ok(None)) {
                        record_scanner_lifecycle_expiry_blocked(context.src, 1);
                        continue;
                    }
                }
                let now = OffsetDateTime::now_utc();
                if event.due.is_some_and(|due| due.unix_timestamp() <= now.unix_timestamp()) {
                    if context.defer_date_expiry_once
                        && !*date_expiry_deferred_once
                        && lifecycle_rule_has_date_expiration(&context.lc, &event.rule_id)
                    {
                        tokio::time::sleep(StdDuration::from_secs(DATE_EXPIRY_EXISTING_OBJECTS_GRACE_SECS as u64)).await;
                        *date_expiry_deferred_once = true;
                    }

                    if event.action == IlmAction::DeleteVersionAction {
                        to_delete_objs.push(ObjectToDelete {
                            object_name: object.name.clone(),
                            version_id: object.version_id,
                            ..Default::default()
                        });
                        if noncurrent_event.is_none() {
                            noncurrent_event = Some(event.clone());
                        }
                    } else {
                        let blocked_by_replication = match lifecycle_delete_all_versions_blocked_by_replication(
                            context.api.clone(),
                            context.bucket,
                            &object.name,
                            event.action,
                        )
                        .await
                        {
                            Ok(blocked) => blocked,
                            Err(err) => {
                                warn!(
                                    bucket = context.bucket,
                                    object = %object.name,
                                    error = %err,
                                    "failed to check lifecycle delete-all replication state"
                                );
                                true
                            }
                        };
                        if blocked_by_replication {
                            record_scanner_lifecycle_expiry_blocked(context.src, 1);
                            continue;
                        }
                        apply_existing_object_expiry(
                            context.api.clone(),
                            object,
                            event,
                            context.src,
                            context.bucket_incarnation_id,
                        )
                        .await;
                    }
                } else {
                    enqueue_expiry_rule_with_incarnation(event, context.src, object, context.bucket_incarnation_id).await;
                }
            }
            _ => {}
        }
    }

    if !to_delete_objs.is_empty()
        && let Some(event) = noncurrent_event
    {
        let expiry_state = runtime_sources::expiry_state_handle();
        expiry_state.write().await.enqueue_by_newer_noncurrent(
            context.bucket,
            to_delete_objs,
            event,
            context.src,
            context.bucket_incarnation_id,
        );
    }
}

pub async fn enqueue_expiry_for_existing_objects(api: Arc<ECStore>, bucket: &str) -> Result<(), Error> {
    let configs = metadata_boundary::get_expiry_configs(&api, bucket).await?;
    if configs.table_bucket_enabled {
        return Ok(());
    }
    let Some(lc) = configs.lifecycle else {
        return Ok(());
    };
    let lock_config = configs.object_lock;
    let mut marker = None;
    let mut version_marker = None;
    let src = LcEventSrc::Scanner;
    let defer_date_expiry_once = should_defer_date_expiry_for_recent_config_update(&lc, OffsetDateTime::now_utc());
    let expiry_context = ExistingObjectExpiryContext {
        api: api.clone(),
        bucket,
        lc: lc.clone(),
        lock_config: lock_config.clone(),
        bucket_incarnation_id: configs.bucket_incarnation_id,
        src: &src,
        defer_date_expiry_once,
    };
    let mut date_expiry_deferred_once = false;
    let mut pending_group = Vec::new();
    let mut pending_object = None::<String>;

    loop {
        let page = api
            .clone()
            .list_object_versions_for_lifecycle(bucket, "", marker.clone(), version_marker.clone(), None, 1000)
            .await?;

        for object in page.objects {
            if pending_object.as_ref().is_some_and(|name| name != &object.name) {
                enqueue_expiry_for_existing_object_group(&expiry_context, &pending_group, &mut date_expiry_deferred_once).await;
                pending_group.clear();
            }
            pending_object = Some(object.name.clone());
            pending_group.push(object);
        }

        if !page.is_truncated {
            enqueue_expiry_for_existing_object_group(&expiry_context, &pending_group, &mut date_expiry_deferred_once).await;
            return Ok(());
        }

        marker = page.next_marker;
        version_marker = page.next_version_idmarker;
    }
}

fn manual_transition_has_more_after_limit(page_index: usize, page_len: usize, page_is_truncated: bool) -> bool {
    page_index.saturating_add(1) < page_len || page_is_truncated
}

fn manual_transition_duration_elapsed(deadline: Option<tokio::time::Instant>) -> bool {
    deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
}

async fn manual_transition_cancel_requested(options: &ManualTransitionRunOptions) -> bool {
    if options.cancel_token.as_ref().is_some_and(|token| token.is_cancelled()) {
        return true;
    }
    match options.cancel_check.as_ref() {
        Some(cancel_check) => cancel_check().await,
        None => false,
    }
}

fn manual_transition_version_marker(oi: &ObjectInfo) -> String {
    oi.version_id
        .map(|version| version.to_string())
        .unwrap_or_else(|| "null".to_string())
}

async fn enqueue_transition_with_lifecycle_report(
    api: Option<Arc<ECStore>>,
    oi: &ObjectInfo,
    lc: &BucketLifecycleConfiguration,
    src: &LcEventSrc,
    options: &ManualTransitionRunOptions,
    report: &mut ManualTransitionRunReport,
) -> bool {
    if oi.transitioned_object.status == TRANSITION_COMPLETE {
        if options
            .tier
            .as_deref()
            .is_some_and(|tier| !oi.transitioned_object.tier.eq_ignore_ascii_case(tier))
        {
            report.skipped_tier = report.skipped_tier.saturating_add(1);
        } else {
            report.skipped_already_transitioned = report.skipped_already_transitioned.saturating_add(1);
        }
        return false;
    }

    let event = lc.eval(&oi.to_lifecycle_opts()).await;
    match event.action {
        IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
            if oi.delete_marker || oi.is_dir {
                if oi.delete_marker {
                    report.skipped_delete_marker = report.skipped_delete_marker.saturating_add(1);
                } else {
                    report.skipped_directory = report.skipped_directory.saturating_add(1);
                }
                return false;
            }
            if lifecycle_action_blocked_by_replication(event.action, oi) {
                report.skipped_replication = report.skipped_replication.saturating_add(1);
                return false;
            }
            if options
                .tier
                .as_deref()
                .is_some_and(|tier| !event.storage_class.eq_ignore_ascii_case(tier))
            {
                report.skipped_tier = report.skipped_tier.saturating_add(1);
                return false;
            }
            if !options.dry_run
                && !runtime_sources::tier_config_mgr_handle()
                    .read()
                    .await
                    .is_tier_valid(&event.storage_class)
            {
                report.tier_failure = report.tier_failure.saturating_add(1);
                return false;
            }
            report.eligible = report.eligible.saturating_add(1);
            if options.dry_run {
                report.dry_run_eligible = report.dry_run_eligible.saturating_add(1);
                return true;
            }
            let outcome = runtime_sources::transition_state_handle()
                .queue_transition_task_outcome(api.clone(), oi, &event, src, options.job_id)
                .await;
            report.record_enqueue_outcome(outcome);
            return outcome.is_handled();
        }
        _ => report.skipped_not_transition = report.skipped_not_transition.saturating_add(1),
    }
    false
}

async fn enqueue_transition_with_lifecycle(oi: &ObjectInfo, lc: &BucketLifecycleConfiguration, src: &LcEventSrc) -> bool {
    let event = lc.eval(&oi.to_lifecycle_opts()).await;
    match event.action {
        IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
            if oi.delete_marker || oi.is_dir {
                return false;
            }
            if lifecycle_action_blocked_by_replication(event.action, oi) {
                return false;
            }
            runtime_sources::transition_state_handle()
                .queue_transition_task(oi, &event, src)
                .await
        }
        _ => false,
    }
}

/// Build the delete options for a lifecycle expiry event on a transitioned
/// object. Versioned events target the exact version; restore-expiry events
/// (`DeleteRestoredAction`/`DeleteRestoredVersionAction`) set
/// `transition.expire_restored` so the set-layer delete strips only the
/// `x-amz-restore` headers and the local restored copy while the version stays
/// transitioned (rustfs/backlog#1302).
fn transitioned_object_delete_opts(
    oi: &ObjectInfo,
    action: IlmAction,
    versioned: bool,
    version_suspended: bool,
    bucket_incarnation_id: Uuid,
) -> crate::error::Result<ObjectOptions> {
    let mut opts = ObjectOptions {
        versioned,
        version_suspended,
        expiration: ExpirationOptions { expire: true },
        expected_bucket_incarnation_id: Some(bucket_incarnation_id),
        ..Default::default()
    };
    if action.delete_versioned() {
        opts.version_id = oi.version_id.map(|id| id.to_string());
    }
    if action.delete_restored() {
        let etag = oi
            .etag
            .as_deref()
            .filter(|etag| !etag.is_empty())
            .ok_or_else(|| Error::other("restored-copy expiry requires an object etag"))?;
        let data_dir = oi
            .data_dir
            .ok_or_else(|| Error::other("restored-copy expiry requires a local data directory"))?;
        let restore_expiry = oi
            .restore_expires
            .ok_or_else(|| Error::other("restored-copy expiry requires a restore expiry"))?;
        opts.transition.expire_restored = true;
        opts.transition.status.clone_from(&oi.transitioned_object.status);
        opts.transition.tier.clone_from(&oi.transitioned_object.tier);
        opts.transition.etag = etag.to_string();
        opts.transition.expected_data_dir = Some(data_dir);
        opts.transition.expected_remote_name.clone_from(&oi.transitioned_object.name);
        opts.transition
            .expected_remote_version_id
            .clone_from(&oi.transitioned_object.version_id);
        opts.transition.restore_expiry = restore_expiry;
        if let Some(version_id) = oi.version_id {
            opts.version_id = Some(version_id.to_string());
        }
    }
    Ok(opts)
}

pub async fn expire_transitioned_object(
    api: Arc<ECStore>,
    oi: &ObjectInfo,
    lc_event: &lifecycle::Event,
    _src: &LcEventSrc,
    bucket_incarnation_id: Uuid,
) -> Result<ObjectInfo, std::io::Error> {
    let publication_guard = lifecycle_expiry_publication_guard(&api, oi, bucket_incarnation_id)
        .await
        .ok_or_else(|| std::io::Error::other("lifecycle expiry is not allowed for this bucket"))?;
    let snapshot = lifecycle_delete_config_snapshot(&api, oi)
        .await
        .map_err(std::io::Error::other)?;
    let (versioned, version_suspended) = snapshot.versioning_config().delete_state(&oi.name);
    let mut opts = transitioned_object_delete_opts(oi, lc_event.action, versioned, version_suspended, bucket_incarnation_id)
        .map_err(std::io::Error::other)?;
    opts.add_namespace_lock_guard(&publication_guard);
    opts.delete_replication_config_snapshot = Some(Arc::new(snapshot));
    //let tags = LcAuditEvent::new(src, lcEvent).Tags();
    if lc_event.action.delete_restored() {
        return match api.delete_object(&oi.bucket, &oi.name, opts).await {
            Ok(dobj) => {
                // Drop any cached restored-copy body so it does not sit resident
                // until TTL after the copy is expired (ODC-26).
                crate::object_api::notify_object_mutation(&oi.bucket, &oi.name).await;
                //audit_log_lifecycle(*oi, ILMExpiry, tags, traceFn);
                Ok(dobj)
            }
            Err(err) => Err(std::io::Error::other(err)),
        };
    }

    let (_remote_object, _remote_version, _tier) = transitioned_cleanup_tuple(oi)?;

    // Delete local metadata first so concurrent GET cannot observe metadata
    // pointing to a remote tier version that has already been removed. If this
    // only creates a delete marker, remote cleanup must be driven by persisted
    // free-version recovery rather than the visible delete result.
    let dobj = match api.delete_object(&oi.bucket, &oi.name, opts).await {
        Ok(obj) => obj,
        Err(e) => {
            error!(
                event = EVENT_LIFECYCLE_DELETE_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket = %oi.bucket,
                object = %oi.name,
                operation = "delete_transitioned_object",
                error = ?e,
                "Lifecycle delete failed"
            );
            return Err(std::io::Error::other(e));
        }
    };

    schedule_lifecycle_replication_delete_if_needed(oi, &dobj).await;

    // The transitioned version is gone; evict any cached body for this object
    // so it does not linger until TTL (ODC-26).
    crate::object_api::notify_object_mutation(&oi.bucket, &oi.name).await;

    //audit_log_lifecycle(oi, ILMExpiry, tags);

    emit_transitioned_expiration_event(oi, &dobj);

    Ok(dobj)
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub fn gen_transition_objname(bucket: &str) -> Result<String, Error> {
    let us = Uuid::new_v4().to_string();
    let mut hasher = Sha256::new();
    hasher.update(format!("{}/{}", runtime_sources::deployment_id().unwrap_or_default(), bucket).as_bytes());
    let hash = rustfs_utils::crypto::hex(hasher.finalize().as_slice());
    let obj = format!("{}/{}/{}/{}", &hash[0..16], &us[0..2], &us[2..4], us);
    Ok(obj)
}

pub async fn transition_object(api: Arc<ECStore>, oi: &ObjectInfo, lae: LcAuditEvent) -> Result<(), Error> {
    let time_ilm = Metrics::time_ilm(lae.event.action);

    let etag = if let Some(etag) = &oi.etag { etag } else { "" };
    let etag = etag.to_string();

    let opts = ObjectOptions {
        transition: TransitionOptions {
            status: lifecycle::TRANSITION_PENDING.to_string(),
            tier: lae.event.storage_class,
            etag,
            ..Default::default()
        },
        //lifecycle_audit_event: lae,
        version_id: oi.version_id.map(|v| v.to_string()),
        versioned: BucketVersioningSys::prefix_enabled(&oi.bucket, &oi.name).await,
        version_suspended: BucketVersioningSys::prefix_suspended(&oi.bucket, &oi.name).await,
        mod_time: oi.mod_time,
        ..Default::default()
    };
    let result = api.transition_object(&oi.bucket, &oi.name, &opts).await;
    time_ilm(1)();
    result
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub fn audit_tier_actions(_tier: &str, bytes: i64) -> TimeFn {
    let tier = _tier.to_string();
    Arc::new(move || {
        let tier = tier.clone();
        Box::pin(async move {
            debug!(
                event = EVENT_LIFECYCLE_TIER_AUDIT,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                tier = %tier,
                bytes = bytes,
                state = "transition_completed",
                "Lifecycle tier transition recorded"
            );
        })
    })
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub async fn get_transitioned_object_reader(
    bucket: &str,
    object: &str,
    rs: &Option<HTTPRangeSpec>,
    h: &HeaderMap,
    oi: &ObjectInfo,
    opts: &ObjectOptions,
    resolver: Option<&dyn ObjectEncryptionResolver>,
) -> Result<GetObjectReader, std::io::Error> {
    let tier_config_mgr = runtime_sources::tier_config_mgr_handle();
    get_transitioned_object_reader_with_tier_manager(bucket, object, rs, h, oi, opts, &tier_config_mgr, resolver).await
}

fn validate_transition_remote_version(oi: &ObjectInfo) -> Result<bool, std::io::Error> {
    let version = oi.transitioned_object.version_id.as_str();
    match oi.transition_version_state {
        rustfs_filemeta::TransitionVersionState::Unknown => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "remote tier object version state is unknown",
        )),
        rustfs_filemeta::TransitionVersionState::KnownDisabled if version.is_empty() => Ok(false),
        rustfs_filemeta::TransitionVersionState::SuspendedNull if version == "null" => Ok(true),
        rustfs_filemeta::TransitionVersionState::Exact if !version.is_empty() && version != "null" => Ok(true),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "remote tier object version state conflicts with its version ID",
        )),
    }
}

// The resolver joins the tier manager as the second injected port this read
// needs; grouping the request half into a struct would churn every call site of
// a bug fix.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn get_transitioned_object_reader_with_tier_manager(
    bucket: &str,
    object: &str,
    rs: &Option<HTTPRangeSpec>,
    h: &HeaderMap,
    oi: &ObjectInfo,
    opts: &ObjectOptions,
    tier_config_mgr: &Arc<RwLock<TierConfigMgr>>,
    resolver: Option<&dyn ObjectEncryptionResolver>,
) -> Result<GetObjectReader, std::io::Error> {
    validate_transition_remote_version(oi)?;
    let expected_identity = tier_destination_id_from_metadata(&oi.user_defined)?;
    let lease = match expected_identity {
        Some(identity) => {
            TierConfigMgr::acquire_operation_lease_for_backend_identity(tier_config_mgr, &oi.transitioned_object.tier, identity)
                .await
        }
        None => TierConfigMgr::acquire_operation_lease(tier_config_mgr, &oi.transitioned_object.tier).await,
    };
    let tgt_client = match lease {
        Ok(d) => d,
        Err(err) => return Err(std::io::Error::other(err)),
    };

    tgt_client.validate_remote_version_id(&oi.transitioned_object.version_id)?;

    // The same read plan the local path uses, so the tier fetch is positioned in
    // the object's *stored* coordinate system and the stream is handed the same
    // decrypt/decompress transforms. Reading an encrypted object's ciphertext
    // through a plaintext-coordinate range and skipping the transform is how a
    // transitioned SSE object used to come back as silently corrupt bytes of the
    // right length (rustfs/rustfs#6025).
    let plan = ReadPlan::build_for_request(rs.clone(), oi, opts, h, resolver)
        .await
        .map_err(|err| std::io::Error::other(format!("building the read plan for {bucket}/{object} failed: {err}")))?;
    let (off, length) = (plan.storage_offset() as i64, plan.storage_length());
    let mut gopts = WarmBackendGetOpts::default();

    if off >= 0 && length >= 0 {
        gopts.start_offset = off;
        gopts.length = length;
    }

    debug!(
        bucket = %bucket,
        object = %object,
        tier = %oi.transitioned_object.tier,
        tier_object = %oi.transitioned_object.name,
        tier_version_id = %oi.transitioned_object.version_id,
        start_offset = gopts.start_offset,
        length = gopts.length,
        "fetching transitioned object from tier"
    );
    let reader = tgt_client
        .get(&oi.transitioned_object.name, &oi.transitioned_object.version_id, gopts)
        .await
        .map_err(|e| {
            tracing::error!(
                event = EVENT_LIFECYCLE_TIER_OPERATION_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket = %bucket,
                object = %object,
                tier = %oi.transitioned_object.tier,
                tier_object = %oi.transitioned_object.name,
                tier_version_id = %oi.transitioned_object.version_id,
                error = %e,
                operation = "tier_get",
                "Lifecycle tier operation failed"
            );
            e
        })?;
    let object_reader = plan
        .into_object_reader(Box::new(reader), oi)
        .map_err(|err| std::io::Error::other(format!("wrapping the tier stream for {bucket}/{object} failed: {err}")))?;
    Ok(attach_tier_operation_lease(object_reader, tgt_client))
}

struct TierOperationLeaseReader {
    inner: Box<dyn AsyncRead + Unpin + Send + Sync>,
    lease: Option<TierOperationLease>,
}

impl AsyncRead for TierOperationLeaseReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let had_capacity = buf.remaining() > 0;
        let filled_before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(poll, Poll::Ready(Err(_)))
            || (had_capacity && matches!(poll, Poll::Ready(Ok(()))) && buf.filled().len() == filled_before)
        {
            self.lease.take();
        }
        poll
    }
}

fn attach_tier_operation_lease(mut reader: GetObjectReader, lease: TierOperationLease) -> GetObjectReader {
    reader.stream = Box::new(TierOperationLeaseReader {
        inner: reader.stream,
        lease: Some(lease),
    });
    reader
}

pub async fn post_restore_opts(version_id: &str, bucket: &str, object: &str) -> Result<ObjectOptions, std::io::Error> {
    let versioned = BucketVersioningSys::prefix_enabled(bucket, object).await;
    let version_suspended = BucketVersioningSys::prefix_suspended(bucket, object).await;
    let vid = version_id.trim();
    if !vid.is_empty() && vid != NULL_VERSION_ID {
        if let Err(_err) = Uuid::parse_str(vid) {
            return Err(std::io::Error::other(
                StorageError::InvalidVersionID(bucket.to_string(), object.to_string(), vid.to_string()).to_string(),
            ));
        }
        if !versioned && !version_suspended {
            return Err(std::io::Error::other(
                StorageError::InvalidArgument(
                    bucket.to_string(),
                    object.to_string(),
                    format!("version-id specified {} but versioning is not enabled on {}", vid, bucket),
                )
                .to_string(),
            ));
        }
    }
    Ok(ObjectOptions {
        versioned,
        version_suspended,
        version_id: Some(vid.to_string()),
        ..Default::default()
    })
}

fn select_restore_s3_location(rreq: &RestoreRequest) -> Result<Option<&s3s::dto::S3Location>, std::io::Error> {
    if rreq
        .type_
        .as_ref()
        .is_none_or(|type_| type_.as_str() != RestoreRequestType::SELECT)
    {
        return Ok(None);
    }
    let output_location = rreq
        .output_location
        .as_ref()
        .ok_or_else(|| std::io::Error::other("OutputLocation required for SELECT requests"))?;
    let s3 = output_location
        .s3
        .as_ref()
        .ok_or_else(|| std::io::Error::other("OutputLocation.S3 required for SELECT requests"))?;
    if let Some(user_metadata) = s3.user_metadata.as_ref() {
        for metadata in user_metadata {
            if metadata.name.as_deref().is_none_or(|name| name.is_empty()) {
                return Err(std::io::Error::other("SELECT restore metadata name is required"));
            }
        }
    }
    Ok(Some(s3))
}

pub async fn put_restore_opts(
    bucket: &str,
    object: &str,
    rreq: &RestoreRequest,
    oi: &ObjectInfo,
) -> Result<ObjectOptions, std::io::Error> {
    let mut meta = HashMap::<String, String>::new();
    /*let mut b = false;
    let Some(Some(Some(mut sc))) = rreq.output_location.s3.storage_class else { b = true; };
    if b || sc == "" {
        //sc = oi.storage_class;
        sc = oi.transitioned_object.tier;
    }
    meta.insert(X_AMZ_STORAGE_CLASS.as_str().to_lowercase(), sc);*/

    if let Some(type_) = &rreq.type_
        && type_.as_str() == RestoreRequestType::SELECT
    {
        let Some(s3) = select_restore_s3_location(rreq)? else {
            return Err(std::io::Error::other("OutputLocation.S3 required for SELECT requests"));
        };
        if let Some(user_metadata) = s3.user_metadata.as_ref() {
            for metadata in user_metadata {
                let name = metadata
                    .name
                    .as_deref()
                    .ok_or_else(|| std::io::Error::other("SELECT restore metadata name is required"))?;
                let value = metadata.value.clone().unwrap_or_default();
                if strings_has_prefix_fold(name, "x-amz-meta") {
                    meta.insert(name.to_string(), value);
                } else {
                    meta.insert(format!("x-amz-meta-{name}"), value);
                }
            }
        }
        if let Some(tags) = &s3.tagging {
            meta.insert(
                AMZ_OBJECT_TAGGING.to_string(),
                serde_urlencoded::to_string(tags.tag_set.clone()).unwrap_or_else(|_| "".to_string()),
            );
        }
        if let Some(encryption) = &s3.encryption
            && encryption.encryption_type.as_str() != ""
        {
            meta.insert(X_AMZ_SERVER_SIDE_ENCRYPTION.as_str().to_string(), AMZ_ENCRYPTION_AES.to_string());
        }
        return Ok(ObjectOptions {
            versioned: BucketVersioningSys::prefix_enabled(bucket, object).await,
            version_suspended: BucketVersioningSys::prefix_suspended(bucket, object).await,
            user_defined: meta,
            ..Default::default()
        });
    }
    for (k, v) in oi.user_defined.iter() {
        meta.insert(k.to_string(), v.clone());
    }
    rustfs_utils::http::metadata_compat::remove_str(&mut meta, rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID);
    if !oi.user_tags.is_empty() {
        meta.insert(AMZ_OBJECT_TAGGING.to_string(), (*oi.user_tags).clone());
    }
    let restore_expiry = lifecycle::expected_expiry_time(OffsetDateTime::now_utc(), rreq.days.unwrap_or(1));
    meta.insert(
        X_AMZ_RESTORE.as_str().to_string(),
        RestoreStatus {
            is_restore_in_progress: Some(false),
            restore_expiry_date: Some(Timestamp::from(restore_expiry)),
        }
        .to_string(),
    );
    Ok(ObjectOptions {
        versioned: BucketVersioningSys::prefix_enabled(bucket, object).await,
        version_suspended: BucketVersioningSys::prefix_suspended(bucket, object).await,
        user_defined: meta,
        version_id: oi.version_id.map(|e| e.to_string()),
        mod_time: oi.mod_time,
        //expires:           oi.expires,
        ..Default::default()
    })
}

pub trait LifecycleOps {
    fn to_lifecycle_opts(&self) -> lifecycle::ObjectOpts;
    fn is_remote(&self) -> bool;
}

impl LifecycleOps for ObjectInfo {
    fn to_lifecycle_opts(&self) -> lifecycle::ObjectOpts {
        lifecycle::object_opts_from_object_info(self)
    }

    fn is_remote(&self) -> bool {
        if self.transitioned_object.status != lifecycle::TRANSITION_COMPLETE {
            return false;
        }
        !is_restored_object_on_disk(&self.user_defined)
    }
}

pub trait RestoreRequestOps {
    fn validate(&self, api: Arc<ECStore>) -> Result<(), std::io::Error>;
}

impl RestoreRequestOps for RestoreRequest {
    fn validate(&self, _api: Arc<ECStore>) -> Result<(), std::io::Error> {
        // SELECT type requires select_parameters, and vice versa
        if self.type_.as_ref().is_none_or(|t| t.as_str() != RestoreRequestType::SELECT) && self.select_parameters.is_some() {
            return Err(std::io::Error::other("Select parameters can only be specified with SELECT request type"));
        }
        if let Some(type_) = &self.type_
            && type_.as_str() == RestoreRequestType::SELECT
            && self.select_parameters.is_none()
        {
            return Err(std::io::Error::other("SELECT restore request requires select parameters to be specified"));
        }

        // OutputLocation is only valid for SELECT requests
        if self.type_.as_ref().is_none_or(|t| t.as_str() != RestoreRequestType::SELECT) && self.output_location.is_some() {
            return Err(std::io::Error::other("OutputLocation can only be specified with SELECT request type"));
        }
        select_restore_s3_location(self)?;

        // Days must not be specified with SELECT requests
        if let Some(type_) = &self.type_
            && type_.as_str() == RestoreRequestType::SELECT
            && self.days.is_some_and(|d| d > 0)
        {
            return Err(std::io::Error::other("Days cannot be specified with SELECT restore request"));
        }

        // For non-SELECT requests, days must be at least 1
        if self.type_.is_none() && self.days.is_none_or(|d| d <= 0) {
            return Err(std::io::Error::other("restoration days should be at least 1"));
        }

        Ok(())
    }
}

const _MAX_RESTORE_OBJECT_REQUEST_SIZE: i64 = 2 << 20;

pub async fn eval_action_from_lifecycle(
    lc: &BucketLifecycleConfiguration,
    lock_config: Option<&ObjectLockConfiguration>,
    oi: &ObjectInfo,
) -> lifecycle::Event {
    let event = lc.eval(&oi.to_lifecycle_opts()).await;
    debug!(
        event = EVENT_LIFECYCLE_SCAN_SKIPPED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
        action = ?event.action,
        state = "evaluated",
        "Evaluated lifecycle action during secondary scan"
    );

    let lock_enabled = lock_config.is_some_and(ObjectLockApi::enabled);
    let object_locked = object_lock_boundary::is_object_locked_by_metadata(&oi.user_defined, oi.delete_marker);

    match event.action {
        IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction if lock_enabled || object_locked => {
            return lifecycle::Event::default();
        }
        IlmAction::DeleteAction
        | IlmAction::DeleteRestoredAction
        | IlmAction::DeleteVersionAction
        | IlmAction::DeleteRestoredVersionAction => {
            if matches!(event.action, IlmAction::DeleteVersionAction | IlmAction::DeleteRestoredVersionAction)
                && oi.version_id.is_none()
            {
                return lifecycle::Event::default();
            }
            // Destructive expiry never bypasses retention. Restore expiry only
            // removes the local copy; the retained logical version remains.
            if !event.action.delete_restored()
                && (object_locked
                    || !matches!(
                        object_lock_boundary::check_object_lock_for_deletion_with_config(lock_config, oi, false),
                        Ok(None)
                    ))
            {
                //if serverDebugLog {
                if oi.version_id.is_some() {
                    debug!(
                        event = EVENT_LIFECYCLE_SCAN_SKIPPED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                        object = %oi.name,
                        version_id = %oi.version_id.map(|v| v.to_string()).unwrap_or_default(),
                        reason = "object_locked",
                        "Skipped lifecycle delete because object version is locked"
                    );
                } else {
                    debug!(
                        event = EVENT_LIFECYCLE_SCAN_SKIPPED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                        object = %oi.name,
                        reason = "object_locked",
                        "Skipped lifecycle delete because object is locked"
                    );
                }
                return lifecycle::Event::default();
            }
        }
        _ => (),
    }

    if lifecycle_action_blocked_by_replication(event.action, oi) {
        let reason = if oi.version_purge_status.is_pending() {
            "version_purge_pending"
        } else if oi.replication_status == ReplicationStatusType::Failed {
            "replication_failed"
        } else {
            "replication_pending"
        };
        debug!(
            event = EVENT_LIFECYCLE_SCAN_SKIPPED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            object = %oi.name,
            version_id = ?oi.version_id,
            action = ?event.action,
            replication_status = ?oi.replication_status,
            version_purge_status = ?oi.version_purge_status,
            reason,
            "Skipped lifecycle action because replication is not terminal"
        );
        return lifecycle::Event::default();
    }

    event
}

pub(crate) async fn lifecycle_delete_all_versions_blocked_by_replication(
    api: Arc<ECStore>,
    bucket: &str,
    object: &str,
    action: IlmAction,
) -> Result<bool, Error> {
    if !action.delete_all() {
        return Ok(false);
    }

    let mut marker = None;
    let mut version_marker = None;
    loop {
        let page = api
            .clone()
            .list_object_versions(bucket, object, marker.clone(), version_marker.clone(), None, 1000)
            .await?;

        match lifecycle_delete_all_versions_replication_scan(object, &page.objects) {
            VersionReplicationScan::Blocked => return Ok(true),
            VersionReplicationScan::Done => return Ok(false),
            VersionReplicationScan::Continue => {}
        }

        if !page.is_truncated {
            return Ok(false);
        }

        marker = page.next_marker;
        version_marker = page.next_version_idmarker;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VersionReplicationScan {
    Blocked,
    Done,
    Continue,
}

fn lifecycle_delete_all_versions_replication_scan(object: &str, versions: &[ObjectInfo]) -> VersionReplicationScan {
    for version in versions {
        let name = version.name.as_str();
        if name == object {
            if lifecycle_replication_blocks_action(version) {
                return VersionReplicationScan::Blocked;
            }
            continue;
        }
        if name > object {
            return VersionReplicationScan::Done;
        }
    }
    VersionReplicationScan::Continue
}

fn lifecycle_action_blocked_by_replication(action: IlmAction, oi: &ObjectInfo) -> bool {
    replication_sink::lifecycle_action_waits_for_replication(action) && lifecycle_replication_blocks_action(oi)
}

fn lifecycle_replication_blocks_action(oi: &ObjectInfo) -> bool {
    replication_sink::replication_status_blocks_lifecycle(&oi.replication_status) || oi.version_purge_status.is_pending()
}

pub async fn apply_transition_rule(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    if oi.delete_marker || oi.is_dir {
        return false;
    }
    runtime_sources::transition_state_handle()
        .queue_transition_task(oi, event, src)
        .await
}

async fn lifecycle_expiry_publication_guard(
    api: &ECStore,
    oi: &ObjectInfo,
    bucket_incarnation_id: Uuid,
) -> Option<rustfs_lock::NamespaceLockGuard> {
    let result = async {
        let lock = api
            .new_ns_lock(&oi.bucket, rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH)
            .await?;
        let guard = lock.get_read_lock(get_lock_acquire_timeout()).await.map_err(Error::other)?;
        if guard.is_lock_lost() {
            return Err(Error::other("table-bucket publication lock was lost before lifecycle delete admission"));
        }
        if !metadata_boundary::lifecycle_expiry_allowed(api, &oi.bucket, bucket_incarnation_id).await? {
            return Ok(None);
        }
        Ok(Some(guard))
    }
    .await;
    match result {
        Ok(guard) => guard,
        Err(err) => {
            warn!(
                event = EVENT_LIFECYCLE_DELETE_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket = %oi.bucket,
                object = %oi.name,
                operation = "authorize_lifecycle_expiry",
                error = %err,
                "Lifecycle delete admission failed"
            );
            None
        }
    }
}

pub async fn apply_expiry_on_transitioned_object(
    api: Arc<ECStore>,
    oi: &ObjectInfo,
    lc_event: &lifecycle::Event,
    src: &LcEventSrc,
    bucket_incarnation_id: Uuid,
) -> bool {
    if lc_event.action.delete_all() {
        return apply_expiry_on_non_transitioned_objects(api, oi, lc_event, src, bucket_incarnation_id).await;
    }
    let time_ilm = Metrics::time_ilm(lc_event.action);
    if let Err(_err) = expire_transitioned_object(api, oi, lc_event, src, bucket_incarnation_id).await {
        return false;
    }
    time_ilm(1)();

    true
}

pub async fn apply_expiry_on_non_transitioned_objects(
    api: Arc<ECStore>,
    oi: &ObjectInfo,
    lc_event: &lifecycle::Event,
    _src: &LcEventSrc,
    bucket_incarnation_id: Uuid,
) -> bool {
    let Some(publication_guard) = lifecycle_expiry_publication_guard(&api, oi, bucket_incarnation_id).await else {
        return false;
    };
    let snapshot = match lifecycle_delete_config_snapshot(&api, oi).await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            error!(
                event = EVENT_LIFECYCLE_DELETE_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket = %oi.bucket,
                object = %oi.name,
                operation = "load_delete_config_snapshot",
                error = ?err,
                "Lifecycle delete admission failed"
            );
            return false;
        }
    };
    let (versioned, version_suspended) = snapshot.versioning_config().delete_state(&oi.name);
    let mut opts = ObjectOptions {
        versioned,
        version_suspended,
        expiration: ExpirationOptions { expire: true },
        delete_replication_config_snapshot: Some(Arc::new(snapshot)),
        expected_bucket_incarnation_id: Some(bucket_incarnation_id),
        ..Default::default()
    };
    opts.add_namespace_lock_guard(&publication_guard);

    if lc_event.action.delete_versioned() {
        opts.version_id = oi.version_id.map(|v| v.to_string());
    }

    if lc_event.action.delete_all() {
        opts.delete_prefix = true;
        opts.delete_prefix_object = true;
        opts.lifecycle_delete_all = Some(crate::object_api::LifecycleDeleteAllRequest {
            version_id: oi.version_id.filter(|version_id| !version_id.is_nil()),
            delete_marker: oi.delete_marker,
            action: lc_event.action,
            rule_id: lc_event.rule_id.clone(),
            phase: crate::object_api::LifecycleDeleteAllPhase::Preflight,
        });
        opts.ensure_lifecycle_delete_all_journal();
    }

    let time_ilm = Metrics::time_ilm(lc_event.action);

    //debug!("lc_event.action: {:?}", lc_event.action);
    debug!("expiry_on_non_transitioned_objects opts: {:?}", opts);
    let mut dobj = match api
        .delete_object_with_tier_delete_journal(&oi.bucket, &encode_dir_object(&oi.name), opts)
        .await
    {
        Ok(dobj) => dobj,
        Err(e) => {
            error!(
                event = EVENT_LIFECYCLE_DELETE_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                bucket = %oi.bucket,
                object = %oi.name,
                operation = "delete_object",
                error = ?e,
                "Lifecycle delete failed"
            );
            return false;
        }
    };
    schedule_lifecycle_replication_delete_if_needed(oi, &dobj).await;

    // The object (or all its versions, for delete_all) was expired; evict any
    // cached body so dead bytes do not sit resident until TTL (ODC-26). The
    // cache identity is the decoded object name used by GET, not the
    // encode_dir_object form passed to delete_object.
    crate::object_api::notify_object_mutation(&oi.bucket, &oi.name).await;

    //debug!("dobj: {:?}", dobj);
    if dobj.name.is_empty() {
        dobj = oi.clone();
    }

    //let tags = LcAuditEvent::new(lc_event.clone(), src.clone()).tags();
    //tags["version-id"] = dobj.version_id;

    emit_non_transitioned_expiration_event(lc_event.action, oi, dobj);

    if lc_event.action != IlmAction::NoneAction {
        let mut num_versions = 1_u64;
        if lc_event.action.delete_all() {
            num_versions = oi.num_versions as u64;
        }
        time_ilm(num_versions)();
    }

    true
}

async fn enqueue_expiry_rule_with_incarnation(
    event: &lifecycle::Event,
    src: &LcEventSrc,
    oi: &ObjectInfo,
    bucket_incarnation_id: Uuid,
) -> bool {
    let expiry_state = runtime_sources::expiry_state_handle();
    let mut expiry_state = expiry_state.write().await;
    expiry_state.enqueue_by_days(oi, event, src, bucket_incarnation_id)
}

pub(crate) async fn apply_expiry_rule_in(api: Arc<ECStore>, event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    let Ok(_lifecycle_guard) = api.acquire_bucket_lifecycle_read_lock(&oi.bucket).await else {
        return false;
    };
    let Ok(bucket_incarnation_id) = api.bucket_incarnation_id_from_disk(&oi.bucket).await else {
        return false;
    };
    let current = match api
        .get_object_info(
            &oi.bucket,
            &oi.name,
            &ObjectOptions {
                version_id: oi.version_id.map(|version_id| version_id.to_string()),
                versioned: oi.version_id.is_some(),
                expected_bucket_incarnation_id: Some(bucket_incarnation_id),
                ..Default::default()
            },
        )
        .await
    {
        Ok(current) => current,
        Err(_) => return false,
    };
    if current.version_id != oi.version_id
        || current.data_dir != oi.data_dir
        || current.mod_time != oi.mod_time
        || current.etag != oi.etag
        || current.delete_marker != oi.delete_marker
        || current.transitioned_object.name != oi.transitioned_object.name
        || current.transitioned_object.version_id != oi.transitioned_object.version_id
        || current.transitioned_object.tier != oi.transitioned_object.tier
        || current.transitioned_object.status != oi.transitioned_object.status
        || current.restore_expires != oi.restore_expires
    {
        return false;
    }
    enqueue_expiry_rule_with_incarnation(event, src, oi, bucket_incarnation_id).await
}

pub async fn apply_expiry_rule(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    let Some(api) = runtime_sources::object_store_handle() else {
        return false;
    };
    apply_expiry_rule_in(api, event, src, oi).await
}

fn lifecycle_deleted_object(oi: &ObjectInfo, dobj: &ObjectInfo) -> DeletedObject {
    let replication_state = dobj.replication_state();
    let replication_state = (!replication_state.targets.is_empty() || !replication_state.purge_targets.is_empty())
        .then(|| replication_state_to_filemeta(&replication_state));

    if dobj.delete_marker {
        return DeletedObject {
            object_name: oi.name.clone(),
            delete_marker: true,
            delete_marker_version_id: dobj.version_id,
            delete_marker_mtime: dobj.mod_time.or(oi.mod_time),
            replication_state,
            ..Default::default()
        };
    }

    if oi.delete_marker && oi.version_id.is_some() {
        return DeletedObject {
            object_name: oi.name.clone(),
            delete_marker: false,
            delete_marker_version_id: oi.version_id,
            delete_marker_mtime: oi.mod_time,
            replication_state,
            ..Default::default()
        };
    }

    DeletedObject {
        object_name: oi.name.clone(),
        delete_marker: false,
        version_id: oi.version_id,
        delete_marker_mtime: oi.mod_time,
        replication_state,
        ..Default::default()
    }
}

async fn schedule_lifecycle_replication_delete_if_needed(oi: &ObjectInfo, dobj: &ObjectInfo) {
    let delete_object = lifecycle_deleted_object(oi, dobj);
    if delete_object.replication_state.is_none() {
        return;
    }
    replication_sink::schedule_delete(oi.bucket.clone(), delete_object).await;
}

async fn lifecycle_delete_config_snapshot(api: &ECStore, oi: &ObjectInfo) -> Result<DeleteReplicationConfigSnapshot, Error> {
    ReplicationObjectBridge::delete_request_config(api, &oi.bucket).await
}

#[allow(
    dead_code,
    reason = "MinIO-parity tier/lifecycle entry point that this port never wired (backlog#1823)"
)]
pub async fn apply_lifecycle_action(event: &lifecycle::Event, src: &LcEventSrc, oi: &ObjectInfo) -> bool {
    let mut success = false;
    match event.action {
        IlmAction::DeleteVersionAction
        | IlmAction::DeleteAction
        | IlmAction::DeleteRestoredAction
        | IlmAction::DeleteRestoredVersionAction
        | IlmAction::DeleteAllVersionsAction
        | IlmAction::DelMarkerDeleteAllVersionsAction => {
            success = apply_expiry_rule(event, src, oi).await;
        }
        IlmAction::TransitionAction | IlmAction::TransitionVersionAction => {
            success = apply_transition_rule(event, src, oi).await;
        }
        _ => (),
    }
    success
}

#[cfg(test)]
mod tests {
    use super::expiry_worker_count;
    use super::{
        DATE_EXPIRY_EXISTING_OBJECTS_GRACE_SECS, DEFAULT_TRANSITION_QUEUE_CAPACITY, DEFAULT_TRANSITION_WORKERS_ABSOLUTE_MAX,
        DEFAULT_TRANSITION_WORKERS_CAP, EVENT_LIFECYCLE_EVALUATION_FAILED, EVENT_LIFECYCLE_EXPIRED_DETECTED,
        EVENT_LIFECYCLE_NOT_ENQUEUED, ExpiryState, ExpiryTask, FreeVersionTask, ManualTransitionJobRecoveryOutcome,
        ManualTransitionQueueSnapshot, ManualTransitionRunOptions, ManualTransitionRunReport, StaleMultipartUploadCandidate,
        TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL, TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL, TRANSITION_COMPLETE,
        TierFreeVersionRecoverySchedule, TransitionEnqueueOutcome, TransitionState, TransitionedObject, VersionReplicationScan,
        cleanup_empty_multipart_sha_dirs_on_local_disks, cleanup_stale_multipart_uploads_once_at,
        enqueue_recovered_free_version_with_state, enqueue_transition_for_existing_objects_scoped,
        enqueue_transition_with_lifecycle, enqueue_transition_with_lifecycle_report, eval_action_from_lifecycle,
        get_lock_acquire_timeout, jitter_tier_free_version_recovery_delay, lifecycle_action_blocked_by_replication,
        lifecycle_delete_all_versions_replication_scan, lifecycle_deleted_object, lifecycle_replication_blocks_action,
        lifecycle_rule_has_date_expiration, manual_transition_duration_elapsed, manual_transition_has_more_after_limit,
        manual_transition_recovery_progress_sink, manual_transition_version_marker, manual_transition_worker_failure_reason,
        mark_delete_opts_skip_decommissioned_on_remote_success, merge_stale_multipart_candidate,
        persist_manual_transition_job_progress_if_owned, persist_manual_transition_page_checkpoint,
        recover_manual_transition_job, recover_manual_transition_jobs, resolve_tier_free_version_recovery_enabled,
        resolve_transition_queue_capacity, resolve_transition_queue_send_timeout, resolve_transition_worker_count,
        resolve_transition_workers_absolute_max, run_tier_free_version_recovery_loop, select_restore_s3_location,
        set_lifecycle_observability_observer, set_recovered_free_version_enqueue_observer,
        should_defer_date_expiry_for_recent_config_update, transitioned_cleanup_tuple, transitioned_object_delete_opts,
        wait_for_tier_free_version_recovery,
    };
    #[cfg(feature = "test-util")]
    use super::{delete_free_version_remote_object_then, encode_dir_object, get_transitioned_object_reader_with_tier_manager};
    use crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
    use crate::bucket::lifecycle::bucket_lifecycle_ops::{
        decode_manual_transition_continuation_token, encode_manual_transition_continuation_token,
    };
    use crate::bucket::lifecycle::config_boundary;
    use crate::bucket::lifecycle::manual_transition_job::{
        ManualTransitionJobCasBarrier, ManualTransitionJobRecord, ManualTransitionJobState, ManualTransitionScopeAdmission,
        ManualTransitionScopeAdmissionClaim, ManualTransitionTaskRecord, ManualTransitionWorkerFailureReason,
        ManualTransitionWorkerResult, ManualTransitionWorkerResultRecord, claim_manual_transition_scope_admission,
        delete_manual_transition_scope_admission_if_current, legacy_manual_transition_scope_key,
        load_manual_transition_job_record, load_manual_transition_job_record_with_etag, load_manual_transition_scope_admission,
        load_manual_transition_scope_admission_with_etag, load_manual_transition_task_record,
        manual_transition_scope_record_object_name, manual_transition_worker_result_object_name,
        manual_transition_worker_result_task_key, reconcile_manual_transition_worker_results,
        record_manual_transition_worker_result, record_manual_transition_worker_result_with_reason,
        renew_manual_transition_job_lease_if_owned, request_manual_transition_job_cancel, save_manual_transition_job_record,
        save_manual_transition_job_record_if_current, save_manual_transition_scope_admission_if_absent,
        save_manual_transition_scope_admission_if_current, save_manual_transition_task_if_absent,
        save_manual_transition_worker_result_if_absent,
    };
    use crate::bucket::lifecycle::replication_sink::{ReplicationStatusType, VersionPurgeStatusType};
    use crate::bucket::lifecycle::runtime_boundary as runtime_sources;
    use crate::bucket::lifecycle::tier_free_version_recovery::{
        FreeVersionRecoveryStats, RecoveryWalkTestAction, list_tier_free_versions, recover_tier_free_versions_with_cancel,
        set_recovery_bucket_list_wait_hook, set_recovery_walk_test_hook,
    };
    use crate::bucket::lifecycle::tier_last_day_stats::LastDayTierStats;
    use crate::bucket::lifecycle::tier_sweeper::Jentry;
    use crate::bucket::metadata::{BUCKET_LIFECYCLE_CONFIG, BUCKET_VERSIONING_CONFIG};
    use crate::bucket::metadata_sys;
    #[cfg(feature = "test-util")]
    use crate::client::transition_api::ReaderImpl;
    use crate::disk::endpoint::Endpoint;
    use crate::disk::{RUSTFS_META_MULTIPART_BUCKET, STORAGE_FORMAT_FILE};
    use crate::error::{Error, is_err_invalid_upload_id};
    use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    use crate::object_api::{ObjectInfo, ObjectOptions, PutObjReader};
    #[cfg(feature = "test-util")]
    use crate::services::tier::test_util::register_mock_tier;
    #[cfg(feature = "test-util")]
    use crate::services::tier::tier::TierConfigMgr;
    #[cfg(feature = "test-util")]
    use crate::services::tier::warm_backend::WarmBackend as _;
    use crate::set_disk::{MultipartCommitBarrier, MultipartCommitPause};
    use crate::set_disk::{RUSTFS_MULTIPART_BUCKET_KEY, RUSTFS_MULTIPART_OBJECT_KEY};
    use crate::storage_api_contracts::namespace::NamespaceLocking as _;
    use crate::storage_api_contracts::{
        bucket::{BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions},
        lifecycle::ExpirationOptions,
        list::ListOperations as _,
        multipart::MultipartOperations as _,
        object::{ObjectIO as _, ObjectOperations as _},
    };
    use crate::store::ECStore;
    #[cfg(feature = "test-util")]
    use bytes::Bytes;
    use futures::FutureExt;
    #[cfg(feature = "test-util")]
    use http::HeaderMap;
    use rustfs_common::metrics::{IlmAction, global_metrics};
    use rustfs_config::ENV_MAX_EXPIRY_WORKERS;
    use rustfs_config::ENV_TRANSITION_WORKERS_ABSOLUTE_MAX;
    use rustfs_data_usage::TierStats;
    use rustfs_filemeta::{FileInfo, FileMeta};
    use s3s::dto::{
        BucketLifecycleConfiguration, DefaultRetention, ExpirationStatus, LifecycleExpiration, LifecycleRule, MetadataEntry,
        ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRetentionMode, ObjectLockRule, OutputLocation, RestoreRequest,
        RestoreRequestType, S3Location, Timestamp, Transition, TransitionStorageClass,
    };
    use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE};
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;
    use std::env;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex, OnceLock};
    use std::time::Duration as StdDuration;
    use time::OffsetDateTime;
    use tokio::fs;
    #[cfg(feature = "test-util")]
    use tokio::io::AsyncReadExt;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    fn free_version_recovery_stats(enqueued: usize, failed: usize, truncated: bool) -> FreeVersionRecoveryStats {
        FreeVersionRecoveryStats {
            scanned: enqueued.saturating_add(failed),
            enqueued,
            failed,
            next_bucket_marker: truncated.then(|| "bucket".to_string()),
            next_object_marker: truncated.then(|| "object".to_string()),
            scanned_entries: 1,
            buckets_scanned: 1,
            truncated,
        }
    }

    static RECOVERY_JITTER_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn counting_recovery_delay(delay: StdDuration) -> StdDuration {
        RECOVERY_JITTER_CALLS.fetch_add(1, Ordering::SeqCst);
        delay
    }

    #[test]
    fn tier_free_version_recovery_backs_off_only_after_complete_idle_sweeps() {
        let idle = free_version_recovery_stats(0, 0, false);
        let mut schedule = TierFreeVersionRecoverySchedule::default();

        assert_eq!(schedule.next_delay, StdDuration::ZERO);
        for expected in [60, 120, 240, 480, 600, 600, 600, 600] {
            schedule.record_success(&idle, StdDuration::ZERO);
            assert_eq!(schedule.next_delay, StdDuration::from_secs(expected));
        }
        assert_eq!(schedule.next_delay.as_secs() / TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL.as_secs(), 10);
    }

    #[test]
    fn tier_free_version_recovery_failure_backoff_waits_after_completion_and_resets_after_success() {
        let mut schedule = TierFreeVersionRecoverySchedule::default();

        for expected in [60, 120, 240, 480, 600, 600] {
            schedule.record_failure(StdDuration::from_secs(75));
            assert_eq!(schedule.next_delay, StdDuration::from_secs(expected));
            assert_eq!(schedule.previous_run_duration, StdDuration::ZERO);
        }

        schedule.record_success(&free_version_recovery_stats(0, 0, false), StdDuration::ZERO);
        schedule.record_failure(StdDuration::from_secs(75));
        assert_eq!(schedule.next_delay, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
        assert_eq!(schedule.previous_run_duration, StdDuration::ZERO);
    }

    #[test]
    fn tier_free_version_recovery_enabled_setting_is_opt_out_and_fails_open() {
        assert!(resolve_tier_free_version_recovery_enabled(Err(env::VarError::NotPresent)));
        assert!(resolve_tier_free_version_recovery_enabled(Ok("true".to_string())));
        assert!(!resolve_tier_free_version_recovery_enabled(Ok(" false ".to_string())));
        assert!(resolve_tier_free_version_recovery_enabled(Ok("invalid".to_string())));
    }

    #[test]
    fn tier_free_version_recovery_pagination_preserves_full_sweep_backoff() {
        let idle = free_version_recovery_stats(0, 0, false);
        let mut schedule = TierFreeVersionRecoverySchedule::default();
        schedule.record_success(&idle, StdDuration::ZERO);
        schedule.record_success(&idle, StdDuration::ZERO);
        assert_eq!(schedule.next_delay, StdDuration::from_secs(120));
        assert_eq!(schedule.idle_interval, StdDuration::from_secs(240));

        schedule.record_success(&free_version_recovery_stats(0, 0, true), StdDuration::ZERO);
        assert_eq!(schedule.next_delay, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
        assert_eq!(schedule.idle_interval, StdDuration::from_secs(240));
        assert_eq!(schedule.bucket_marker.as_deref(), Some("bucket"));
        assert_eq!(schedule.object_marker.as_deref(), Some("object"));

        schedule.record_success(&free_version_recovery_stats(0, 0, true), StdDuration::ZERO);
        assert_eq!(schedule.next_delay, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
        assert_eq!(schedule.idle_interval, StdDuration::from_secs(240));

        schedule.record_success(&idle, StdDuration::ZERO);
        assert_eq!(schedule.next_delay, StdDuration::from_secs(240));
        assert_eq!(schedule.idle_interval, StdDuration::from_secs(480));
        assert!(schedule.bucket_marker.is_none());
        assert!(schedule.object_marker.is_none());
    }

    #[test]
    fn tier_free_version_recovery_wake_during_pagination_keeps_one_full_follow_up() {
        let mut schedule = TierFreeVersionRecoverySchedule::default();
        schedule.record_success(&free_version_recovery_stats(0, 0, true), StdDuration::ZERO);

        schedule.request_retry();
        schedule.request_retry();
        assert!(schedule.follow_up_sweep);
        assert_eq!(schedule.bucket_marker.as_deref(), Some("bucket"));
        assert_eq!(schedule.object_marker.as_deref(), Some("object"));

        schedule.record_success(&free_version_recovery_stats(0, 0, false), StdDuration::ZERO);
        assert_eq!(schedule.next_delay, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
        assert!(!schedule.follow_up_sweep);
        assert!(schedule.bucket_marker.is_none());
        assert!(schedule.object_marker.is_none());
    }

    #[test]
    fn tier_free_version_recovery_work_during_pagination_keeps_one_full_follow_up() {
        let idle = free_version_recovery_stats(0, 0, false);
        for work in [
            free_version_recovery_stats(1, 0, true),
            free_version_recovery_stats(0, 1, true),
        ] {
            let mut schedule = TierFreeVersionRecoverySchedule::default();
            schedule.record_success(&idle, StdDuration::ZERO);
            schedule.record_success(&idle, StdDuration::ZERO);
            assert_eq!(schedule.idle_interval, StdDuration::from_secs(240));

            schedule.record_success(&work, StdDuration::ZERO);
            assert!(schedule.follow_up_sweep);
            assert_eq!(schedule.idle_interval, StdDuration::from_secs(240));
            assert!(!schedule.jitter_next_delay);

            schedule.record_success(&idle, StdDuration::ZERO);
            assert!(!schedule.follow_up_sweep);
            assert_eq!(schedule.next_delay, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
            assert_eq!(schedule.idle_interval, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
            assert!(!schedule.jitter_next_delay);

            schedule.record_success(&idle, StdDuration::ZERO);
            assert_eq!(schedule.next_delay, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
            assert_eq!(schedule.idle_interval, StdDuration::from_secs(120));
        }
    }

    #[test]
    fn tier_free_version_recovery_work_on_complete_page_gets_full_follow_up() {
        let idle = free_version_recovery_stats(0, 0, false);
        for work in [
            free_version_recovery_stats(1, 0, false),
            free_version_recovery_stats(0, 1, false),
        ] {
            let mut schedule = TierFreeVersionRecoverySchedule::default();
            schedule.record_success(&idle, StdDuration::ZERO);
            schedule.record_success(&idle, StdDuration::ZERO);
            assert_eq!(schedule.idle_interval, StdDuration::from_secs(240));

            schedule.record_success(&work, StdDuration::ZERO);
            assert_eq!(schedule.next_delay, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
            assert_eq!(schedule.idle_interval, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
            assert!(!schedule.follow_up_sweep);
            assert!(!schedule.jitter_next_delay);

            schedule.record_success(&idle, StdDuration::ZERO);
            assert_eq!(schedule.next_delay, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
            assert_eq!(schedule.idle_interval, StdDuration::from_secs(120));
        }
    }

    #[test]
    fn tier_free_version_recovery_jitter_stays_within_bounded_window() {
        assert_eq!(jitter_tier_free_version_recovery_delay(StdDuration::ZERO), StdDuration::ZERO);
        for _ in 0..100 {
            let below_base = jitter_tier_free_version_recovery_delay(StdDuration::from_secs(1));
            assert!(below_base >= TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
            assert!(below_base <= StdDuration::from_secs(66));

            let base = jitter_tier_free_version_recovery_delay(TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
            assert!(base >= TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
            assert!(base <= StdDuration::from_secs(66));

            let max = jitter_tier_free_version_recovery_delay(TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL);
            assert!(max >= StdDuration::from_secs(9 * 60));
            assert!(max <= TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL);

            let above_max = jitter_tier_free_version_recovery_delay(StdDuration::from_secs(2 * 60 * 60));
            assert!(above_max >= StdDuration::from_secs(9 * 60));
            assert!(above_max <= TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL);
        }
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn tier_free_version_recovery_coalesces_notify_bursts_per_wait() {
        RECOVERY_JITTER_CALLS.store(0, Ordering::SeqCst);
        let cancel = CancellationToken::new();
        let notify = Arc::new(tokio::sync::Notify::new());
        let mut schedule = TierFreeVersionRecoverySchedule {
            next_delay: TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL,
            idle_interval: TIER_FREE_VERSION_RECOVERY_MAX_IDLE_INTERVAL,
            jitter_next_delay: true,
            ..Default::default()
        };
        let wait_notify = Arc::clone(&notify);
        let wait_cancel = cancel.clone();
        let waiter = tokio::spawn(async move {
            let ready =
                wait_for_tier_free_version_recovery(&wait_cancel, wait_notify.as_ref(), &mut schedule, counting_recovery_delay)
                    .await;
            (ready, schedule)
        });

        tokio::task::yield_now().await;
        assert_eq!(RECOVERY_JITTER_CALLS.load(Ordering::SeqCst), 1);
        notify.notify_one();
        notify.notify_one();
        notify.notify_one();
        tokio::task::yield_now().await;
        assert_eq!(RECOVERY_JITTER_CALLS.load(Ordering::SeqCst), 1);

        tokio::time::advance(StdDuration::from_secs(59)).await;
        assert!(!waiter.is_finished());
        tokio::time::advance(StdDuration::from_secs(1)).await;

        let (ready, schedule) = waiter.await.expect("recovery wait task should complete");
        assert!(ready);
        assert_eq!(schedule.next_delay, TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL);
        assert_eq!(RECOVERY_JITTER_CALLS.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn tier_free_version_recovery_loop_uses_exponential_idle_schedule() {
        let cancel = CancellationToken::new();
        let state = ExpiryState::new();
        let recovery_notify = Arc::clone(&state.read().await.recovery_notify);
        let started_at = tokio::time::Instant::now();
        let call_times = Arc::new(StdMutex::new(Vec::new()));
        let recorded_call_times = Arc::clone(&call_times);
        let loop_cancel = cancel.clone();
        let worker = tokio::spawn(async move {
            run_tier_free_version_recovery_loop(loop_cancel, state, std::convert::identity, move |_, _, _| {
                recorded_call_times
                    .lock()
                    .expect("recovery call times lock should not be poisoned")
                    .push(tokio::time::Instant::now().duration_since(started_at));
                async { Ok(free_version_recovery_stats(0, 0, false)) }
            })
            .await;
        });

        tokio::task::yield_now().await;
        assert_eq!(
            call_times
                .lock()
                .expect("recovery call times lock should not be poisoned")
                .as_slice(),
            &[StdDuration::ZERO]
        );

        tokio::time::advance(StdDuration::from_secs(60)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(StdDuration::from_secs(120)).await;
        tokio::task::yield_now().await;
        for delay in [240, 480, 600, 600, 600] {
            tokio::time::advance(StdDuration::from_secs(delay)).await;
            tokio::task::yield_now().await;
        }
        tokio::time::advance(StdDuration::from_secs(5 * 60)).await;
        recovery_notify.notify_one();
        tokio::task::yield_now().await;
        tokio::time::advance(StdDuration::from_secs(59)).await;
        assert_eq!(
            call_times
                .lock()
                .expect("recovery call times lock should not be poisoned")
                .len(),
            8
        );
        tokio::time::advance(StdDuration::from_secs(1)).await;
        tokio::task::yield_now().await;
        cancel.cancel();
        worker.await.expect("recovery loop should stop after cancellation");

        assert_eq!(
            call_times
                .lock()
                .expect("recovery call times lock should not be poisoned")
                .as_slice(),
            &[
                StdDuration::ZERO,
                StdDuration::from_secs(60),
                StdDuration::from_secs(180),
                StdDuration::from_secs(420),
                StdDuration::from_secs(900),
                StdDuration::from_secs(1_500),
                StdDuration::from_secs(2_100),
                StdDuration::from_secs(2_700),
                StdDuration::from_secs(3_060),
            ]
        );
    }

    async fn tier_free_version_recovery_page_call_times(run_duration: StdDuration) -> Vec<StdDuration> {
        RECOVERY_JITTER_CALLS.store(0, Ordering::SeqCst);
        let cancel = CancellationToken::new();
        let state = ExpiryState::new();
        let started_at = tokio::time::Instant::now();
        let call_times = Arc::new(StdMutex::new(Vec::new()));
        let recorded_call_times = Arc::clone(&call_times);
        let call_index = Arc::new(AtomicUsize::new(0));
        let recorded_call_index = Arc::clone(&call_index);
        let loop_cancel = cancel.clone();
        let recovery_cancel = cancel.clone();
        let worker = tokio::spawn(async move {
            run_tier_free_version_recovery_loop(loop_cancel, state, counting_recovery_delay, move |_, _, _| {
                recorded_call_times
                    .lock()
                    .expect("recovery call times lock should not be poisoned")
                    .push(tokio::time::Instant::now().duration_since(started_at));
                let index = recorded_call_index.fetch_add(1, Ordering::SeqCst);
                let cancel = recovery_cancel.clone();
                async move {
                    if index == 0 {
                        tokio::time::sleep(run_duration).await;
                        Ok(free_version_recovery_stats(0, 0, true))
                    } else {
                        cancel.cancel();
                        Ok(free_version_recovery_stats(0, 0, false))
                    }
                }
            })
            .await;
        });

        tokio::task::yield_now().await;
        tokio::time::advance(run_duration).await;
        tokio::task::yield_now().await;
        if run_duration < TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL {
            let remaining = TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL - run_duration;
            tokio::time::advance(remaining - StdDuration::from_secs(1)).await;
            assert_eq!(call_index.load(Ordering::SeqCst), 1);
            tokio::time::advance(StdDuration::from_secs(1)).await;
        }
        tokio::task::yield_now().await;
        worker.await.expect("recovery loop should stop after cancellation");
        assert_eq!(
            RECOVERY_JITTER_CALLS.load(Ordering::SeqCst),
            0,
            "active pagination must not invoke idle jitter"
        );

        Arc::try_unwrap(call_times)
            .expect("recovery loop should release the call-time log")
            .into_inner()
            .expect("recovery call times lock should not be poisoned")
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn tier_free_version_recovery_preserves_start_to_start_page_cadence() {
        assert_eq!(
            tier_free_version_recovery_page_call_times(StdDuration::from_secs(45)).await,
            vec![StdDuration::ZERO, StdDuration::from_secs(60)]
        );
        assert_eq!(
            tier_free_version_recovery_page_call_times(StdDuration::from_secs(75)).await,
            vec![StdDuration::ZERO, StdDuration::from_secs(75)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tier_free_version_recovery_loop_resets_high_backoff_after_error() {
        let cancel = CancellationToken::new();
        let state = ExpiryState::new();
        let call_index = Arc::new(AtomicUsize::new(0));
        let recorded_call_index = Arc::clone(&call_index);
        let loop_cancel = cancel.clone();
        let worker = tokio::spawn(async move {
            run_tier_free_version_recovery_loop(loop_cancel, state, std::convert::identity, move |_, _, _| {
                let index = recorded_call_index.fetch_add(1, Ordering::SeqCst);
                async move {
                    if index == 2 {
                        Err(std::io::Error::other("injected recovery failure").into())
                    } else {
                        Ok(free_version_recovery_stats(0, 0, false))
                    }
                }
            })
            .await;
        });

        tokio::task::yield_now().await;
        tokio::time::advance(StdDuration::from_secs(60)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(StdDuration::from_secs(120)).await;
        tokio::task::yield_now().await;
        assert_eq!(call_index.load(Ordering::SeqCst), 3);

        tokio::time::advance(StdDuration::from_secs(59)).await;
        assert_eq!(call_index.load(Ordering::SeqCst), 3);
        tokio::time::advance(StdDuration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(call_index.load(Ordering::SeqCst), 4);

        cancel.cancel();
        worker.await.expect("recovery loop should stop after cancellation");
    }

    #[tokio::test(start_paused = true)]
    async fn tier_free_version_recovery_loop_carries_markers_across_errors() {
        let cancel = CancellationToken::new();
        let state = ExpiryState::new();
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let recorded_calls = Arc::clone(&calls);
        let call_index = Arc::new(AtomicUsize::new(0));
        let recorded_call_index = Arc::clone(&call_index);
        let loop_cancel = cancel.clone();
        let worker = tokio::spawn(async move {
            run_tier_free_version_recovery_loop(loop_cancel, state, std::convert::identity, move |bucket, object, _| {
                recorded_calls
                    .lock()
                    .expect("recovery call log lock should not be poisoned")
                    .push((bucket, object));
                let index = recorded_call_index.fetch_add(1, Ordering::SeqCst);
                async move {
                    match index {
                        0 => Ok(free_version_recovery_stats(0, 0, true)),
                        1 => Err(std::io::Error::other("injected recovery failure").into()),
                        _ => Ok(free_version_recovery_stats(0, 0, false)),
                    }
                }
            })
            .await;
        });

        tokio::task::yield_now().await;
        assert_eq!(call_index.load(Ordering::SeqCst), 1);
        tokio::time::advance(TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL).await;
        tokio::task::yield_now().await;
        assert_eq!(call_index.load(Ordering::SeqCst), 2);
        tokio::time::advance(TIER_FREE_VERSION_RECOVERY_BASE_INTERVAL).await;
        tokio::task::yield_now().await;
        assert_eq!(call_index.load(Ordering::SeqCst), 3);

        cancel.cancel();
        worker.await.expect("recovery loop should stop after cancellation");
        assert_eq!(
            calls
                .lock()
                .expect("recovery call log lock should not be poisoned")
                .as_slice(),
            &[
                (None, None),
                (Some("bucket".to_string()), Some("object".to_string())),
                (Some("bucket".to_string()), Some("object".to_string())),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tier_free_version_recovery_loop_cancels_active_sweep() {
        let cancel = CancellationToken::new();
        let state = ExpiryState::new();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut started_tx = Some(started_tx);
        let loop_cancel = cancel.clone();
        let worker = tokio::spawn(async move {
            run_tier_free_version_recovery_loop(loop_cancel, state, std::convert::identity, move |_, _, recovery_cancel| {
                started_tx
                    .take()
                    .expect("recovery should start only once")
                    .send(recovery_cancel.clone())
                    .expect("test should receive active recovery token");
                async move {
                    recovery_cancel.cancelled().await;
                    Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "cancelled").into())
                }
            })
            .await;
        });

        let active_recovery = started_rx.await.expect("recovery loop should start immediately");
        cancel.cancel();
        worker.await.expect("recovery loop should stop after cancellation");

        assert!(active_recovery.is_cancelled());
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn transitioned_get_reader_holds_tier_operation_lease_until_stream_finishes() {
        let manager = TierConfigMgr::new();
        let tier = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&manager, &tier).await;
        let remote_object = format!("remote/{}", Uuid::new_v4());
        let body = Bytes::from_static(b"transitioned object body");
        let remote_version = backend
            .put(
                &remote_object,
                ReaderImpl::Body(body.clone()),
                i64::try_from(body.len()).expect("body length should fit"),
            )
            .await
            .expect("mock remote object should be stored");
        let object_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: i64::try_from(body.len()).expect("body length should fit"),
            transitioned_object: TransitionedObject {
                name: remote_object,
                version_id: remote_version,
                status: crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string(),
                tier: tier.clone(),
                ..Default::default()
            },
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            ..Default::default()
        };

        let mut reader = get_transitioned_object_reader_with_tier_manager(
            &object_info.bucket,
            &object_info.name,
            &None,
            &HeaderMap::new(),
            &object_info,
            &ObjectOptions::default(),
            &manager,
            None,
        )
        .await
        .expect("transitioned reader should open");

        assert_eq!(
            TierConfigMgr::active_operation_lease_count(&manager, &tier).await,
            1,
            "returned reader must keep the tier generation leased"
        );

        let mut got = Vec::new();
        reader
            .stream
            .read_to_end(&mut got)
            .await
            .expect("transitioned reader should drain");
        assert_eq!(got, body.as_ref());
        assert_eq!(
            TierConfigMgr::active_operation_lease_count(&manager, &tier).await,
            0,
            "tier generation lease should release after EOF"
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn transitioned_get_rejects_nonempty_remote_version_before_backend_io() {
        let manager = TierConfigMgr::new();
        let tier = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&manager, &tier).await;
        let remote_object = format!("remote/{}", Uuid::new_v4());
        let body = Bytes::from_static(b"transitioned object body");
        let remote_version = backend
            .put(
                &remote_object,
                ReaderImpl::Body(body.clone()),
                i64::try_from(body.len()).expect("body length should fit"),
            )
            .await
            .expect("mock remote object should be stored");
        backend.set_reject_non_empty_remote_versions(true);
        let object_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: i64::try_from(body.len()).expect("body length should fit"),
            transitioned_object: TransitionedObject {
                name: remote_object,
                version_id: remote_version,
                status: crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string(),
                tier,
                ..Default::default()
            },
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            ..Default::default()
        };

        let err = match get_transitioned_object_reader_with_tier_manager(
            &object_info.bucket,
            &object_info.name,
            &None,
            &HeaderMap::new(),
            &object_info,
            &ObjectOptions::default(),
            &manager,
            None,
        )
        .await
        {
            Ok(_) => panic!("a provider that rejects versioned GET must fail before remote IO"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("requires an unversioned remote object"));
        assert_eq!(backend.get_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn transitioned_get_rejects_unknown_version_state_before_backend_io() {
        let manager = TierConfigMgr::new();
        let tier = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&manager, &tier).await;
        let object_info = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            size: 1,
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: String::new(),
                status: crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string(),
                tier,
                ..Default::default()
            },
            transition_version_state: rustfs_filemeta::TransitionVersionState::Unknown,
            ..Default::default()
        };

        let err = match get_transitioned_object_reader_with_tier_manager(
            &object_info.bucket,
            &object_info.name,
            &None,
            &HeaderMap::new(),
            &object_info,
            &ObjectOptions::default(),
            &manager,
            None,
        )
        .await
        {
            Ok(_) => panic!("unknown remote version state must fail before backend IO"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(backend.get_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn free_version_delete_rejects_unknown_version_state_before_backend_io() {
        let manager = TierConfigMgr::new();
        let backend = register_mock_tier(&manager, "WARM").await;
        let object_info = ObjectInfo {
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: "legacy-version".to_string(),
                tier: "WARM".to_string(),
                ..Default::default()
            },
            transition_version_state: rustfs_filemeta::TransitionVersionState::Unknown,
            ..Default::default()
        };

        let err = super::delete_free_version_remote_object(&object_info, &manager)
            .await
            .expect_err("unknown remote version state must fail before backend IO");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(backend.remove_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn free_version_remote_delete_requires_persisted_destination_identity() {
        let manager = crate::services::tier::tier::TierConfigMgr::new();
        let old_backend = crate::services::tier::test_util::register_mock_tier(&manager, "WARM").await;
        let mut conflicting_sys = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_bytes(
            &mut conflicting_sys,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_STATUS,
            crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.as_bytes().to_vec(),
        );
        conflicting_sys.insert(
            format!(
                "{}{}",
                rustfs_utils::http::metadata_compat::RUSTFS_INTERNAL_PREFIX,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID
            ),
            b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_vec(),
        );
        conflicting_sys.insert(
            format!(
                "{}{}",
                rustfs_utils::http::metadata_compat::MINIO_INTERNAL_PREFIX,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID
            ),
            b"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_vec(),
        );
        let conflicting_meta = rustfs_filemeta::MetaObject {
            meta_sys: conflicting_sys,
            ..Default::default()
        };
        let mut free_version_info = rustfs_filemeta::FileInfo::new("object", 2, 2);
        free_version_info.set_tier_free_version_id(&Uuid::new_v4().to_string());
        assert_eq!(
            conflicting_meta
                .init_free_version(&free_version_info)
                .expect_err("conflicting persisted identities must not create an executable free-version"),
            rustfs_filemeta::Error::FileCorrupt
        );
        assert_eq!(old_backend.remove_count().await, 0);

        let old_identity = crate::services::tier::tier::TierConfigMgr::acquire_operation_lease(&manager, "WARM")
            .await
            .expect("old tier lease should be available")
            .backend_identity();
        let mut oi = ObjectInfo::default();
        oi.transitioned_object.tier = "WARM".to_string();
        oi.transitioned_object.name = "remote/object".to_string();
        oi.transitioned_object.version_id = "remote-version".to_string();
        oi.transition_version_state = rustfs_filemeta::TransitionVersionState::Exact;
        let local_delete_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let legacy_err = delete_free_version_remote_object_then(&oi, &manager, {
            let local_delete_calls = Arc::clone(&local_delete_calls);
            move || async move {
                local_delete_calls.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await
        .expect_err("legacy free-version without identity must be retained");
        assert_eq!(legacy_err.kind(), std::io::ErrorKind::Other);
        assert_eq!(old_backend.remove_count().await, 0);
        assert_eq!(local_delete_calls.load(Ordering::Relaxed), 0);

        let mut invalid_metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut invalid_metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            "not-a-backend-identity".to_string(),
        );
        oi.user_defined = Arc::new(invalid_metadata);
        let invalid_err = delete_free_version_remote_object_then(&oi, &manager, {
            let local_delete_calls = Arc::clone(&local_delete_calls);
            move || async move {
                local_delete_calls.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await
        .expect_err("free-version with an invalid identity must be retained");
        assert!(invalid_err.to_string().contains("invalid length"));
        assert_eq!(local_delete_calls.load(Ordering::Relaxed), 0);

        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(old_identity),
        );
        oi.user_defined = Arc::new(metadata.clone());
        delete_free_version_remote_object_then(&oi, &manager, {
            let local_delete_calls = Arc::clone(&local_delete_calls);
            move || async move {
                local_delete_calls.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await
        .expect("matching destination identity should allow idempotent remote cleanup");
        assert_eq!(old_backend.remove_count().await, 1);
        assert_eq!(local_delete_calls.load(Ordering::Relaxed), 1);

        let mut single_prefix_metadata = HashMap::new();
        single_prefix_metadata.insert(
            format!(
                "{}{}",
                rustfs_utils::http::metadata_compat::MINIO_INTERNAL_PREFIX,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID
            ),
            rustfs_utils::crypto::hex(old_identity),
        );
        oi.user_defined = Arc::new(single_prefix_metadata);
        delete_free_version_remote_object_then(&oi, &manager, {
            let local_delete_calls = Arc::clone(&local_delete_calls);
            move || async move {
                local_delete_calls.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await
        .expect("single-prefix legacy identity should remain compatible");
        assert_eq!(old_backend.remove_count().await, 2);
        assert_eq!(local_delete_calls.load(Ordering::Relaxed), 2);

        let new_backend = crate::services::tier::test_util::register_mock_tier(&manager, "WARM").await;
        let new_identity = crate::services::tier::tier::TierConfigMgr::acquire_operation_lease(&manager, "WARM")
            .await
            .expect("rebound tier lease should be available")
            .backend_identity();

        let mut conflicting_metadata = HashMap::from([(
            rustfs_utils::http::metadata_compat::internal_key_rustfs(
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            ),
            rustfs_utils::crypto::hex(new_identity),
        )]);
        conflicting_metadata.insert(
            format!(
                "{}{}",
                rustfs_utils::http::metadata_compat::MINIO_INTERNAL_PREFIX,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID
            ),
            rustfs_utils::crypto::hex(old_identity),
        );
        oi.user_defined = Arc::new(conflicting_metadata);
        let conflict_err = delete_free_version_remote_object_then(&oi, &manager, {
            let local_delete_calls = Arc::clone(&local_delete_calls);
            move || async move {
                local_delete_calls.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await
        .expect_err("conflicting compatibility identities must retain the free-version");
        assert!(conflict_err.to_string().contains("compatibility keys conflict"));
        assert_eq!(new_backend.remove_count().await, 0);
        assert_eq!(local_delete_calls.load(Ordering::Relaxed), 2);

        oi.user_defined = Arc::new(metadata);
        let rebound_err = delete_free_version_remote_object_then(&oi, &manager, {
            let local_delete_calls = Arc::clone(&local_delete_calls);
            move || async move {
                local_delete_calls.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await
        .expect_err("same-name tier rebind must retain the old free-version");
        assert!(rebound_err.to_string().contains("identity no longer matches"));
        assert_eq!(new_backend.remove_count().await, 0);
        assert_eq!(local_delete_calls.load(Ordering::Relaxed), 2);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn transitioned_get_rejects_same_name_rebind_before_remote_io() {
        let manager = crate::services::tier::tier::TierConfigMgr::new();
        crate::services::tier::test_util::register_mock_tier(&manager, "WARM").await;
        let old_identity = crate::services::tier::tier::TierConfigMgr::acquire_operation_lease(&manager, "WARM")
            .await
            .expect("old tier lease should be available")
            .backend_identity();
        let new_backend = crate::services::tier::test_util::register_mock_tier(&manager, "WARM").await;

        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(old_identity),
        );
        let mut oi = ObjectInfo {
            user_defined: Arc::new(metadata),
            ..Default::default()
        };
        oi.transitioned_object.tier = "WARM".to_string();
        oi.transitioned_object.name = "remote/object".to_string();
        oi.transitioned_object.version_id = "remote-version".to_string();
        oi.transition_version_state = rustfs_filemeta::TransitionVersionState::Exact;

        let err = match get_transitioned_object_reader_with_tier_manager(
            "bucket",
            "object",
            &None,
            &http::HeaderMap::new(),
            &oi,
            &ObjectOptions::default(),
            &manager,
            None,
        )
        .await
        {
            Ok(_) => panic!("identity-bound GET must reject a same-name tier rebind"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        let admin_err = err
            .get_ref()
            .and_then(|source| source.downcast_ref::<crate::client::admin_handler_utils::AdminError>())
            .expect("identity mismatch should retain the typed tier error");
        assert_eq!(admin_err.code, crate::services::tier::tier::ERR_TIER_INVALID_CONFIG.code);
        assert_eq!(new_backend.get_count().await, 0);

        oi.user_defined = Arc::new(HashMap::new());
        let err = match get_transitioned_object_reader_with_tier_manager(
            "bucket",
            "object",
            &None,
            &http::HeaderMap::new(),
            &oi,
            &ObjectOptions::default(),
            &manager,
            None,
        )
        .await
        {
            Ok(_) => panic!("missing remote legacy object should return an error"),
            Err(err) => err,
        };
        assert!(!err.to_string().is_empty());
        assert_eq!(new_backend.get_count().await, 1);
    }

    /// Pins the expiry-event routing for transitioned objects
    /// (rustfs/backlog#1302): restore-expiry events must set
    /// `transition.expire_restored` (strip-restored-copy semantics, never a
    /// full delete), and versioned events must target the exact version.
    #[test]
    fn transitioned_object_delete_opts_routes_expiry_actions() {
        let vid = Uuid::new_v4();
        let vid_str = vid.to_string();
        let oi = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(vid),
            data_dir: Some(Uuid::new_v4()),
            etag: Some("etag".to_string()),
            restore_expires: Some(OffsetDateTime::now_utc() - StdDuration::from_secs(1)),
            transitioned_object: TransitionedObject {
                name: "remote-object".to_string(),
                tier: "tier".to_string(),
                status: TRANSITION_COMPLETE.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        // Plain version expiry: exact version, real delete.
        let incarnation = Uuid::new_v4();
        let opts = transitioned_object_delete_opts(&oi, IlmAction::DeleteVersionAction, true, false, incarnation)
            .expect("build version expiry options");
        assert_eq!(opts.version_id.as_deref(), Some(vid_str.as_str()));
        assert_eq!(opts.expected_bucket_incarnation_id, Some(incarnation));
        assert!(!opts.transition.expire_restored);
        assert!(opts.expiration.expire);

        // Restore-expiry of the latest version: restored-copy cleanup only.
        let opts = transitioned_object_delete_opts(&oi, IlmAction::DeleteRestoredAction, true, false, incarnation)
            .expect("build restored expiry options");
        assert_eq!(opts.version_id.as_deref(), Some(vid_str.as_str()));
        assert!(opts.transition.expire_restored);

        // Restore-expiry of a noncurrent version: restored-copy cleanup of the
        // exact version. Routing this through the full transitioned-object
        // delete instead would remove the remote tier data.
        let opts = transitioned_object_delete_opts(&oi, IlmAction::DeleteRestoredVersionAction, true, false, incarnation)
            .expect("build restored-version expiry options");
        assert_eq!(opts.version_id.as_deref(), Some(vid_str.as_str()));
        assert!(opts.transition.expire_restored);

        // Whole-object expiry stays a real delete.
        let opts = transitioned_object_delete_opts(&oi, IlmAction::DeleteAction, false, false, incarnation)
            .expect("build object expiry options");
        assert!(opts.version_id.is_none());
        assert!(!opts.transition.expire_restored);
    }

    #[tokio::test]
    #[serial]
    async fn expiry_enqueue_reports_missed_without_worker_channel() {
        let before = global_metrics().report().await.lifecycle_expiry;
        let observed = Arc::new(StdMutex::new(Vec::new()));
        let observer_events = Arc::clone(&observed);
        let _observer = set_lifecycle_observability_observer(move |event, state, reason| {
            observer_events
                .lock()
                .expect("observability test events should not poison")
                .push((event, state, reason));
        });
        let state = ExpiryState::new();
        let mut state = state.write().await;
        let object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::DeleteAction,
            ..Default::default()
        };

        let queued = state.enqueue_by_days(&object, &event, &LcEventSrc::Scanner, Uuid::new_v4());

        assert!(!queued);
        assert_eq!(state.stats.missed_tasks(), 1);
        let after = global_metrics().report().await.lifecycle_expiry;
        assert!(after.scanner_missed >= before.scanner_missed.saturating_add(1));
        assert!(after.scanner_not_enqueued >= before.scanner_not_enqueued.saturating_add(1));
        let observed = observed.lock().expect("observability test events should not poison");
        assert!(observed.contains(&(EVENT_LIFECYCLE_EXPIRED_DETECTED, "detected", None)));
        assert!(observed.contains(&(EVENT_LIFECYCLE_NOT_ENQUEUED, "not_enqueued", Some("worker_unavailable"))));
    }

    #[tokio::test]
    async fn enqueue_tier_journal_entry_reports_error_without_worker_channel() {
        let state = ExpiryState::new();
        let mut state = state.write().await;
        let je = Jentry {
            obj_name: "remote/object".to_string(),
            version_id: "remote-version".to_string(),
            tier_name: "WARM".to_string(),
            backend_identity: Some([1; 32]),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed,
            source: None,
        };

        let err = state
            .enqueue_tier_journal_entry(&je)
            .expect_err("missing worker should be reported to caller");

        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(state.stats.missed_tier_journal_tasks(), 1);
    }

    #[tokio::test]
    async fn enqueue_free_version_reports_false_without_worker_channel() {
        let state = ExpiryState::new();
        let recovery_notify = Arc::clone(&state.read().await.recovery_notify);
        let mut state = state.write().await;
        let oi = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: "remote-version".to_string(),
                tier: "WARM".to_string(),
                free_version: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let queued = state.enqueue_free_version(oi);

        assert!(!queued);
        assert_eq!(state.stats.missed_free_vers_tasks(), 1);
        assert!(recovery_notify.notified().now_or_never().is_some());
    }

    #[tokio::test]
    async fn enqueue_recovered_free_version_reports_false_without_worker_channel() {
        let state = ExpiryState::new();
        let oi = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: "remote-version".to_string(),
                tier: "WARM".to_string(),
                free_version: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let queued = enqueue_recovered_free_version_with_state(&state, oi).await;
        let state = state.read().await;

        assert!(!queued);
        assert_eq!(state.stats.missed_free_vers_tasks(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn expiry_enqueue_reports_missed_when_worker_queue_full() {
        let observed = Arc::new(StdMutex::new(Vec::new()));
        let observer_events = Arc::clone(&observed);
        let _observer = set_lifecycle_observability_observer(move |event, state, reason| {
            observer_events
                .lock()
                .expect("observability test events should not poison")
                .push((event, state, reason));
        });
        let state = ExpiryState::new_with_unconsumed_worker_channel(1);
        let mut state = state.write().await;
        let object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::DeleteAction,
            ..Default::default()
        };

        let incarnation = Uuid::new_v4();
        let first = state.enqueue_by_days(&object, &event, &LcEventSrc::Scanner, incarnation);
        let second = state.enqueue_by_days(&object, &event, &LcEventSrc::Scanner, incarnation);

        assert!(first);
        assert!(!second);
        assert_eq!(state.stats.pending_tasks(), 1);
        assert_eq!(state.stats.missed_tasks(), 1);
        let after = global_metrics().report().await.lifecycle_expiry;
        assert!(after.scanner_not_enqueued >= 1);
        let observed = observed.lock().expect("observability test events should not poison");
        assert_eq!(
            observed
                .iter()
                .filter(|(event, state, _)| *event == EVENT_LIFECYCLE_EXPIRED_DETECTED && *state == "detected")
                .count(),
            2
        );
        assert!(observed.contains(&(EVENT_LIFECYCLE_NOT_ENQUEUED, "not_enqueued", Some("queue_full"))));
    }

    #[tokio::test]
    async fn expiry_task_retains_enqueue_time_bucket_incarnation() {
        let state = ExpiryState::new_with_unconsumed_worker_channel(1);
        let incarnation = Uuid::new_v4();
        let object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::DeleteAction,
            ..Default::default()
        };
        {
            let mut state = state.write().await;
            assert!(state.enqueue_by_days(&object, &event, &LcEventSrc::Scanner, incarnation));
        }

        let receiver = state.read().await.tasks_rx[0].clone();
        let task = receiver
            .lock()
            .await
            .recv()
            .await
            .expect("expiry task should be queued")
            .expect("expiry task payload should be present");
        let task = task
            .as_any()
            .downcast_ref::<ExpiryTask>()
            .expect("queued payload should be an expiry task");
        assert_eq!(task.bucket_incarnation_id, incarnation);
    }

    #[tokio::test]
    async fn enqueue_tier_journal_entry_reports_error_when_worker_queue_full() {
        let state = ExpiryState::new_with_unconsumed_worker_channel(1);
        let mut state = state.write().await;
        let je = Jentry {
            obj_name: "remote/object".to_string(),
            version_id: "remote-version".to_string(),
            tier_name: "WARM".to_string(),
            backend_identity: Some([1; 32]),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed,
            source: None,
        };

        state
            .enqueue_tier_journal_entry(&je)
            .expect("first tier journal task should be queued");
        let err = state
            .enqueue_tier_journal_entry(&je)
            .expect_err("full worker queue should be reported to caller");

        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(state.stats.pending_tasks(), 1);
        assert_eq!(state.stats.missed_tier_journal_tasks(), 1);
    }

    #[tokio::test]
    async fn enqueue_recovered_free_version_reports_false_when_worker_queue_full() {
        let state = ExpiryState::new_with_unconsumed_worker_channel(1);
        let oi = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: "remote-version".to_string(),
                tier: "WARM".to_string(),
                free_version: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let first = enqueue_recovered_free_version_with_state(&state, oi.clone()).await;
        let second = enqueue_recovered_free_version_with_state(&state, oi).await;
        let state = state.read().await;

        assert!(first);
        assert!(!second);
        assert_eq!(state.stats.pending_tasks(), 1);
        assert_eq!(state.stats.missed_free_vers_tasks(), 1);
    }

    #[tokio::test]
    async fn enqueue_free_version_notifies_recovery_when_worker_queue_full() {
        let state = ExpiryState::new_with_unconsumed_worker_channel(1);
        let recovery_notify = Arc::clone(&state.read().await.recovery_notify);
        let oi = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: "remote-version".to_string(),
                tier: "WARM".to_string(),
                free_version: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut state = state.write().await;

        assert!(state.enqueue_free_version(oi.clone()));
        assert!(recovery_notify.notified().now_or_never().is_none());
        assert!(!state.enqueue_free_version(oi));
        assert_eq!(state.stats.pending_tasks(), 1);
        assert_eq!(state.stats.missed_free_vers_tasks(), 1);
        assert!(recovery_notify.notified().now_or_never().is_some());
    }

    #[tokio::test]
    #[serial]
    async fn free_version_worker_failure_notifies_recovery() {
        let (_paths, ecstore) = setup_test_env().await;
        let state = ExpiryState::new();
        let (stats, recovery_notify) = {
            let state = state.read().await;
            (Arc::clone(&state.stats), Arc::clone(&state.recovery_notify))
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let worker_stats = Arc::clone(&stats);
        let worker_notify = Arc::clone(&recovery_notify);
        let worker = tokio::spawn(async move {
            ExpiryState::worker(&mut rx, ecstore, worker_stats, worker_notify).await;
        });
        let oi = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: "remote-version".to_string(),
                tier: format!("missing-tier-{}", Uuid::new_v4()),
                free_version: true,
                ..Default::default()
            },
            ..Default::default()
        };

        stats.increment_pending_tasks();
        tx.send(Some(Box::new(FreeVersionTask(oi))))
            .await
            .expect("free-version task should reach the worker");
        tx.send(None).await.expect("worker stop signal should be delivered");
        worker.await.expect("free-version worker should stop cleanly");

        assert!(recovery_notify.notified().now_or_never().is_some());
    }

    #[tokio::test]
    #[serial]
    async fn resize_workers_wires_recovery_notify_to_worker_failures() {
        // A recovered free version without a persisted durable backend identity
        // fails closed during remote cleanup, which must wake recovery.
        let (_paths, ecstore) = setup_test_env().await;
        let runtime_state = reset_runtime_expiry_state(&ecstore).await;
        ExpiryState::resize_workers(1, Arc::clone(&ecstore)).await;
        let (recovery_notify, stop_tx) = {
            let state = runtime_state.read().await;
            assert_eq!(state.tasks_tx.len(), 1);
            (Arc::clone(&state.recovery_notify), state.tasks_tx[0].clone())
        };
        let oi = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: "remote-version".to_string(),
                tier: "WARM".to_string(),
                free_version: true,
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(
            super::enqueue_recovered_free_version(oi).await,
            "the resized production worker queue should accept the task"
        );
        stop_tx.send(None).await.expect("worker stop signal should be delivered");
        if tokio::time::timeout(StdDuration::from_secs(30), recovery_notify.notified())
            .await
            .is_err()
        {
            let state = runtime_state.read().await;
            panic!(
                "worker failure did not wake recovery: pending={}, active={}, workers={}",
                state.stats.pending_tasks(),
                state.stats.active_tasks(),
                state.stats.num_workers()
            );
        }

        reset_runtime_expiry_state(&ecstore).await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial]
    async fn free_version_worker_success_does_not_notify_recovery() {
        let (disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-worker-success-{}", Uuid::new_v4());
        let object = "free-version/";
        let local_object = encode_dir_object(object);
        create_test_bucket(&ecstore, &bucket).await;
        let (_backend, identity_hex) = register_recovery_mock_tier(&ecstore).await;
        seed_recoverable_free_version(&disk_paths, &bucket, &local_object, None, Some(identity_hex)).await;
        let page = list_tier_free_versions(Arc::clone(&ecstore), 1, None, None, CancellationToken::new())
            .await
            .expect("seeded free version should be listed");
        let oi = page
            .items
            .into_iter()
            .next()
            .expect("seeded free version should be recoverable");
        assert_eq!(oi.name, object);

        let state = ExpiryState::new();
        let (stats, recovery_notify) = {
            let state = state.read().await;
            (Arc::clone(&state.stats), Arc::clone(&state.recovery_notify))
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let worker_stats = Arc::clone(&stats);
        let worker_notify = Arc::clone(&recovery_notify);
        let worker = tokio::spawn(async move {
            ExpiryState::worker(&mut rx, ecstore, worker_stats, worker_notify).await;
        });

        stats.increment_pending_tasks();
        tx.send(Some(Box::new(FreeVersionTask(oi))))
            .await
            .expect("free-version task should reach the worker");
        tx.send(None).await.expect("worker stop signal should be delivered");
        worker.await.expect("free-version worker should stop cleanly");

        assert!(recovery_notify.notified().now_or_never().is_none());
        for disk_path in &disk_paths {
            assert!(
                !fs::try_exists(disk_path.join(&bucket).join(&local_object))
                    .await
                    .expect("free-version path existence check should succeed")
            );
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial]
    async fn free_version_worker_serializes_cleanup_with_object_writes() {
        let (disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-worker-lock-{}", Uuid::new_v4());
        let object = "free-version";
        create_test_bucket(&ecstore, &bucket).await;
        let (remote_backend, identity_hex) = register_recovery_mock_tier(&ecstore).await;
        seed_recoverable_free_version(&disk_paths, &bucket, object, None, Some(identity_hex)).await;
        let page = list_tier_free_versions(Arc::clone(&ecstore), 1, None, None, CancellationToken::new())
            .await
            .expect("seeded free version should be listed");
        let oi = page
            .items
            .into_iter()
            .next()
            .expect("seeded free version should be recoverable");
        let target_set = ecstore.pools[0].get_disks_by_key(object);
        let ns_lock = target_set
            .new_ns_lock(&bucket, object)
            .await
            .expect("target erasure-set object namespace lock should be created");
        let object_lock_guard = ns_lock
            .get_write_lock(StdDuration::from_secs(1))
            .await
            .expect("competing object writer lock should be acquired");

        let state = ExpiryState::new();
        let (stats, recovery_notify) = {
            let state = state.read().await;
            (Arc::clone(&state.stats), Arc::clone(&state.recovery_notify))
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let worker_stats = Arc::clone(&stats);
        let worker_notify = Arc::clone(&recovery_notify);
        let worker_store = Arc::clone(&ecstore);
        let worker = tokio::spawn(async move {
            ExpiryState::worker(&mut rx, worker_store, worker_stats, worker_notify).await;
        });

        stats.increment_pending_tasks();
        tx.send(Some(Box::new(FreeVersionTask(oi))))
            .await
            .expect("free-version task should reach the worker");
        tokio::time::timeout(StdDuration::from_secs(30), async {
            while remote_backend.remove_count().await == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker should complete remote cleanup before taking the local lock");
        let completed_while_locked = tokio::time::timeout(StdDuration::from_millis(100), async {
            while stats.active_tasks() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            completed_while_locked.is_err(),
            "local cleanup must wait while a competing object writer owns the namespace lock"
        );
        for disk_path in &disk_paths {
            assert!(
                fs::try_exists(disk_path.join(&bucket).join(object).join(STORAGE_FORMAT_FILE))
                    .await
                    .expect("free-version metadata existence check should succeed")
            );
        }

        drop(object_lock_guard);
        tx.send(None).await.expect("worker stop signal should be delivered");
        worker.await.expect("free-version worker should stop cleanly");

        assert!(recovery_notify.notified().now_or_never().is_none());
        for disk_path in &disk_paths {
            assert!(
                !fs::try_exists(disk_path.join(&bucket).join(object))
                    .await
                    .expect("free-version path existence check should succeed")
            );
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial]
    async fn free_version_worker_duplicate_does_not_create_delete_marker_or_notify() {
        let (disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-worker-idempotent-{}", Uuid::new_v4());
        let object = "already-removed";
        let retained_version = Uuid::new_v4();
        create_test_bucket(&ecstore, &bucket).await;
        let (_backend, identity_hex) = register_recovery_mock_tier(&ecstore).await;
        seed_recoverable_free_version(&disk_paths, &bucket, object, Some(retained_version), Some(identity_hex)).await;
        let page = list_tier_free_versions(Arc::clone(&ecstore), 1, None, None, CancellationToken::new())
            .await
            .expect("seeded free version should be listed");
        let oi = page
            .items
            .into_iter()
            .next()
            .expect("seeded free version should be recoverable");
        let state = ExpiryState::new();
        let (stats, recovery_notify) = {
            let state = state.read().await;
            (Arc::clone(&state.stats), Arc::clone(&state.recovery_notify))
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let worker_stats = Arc::clone(&stats);
        let worker_notify = Arc::clone(&recovery_notify);
        let worker_store = Arc::clone(&ecstore);
        let worker = tokio::spawn(async move {
            ExpiryState::worker(&mut rx, worker_store, worker_stats, worker_notify).await;
        });

        for _ in 0..2 {
            stats.increment_pending_tasks();
            tx.send(Some(Box::new(FreeVersionTask(oi.clone()))))
                .await
                .expect("duplicate free-version task should reach the worker");
        }
        tx.send(None).await.expect("worker stop signal should be delivered");
        worker.await.expect("free-version worker should stop cleanly");

        assert!(recovery_notify.notified().now_or_never().is_none());
        for disk_path in &disk_paths {
            let encoded = fs::read(disk_path.join(&bucket).join(object).join(STORAGE_FORMAT_FILE))
                .await
                .expect("retained object metadata should remain readable");
            let metadata = FileMeta::load(&encoded).expect("retained object metadata should decode");
            let versions = metadata
                .get_file_info_versions(&bucket, object, true)
                .expect("retained object versions should parse");
            assert_eq!(versions.versions.len(), 1);
            assert_eq!(versions.versions[0].version_id, Some(retained_version));
            assert!(versions.versions[0].deleted);
            assert!(!versions.versions[0].tier_free_version());
        }

        remove_seeded_free_version(&disk_paths, &bucket, object).await;
        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial]
    async fn free_version_worker_stale_task_does_not_delete_same_id_marker() {
        let (disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-worker-stale-{}", Uuid::new_v4());
        let object = "same-id-marker";
        create_test_bucket(&ecstore, &bucket).await;
        let (_backend, identity_hex) = register_recovery_mock_tier(&ecstore).await;
        seed_recoverable_free_version(&disk_paths, &bucket, object, None, Some(identity_hex)).await;
        let page = list_tier_free_versions(Arc::clone(&ecstore), 1, None, None, CancellationToken::new())
            .await
            .expect("seeded free version should be listed");
        let oi = page
            .items
            .into_iter()
            .next()
            .expect("seeded free version should be recoverable");
        let stale_version_id = oi.version_id.expect("free version should have a concrete UUID");

        for disk_path in &disk_paths {
            let metadata_path = disk_path.join(&bucket).join(object).join(STORAGE_FORMAT_FILE);
            let encoded = fs::read(&metadata_path)
                .await
                .expect("free-version metadata should remain readable");
            let mut metadata = FileMeta::load(&encoded).expect("free-version metadata should decode");
            metadata
                .add_version(FileInfo {
                    volume: bucket.clone(),
                    name: object.to_string(),
                    version_id: Some(stale_version_id),
                    deleted: true,
                    mod_time: Some(OffsetDateTime::now_utc()),
                    ..Default::default()
                })
                .expect("same-ID ordinary marker should replace the stale free version");
            fs::write(
                &metadata_path,
                metadata
                    .marshal_msg()
                    .expect("same-ID ordinary marker metadata should encode"),
            )
            .await
            .expect("same-ID ordinary marker metadata should be written");
        }

        let state = ExpiryState::new();
        let (stats, recovery_notify) = {
            let state = state.read().await;
            (Arc::clone(&state.stats), Arc::clone(&state.recovery_notify))
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let worker_stats = Arc::clone(&stats);
        let worker_notify = Arc::clone(&recovery_notify);
        let worker_store = Arc::clone(&ecstore);
        let worker = tokio::spawn(async move {
            ExpiryState::worker(&mut rx, worker_store, worker_stats, worker_notify).await;
        });

        stats.increment_pending_tasks();
        tx.send(Some(Box::new(FreeVersionTask(oi))))
            .await
            .expect("stale free-version task should reach the worker");
        tx.send(None).await.expect("worker stop signal should be delivered");
        worker.await.expect("free-version worker should stop cleanly");

        assert!(recovery_notify.notified().now_or_never().is_none());
        for disk_path in &disk_paths {
            let encoded = fs::read(disk_path.join(&bucket).join(object).join(STORAGE_FORMAT_FILE))
                .await
                .expect("same-ID ordinary marker should remain readable");
            let metadata = FileMeta::load(&encoded).expect("same-ID ordinary marker metadata should decode");
            let versions = metadata
                .get_file_info_versions(&bucket, object, true)
                .expect("same-ID ordinary marker should parse");
            assert_eq!(versions.versions.len(), 1);
            assert_eq!(versions.versions[0].version_id, Some(stale_version_id));
            assert!(versions.versions[0].deleted);
            assert!(!versions.versions[0].tier_free_version());
        }

        remove_seeded_free_version(&disk_paths, &bucket, object).await;
        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial]
    async fn free_version_worker_local_cleanup_failure_notifies_recovery() {
        let (_paths, ecstore) = setup_test_env().await;
        let (_backend, identity_hex) = register_recovery_mock_tier(&ecstore).await;
        let mut user_defined = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut user_defined,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            identity_hex,
        );
        let state = ExpiryState::new();
        let (stats, recovery_notify) = {
            let state = state.read().await;
            (Arc::clone(&state.stats), Arc::clone(&state.recovery_notify))
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let worker_stats = Arc::clone(&stats);
        let worker_notify = Arc::clone(&recovery_notify);
        let worker = tokio::spawn(async move {
            ExpiryState::worker(&mut rx, ecstore, worker_stats, worker_notify).await;
        });
        let oi = ObjectInfo {
            bucket: format!("missing-bucket-{}", Uuid::new_v4()),
            name: "object".to_string(),
            transitioned_object: TransitionedObject {
                name: "remote/object".to_string(),
                version_id: "remote-version".to_string(),
                tier: "WARM".to_string(),
                free_version: true,
                ..Default::default()
            },
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        stats.increment_pending_tasks();
        tx.send(Some(Box::new(FreeVersionTask(oi))))
            .await
            .expect("free-version task should reach the worker");
        tx.send(None).await.expect("worker stop signal should be delivered");
        worker.await.expect("free-version worker should stop cleanly");

        assert!(recovery_notify.notified().now_or_never().is_some());
    }

    #[tokio::test]
    #[serial]
    async fn scanner_transition_enqueue_reports_full_queue() {
        let state = TransitionState::new_with_capacity(1);
        let object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            ..Default::default()
        };

        // A distinct object fills past the capacity-1 queue and is reported as a
        // missed enqueue. (Re-enqueuing the same object would instead be deduped;
        // see transition_reserve_dedupes_same_object_version.)
        let other = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object-2".to_string(),
            ..Default::default()
        };
        let first = state.queue_transition_task(&object, &event, &LcEventSrc::Scanner).await;
        let second = state.queue_transition_task(&other, &event, &LcEventSrc::Scanner).await;

        assert!(first);
        assert!(!second);
        assert_eq!(state.transition_rx.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn scanner_transition_enqueue_waits_for_saturated_queue_to_recover() {
        let state = TransitionState::new_with_capacity(1);
        let first_object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "first".to_string(),
            ..Default::default()
        };
        let deferred_object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "deferred".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            ..Default::default()
        };

        assert!(
            state.queue_transition_task(&first_object, &event, &LcEventSrc::Scanner).await,
            "first scanner transition should fill the queue"
        );

        let deferred = state.queue_transition_task(&deferred_object, &event, &LcEventSrc::Scanner);
        tokio::pin!(deferred);
        assert!(
            (&mut deferred).now_or_never().is_none(),
            "a saturated scanner queue should apply bounded backpressure instead of dropping the task"
        );

        let first_task = state
            .transition_rx
            .recv()
            .await
            .expect("queue should remain open")
            .expect("first queued transition task should be present");
        state.release_transition(&first_task.obj_info);

        assert!(
            deferred.await,
            "the deferred scanner transition should enqueue as soon as capacity recovers"
        );
        let recovered_task = state
            .transition_rx
            .recv()
            .await
            .expect("queue should remain open")
            .expect("deferred transition task should be present");
        assert_eq!(recovered_task.obj_info.name, deferred_object.name);
    }

    #[tokio::test]
    #[serial]
    async fn scanner_transition_sustained_saturation_schedules_compensation() {
        let state = TransitionState::new_with_capacity_and_timeout(1, StdDuration::ZERO);
        let first_object = ObjectInfo {
            bucket: "saturated-bucket".to_string(),
            name: "first".to_string(),
            ..Default::default()
        };
        let missed_object = ObjectInfo {
            bucket: "saturated-bucket".to_string(),
            name: "missed".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            ..Default::default()
        };

        assert!(
            state.queue_transition_task(&first_object, &event, &LcEventSrc::Scanner).await,
            "first scanner transition should fill the queue"
        );
        assert!(
            !state
                .queue_transition_task(&missed_object, &event, &LcEventSrc::Scanner)
                .await,
            "a continuously saturated queue should report that the object was not admitted"
        );
        assert_eq!(state.queue_full_tasks(), 1);
        assert_eq!(state.queue_send_timeout_tasks(), 1);
        assert_eq!(
            state.compensation_scheduled_tasks(),
            1,
            "timed-out scanner work must schedule a bounded bucket backfill"
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_transition_enqueue_updates_transition_status() {
        let before = global_metrics().report().await.lifecycle_transition;
        let state = TransitionState::new_with_capacity(1);
        let object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            ..Default::default()
        };

        let other = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object-2".to_string(),
            ..Default::default()
        };
        let first = state.queue_transition_task(&object, &event, &LcEventSrc::Scanner).await;
        let second = state.queue_transition_task(&other, &event, &LcEventSrc::Scanner).await;

        assert!(first);
        assert!(!second);
        let after = global_metrics().report().await.lifecycle_transition;
        assert_eq!(after.scanner_queued.saturating_sub(before.scanner_queued), 1);
        assert_eq!(after.scanner_missed.saturating_sub(before.scanner_missed), 1);
        assert_eq!(after.queue_full, 1);
        assert_eq!(after.current_queue_capacity, 1);
        assert_eq!(after.current_queued, 1);
        assert_eq!(after.current_active, 0);
    }

    #[test]
    fn mark_delete_opts_skip_decommissioned_on_remote_success_sets_flag_on_success() {
        let mut opts = ObjectOptions::default();

        mark_delete_opts_skip_decommissioned_on_remote_success(&mut opts, true);

        assert!(opts.skip_decommissioned);
    }

    #[test]
    fn transitioned_expiry_must_not_skip_free_version_before_remote_cleanup() {
        let mut opts = ObjectOptions::default();

        mark_delete_opts_skip_decommissioned_on_remote_success(&mut opts, false);

        assert!(!opts.skip_decommissioned);
        assert!(!opts.skip_free_version);
    }

    #[test]
    fn transitioned_cleanup_tuple_preserves_versioned_remote() {
        let mut oi = ObjectInfo::default();
        oi.transitioned_object.status = crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string();
        oi.transitioned_object.name = "remote/object".to_string();
        oi.transitioned_object.version_id = "remote-version".to_string();
        oi.transitioned_object.tier = "WARM".to_string();

        let tuple = transitioned_cleanup_tuple(&oi).expect("complete tuple should be accepted");

        assert_eq!(tuple, ("remote/object", "remote-version", "WARM"));
    }

    #[test]
    fn transitioned_cleanup_tuple_accepts_unversioned_remote() {
        let mut oi = ObjectInfo::default();
        oi.transitioned_object.status = crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string();
        oi.transitioned_object.name = "remote/object".to_string();
        oi.transitioned_object.tier = "WARM".to_string();

        let tuple = transitioned_cleanup_tuple(&oi).expect("an empty remote version identifies an unversioned tier bucket");

        assert_eq!(tuple, ("remote/object", "", "WARM"));
    }

    #[test]
    fn transitioned_cleanup_tuple_rejects_missing_remote_name_or_tier() {
        for (name, tier) in [("", "WARM"), ("remote/object", "")] {
            let mut oi = ObjectInfo::default();
            oi.transitioned_object.status = crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string();
            oi.transitioned_object.name = name.to_string();
            oi.transitioned_object.tier = tier.to_string();

            let err = transitioned_cleanup_tuple(&oi).expect_err("remote name and tier must remain required");

            assert!(err.to_string().contains("cleanup tuple is incomplete"));
        }
    }

    #[test]
    fn transitioned_cleanup_tuple_rejects_non_complete_status() {
        let mut oi = ObjectInfo::default();
        oi.transitioned_object.name = "remote/object".to_string();
        oi.transitioned_object.version_id = "remote-version".to_string();
        oi.transitioned_object.tier = "WARM".to_string();
        oi.transitioned_object.status = "pending".to_string();

        let err = transitioned_cleanup_tuple(&oi).expect_err("non-complete transition must be rejected");

        assert!(err.to_string().contains("not complete"));
    }

    #[test]
    fn transitioned_expiry_sets_version_suspended_in_delete_options() {
        let opts = ObjectOptions {
            versioned: true,
            version_suspended: true,
            expiration: ExpirationOptions { expire: true },
            ..Default::default()
        };

        assert!(opts.versioned);
        assert!(opts.version_suspended);
        assert!(!opts.skip_free_version);
    }

    #[test]
    fn delete_marker_result_must_not_drive_remote_cleanup() {
        let dobj = ObjectInfo {
            delete_marker: true,
            transitioned_object: TransitionedObject::default(),
            ..Default::default()
        };

        assert!(dobj.delete_marker);
        assert!(dobj.transitioned_object.name.is_empty());
    }

    fn select_restore_request(output_location: Option<OutputLocation>) -> RestoreRequest {
        RestoreRequest {
            days: None,
            description: None,
            glacier_job_parameters: None,
            output_location,
            select_parameters: None,
            tier: None,
            type_: Some(RestoreRequestType::from_static(RestoreRequestType::SELECT)),
        }
    }

    #[test]
    fn select_restore_s3_location_rejects_missing_s3_output_location() {
        let request = select_restore_request(Some(OutputLocation { s3: None }));

        let err = select_restore_s3_location(&request).expect_err("missing S3 location should be rejected");

        assert!(err.to_string().contains("OutputLocation.S3 required"));
    }

    #[test]
    fn select_restore_s3_location_rejects_missing_metadata_name() {
        let request = select_restore_request(Some(OutputLocation {
            s3: Some(S3Location {
                user_metadata: Some(vec![MetadataEntry {
                    name: None,
                    value: Some("value".to_string()),
                }]),
                ..Default::default()
            }),
        }));

        let err = select_restore_s3_location(&request).expect_err("metadata without name should be rejected");

        assert!(err.to_string().contains("metadata name is required"));
    }

    #[test]
    fn select_restore_s3_location_allows_missing_user_metadata() {
        let request = select_restore_request(Some(OutputLocation {
            s3: Some(S3Location::default()),
        }));

        let s3 = select_restore_s3_location(&request)
            .expect("missing user metadata should be allowed")
            .expect("S3 location should be present");

        assert!(s3.user_metadata.is_none());
    }

    // SAFETY: this helper is only used from `#[serial]` tests and those tests run under a
    // single-thread runtime (`worker_threads = 1`), so no concurrent reader/writer can access
    // process environment while `env::set_var`/`env::remove_var` is active.
    #[allow(unsafe_code)]
    fn with_transition_worker_env<F>(transition: Option<&str>, absolute: Option<&str>, test_fn: F)
    where
        F: FnOnce(),
    {
        let original_transition = env::var_os("RUSTFS_MAX_TRANSITION_WORKERS");
        let original_absolute = env::var_os(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX);

        match transition {
            Some(value) => unsafe {
                env::set_var("RUSTFS_MAX_TRANSITION_WORKERS", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_MAX_TRANSITION_WORKERS");
            },
        }
        match absolute {
            Some(value) => unsafe {
                env::set_var(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX, value);
            },
            None => unsafe {
                env::remove_var(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX);
            },
        }

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test_fn));

        match original_transition {
            Some(value) => unsafe {
                env::set_var("RUSTFS_MAX_TRANSITION_WORKERS", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_MAX_TRANSITION_WORKERS");
            },
        }
        match original_absolute {
            Some(value) => unsafe {
                env::set_var(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX, value);
            },
            None => unsafe {
                env::remove_var(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX);
            },
        }

        if let Err(e) = result {
            std::panic::resume_unwind(e);
        }
    }

    // SAFETY: same contract as with_transition_worker_env — only used from
    // `#[serial]` tests, so no concurrent reader/writer can access the process
    // environment while `env::set_var`/`env::remove_var` is active.
    #[allow(unsafe_code)]
    fn with_expiry_worker_env<F>(value: Option<&str>, test_fn: F)
    where
        F: FnOnce(),
    {
        let original = env::var_os(ENV_MAX_EXPIRY_WORKERS);

        match value {
            Some(v) => unsafe {
                env::set_var(ENV_MAX_EXPIRY_WORKERS, v);
            },
            None => unsafe {
                env::remove_var(ENV_MAX_EXPIRY_WORKERS);
            },
        }

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test_fn));

        match original {
            Some(v) => unsafe {
                env::set_var(ENV_MAX_EXPIRY_WORKERS, v);
            },
            None => unsafe {
                env::remove_var(ENV_MAX_EXPIRY_WORKERS);
            },
        }

        if let Err(e) = result {
            std::panic::resume_unwind(e);
        }
    }

    /// backlog#1832: the single expiry knob must resolve all four env states
    /// (unset / zero / valid / garbage); the removed `_RUSTFS_ILM_EXPIRATION_WORKERS`
    /// override and `RUSTFS_DEFAULT_EXPIRY_WORKERS` fallback must stay gone.
    #[test]
    #[serial]
    fn expiry_worker_count_resolves_all_env_states() {
        let default = std::cmp::min(num_cpus::get(), 16);

        with_expiry_worker_env(None, || {
            assert_eq!(expiry_worker_count(), default, "unset env must fall back to min(cpus, 16)");
        });
        with_expiry_worker_env(Some("0"), || {
            assert_eq!(expiry_worker_count(), default, "zero must fall back instead of spawning zero workers");
        });
        with_expiry_worker_env(Some("4"), || {
            assert_eq!(expiry_worker_count(), 4, "a valid positive value must win");
        });
        with_expiry_worker_env(Some("not-a-number"), || {
            assert_eq!(expiry_worker_count(), default, "garbage must fall back to the default");
        });
    }

    // SAFETY: this helper is only used from `#[serial]` tests and those tests run under a
    // single-thread runtime (`worker_threads = 1`), so no concurrent reader/writer can access
    // process environment while `env::set_var`/`env::remove_var` is active.
    #[allow(unsafe_code)]
    async fn with_transition_worker_env_async<F, Fut>(transition: Option<&str>, absolute: Option<&str>, test_fn: F)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let original_transition = env::var_os("RUSTFS_MAX_TRANSITION_WORKERS");
        let original_absolute = env::var_os(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX);

        match transition {
            Some(value) => unsafe {
                env::set_var("RUSTFS_MAX_TRANSITION_WORKERS", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_MAX_TRANSITION_WORKERS");
            },
        }
        match absolute {
            Some(value) => unsafe {
                env::set_var(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX, value);
            },
            None => unsafe {
                env::remove_var(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX);
            },
        }

        let result = std::panic::AssertUnwindSafe(test_fn()).catch_unwind().await;

        match original_transition {
            Some(value) => unsafe {
                env::set_var("RUSTFS_MAX_TRANSITION_WORKERS", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_MAX_TRANSITION_WORKERS");
            },
        }
        match original_absolute {
            Some(value) => unsafe {
                env::set_var(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX, value);
            },
            None => unsafe {
                env::remove_var(ENV_TRANSITION_WORKERS_ABSOLUTE_MAX);
            },
        }

        if let Err(e) = result {
            std::panic::resume_unwind(e);
        }
    }

    // SAFETY: this helper is only used from `#[serial]` tests and those tests run under a
    // single-thread runtime (`worker_threads = 1`), so no concurrent reader/writer can access
    // process environment while `env::set_var`/`env::remove_var` is active.
    #[allow(unsafe_code)]
    fn with_transition_queue_env<F>(capacity: Option<&str>, timeout_ms: Option<&str>, test_fn: F)
    where
        F: FnOnce(),
    {
        let original_capacity = env::var_os("RUSTFS_TRANSITION_QUEUE_CAPACITY");
        let original_timeout = env::var_os("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS");

        match capacity {
            Some(value) => unsafe {
                env::set_var("RUSTFS_TRANSITION_QUEUE_CAPACITY", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_TRANSITION_QUEUE_CAPACITY");
            },
        }
        match timeout_ms {
            Some(value) => unsafe {
                env::set_var("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS");
            },
        }

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(test_fn));

        match original_capacity {
            Some(value) => unsafe {
                env::set_var("RUSTFS_TRANSITION_QUEUE_CAPACITY", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_TRANSITION_QUEUE_CAPACITY");
            },
        }
        match original_timeout {
            Some(value) => unsafe {
                env::set_var("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS");
            },
        }

        if let Err(e) = result {
            std::panic::resume_unwind(e);
        }
    }

    // SAFETY: this helper is only used from `#[serial]` tests and those tests run under a
    // single-thread runtime (`worker_threads = 1`), so no concurrent reader/writer can access
    // process environment while `env::set_var`/`env::remove_var` is active.
    // SAFETY: keep this note adjacent to the allowance for the repository guard.
    #[allow(unsafe_code)]
    #[allow(
        dead_code,
        reason = "transition-queue env fixture kept for tests that scope those vars; no test uses it today (backlog#1823)"
    )]
    async fn with_transition_queue_env_async<F, Fut>(capacity: Option<&str>, timeout_ms: Option<&str>, test_fn: F)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let original_capacity = env::var_os("RUSTFS_TRANSITION_QUEUE_CAPACITY");
        let original_timeout = env::var_os("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS");

        match capacity {
            Some(value) => unsafe {
                env::set_var("RUSTFS_TRANSITION_QUEUE_CAPACITY", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_TRANSITION_QUEUE_CAPACITY");
            },
        }
        match timeout_ms {
            Some(value) => unsafe {
                env::set_var("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS");
            },
        }

        let result = std::panic::AssertUnwindSafe(test_fn()).catch_unwind().await;

        match original_capacity {
            Some(value) => unsafe {
                env::set_var("RUSTFS_TRANSITION_QUEUE_CAPACITY", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_TRANSITION_QUEUE_CAPACITY");
            },
        }
        match original_timeout {
            Some(value) => unsafe {
                env::set_var("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS", value);
            },
            None => unsafe {
                env::remove_var("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS");
            },
        }

        if let Err(e) = result {
            std::panic::resume_unwind(e);
        }
    }

    #[test]
    fn lifecycle_rule_has_date_expiration_detects_enabled_date_rule() {
        let lc = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    date: Some(Timestamp::from(OffsetDateTime::now_utc())),
                    ..Default::default()
                }),
                id: Some("rule-date".to_string()),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        };

        assert!(lifecycle_rule_has_date_expiration(&lc, "rule-date"));
        assert!(!lifecycle_rule_has_date_expiration(&lc, "missing-rule"));
    }

    #[test]
    #[serial]
    fn resolve_transition_worker_count_uses_fallback_when_env_missing() {
        with_transition_worker_env(None, None, || {
            let (configured, absolute_max, effective) = resolve_transition_worker_count();

            let fallback = std::cmp::min(num_cpus::get() as i64, DEFAULT_TRANSITION_WORKERS_CAP);
            assert_eq!(configured, fallback);
            assert_eq!(absolute_max, DEFAULT_TRANSITION_WORKERS_ABSOLUTE_MAX);
            assert_eq!(effective, fallback);
        });
    }

    #[test]
    #[serial]
    fn resolve_transition_worker_count_honors_positive_env_value() {
        with_transition_worker_env(Some("4"), Some("32"), || {
            let (configured, absolute_max, effective) = resolve_transition_worker_count();

            assert_eq!(configured, 4);
            assert_eq!(absolute_max, 32);
            assert_eq!(effective, 4);
        });
    }

    #[test]
    #[serial]
    fn resolve_transition_worker_count_clamps_to_absolute_max() {
        with_transition_worker_env(Some("64"), Some("16"), || {
            let (configured, absolute_max, effective) = resolve_transition_worker_count();

            assert_eq!(configured, 64);
            assert_eq!(absolute_max, 16);
            assert_eq!(effective, 16);
        });
    }

    #[test]
    #[serial]
    fn resolve_transition_worker_count_ignores_non_positive_absolute_max() {
        with_transition_worker_env(Some("4"), Some("0"), || {
            let (configured, absolute_max, effective) = resolve_transition_worker_count();

            assert_eq!(configured, 4);
            assert_eq!(absolute_max, DEFAULT_TRANSITION_WORKERS_ABSOLUTE_MAX);
            assert_eq!(effective, 4);
        });

        with_transition_worker_env(Some("4"), Some("-1"), || {
            let (configured, absolute_max, effective) = resolve_transition_worker_count();

            assert_eq!(configured, 4);
            assert_eq!(absolute_max, DEFAULT_TRANSITION_WORKERS_ABSOLUTE_MAX);
            assert_eq!(effective, 4);
        });
    }

    #[test]
    #[serial]
    fn resolve_transition_worker_count_falls_back_for_zero_value() {
        with_transition_worker_env(Some("0"), Some("32"), || {
            let (configured, absolute_max, effective) = resolve_transition_worker_count();

            let fallback = std::cmp::min(num_cpus::get() as i64, DEFAULT_TRANSITION_WORKERS_CAP);
            assert_eq!(configured, fallback);
            assert_eq!(absolute_max, 32);
            assert_eq!(effective, fallback);
        });
    }

    #[test]
    #[serial]
    fn resolve_transition_queue_capacity_uses_default_when_env_missing() {
        with_transition_queue_env(None, None, || {
            assert_eq!(resolve_transition_queue_capacity(), DEFAULT_TRANSITION_QUEUE_CAPACITY);
        });
    }

    #[test]
    #[serial]
    fn resolve_transition_queue_capacity_honors_positive_env_value() {
        with_transition_queue_env(Some("128"), None, || {
            assert_eq!(resolve_transition_queue_capacity(), 128);
        });
    }

    #[test]
    #[serial]
    fn resolve_transition_queue_send_timeout_honors_positive_env_value() {
        with_transition_queue_env(None, Some("250"), || {
            assert_eq!(resolve_transition_queue_send_timeout(), StdDuration::from_millis(250));
        });
    }

    #[test]
    fn reserve_bucket_compensation_deduplicates_same_bucket() {
        let state = TransitionState::new_with_capacity(1);

        let first = state.reserve_bucket_compensation("bucket-a");
        let second = state.reserve_bucket_compensation("bucket-a");

        assert!(first);
        assert!(!second);
        assert_eq!(state.compensation_scheduled_tasks(), 1);
        assert_eq!(state.compensation_pending_tasks(), 1);
    }

    #[test]
    fn poisoned_compensation_set_can_release_completed_bucket() {
        let state = TransitionState::new_with_capacity(1);
        state
            .compensation_buckets
            .lock()
            .expect("fresh mutex should lock")
            .insert("bucket-a".to_string());
        let poison_target = Arc::clone(&state.compensation_buckets);
        let _ = std::thread::spawn(move || {
            let _guard = poison_target.lock().expect("fresh mutex should lock");
            panic!("poison compensation set");
        })
        .join();

        state.finish_bucket_compensation("bucket-a");
        assert_eq!(state.compensation_pending_tasks(), 0);
        assert!(
            state.compensation_buckets.lock().is_ok(),
            "validated compensation state must clear poison"
        );
    }

    #[test]
    fn poisoned_tier_stats_are_reset_before_reuse() {
        let state = TransitionState::new_with_capacity(1);
        let poison_target = Arc::clone(&state.last_day_stats);
        let _ = std::thread::spawn(move || {
            let mut stats = poison_target.lock().expect("fresh mutex should lock");
            stats.insert("stale".to_string(), LastDayTierStats::default());
            panic!("poison tier stats");
        })
        .join();

        state.add_lastday_stats("fresh", TierStats::default());
        let stats = state.get_daily_all_tier_stats();
        assert!(!stats.contains_key("stale"), "possibly partial statistics must be discarded");
        assert!(stats.contains_key("fresh"), "statistics must accept new samples after recovery");
    }

    #[test]
    fn first_tier_sample_is_not_dropped() {
        // The first completed transition to a tier both creates the map entry
        // and carries a sample. Creating the entry must not discard that
        // sample, or the admin GetTierInfo response undercounts every tier by
        // its first transition while reporting later ones correctly.
        let state = TransitionState::new_with_capacity(1);

        state.add_lastday_stats(
            "warm",
            TierStats {
                total_size: 10,
                num_versions: 1,
                num_objects: 1,
            },
        );

        let stats = state.get_daily_all_tier_stats();
        let total = stats.get("warm").expect("first sample must create the tier entry").total();
        assert_eq!(total.total_size, 10, "first sample size must be counted");
        assert_eq!(total.num_versions, 1, "first sample version must be counted");
        assert_eq!(total.num_objects, 1, "first sample object must be counted");

        // Later samples must accumulate onto the existing entry rather than
        // replace it; `LastDayTierStats::add_stats` coverage alone does not
        // reach this path, because it never goes through the tier map.
        state.add_lastday_stats(
            "warm",
            TierStats {
                total_size: 20,
                num_versions: 2,
                num_objects: 1,
            },
        );

        let stats = state.get_daily_all_tier_stats();
        let total = stats.get("warm").expect("tier entry must survive later samples").total();
        assert_eq!(total.total_size, 30, "later sample size must accumulate");
        assert_eq!(total.num_versions, 3, "later sample versions must accumulate");
        assert_eq!(total.num_objects, 2, "later sample objects must accumulate");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scanner_transition_state_reports_compensation_pending_buckets() {
        let state = TransitionState::new_with_capacity(1);

        assert_eq!(state.scanner_transition_state_update().compensation_pending, 0);
        state.compensation_buckets.lock().unwrap().insert("bucket-a".to_string());
        assert_eq!(state.scanner_transition_state_update().compensation_pending, 1);
        state.compensation_buckets.lock().unwrap().insert("bucket-a".to_string());
        assert_eq!(state.scanner_transition_state_update().compensation_pending, 1);
    }

    #[test]
    fn transition_reserve_dedupes_same_object_version() {
        // Regression for rustfs/backlog#1268: a single object version can be
        // enqueued twice (immediate transition + compensation backfill). The
        // second claim must be rejected until the first is released, so the two
        // do not run concurrently and race the source cleanup.
        let state = TransitionState::new_with_capacity(4);

        let oi = ObjectInfo {
            bucket: "foo".to_string(),
            name: "payload.bin".to_string(),
            version_id: None,
            ..Default::default()
        };

        assert!(state.reserve_transition(&oi), "first claim must succeed");
        assert!(!state.reserve_transition(&oi), "duplicate claim must be rejected");

        // A different object is independent.
        let other = ObjectInfo {
            bucket: "foo".to_string(),
            name: "other.bin".to_string(),
            version_id: None,
            ..Default::default()
        };
        assert!(state.reserve_transition(&other), "distinct object must claim independently");

        // After release, the same object can be re-claimed (later lifecycle pass).
        state.release_transition(&oi);
        assert!(state.reserve_transition(&oi), "re-claim after release must succeed");
    }

    #[tokio::test]
    #[serial]
    async fn queue_transition_task_dedupes_same_object_without_second_enqueue() {
        // Capacity 4 leaves room, so a rejected second enqueue can only be the
        // dedup guard, not queue pressure (rustfs/backlog#1268).
        let state = TransitionState::new_with_capacity(4);
        let object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            ..Default::default()
        };

        let first = state.queue_transition_task(&object, &event, &LcEventSrc::Scanner).await;
        let second = state.queue_transition_task(&object, &event, &LcEventSrc::Scanner).await;

        assert!(first, "first enqueue must succeed");
        assert!(second, "duplicate enqueue is reported handled (already queued)");
        assert_eq!(state.transition_rx.len(), 1, "only one task must actually be queued");
    }

    #[tokio::test]
    #[serial]
    async fn queue_transition_task_outcome_reports_duplicate_separately() {
        let state = TransitionState::new_with_capacity(4);
        let object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            ..Default::default()
        };

        let first = state
            .queue_transition_task_outcome(None, &object, &event, &LcEventSrc::Scanner, None)
            .await;
        let second = state
            .queue_transition_task_outcome(None, &object, &event, &LcEventSrc::Scanner, None)
            .await;

        assert_eq!(first, TransitionEnqueueOutcome::Queued);
        assert_eq!(second, TransitionEnqueueOutcome::AlreadyInFlight);
        assert_eq!(state.transition_rx.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn queue_transition_task_outcome_reports_queue_full_separately() {
        let state = TransitionState::new_with_capacity(1);
        let first_object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "first".to_string(),
            ..Default::default()
        };
        let second_object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "second".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            ..Default::default()
        };

        let first = state
            .queue_transition_task_outcome(None, &first_object, &event, &LcEventSrc::Scanner, None)
            .await;
        let second = state
            .queue_transition_task_outcome(None, &second_object, &event, &LcEventSrc::Scanner, None)
            .await;

        assert_eq!(first, TransitionEnqueueOutcome::Queued);
        assert_eq!(second, TransitionEnqueueOutcome::QueueFull);
        assert_eq!(state.transition_rx.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn queue_transition_task_outcome_persists_manual_task_journal() {
        let (_paths, ecstore) = setup_test_env().await;
        let state = TransitionState::new_with_capacity(4);
        let job_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        let object = ObjectInfo {
            bucket: "manual-task-journal-bucket".to_string(),
            name: "logs/object".to_string(),
            version_id: Some(version_id),
            etag: Some("task-etag".to_string()),
            mod_time: Some(OffsetDateTime::now_utc()),
            size: 42,
            is_latest: true,
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            storage_class: "WARM".to_string(),
            ..Default::default()
        };
        let task_key = manual_transition_worker_result_task_key(&object.bucket, &object.name, object.version_id);

        let outcome = state
            .queue_transition_task_outcome(Some(ecstore.clone()), &object, &event, &LcEventSrc::Scanner, Some(job_id))
            .await;

        assert_eq!(outcome, TransitionEnqueueOutcome::Queued);
        assert_eq!(state.transition_rx.len(), 1);
        let task_record = load_manual_transition_task_record(ecstore, job_id, &task_key)
            .await
            .expect("manual task journal marker should load");
        assert_eq!(task_record.job_id, job_id);
        assert_eq!(task_record.task_key, task_key);
        assert_eq!(task_record.bucket, object.bucket);
        assert_eq!(task_record.object, object.name);
        assert_eq!(task_record.version_id, Some(version_id));
        assert_eq!(task_record.storage_class, "WARM");
        assert_eq!(task_record.etag.as_deref(), Some("task-etag"));
        assert_eq!(task_record.mod_time_unix_nanos, object.mod_time.map(|time| time.unix_timestamp_nanos()));
        assert_eq!(task_record.size, Some(42));
        assert_eq!(task_record.is_latest, Some(true));
    }

    #[tokio::test]
    #[serial]
    async fn queue_transition_task_outcome_fails_closed_when_manual_task_journal_fails() {
        let (_paths, ecstore) = setup_test_env().await;
        let state = TransitionState::new_with_capacity(4);
        let object = ObjectInfo {
            bucket: "manual-task-journal-fail-bucket".to_string(),
            name: "logs/object".to_string(),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            storage_class: "WARM".to_string(),
            ..Default::default()
        };

        let failed = state
            .queue_transition_task_outcome(Some(ecstore.clone()), &object, &event, &LcEventSrc::Scanner, Some(Uuid::nil()))
            .await;

        assert_eq!(failed, TransitionEnqueueOutcome::TaskJournalFailed);
        assert_eq!(state.transition_rx.len(), 0);

        let retried = state
            .queue_transition_task_outcome(Some(ecstore), &object, &event, &LcEventSrc::Scanner, Some(Uuid::new_v4()))
            .await;
        assert_eq!(retried, TransitionEnqueueOutcome::Queued);
        assert_eq!(state.transition_rx.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn queue_transition_task_dedupes_immediate_and_scanner_sources_for_same_version() {
        let state = TransitionState::new_with_capacity(4);
        let version_id = Uuid::new_v4();
        let object = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(version_id),
            ..Default::default()
        };
        let event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::TransitionAction,
            ..Default::default()
        };

        let immediate = state.queue_transition_task(&object, &event, &LcEventSrc::S3PutObject).await;
        let scanner = state.queue_transition_task(&object, &event, &LcEventSrc::Scanner).await;

        assert!(immediate, "immediate transition enqueue must succeed");
        assert!(scanner, "scanner duplicate is reported handled while already queued");
        assert_eq!(
            state.transition_rx.len(),
            1,
            "immediate transition plus scanner/backfill duplicate must queue one task"
        );

        let next_version = ObjectInfo {
            version_id: Some(Uuid::new_v4()),
            ..object
        };
        let queued_next_version = state.queue_transition_task(&next_version, &event, &LcEventSrc::Scanner).await;

        assert!(queued_next_version, "a different version of the same object must enqueue independently");
        assert_eq!(state.transition_rx.len(), 2, "deduplication must be scoped to the exact object version");
    }

    #[tokio::test]
    #[serial]
    async fn transition_state_init_honors_runtime_configured_worker_count() {
        let (_paths, ecstore) = setup_test_env().await;
        let transition_state = runtime_sources::transition_state_handle();
        let original_workers = transition_state.num_workers.load(Ordering::SeqCst);
        with_transition_worker_env_async(Some("3"), Some("8"), || async {
            TransitionState::update_workers(ecstore.clone(), 0).await;
            assert_eq!(transition_state.num_workers.load(Ordering::SeqCst), 3);
        })
        .await;

        let absolute_max = resolve_transition_workers_absolute_max();
        TransitionState::resize_workers_to(ecstore, original_workers, original_workers, absolute_max);
    }

    #[tokio::test]
    #[serial]
    async fn transition_worker_resize_cancels_removed_workers_directly() {
        let (_paths, ecstore) = setup_test_env().await;
        let transition_state = runtime_sources::transition_state_handle();
        let original_workers = transition_state.num_workers.load(Ordering::SeqCst);
        let absolute_max = resolve_transition_workers_absolute_max();

        TransitionState::resize_workers_to(ecstore.clone(), 0, 0, absolute_max);
        assert_eq!(transition_state.num_workers.load(Ordering::SeqCst), 0);

        TransitionState::resize_workers_to(ecstore.clone(), 2, 2, absolute_max);
        let worker_tokens = {
            let workers = transition_state.workers.lock().unwrap();
            assert_eq!(workers.len(), 2);
            workers.iter().map(|worker| worker.cancel.clone()).collect::<Vec<_>>()
        };

        TransitionState::resize_workers_to(ecstore.clone(), 1, 1, absolute_max);

        assert_eq!(transition_state.num_workers.load(Ordering::SeqCst), 1);
        assert_eq!(worker_tokens.iter().filter(|token| token.is_cancelled()).count(), 1);

        let remaining_token = {
            let workers = transition_state.workers.lock().unwrap();
            assert_eq!(workers.len(), 1);
            let token = workers[0].cancel.clone();
            assert!(!token.is_cancelled());
            token
        };

        TransitionState::resize_workers_to(ecstore.clone(), 0, 0, absolute_max);
        assert_eq!(transition_state.num_workers.load(Ordering::SeqCst), 0);
        assert!(remaining_token.is_cancelled());

        TransitionState::resize_workers_to(ecstore, original_workers, original_workers, absolute_max);
    }

    #[tokio::test]
    #[serial]
    async fn transition_worker_resize_without_runtime_does_not_poison_tracking() {
        let (_paths, ecstore) = setup_test_env().await;
        let transition_state = runtime_sources::transition_state_handle();
        let resize = std::thread::spawn(move || {
            TransitionState::resize_workers_to(ecstore, 1, 1, resolve_transition_workers_absolute_max());
        })
        .join();

        assert!(resize.is_ok(), "missing Tokio runtime must not panic while worker tracking is locked");
        assert!(
            transition_state.workers.lock().is_ok(),
            "failed resize must leave worker tracking unpoisoned"
        );
    }

    #[test]
    fn should_defer_date_expiry_for_recent_config_update_respects_grace_window() {
        let now = OffsetDateTime::now_utc();
        let recent = BucketLifecycleConfiguration {
            expiry_updated_at: Some(Timestamp::from(now - time::Duration::seconds(1))),
            rules: Vec::new(),
        };
        let stale = BucketLifecycleConfiguration {
            expiry_updated_at: Some(Timestamp::from(
                now - time::Duration::seconds(DATE_EXPIRY_EXISTING_OBJECTS_GRACE_SECS + 1),
            )),
            rules: Vec::new(),
        };

        assert!(should_defer_date_expiry_for_recent_config_update(&recent, now));
        assert!(!should_defer_date_expiry_for_recent_config_update(&stale, now));
    }

    #[test]
    fn mark_delete_opts_skip_decommissioned_on_remote_success_preserves_false_on_failure() {
        let mut opts = ObjectOptions::default();

        mark_delete_opts_skip_decommissioned_on_remote_success(&mut opts, false);

        assert!(!opts.skip_decommissioned);
    }

    #[test]
    fn mark_delete_opts_skip_decommissioned_on_remote_success_preserves_existing_true_on_failure() {
        let mut opts = ObjectOptions {
            skip_decommissioned: true,
            ..ObjectOptions::default()
        };

        mark_delete_opts_skip_decommissioned_on_remote_success(&mut opts, false);

        assert!(opts.skip_decommissioned);
    }

    #[test]
    fn lifecycle_deleted_object_uses_delete_marker_created_by_expiry() {
        let source = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "key".to_string(),
            ..Default::default()
        };
        let delete_result = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "key".to_string(),
            delete_marker: true,
            version_id: Some(Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        let deleted = lifecycle_deleted_object(&source, &delete_result);

        assert!(deleted.delete_marker);
        assert_eq!(deleted.delete_marker_version_id, delete_result.version_id);
        assert_eq!(deleted.version_id, None);
        assert_eq!(deleted.object_name, "key");
    }

    #[test]
    fn lifecycle_deleted_object_hands_off_only_persisted_delete_admission_state() {
        let source = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "key".to_string(),
            ..Default::default()
        };
        let marker_result = ObjectInfo {
            delete_marker: true,
            version_id: Some(Uuid::new_v4()),
            replication_status_internal: Some("arn:target=PENDING;".to_string()),
            replication_decision: "arn:target=true".to_string(),
            ..Default::default()
        };
        let marker_delete = lifecycle_deleted_object(&source, &marker_result);
        let marker_state = marker_delete
            .replication_state
            .expect("persisted marker admission must be handed off");
        assert_eq!(marker_state.replication_status_internal.as_deref(), Some("arn:target=PENDING;"));
        assert!(marker_state.version_purge_status_internal.is_none());

        let version_result = ObjectInfo {
            version_purge_status_internal: Some("arn:target=PENDING;".to_string()),
            replication_decision: "arn:target=true".to_string(),
            ..Default::default()
        };
        let version_delete = lifecycle_deleted_object(
            &ObjectInfo {
                version_id: Some(Uuid::new_v4()),
                ..source
            },
            &version_result,
        );
        let version_state = version_delete
            .replication_state
            .expect("persisted version purge admission must be handed off");
        assert!(version_state.replication_status_internal.is_none());
        assert_eq!(version_state.version_purge_status_internal.as_deref(), Some("arn:target=PENDING;"));
    }

    #[test]
    fn lifecycle_deleted_object_uses_version_id_for_noncurrent_version_purge() {
        let version_id = Uuid::new_v4();
        let source = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "key".to_string(),
            version_id: Some(version_id),
            ..Default::default()
        };

        let deleted = lifecycle_deleted_object(&source, &ObjectInfo::default());

        assert!(!deleted.delete_marker);
        assert_eq!(deleted.version_id, Some(version_id));
        assert_eq!(deleted.delete_marker_version_id, None);
    }

    #[test]
    fn lifecycle_deleted_object_uses_delete_marker_version_for_marker_purge() {
        let version_id = Uuid::new_v4();
        let source = ObjectInfo {
            bucket: "bucket".to_string(),
            name: "key".to_string(),
            delete_marker: true,
            version_id: Some(version_id),
            ..Default::default()
        };

        let deleted = lifecycle_deleted_object(&source, &ObjectInfo::default());

        assert!(!deleted.delete_marker);
        assert_eq!(deleted.delete_marker_version_id, Some(version_id));
        assert_eq!(deleted.version_id, None);
    }

    fn expired_delete_marker_lifecycle() -> BucketLifecycleConfiguration {
        BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    expired_object_delete_marker: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("expired-marker".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        }
    }

    fn latest_expiration_lifecycle() -> BucketLifecycleConfiguration {
        BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("expire-current".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        }
    }

    fn all_versions_expiration_lifecycle() -> BucketLifecycleConfiguration {
        BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(1),
                    expired_object_all_versions: Some(true),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("delete-all".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            }],
        }
    }

    fn lock_enabled_without_default_retention() -> ObjectLockConfiguration {
        ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: None,
        }
    }

    fn lock_enabled_with_default_retention() -> ObjectLockConfiguration {
        ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    days: Some(30),
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                    years: None,
                }),
            }),
        }
    }

    fn latest_transition_lifecycle() -> BucketLifecycleConfiguration {
        BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("transition-current".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: Some(1),
                    date: None,
                    storage_class: Some(TransitionStorageClass::from_static("WARM")),
                }]),
            }],
        }
    }

    fn delete_marker_object(
        replication_status: ReplicationStatusType,
        version_purge_status: VersionPurgeStatusType,
    ) -> ObjectInfo {
        ObjectInfo {
            bucket: "bucket".to_string(),
            name: "logs/object".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).expect("valid fixed test timestamp")),
            version_id: Some(Uuid::new_v4()),
            is_latest: true,
            delete_marker: true,
            num_versions: 1,
            replication_status,
            version_purge_status,
            ..Default::default()
        }
    }

    fn current_object(replication_status: ReplicationStatusType) -> ObjectInfo {
        ObjectInfo {
            bucket: "bucket".to_string(),
            name: "logs/object".to_string(),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_000_000).expect("valid fixed test timestamp")),
            version_id: Some(Uuid::new_v4()),
            is_latest: true,
            num_versions: 1,
            replication_status,
            ..Default::default()
        }
    }

    fn current_object_with_metadata(
        replication_status: ReplicationStatusType,
        user_defined: HashMap<String, String>,
    ) -> ObjectInfo {
        ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..current_object(replication_status)
        }
    }

    #[test]
    fn lifecycle_replication_blocks_only_pending_failed_or_pending_purge() {
        assert!(lifecycle_replication_blocks_action(&delete_marker_object(
            ReplicationStatusType::Pending,
            VersionPurgeStatusType::default(),
        )));
        assert!(lifecycle_replication_blocks_action(&delete_marker_object(
            ReplicationStatusType::Completed,
            VersionPurgeStatusType::Failed,
        )));
        assert!(!lifecycle_replication_blocks_action(&delete_marker_object(
            ReplicationStatusType::Completed,
            VersionPurgeStatusType::Complete,
        )));
        assert!(!lifecycle_replication_blocks_action(&delete_marker_object(
            ReplicationStatusType::Empty,
            VersionPurgeStatusType::Empty,
        )));
    }

    #[test]
    fn lifecycle_action_replication_guard_requires_waiting_action_and_pending_state() {
        let pending = current_object(ReplicationStatusType::Pending);
        let completed = current_object(ReplicationStatusType::Completed);

        assert!(lifecycle_action_blocked_by_replication(IlmAction::TransitionAction, &pending));
        assert!(lifecycle_action_blocked_by_replication(IlmAction::DeleteAction, &pending));
        assert!(!lifecycle_action_blocked_by_replication(IlmAction::NoneAction, &pending));
        assert!(!lifecycle_action_blocked_by_replication(IlmAction::TransitionAction, &completed));
    }

    #[test]
    fn delete_all_version_replication_scan_stops_after_exact_object_key() {
        let completed = ObjectInfo {
            name: "a".to_string(),
            replication_status: ReplicationStatusType::Completed,
            ..Default::default()
        };
        let pending_exact = ObjectInfo {
            name: "a".to_string(),
            replication_status: ReplicationStatusType::Pending,
            ..Default::default()
        };
        let pending_child = ObjectInfo {
            name: "a/child".to_string(),
            replication_status: ReplicationStatusType::Pending,
            ..Default::default()
        };
        let later_key = ObjectInfo {
            name: "ab".to_string(),
            replication_status: ReplicationStatusType::Pending,
            ..Default::default()
        };

        assert_eq!(
            lifecycle_delete_all_versions_replication_scan("a", &[pending_exact]),
            VersionReplicationScan::Blocked
        );
        assert_eq!(
            lifecycle_delete_all_versions_replication_scan("a", &[completed.clone(), pending_child]),
            VersionReplicationScan::Done
        );
        assert_eq!(
            lifecycle_delete_all_versions_replication_scan("a", &[completed]),
            VersionReplicationScan::Continue
        );
        assert_eq!(
            lifecycle_delete_all_versions_replication_scan("a", &[later_key]),
            VersionReplicationScan::Done
        );
    }

    #[tokio::test]
    async fn enqueue_transition_with_lifecycle_skips_transition_while_replication_pending() {
        let lc = latest_transition_lifecycle();
        let object = current_object(ReplicationStatusType::Pending);

        let queued = enqueue_transition_with_lifecycle(&object, &lc, &LcEventSrc::Scanner).await;

        assert!(!queued);
    }

    #[tokio::test]
    async fn manual_transition_dry_run_counts_due_object_without_enqueue() {
        let lc = latest_transition_lifecycle();
        let object = current_object(ReplicationStatusType::Completed);
        let options = ManualTransitionRunOptions {
            dry_run: true,
            ..Default::default()
        };
        let mut report = ManualTransitionRunReport::new(&object.bucket, &options);

        let handled =
            enqueue_transition_with_lifecycle_report(None, &object, &lc, &LcEventSrc::Scanner, &options, &mut report).await;

        assert!(handled);
        assert_eq!(report.eligible, 1);
        assert_eq!(report.dry_run_eligible, 1);
        assert_eq!(report.enqueued, 0);
    }

    #[tokio::test]
    async fn manual_transition_respects_not_yet_due_lifecycle_rule() {
        let lc = latest_transition_lifecycle();
        let object = ObjectInfo {
            mod_time: Some(OffsetDateTime::now_utc()),
            ..current_object(ReplicationStatusType::Completed)
        };
        let options = ManualTransitionRunOptions {
            dry_run: true,
            ..Default::default()
        };
        let mut report = ManualTransitionRunReport::new(&object.bucket, &options);

        let handled =
            enqueue_transition_with_lifecycle_report(None, &object, &lc, &LcEventSrc::Scanner, &options, &mut report).await;

        assert!(!handled);
        assert_eq!(report.eligible, 0);
        assert_eq!(report.skipped_not_transition, 1);
    }

    #[tokio::test]
    async fn manual_transition_tier_filter_skips_non_matching_transition() {
        let lc = latest_transition_lifecycle();
        let object = current_object(ReplicationStatusType::Completed);
        let options = ManualTransitionRunOptions {
            tier: Some("COLD".to_string()),
            dry_run: true,
            ..Default::default()
        };
        let mut report = ManualTransitionRunReport::new(&object.bucket, &options);

        let handled =
            enqueue_transition_with_lifecycle_report(None, &object, &lc, &LcEventSrc::Scanner, &options, &mut report).await;

        assert!(!handled);
        assert_eq!(report.eligible, 0);
        assert_eq!(report.skipped_tier, 1);
    }

    #[tokio::test]
    async fn manual_transition_reports_runtime_tier_failure_before_enqueue() {
        let lc = latest_transition_lifecycle();
        let object = current_object(ReplicationStatusType::Completed);
        let options = ManualTransitionRunOptions::default();
        let mut report = ManualTransitionRunReport::new(&object.bucket, &options);

        let handled =
            enqueue_transition_with_lifecycle_report(None, &object, &lc, &LcEventSrc::Scanner, &options, &mut report).await;

        assert!(!handled);
        assert_eq!(report.eligible, 0);
        assert_eq!(report.enqueued, 0);
        assert_eq!(report.tier_failure, 1);
        assert!(!report.has_partial_enqueue());
    }

    #[test]
    fn manual_transition_complete_preserves_worker_failure_summary() {
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-worker-summary-bucket", &options, "owner-a");

        record.record_worker_result(ManualTransitionWorkerResult::Completed, ManualTransitionQueueSnapshot::default());
        record.record_worker_result(ManualTransitionWorkerResult::TierFailure, ManualTransitionQueueSnapshot::default());
        record.complete(
            ManualTransitionRunReport {
                bucket: "manual-worker-summary-bucket".to_string(),
                scanned: 3,
                eligible: 2,
                enqueued: 2,
                tier_failure: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );

        assert_eq!(record.state, ManualTransitionJobState::Partial);
        assert!(record.scan_completed);
        assert_eq!(record.report.scanned, 3);
        assert_eq!(record.report.eligible, 2);
        assert_eq!(record.report.enqueued, 2);
        assert_eq!(record.report.transition_completed, 1);
        assert_eq!(record.report.transition_failed, 1);
        assert_eq!(record.report.tier_failure, 2);
        assert!(record.completed_at_unix_nanos.is_some());
    }

    #[test]
    fn manual_transition_worker_failure_reason_classifies_known_failures() {
        assert_eq!(
            manual_transition_worker_failure_reason(&Error::ObjectNotFound("bucket".to_string(), "obj".to_string())),
            ManualTransitionWorkerFailureReason::NotFound
        );
        assert_eq!(
            manual_transition_worker_failure_reason(&Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "file access denied",
            ))),
            ManualTransitionWorkerFailureReason::PermissionDenied
        );
        assert_eq!(
            manual_transition_worker_failure_reason(&Error::ErasureReadQuorum),
            ManualTransitionWorkerFailureReason::Quorum
        );
        assert_eq!(
            manual_transition_worker_failure_reason(&Error::Timeout),
            ManualTransitionWorkerFailureReason::Timeout
        );
        assert_eq!(
            manual_transition_worker_failure_reason(&Error::SlowDown),
            ManualTransitionWorkerFailureReason::SlowDown
        );
        assert_eq!(
            manual_transition_worker_failure_reason(&Error::Io(std::io::Error::other(
                "remote tier request failed with status 403 Forbidden: InvalidAccessKeyId",
            ))),
            ManualTransitionWorkerFailureReason::PermissionDenied
        );
        assert_eq!(
            manual_transition_worker_failure_reason(&Error::Io(std::io::Error::other("client error (SendRequest)",))),
            ManualTransitionWorkerFailureReason::Network
        );
        assert_eq!(
            manual_transition_worker_failure_reason(&Error::MethodNotAllowed),
            ManualTransitionWorkerFailureReason::Unknown
        );
    }

    #[tokio::test]
    async fn manual_transition_counts_already_transitioned_object() {
        let lc = latest_transition_lifecycle();
        let mut object = current_object(ReplicationStatusType::Completed);
        object.transitioned_object.status = TRANSITION_COMPLETE.to_string();
        object.transitioned_object.tier = "WARM".to_string();
        let options = ManualTransitionRunOptions {
            dry_run: true,
            ..Default::default()
        };
        let mut report = ManualTransitionRunReport::new(&object.bucket, &options);

        let handled =
            enqueue_transition_with_lifecycle_report(None, &object, &lc, &LcEventSrc::Scanner, &options, &mut report).await;

        assert!(!handled);
        assert_eq!(report.eligible, 0);
        assert_eq!(report.skipped_not_transition, 0);
        assert_eq!(report.skipped_already_transitioned, 1);
    }

    #[test]
    fn manual_transition_report_marks_queue_pressure_partial() {
        let options = ManualTransitionRunOptions::default();
        let mut report = ManualTransitionRunReport::new("bucket", &options);

        report.record_enqueue_outcome(TransitionEnqueueOutcome::QueueFull);

        assert_eq!(report.enqueued, 0);
        assert_eq!(report.skipped_queue_full, 1);
        assert_eq!(report.skipped_queue_closed, 0);
        assert_eq!(report.skipped_queue_timeout, 0);
        assert!(report.has_partial_enqueue());
    }

    #[test]
    fn manual_transition_job_record_queue_pressure_reports_are_partial() {
        let options = ManualTransitionRunOptions::default();

        for (bucket, outcome, skipped_queue_full, skipped_queue_closed, skipped_queue_timeout, queue_snapshot) in [
            (
                "manual-queue-full-bucket",
                TransitionEnqueueOutcome::QueueFull,
                1,
                0,
                0,
                ManualTransitionQueueSnapshot {
                    queue_capacity: 1,
                    workers: 1,
                    queue_full: 1,
                    ..Default::default()
                },
            ),
            (
                "manual-queue-closed-bucket",
                TransitionEnqueueOutcome::QueueClosed,
                0,
                1,
                0,
                ManualTransitionQueueSnapshot {
                    queue_capacity: 1,
                    workers: 1,
                    ..Default::default()
                },
            ),
            (
                "manual-queue-timeout-bucket",
                TransitionEnqueueOutcome::QueueSendTimedOut,
                0,
                0,
                1,
                ManualTransitionQueueSnapshot {
                    queue_capacity: 1,
                    workers: 1,
                    queue_send_timeout: 1,
                    ..Default::default()
                },
            ),
        ] {
            let mut report = ManualTransitionRunReport::new(bucket, &options);
            report.record_enqueue_outcome(outcome);
            assert!(report.has_partial_enqueue());

            let mut record = ManualTransitionJobRecord::new(Uuid::new_v4(), bucket, &options, "owner");
            record.complete(report, queue_snapshot);

            assert_eq!(record.state, ManualTransitionJobState::Partial);
            assert_eq!(record.report.enqueued, 0);
            assert_eq!(record.report.skipped_queue_full, skipped_queue_full);
            assert_eq!(record.report.skipped_queue_closed, skipped_queue_closed);
            assert_eq!(record.report.skipped_queue_timeout, skipped_queue_timeout);
            assert_eq!(record.queue_snapshot.queue_full, queue_snapshot.queue_full);
            assert_eq!(record.queue_snapshot.queue_send_timeout, queue_snapshot.queue_send_timeout);
            assert!(record.completed_at_unix_nanos.is_some());
            assert!(record.error.is_none());
        }
    }

    #[test]
    fn manual_transition_version_marker_preserves_null_version_cursor() {
        let null_version = ObjectInfo {
            version_id: None,
            ..Default::default()
        };
        let version_id = Uuid::new_v4();
        let versioned = ObjectInfo {
            version_id: Some(version_id),
            ..Default::default()
        };

        assert_eq!(manual_transition_version_marker(&null_version), "null");
        assert_eq!(manual_transition_version_marker(&versioned), version_id.to_string());
    }

    #[test]
    fn manual_transition_limit_boundary_only_truncates_when_more_objects_exist() {
        assert!(!manual_transition_has_more_after_limit(9, 10, false));
        assert!(manual_transition_has_more_after_limit(9, 11, false));
        assert!(manual_transition_has_more_after_limit(9, 10, true));
    }

    #[test]
    fn manual_transition_duration_budget_detects_elapsed_deadline() {
        let report = ManualTransitionRunReport {
            truncated_by_duration: true,
            ..Default::default()
        };

        assert!(!manual_transition_duration_elapsed(None));
        assert!(manual_transition_duration_elapsed(Some(tokio::time::Instant::now())));
        assert!(!manual_transition_duration_elapsed(Some(
            tokio::time::Instant::now() + StdDuration::from_secs(60)
        )));
        assert!(report.was_truncated());
    }

    #[test]
    fn manual_transition_continuation_token_round_trips_resume_cursor() {
        let token = encode_manual_transition_continuation_token(Some("logs/a".to_string()), Some("null".to_string()))
            .expect("non-empty cursor should encode");

        let (marker, version_marker) =
            decode_manual_transition_continuation_token(&token).expect("continuation token should decode");

        assert_eq!(marker.as_deref(), Some("logs/a"));
        assert_eq!(version_marker.as_deref(), Some("null"));
    }

    #[test]
    fn manual_transition_continuation_token_rejects_malformed_input() {
        let err =
            decode_manual_transition_continuation_token("not-base64").expect_err("malformed continuation token must fail closed");

        assert!(err.to_string().contains("decode manual transition continuation token failed"));
    }

    #[test]
    fn manual_transition_report_serializes_public_continuation_only() {
        let report = ManualTransitionRunReport {
            continuation_token: Some("opaque".to_string()),
            next_marker: Some("logs/a".to_string()),
            next_version_idmarker: Some("null".to_string()),
            ..Default::default()
        };

        let value = serde_json::to_value(report).expect("report should serialize");

        assert_eq!(value.get("continuation_token").and_then(|value| value.as_str()), Some("opaque"));
        assert!(value.get("next_marker").is_none());
        assert!(value.get("next_version_idmarker").is_none());
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_progress_persists_checkpoint_and_renews_admission() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            ..Default::default()
        };
        let record = ManualTransitionJobRecord::new(job_id, "manual-progress-journal-bucket", &options, "owner-a");
        let scope_key = record.scope_key.clone();
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("running job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("running scope admission should save");
        let continuation_token =
            encode_manual_transition_continuation_token(Some("logs/page-end".to_string()), Some("null".to_string()))
                .expect("resume cursor should encode");
        let queue_snapshot = ManualTransitionQueueSnapshot {
            queued: 1,
            active: 2,
            workers: 3,
            ..Default::default()
        };
        let report = ManualTransitionRunReport {
            bucket: "manual-progress-journal-bucket".to_string(),
            prefix: "logs/".to_string(),
            scanned: 1000,
            continuation_token: Some(continuation_token.clone()),
            ..Default::default()
        };

        let persisted =
            persist_manual_transition_job_progress_if_owned(ecstore.clone(), job_id, record.lease_id, &report, queue_snapshot)
                .await
                .expect("page checkpoint should persist to the job record");

        assert_eq!(persisted.state, ManualTransitionJobState::Running);
        assert_eq!(persisted.report.scanned, 1000);
        assert_eq!(persisted.report.continuation_token.as_deref(), Some(continuation_token.as_str()));
        assert_eq!(persisted.queue_snapshot, queue_snapshot);
        let loaded = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("checkpointed job should reload");
        assert_eq!(loaded.report.continuation_token.as_deref(), Some(continuation_token.as_str()));
        assert_eq!(loaded.queue_snapshot, queue_snapshot);
        let admission = load_manual_transition_scope_admission(ecstore, &scope_key)
            .await
            .expect("running checkpoint must keep the scope admission alive");
        assert_eq!(admission.job_id, job_id);
        assert_eq!(admission.lease_id, loaded.lease_id);
        assert_eq!(admission.updated_at_unix_nanos, loaded.updated_at_unix_nanos);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_progress_retries_heartbeat_cas_without_losing_checkpoint() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            ..Default::default()
        };
        let record = ManualTransitionJobRecord::new(job_id, "manual-progress-cas-bucket", &options, "owner-a");
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("running job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("running scope admission should save");
        let lease_id = record.lease_id;
        let barrier = ManualTransitionJobCasBarrier::install(job_id);
        let progress_store = ecstore.clone();
        let progress = tokio::spawn(async move {
            persist_manual_transition_job_progress_if_owned(
                progress_store,
                job_id,
                lease_id,
                &ManualTransitionRunReport {
                    bucket: "manual-progress-cas-bucket".to_string(),
                    prefix: "logs/".to_string(),
                    scanned: 1000,
                    eligible: 900,
                    enqueued: 800,
                    continuation_token: Some("opaque-page-cursor".to_string()),
                    ..Default::default()
                },
                ManualTransitionQueueSnapshot {
                    queued: 7,
                    active: 3,
                    ..Default::default()
                },
            )
            .await
        });
        barrier.wait_until_paused().await;

        let heartbeat = renew_manual_transition_job_lease_if_owned(
            ecstore.clone(),
            job_id,
            lease_id,
            ManualTransitionQueueSnapshot {
                queued: 2,
                active: 1,
                ..Default::default()
            },
        )
        .await
        .expect("heartbeat should win the first CAS write");
        barrier.release();
        let checkpointed = progress
            .await
            .expect("progress task should join")
            .expect("progress should retry its stale ETag");

        assert_eq!(checkpointed.lease_id, heartbeat.lease_id);
        assert_eq!(checkpointed.report.scanned, 1000);
        assert_eq!(checkpointed.report.eligible, 900);
        assert_eq!(checkpointed.report.enqueued, 800);
        assert_eq!(checkpointed.report.continuation_token.as_deref(), Some("opaque-page-cursor"));
        assert_eq!(checkpointed.queue_snapshot.queued, 7);
        assert_eq!(checkpointed.queue_snapshot.active, 3);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_progress_rejects_stale_recovery_lease() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let record = ManualTransitionJobRecord::new(
            job_id,
            "manual-progress-stale-lease-bucket",
            &ManualTransitionRunOptions::default(),
            "owner-a",
        );
        let stale_lease_id = record.lease_id;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("running job record should save");

        let (mut recovered, etag) = load_manual_transition_job_record_with_etag(ecstore.clone(), job_id)
            .await
            .expect("running job record should load");
        recovered.lease_id = Uuid::new_v4();
        recovered.owner_id = "owner-b".to_string();
        save_manual_transition_job_record_if_current(ecstore.clone(), &recovered, &etag)
            .await
            .expect("recovery owner should replace the lease");

        let error = persist_manual_transition_job_progress_if_owned(
            ecstore.clone(),
            job_id,
            stale_lease_id,
            &ManualTransitionRunReport {
                scanned: 1000,
                continuation_token: Some("stale-owner-cursor".to_string()),
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect_err("the stale owner must not update the recovered job");
        let heartbeat_error = renew_manual_transition_job_lease_if_owned(
            ecstore.clone(),
            job_id,
            stale_lease_id,
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect_err("the stale owner must not renew the recovered job");

        assert_eq!(error, Error::PreconditionFailed);
        assert_eq!(heartbeat_error, Error::PreconditionFailed);
        let loaded = load_manual_transition_job_record(ecstore, job_id)
            .await
            .expect("recovered job record should load");
        assert_eq!(loaded.lease_id, recovered.lease_id);
        assert_eq!(loaded.owner_id, "owner-b");
        assert_eq!(loaded.report.scanned, 0);
        assert!(loaded.report.continuation_token.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_reconcile_rejects_lease_takeover_during_cas() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-reconcile-lease-race-{}", job_id.simple());
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &ManualTransitionRunOptions::default(), "owner-a");
        record.scan_completed = true;
        let stale_lease_id = record.lease_id;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("running job record should save");
        let task_key = manual_transition_worker_result_task_key(&bucket, "logs/a", None);
        let task = ManualTransitionTaskRecord::new(job_id, &task_key, &bucket, "logs/a", None, "WARM");
        assert!(
            save_manual_transition_task_if_absent(ecstore.clone(), &task)
                .await
                .expect("task journal marker should save")
        );

        let barrier = ManualTransitionJobCasBarrier::install(job_id);
        let heartbeat_store = ecstore.clone();
        let heartbeat = tokio::spawn(async move {
            renew_manual_transition_job_lease_if_owned(
                heartbeat_store,
                job_id,
                stale_lease_id,
                ManualTransitionQueueSnapshot::default(),
            )
            .await
        });
        barrier.wait_until_paused().await;

        let (mut recovered, etag) = load_manual_transition_job_record_with_etag(ecstore.clone(), job_id)
            .await
            .expect("running job record should load during reconciliation");
        recovered.lease_id = Uuid::new_v4();
        recovered.owner_id = "owner-b".to_string();
        save_manual_transition_job_record_if_current(ecstore.clone(), &recovered, &etag)
            .await
            .expect("recovery owner should replace the lease");
        barrier.release();

        let error = heartbeat
            .await
            .expect("heartbeat task should join")
            .expect_err("stale reconciliation must reject the recovery lease");
        assert_eq!(error, Error::PreconditionFailed);
        let loaded = load_manual_transition_job_record(ecstore, job_id)
            .await
            .expect("recovered job record should load");
        assert_eq!(loaded.lease_id, recovered.lease_id);
        assert_eq!(loaded.owner_id, "owner-b");
        assert_eq!(loaded.state, ManualTransitionJobState::Running);
        assert_eq!(loaded.report.enqueued, 0);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_progress_does_not_regress_newer_admission_lease() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let record = ManualTransitionJobRecord::new(
            job_id,
            "manual-progress-admission-order-bucket",
            &ManualTransitionRunOptions::default(),
            "owner-a",
        );
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("running job record should save");
        let mut newer_admission = ManualTransitionScopeAdmission::from_job(&record);
        newer_admission.lease_expires_at_unix_nanos = newer_admission.lease_expires_at_unix_nanos.saturating_add(60_000_000_000);
        newer_admission.updated_at_unix_nanos = newer_admission.updated_at_unix_nanos.saturating_add(60_000_000_000);
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &newer_admission)
            .await
            .expect("newer scope admission should save");

        persist_manual_transition_job_progress_if_owned(
            ecstore.clone(),
            job_id,
            record.lease_id,
            &ManualTransitionRunReport {
                scanned: 1000,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect("progress should preserve the newer admission lease");

        let admission = load_manual_transition_scope_admission(ecstore, &record.scope_key)
            .await
            .expect("scope admission should load");
        assert_eq!(admission.lease_expires_at_unix_nanos, newer_admission.lease_expires_at_unix_nanos);
        assert_eq!(admission.updated_at_unix_nanos, newer_admission.updated_at_unix_nanos);
    }

    #[tokio::test]
    async fn manual_transition_page_checkpoint_persists_resume_cursor() {
        let observed = Arc::new(StdMutex::new(Vec::new()));
        let sink_observed = Arc::clone(&observed);
        let options = ManualTransitionRunOptions {
            progress_sink: Some(Arc::new(move |report| {
                let sink_observed = Arc::clone(&sink_observed);
                Box::pin(async move {
                    sink_observed.lock().expect("observed reports mutex poisoned").push(report);
                    Ok(())
                })
            })),
            ..Default::default()
        };
        let report = ManualTransitionRunReport {
            bucket: "bucket".to_string(),
            prefix: "logs/".to_string(),
            scanned: 1000,
            ..Default::default()
        };

        persist_manual_transition_page_checkpoint(&options, &report, Some("logs/page-end".to_string()), Some("null".to_string()))
            .await
            .expect("page checkpoint should persist");

        assert!(report.continuation_token.is_none());
        let observed = observed.lock().expect("observed reports mutex poisoned");
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].scanned, 1000);
        assert_eq!(observed[0].next_marker.as_deref(), Some("logs/page-end"));
        assert_eq!(observed[0].next_version_idmarker.as_deref(), Some("null"));
        let token = observed[0]
            .continuation_token
            .as_deref()
            .expect("checkpoint should carry resume token");
        let (marker, version_marker) =
            decode_manual_transition_continuation_token(token).expect("checkpoint token should decode");
        assert_eq!(marker.as_deref(), Some("logs/page-end"));
        assert_eq!(version_marker.as_deref(), Some("null"));
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_page_checkpoint_persists_durable_job_progress() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-checkpoint-{}", Uuid::new_v4().simple());
        let prefix = "logs/";
        let options = ManualTransitionRunOptions {
            prefix: prefix.to_string(),
            tier: Some("WARM".to_string()),
            dry_run: true,
            max_objects: Some(100),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &options, "old-owner");
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("expired job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("expired scope admission should save");
        let checkpoint_options = ManualTransitionRunOptions {
            progress_sink: Some(manual_transition_recovery_progress_sink(ecstore.clone(), job_id, record.lease_id)),
            ..options
        };
        let report = ManualTransitionRunReport {
            bucket: bucket.clone(),
            prefix: prefix.to_string(),
            tier: Some("WARM".to_string()),
            lifecycle_config_found: true,
            scanned: 37,
            eligible: 11,
            enqueued: 5,
            ..Default::default()
        };

        persist_manual_transition_page_checkpoint(
            &checkpoint_options,
            &report,
            Some("logs/page-end".to_string()),
            Some("null".to_string()),
        )
        .await
        .expect("page checkpoint should persist through the durable job progress sink");

        let loaded = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("checkpointed job should reload");
        assert_eq!(loaded.state, ManualTransitionJobState::Running);
        assert_eq!(loaded.report.scanned, 37);
        assert_eq!(loaded.report.eligible, 11);
        assert_eq!(loaded.report.enqueued, 5);
        assert_eq!(loaded.cursor_revision, Some(37));
        assert!(loaded.lease_expires_at_unix_nanos > 0);
        let token = loaded
            .report
            .continuation_token
            .as_deref()
            .expect("durable checkpoint should persist the opaque resume token");
        let (marker, version_marker) =
            decode_manual_transition_continuation_token(token).expect("durable checkpoint token should decode");
        assert_eq!(marker.as_deref(), Some("logs/page-end"));
        assert_eq!(version_marker.as_deref(), Some("null"));

        let admission = load_manual_transition_scope_admission(ecstore.clone(), &record.scope_key)
            .await
            .expect("checkpoint should keep the scope admission aligned with the renewed job lease");
        assert_eq!(admission.job_id, job_id);
        assert_eq!(admission.lease_id, loaded.lease_id);
        assert_eq!(admission.lease_expires_at_unix_nanos, loaded.lease_expires_at_unix_nanos);

        let mut same_marker_report = report.clone();
        same_marker_report.scanned += 1;
        persist_manual_transition_page_checkpoint(
            &checkpoint_options,
            &same_marker_report,
            Some("logs/page-end".to_string()),
            Some("opaque-next-version".to_string()),
        )
        .await
        .expect("same-marker version checkpoint should persist through the durable progress sink");
        let same_marker_checkpointed = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("same-marker version checkpoint should reload");
        assert_eq!(same_marker_checkpointed.cursor_revision, Some(38));
        let (_, version_marker) = decode_manual_transition_continuation_token(
            same_marker_checkpointed
                .report
                .continuation_token
                .as_deref()
                .expect("same-marker version checkpoint should persist a cursor"),
        )
        .expect("same-marker version cursor should decode");
        assert_eq!(version_marker.as_deref(), Some("opaque-next-version"));

        create_test_bucket(&ecstore, &bucket).await;
        let lifecycle_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>manual-checkpoint</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>{prefix}</Prefix>
    </Filter>
    <Transition>
      <Days>1</Days>
      <StorageClass>WARM</StorageClass>
    </Transition>
  </Rule>
</LifecycleConfiguration>"#
        );
        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("manual transition lifecycle metadata should be stored");
        for index in 0..=1000 {
            let object = format!("{prefix}obj-{index:04}");
            let mut reader = PutObjReader::from_vec(b"manual checkpoint payload".to_vec());
            ecstore
                .put_object(
                    &bucket,
                    &object,
                    &mut reader,
                    &ObjectOptions {
                        mod_time: Some(OffsetDateTime::now_utc() - time::Duration::days(2)),
                        ..Default::default()
                    },
                )
                .await
                .expect("manual transition checkpoint object should be created");
        }

        let production_path_options = ManualTransitionRunOptions {
            prefix: prefix.to_string(),
            tier: Some("WARM".to_string()),
            dry_run: true,
            progress_sink: Some(manual_transition_recovery_progress_sink(ecstore.clone(), job_id, record.lease_id)),
            ..Default::default()
        };
        let final_report = enqueue_transition_for_existing_objects_scoped(ecstore.clone(), &bucket, production_path_options)
            .await
            .expect("manual transition scan should cross a durable page checkpoint");
        assert_eq!(final_report.scanned, 1001);

        let checkpointed = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("production checkpointed job should reload");
        assert_eq!(checkpointed.state, ManualTransitionJobState::Running);
        assert_eq!(checkpointed.report.scanned, 1000);
        assert_eq!(checkpointed.report.eligible, 1000);
        assert_eq!(checkpointed.report.dry_run_eligible, 1000);
        assert_eq!(checkpointed.cursor_revision, Some(1000));
        let token = checkpointed
            .report
            .continuation_token
            .as_deref()
            .expect("production page rollover should persist a durable resume token");
        let (marker, version_marker) =
            decode_manual_transition_continuation_token(token).expect("production checkpoint token should decode");
        let expected_marker = format!("{prefix}obj-0999");
        let marker = marker
            .as_deref()
            .expect("production checkpoint should persist an object marker");
        let marker_suffix = marker
            .strip_prefix(&expected_marker)
            .expect("production checkpoint should stop at the first page boundary");
        assert!(
            marker_suffix.is_empty() || marker_suffix.starts_with("[rustfs_cache:v2,"),
            "unexpected production checkpoint marker suffix: {marker_suffix}"
        );
        assert_eq!(version_marker.as_deref(), Some("null"));
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_active_cancel_returns_resume_cursor_after_progress() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("manual-active-cancel-{}", Uuid::new_v4().simple());
        let prefix = "manual-active-cancel/";
        let keys = [format!("{prefix}obj-001"), format!("{prefix}obj-002")];
        create_test_bucket(&ecstore, &bucket).await;
        let lifecycle_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>manual-active-cancel</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>{prefix}</Prefix>
    </Filter>
    <Transition>
      <Days>1</Days>
      <StorageClass>WARM</StorageClass>
    </Transition>
  </Rule>
</LifecycleConfiguration>"#
        );
        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("manual transition lifecycle metadata should be stored");
        for key in &keys {
            let mut reader = PutObjReader::from_vec(b"manual active cancel payload".to_vec());
            ecstore
                .put_object(
                    &bucket,
                    key,
                    &mut reader,
                    &ObjectOptions {
                        mod_time: Some(OffsetDateTime::now_utc() - time::Duration::days(2)),
                        ..Default::default()
                    },
                )
                .await
                .expect("manual transition object should be created");
        }

        let cancel_polls = Arc::new(AtomicUsize::new(0));
        let cancel_polls_for_check = Arc::clone(&cancel_polls);
        let options = ManualTransitionRunOptions {
            prefix: prefix.to_string(),
            tier: Some("WARM".to_string()),
            dry_run: true,
            max_objects: Some(10),
            cancel_check: Some(Arc::new(move || {
                let cancel_polls_for_check = Arc::clone(&cancel_polls_for_check);
                Box::pin(async move { cancel_polls_for_check.fetch_add(1, Ordering::SeqCst) >= 1 })
            })),
            ..Default::default()
        };

        let report = enqueue_transition_for_existing_objects_scoped(ecstore.clone(), &bucket, options)
            .await
            .expect("manual transition dry-run scan should stop on active cancel");

        assert!(report.cancelled);
        assert!(report.was_truncated());
        assert_eq!(report.scanned, 1);
        assert_eq!(report.eligible, 1);
        assert_eq!(report.dry_run_eligible, 1);
        assert_eq!(report.enqueued, 0);
        assert_eq!(report.transition_completed, 0);
        assert_eq!(report.transition_failed, 0);
        assert_eq!(report.tier_failure, 0);
        assert_eq!(report.next_marker.as_deref(), Some(keys[0].as_str()));
        assert_eq!(report.next_version_idmarker.as_deref(), Some("null"));
        let token = report
            .continuation_token
            .as_deref()
            .expect("cancelled active scan should expose an opaque resume token");
        let (marker, version_marker) =
            decode_manual_transition_continuation_token(token).expect("cancel continuation token should decode");
        assert_eq!(marker.as_deref(), Some(keys[0].as_str()));
        assert_eq!(version_marker.as_deref(), Some("null"));
        assert_eq!(cancel_polls.load(Ordering::SeqCst), 2);

        let resumed = enqueue_transition_for_existing_objects_scoped(
            ecstore,
            &bucket,
            ManualTransitionRunOptions {
                prefix: prefix.to_string(),
                continuation_token: Some(token.to_string()),
                tier: Some("WARM".to_string()),
                dry_run: true,
                max_objects: Some(10),
                ..Default::default()
            },
        )
        .await
        .expect("manual transition dry-run scan should resume after active cancel");

        assert!(!resumed.cancelled);
        assert!(!resumed.was_truncated());
        assert_eq!(resumed.scanned, 1);
        assert_eq!(resumed.eligible, 1);
        assert_eq!(resumed.dry_run_eligible, 1);
        assert_eq!(resumed.enqueued, 0);
        assert!(resumed.continuation_token.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_recovery_replays_expired_running_record() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            continuation_token: Some(
                encode_manual_transition_continuation_token(Some("logs/page-end".to_string()), Some("null".to_string()))
                    .expect("resume token should encode"),
            ),
            max_objects: Some(7),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(job_id, "manual-recovery-bucket", &options, "old-owner");
        record.report.continuation_token.clone_from(&options.continuation_token);
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("expired job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("expired scope admission should save");

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("manual transition recovery should run");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Resumed);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("recovered job should load");
        assert_eq!(recovered.state, ManualTransitionJobState::Completed);
        assert_eq!(recovered.max_objects, Some(7));
        assert_eq!(recovered.report.bucket, "manual-recovery-bucket");
        assert_eq!(recovered.report.prefix, "logs/");
        assert!(!recovered.report.lifecycle_config_found);
        assert!(recovered.report.continuation_token.is_none());
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &recovered.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "completed recovery must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_recovery_marks_unknown_when_cursor_would_skip_pending_work() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let continuation_token =
            encode_manual_transition_continuation_token(Some("logs/page-end".to_string()), Some("null".to_string()))
                .expect("resume token should encode");
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            continuation_token: Some(continuation_token.clone()),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(job_id, "manual-recovery-pending-page-bucket", &options, "old-owner");
        record.report.continuation_token = Some(continuation_token);
        record.report.enqueued = 2;
        record.report.transition_completed = 1;
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("expired job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("expired scope admission should save");

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("manual transition recovery should process pending cursor jobs");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Unknown);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("unknown job should load");
        assert_eq!(recovered.state, ManualTransitionJobState::Unknown);
        assert_eq!(recovered.report.enqueued, 2);
        assert_eq!(recovered.report.transition_completed, 1);
        assert!(
            recovered
                .error
                .as_deref()
                .is_some_and(|error| error.contains("page/task journal"))
        );
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "unknown recovery must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_recovery_marks_unknown_for_cursor_pending_work_when_queue_is_busy() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let continuation_token =
            encode_manual_transition_continuation_token(Some("logs/page-end".to_string()), Some("null".to_string()))
                .expect("resume token should encode");
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            continuation_token: Some(continuation_token.clone()),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(job_id, "manual-recovery-busy-queue-bucket", &options, "old-owner");
        record.report.continuation_token = Some(continuation_token);
        record.report.enqueued = 2;
        record.report.transition_completed = 1;
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("expired job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("expired scope admission should save");

        let outcome = recover_manual_transition_job(
            ecstore.clone(),
            job_id,
            ManualTransitionQueueSnapshot {
                queued: 1,
                ..Default::default()
            },
        )
        .await
        .expect("busy queue recovery should fail closed for pending cursor jobs");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Unknown);
        let loaded = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("unknown job should remain loadable");
        assert_eq!(loaded.state, ManualTransitionJobState::Unknown);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "unknown recovery must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_recovery_cancels_cursor_pending_work_when_requested() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let continuation_token =
            encode_manual_transition_continuation_token(Some("logs/page-end".to_string()), Some("null".to_string()))
                .expect("resume token should encode");
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            continuation_token: Some(continuation_token.clone()),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(job_id, "manual-recovery-cancel-pending-bucket", &options, "old-owner");
        record.report.continuation_token = Some(continuation_token);
        record.report.enqueued = 2;
        record.report.transition_completed = 1;
        record.lease_expires_at_unix_nanos = 0;
        record.mark_cancel_requested();
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("expired cancelled job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("expired cancelled scope admission should save");

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("manual transition recovery should cancel requested pending cursor jobs");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Cancelled);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("cancelled job should load");
        assert_eq!(recovered.state, ManualTransitionJobState::Cancelled);
        assert!(recovered.cancel_requested);
        assert!(recovered.error.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_recovery_marks_unknown_when_completed_scan_lost_worker_results() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            ..Default::default()
        };
        let mut record =
            ManualTransitionJobRecord::new(job_id, "manual-recovery-lost-worker-result-bucket", &options, "old-owner");
        record.complete(
            ManualTransitionRunReport {
                bucket: record.bucket.clone(),
                prefix: record.prefix.clone(),
                enqueued: 2,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("expired job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("expired scope admission should save");

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("manual transition recovery should process completed scans with lost worker results");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Unknown);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("unknown job should load");
        assert_eq!(recovered.state, ManualTransitionJobState::Unknown);
        assert!(
            recovered
                .error
                .as_deref()
                .is_some_and(|error| error.contains("worker result"))
        );
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "unknown recovery must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_recovery_drains_multiple_record_pages() {
        let (_paths, ecstore) = setup_test_env().await;
        let mut job_ids = Vec::new();

        for bucket in ["manual-recovery-page-a", "manual-recovery-page-b"] {
            let job_id = Uuid::new_v4();
            let options = ManualTransitionRunOptions {
                prefix: "logs/".to_string(),
                ..Default::default()
            };
            let mut record = ManualTransitionJobRecord::new(job_id, bucket, &options, "old-owner");
            record.lease_expires_at_unix_nanos = 0;
            save_manual_transition_job_record(ecstore.clone(), &record)
                .await
                .expect("expired job record should save");
            save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
                .await
                .expect("expired scope admission should save");
            job_ids.push((job_id, record.scope_key.clone()));
        }

        let stats = recover_manual_transition_jobs(ecstore.clone(), 1)
            .await
            .expect("manual transition recovery should drain all pages");

        assert_eq!(stats.resumed, 2);
        assert_eq!(stats.failed, 0);
        assert!(!stats.truncated);
        assert!(stats.next_marker.is_none());
        for (job_id, scope_key) in job_ids {
            let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
                .await
                .expect("recovered job should load");
            assert_eq!(recovered.state, ManualTransitionJobState::Completed);
            assert!(
                matches!(
                    load_manual_transition_scope_admission(ecstore.clone(), &scope_key).await,
                    Err(Error::ConfigNotFound)
                ),
                "completed recovery must release every scope admission"
            );
        }
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_recovery_completes_expired_cancelled_record() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let continuation_token =
            encode_manual_transition_continuation_token(Some("logs/page-end".to_string()), Some("null".to_string()))
                .expect("resume token should encode");
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            continuation_token: Some(continuation_token.clone()),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(job_id, "manual-recovery-cancel-bucket", &options, "old-owner");
        record.report.continuation_token = Some(continuation_token);
        record.report.enqueued = 2;
        record.report.transition_completed = 2;
        record.lease_expires_at_unix_nanos = 0;
        record.mark_cancel_requested();
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("expired cancelled job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("expired cancelled scope admission should save");

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("manual transition recovery should process cancelled jobs");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Cancelled);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("cancelled job should load");
        assert_eq!(recovered.state, ManualTransitionJobState::Cancelled);
        assert!(recovered.cancel_requested);
        assert!(recovered.report.cancelled);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "cancelled recovery must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_worker_result_duplicate_marker_is_noop() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-worker-result-{}", job_id.simple());
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &options, "worker-owner");
        record.complete(
            ManualTransitionRunReport {
                bucket: bucket.clone(),
                enqueued: 2,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("worker result job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("worker result admission should save");

        let first_key = manual_transition_worker_result_task_key(&bucket, "logs/a", None);
        let first = record_manual_transition_worker_result(
            ecstore.clone(),
            job_id,
            &first_key,
            ManualTransitionWorkerResult::Completed,
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect("first worker result should persist");
        assert_eq!(first.state, ManualTransitionJobState::Running);
        assert_eq!(first.report.transition_completed, 0);
        assert_eq!(first.report.transition_failed, 0);

        let duplicate = record_manual_transition_worker_result(
            ecstore.clone(),
            job_id,
            &first_key,
            ManualTransitionWorkerResult::Completed,
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect("duplicate worker result should be idempotent");
        assert_eq!(duplicate.state, ManualTransitionJobState::Running);
        assert_eq!(duplicate.report.transition_completed, 0);
        assert_eq!(duplicate.report.transition_failed, 0);

        let second_key = manual_transition_worker_result_task_key(&bucket, "logs/b", None);
        let pending_record = record_manual_transition_worker_result(
            ecstore.clone(),
            job_id,
            &second_key,
            ManualTransitionWorkerResult::TierFailure,
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect("second distinct worker result should persist");
        assert_eq!(pending_record.state, ManualTransitionJobState::Running);
        assert_eq!(pending_record.report.transition_completed, 0);
        assert_eq!(pending_record.report.transition_failed, 0);

        let final_record =
            reconcile_manual_transition_worker_results(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
                .await
                .expect("worker result journal should reconcile");
        assert_eq!(final_record.state, ManualTransitionJobState::Partial);
        assert_eq!(final_record.report.transition_completed, 1);
        assert_eq!(final_record.report.transition_failed, 1);
        assert_eq!(final_record.report.tier_failure, 1);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "terminal worker result must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_worker_result_stores_failure_reason() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-worker-failure-reason-{}", job_id.simple());
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &options, "worker-owner");
        record.complete(
            ManualTransitionRunReport {
                bucket: bucket.clone(),
                enqueued: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("worker result job record should save");

        let task_key = manual_transition_worker_result_task_key(&bucket, "logs/fail", None);
        let pending_record = record_manual_transition_worker_result_with_reason(
            ecstore.clone(),
            job_id,
            &task_key,
            ManualTransitionWorkerResult::TierFailure,
            ManualTransitionQueueSnapshot::default(),
            Some(ManualTransitionWorkerFailureReason::Network),
        )
        .await
        .expect("worker result with failure reason should persist");
        assert!(pending_record.report.tier_failure_by_reason.is_empty());
        assert_eq!(pending_record.report.transition_failed, 0);

        let final_record = reconcile_manual_transition_worker_results(ecstore, job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("worker failure reason should reconcile");
        assert_eq!(
            final_record
                .report
                .tier_failure_by_reason
                .get(&ManualTransitionWorkerFailureReason::Network),
            Some(&1)
        );
        assert_eq!(final_record.report.transition_failed, 1);
        assert_eq!(final_record.report.tier_failure, 1);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_worker_result_reconcile_applies_marker_and_releases_admission() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-worker-reconcile-{}", job_id.simple());
        let options = ManualTransitionRunOptions::default();
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &options, "worker-owner");
        record.complete(
            ManualTransitionRunReport {
                bucket: bucket.clone(),
                enqueued: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("worker reconcile job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("worker reconcile admission should save");

        let task_key = manual_transition_worker_result_task_key(&bucket, "logs/a", None);
        let marker = ManualTransitionWorkerResultRecord::new(job_id, &task_key, ManualTransitionWorkerResult::Completed);
        assert!(
            save_manual_transition_worker_result_if_absent(ecstore.clone(), &marker)
                .await
                .expect("worker result marker should save"),
            "new worker result marker must be created once"
        );
        assert!(
            !save_manual_transition_worker_result_if_absent(ecstore.clone(), &marker)
                .await
                .expect("duplicate worker result marker should not fail"),
            "duplicate worker result marker must be reported as existing"
        );
        let duplicate_noop = record_manual_transition_worker_result(
            ecstore.clone(),
            job_id,
            &task_key,
            ManualTransitionWorkerResult::Completed,
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect("duplicate worker result should not apply journal counts");
        assert_eq!(duplicate_noop.state, ManualTransitionJobState::Running);
        assert_eq!(duplicate_noop.report.transition_completed, 0);
        assert_eq!(duplicate_noop.report.transition_failed, 0);

        let reconciled =
            reconcile_manual_transition_worker_results(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
                .await
                .expect("worker result marker should reconcile into job record");

        assert_eq!(reconciled.state, ManualTransitionJobState::Completed);
        assert_eq!(reconciled.report.transition_completed, 1);
        assert_eq!(reconciled.report.transition_failed, 0);
        assert_eq!(reconciled.report.tier_failure, 0);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "terminal reconcile must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_worker_result_heartbeat_applies_marker_before_record_update() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-worker-heartbeat-{}", job_id.simple());
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &ManualTransitionRunOptions::default(), "worker-owner");
        record.complete(
            ManualTransitionRunReport {
                bucket: bucket.clone(),
                enqueued: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("worker heartbeat job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("worker heartbeat admission should save");
        let task_key = manual_transition_worker_result_task_key(&bucket, "logs/a", None);
        let marker = ManualTransitionWorkerResultRecord::new(job_id, &task_key, ManualTransitionWorkerResult::Completed);
        assert!(
            save_manual_transition_worker_result_if_absent(ecstore.clone(), &marker)
                .await
                .expect("worker result marker should save"),
            "new worker result marker must be created"
        );

        let renewed = renew_manual_transition_job_lease_if_owned(
            ecstore.clone(),
            job_id,
            record.lease_id,
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect("heartbeat should reconcile marker before unknown fallback");

        assert_eq!(renewed.state, ManualTransitionJobState::Completed);
        assert_eq!(renewed.report.transition_completed, 1);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "terminal heartbeat reconcile must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_worker_result_heartbeat_uses_task_journal_enqueued_floor() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-task-heartbeat-{}", job_id.simple());
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &ManualTransitionRunOptions::default(), "worker-owner");
        record.scan_completed = true;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("worker heartbeat task journal job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("worker heartbeat task journal admission should save");
        let task_key = manual_transition_worker_result_task_key(&bucket, "logs/a", None);
        let task_marker = ManualTransitionTaskRecord::new(job_id, &task_key, &bucket, "logs/a", None, "WARM");
        assert!(
            save_manual_transition_task_if_absent(ecstore.clone(), &task_marker)
                .await
                .expect("task journal marker should save"),
            "new task journal marker must be created"
        );
        let result_marker = ManualTransitionWorkerResultRecord::new(job_id, &task_key, ManualTransitionWorkerResult::Completed);
        assert!(
            save_manual_transition_worker_result_if_absent(ecstore.clone(), &result_marker)
                .await
                .expect("worker result marker should save"),
            "new worker result marker must be created"
        );

        let renewed = renew_manual_transition_job_lease_if_owned(
            ecstore.clone(),
            job_id,
            record.lease_id,
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect("heartbeat should reconcile task and result journals");

        assert_eq!(renewed.state, ManualTransitionJobState::Completed);
        assert_eq!(renewed.report.enqueued, 1);
        assert_eq!(renewed.report.transition_completed, 1);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "terminal task journal heartbeat must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_worker_result_recovery_applies_marker_before_record_update() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-worker-recovery-{}", job_id.simple());
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &ManualTransitionRunOptions::default(), "old-owner");
        record.complete(
            ManualTransitionRunReport {
                bucket: bucket.clone(),
                enqueued: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("worker recovery job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("worker recovery admission should save");
        let task_key = manual_transition_worker_result_task_key(&bucket, "logs/a", None);
        let marker = ManualTransitionWorkerResultRecord::new(job_id, &task_key, ManualTransitionWorkerResult::Completed);
        assert!(
            save_manual_transition_worker_result_if_absent(ecstore.clone(), &marker)
                .await
                .expect("worker result marker should save"),
            "new worker result marker must be created"
        );

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("recovery should reconcile marker before unknown fallback");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Resumed);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("reconciled job should load");
        assert_eq!(recovered.state, ManualTransitionJobState::Completed);
        assert_eq!(recovered.report.transition_completed, 1);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "terminal recovery reconcile must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_recovery_uses_task_journal_as_enqueued_floor() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-task-floor-{}", job_id.simple());
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &ManualTransitionRunOptions::default(), "old-owner");
        record.scan_completed = true;
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("task journal recovery job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("task journal recovery admission should save");
        let task_key = manual_transition_worker_result_task_key(&bucket, "logs/a", None);
        let task_marker = ManualTransitionTaskRecord::new(job_id, &task_key, &bucket, "logs/a", None, "WARM");
        assert!(
            save_manual_transition_task_if_absent(ecstore.clone(), &task_marker)
                .await
                .expect("task journal marker should save"),
            "new task journal marker must be created"
        );
        let result_marker = ManualTransitionWorkerResultRecord::new(job_id, &task_key, ManualTransitionWorkerResult::Completed);
        assert!(
            save_manual_transition_worker_result_if_absent(ecstore.clone(), &result_marker)
                .await
                .expect("worker result marker should save"),
            "new worker result marker must be created"
        );

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("recovery should reconcile task and result journals");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Resumed);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("reconciled task journal job should load");
        assert_eq!(recovered.state, ManualTransitionJobState::Completed);
        assert_eq!(recovered.report.enqueued, 1);
        assert_eq!(recovered.report.transition_completed, 1);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "terminal task journal recovery must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_recovery_replays_task_journal_without_result() {
        let (_paths, ecstore) = setup_test_env().await;
        let transition_state = runtime_sources::transition_state_handle();
        let original_workers = transition_state.num_workers.load(Ordering::SeqCst);
        let absolute_max = resolve_transition_workers_absolute_max();
        TransitionState::resize_workers_to(ecstore.clone(), 0, 0, absolute_max);
        let pending_before = transition_state.pending_tasks();
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-task-lost-result-{}", job_id.simple());
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &ManualTransitionRunOptions::default(), "old-owner");
        record.scan_completed = true;
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("lost-result job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("lost-result admission should save");
        let task_key = manual_transition_worker_result_task_key(&bucket, "logs/a", None);
        let task_marker = ManualTransitionTaskRecord::new(job_id, &task_key, &bucket, "logs/a", None, "WARM");
        assert!(
            save_manual_transition_task_if_absent(ecstore.clone(), &task_marker)
                .await
                .expect("task journal marker should save"),
            "new task journal marker must be created"
        );

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("recovery should replay a task journal marker with no worker result");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Resumed);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("replayed task journal job should load");
        assert_eq!(recovered.state, ManualTransitionJobState::Running);
        assert_eq!(recovered.report.enqueued, 1);
        assert_eq!(recovered.report.transition_completed, 0);
        assert_eq!(recovered.report.transition_failed, 0);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore.clone(), &record.scope_key).await,
                Ok(admission) if admission.job_id == job_id
            ),
            "replayed task journal recovery must keep the scope admission until worker results arrive"
        );
        if transition_state.pending_tasks() > pending_before {
            let _ = transition_state.transition_rx.try_recv();
        }
        TransitionState::resize_workers_to(ecstore, original_workers, original_workers, absolute_max);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_worker_result_recovery_marks_unknown_for_corrupt_marker() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = format!("manual-worker-corrupt-{}", job_id.simple());
        let mut record = ManualTransitionJobRecord::new(job_id, &bucket, &ManualTransitionRunOptions::default(), "old-owner");
        record.complete(
            ManualTransitionRunReport {
                bucket: bucket.clone(),
                enqueued: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("corrupt marker job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("corrupt marker admission should save");
        let task_key = manual_transition_worker_result_task_key(&bucket, "logs/a", None);
        let marker = ManualTransitionWorkerResultRecord::new(job_id, &task_key, ManualTransitionWorkerResult::Completed);
        let encoded = marker.encode().expect("worker result marker should encode");
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).expect("worker result marker should be json");
        value["record"]["result"] = serde_json::Value::String("tier_failure".to_string());
        let object = manual_transition_worker_result_object_name(job_id, &task_key).expect("worker result object should encode");
        config_boundary::save_config(
            ecstore.clone(),
            &object,
            serde_json::to_vec(&value).expect("corrupt marker should encode"),
        )
        .await
        .expect("corrupt worker result marker should save");

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("recovery should fail closed on corrupt marker");

        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Unknown);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("unknown job should load");
        assert_eq!(recovered.state, ManualTransitionJobState::Unknown);
        assert!(
            recovered
                .error
                .as_deref()
                .is_some_and(|error| error.contains("worker result journal is corrupt"))
        );
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "corrupt marker unknown recovery must release the scope admission"
        );
    }

    #[test]
    fn manual_transition_cancelled_report_is_partial_and_resumable() {
        let report = ManualTransitionRunReport {
            cancelled: true,
            continuation_token: Some("opaque".to_string()),
            ..Default::default()
        };

        assert!(report.was_truncated());
        assert!(!report.has_partial_enqueue());
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_job_cancel_marks_running_record_only() {
        let (_paths, ecstore) = setup_test_env().await;
        let running_id = Uuid::new_v4();
        let running = ManualTransitionJobRecord::new(
            running_id,
            "manual-cancel-running-bucket",
            &ManualTransitionRunOptions::default(),
            "owner-a",
        );
        save_manual_transition_job_record(ecstore.clone(), &running)
            .await
            .expect("running job record should save");

        let cancelled = request_manual_transition_job_cancel(ecstore.clone(), running_id)
            .await
            .expect("running job cancel request should persist");

        assert_eq!(cancelled.state, ManualTransitionJobState::Running);
        assert!(cancelled.cancel_requested);
        let loaded = load_manual_transition_job_record(ecstore.clone(), running_id)
            .await
            .expect("cancelled running job should reload");
        assert!(loaded.cancel_requested);

        let terminal_id = Uuid::new_v4();
        let mut terminal = ManualTransitionJobRecord::new(
            terminal_id,
            "manual-cancel-terminal-bucket",
            &ManualTransitionRunOptions::default(),
            "owner-a",
        );
        terminal.complete(
            ManualTransitionRunReport {
                bucket: "manual-cancel-terminal-bucket".to_string(),
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        );
        save_manual_transition_job_record(ecstore.clone(), &terminal)
            .await
            .expect("terminal job record should save");

        let after_terminal_cancel = request_manual_transition_job_cancel(ecstore, terminal_id)
            .await
            .expect("terminal job cancel should be idempotent");

        assert_eq!(after_terminal_cancel.state, ManualTransitionJobState::Completed);
        assert!(!after_terminal_cancel.cancel_requested);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_progress_checkpoint_survives_reload_and_recovery() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let bucket = "manual-progress-checkpoint-bucket";
        let continuation_token =
            encode_manual_transition_continuation_token(Some("logs/page-end".to_string()), Some("null".to_string()))
                .expect("resume token should encode");
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(job_id, bucket, &options, "owner-a");
        record.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("running job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("running scope admission should save");

        let checkpointed = persist_manual_transition_job_progress_if_owned(
            ecstore.clone(),
            job_id,
            record.lease_id,
            &ManualTransitionRunReport {
                bucket: bucket.to_string(),
                prefix: "logs/".to_string(),
                scanned: 2,
                eligible: 2,
                enqueued: 2,
                continuation_token: Some(continuation_token.clone()),
                truncated_by_limit: true,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect("running progress checkpoint should persist");

        assert_eq!(checkpointed.report.continuation_token.as_deref(), Some(continuation_token.as_str()));
        assert_eq!(checkpointed.report.enqueued, 2);

        let reloaded = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("checkpointed job should reload");
        assert_eq!(reloaded.report.continuation_token.as_deref(), Some(continuation_token.as_str()));
        assert_eq!(reloaded.report.scanned, 2);
        assert_eq!(reloaded.report.enqueued, 2);

        let mut expired = reloaded.clone();
        expired.lease_expires_at_unix_nanos = 0;
        save_manual_transition_job_record(ecstore.clone(), &expired)
            .await
            .expect("checkpointed job owner loss should persist");

        let outcome = recover_manual_transition_job(ecstore.clone(), job_id, ManualTransitionQueueSnapshot::default())
            .await
            .expect("recovery should fail closed when checkpointed page has pending worker results");
        assert_eq!(outcome, ManualTransitionJobRecoveryOutcome::Unknown);
        let recovered = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("unknown checkpointed job should reload");
        assert_eq!(recovered.state, ManualTransitionJobState::Unknown);
        assert!(
            recovered
                .error
                .as_deref()
                .is_some_and(|error| error.contains("page/task journal")),
            "unknown checkpoint recovery must retain page/task journal context: {recovered:#?}"
        );
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "unknown checkpoint recovery must release scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_heartbeat_persists_backpressure_status_snapshot() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some("WARM".to_string()),
            max_objects: Some(25),
            ..Default::default()
        };
        let mut record = ManualTransitionJobRecord::new(job_id, "manual-heartbeat-backpressure-bucket", &options, "owner-a");
        record.report.scanned = 17;
        record.report.eligible = 11;
        record.report.enqueued = 3;
        let old_lease_id = record.lease_id;
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("running job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("running scope admission should save");
        let queue_snapshot = ManualTransitionQueueSnapshot {
            queue_capacity: 4,
            queued: 2,
            active: 1,
            workers: 2,
            queue_full: 5,
            queue_send_timeout: 7,
            compensation_pending: 3,
            compensation_running: 1,
        };

        let renewed = renew_manual_transition_job_lease_if_owned(ecstore.clone(), job_id, record.lease_id, queue_snapshot)
            .await
            .expect("running job heartbeat should persist queue pressure status");

        assert_eq!(renewed.state, ManualTransitionJobState::Running);
        assert_eq!(renewed.lease_id, old_lease_id);
        assert_eq!(renewed.queue_snapshot, queue_snapshot);
        assert_eq!(renewed.report.scanned, 17);
        assert_eq!(renewed.report.eligible, 11);
        assert_eq!(renewed.report.enqueued, 3);
        let loaded = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("renewed heartbeat should reload");
        assert_eq!(loaded.queue_snapshot, queue_snapshot);
        let admission = load_manual_transition_scope_admission(ecstore, &record.scope_key)
            .await
            .expect("heartbeat must keep scope admission aligned with the renewed job");
        assert_eq!(admission.job_id, job_id);
        assert_eq!(admission.lease_id, renewed.lease_id);
        assert_eq!(admission.lease_expires_at_unix_nanos, renewed.lease_expires_at_unix_nanos);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_job_lost_worker_results_mark_unknown_and_release_admission() {
        let (_paths, ecstore) = setup_test_env().await;
        let job_id = Uuid::new_v4();
        let mut record = ManualTransitionJobRecord::new(
            job_id,
            "manual-lost-worker-result-bucket",
            &ManualTransitionRunOptions::default(),
            "owner-a",
        );
        record.complete(
            ManualTransitionRunReport {
                bucket: "manual-lost-worker-result-bucket".to_string(),
                enqueued: 1,
                ..Default::default()
            },
            ManualTransitionQueueSnapshot {
                queued: 1,
                ..Default::default()
            },
        );
        save_manual_transition_job_record(ecstore.clone(), &record)
            .await
            .expect("running job record with pending worker result should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&record))
            .await
            .expect("running job admission should save");

        let renewed = renew_manual_transition_job_lease_if_owned(
            ecstore.clone(),
            job_id,
            record.lease_id,
            ManualTransitionQueueSnapshot::default(),
        )
        .await
        .expect("lost worker result should persist unknown state");

        assert_eq!(renewed.state, ManualTransitionJobState::Unknown);
        assert!(renewed.completed_at_unix_nanos.is_some());
        assert!(
            renewed.error.as_deref().is_some_and(|error| error.contains("worker result")),
            "unknown job should keep actionable lost-worker context: {renewed:#?}"
        );
        let loaded = load_manual_transition_job_record(ecstore.clone(), job_id)
            .await
            .expect("unknown job should reload");
        assert_eq!(loaded.state, ManualTransitionJobState::Unknown);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore, &record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "unknown lost-worker job must release the scope admission"
        );
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_admission_blocks_active_legacy_scope_record() {
        let (_paths, ecstore) = setup_test_env().await;
        let legacy_options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some("warm".to_string()),
            ..Default::default()
        };
        let legacy_id = Uuid::new_v4();
        let mut legacy = ManualTransitionJobRecord::new(legacy_id, "manual-legacy-scope-bucket", &legacy_options, "old-owner");
        legacy.scope_key = legacy_manual_transition_scope_key(&legacy.bucket, &legacy_options);
        save_manual_transition_job_record(ecstore.clone(), &legacy)
            .await
            .expect("legacy job record should save");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&legacy))
            .await
            .expect("legacy scope admission should save");

        let new_options = ManualTransitionRunOptions {
            prefix: "archive/".to_string(),
            tier: Some("cold".to_string()),
            ..Default::default()
        };
        let new_record = ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-legacy-scope-bucket", &new_options, "new-owner");
        save_manual_transition_job_record(ecstore.clone(), &new_record)
            .await
            .expect("new job record should save");

        let claim =
            claim_manual_transition_scope_admission(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&new_record))
                .await
                .expect("new bucket-level admission claim should resolve");

        let ManualTransitionScopeAdmissionClaim::Conflict(active) = claim else {
            panic!("active legacy job must block new bucket-level admission");
        };
        assert_eq!(active.job_id, legacy_id);
        assert_eq!(active.scope_key, legacy.scope_key);
        assert!(
            matches!(
                load_manual_transition_scope_admission(ecstore.clone(), &new_record.scope_key).await,
                Err(Error::ConfigNotFound)
            ),
            "conflicted bucket-level admission must be released"
        );

        let other_bucket_record =
            ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-legacy-scope-other-bucket", &legacy_options, "new-owner");
        save_manual_transition_job_record(ecstore.clone(), &other_bucket_record)
            .await
            .expect("other bucket job record should save");

        let other_bucket_claim = claim_manual_transition_scope_admission(
            ecstore.clone(),
            &ManualTransitionScopeAdmission::from_job(&other_bucket_record),
        )
        .await
        .expect("cross-bucket admission claim should resolve");

        assert_eq!(other_bucket_claim, ManualTransitionScopeAdmissionClaim::Claimed);
        let current = load_manual_transition_scope_admission(ecstore, &other_bucket_record.scope_key)
            .await
            .expect("other bucket admission should be saved");
        assert_eq!(current.job_id, other_bucket_record.job_id);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_scope_release_preserves_replaced_admission() {
        let (_paths, ecstore) = setup_test_env().await;
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            ..Default::default()
        };
        let first = ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-release-race-bucket", &options, "first-owner");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&first))
            .await
            .expect("first scope admission should save");
        let (_loaded, etag) = load_manual_transition_scope_admission_with_etag(ecstore.clone(), &first.scope_key)
            .await
            .expect("first scope admission should load with an ETag");

        let second = ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-release-race-bucket", &options, "second-owner");
        save_manual_transition_scope_admission_if_current(
            ecstore.clone(),
            &ManualTransitionScopeAdmission::from_job(&second),
            &etag,
        )
        .await
        .expect("second scope admission should replace first admission");

        let object = manual_transition_scope_record_object_name(&first.scope_key).expect("scope admission path should encode");
        let stale_delete = config_boundary::delete_config_if_match(ecstore.clone(), &object, &etag)
            .await
            .expect_err("stale ETag must not delete a replaced scope admission");
        assert_eq!(stale_delete, Error::PreconditionFailed);

        let released =
            delete_manual_transition_scope_admission_if_current(ecstore.clone(), &first.scope_key, first.job_id, first.lease_id)
                .await
                .expect("stale release should not fail");

        assert!(!released);
        let current = load_manual_transition_scope_admission(ecstore, &first.scope_key)
            .await
            .expect("replaced scope admission should remain");
        assert_eq!(current.job_id, second.job_id);
        assert_eq!(current.lease_id, second.lease_id);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_scope_missing_current_is_retryable_cas_miss() {
        let (_paths, ecstore) = setup_test_env().await;
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            ..Default::default()
        };
        let first = ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-replace-race-bucket", &options, "first-owner");
        save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&first))
            .await
            .expect("first scope admission should save");
        let (_loaded, etag) = load_manual_transition_scope_admission_with_etag(ecstore.clone(), &first.scope_key)
            .await
            .expect("first scope admission should load with an ETag");
        let object = manual_transition_scope_record_object_name(&first.scope_key).expect("scope admission path should encode");
        config_boundary::delete_config(ecstore.clone(), &object)
            .await
            .expect("current scope admission should be deleted");

        let replacement =
            ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-replace-race-bucket", &options, "replacement-owner");
        let stale_replace = save_manual_transition_scope_admission_if_current(
            ecstore.clone(),
            &ManualTransitionScopeAdmission::from_job(&replacement),
            &etag,
        )
        .await
        .expect_err("replacing a disappeared scope admission must report a CAS miss");
        assert_eq!(stale_replace, Error::PreconditionFailed);

        let claim =
            claim_manual_transition_scope_admission(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&replacement))
                .await
                .expect("replacement claim should recover through the create path");
        assert_eq!(claim, ManualTransitionScopeAdmissionClaim::Claimed);
        let current = load_manual_transition_scope_admission(ecstore, &replacement.scope_key)
            .await
            .expect("replacement scope admission should be saved");
        assert_eq!(current.job_id, replacement.job_id);
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_admission_reclaims_stale_scope_records() {
        let (_paths, ecstore) = setup_test_env().await;

        for stale_case in ["missing-job", "terminal-job"] {
            let bucket = format!("manual-stale-admission-{stale_case}");
            let options = ManualTransitionRunOptions {
                prefix: "logs/".to_string(),
                tier: Some("warm".to_string()),
                ..Default::default()
            };
            let mut stale = ManualTransitionJobRecord::new(Uuid::new_v4(), &bucket, &options, "old-owner");
            if stale_case == "missing-job" {
                stale.lease_expires_at_unix_nanos = OffsetDateTime::now_utc().unix_timestamp_nanos() - 1;
            }
            if stale_case == "terminal-job" {
                stale.complete(
                    ManualTransitionRunReport {
                        bucket: bucket.clone(),
                        ..Default::default()
                    },
                    ManualTransitionQueueSnapshot::default(),
                );
                save_manual_transition_job_record(ecstore.clone(), &stale)
                    .await
                    .expect("terminal stale job record should save");
            }
            save_manual_transition_scope_admission_if_absent(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&stale))
                .await
                .expect("stale scope admission should save");

            let replacement = ManualTransitionJobRecord::new(Uuid::new_v4(), &bucket, &options, "new-owner");
            save_manual_transition_job_record(ecstore.clone(), &replacement)
                .await
                .expect("replacement job record should save");

            let claim =
                claim_manual_transition_scope_admission(ecstore.clone(), &ManualTransitionScopeAdmission::from_job(&replacement))
                    .await
                    .expect("replacement claim should resolve");

            assert_eq!(claim, ManualTransitionScopeAdmissionClaim::Claimed);
            let loaded = load_manual_transition_scope_admission(ecstore.clone(), &replacement.scope_key)
                .await
                .expect("replacement scope admission should load");
            assert_eq!(loaded.job_id, replacement.job_id);
            assert_eq!(loaded.lease_id, replacement.lease_id);
            assert_eq!(loaded.owner_id, "new-owner");
        }
    }

    #[tokio::test]
    #[serial]
    async fn manual_transition_admission_concurrent_same_scope_writes_is_singleton() {
        let (_paths, ecstore) = setup_test_env().await;
        let options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some("warm".to_string()),
            ..Default::default()
        };
        let first = ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-concurrent-scope-bucket", &options, "owner-a");
        let first_admission = ManualTransitionScopeAdmission::from_job(&first);
        let second = ManualTransitionJobRecord::new(Uuid::new_v4(), "manual-concurrent-scope-bucket", &options, "owner-b");
        let second_admission = ManualTransitionScopeAdmission::from_job(&second);

        let first_claim = claim_manual_transition_scope_admission(ecstore.clone(), &first_admission);
        let second_claim = claim_manual_transition_scope_admission(ecstore.clone(), &second_admission);
        let (first_result, second_result) = tokio::join!(first_claim, second_claim);
        let first_claim = first_result.expect("first concurrent claim should resolve");
        let second_claim = second_result.expect("second concurrent claim should resolve");

        let mut claimed = 0;
        let mut conflicted = 0;
        let mut active_job_id = None;
        for item in [first_claim, second_claim] {
            match item {
                ManualTransitionScopeAdmissionClaim::Claimed => claimed += 1,
                ManualTransitionScopeAdmissionClaim::Conflict(active) => {
                    conflicted += 1;
                    active_job_id = Some(active.job_id);
                }
            }
        }

        assert_eq!(claimed, 1, "only one concurrent same-scope claim should be accepted");
        assert_eq!(conflicted, 1, "only one concurrent same-scope claim should report conflict");
        let active = load_manual_transition_scope_admission(ecstore.clone(), &first.scope_key)
            .await
            .expect("scope admission should remain");
        assert!(active.job_id == first.job_id || active.job_id == second.job_id);
        assert_eq!(active_job_id, Some(active.job_id), "conflict response must carry active owner");
    }

    #[tokio::test]
    async fn existing_object_lifecycle_allows_expired_marker_after_replication_completed() {
        let lc = expired_delete_marker_lifecycle();
        let object = delete_marker_object(ReplicationStatusType::Completed, VersionPurgeStatusType::Complete);

        let event = eval_action_from_lifecycle(&lc, None, &object).await;

        assert_eq!(event.action, IlmAction::DeleteVersionAction);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_skips_expired_marker_while_replication_pending() {
        let lc = expired_delete_marker_lifecycle();
        let object = delete_marker_object(ReplicationStatusType::Pending, VersionPurgeStatusType::default());

        let event = eval_action_from_lifecycle(&lc, None, &object).await;

        assert_eq!(event.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_skips_current_expiration_while_replication_pending() {
        let lc = latest_expiration_lifecycle();
        let object = current_object(ReplicationStatusType::Pending);

        let event = eval_action_from_lifecycle(&lc, None, &object).await;

        assert_eq!(event.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_skips_current_expiration_while_replication_pending_without_config() {
        let lc = latest_expiration_lifecycle();
        let object = current_object(ReplicationStatusType::Pending);

        let event = eval_action_from_lifecycle(&lc, None, &object).await;

        assert_eq!(event.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_allows_current_expiration_after_replication_completed() {
        let lc = latest_expiration_lifecycle();
        let object = current_object(ReplicationStatusType::Completed);

        let event = eval_action_from_lifecycle(&lc, None, &object).await;

        assert_eq!(event.action, IlmAction::DeleteAction);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_skips_current_expiration_for_bucket_default_retention() {
        let lc = latest_expiration_lifecycle();
        let mut object = current_object(ReplicationStatusType::Completed);
        object.mod_time = Some(OffsetDateTime::now_utc());
        let lock_config = lock_enabled_with_default_retention();

        let event = eval_action_from_lifecycle(&lc, Some(&lock_config), &object).await;

        assert_eq!(event.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_skips_delete_all_when_lock_enabled_without_default_retention() {
        let lc = all_versions_expiration_lifecycle();
        let object = current_object(ReplicationStatusType::Completed);
        let lock_config = lock_enabled_without_default_retention();

        let event = eval_action_from_lifecycle(&lc, Some(&lock_config), &object).await;

        assert_eq!(event.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    #[serial]
    async fn lifecycle_expiry_fails_closed_on_corrupt_object_lock_metadata() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("lifecycle-lock-metadata-error-{}", Uuid::new_v4().simple());
        let object = "due/object";
        create_test_bucket(&ecstore, &bucket).await;

        let mut reader = PutObjReader::from_vec(b"must survive lifecycle metadata failure".to_vec());
        let object_info = ecstore
            .put_object(
                &bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    mod_time: Some(OffsetDateTime::now_utc() - time::Duration::days(2)),
                    ..Default::default()
                },
            )
            .await
            .expect("due object should be created");

        let lifecycle = latest_expiration_lifecycle();
        let sys = metadata_sys::bucket_metadata_sys_of(&ecstore.ctx).expect("metadata system should be initialized");
        let sys = sys.read().await.clone();
        let mut metadata = (*sys.get(&bucket).await.expect("bucket metadata should exist")).clone();
        metadata.lifecycle_config_xml = crate::bucket::utils::serialize(&lifecycle).unwrap();
        metadata.lifecycle_config = Some(lifecycle);
        metadata.object_lock_config_xml = b"<ObjectLockConfiguration>".to_vec();
        metadata.object_lock_config = None;
        sys.persist_and_set(metadata)
            .await
            .expect("corrupt Object Lock payload should be persisted for the read-boundary test");
        sys.reload_from_store(&bucket)
            .await
            .expect("peer-style reload should publish the malformed persisted snapshot");

        let exact_error = super::metadata_boundary::get_expiry_configs(&ecstore, &bucket)
            .await
            .expect_err("malformed Object Lock metadata must reject lifecycle config resolution");
        assert!(
            exact_error
                .to_string()
                .contains("persisted bucket Object Lock configuration is invalid")
        );

        let runtime_state = install_unconsumed_runtime_expiry_worker(&ecstore, 1).await;
        let observed = Arc::new(StdMutex::new(Vec::new()));
        let observed_events = Arc::clone(&observed);
        let _observer = set_lifecycle_observability_observer(move |event, state, reason| {
            observed_events
                .lock()
                .expect("lifecycle metadata error observer should not poison")
                .push((event, state, reason));
        });

        super::enqueue_immediate_expiry(&object_info, LcEventSrc::S3PutObject).await;

        assert!(
            observed.lock().expect("observed events should not poison").contains(&(
                EVENT_LIFECYCLE_EVALUATION_FAILED,
                "failed",
                Some("metadata_unavailable")
            )),
            "immediate expiry must expose the authoritative metadata failure"
        );
        {
            let state = runtime_state.read().await;
            assert_eq!(state.stats.pending_tasks(), 0, "immediate expiry must not enqueue a delete");
        }
        assert!(
            ecstore
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .is_ok(),
            "immediate expiry must leave the due object intact"
        );

        let scanner_error = super::enqueue_expiry_for_existing_objects(ecstore.clone(), &bucket)
            .await
            .expect_err("scanner must propagate the authoritative Object Lock metadata error");
        assert_eq!(scanner_error.to_string(), exact_error.to_string());
        {
            let state = runtime_state.read().await;
            assert_eq!(state.stats.pending_tasks(), 0, "scanner must not enqueue a delete");
        }
        assert!(
            ecstore
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .is_ok(),
            "scanner must leave the due object intact"
        );
    }

    #[tokio::test]
    #[serial]
    async fn queued_lifecycle_expiry_does_not_delete_from_table_bucket() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("table-bucket-lifecycle-{}", Uuid::new_v4().simple());
        let object = "tables/table-id/data/part-00001.parquet";
        create_test_bucket(&ecstore, &bucket).await;

        let mut reader = PutObjReader::from_vec(b"referenced table data".to_vec());
        let object_info = ecstore
            .put_object(&bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("table data object should be created");

        let publication_lock = ecstore
            .new_ns_lock(&bucket, rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH)
            .await
            .expect("table-bucket publication lock should be created");
        let enable_guard = publication_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .expect("table-bucket enablement should acquire the publication lock");
        let expiry_store = ecstore.clone();
        let expiry_object = object_info.clone();
        let (expiry_started_tx, expiry_started_rx) = tokio::sync::oneshot::channel();
        let mut expiry = tokio::spawn(async move {
            let event = crate::bucket::lifecycle::lifecycle::Event {
                action: IlmAction::DeleteAction,
                ..Default::default()
            };
            let bucket_incarnation_id = expiry_store
                .bucket_incarnation_id_from_disk(&expiry_object.bucket)
                .await
                .expect("bucket incarnation should be available");
            expiry_started_tx.send(()).expect("lifecycle expiry start should be observed");
            super::apply_expiry_on_non_transitioned_objects(
                expiry_store,
                &expiry_object,
                &event,
                &LcEventSrc::Scanner,
                bucket_incarnation_id,
            )
            .await
        });
        expiry_started_rx.await.expect("lifecycle expiry should start");
        assert!(
            tokio::time::timeout(StdDuration::from_millis(100), &mut expiry)
                .await
                .is_err(),
            "queued lifecycle expiry must wait for table-bucket enablement"
        );

        let sys = metadata_sys::bucket_metadata_sys_of(&ecstore.ctx).expect("metadata system should be initialized");
        let sys = sys.read().await.clone();
        let mut metadata = (*sys.get(&bucket).await.expect("bucket metadata should exist")).clone();
        metadata.table_bucket_config_json = br#"{"enabled":true}"#.to_vec();
        sys.persist_and_set(metadata)
            .await
            .expect("table bucket marker should be persisted");
        sys.reload_from_store(&bucket)
            .await
            .expect("table bucket marker should become authoritative");
        drop(enable_guard);
        assert!(
            !tokio::time::timeout(StdDuration::from_secs(2), expiry)
                .await
                .expect("queued lifecycle expiry should resume after enablement")
                .expect("queued lifecycle expiry task should join"),
            "a queued lifecycle task must be rejected after the bucket becomes table-enabled"
        );
        assert!(
            ecstore
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .is_ok(),
            "table data must remain readable after lifecycle admission rejects the delete"
        );
    }

    #[tokio::test]
    #[serial]
    async fn queued_delete_all_rechecks_a_same_id_rule_moved_into_the_future() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-delete-all-rule-{}", Uuid::new_v4().simple());
        let object = "object";
        create_test_bucket(&ecstore, &bucket).await;
        metadata_sys::update(
            &bucket,
            BUCKET_VERSIONING_CONFIG,
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
        )
        .await
        .expect("bucket versioning should be enabled");
        let lifecycle_xml = |days| {
            format!(
                r#"<LifecycleConfiguration>
  <Rule>
    <ID>delete-marker-history</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <DelMarkerExpiration><Days>{days}</Days></DelMarkerExpiration>
  </Rule>
</LifecycleConfiguration>"#
            )
        };
        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml(1).into_bytes())
            .await
            .expect("initial lifecycle rule should be stored");

        let old_time = OffsetDateTime::now_utc() - time::Duration::days(3);
        let mut reader = PutObjReader::from_vec(b"old version".to_vec());
        ecstore
            .put_object(
                &bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    mod_time: Some(old_time - time::Duration::hours(1)),
                    ..Default::default()
                },
            )
            .await
            .expect("old version should be stored");
        let marker = ecstore
            .delete_object(
                &bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    mod_time: Some(old_time),
                    ..Default::default()
                },
            )
            .await
            .expect("delete marker should be created");

        let queued_event = crate::bucket::lifecycle::lifecycle::Event {
            action: IlmAction::DelMarkerDeleteAllVersionsAction,
            rule_id: "delete-marker-history".to_string(),
            ..Default::default()
        };
        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml(30).into_bytes())
            .await
            .expect("updated lifecycle rule should be stored");
        let incarnation = ecstore
            .bucket_incarnation_id_from_disk(&bucket)
            .await
            .expect("bucket incarnation should be available");

        let deleted = super::apply_expiry_on_non_transitioned_objects(
            ecstore.clone(),
            &marker,
            &queued_event,
            &LcEventSrc::Scanner,
            incarnation,
        )
        .await;
        assert!(!deleted, "the stale queued rule must be rejected");
        let versions = ecstore
            .clone()
            .list_object_versions(&bucket, object, None, None, None, 10)
            .await
            .expect("remaining versions should be listable");
        assert_eq!(versions.objects.iter().filter(|version| version.name == object).count(), 2);

        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml(1).into_bytes())
            .await
            .expect("due lifecycle rule should be restored");
        let deleted = super::apply_expiry_on_non_transitioned_objects(
            ecstore.clone(),
            &marker,
            &queued_event,
            &LcEventSrc::Scanner,
            incarnation,
        )
        .await;
        assert!(deleted, "the current due rule should purge marker and history");
        let versions = ecstore
            .clone()
            .list_object_versions(&bucket, object, None, None, None, 10)
            .await
            .expect("purged versions should be listable");
        assert_eq!(versions.objects.iter().filter(|version| version.name == object).count(), 0);
    }

    #[tokio::test]
    #[serial]
    async fn queued_expired_object_all_versions_purges_history_through_transitioned_dispatch() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("expired-all-versions-{}", Uuid::new_v4().simple());
        let object = "object";
        create_test_bucket(&ecstore, &bucket).await;
        metadata_sys::update(
            &bucket,
            BUCKET_VERSIONING_CONFIG,
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
        )
        .await
        .expect("bucket versioning should be enabled");
        metadata_sys::update(
            &bucket,
            BUCKET_LIFECYCLE_CONFIG,
            br#"<LifecycleConfiguration>
  <Rule>
    <ID>delete-all-versions</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><Days>1</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration>
  </Rule>
</LifecycleConfiguration>"#
                .to_vec(),
        )
        .await
        .expect("delete-all lifecycle rule should be stored");

        let old_time = OffsetDateTime::now_utc() - time::Duration::days(3);
        let mut old_reader = PutObjReader::from_vec(b"old version".to_vec());
        ecstore
            .put_object(
                &bucket,
                object,
                &mut old_reader,
                &ObjectOptions {
                    versioned: true,
                    mod_time: Some(old_time - time::Duration::hours(1)),
                    ..Default::default()
                },
            )
            .await
            .expect("old version should be stored");
        let mut current_reader = PutObjReader::from_vec(b"current version".to_vec());
        let mut current = ecstore
            .put_object(
                &bucket,
                object,
                &mut current_reader,
                &ObjectOptions {
                    versioned: true,
                    mod_time: Some(old_time),
                    ..Default::default()
                },
            )
            .await
            .expect("current version should be stored");
        current.transitioned_object.status = crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string();

        let incarnation = ecstore
            .bucket_incarnation_id_from_disk(&bucket)
            .await
            .expect("bucket incarnation should be available");
        let deleted = super::apply_expiry_on_transitioned_object(
            ecstore.clone(),
            &current,
            &crate::bucket::lifecycle::lifecycle::Event {
                action: IlmAction::DeleteAllVersionsAction,
                rule_id: "delete-all-versions".to_string(),
                ..Default::default()
            },
            &LcEventSrc::Scanner,
            incarnation,
        )
        .await;

        assert!(deleted, "delete-all must not degrade to transitioned single-version expiry");
        let versions = ecstore
            .list_object_versions(&bucket, object, None, None, None, 10)
            .await
            .expect("purged versions should be listable");
        assert_eq!(versions.objects.iter().filter(|version| version.name == object).count(), 0);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_skips_current_expiration_for_explicit_legal_hold() {
        let lc = latest_expiration_lifecycle();
        let object = current_object_with_metadata(
            ReplicationStatusType::Completed,
            HashMap::from([(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(), "ON".to_string())]),
        );

        let event = eval_action_from_lifecycle(&lc, None, &object).await;

        assert_eq!(event.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_skips_current_expiration_for_explicit_retention() {
        let lc = latest_expiration_lifecycle();
        let retain_until = (OffsetDateTime::now_utc() + time::Duration::days(30))
            .format(&time::format_description::well_known::Rfc3339)
            .expect("future retain-until date should format");
        let object = current_object_with_metadata(
            ReplicationStatusType::Completed,
            HashMap::from([
                (
                    X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
                    s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string(),
                ),
                (X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(), retain_until),
            ]),
        );

        let event = eval_action_from_lifecycle(&lc, None, &object).await;

        assert_eq!(event.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn restored_copy_expiry_is_not_blocked_by_retention() {
        let lifecycle = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: Vec::new(),
        };
        let retain_until = (OffsetDateTime::now_utc() + time::Duration::days(30))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        let mut object = current_object_with_metadata(
            ReplicationStatusType::Completed,
            HashMap::from([
                (
                    X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
                    ObjectLockRetentionMode::COMPLIANCE.to_string(),
                ),
                (X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(), retain_until),
            ]),
        );
        object.transitioned_object.status = TRANSITION_COMPLETE.to_string();
        object.restore_expires = Some(OffsetDateTime::now_utc() - time::Duration::hours(1));

        let current = eval_action_from_lifecycle(&lifecycle, None, &object).await;
        assert_eq!(current.action, IlmAction::DeleteRestoredAction);

        object.is_latest = false;
        object.successor_mod_time = Some(OffsetDateTime::now_utc());
        let noncurrent = eval_action_from_lifecycle(&lifecycle, None, &object).await;
        assert_eq!(noncurrent.action, IlmAction::DeleteRestoredVersionAction);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_skips_transition_while_replication_pending() {
        let lc = latest_transition_lifecycle();
        let object = current_object(ReplicationStatusType::Pending);

        let event = eval_action_from_lifecycle(&lc, None, &object).await;

        assert_eq!(event.action, IlmAction::NoneAction);
    }

    #[tokio::test]
    async fn existing_object_lifecycle_allows_transition_after_replication_completed() {
        let lc = latest_transition_lifecycle();
        let object = current_object(ReplicationStatusType::Completed);

        let event = eval_action_from_lifecycle(&lc, None, &object).await;

        assert_eq!(event.action, IlmAction::TransitionAction);
    }

    static STALE_MULTIPART_TEST_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();

    /// Re-register the cached environment's disks into its (shared bootstrap)
    /// context registry. Other `#[serial]` tests reset or reshape that
    /// registry (`reset_local_disk_test_state`, their own `init_local_disks`),
    /// and the peer-sys bucket operations of this env resolve local disks
    /// through it at call time — without this repair, a lifecycle test that
    /// runs after such a test fails bucket creation on write quorum.
    async fn reregister_env_local_disks(ecstore: &Arc<ECStore>) {
        use crate::disk::DiskAPI as _;

        let map = ecstore.ctx.local_disk_map();
        let mut guard = map.write().await;
        for disks in ecstore.disk_map.values() {
            for disk in disks.iter().flatten() {
                guard.insert(disk.endpoint().to_string(), Some(disk.clone()));
            }
        }
    }

    async fn setup_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
        if let Some((paths, ecstore)) = STALE_MULTIPART_TEST_ENV.get() {
            reregister_env_local_disks(ecstore).await;
            return (paths.clone(), ecstore.clone());
        }

        let test_base_dir = format!("/tmp/rustfs_stale_multipart_test_{}", Uuid::new_v4());
        let temp_dir = PathBuf::from(&test_base_dir);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir).await.ok();
        }
        fs::create_dir_all(&temp_dir).await.unwrap();

        let disk_paths = vec![
            temp_dir.join("disk1"),
            temp_dir.join("disk2"),
            temp_dir.join("disk3"),
            temp_dir.join("disk4"),
        ];

        for disk_path in &disk_paths {
            fs::create_dir_all(disk_path).await.unwrap();
        }

        let mut endpoints = Vec::new();
        for (i, disk_path) in disk_paths.iter().enumerate() {
            let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(i);
            endpoints.push(endpoint);
        }

        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "stale-multipart-test".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        }]);

        crate::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

        let ecstore = ECStore::new("127.0.0.1:0".parse().unwrap(), endpoint_pools, CancellationToken::new())
            .await
            .unwrap();

        let buckets = ecstore
            .list_bucket(&BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_iter()
            .map(|bucket| bucket.name)
            .collect();
        metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

        let _ = STALE_MULTIPART_TEST_ENV.set((disk_paths.clone(), ecstore.clone()));

        (disk_paths, ecstore)
    }

    async fn create_test_bucket(ecstore: &Arc<ECStore>, bucket: &str) {
        ecstore
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
    }

    async fn reset_runtime_expiry_state(ecstore: &Arc<ECStore>) -> Arc<tokio::sync::RwLock<ExpiryState>> {
        let runtime_state = runtime_sources::expiry_state_handle();
        assert!(
            Arc::ptr_eq(&runtime_state, &ecstore.ctx.expiry_state()),
            "recovery enqueue tests must exercise the ECStore runtime state"
        );
        {
            let mut state = runtime_state.write().await;
            state.tasks_tx.clear();
            state.tasks_rx.clear();
            state.stats.missed_expiry_tasks.store(0, Ordering::SeqCst);
            state.stats.missed_freevers_tasks.store(0, Ordering::SeqCst);
            state.stats.missed_tier_journal_tasks.store(0, Ordering::SeqCst);
            state.stats.pending_tasks.store(0, Ordering::SeqCst);
            state.stats.active_tasks.store(0, Ordering::SeqCst);
            state.stats.workers.store(0, Ordering::SeqCst);
            state.recovery_notify = Arc::new(tokio::sync::Notify::new());
        }
        runtime_state
    }

    async fn install_unconsumed_runtime_expiry_worker(
        ecstore: &Arc<ECStore>,
        capacity: usize,
    ) -> Arc<tokio::sync::RwLock<ExpiryState>> {
        let runtime_state = reset_runtime_expiry_state(ecstore).await;
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        let mut state = runtime_state.write().await;
        state.tasks_tx.push(tx);
        state.tasks_rx.push(Arc::new(tokio::sync::Mutex::new(rx)));
        state.stats.workers.store(1, Ordering::SeqCst);
        drop(state);
        runtime_state
    }

    /// Register a MinIO-typed mock `WARM` tier on `ecstore`'s tier manager and
    /// return the backend handle together with the hex-encoded durable backend
    /// identity to persist on seeded free versions so the worker's fail-closed
    /// remote cleanup can acquire an identity-bound lease.
    #[cfg(feature = "test-util")]
    async fn register_recovery_mock_tier(ecstore: &Arc<ECStore>) -> (crate::services::tier::test_util::MockWarmBackend, String) {
        let backend = crate::services::tier::test_util::register_mock_tier(&ecstore.tier_config_mgr(), "WARM").await;
        let identity = crate::services::tier::tier::TierConfigMgr::acquire_operation_lease(&ecstore.tier_config_mgr(), "WARM")
            .await
            .expect("mock WARM tier lease should be available")
            .backend_identity();
        let identity_hex = identity.iter().map(|byte| format!("{byte:02x}")).collect();
        (backend, identity_hex)
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn journal_replay_rejects_unknown_version_state_before_backend_io() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let (backend, _) = register_recovery_mock_tier(&ecstore).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ecstore.tier_config_mgr(), "WARM")
            .await
            .expect("mock tier lease should be available")
            .backend_identity();
        let je = Jentry {
            obj_name: "remote/object".to_string(),
            version_id: "legacy-version".to_string(),
            tier_name: "WARM".to_string(),
            backend_identity: Some(identity),
            version_id_exact: false,
            version_state: rustfs_filemeta::TransitionVersionState::Unknown,
            state: crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed,
            source: None,
        };

        let err = crate::bucket::lifecycle::tier_delete_journal::process_tier_delete_journal_entry(ecstore, &je)
            .await
            .expect_err("unknown journal state must fail before backend IO");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(backend.remove_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn journal_replay_deletes_confirmed_exact_provider_token() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let (backend, _) = register_recovery_mock_tier(&ecstore).await;
        let lease = TierConfigMgr::acquire_operation_lease(&ecstore.tier_config_mgr(), "WARM")
            .await
            .expect("mock tier lease should be available");
        let identity = lease.backend_identity();
        backend
            .set_put_remote_version(Some("provider-version-token".to_string()))
            .await;
        lease
            .put(
                "remote/object",
                crate::client::transition_api::ReaderImpl::Body(bytes::Bytes::from_static(b"candidate")),
                9,
            )
            .await
            .expect("confirmed remote candidate should be seeded");
        backend.set_remove_failure(true);
        backend.set_reject_non_empty_remote_versions(true);
        let je = Jentry {
            obj_name: "remote/object".to_string(),
            version_id: "provider-version-token".to_string(),
            tier_name: "WARM".to_string(),
            backend_identity: Some(identity),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed,
            source: None,
        };

        crate::set_disk::cleanup_rejected_transition_upload_durably(
            &lease,
            &je.obj_name,
            &je.version_id,
            true,
            Some(ecstore.clone()),
        )
        .await
        .expect("failed immediate cleanup should remain durable in the journal");
        assert!(backend.contains(&je.obj_name).await);

        backend.set_remove_failure(false);
        crate::bucket::lifecycle::tier_delete_journal::process_tier_delete_journal_entry(ecstore, &je)
            .await
            .expect("identity-bound exact journal must retry confirmed candidate cleanup");

        assert!(!backend.contains(&je.obj_name).await);
        assert_eq!(backend.exact_remove_count(), 2);
        assert_eq!(
            backend.remove_versions().await,
            vec![("remote/object".to_string(), "provider-version-token".to_string())]
        );
    }

    async fn seed_recoverable_free_version(
        disk_paths: &[PathBuf],
        bucket: &str,
        object: &str,
        retained_delete_marker: Option<Uuid>,
        backend_identity: Option<String>,
    ) {
        let object_version_id = Uuid::new_v4();
        // Persist the durable backend identity on the transitioned version so the
        // recovered free version carries it (matching a registered mock tier);
        // free-version remote cleanup fails closed without it.
        let mut transitioned_metadata = HashMap::new();
        if let Some(identity) = backend_identity {
            rustfs_utils::http::metadata_compat::insert_str(
                &mut transitioned_metadata,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
                identity,
            );
        }
        let transition_version_id = Uuid::new_v4();
        let mut metadata = FileMeta::new();
        metadata
            .add_version(FileInfo {
                volume: bucket.to_string(),
                name: object.to_string(),
                version_id: Some(object_version_id),
                transition_status: crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string(),
                transitioned_objname: format!("remote/{bucket}/{object}"),
                transition_version_id: Some(transition_version_id),
                transition_version: Some(transition_version_id.to_string()),
                transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
                transition_tier: "WARM".to_string(),
                mod_time: Some(OffsetDateTime::now_utc()),
                metadata: transitioned_metadata,
                ..Default::default()
            })
            .expect("transitioned object metadata should be created");
        let mut delete_info = FileInfo {
            volume: bucket.to_string(),
            name: object.to_string(),
            version_id: Some(object_version_id),
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        delete_info.set_tier_free_version_id(&Uuid::new_v4().to_string());
        metadata
            .delete_version(&delete_info)
            .expect("transitioned delete should create a recoverable free version");
        if let Some(version_id) = retained_delete_marker {
            metadata
                .add_version(FileInfo {
                    volume: bucket.to_string(),
                    name: object.to_string(),
                    version_id: Some(version_id),
                    deleted: true,
                    mod_time: Some(OffsetDateTime::now_utc()),
                    ..Default::default()
                })
                .expect("retained delete marker should be created");
        }
        let encoded = metadata.marshal_msg().expect("free-version metadata should encode");

        for disk_path in disk_paths {
            let object_dir = disk_path.join(bucket).join(object);
            fs::create_dir_all(&object_dir)
                .await
                .expect("free-version object directory should be created");
            fs::write(object_dir.join(STORAGE_FORMAT_FILE), &encoded)
                .await
                .expect("free-version xl.meta should be written");
        }
    }

    async fn remove_seeded_free_version(disk_paths: &[PathBuf], bucket: &str, object: &str) {
        for disk_path in disk_paths {
            fs::remove_dir_all(disk_path.join(bucket).join(object))
                .await
                .expect("seeded free-version object directory should be removed");
        }
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_disabled_does_not_consume_start_guard() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let started = OnceLock::new();

        temp_env::async_with_vars([(super::ENV_TIER_FREE_VERSION_RECOVERY_ENABLED, Some("false"))], async {
            let recovery = super::spawn_tier_free_version_recovery_once(Arc::clone(&ecstore), &started);

            assert!(recovery.is_none());
            assert!(started.get().is_none());
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_production_entrypoint_enqueues_seeded_item() {
        let (disk_paths, ecstore) = setup_test_env().await;
        let runtime_state = install_unconsumed_runtime_expiry_worker(&ecstore, 1).await;
        let recovery_rx = {
            let state = runtime_state.read().await;
            Arc::clone(&state.tasks_rx[0])
        };
        let bucket = format!("recovery-entrypoint-{}", Uuid::new_v4());
        let object = "free-version";
        create_test_bucket(&ecstore, &bucket).await;
        seed_recoverable_free_version(&disk_paths, &bucket, object, None, None).await;

        let started = OnceLock::new();
        let recovery = super::spawn_tier_free_version_recovery_once(Arc::clone(&ecstore), &started)
            .expect("production recovery entrypoint should start once");
        let task = tokio::time::timeout(StdDuration::from_secs(30), async { recovery_rx.lock().await.recv().await })
            .await
            .expect("production recovery entrypoint should enqueue the seeded item")
            .expect("recovery queue should remain open");
        let task = task.expect("recovery queue should contain a task");
        let recovered = task
            .as_any()
            .downcast_ref::<FreeVersionTask>()
            .expect("production recovery entrypoint should enqueue a free-version task");
        assert_eq!(recovered.0.bucket, bucket);
        assert_eq!(recovered.0.name, object);

        recovery.abort();
        let join_error = recovery
            .await
            .expect_err("aborted production recovery task should report cancellation");
        assert!(join_error.is_cancelled());
        remove_seeded_free_version(&disk_paths, &bucket, object).await;
        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
        reset_runtime_expiry_state(&ecstore).await;
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_real_enqueue_failure_retries_same_object() {
        let (disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-enqueue-failure-{}", Uuid::new_v4());
        let object = "free-version-b";
        let start_marker = "free-version-a0";
        create_test_bucket(&ecstore, &bucket).await;
        seed_recoverable_free_version(&disk_paths, &bucket, object, None, None).await;

        let runtime_state = install_unconsumed_runtime_expiry_worker(&ecstore, 1).await;
        let recovery_rx = {
            let state = runtime_state.read().await;
            Arc::clone(&state.tasks_rx[0])
        };
        let mut recovery_rx = recovery_rx.lock().await;
        assert!(
            super::enqueue_recovered_free_version(ObjectInfo {
                bucket: "prefill".to_string(),
                name: "prefill".to_string(),
                ..Default::default()
            })
            .await,
            "the production recovery queue should accept its first task"
        );

        let first = recover_tier_free_versions_with_cancel(
            Arc::clone(&ecstore),
            1,
            Some(bucket.clone()),
            Some(start_marker.to_string()),
            CancellationToken::new(),
        )
        .await
        .expect("queue failure should return retry markers");
        assert_eq!(first.scanned, 1);
        assert_eq!(first.enqueued, 0);
        assert_eq!(first.failed, 1);
        assert!(first.truncated);
        assert_eq!(first.next_bucket_marker.as_deref(), Some(bucket.as_str()));
        assert_eq!(first.next_object_marker.as_deref(), Some(start_marker));

        drop(
            recovery_rx
                .try_recv()
                .expect("the failed recovery attempt must leave the prefilled task queued")
                .expect("the prefilled recovery queue entry should contain a task"),
        );

        let retried = recover_tier_free_versions_with_cancel(
            Arc::clone(&ecstore),
            1,
            first.next_bucket_marker,
            first.next_object_marker,
            CancellationToken::new(),
        )
        .await
        .expect("retry markers should revisit the failed free version");
        assert_eq!(retried.scanned, 1);
        assert_eq!(retried.enqueued, 1);
        assert_eq!(retried.failed, 0);

        let retried_task = recovery_rx
            .try_recv()
            .expect("the retry should enqueue the recovered free-version task")
            .expect("the recovered queue entry should contain a task");
        let retried_task = retried_task
            .as_any()
            .downcast_ref::<FreeVersionTask>()
            .expect("the recovered queue entry should be a free-version task");
        assert_eq!(retried_task.0.bucket, bucket);
        assert_eq!(retried_task.0.name, object);

        remove_seeded_free_version(&disk_paths, &bucket, object).await;
        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
        reset_runtime_expiry_state(&ecstore).await;
        drop(runtime_state);
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_cancels_between_enqueues() {
        let (disk_paths, ecstore) = setup_test_env().await;
        let runtime_state = install_unconsumed_runtime_expiry_worker(&ecstore, 2).await;
        let bucket = format!("recovery-enqueue-cancel-{}", Uuid::new_v4());
        let objects = ["free-version-a", "free-version-b"];
        create_test_bucket(&ecstore, &bucket).await;
        for object in objects {
            seed_recoverable_free_version(&disk_paths, &bucket, object, None, None).await;
        }

        let cancel = CancellationToken::new();
        let hook_cancel = cancel.clone();
        let enqueue_calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = Arc::clone(&enqueue_calls);
        let _enqueue = set_recovered_free_version_enqueue_observer(move |_queued| {
            if hook_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                hook_cancel.cancel();
            }
        });
        let err = recover_tier_free_versions_with_cancel(Arc::clone(&ecstore), 2, None, None, cancel)
            .await
            .expect_err("cancellation after the first enqueue should stop the recovery page");

        assert!(matches!(
            err,
            crate::error::Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::Interrupted
        ));
        assert_eq!(enqueue_calls.load(Ordering::SeqCst), 1);

        for object in objects {
            remove_seeded_free_version(&disk_paths, &bucket, object).await;
        }
        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
        reset_runtime_expiry_state(&ecstore).await;
        drop(runtime_state);
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_real_path_honors_cancellation() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-cancel-{}", Uuid::new_v4());
        create_test_bucket(&ecstore, &bucket).await;
        let cancel = CancellationToken::new();
        cancel.cancel();

        let err = recover_tier_free_versions_with_cancel(ecstore, 1, None, None, cancel)
            .await
            .expect_err("cancelled recovery should stop before walking buckets");

        assert!(matches!(
            err,
            crate::error::Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::Interrupted
        ));
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_cancels_blocked_bucket_listing() {
        let (_paths, ecstore) = setup_test_env().await;
        let list_started = Arc::new(tokio::sync::Notify::new());
        let _list_wait = set_recovery_bucket_list_wait_hook(Arc::clone(&list_started));
        let cancel = CancellationToken::new();
        let list_cancel = cancel.clone();
        let recovery = tokio::spawn(async move { list_tier_free_versions(ecstore, 1, None, None, list_cancel).await });

        list_started.notified().await;
        cancel.cancel();
        let err = tokio::time::timeout(StdDuration::from_secs(30), recovery)
            .await
            .expect("blocked bucket listing should stop promptly")
            .expect("recovery task should not panic")
            .expect_err("blocked bucket listing should return cancellation");

        assert!(matches!(
            err,
            crate::error::Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::Interrupted
        ));
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_exact_limit_is_not_truncated() {
        let (disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-exact-limit-{}", Uuid::new_v4());
        let objects = ["free-version-a", "free-version-b"];
        create_test_bucket(&ecstore, &bucket).await;
        for object in objects {
            seed_recoverable_free_version(&disk_paths, &bucket, object, None, None).await;
        }

        let page = list_tier_free_versions(Arc::clone(&ecstore), objects.len(), None, None, CancellationToken::new())
            .await
            .expect("an exactly full final page should be listed");

        assert_eq!(page.items.len(), objects.len());
        assert!(!page.truncated);
        assert!(page.next_bucket_marker.is_none());
        assert!(page.next_object_marker.is_none());

        for object in objects {
            remove_seeded_free_version(&disk_paths, &bucket, object).await;
        }
        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_propagates_walk_item_error() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-walk-error-{}", Uuid::new_v4());
        create_test_bucket(&ecstore, &bucket).await;
        let injected_bucket = bucket.clone();
        let _walk_error = set_recovery_walk_test_hook(move |walk_bucket| {
            (walk_bucket == injected_bucket.as_str())
                .then(|| RecoveryWalkTestAction::SendItemsThenError(Vec::new(), crate::error::Error::DiskNotFound))
        });

        let err = list_tier_free_versions(Arc::clone(&ecstore), 1, None, None, CancellationToken::new())
            .await
            .expect_err("walk errors must not become a partial successful page");
        assert!(matches!(err, crate::error::Error::DiskNotFound));

        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_propagates_error_queued_after_truncation() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-post-error-{}", Uuid::new_v4());
        create_test_bucket(&ecstore, &bucket).await;
        let injected_bucket = bucket.clone();
        let _walk_error = set_recovery_walk_test_hook(move |walk_bucket| {
            (walk_bucket == injected_bucket.as_str()).then(|| {
                RecoveryWalkTestAction::SendItemsThenError(
                    vec![
                        ObjectInfo {
                            bucket: injected_bucket.clone(),
                            name: "recoverable-a".to_string(),
                            transitioned_object: TransitionedObject {
                                name: "remote/recoverable-a".to_string(),
                                tier: "WARM".to_string(),
                                free_version: true,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        ObjectInfo {
                            bucket: injected_bucket.clone(),
                            name: "nonrecoverable-b".to_string(),
                            ..Default::default()
                        },
                    ],
                    crate::error::Error::FaultyDisk,
                )
            })
        });

        let err = list_tier_free_versions(Arc::clone(&ecstore), 1, None, None, CancellationToken::new())
            .await
            .expect_err("errors queued after pagination truncation must still propagate");
        assert!(matches!(err, crate::error::Error::FaultyDisk));

        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_propagates_walk_task_error() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-walk-task-error-{}", Uuid::new_v4());
        create_test_bucket(&ecstore, &bucket).await;
        let injected_bucket = bucket.clone();
        let _walk_error = set_recovery_walk_test_hook(move |walk_bucket| {
            (walk_bucket == injected_bucket.as_str())
                .then(|| RecoveryWalkTestAction::ReturnError(crate::error::Error::FaultyDisk))
        });

        let err = list_tier_free_versions(Arc::clone(&ecstore), 1, None, None, CancellationToken::new())
            .await
            .expect_err("walk task errors must not become a partial successful page");
        assert!(matches!(err, crate::error::Error::FaultyDisk));

        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_cancels_active_walk() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-active-cancel-{}", Uuid::new_v4());
        create_test_bucket(&ecstore, &bucket).await;
        let injected_bucket = bucket.clone();
        let walk_started = Arc::new(tokio::sync::Notify::new());
        let hook_started = Arc::clone(&walk_started);
        let _walk = set_recovery_walk_test_hook(move |walk_bucket| {
            (walk_bucket == injected_bucket.as_str())
                .then(|| RecoveryWalkTestAction::WaitForCancellation(Arc::clone(&hook_started)))
        });
        let cancel = CancellationToken::new();
        let list_cancel = cancel.clone();
        let list_store = Arc::clone(&ecstore);
        let recovery = tokio::spawn(async move { list_tier_free_versions(list_store, 1, None, None, list_cancel).await });

        walk_started.notified().await;
        cancel.cancel();
        let err = tokio::time::timeout(StdDuration::from_secs(30), recovery)
            .await
            .expect("active recovery walk should stop promptly")
            .expect("recovery task should not panic")
            .expect_err("active recovery walk should return cancellation");
        assert!(matches!(
            err,
            crate::error::Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::Interrupted
        ));

        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_aborts_walk_that_ignores_cancellation() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-stuck-cancel-{}", Uuid::new_v4());
        create_test_bucket(&ecstore, &bucket).await;
        let injected_bucket = bucket.clone();
        let walk_started = Arc::new(tokio::sync::Notify::new());
        let hook_started = Arc::clone(&walk_started);
        let _walk = set_recovery_walk_test_hook(move |walk_bucket| {
            (walk_bucket == injected_bucket.as_str())
                .then(|| RecoveryWalkTestAction::SendItemsThenHang(Vec::new(), Arc::clone(&hook_started)))
        });
        let cancel = CancellationToken::new();
        let list_cancel = cancel.clone();
        let list_store = Arc::clone(&ecstore);
        let recovery = tokio::spawn(async move { list_tier_free_versions(list_store, 1, None, None, list_cancel).await });

        walk_started.notified().await;
        cancel.cancel();
        let err = tokio::time::timeout(StdDuration::from_secs(2), recovery)
            .await
            .expect("unresponsive recovery walk should be aborted after cancellation")
            .expect("recovery task should not panic")
            .expect_err("cancelled recovery walk should return cancellation");
        assert!(matches!(
            err,
            crate::error::Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::Interrupted
        ));

        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_bounds_truncated_page_drain() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-stuck-drain-{}", Uuid::new_v4());
        create_test_bucket(&ecstore, &bucket).await;
        let injected_bucket = bucket.clone();
        let walk_started = Arc::new(tokio::sync::Notify::new());
        let hook_started = Arc::clone(&walk_started);
        let _walk = set_recovery_walk_test_hook(move |walk_bucket| {
            (walk_bucket == injected_bucket.as_str()).then(|| {
                RecoveryWalkTestAction::SendItemsThenHang(
                    vec![
                        ObjectInfo {
                            bucket: injected_bucket.clone(),
                            name: "recoverable-a".to_string(),
                            transitioned_object: TransitionedObject {
                                name: "remote/recoverable-a".to_string(),
                                tier: "WARM".to_string(),
                                free_version: true,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        ObjectInfo {
                            bucket: injected_bucket.clone(),
                            name: "nonrecoverable-b".to_string(),
                            ..Default::default()
                        },
                    ],
                    Arc::clone(&hook_started),
                )
            })
        });
        let list_store = Arc::clone(&ecstore);
        let recovery =
            tokio::spawn(async move { list_tier_free_versions(list_store, 1, None, None, CancellationToken::new()).await });

        walk_started.notified().await;
        let err = tokio::time::timeout(StdDuration::from_secs(2), recovery)
            .await
            .expect("truncated-page drain should stop after its shutdown deadline")
            .expect("recovery task should not panic")
            .expect_err("an unresponsive truncated-page walk should return a timeout");
        assert!(matches!(
            err,
            crate::error::Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::TimedOut
        ));

        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_cancellation_unblocks_backpressured_walk() {
        let (_disk_paths, ecstore) = setup_test_env().await;
        let bucket = format!("recovery-bp-cancel-{}", Uuid::new_v4());
        create_test_bucket(&ecstore, &bucket).await;
        let injected_bucket = bucket.clone();
        let walk_started = Arc::new(tokio::sync::Notify::new());
        let hook_started = Arc::clone(&walk_started);
        let _walk = set_recovery_walk_test_hook(move |walk_bucket| {
            (walk_bucket == injected_bucket.as_str())
                .then(|| RecoveryWalkTestAction::SendItemsUntilReceiverCloses(Arc::clone(&hook_started)))
        });
        let cancel = CancellationToken::new();
        let list_cancel = cancel.clone();
        let list_store = Arc::clone(&ecstore);
        let recovery = tokio::spawn(async move { list_tier_free_versions(list_store, 1, None, None, list_cancel).await });

        walk_started.notified().await;
        cancel.cancel();
        let err = tokio::time::timeout(StdDuration::from_secs(30), recovery)
            .await
            .expect("backpressured recovery walk should stop promptly")
            .expect("recovery task should not panic")
            .expect_err("backpressured recovery walk should return cancellation");
        assert!(matches!(
            err,
            crate::error::Error::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::Interrupted
        ));

        ecstore
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("empty recovery test bucket should be removed");
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_continues_after_deleted_marker_bucket() {
        let (_paths, ecstore) = setup_test_env().await;
        let suffix = Uuid::new_v4().simple();
        let earlier_bucket = format!("zzzz-recovery-{suffix}-a");
        let deleted_marker = format!("zzzz-recovery-{suffix}-m");
        let later_bucket = format!("zzzz-recovery-{suffix}-z");
        let later_object = "a-before-stale-marker";
        create_test_bucket(&ecstore, &earlier_bucket).await;
        create_test_bucket(&ecstore, &later_bucket).await;
        let mut reader = PutObjReader::from_vec(b"cursor reset probe".to_vec());
        ecstore
            .put_object(&later_bucket, later_object, &mut reader, &ObjectOptions::default())
            .await
            .expect("successor bucket object should be created");

        let page = list_tier_free_versions(
            Arc::clone(&ecstore),
            1,
            Some(deleted_marker),
            Some("z-stale-object-marker".to_string()),
            CancellationToken::new(),
        )
        .await
        .expect("recovery should resume at the first bucket after a deleted marker bucket");

        assert_eq!(page.buckets_scanned, 1, "the later bucket must not be skipped");
        assert_eq!(
            page.scanned_entries, 1,
            "the deleted bucket's object marker must not skip objects in the successor bucket"
        );
        ecstore
            .delete_object(&later_bucket, later_object, ObjectOptions::default())
            .await
            .expect("successor bucket object should be removed");
        for bucket in [&earlier_bucket, &later_bucket] {
            ecstore
                .delete_bucket(bucket, &DeleteBucketOptions::default())
                .await
                .expect("empty recovery test bucket should be removed");
        }
    }

    #[tokio::test]
    #[serial]
    async fn tier_free_version_recovery_limit_holds_across_buckets_with_same_object_key() {
        let (disk_paths, ecstore) = setup_test_env().await;
        let suffix = Uuid::new_v4().simple();
        let first_bucket = format!("zzzz-recovery-{suffix}-a");
        let second_bucket = format!("zzzz-recovery-{suffix}-b");
        let object = "same-key";
        for bucket in [&first_bucket, &second_bucket] {
            create_test_bucket(&ecstore, bucket).await;
            seed_recoverable_free_version(&disk_paths, bucket, object, None, None).await;
        }

        let first_page = list_tier_free_versions(Arc::clone(&ecstore), 1, None, None, CancellationToken::new())
            .await
            .expect("first recovery page should be listed");
        assert_eq!(first_page.items.len(), 1);
        assert_eq!(first_page.items[0].bucket, first_bucket);
        assert!(first_page.truncated);
        assert_eq!(first_page.next_bucket_marker.as_deref(), Some(first_bucket.as_str()));
        assert_eq!(first_page.next_object_marker.as_deref(), Some(object));

        remove_seeded_free_version(&disk_paths, &first_bucket, object).await;
        let second_page = list_tier_free_versions(
            Arc::clone(&ecstore),
            1,
            first_page.next_bucket_marker,
            first_page.next_object_marker,
            CancellationToken::new(),
        )
        .await
        .expect("second recovery page should resume without loss");
        assert_eq!(second_page.items.len(), 1);
        assert_eq!(second_page.items[0].bucket, second_bucket);

        remove_seeded_free_version(&disk_paths, &second_bucket, object).await;
        for bucket in [&first_bucket, &second_bucket] {
            ecstore
                .delete_bucket(bucket, &DeleteBucketOptions::default())
                .await
                .expect("empty recovery test bucket should be removed");
        }
    }

    async fn set_abort_incomplete_lifecycle(bucket: &str, prefix: &str, days_after_initiation: i32) {
        let lifecycle_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>abort-multipart</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>{prefix}</Prefix>
    </Filter>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>{days_after_initiation}</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>"#
        );

        metadata_sys::update(bucket, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("lifecycle metadata should be stored");
    }

    async fn set_abort_incomplete_lifecycle_with_size(
        bucket: &str,
        prefix: &str,
        days_after_initiation: i32,
        object_size_greater_than: usize,
    ) {
        let lifecycle_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>abort-multipart-size</ID>
    <Status>Enabled</Status>
    <Filter>
      <And>
        <Prefix>{prefix}</Prefix>
        <ObjectSizeGreaterThan>{object_size_greater_than}</ObjectSizeGreaterThan>
      </And>
    </Filter>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>{days_after_initiation}</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>"#
        );

        metadata_sys::update(bucket, BUCKET_LIFECYCLE_CONFIG, lifecycle_xml.into_bytes())
            .await
            .expect("lifecycle metadata should be stored");
    }

    fn multipart_sha_dir(bucket: &str, object: &str) -> String {
        hex_simd::encode_to_string(Sha256::digest(format!("{bucket}/{object}").as_bytes()), hex_simd::AsciiCase::Lower)
    }

    #[test]
    fn merge_stale_multipart_candidate_prefers_metadata_over_fallback() {
        let mut candidates = HashMap::new();

        merge_stale_multipart_candidate(
            &mut candidates,
            StaleMultipartUploadCandidate {
                path: "sha/upload".to_string(),
                initiated: OffsetDateTime::UNIX_EPOCH,
                metadata: None,
            },
        );
        merge_stale_multipart_candidate(
            &mut candidates,
            StaleMultipartUploadCandidate {
                path: "sha/upload".to_string(),
                initiated: OffsetDateTime::UNIX_EPOCH,
                metadata: Some(HashMap::from([("k".to_string(), "v".to_string())])),
            },
        );

        assert_eq!(
            candidates
                .get("sha/upload")
                .and_then(|candidate| candidate.metadata.as_ref())
                .and_then(|metadata| metadata.get("k")),
            Some(&"v".to_string())
        );
    }

    #[tokio::test]
    #[serial]
    async fn ecstore_new_succeeds_on_fresh_local_volumes() {
        let test_base_dir = format!("/tmp/rustfs_ecstore_empty_boot_{}", Uuid::new_v4());
        let temp_dir = PathBuf::from(&test_base_dir);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir).await.ok();
        }
        fs::create_dir_all(&temp_dir).await.unwrap();

        let disk_paths = vec![
            temp_dir.join("disk1"),
            temp_dir.join("disk2"),
            temp_dir.join("disk3"),
            temp_dir.join("disk4"),
        ];

        for disk_path in &disk_paths {
            fs::create_dir_all(disk_path).await.unwrap();
        }

        let mut endpoints = Vec::new();
        for (i, disk_path) in disk_paths.iter().enumerate() {
            let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(i);
            endpoints.push(endpoint);
        }

        let endpoint_pools = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "fresh-boot-test".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        }]);

        crate::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

        let ecstore = ECStore::new("127.0.0.1:0".parse().unwrap(), endpoint_pools, CancellationToken::new()).await;
        assert!(ecstore.is_ok(), "fresh local ECStore boot should succeed, got {ecstore:?}");
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_uses_default_expiry_without_lifecycle() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-default-{}", Uuid::new_v4().simple());
        let object = "default-cleanup/object.txt";
        create_test_bucket(&ecstore, &bucket).await;

        let initiated = OffsetDateTime::now_utc() - time::Duration::hours(30);
        let upload = ecstore
            .new_multipart_upload(
                &bucket,
                object,
                &ObjectOptions {
                    mod_time: Some(initiated),
                    ..Default::default()
                },
            )
            .await
            .expect("multipart upload should be created");

        let deleted = cleanup_stale_multipart_uploads_once_at(
            ecstore.clone(),
            OffsetDateTime::now_utc(),
            StdDuration::from_secs(24 * 60 * 60),
        )
        .await;
        assert!(deleted >= 1, "expected at least one stale multipart upload to be removed");

        let err = ecstore
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("stale multipart upload should be removed");
        assert!(is_err_invalid_upload_id(&err));
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_handles_data_movement_namespace() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-data-movement-{}", Uuid::new_v4().simple());
        create_test_bucket(&ecstore, &bucket).await;

        let create_upload = |object: &'static str, mod_time| {
            let ecstore = ecstore.clone();
            let bucket = bucket.clone();
            async move {
                let mut metadata = HashMap::new();
                rustfs_utils::http::insert_str(
                    &mut metadata,
                    rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
                    "cleanup-test".to_string(),
                );
                ecstore
                    .new_multipart_upload(
                        &bucket,
                        object,
                        &ObjectOptions {
                            data_movement: true,
                            mod_time: Some(mod_time),
                            user_defined: metadata,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("data movement multipart upload should be created")
                    .upload_id
            }
        };

        let stale_object = "stale-internal.bin";
        let active_object = "active-internal.bin";
        let now = OffsetDateTime::now_utc();
        let stale_upload_id = create_upload(stale_object, now - time::Duration::hours(30)).await;
        let active_upload_id = create_upload(active_object, now).await;

        let deleted = cleanup_stale_multipart_uploads_once_at(ecstore.clone(), now, StdDuration::from_secs(24 * 60 * 60)).await;
        assert!(deleted >= 1, "expected stale data movement upload to be removed");

        let internal_opts = ObjectOptions {
            data_movement: true,
            ..Default::default()
        };
        let stale_err = ecstore
            .get_multipart_info(&bucket, stale_object, &stale_upload_id, &internal_opts)
            .await
            .expect_err("stale data movement upload should be removed");
        assert!(is_err_invalid_upload_id(&stale_err));
        ecstore
            .get_multipart_info(&bucket, active_object, &active_upload_id, &internal_opts)
            .await
            .expect("active data movement upload should remain available");
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_waits_for_data_movement_part_commit() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-data-movement-lock-{}", Uuid::new_v4().simple());
        let object = "stale-internal.bin";
        create_test_bucket(&ecstore, &bucket).await;

        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
            "cleanup-lock-test".to_string(),
        );
        let opts = ObjectOptions {
            data_movement: true,
            mod_time: Some(OffsetDateTime::now_utc() - time::Duration::hours(30)),
            user_defined: metadata,
            ..Default::default()
        };
        let upload = ecstore
            .new_multipart_upload(&bucket, object, &opts)
            .await
            .expect("data movement multipart upload should be created");
        let barrier = MultipartCommitBarrier::install(bucket.as_str(), object, MultipartCommitPause::PutPartAfterRename);
        let put_store = ecstore.clone();
        let put_bucket = bucket.clone();
        let upload_id = upload.upload_id.clone();
        let put_task = tokio::spawn(async move {
            let mut data = PutObjReader::from_vec(vec![1, 2, 3, 4]);
            put_store
                .put_object_part(
                    &put_bucket,
                    object,
                    &upload_id,
                    1,
                    &mut data,
                    &ObjectOptions {
                        data_movement: true,
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_paused().await;

        let cleanup_store = ecstore.clone();
        let mut cleanup_task = tokio::spawn(async move {
            cleanup_stale_multipart_uploads_once_at(
                cleanup_store,
                OffsetDateTime::now_utc(),
                StdDuration::from_secs(24 * 60 * 60),
            )
            .await
        });
        assert!(
            tokio::time::timeout(StdDuration::from_millis(200), &mut cleanup_task)
                .await
                .is_err(),
            "stale cleanup must wait for the in-flight part commit upload lock"
        );

        barrier.release();
        put_task
            .await
            .expect("part upload task should join")
            .expect("part upload should commit before stale cleanup");
        let deleted = cleanup_task.await.expect("stale cleanup task should join");
        assert!(deleted >= 1, "stale cleanup should proceed after the part commit releases its lock");
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_applies_abort_incomplete_lifecycle_before_default_expiry() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-lifecycle-{}", Uuid::new_v4().simple());
        let object = "logs/prefix/object.txt";
        create_test_bucket(&ecstore, &bucket).await;
        set_abort_incomplete_lifecycle(&bucket, "logs/", 1).await;

        let initiated = OffsetDateTime::now_utc() - time::Duration::hours(48);
        let upload = ecstore
            .new_multipart_upload(
                &bucket,
                object,
                &ObjectOptions {
                    mod_time: Some(initiated),
                    ..Default::default()
                },
            )
            .await
            .expect("multipart upload should be created");

        let deleted = cleanup_stale_multipart_uploads_once_at(
            ecstore.clone(),
            OffsetDateTime::now_utc(),
            StdDuration::from_secs(7 * 24 * 60 * 60),
        )
        .await;
        assert!(deleted >= 1, "expected lifecycle-driven stale multipart cleanup to run");

        let err = ecstore
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("multipart upload should be removed by lifecycle abort rule");
        assert!(is_err_invalid_upload_id(&err));
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_applies_zero_day_abort_lifecycle_immediately() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-zero-lifecycle-{}", Uuid::new_v4().simple());
        let object = "logs/immediate/object.txt";
        create_test_bucket(&ecstore, &bucket).await;
        set_abort_incomplete_lifecycle(&bucket, "logs/", 0).await;

        let initiated = OffsetDateTime::now_utc() - time::Duration::minutes(5);
        let upload = ecstore
            .new_multipart_upload(
                &bucket,
                object,
                &ObjectOptions {
                    mod_time: Some(initiated),
                    ..Default::default()
                },
            )
            .await
            .expect("multipart upload should be created");

        let deleted = cleanup_stale_multipart_uploads_once_at(
            ecstore.clone(),
            OffsetDateTime::now_utc(),
            StdDuration::from_secs(7 * 24 * 60 * 60),
        )
        .await;
        assert!(deleted >= 1, "expected zero-day lifecycle abort cleanup to run immediately");

        let err = ecstore
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("multipart upload should be removed by zero-day lifecycle abort rule");
        assert!(is_err_invalid_upload_id(&err));
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_excludes_data_movement_from_abort_lifecycle() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-internal-lifecycle-{}", Uuid::new_v4().simple());
        let object = "logs/internal/object.bin";
        create_test_bucket(&ecstore, &bucket).await;
        set_abort_incomplete_lifecycle(&bucket, "logs/", 0).await;

        let initiated = OffsetDateTime::now_utc() - time::Duration::minutes(5);
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
            "lifecycle-exclusion-test".to_string(),
        );
        let upload = ecstore
            .new_multipart_upload(
                &bucket,
                object,
                &ObjectOptions {
                    data_movement: true,
                    mod_time: Some(initiated),
                    user_defined: metadata,
                    ..Default::default()
                },
            )
            .await
            .expect("data movement multipart upload should be created");

        let deleted = cleanup_stale_multipart_uploads_once_at(
            ecstore.clone(),
            OffsetDateTime::now_utc(),
            StdDuration::from_secs(7 * 24 * 60 * 60),
        )
        .await;
        assert_eq!(deleted, 0, "bucket lifecycle must not remove active data movement uploads");

        ecstore
            .get_multipart_info(
                &bucket,
                object,
                &upload.upload_id,
                &ObjectOptions {
                    data_movement: true,
                    ..Default::default()
                },
            )
            .await
            .expect("active data movement upload should remain available");
    }

    #[tokio::test]
    #[serial]
    async fn stale_multipart_cleanup_applies_abort_lifecycle_with_size_filter() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-size-{}", Uuid::new_v4().simple());
        let object = "logs/sized/object.txt";
        create_test_bucket(&ecstore, &bucket).await;
        set_abort_incomplete_lifecycle_with_size(&bucket, "logs/", 1, 5).await;

        let initiated = OffsetDateTime::now_utc() - time::Duration::hours(48);
        let upload = ecstore
            .new_multipart_upload(
                &bucket,
                object,
                &ObjectOptions {
                    mod_time: Some(initiated),
                    ..Default::default()
                },
            )
            .await
            .expect("multipart upload should be created");

        let mut data = PutObjReader::from_vec(vec![1, 2, 3, 4, 5, 6]);
        ecstore
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut data, &ObjectOptions::default())
            .await
            .expect("multipart part should be uploaded");

        let deleted = cleanup_stale_multipart_uploads_once_at(
            ecstore.clone(),
            OffsetDateTime::now_utc(),
            StdDuration::from_secs(7 * 24 * 60 * 60),
        )
        .await;
        assert!(deleted >= 1, "expected lifecycle-driven stale multipart cleanup to run");

        let err = ecstore
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("multipart upload should be removed by size-qualified lifecycle abort rule");
        assert!(is_err_invalid_upload_id(&err));
    }

    #[tokio::test]
    #[serial]
    async fn multipart_info_and_list_parts_do_not_expose_internal_metadata_keys() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-sanitize-{}", Uuid::new_v4().simple());
        let object = "sanitize/object.txt";
        create_test_bucket(&ecstore, &bucket).await;

        let upload = ecstore
            .new_multipart_upload(&bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");

        let multipart_info = ecstore
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect("multipart info should be readable");
        assert!(!multipart_info.user_defined.contains_key(RUSTFS_MULTIPART_BUCKET_KEY));
        assert!(!multipart_info.user_defined.contains_key(RUSTFS_MULTIPART_OBJECT_KEY));

        let parts = ecstore
            .list_object_parts(&bucket, object, &upload.upload_id, None, 0, &ObjectOptions::default())
            .await
            .expect("multipart parts should be readable");
        assert!(!parts.user_defined.contains_key(RUSTFS_MULTIPART_BUCKET_KEY));
        assert!(!parts.user_defined.contains_key(RUSTFS_MULTIPART_OBJECT_KEY));
    }

    #[tokio::test]
    #[serial]
    async fn repeated_upload_part_overwrites_previous_part_state() {
        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("multipart-overwrite-{}", Uuid::new_v4().simple());
        let object = "overwrite/object.txt";
        create_test_bucket(&ecstore, &bucket).await;

        let upload = ecstore
            .new_multipart_upload(&bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");

        let mut first = PutObjReader::from_vec(vec![1, 2, 3]);
        let first_part = ecstore
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut first, &ObjectOptions::default())
            .await
            .expect("first multipart part should be uploaded");

        let mut second = PutObjReader::from_vec(vec![4, 5, 6, 7]);
        let second_part = ecstore
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut second, &ObjectOptions::default())
            .await
            .expect("second multipart part should overwrite the previous part");

        assert_ne!(
            first_part.etag, second_part.etag,
            "the overwrite path should persist the latest part metadata rather than reusing stale state"
        );

        let parts = ecstore
            .list_object_parts(
                &bucket,
                object,
                &upload.upload_id,
                None,
                crate::set_disk::MAX_PARTS_COUNT,
                &ObjectOptions::default(),
            )
            .await
            .expect("multipart parts should be readable after overwrite");

        assert_eq!(parts.parts.len(), 1, "only the latest version of part 1 should remain visible");
        assert_eq!(parts.parts[0].part_num, 1);
        assert_eq!(parts.parts[0].etag, second_part.etag);
        assert_eq!(parts.parts[0].size, second_part.size);
        assert_eq!(parts.parts[0].actual_size, second_part.actual_size);

        let completed = ecstore
            .complete_multipart_upload(
                &bucket,
                object,
                &upload.upload_id,
                vec![crate::storage_api_contracts::multipart::CompletePart {
                    part_num: 1,
                    etag: second_part.etag.clone(),
                    checksum_crc32: None,
                    checksum_crc32c: None,
                    checksum_sha1: None,
                    checksum_sha256: None,
                    checksum_crc64nvme: None,
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("complete multipart upload should succeed with the latest overwritten part");

        assert_eq!(completed.size, second_part.size as i64);
    }

    // backlog#853: concurrently re-transmitting the same part must not mix two
    // shard generations. The uploadId commit lock only wraps rename_part, so the
    // slow streaming phase stays fully concurrent (regression guard: no
    // ServiceUnavailable / lock-acquire timeout) while the cross-disk commit is
    // serialized, leaving exactly one self-consistent generation visible.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn concurrent_resend_same_part_commits_one_generation() {
        use crate::set_disk::{MultipartCommitBarrier, MultipartCommitPause};
        use crate::storage_api_contracts::object::ObjectIO as _;

        let (_paths, ecstore) = setup_test_env().await;
        let bucket = format!("multipart-resend-{}", Uuid::new_v4().simple());
        let object = "resend/object.txt";
        create_test_bucket(&ecstore, &bucket).await;

        let upload = ecstore
            .new_multipart_upload(&bucket, object, &ObjectOptions::default())
            .await
            .expect("multipart upload should be created");

        // Distinct payloads with distinct sizes: a mixed-generation reassembly
        // would produce bytes matching none of them (or fail the read outright).
        let candidates: Vec<Vec<u8>> = (0..2)
            .map(|g| {
                let len = 4096 + g * 512;
                vec![b'a' + g as u8; len]
            })
            .collect();

        let commit_barrier = MultipartCommitBarrier::install_for_arrivals(
            &bucket,
            object,
            MultipartCommitPause::PutPartBeforeLockAcquire,
            candidates.len(),
        );
        let mut tasks = tokio::task::JoinSet::new();
        for payload in candidates.iter().cloned() {
            let store = ecstore.clone();
            let bucket = bucket.clone();
            let upload_id = upload.upload_id.clone();
            tasks.spawn(async move {
                let mut data = PutObjReader::from_vec(payload.clone());
                store
                    .put_object_part(&bucket, object, &upload_id, 1, &mut data, &ObjectOptions::default())
                    .await
                    .map(|info| (info, payload))
            });
        }

        // Both writers finish streaming before racing for the uploadId commit
        // lock. Two generations are sufficient to exercise the mixed-shard
        // hazard, while each waiter sits behind at most one cross-disk rename.
        commit_barrier.wait_until_paused().await;
        commit_barrier.release();

        let mut results = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            let outcome = joined.expect("put_object_part task should not panic");
            results.push(outcome.expect("every concurrent same-part resend must succeed without lock timeout"));
        }
        assert_eq!(results.len(), candidates.len());

        // Exactly one generation is visible after the serialized commits, and its
        // recorded size/etag matches one of the payloads we actually wrote.
        let parts = ecstore
            .list_object_parts(
                &bucket,
                object,
                &upload.upload_id,
                None,
                crate::set_disk::MAX_PARTS_COUNT,
                &ObjectOptions::default(),
            )
            .await
            .expect("multipart parts should be readable after concurrent resends");
        assert_eq!(parts.parts.len(), 1, "only one generation of part 1 should remain visible");
        let visible = &parts.parts[0];
        assert_eq!(visible.part_num, 1);

        let winner = results
            .iter()
            .find(|(info, _)| info.etag == visible.etag && info.size == visible.size)
            .map(|(_, payload)| payload.clone())
            .expect("the visible part must match exactly one payload that was committed");

        let completed = ecstore
            .clone()
            .complete_multipart_upload(
                &bucket,
                object,
                &upload.upload_id,
                vec![crate::storage_api_contracts::multipart::CompletePart {
                    part_num: 1,
                    etag: visible.etag.clone(),
                    checksum_crc32: None,
                    checksum_crc32c: None,
                    checksum_sha1: None,
                    checksum_sha256: None,
                    checksum_crc64nvme: None,
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("complete multipart upload should succeed with the committed generation");
        assert_eq!(completed.size, winner.len() as i64);

        // The reassembled object bytes must equal the winning generation exactly
        // — proof that no shard from a different generation leaked into the read.
        let mut reader = ecstore
            .get_object_reader(&bucket, object, None, http::HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("completed object should be readable");
        let bytes = reader.read_all().await.expect("object bytes should be readable");
        assert_eq!(bytes, winner, "reassembled object must match one intact generation, not a mix of shards");
    }

    #[tokio::test]
    #[serial]
    async fn cleanup_removes_empty_multipart_sha_dirs() {
        let (paths, ecstore) = setup_test_env().await;
        let bucket = format!("stale-empty-sha-{}", Uuid::new_v4().simple());
        let object = "empty-sha/object.txt";
        let sha_dir = multipart_sha_dir(&bucket, object);
        for path in &paths {
            fs::create_dir_all(path.join(RUSTFS_META_MULTIPART_BUCKET).join(&sha_dir))
                .await
                .expect("empty multipart sha dir should be created for cleanup");
        }

        cleanup_empty_multipart_sha_dirs_on_local_disks(&ecstore.pools[0].disk_set[0]).await;

        for path in &paths {
            assert!(
                !path.join(RUSTFS_META_MULTIPART_BUCKET).join(&sha_dir).exists(),
                "empty multipart sha dir should be removed"
            );
        }
    }
}
