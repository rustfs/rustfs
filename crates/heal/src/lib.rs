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

#![recursion_limit = "256"]

mod error;
pub mod heal;

pub use error::{Error, Result};
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
pub use heal::{
    HealAdmissionTelemetry, HealManager, HealOperationsSnapshot, HealOptions, HealPriority, HealPriorityCounts, HealRequest,
    HealSourceCounts, HealType,
    channel::HealChannelProcessor,
    progress::{HealProgress, aggregate_heal_progress},
    resume::{ReplacementRecoveryBlock, ReplacementRecoveryRecord, ReplacementRecoveryState, ResumeUtils},
};
use rustfs_concurrency::WorkloadAdmissionSnapshotProvider;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, OnceCell};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

const LOG_COMPONENT_HEAL: &str = "heal";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
const EVENT_HEAL_RUNTIME_STATE: &str = "heal_runtime_state";

// Global cancellation token for heal and related services
static GLOBAL_AHM_SERVICES_CANCEL_TOKEN: OnceLock<CancellationToken> = OnceLock::new();

/// Initialize the global heal services cancellation token
pub fn init_ahm_services_cancel_token(cancel_token: CancellationToken) -> Result<()> {
    GLOBAL_AHM_SERVICES_CANCEL_TOKEN
        .set(cancel_token)
        .map_err(|_| Error::Config("Heal services cancel token already initialized".to_string()))
}

/// Get the global heal services cancellation token
pub fn get_ahm_services_cancel_token() -> Option<&'static CancellationToken> {
    GLOBAL_AHM_SERVICES_CANCEL_TOKEN.get()
}

/// Create and initialize the global heal services cancellation token
pub fn create_ahm_services_cancel_token() -> CancellationToken {
    let cancel_token = CancellationToken::new();
    init_ahm_services_cancel_token(cancel_token.clone()).expect("Heal services cancel token already initialized");
    cancel_token
}

/// Shutdown all heal services gracefully
pub async fn shutdown_ahm_services() -> Result<()> {
    if let Some(runtime) = GLOBAL_HEAL_RUNTIME.get() {
        return runtime.shutdown(GLOBAL_AHM_SERVICES_CANCEL_TOKEN.get()).await;
    }
    if let Some(cancel_token) = GLOBAL_AHM_SERVICES_CANCEL_TOKEN.get() {
        cancel_token.cancel();
    }
    Ok(())
}

// The runtime retains the shared future that owns the actual consumer handle.
// Each waiter only clones the completion receipt; dropping one cannot detach
// the worker, and completed success/failure remains available to later waiters.
struct ConsumerJoin {
    completion: Option<Shared<BoxFuture<'static, std::result::Result<(), Arc<Error>>>>>,
}

impl ConsumerJoin {
    fn new(task: Option<tokio::task::JoinHandle<()>>) -> Self {
        Self {
            completion: task.map(|task| {
                async move {
                    task.await
                        .map_err(|error| Arc::new(Error::other(format!("Heal consumer failed to join: {error}"))))
                }
                .boxed()
                .shared()
            }),
        }
    }

    fn from_result(task: tokio::task::JoinHandle<Result<()>>) -> Self {
        Self {
            completion: Some(
                async move {
                    task.await
                        .map_err(|error| Arc::new(Error::other(format!("Heal consumer failed to join: {error}"))))?
                        .map_err(Arc::new)
                }
                .boxed()
                .shared(),
            ),
        }
    }

    async fn wait(&self) -> Result<()> {
        let Some(completion) = &self.completion else {
            return Err(Error::other("Heal consumer did not start with an owned handle"));
        };
        completion
            .clone()
            .await
            .map_err(|error| Error::other(format!("Heal consumer failed: {error}")))
    }
}

struct HealRuntime {
    manager: Arc<HealManager>,
    channel_processor: Arc<Mutex<HealChannelProcessor>>,
    channel_stop: CancellationToken,
    channel_task: ConsumerJoin,
    mrf_stop: CancellationToken,
    mrf_task: ConsumerJoin,
}

impl HealRuntime {
    async fn shutdown(&self, ahm_stop: Option<&CancellationToken>) -> Result<()> {
        let manager_result = self.manager.stop().await;
        self.finish_shutdown(manager_result, ahm_stop).await
    }

    async fn finish_shutdown(&self, manager_result: Result<()>, ahm_stop: Option<&CancellationToken>) -> Result<()> {
        if !self.manager.stop_handoff_completed() {
            return manager_result.and_then(|()| Err(Error::other("Heal manager stop returned without durable handoff")));
        }
        let mut first_error = manager_result.err();
        self.channel_stop.cancel();
        if let Some(stop) = ahm_stop {
            stop.cancel();
        }
        if let Err(error) = self.channel_task.wait().await {
            first_error.get_or_insert(error);
        }
        // The active channel command must finish its final publication/reply
        // before the MRF consumer closes its receivers.
        self.mrf_stop.cancel();
        if let Err(error) = self.mrf_task.wait().await {
            first_error.get_or_insert(error);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Process-wide heal runtime. Async single-flight initialization prevents two
/// callers from starting independent managers or channel processors.
static GLOBAL_HEAL_RUNTIME: OnceCell<HealRuntime> = OnceCell::const_new();
static GLOBAL_HEAL_RUNTIME_INIT: Mutex<()> = Mutex::const_new(());
static GLOBAL_HEAL_ACTIVE_TASKS: AtomicU64 = AtomicU64::new(0);
static GLOBAL_HEAL_QUEUE_LENGTH: AtomicU64 = AtomicU64::new(0);

/// Local view of durable replacement recovery state. `definitive` only covers
/// the local survivor-disk records; a distributed caller must additionally
/// establish that every peer returned a compatible snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplacementRecoverySnapshot {
    pub records: Vec<ReplacementRecoveryRecord>,
    pub definitive: bool,
    pub reason: Option<String>,
}

/// Read-only recovery evidence from this node's local disks. It is never
/// merged with peer status and does not certify completion.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalHealRecoveryDiagnostics {
    pub scope: &'static str,
    pub definitive: bool,
    pub reason: Option<String>,
    pub root_blocked: Vec<RootHealRecoveryBlock>,
    pub checkpoint_blocked: Vec<BlockedCheckpointRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RootHealRecoveryBlock {
    pub task_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockedCheckpointRecord {
    pub task_id: String,
    pub disk_id: Option<uuid::Uuid>,
    pub reason: String,
    pub blocked_at: Option<u64>,
    pub revalidate_after: Option<u64>,
    pub validation: CheckpointValidation,
}

/// A bounded, read-only observation of the execution reader's validation.
/// Acceptance proves neither a live owner/fence nor permission to execute.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointValidation {
    schema_version: Option<u32>,
    stage: CheckpointValidationStage,
    signature_present: Option<bool>,
    signature_valid: Option<bool>,
    task_identity_matches: Option<bool>,
    legacy_rewind: Option<bool>,
    reader_accepted: Option<bool>,
    recovery_status: CheckpointRecoveryStatus,
    owner_fence: &'static str,
    execution_authorized: bool,
}

impl Default for CheckpointValidation {
    fn default() -> Self {
        Self {
            schema_version: None,
            stage: CheckpointValidationStage::ReadUnavailable,
            signature_present: None,
            signature_valid: None,
            task_identity_matches: None,
            legacy_rewind: None,
            reader_accepted: None,
            recovery_status: CheckpointRecoveryStatus::Unknown,
            owner_fence: "unknown",
            execution_authorized: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckpointValidationStage {
    InvalidTaskId,
    SnapshotMissing,
    ReadUnavailable,
    TooLarge,
    BudgetExhausted,
    Malformed,
    IdentityConflict,
    InvalidDeadline,
    InvalidAdminSchema,
    FutureSchema,
    SignatureMissing,
    DigestMismatch,
    Validated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckpointRecoveryStatus {
    Unknown,
    Rejected,
    CandidateExecutionUnauthorized,
}

impl LocalHealRecoveryDiagnostics {
    fn unavailable(reason: &str) -> Self {
        Self {
            scope: "local_disks",
            definitive: false,
            reason: Some(reason.to_owned()),
            root_blocked: Vec::new(),
            checkpoint_blocked: Vec::new(),
        }
    }
}

#[cfg(test)]
#[derive(Default)]
struct HealRuntimeInitTestHook {
    starts: AtomicU64,
    stops: AtomicU64,
    fail_channel_init: std::sync::atomic::AtomicBool,
    pause_after_start: std::sync::atomic::AtomicBool,
    start_paused: tokio::sync::Notify,
    release_start: tokio::sync::Notify,
}

#[cfg(test)]
static HEAL_RUNTIME_INIT_TEST_HOOK: OnceLock<Arc<HealRuntimeInitTestHook>> = OnceLock::new();

#[cfg(test)]
async fn test_hook_after_manager_start() {
    if let Some(hook) = HEAL_RUNTIME_INIT_TEST_HOOK.get() {
        hook.starts.fetch_add(1, Ordering::SeqCst);
        if hook.pause_after_start.load(Ordering::SeqCst) {
            hook.start_paused.notify_one();
            hook.release_start.notified().await;
        }
    }
}

async fn stop_initializing_manager(manager: &HealManager) -> Result<()> {
    #[cfg(test)]
    if let Some(hook) = HEAL_RUNTIME_INIT_TEST_HOOK.get() {
        hook.stops.fetch_add(1, Ordering::SeqCst);
    }
    manager.stop().await
}

async fn run_owned_initialization<T, F>(initialization: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    tokio::spawn(initialization)
        .await
        .map_err(|err| Error::Other(format!("Heal runtime initialization task failed: {err}")))?
}

/// Initialize and start heal manager with channel processor
pub async fn init_heal_manager(
    storage: Arc<dyn heal::storage::HealStorageAPI>,
    config: Option<heal::manager::HealConfig>,
) -> Result<Arc<HealManager>> {
    init_heal_manager_with_workload_provider(storage, config, None).await
}

/// Initialize and start heal manager with channel processor and workload snapshots.
pub async fn init_heal_manager_with_workload_provider(
    storage: Arc<dyn heal::storage::HealStorageAPI>,
    config: Option<heal::manager::HealConfig>,
    workload_provider: Option<Arc<dyn WorkloadAdmissionSnapshotProvider + Send + Sync>>,
) -> Result<Arc<HealManager>> {
    // Run initialization in an owned task so cancelling an HTTP/startup caller
    // cannot abandon a manager after its scheduler has been spawned.
    run_owned_initialization(async move {
        let _init_guard = GLOBAL_HEAL_RUNTIME_INIT.lock().await;
        if GLOBAL_HEAL_RUNTIME.get().is_some() {
            return Err(Error::Config("Heal manager already initialized".to_string()));
        }

        let heal_manager = Arc::new(HealManager::new_with_workload_provider(storage, config, workload_provider));
        if let Err(err) = heal_manager.start().await {
            let _ = stop_initializing_manager(&heal_manager).await;
            return Err(err);
        }

        #[cfg(test)]
        test_hook_after_manager_start().await;

        #[cfg(test)]
        let force_channel_failure = HEAL_RUNTIME_INIT_TEST_HOOK
            .get()
            .is_some_and(|hook| hook.fail_channel_init.load(Ordering::SeqCst));
        #[cfg(not(test))]
        let force_channel_failure = false;
        let channel_receiver = if force_channel_failure {
            Err("forced heal channel initialization failure")
        } else {
            rustfs_heal_contracts::heal_channel::init_heal_channels()
        };
        let (receiver, receipt_receiver) = match channel_receiver {
            Ok(receivers) => receivers,
            Err(err) => {
                stop_initializing_manager(&heal_manager).await?;
                return Err(Error::Config(err.to_string()));
            }
        };
        let channel_processor = Arc::new(Mutex::new(HealChannelProcessor::new(heal_manager.clone())));
        // Channel initialization can fail and be retried. Do not claim the
        // process-wide MRF channels before this recoverable boundary succeeds.
        let mrf_stop = CancellationToken::new();
        let mrf_task = ConsumerJoin::new(heal::mrf_queue::spawn_owned_mrf_consumer(heal_manager.clone(), mrf_stop.clone()));
        let channel_stop = CancellationToken::new();
        let processor_stop = channel_stop.clone();
        let processor = channel_processor.clone();
        let channel_task = ConsumerJoin::from_result(tokio::spawn(async move {
            let mut processor = processor.lock().await;
            let result = processor
                .start_with_shutdown(receiver, receipt_receiver, processor_stop)
                .await;
            if let Err(e) = &result {
                error!(
                    target: "rustfs::heal",
                    event = EVENT_HEAL_RUNTIME_STATE,
                    component = LOG_COMPONENT_HEAL,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    state = "channel_processor_failed",
                    error = %e,
                    "Heal runtime channel processor failed"
                );
            }
            result
        }));
        GLOBAL_HEAL_RUNTIME
            .set(HealRuntime {
                manager: heal_manager.clone(),
                channel_processor,
                channel_stop,
                channel_task,
                mrf_stop,
                mrf_task,
            })
            .map_err(|_| Error::Config("Heal manager already initialized".to_string()))?;

        info!(
            target: "rustfs::heal",
            event = EVENT_HEAL_RUNTIME_STATE,
            component = LOG_COMPONENT_HEAL,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "initialized",
            "Heal runtime initialized"
        );
        Ok(heal_manager)
    })
    .await
}

/// Get global heal manager instance
pub fn get_heal_manager() -> Option<&'static Arc<HealManager>> {
    GLOBAL_HEAL_RUNTIME.get().map(|runtime| &runtime.manager)
}

/// Get global heal channel processor instance
pub fn get_heal_channel_processor() -> Option<&'static Arc<Mutex<HealChannelProcessor>>> {
    GLOBAL_HEAL_RUNTIME.get().map(|runtime| &runtime.channel_processor)
}

pub fn heal_runtime_initialized() -> bool {
    get_heal_manager().is_some() && get_heal_channel_processor().is_some()
}

pub fn current_heal_active_tasks() -> u64 {
    GLOBAL_HEAL_ACTIVE_TASKS.load(Ordering::Relaxed)
}

pub fn current_heal_queue_length() -> u64 {
    GLOBAL_HEAL_QUEUE_LENGTH.load(Ordering::Relaxed)
}

pub async fn current_heal_operations_snapshot() -> HealOperationsSnapshot {
    if let Some(manager) = get_heal_manager() {
        manager.operations_snapshot().await
    } else {
        HealOperationsSnapshot {
            queue_length: current_heal_queue_length(),
            active_tasks: current_heal_active_tasks(),
            ..Default::default()
        }
    }
}

pub async fn current_heal_progress_snapshot() -> Option<HealProgress> {
    if let Some(manager) = get_heal_manager() {
        manager.active_progress_snapshot().await
    } else {
        None
    }
}

/// Durable replacement generations that this node's discovery could not
/// verify or resolve, with their reasons. Node-local; empty when the heal
/// runtime is not initialized.
pub fn current_replacement_recovery_blocks() -> Vec<ReplacementRecoveryBlock> {
    get_heal_manager()
        .map(|manager| manager.replacement_recovery_blocks())
        .unwrap_or_default()
}

/// List local root and checkpoint blocks without repairing records or
/// revalidating, resetting or removing checkpoint markers.
pub async fn current_local_heal_recovery_diagnostics() -> LocalHealRecoveryDiagnostics {
    match get_heal_manager() {
        Some(manager) => manager.local_recovery_diagnostics().await,
        None => LocalHealRecoveryDiagnostics::unavailable("heal_runtime_not_initialized"),
    }
}

/// Read all local survivor-disk replacement records without conflating an I/O
/// failure or conflicting copies with successful completion.
pub async fn current_replacement_recovery_snapshot() -> ReplacementRecoverySnapshot {
    if !heal_runtime_initialized() {
        return ReplacementRecoverySnapshot {
            records: Vec::new(),
            definitive: false,
            reason: Some("heal runtime is not initialized".to_string()),
        };
    }

    let disks = {
        let local_disk_map = heal::local_disk_map_read().await;
        local_disk_map.values().flatten().cloned().collect::<Vec<_>>()
    };
    if disks.is_empty() {
        return ReplacementRecoverySnapshot {
            records: Vec::new(),
            definitive: false,
            reason: Some("no local survivor disks are available".to_string()),
        };
    }

    let mut records = BTreeMap::<String, ReplacementRecoveryRecord>::new();
    let mut reason = None;
    for disk in disks {
        match ResumeUtils::get_replacement_recovery_records(&disk).await {
            Ok(disk_records) => {
                for record in disk_records {
                    let task_id = record.task_id.clone();
                    if matches!(record.state, ReplacementRecoveryState::Unknown) {
                        reason.get_or_insert_with(|| "invalid durable replacement record".to_string());
                    }
                    match records.entry(task_id.clone()) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(record);
                        }
                        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &record => {}
                        std::collections::btree_map::Entry::Occupied(entry)
                            if matches!(entry.get().state, ReplacementRecoveryState::CleanupPending)
                                && matches!(record.state, ReplacementRecoveryState::Completed) => {}
                        std::collections::btree_map::Entry::Occupied(mut entry)
                            if matches!(entry.get().state, ReplacementRecoveryState::Completed)
                                && matches!(record.state, ReplacementRecoveryState::CleanupPending) =>
                        {
                            entry.insert(record);
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            entry.insert(ReplacementRecoveryRecord {
                                task_id,
                                state: ReplacementRecoveryState::Unknown,
                                generation: None,
                                set_disk_id: None,
                                target_slots: Vec::new(),
                                reason: Some("conflicting durable replacement records across survivor disks".to_string()),
                                verified_at: None,
                            });
                            reason.get_or_insert_with(|| "conflicting durable replacement records".to_string());
                        }
                    }
                }
            }
            Err(error) => {
                reason.get_or_insert_with(|| format!("failed to read local replacement recovery records: {error}"));
            }
        }
    }

    for record in records.values_mut() {
        if matches!(record.state, ReplacementRecoveryState::Running)
            && !match get_heal_manager() {
                Some(manager) => manager.replacement_generation_is_running(&record.task_id).await,
                None => false,
            }
        {
            record.state = ReplacementRecoveryState::Unknown;
            record.reason = Some("durable rebuilding generation has no active replacement owner".to_string());
            reason.get_or_insert_with(|| "replacement execution ownership is not established".to_string());
        }
    }

    ReplacementRecoverySnapshot {
        records: records.into_values().collect(),
        definitive: reason.is_none(),
        reason,
    }
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(crate) fn set_heal_active_tasks(count: usize) {
    GLOBAL_HEAL_ACTIVE_TASKS.store(usize_to_u64_saturated(count), Ordering::Relaxed);
}

pub(crate) fn set_heal_queue_length(count: usize) {
    GLOBAL_HEAL_QUEUE_LENGTH.store(usize_to_u64_saturated(count), Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::{
        ConsumerJoin, Error, HEAL_RUNTIME_INIT_TEST_HOOK, HealRuntime, HealRuntimeInitTestHook, get_heal_channel_processor,
        get_heal_manager, heal::DiskStore, heal::manager::HealConfig, heal::storage::HealListItem, heal::storage::HealObjectInfo,
        heal::storage::HealStorageAPI, init_heal_manager, run_owned_initialization,
    };
    use crate::heal::storage_api::status::BucketInfo;
    use rustfs_heal_contracts::heal_channel::HealOpts;
    use rustfs_madmin::heal_commands::HealResultItem;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::{Notify, oneshot};

    struct MockStorage;

    #[async_trait::async_trait]
    impl HealStorageAPI for MockStorage {
        async fn get_object_meta(&self, _bucket: &str, _object: &str) -> Result<Option<HealObjectInfo>, Error> {
            Ok(None)
        }

        async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> Result<Vec<u8>, Error> {
            Ok(Vec::new())
        }

        async fn get_bucket_info(&self, _bucket: &str) -> Result<Option<BucketInfo>, Error> {
            Ok(None)
        }

        async fn list_buckets_for_admin_heal(
            &self,
            _selector: Option<crate::heal::storage_api::storage::DiskSetSelector>,
        ) -> Result<Vec<BucketInfo>, Error> {
            self.list_buckets().await
        }

        async fn list_buckets(&self) -> Result<Vec<BucketInfo>, Error> {
            Ok(Vec::new())
        }

        async fn object_exists(&self, _bucket: &str, _object: &str) -> Result<bool, Error> {
            Ok(false)
        }

        async fn heal_object(
            &self,
            _bucket: &str,
            _object: &str,
            _version_id: Option<&str>,
            _opts: &HealOpts,
        ) -> Result<(HealResultItem, Option<Error>), Error> {
            Ok((HealResultItem::default(), None))
        }

        async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> Result<HealResultItem, Error> {
            Ok(HealResultItem::default())
        }

        async fn heal_format(&self, _dry_run: bool) -> Result<(HealResultItem, Option<Error>), Error> {
            Ok((HealResultItem::default(), None))
        }

        async fn list_objects_for_heal_page(
            &self,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<&str>,
            _include_lifecycle_object_info: bool,
        ) -> Result<(Vec<HealListItem>, Option<String>, bool), Error> {
            Ok((Vec::new(), None, false))
        }

        async fn get_disk_for_resume(&self, _set_disk_id: &str) -> Result<DiskStore, Error> {
            Err(Error::other("not implemented in tests"))
        }
    }

    #[tokio::test]
    async fn mrf_owned_join_survives_cancelled_shutdown_waiter() {
        let release = Arc::new(Notify::new());
        let completed = Arc::new(AtomicBool::new(false));
        let worker_release = release.clone();
        let worker_completed = completed.clone();
        let owner = Arc::new(ConsumerJoin::new(Some(tokio::spawn(async move {
            worker_release.notified().await;
            worker_completed.store(true, Ordering::Release);
        }))));
        let waiting_owner = owner.clone();
        let (started, entered) = oneshot::channel();
        let waiter = tokio::spawn(async move {
            let _ = started.send(());
            waiting_owner.wait().await
        });
        entered.await.expect("shutdown waiter polls the shared actual handle");
        waiter.abort();
        let _ = waiter.await;
        assert!(!completed.load(Ordering::Acquire));
        assert!(owner.completion.as_ref().expect("owned handle").peek().is_none());
        let second_owner = owner.clone();
        let second = tokio::spawn(async move { second_owner.wait().await });
        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), owner.wait())
            .await
            .expect("owned MRF worker completes after release")
            .expect("successful real join");
        second
            .await
            .expect("concurrent waiter completes")
            .expect("same successful receipt");
        assert!(completed.load(Ordering::Acquire));
        owner.wait().await.expect("repeat join receipt remains successful");
    }

    #[tokio::test]
    async fn mrf_owned_join_failure_stays_failed() {
        let owner = ConsumerJoin::new(Some(tokio::spawn(async { panic!("MRF owned-join test failure") })));
        assert!(owner.wait().await.is_err());
        assert!(owner.wait().await.is_err(), "a failed join must not become a successful receipt");
        let unavailable = ConsumerJoin::new(None);
        assert!(unavailable.wait().await.is_err(), "a missing owned handle is not a successful receipt");
    }

    #[tokio::test]
    async fn owned_initialization_survives_waiter_cancellation() {
        let completed = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());
        let (started_tx, started_rx) = oneshot::channel();
        let task_completed = completed.clone();
        let task_release = release.clone();
        let waiter = tokio::spawn(async move {
            run_owned_initialization(async move {
                let _ = started_tx.send(());
                task_release.notified().await;
                task_completed.store(true, Ordering::Release);
                Ok(())
            })
            .await
        });

        started_rx.await.expect("owned initializer should start");
        waiter.abort();
        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !completed.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned initializer should outlive its cancelled waiter");
        assert!(completed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn runtime_initialization_is_atomic_cancellation_safe_and_retryable() {
        let hook = Arc::new(HealRuntimeInitTestHook::default());
        assert!(
            HEAL_RUNTIME_INIT_TEST_HOOK.set(hook.clone()).is_ok(),
            "runtime initialization hook should be installed once"
        );
        let config = HealConfig {
            enable_auto_heal: false,
            ..HealConfig::default()
        };

        hook.fail_channel_init.store(true, Ordering::SeqCst);
        let failed = init_heal_manager(Arc::new(MockStorage), Some(config.clone())).await;
        assert!(matches!(failed, Err(Error::Config(message)) if message == "forced heal channel initialization failure"));
        assert_eq!(hook.starts.load(Ordering::SeqCst), 1);
        assert_eq!(hook.stops.load(Ordering::SeqCst), 1);
        assert!(get_heal_manager().is_none());
        assert!(get_heal_channel_processor().is_none());

        hook.fail_channel_init.store(false, Ordering::SeqCst);
        hook.pause_after_start.store(true, Ordering::SeqCst);
        let first_config = config.clone();
        let first = tokio::spawn(async move { init_heal_manager(Arc::new(MockStorage), Some(first_config)).await });
        hook.start_paused.notified().await;
        let second = tokio::spawn(async move { init_heal_manager(Arc::new(MockStorage), Some(config)).await });
        tokio::task::yield_now().await;

        assert_eq!(hook.starts.load(Ordering::SeqCst), 2, "concurrent loser must not start a manager");
        assert!(get_heal_manager().is_none(), "manager must not publish before the runtime is complete");
        assert!(
            get_heal_channel_processor().is_none(),
            "channel processor must publish atomically with the manager"
        );

        first.abort();
        hook.release_start.notify_one();
        let conflict = second
            .await
            .expect("second caller should complete")
            .expect_err("second caller should observe the initialized runtime");
        assert!(matches!(conflict, Error::Config(message) if message == "Heal manager already initialized"));
        let manager = get_heal_manager().expect("cancelled caller's owned initializer should publish the manager");
        assert!(get_heal_channel_processor().is_some());
        assert_eq!(hook.starts.load(Ordering::SeqCst), 2);
        assert_eq!(hook.stops.load(Ordering::SeqCst), 1);

        manager.stop().await.expect("manager should stop cleanly");
        super::shutdown_ahm_services()
            .await
            .expect("retried initialization retains an actual MRF join receipt");
        let runtime = super::GLOBAL_HEAL_RUNTIME.get().expect("published complete runtime");
        assert!(
            runtime
                .mrf_task
                .completion
                .as_ref()
                .expect("actual owned handle")
                .peek()
                .is_some_and(|result| result.is_ok())
        );
    }
    #[tokio::test]
    async fn channel_owned_join_inner_failure_stays_failed() {
        let owner = ConsumerJoin::from_result(tokio::spawn(async { Err(Error::Config("channel loop failure".into())) }));
        for _ in 0..2 {
            assert!(owner.wait().await.unwrap_err().to_string().contains("channel loop failure"));
        }
    }

    #[tokio::test]
    async fn shutdown_pre_handoff_error_keeps_all_consumer_tokens_open() {
        let manager = Arc::new(crate::HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None));
        let channel_stop = tokio_util::sync::CancellationToken::new();
        let mrf_stop = tokio_util::sync::CancellationToken::new();
        let ahm_stop = tokio_util::sync::CancellationToken::new();
        let channel = channel_stop.clone();
        let mrf = mrf_stop.clone();
        let runtime = HealRuntime {
            channel_processor: Arc::new(tokio::sync::Mutex::new(crate::HealChannelProcessor::new(manager.clone()))),
            manager,
            channel_stop,
            channel_task: ConsumerJoin::from_result(tokio::spawn(async move {
                channel.cancelled().await;
                Ok(())
            })),
            mrf_stop,
            mrf_task: ConsumerJoin::new(Some(tokio::spawn(async move { mrf.cancelled().await }))),
        };
        // Inject only the orchestration phase result; actual persistence failure
        // and the witness ordering are exercised by the manager recovery test.
        let error = runtime
            .finish_shutdown(Err(Error::Config("handoff failed".into())), Some(&ahm_stop))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(ref message) if message == "handoff failed"));
        assert!(!runtime.manager.stop_handoff_completed());
        assert!(!runtime.channel_stop.is_cancelled());
        assert!(!runtime.mrf_stop.is_cancelled());
        assert!(!ahm_stop.is_cancelled());
        runtime.shutdown(Some(&ahm_stop)).await.unwrap();
        assert!(runtime.manager.stop_handoff_completed());
    }

    #[tokio::test]
    async fn shutdown_failed_manager_waits_channel_before_mrf_and_survives_cancelled_waiter() {
        let manager = Arc::new(crate::HealManager::new_without_root_recovery_for_test(
            Arc::new(MockStorage),
            Some(HealConfig {
                heal_interval: std::time::Duration::ZERO,
                enable_auto_heal: false,
                event_driven_scheduler_enable: false,
                ..Default::default()
            }),
        ));
        manager.start().await.unwrap();
        let manager_error = manager
            .stop()
            .await
            .expect_err("actual scheduler interval panic is a sticky join failure")
            .to_string();
        assert!(manager.stop_handoff_completed());
        let channel_stop = tokio_util::sync::CancellationToken::new();
        let mrf_stop = tokio_util::sync::CancellationToken::new();
        let ahm_stop = tokio_util::sync::CancellationToken::new();
        let channel = channel_stop.clone();
        let mrf = mrf_stop.clone();
        let channel_mrf = mrf_stop.clone();
        let completed = Arc::new(AtomicBool::new(false));
        let mrf_completed = completed.clone();
        let (selected_tx, selected_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let runtime = Arc::new(HealRuntime {
            channel_processor: Arc::new(tokio::sync::Mutex::new(crate::HealChannelProcessor::new(manager.clone()))),
            manager,
            channel_stop,
            channel_task: ConsumerJoin::from_result(tokio::spawn(async move {
                channel.cancelled().await;
                assert!(!channel_mrf.is_cancelled());
                selected_tx.send(()).unwrap();
                release_rx.await.unwrap();
                assert!(!channel_mrf.is_cancelled(), "MRF stays open through final channel publication");
                Ok(())
            })),
            mrf_stop,
            mrf_task: ConsumerJoin::new(Some(tokio::spawn(async move {
                mrf.cancelled().await;
                mrf_completed.store(true, Ordering::Release);
            }))),
        });
        let stopping = runtime.clone();
        let ahm = ahm_stop.clone();
        let waiter = tokio::spawn(async move { stopping.shutdown(Some(&ahm)).await });
        tokio::time::timeout(std::time::Duration::from_secs(1), selected_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(ahm_stop.is_cancelled());
        assert!(!runtime.mrf_stop.is_cancelled());
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let stopping = runtime.clone();
        let retry = tokio::spawn(async move { stopping.shutdown(None).await });
        tokio::task::yield_now().await;
        assert!(!retry.is_finished(), "cancelled waiter cannot detach the held channel worker");
        assert!(!runtime.mrf_stop.is_cancelled());
        release_tx.send(()).unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), retry)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap_err().to_string(), manager_error);
        assert!(completed.load(Ordering::Acquire));
        assert_eq!(runtime.shutdown(None).await.unwrap_err().to_string(), manager_error);
    }

    #[tokio::test]
    async fn shutdown_channel_failure_still_awaits_actual_mrf_failure() {
        let manager = Arc::new(crate::HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None));
        let channel_stop = tokio_util::sync::CancellationToken::new();
        let mrf_stop = tokio_util::sync::CancellationToken::new();
        let channel = channel_stop.clone();
        let mrf = mrf_stop.clone();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let runtime = Arc::new(HealRuntime {
            channel_processor: Arc::new(tokio::sync::Mutex::new(crate::HealChannelProcessor::new(manager.clone()))),
            manager,
            channel_stop,
            channel_task: ConsumerJoin::from_result(tokio::spawn(async move {
                channel.cancelled().await;
                Err(Error::Config("channel inner failure".into()))
            })),
            mrf_stop,
            mrf_task: ConsumerJoin::new(Some(tokio::spawn(async move {
                mrf.cancelled().await;
                entered_tx.send(()).unwrap();
                release_rx.await.unwrap();
                panic!("actual MRF join failure");
            }))),
        });
        let stopping = runtime.clone();
        let waiter = tokio::spawn(async move { stopping.shutdown(None).await });
        tokio::time::timeout(std::time::Duration::from_secs(1), entered_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(!waiter.is_finished(), "channel error cannot skip the actual MRF join");
        release_tx.send(()).unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(result.unwrap_err().to_string().contains("channel inner failure"));
        assert!(runtime.channel_task.wait().await.is_err());
        assert!(runtime.mrf_task.wait().await.is_err());
    }
}
