// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Bounded observations of original scheduler task completions in test builds.

use super::worker::{VectorMaintenanceExecutionError, VectorMaintenanceWorkerResult};
use crate::table_catalog::{
    CatalogIoErrorKind, CatalogMetadataRefRecordV1, TableCatalogStoreError, VectorMaintenanceFailureCode,
    VectorMaintenanceJobSnapshot, VectorMaintenanceJobState, VectorMaintenanceKind, VectorMaintenanceLeaseToken,
    VectorMaintenanceResourceV1, VectorMaintenanceWork, VectorRetirementTargetV1,
};
use parking_lot::Mutex;
use rustfs_lock::{LockError, ObjectKey};
use serde::{Serialize, Serializer};
use std::error::Error;
use std::fmt::{self, Write};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::task::{JoinError, JoinHandle};
use uuid::Uuid;

const MAX_COMPLETIONS: usize = 256;
const MAX_DIAGNOSTICS: usize = 8;
const MAX_SOURCES: usize = 8;
const MAX_MESSAGE_BYTES: usize = 2048;

type WorkerResult = Result<VectorMaintenanceWorkerResult, VectorMaintenanceExecutionError>;
type SourceClassifier = fn(&(dyn Error + 'static)) -> Option<VectorMaintenanceObservedSourceKind>;

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum VectorMaintenanceObservedOperation {
    Job,
    InspectRetirementRecovery,
    PrepareRecovery,
    PrepareGc,
    EnrollCurrent,
    EnrollDeleted,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum VectorMaintenanceObservedPhase {
    Admission,
    Claim,
    NativeRecovery,
    Readmission,
    Executor,
    Heartbeat,
    FinishRead,
    Finish,
    FinishReadback,
    Preparation,
    Join,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum VectorMaintenanceObservedErrorVariant {
    Executor,
    Catalog,
    Budget,
    Join,
    FailedJob,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum VectorMaintenanceObservedSourceKind {
    ExpectedHeadConflict,
    EngineCommitConflict,
    CatalogNotFound,
    CatalogAlreadyExists,
    CatalogConflict,
    CatalogInvalid,
    CatalogUnsupported,
    CatalogUnavailable,
    CatalogLockUnavailable,
    CatalogInternal,
    CatalogIoDenied,
    CatalogIoConflict,
    CatalogIoUnavailable,
    CatalogIoInternal,
    Io,
    Other,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceObservedSource {
    pub(crate) kind: VectorMaintenanceObservedSourceKind,
    pub(crate) message: String,
    pub(crate) io_raw_os_error: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceObservedFailure {
    pub(crate) phase: VectorMaintenanceObservedPhase,
    pub(crate) variant: VectorMaintenanceObservedErrorVariant,
    pub(crate) category: VectorMaintenanceFailureCode,
    pub(crate) message: String,
    pub(crate) sources: Vec<VectorMaintenanceObservedSource>,
    pub(crate) join_task_id: Option<String>,
    pub(crate) join_panic: bool,
    pub(crate) join_cancelled: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum VectorMaintenanceObservedResult {
    Prepared,
    Finished,
    Replay,
    Skipped,
    Error,
    JoinError,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceObservedDurableJob {
    pub(crate) job_id: Uuid,
    pub(crate) attempt: u16,
    pub(crate) revision: Uuid,
    pub(crate) state: VectorMaintenanceJobState,
    pub(crate) failure: Option<VectorMaintenanceFailureCode>,
}

#[derive(Debug, Serialize)]
pub(crate) struct VectorMaintenanceCompletion {
    pub(crate) sequence: u64,
    pub(crate) resource: VectorMaintenanceResourceV1,
    pub(crate) job_id: Option<Uuid>,
    pub(crate) attempt: Option<u16>,
    pub(crate) kind: Option<VectorMaintenanceKind>,
    pub(crate) operation: VectorMaintenanceObservedOperation,
    pub(crate) phase: VectorMaintenanceObservedPhase,
    pub(crate) result: VectorMaintenanceObservedResult,
    pub(crate) state: Option<VectorMaintenanceJobState>,
    pub(crate) error: Option<VectorMaintenanceObservedFailure>,
    pub(crate) diagnostics: Vec<VectorMaintenanceObservedFailure>,
    pub(crate) durable_job: Option<VectorMaintenanceObservedDurableJob>,
}

/// Snapshot clones retain immutable evidence without copying message buffers.
#[derive(Debug, Clone)]
pub(crate) struct VectorMaintenanceCompletionRecord(Arc<VectorMaintenanceCompletion>);

impl Deref for VectorMaintenanceCompletionRecord {
    type Target = VectorMaintenanceCompletion;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for VectorMaintenanceCompletionRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_ref().serialize(serializer)
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceCompletionSnapshot {
    pub(crate) next_sequence: u64,
    pub(crate) overflow: bool,
    pub(crate) completions: Vec<VectorMaintenanceCompletionRecord>,
}

struct ObserverState {
    gc: GcObserverState,
    next_sequence: u64,
    started: usize,
    overflow: bool,
    completions: Vec<VectorMaintenanceCompletionRecord>,
}

pub(crate) struct VectorMaintenanceCompletionObserver {
    classify_source: SourceClassifier,
    state: Mutex<ObserverState>,
}

impl VectorMaintenanceCompletionObserver {
    pub(crate) fn new(classify_source: SourceClassifier) -> Arc<Self> {
        Arc::new(Self {
            classify_source,
            state: Mutex::new(ObserverState {
                gc: GcObserverState::default(),
                next_sequence: 1,
                started: 0,
                overflow: false,
                completions: Vec::with_capacity(MAX_COMPLETIONS),
            }),
        })
    }

    pub(crate) fn snapshot(&self) -> VectorMaintenanceCompletionSnapshot {
        let state = self.state.lock();
        VectorMaintenanceCompletionSnapshot {
            next_sequence: state.next_sequence,
            overflow: state.overflow,
            completions: state.completions.clone(),
        }
    }

    fn overflow(&self) {
        self.state.lock().overflow = true;
    }

    pub(super) fn begin(
        self: &Arc<Self>,
        resource: &VectorMaintenanceResourceV1,
        job_id: Option<Uuid>,
        kind: Option<VectorMaintenanceKind>,
        operation: VectorMaintenanceObservedOperation,
    ) -> VectorMaintenanceTaskObservation {
        let mut state = self.state.lock();
        let data = if state.started == MAX_COMPLETIONS {
            state.overflow = true;
            None
        } else {
            state.started += 1;
            Some(TaskData {
                resource: resource.clone(),
                requested_job_id: job_id,
                job_id: None,
                attempt: None,
                kind,
                operation,
                phase: VectorMaintenanceObservedPhase::Admission,
                diagnostic_phase: None,
                diagnostics: Vec::with_capacity(MAX_DIAGNOSTICS),
                durable_job: None,
                claimed_lease: None,
            })
        };
        VectorMaintenanceTaskObservation {
            observer: self.clone(),
            data: Arc::new(Mutex::new(data)),
        }
    }
}

struct TaskData {
    claimed_lease: Option<VectorMaintenanceLeaseToken>,
    resource: VectorMaintenanceResourceV1,
    requested_job_id: Option<Uuid>,
    job_id: Option<Uuid>,
    attempt: Option<u16>,
    kind: Option<VectorMaintenanceKind>,
    operation: VectorMaintenanceObservedOperation,
    phase: VectorMaintenanceObservedPhase,
    diagnostic_phase: Option<VectorMaintenanceObservedPhase>,
    diagnostics: Vec<VectorMaintenanceObservedFailure>,
    durable_job: Option<VectorMaintenanceObservedDurableJob>,
}

#[derive(Clone)]
pub(super) struct VectorMaintenanceTaskObservation {
    observer: Arc<VectorMaintenanceCompletionObserver>,
    data: Arc<Mutex<Option<TaskData>>>,
}

tokio::task_local! {
    static TASK_OBSERVATION: VectorMaintenanceTaskObservation;
}

pub(super) async fn scope<F: Future>(observation: Option<VectorMaintenanceTaskObservation>, future: F) -> F::Output {
    match observation {
        Some(observation) => TASK_OBSERVATION.scope(observation, future).await,
        None => future.await,
    }
}

pub(super) fn phase(phase: VectorMaintenanceObservedPhase) {
    let _ = TASK_OBSERVATION.try_with(|task| {
        if let Some(data) = task.data.lock().as_mut() {
            data.phase = phase;
        }
    });
}

pub(super) fn claimed(work: &VectorMaintenanceWork) {
    let _ = TASK_OBSERVATION.try_with(|task| {
        if let Some(data) = task.data.lock().as_mut() {
            let job = work.snapshot().job();
            if &data.resource != job.resource() || data.requested_job_id != Some(*job.job_id()) || data.kind != Some(*job.kind())
            {
                task.observer.overflow();
            }
            data.claimed_lease = Some(work.lease().clone());
            data.resource = job.resource().clone();
            data.job_id = Some(work.lease().job_id());
            data.attempt = Some(work.lease().attempt());
            data.kind = Some(*job.kind());
        }
    });
}

/// Captures each diagnostic before production selects the error it returns.
pub(super) fn diagnostic(phase: VectorMaintenanceObservedPhase, error: &VectorMaintenanceExecutionError) {
    record_diagnostic(phase, error, true);
}

pub(super) fn capture(phase: VectorMaintenanceObservedPhase, error: &VectorMaintenanceExecutionError) {
    record_diagnostic(phase, error, false);
}

fn record_diagnostic(phase: VectorMaintenanceObservedPhase, error: &VectorMaintenanceExecutionError, selected: bool) {
    let _ = TASK_OBSERVATION.try_with(|task| {
        if let Some(data) = task.data.lock().as_mut() {
            if selected {
                data.diagnostic_phase = Some(phase);
            }
            if data.diagnostics.len() == MAX_DIAGNOSTICS {
                task.observer.overflow();
            } else {
                data.diagnostics.push(task.failure(phase, error));
            }
        }
    });
}

pub(super) fn returning_diagnostic() {
    let _ = TASK_OBSERVATION.try_with(|task| {
        if let Some(data) = task.data.lock().as_mut() {
            if let Some(phase) = data.diagnostic_phase {
                data.phase = phase;
            } else {
                task.observer.overflow();
            }
        }
    });
}

pub(super) fn durable_job(snapshot: &VectorMaintenanceJobSnapshot) {
    let _ = TASK_OBSERVATION.try_with(|task| {
        if let Some(data) = task.data.lock().as_mut() {
            let job = snapshot.job();
            if &data.resource != job.resource() || data.job_id != Some(*job.job_id()) || data.attempt != Some(*job.attempt()) {
                task.observer.overflow();
                return;
            }
            data.durable_job = Some(VectorMaintenanceObservedDurableJob {
                job_id: *job.job_id(),
                attempt: *job.attempt(),
                revision: *job.revision(),
                state: *job.state(),
                failure: job.failure,
            });
        }
    });
}

impl VectorMaintenanceTaskObservation {
    fn failure(
        &self,
        phase: VectorMaintenanceObservedPhase,
        error: &VectorMaintenanceExecutionError,
    ) -> VectorMaintenanceObservedFailure {
        let (variant, join) = match error {
            VectorMaintenanceExecutionError::Executor { .. } => (VectorMaintenanceObservedErrorVariant::Executor, None),
            VectorMaintenanceExecutionError::Catalog(_) => (VectorMaintenanceObservedErrorVariant::Catalog, None),
            VectorMaintenanceExecutionError::Budget(_) => (VectorMaintenanceObservedErrorVariant::Budget, None),
            VectorMaintenanceExecutionError::Join(error) => (VectorMaintenanceObservedErrorVariant::Join, Some(error)),
            VectorMaintenanceExecutionError::FailedJob => (VectorMaintenanceObservedErrorVariant::FailedJob, None),
        };
        let mut sources = Vec::with_capacity(MAX_SOURCES);
        let mut source: Option<&(dyn Error + 'static)> = match error {
            VectorMaintenanceExecutionError::Executor { source, .. } => Some(source.as_ref()),
            VectorMaintenanceExecutionError::Catalog(error) => Some(error),
            VectorMaintenanceExecutionError::Budget(error) => Some(error),
            VectorMaintenanceExecutionError::Join(error) => Some(error),
            VectorMaintenanceExecutionError::FailedJob => None,
        };
        while let Some(current) = source {
            if sources.len() == MAX_SOURCES {
                self.observer.overflow();
                break;
            }
            let kind = (self.observer.classify_source)(current).unwrap_or_else(|| source_kind(current));
            sources.push(VectorMaintenanceObservedSource {
                kind,
                message: self.message(current),
                io_raw_os_error: current
                    .downcast_ref::<std::io::Error>()
                    .and_then(std::io::Error::raw_os_error),
            });
            source = current.source();
        }
        VectorMaintenanceObservedFailure {
            phase,
            variant,
            category: error.category(),
            message: self.message(error),
            sources,
            join_task_id: join.map(|error| error.id().to_string()),
            join_panic: join.is_some_and(JoinError::is_panic),
            join_cancelled: join.is_some_and(JoinError::is_cancelled),
        }
    }

    fn message(&self, error: &dyn Error) -> String {
        let mut output = BoundedMessage(String::with_capacity(MAX_MESSAGE_BYTES));
        if write!(&mut output, "{error}").is_err() {
            self.observer.overflow();
        }
        output.0
    }

    fn complete(self, result: &Result<WorkerResult, JoinError>) {
        let data = self.data.lock().take();
        let completion = data.map(|mut data| {
            let (result, state, error) = match result {
                Ok(Ok(VectorMaintenanceWorkerResult::Prepared)) => (VectorMaintenanceObservedResult::Prepared, None, None),
                Ok(Ok(VectorMaintenanceWorkerResult::Finished(state))) => {
                    let error = (*state == VectorMaintenanceJobState::Failed)
                        .then(|| self.failure(data.phase, &VectorMaintenanceExecutionError::FailedJob));
                    (VectorMaintenanceObservedResult::Finished, Some(*state), error)
                }
                Ok(Ok(VectorMaintenanceWorkerResult::Replay(state))) => {
                    (VectorMaintenanceObservedResult::Replay, Some(*state), None)
                }
                Ok(Ok(VectorMaintenanceWorkerResult::Skipped(_))) => (VectorMaintenanceObservedResult::Skipped, None, None),
                Ok(Err(error)) => (VectorMaintenanceObservedResult::Error, None, Some(self.failure(data.phase, error))),
                Err(error) => {
                    data.phase = VectorMaintenanceObservedPhase::Join;
                    let message = self.message(error);
                    let failure = VectorMaintenanceObservedFailure {
                        phase: data.phase,
                        variant: VectorMaintenanceObservedErrorVariant::Join,
                        category: VectorMaintenanceFailureCode::UnknownOutcome,
                        message,
                        sources: Vec::new(),
                        join_task_id: Some(error.id().to_string()),
                        join_panic: error.is_panic(),
                        join_cancelled: error.is_cancelled(),
                    };
                    (VectorMaintenanceObservedResult::JoinError, None, Some(failure))
                }
            };
            VectorMaintenanceCompletion {
                sequence: 0,
                resource: data.resource,
                job_id: data.job_id,
                attempt: data.attempt,
                kind: data.kind,
                operation: data.operation,
                phase: data.phase,
                result,
                state,
                error,
                diagnostics: data.diagnostics,
                durable_job: data.durable_job,
            }
        });
        let mut state = self.observer.state.lock();
        let sequence = state.next_sequence;
        match sequence.checked_add(1) {
            Some(next) => state.next_sequence = next,
            None => state.overflow = true,
        }
        if let Some(mut completion) = completion {
            completion.sequence = sequence;
            if state.completions.len() == MAX_COMPLETIONS {
                state.overflow = true;
            } else {
                state
                    .completions
                    .push(VectorMaintenanceCompletionRecord(Arc::new(completion)));
            }
        } else {
            state.overflow = true;
        }
    }
}

struct BoundedMessage(String);

impl fmt::Write for BoundedMessage {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if text.len() > MAX_MESSAGE_BYTES - self.0.len() {
            return Err(fmt::Error);
        }
        self.0.push_str(text);
        Ok(())
    }
}

fn source_kind(error: &(dyn Error + 'static)) -> VectorMaintenanceObservedSourceKind {
    use VectorMaintenanceObservedSourceKind as Kind;
    if let Some(error) = error.downcast_ref::<TableCatalogStoreError>() {
        return match error {
            TableCatalogStoreError::NotFound(_)
            | TableCatalogStoreError::NamespaceNotFound(_)
            | TableCatalogStoreError::TableNotFound(_) => Kind::CatalogNotFound,
            TableCatalogStoreError::AlreadyExists(_) => Kind::CatalogAlreadyExists,
            TableCatalogStoreError::Conflict(_) => Kind::CatalogConflict,
            TableCatalogStoreError::Invalid(_) => Kind::CatalogInvalid,
            TableCatalogStoreError::Unsupported(_) => Kind::CatalogUnsupported,
            TableCatalogStoreError::Unavailable(_) => Kind::CatalogUnavailable,
            TableCatalogStoreError::LockUnavailable { .. } => Kind::CatalogLockUnavailable,
            TableCatalogStoreError::Internal(_) => Kind::CatalogInternal,
            TableCatalogStoreError::Io { classification, .. } => match classification {
                CatalogIoErrorKind::Denied => Kind::CatalogIoDenied,
                CatalogIoErrorKind::Conflict => Kind::CatalogIoConflict,
                CatalogIoErrorKind::Unavailable => Kind::CatalogIoUnavailable,
                CatalogIoErrorKind::Internal => Kind::CatalogIoInternal,
            },
        };
    }
    if error.is::<std::io::Error>() { Kind::Io } else { Kind::Other }
}

/// Owns and polls the original JoinHandle. Observation never replaces its result.
pub(super) struct VectorMaintenanceObservedTask {
    handle: JoinHandle<WorkerResult>,
    observation: Option<VectorMaintenanceTaskObservation>,
}

impl VectorMaintenanceObservedTask {
    pub(super) fn new(handle: JoinHandle<WorkerResult>, observation: Option<VectorMaintenanceTaskObservation>) -> Self {
        Self { handle, observation }
    }
}

impl From<JoinHandle<WorkerResult>> for VectorMaintenanceObservedTask {
    fn from(handle: JoinHandle<WorkerResult>) -> Self {
        Self::new(handle, None)
    }
}

impl Future for VectorMaintenanceObservedTask {
    type Output = Result<WorkerResult, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = match Pin::new(&mut this.handle).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        if let Some(observation) = this.observation.take() {
            observation.complete(&result);
        }
        Poll::Ready(result)
    }
}

const MAX_GC_ACQUISITIONS: usize = 32;
const MAX_GC_BINDING_BYTES: usize = 32 * 1024;
const MAX_GC_RECORD_BYTES: usize = 64 * 1024;

/// Owner-derived context of the original retirement authorization acquisition.
#[derive(Debug, Clone, Serialize)]
pub(in crate::table_catalog) struct VectorMaintenanceGcLockBinding {
    pub(in crate::table_catalog) resource: VectorMaintenanceResourceV1,
    pub(in crate::table_catalog) job_id: Uuid,
    pub(in crate::table_catalog) job_revision: Uuid,
    pub(in crate::table_catalog) job_object: CatalogMetadataRefRecordV1,
    pub(in crate::table_catalog) kind: VectorMaintenanceKind,
    pub(in crate::table_catalog) lease: VectorMaintenanceLeaseToken,
    pub(in crate::table_catalog) plan_ref: CatalogMetadataRefRecordV1,
    pub(in crate::table_catalog) evidence_ref: CatalogMetadataRefRecordV1,
    pub(in crate::table_catalog) targets: Vec<VectorRetirementTargetV1>,
    pub(in crate::table_catalog) lock_resource: ObjectKey,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum VectorMaintenanceGcLockTerminal {
    ReadyOk,
    ReadyErr,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct VectorMaintenanceGcLockMoment {
    pub(crate) sequence: u64,
    pub(crate) elapsed_micros: u128,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum VectorMaintenanceGcLockErrorKind {
    Timeout,
    ResourceNotFound,
    PermissionDenied,
    Network,
    Internal,
    AlreadyLocked,
    InvalidHandle,
    Configuration,
    Serialization,
    Deserialization,
    InsufficientNodes,
    QuorumNotReached,
    QueueFull,
    NotOwner,
}

/// A bounded projection; the original LockError continues through the native result.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceGcLockFailure {
    pub(crate) kind: VectorMaintenanceGcLockErrorKind,
    pub(crate) message: String,
    pub(crate) sources: Vec<VectorMaintenanceObservedSource>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceGcLockRecord {
    pub(crate) acquisition_id: u64,
    pub(in crate::table_catalog) binding: VectorMaintenanceGcLockBinding,
    pub(crate) native_resource: ObjectKey,
    pub(crate) native_owner: String,
    pub(crate) acquire_timeout: Duration,
    pub(crate) poll_count: u64,
    pub(crate) first_poll: Option<VectorMaintenanceGcLockMoment>,
    pub(crate) first_pending: Option<VectorMaintenanceGcLockMoment>,
    pub(crate) terminal: Option<VectorMaintenanceGcLockTerminal>,
    pub(crate) terminal_moment: Option<VectorMaintenanceGcLockMoment>,
    pub(crate) error: Option<VectorMaintenanceGcLockFailure>,
    pub(crate) cancelled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VectorMaintenanceGcLockSnapshot {
    pub(crate) instance_id: Option<Uuid>,
    pub(crate) next_sequence: u64,
    pub(crate) overflow: bool,
    pub(crate) context_error: bool,
    pub(crate) acquisitions: Vec<VectorMaintenanceGcLockRecord>,
}

struct GcObserverState {
    instance_id: Option<Uuid>,
    started: Instant,
    next_sequence: u64,
    overflow: bool,
    context_error: bool,
    acquisitions: Vec<VectorMaintenanceGcLockRecord>,
}

impl Default for GcObserverState {
    fn default() -> Self {
        Self {
            instance_id: None,
            started: Instant::now(),
            next_sequence: 1,
            overflow: false,
            context_error: false,
            acquisitions: Vec::with_capacity(MAX_GC_ACQUISITIONS),
        }
    }
}

impl GcObserverState {
    fn moment(&mut self) -> VectorMaintenanceGcLockMoment {
        let sequence = self.next_sequence;
        if let Some(next) = sequence.checked_add(1) {
            self.next_sequence = next;
        } else {
            self.overflow = true;
        }
        VectorMaintenanceGcLockMoment {
            sequence,
            elapsed_micros: self.started.elapsed().as_micros(),
        }
    }
}

impl VectorMaintenanceCompletionObserver {
    /// The App owner supplies its actual store instance, never a worker request field.
    pub(crate) fn new_for_instance(classify_source: SourceClassifier, instance_id: Uuid) -> Arc<Self> {
        let observer = Self::new(classify_source);
        {
            let mut state = observer.state.lock();
            state.gc.instance_id = Some(instance_id);
            state.gc.context_error = instance_id.is_nil();
        }
        observer
    }

    /// Reads only retained task evidence; no Catalog or storage lock is acquired.
    pub(crate) fn gc_lock_snapshot(&self) -> VectorMaintenanceGcLockSnapshot {
        let state = self.state.lock();
        VectorMaintenanceGcLockSnapshot {
            instance_id: state.gc.instance_id,
            next_sequence: state.gc.next_sequence,
            overflow: state.overflow || state.gc.overflow,
            context_error: state.gc.context_error,
            acquisitions: state.gc.acquisitions.clone(),
        }
    }
}

struct GcAcquisitionScope {
    task: VectorMaintenanceTaskObservation,
    binding: VectorMaintenanceGcLockBinding,
    used: AtomicBool,
}

tokio::task_local! {
    static GC_ACQUISITION: Option<GcAcquisitionScope>;
}

/// Binding establishes context only; it does not report a lock request or Pending.
pub(in crate::table_catalog) async fn with_gc_acquisition<F: Future>(
    binding: VectorMaintenanceGcLockBinding,
    future: F,
) -> F::Output {
    let task = TASK_OBSERVATION.try_with(Clone::clone).ok();
    let scope = task.and_then(|task| {
        let matches = {
            let data = task.data.lock();
            data.as_ref().is_some_and(|data| {
                data.resource == binding.resource
                    && data.requested_job_id == Some(binding.job_id)
                    && data.job_id == Some(binding.job_id)
                    && data.attempt == Some(binding.lease.attempt())
                    && data.kind == Some(binding.kind)
                    && matches!(data.operation, VectorMaintenanceObservedOperation::Job)
                    && matches!(binding.kind, VectorMaintenanceKind::HistoryGc | VectorMaintenanceKind::OrphanGc)
                    && data.claimed_lease.as_ref() == Some(&binding.lease)
            })
        };
        if !matches {
            task.observer.state.lock().gc.context_error = true;
            return None;
        }
        if !serialized_within(&binding, MAX_GC_BINDING_BYTES) {
            task.observer.state.lock().gc.overflow = true;
            return None;
        }
        Some(GcAcquisitionScope {
            task,
            binding,
            used: AtomicBool::new(false),
        })
    });
    GC_ACQUISITION.scope(scope, future).await
}

struct GcNativeObservation {
    observer: Arc<VectorMaintenanceCompletionObserver>,
    index: usize,
    terminal: bool,
}

impl GcNativeObservation {
    fn begin(resource: &ObjectKey, owner: &str, timeout: Duration) -> Option<Self> {
        GC_ACQUISITION
            .try_with(|scope| {
                let scope = scope.as_ref()?;
                let observer = &scope.task.observer;
                if resource != &scope.binding.lock_resource || scope.used.swap(true, Ordering::Relaxed) {
                    observer.state.lock().gc.context_error = true;
                    return None;
                }
                let mut state = observer.state.lock();
                if state.gc.instance_id.is_none_or(|id| id.is_nil()) {
                    state.gc.context_error = true;
                    return None;
                }
                if state.gc.acquisitions.len() == MAX_GC_ACQUISITIONS || owner.len() > 1024 {
                    state.gc.overflow = true;
                    return None;
                }
                let index = state.gc.acquisitions.len();
                let record = VectorMaintenanceGcLockRecord {
                    acquisition_id: u64::try_from(index).expect("bounded acquisition index") + 1,
                    binding: scope.binding.clone(),
                    native_resource: resource.clone(),
                    native_owner: owner.to_owned(),
                    acquire_timeout: timeout,
                    poll_count: 0,
                    first_poll: None,
                    first_pending: None,
                    terminal: None,
                    terminal_moment: None,
                    error: None,
                    cancelled: false,
                };
                // Reserve space for bounded sequence/counter fields and terminal metadata.
                if !serialized_within(&record, MAX_GC_RECORD_BYTES - 4096) {
                    state.gc.overflow = true;
                    return None;
                }
                state.gc.acquisitions.push(record);
                Some(Self {
                    observer: observer.clone(),
                    index,
                    terminal: false,
                })
            })
            .ok()
            .flatten()
    }

    fn polled<T>(&mut self, result: &Poll<Result<T, LockError>>) {
        let (error, error_overflow) = match result {
            Poll::Ready(Err(error)) => gc_failure(error),
            _ => (None, false),
        };
        let mut state = self.observer.state.lock();
        state.gc.overflow |= error_overflow;
        let gc = &mut state.gc;
        if let Some(count) = gc.acquisitions[self.index].poll_count.checked_add(1) {
            gc.acquisitions[self.index].poll_count = count;
        } else {
            gc.overflow = true;
        }
        if gc.acquisitions[self.index].first_poll.is_none() {
            let moment = gc.moment();
            gc.acquisitions[self.index].first_poll = Some(moment);
        }
        match result {
            Poll::Pending => {
                if gc.acquisitions[self.index].first_pending.is_none() {
                    let moment = gc.moment();
                    gc.acquisitions[self.index].first_pending = Some(moment);
                }
            }
            Poll::Ready(result) => {
                let moment = gc.moment();
                let record = &mut gc.acquisitions[self.index];
                record.terminal_moment = Some(moment);
                record.terminal = Some(if result.is_ok() {
                    VectorMaintenanceGcLockTerminal::ReadyOk
                } else {
                    VectorMaintenanceGcLockTerminal::ReadyErr
                });
                record.error = error;
                if !serialized_within(record, MAX_GC_RECORD_BYTES) {
                    record.error = None;
                    gc.overflow = true;
                }
                self.terminal = true;
            }
        }
    }
}

impl Drop for GcNativeObservation {
    fn drop(&mut self) {
        if !self.terminal {
            let mut state = self.observer.state.lock();
            let moment = state.gc.moment();
            let record = &mut state.gc.acquisitions[self.index];
            record.terminal = Some(VectorMaintenanceGcLockTerminal::Cancelled);
            record.terminal_moment = Some(moment);
            record.cancelled = true;
        }
    }
}

/// Polls the original native future with the caller's unchanged Context.
pub(in crate::table_catalog) async fn observe_native_gc_write_lock<F, T>(
    resource: &ObjectKey,
    owner: &str,
    timeout: Duration,
    future: F,
) -> Result<T, LockError>
where
    F: Future<Output = Result<T, LockError>>,
{
    let mut observation = GcNativeObservation::begin(resource, owner, timeout);
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        // No observer mutex is held while the original acquisition is polled.
        let result = future.as_mut().poll(cx);
        if let Some(observation) = observation.as_mut() {
            observation.polled(&result);
        }
        result
    })
    .await
}

fn gc_failure(error: &LockError) -> (Option<VectorMaintenanceGcLockFailure>, bool) {
    use VectorMaintenanceGcLockErrorKind as Kind;
    let kind = match error {
        LockError::Timeout { .. } => Kind::Timeout,
        LockError::ResourceNotFound { .. } => Kind::ResourceNotFound,
        LockError::PermissionDenied { .. } => Kind::PermissionDenied,
        LockError::Network { .. } => Kind::Network,
        LockError::Internal { .. } => Kind::Internal,
        LockError::AlreadyLocked { .. } => Kind::AlreadyLocked,
        LockError::InvalidHandle { .. } => Kind::InvalidHandle,
        LockError::Configuration { .. } => Kind::Configuration,
        LockError::Serialization { .. } => Kind::Serialization,
        LockError::Deserialization { .. } => Kind::Deserialization,
        LockError::InsufficientNodes { .. } => Kind::InsufficientNodes,
        LockError::QuorumNotReached { .. } => Kind::QuorumNotReached,
        LockError::QueueFull { .. } => Kind::QueueFull,
        LockError::NotOwner { .. } => Kind::NotOwner,
    };
    let mut overflow = false;
    let mut message = |error: &dyn Error| {
        let mut text = BoundedMessage(String::with_capacity(MAX_MESSAGE_BYTES));
        overflow |= write!(&mut text, "{error}").is_err();
        text.0
    };
    let original_message = message(error);
    let mut sources = Vec::with_capacity(MAX_SOURCES);
    let mut source = error.source();
    while let Some(current) = source {
        if sources.len() == MAX_SOURCES {
            // End the closure borrow before marking capacity failure below.
            break;
        }
        sources.push(VectorMaintenanceObservedSource {
            kind: source_kind(current),
            message: message(current),
            io_raw_os_error: current
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::raw_os_error),
        });
        source = current.source();
    }
    overflow |= source.is_some();
    (
        Some(VectorMaintenanceGcLockFailure {
            kind,
            message: original_message,
            sources,
        }),
        overflow,
    )
}

pub(in crate::table_catalog) fn serialized_within(value: &impl Serialize, limit: usize) -> bool {
    struct LimitWriter {
        remaining: usize,
    }
    impl std::io::Write for LimitWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.remaining {
                return Err(std::io::Error::other("GC observation byte capacity exceeded"));
            }
            self.remaining -= bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(LimitWriter { remaining: limit }, value).is_ok()
}

#[path = "fault_observation_gc_tests.rs"]
mod gc_tests;
