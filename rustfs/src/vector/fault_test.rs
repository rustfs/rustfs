// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! In-process observations of real guarded Vector operations, compiled only for tests.

use std::{
    future::Future,
    num::NonZeroU32,
    sync::{Arc, OnceLock, Weak},
    time::Duration,
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::{sync::Notify, time::Instant};
use uuid::Uuid;

use crate::table_catalog::{
    CatalogExpectedHead, CatalogFormat, CatalogFormatDescriptorRecordV1, CatalogMetadataRef, CatalogMetadataRefRecordV1,
    CatalogResourceIdentity, PreparedCatalogCommit, TableCatalogStoreError, VectorBucketBindingV1, VectorMaintenanceHeadV1,
};

const MAX_GATES: usize = 32;
const MAX_EVENTS: usize = 64;
const MAX_KEYS: usize = 500;
const MAX_FRAGMENTS: usize = 4096;
const MAX_CANDIDATE_REFERENCES: usize = 4096;
const MAX_OBSERVATION_BYTES: usize = 1_048_576;
const MAX_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_SDK_ACCOUNT_BYTES: usize = 256;
const MAX_ERROR_SOURCE_DEPTH: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum FaultPoint {
    PutBaseOpened,
    AnnInputOpened,
    CompactionReserved,
    QuerySnapshotOpened,
    /// Fail one selected SDK publication after its candidate and references are verified.
    SdkCandidatePrepared,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FaultError {
    #[error("fault selector requires a nonnil instance and a Vector resource")]
    InvalidIdentity,
    #[error("fault timeout must be positive and at most 300 seconds")]
    InvalidTimeout,
    #[error("fault registry is full or the exact selector is already armed")]
    Unavailable,
    #[error("fault evidence exceeded its bound")]
    Overflow,
    #[error("injected SDK candidate failure before Catalog publication: {observation_token}")]
    InjectedBeforeCatalog { observation_token: Uuid },
    #[error(transparent)]
    Catalog(#[from] TableCatalogStoreError),
}

pub(crate) struct FaultSelector {
    instance_id: Uuid,
    resource: CatalogResourceIdentity,
    point: FaultPoint,
    occurrence: NonZeroU32,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FaultResource {
    pub bucket: VectorBucketBindingV1,
    pub table_id: String,
    pub format: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExpectedHeadObservation {
    pub version_token: String,
    pub generation: u64,
    pub metadata: Option<CatalogMetadataRefRecordV1>,
    pub descriptor: Option<CatalogFormatDescriptorRecordV1>,
}

impl From<&CatalogExpectedHead> for ExpectedHeadObservation {
    fn from(head: &CatalogExpectedHead) -> Self {
        Self {
            version_token: head.version_token().as_str().to_owned(),
            generation: head.generation().get(),
            metadata: head.metadata().map(Into::into),
            descriptor: head.format_descriptor().map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MaintenanceObservation {
    pub job_id: Uuid,
    pub attempt: u16,
    pub stage: &'static str,
    pub stage_commit_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SdkCandidateObservation {
    pub observation_token: Uuid,
    pub commit_id: String,
    pub request_digest_sha256: String,
    pub operation: String,
    pub metadata: CatalogMetadataRefRecordV1,
    pub descriptor: CatalogFormatDescriptorRecordV1,
    /// Exact candidate closure, including original-base dependencies needed by its transaction.
    pub references: Vec<CatalogMetadataRefRecordV1>,
}

/// Borrowed facts from the actual admitted App request, before protocol error conversion.
pub(crate) struct SdkRequestFailureContext<'a> {
    pub instance_id: Option<Uuid>,
    pub resource: Option<&'a CatalogResourceIdentity>,
    pub request_id: Uuid,
    pub http_request_id: Option<&'a str>,
    pub account: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum SdkRequestFailureSource {
    InjectedBeforeCatalog,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SdkRequestFailureObservation {
    pub observation_token: Uuid,
    pub request_id: Uuid,
    pub http_request_id: String,
    pub account: String,
    pub commit_id: String,
    pub request_digest_sha256: String,
    pub source: SdkRequestFailureSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum SdkRequestCorrelationFailure {
    InvalidInstance,
    MissingResource,
    IdentityMismatch,
    InvalidRequestId,
    InvalidHttpRequestId,
    InvalidAccount,
    Duplicate,
    UnavailableCandidate,
    UnissuedToken,
    CapacityExceeded,
    SourceChainLimit,
}

/// Only a handle that observed an actual injected call can issue this read-only receipt.
#[derive(Debug, Clone)]
pub(crate) struct SdkCandidateReceipt {
    instance_id: Uuid,
    resource: CatalogResourceIdentity,
    expected_head: ExpectedHeadObservation,
    manifest_version: u64,
    observation: SdkCandidateObservation,
}

impl SdkCandidateReceipt {
    pub(crate) fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    pub(crate) fn resource(&self) -> &CatalogResourceIdentity {
        &self.resource
    }

    pub(crate) fn observation(&self) -> &SdkCandidateObservation {
        &self.observation
    }

    pub(crate) fn expected_head(&self) -> &ExpectedHeadObservation {
        &self.expected_head
    }

    pub(crate) fn manifest_version(&self) -> u64 {
        self.manifest_version
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FaultObservation {
    pub expected_head: ExpectedHeadObservation,
    pub manifest_version: u64,
    pub selected_keys: Vec<String>,
    pub present_keys: Vec<String>,
    pub fragment_ids: Vec<u64>,
    pub eligible_rows: Option<u64>,
    pub maintenance: Option<MaintenanceObservation>,
    pub published_head: Option<ExpectedHeadObservation>,
    pub reservation_record: Option<VectorMaintenanceHeadV1>,
    pub sdk_candidate: Option<SdkCandidateObservation>,
    pub sdk_request_failure: Option<SdkRequestFailureObservation>,
}

impl FaultObservation {
    pub(crate) fn new(head: &CatalogExpectedHead, manifest_version: u64) -> Self {
        Self {
            expected_head: head.into(),
            manifest_version,
            selected_keys: Vec::new(),
            present_keys: Vec::new(),
            fragment_ids: Vec::new(),
            eligible_rows: None,
            maintenance: None,
            published_head: None,
            reservation_record: None,
            sdk_candidate: None,
            sdk_request_failure: None,
        }
    }

    fn within_bounds(&self) -> bool {
        self.selected_keys.len() <= MAX_KEYS
            && self.present_keys.len() <= MAX_KEYS
            && self.fragment_ids.len() <= MAX_FRAGMENTS
            && self
                .sdk_candidate
                .as_ref()
                .is_none_or(|candidate| candidate.references.len() <= MAX_CANDIDATE_REFERENCES)
            && serde_json::to_vec(self).is_ok_and(|bytes| bytes.len() <= MAX_OBSERVATION_BYTES)
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum FaultEventKind {
    Hit,
    Arrived,
    ReleaseRequested,
    BoundaryCompleted,
    Cancelled,
    ObservationFailed,
    TimedOut,
    HandleDropped,
    Overflow,
    FailureInjected,
    SdkRequestFailureObserved,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FaultEvent {
    pub sequence: usize,
    pub elapsed_ms: u64,
    pub kind: FaultEventKind,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FaultSnapshot {
    pub instance_id: Uuid,
    pub resource: FaultResource,
    pub point: FaultPoint,
    pub occurrence: u32,
    pub hits: u64,
    pub arrivals: u32,
    /// Counts resumed observation boundaries; operation results remain owned by their callers.
    pub completions: u32,
    pub released: bool,
    pub timed_out: bool,
    pub cancelled: bool,
    pub observation_failed: bool,
    pub overflow: bool,
    pub handle_dropped: bool,
    pub injected_before_catalog: bool,
    pub sdk_request_failure_error: Option<SdkRequestCorrelationFailure>,
    pub events: Vec<FaultEvent>,
    pub observation: Option<FaultObservation>,
}

struct Gate {
    selector: FaultSelector,
    armed_at: Instant,
    deadline: Instant,
    state: Mutex<FaultSnapshot>,
    changed: Notify,
}

impl Gate {
    fn event(&self, state: &mut FaultSnapshot, kind: FaultEventKind) {
        if state.events.len() == MAX_EVENTS {
            state.overflow = true;
            return;
        }
        state.events.push(FaultEvent {
            sequence: state.events.len() + 1,
            elapsed_ms: u64::try_from(self.armed_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            kind,
        });
    }

    fn expire(&self, state: &mut FaultSnapshot) {
        if !state.released
            && !state.timed_out
            && !state.handle_dropped
            && !state.injected_before_catalog
            && Instant::now() >= self.deadline
        {
            state.timed_out = true;
            self.event(state, FaultEventKind::TimedOut);
            self.changed.notify_waiters();
        }
    }

    fn retained_for_registry(&self) -> bool {
        let mut state = self.state.lock();
        self.expire(&mut state);
        // The original handle owns an injected candidate until App associates its returned error.
        // Keeping its weak registry entry also detects duplicate callbacks without a second owner.
        if state.injected_before_catalog {
            return !state.handle_dropped;
        }
        !state.released && !state.timed_out && !state.handle_dropped && !state.overflow
    }
}

fn registry() -> &'static Mutex<Vec<Weak<Gate>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<Gate>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

fn sdk_source_token(mut error: &(dyn std::error::Error + 'static)) -> Result<Option<Uuid>, SdkRequestCorrelationFailure> {
    for _ in 0..MAX_ERROR_SOURCE_DEPTH {
        if let Some(FaultError::InjectedBeforeCatalog { observation_token }) = error.downcast_ref::<FaultError>() {
            return Ok(Some(*observation_token));
        }
        let Some(source) = error.source() else {
            return Ok(None);
        };
        error = source;
    }
    Err(SdkRequestCorrelationFailure::SourceChainLimit)
}

fn fail_sdk_request_correlation(gate: &Gate, state: &mut FaultSnapshot, failure: SdkRequestCorrelationFailure) {
    state.observation_failed = true;
    state.sdk_request_failure_error.get_or_insert(failure);
    if failure == SdkRequestCorrelationFailure::CapacityExceeded {
        state.overflow = true;
    }
    gate.event(state, FaultEventKind::ObservationFailed);
    gate.changed.notify_waiters();
}

fn associate_sdk_request(
    gate: &Gate,
    state: &mut FaultSnapshot,
    context: &SdkRequestFailureContext<'_>,
    token: Uuid,
) -> Result<(), SdkRequestCorrelationFailure> {
    use SdkRequestCorrelationFailure as Failure;
    let observation = state.observation.as_ref().ok_or(Failure::UnavailableCandidate)?;
    if observation.sdk_request_failure.is_some() {
        return Err(Failure::Duplicate);
    }
    if !state.injected_before_catalog
        || state.released
        || state.handle_dropped
        || state.cancelled
        || state.timed_out
        || state.observation_failed
        || state.overflow
    {
        return Err(Failure::UnavailableCandidate);
    }
    let instance_id = context
        .instance_id
        .filter(|instance| !instance.is_nil())
        .ok_or(Failure::InvalidInstance)?;
    let resource = context.resource.ok_or(Failure::MissingResource)?;
    if instance_id != gate.selector.instance_id || resource != &gate.selector.resource {
        return Err(Failure::IdentityMismatch);
    }
    if context.request_id.is_nil() {
        return Err(Failure::InvalidRequestId);
    }
    let http_request_id = context
        .http_request_id
        .filter(|id| {
            !id.is_empty()
                && id.len() <= rustfs_gateway::RequestId::MAX_LEN
                && id.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .ok_or(Failure::InvalidHttpRequestId)?;
    let account = context
        .account
        .filter(|account| {
            !account.is_empty()
                && account.len() <= MAX_SDK_ACCOUNT_BYTES
                && account.trim() == *account
                && !account.chars().any(char::is_control)
        })
        .ok_or(Failure::InvalidAccount)?;
    let candidate = observation.sdk_candidate.as_ref().ok_or(Failure::UnavailableCandidate)?;
    if candidate.observation_token != token {
        return Err(Failure::UnissuedToken);
    }
    if state.events.len() >= MAX_EVENTS {
        return Err(Failure::CapacityExceeded);
    }
    let association = SdkRequestFailureObservation {
        observation_token: token,
        request_id: context.request_id,
        http_request_id: http_request_id.to_owned(),
        account: account.to_owned(),
        commit_id: candidate.commit_id.clone(),
        request_digest_sha256: candidate.request_digest_sha256.clone(),
        source: SdkRequestFailureSource::InjectedBeforeCatalog,
    };
    let observation = state.observation.as_mut().ok_or(Failure::UnavailableCandidate)?;
    observation.sdk_request_failure = Some(association);
    if !observation.within_bounds() {
        observation.sdk_request_failure = None;
        return Err(Failure::CapacityExceeded);
    }
    gate.event(state, FaultEventKind::SdkRequestFailureObserved);
    gate.changed.notify_waiters();
    Ok(())
}

/// Associate only an original typed injected error; leave the original request result untouched.
/// Failure is retained by the existing gate and never returned as a replacement HTTP result.
pub(crate) fn observe_sdk_request_failure(context: SdkRequestFailureContext<'_>, error: &(dyn std::error::Error + 'static)) {
    let token = match sdk_source_token(error) {
        Ok(None) => return,
        result => result,
    };
    let gates = registry().lock();
    if let Ok(Some(token)) = token {
        for gate in gates.iter().filter_map(Weak::upgrade) {
            if gate.selector.point != FaultPoint::SdkCandidatePrepared {
                continue;
            }
            let mut state = gate.state.lock();
            let owns_token = state
                .observation
                .as_ref()
                .and_then(|observation| observation.sdk_candidate.as_ref())
                .is_some_and(|candidate| candidate.observation_token == token);
            if owns_token {
                if let Err(failure) = associate_sdk_request(&gate, &mut state, &context, token) {
                    fail_sdk_request_correlation(&gate, &mut state, failure);
                }
                return;
            }
        }
    }
    // A recognized injection without a resolvable token can invalidate only its exact live context owner.
    // It cannot create evidence or affect a different instance/resource selector.
    let failure = token.err().unwrap_or(SdkRequestCorrelationFailure::UnissuedToken);
    for gate in gates.iter().filter_map(Weak::upgrade) {
        if gate.selector.point != FaultPoint::SdkCandidatePrepared
            || Some(gate.selector.instance_id) != context.instance_id
            || Some(&gate.selector.resource) != context.resource
        {
            continue;
        }
        let mut state = gate.state.lock();
        if state.injected_before_catalog && !state.handle_dropped {
            fail_sdk_request_correlation(&gate, &mut state, failure);
        }
    }
}

/// Arm one exact instance/resource/phase. Dropping the handle releases its waiters.
/// The deadline includes time waiting for arrival and never changes production maintenance policy.
///
/// # Errors
/// Rejects invalid identities, excessive deadlines, full registries and duplicate selectors.
pub(crate) fn arm(
    instance_id: Uuid,
    resource: CatalogResourceIdentity,
    point: FaultPoint,
    occurrence: NonZeroU32,
    timeout: Duration,
) -> Result<FaultHandle, FaultError> {
    if instance_id.is_nil() || resource.format() != CatalogFormat::Lance {
        return Err(FaultError::InvalidIdentity);
    }
    let bucket = resource.vector_bucket().ok_or(FaultError::InvalidIdentity)?;
    let evidence = FaultResource {
        bucket: VectorBucketBindingV1::from_handle(bucket)?,
        table_id: resource.table_id().as_str().to_owned(),
        format: "lance",
    };
    if timeout.is_zero() || timeout > MAX_TIMEOUT {
        return Err(FaultError::InvalidTimeout);
    }
    let mut gates = registry().lock();
    gates.retain(|gate| gate.upgrade().is_some_and(|gate| gate.retained_for_registry()));
    if gates.len() >= MAX_GATES
        || gates.iter().filter_map(Weak::upgrade).any(|gate| {
            gate.selector.instance_id == instance_id && gate.selector.resource == resource && gate.selector.point == point
        })
    {
        return Err(FaultError::Unavailable);
    }
    let armed_at = Instant::now();
    let gate = Arc::new(Gate {
        selector: FaultSelector {
            instance_id,
            resource,
            point,
            occurrence,
        },
        armed_at,
        deadline: armed_at + timeout,
        state: Mutex::new(FaultSnapshot {
            instance_id,
            resource: evidence,
            point,
            occurrence: occurrence.get(),
            hits: 0,
            arrivals: 0,
            completions: 0,
            released: false,
            timed_out: false,
            cancelled: false,
            observation_failed: false,
            overflow: false,
            handle_dropped: false,
            injected_before_catalog: false,
            sdk_request_failure_error: None,
            events: Vec::new(),
            observation: None,
        }),
        changed: Notify::new(),
    });
    gates.push(Arc::downgrade(&gate));
    Ok(FaultHandle { gate })
}

pub(crate) struct FaultHandle {
    gate: Arc<Gate>,
}

impl FaultHandle {
    /// Resolve an opaque token issued by this actual fault, without accepting caller-supplied references.
    pub(crate) fn observed_sdk_candidate(&self, observation_token: Uuid) -> Result<SdkCandidateReceipt, FaultError> {
        let state = self.gate.state.lock();
        let observation = state.observation.as_ref().ok_or(FaultError::InvalidIdentity)?;
        let candidate = observation
            .sdk_candidate
            .as_ref()
            .filter(|candidate| {
                state.injected_before_catalog
                    && !state.overflow
                    && !state.observation_failed
                    && !state.cancelled
                    && !state.timed_out
                    && candidate.observation_token == observation_token
            })
            .ok_or(FaultError::InvalidIdentity)?;
        Ok(SdkCandidateReceipt {
            instance_id: self.gate.selector.instance_id,
            resource: self.gate.selector.resource.clone(),
            expected_head: observation.expected_head.clone(),
            manifest_version: observation.manifest_version,
            observation: candidate.clone(),
        })
    }

    pub(crate) fn snapshot(&self) -> FaultSnapshot {
        let mut state = self.gate.state.lock();
        self.gate.expire(&mut state);
        state.clone()
    }

    pub(crate) fn release(&self) -> FaultSnapshot {
        let mut state = self.gate.state.lock();
        self.gate.expire(&mut state);
        if !state.released {
            state.released = true;
            self.gate.event(&mut state, FaultEventKind::ReleaseRequested);
        }
        self.gate.changed.notify_waiters();
        state.clone()
    }
}

impl Drop for FaultHandle {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock();
        state.handle_dropped = true;
        self.gate.event(&mut state, FaultEventKind::HandleDropped);
        self.gate.changed.notify_waiters();
    }
}

/// A selected real call owns this value; cancellation drops it in that same future.
pub(crate) struct FaultProbe {
    gate: Arc<Gate>,
    completed: bool,
}

pub(crate) fn hit(instance_id: Option<Uuid>, resource: &CatalogResourceIdentity, point: FaultPoint) -> Option<FaultProbe> {
    let instance_id = instance_id.filter(|id| !id.is_nil())?;
    let gates = registry().lock();
    for gate in gates.iter().filter_map(Weak::upgrade) {
        if gate.selector.instance_id != instance_id || gate.selector.resource != *resource || gate.selector.point != point {
            continue;
        }
        let mut state = gate.state.lock();
        gate.expire(&mut state);
        if state.released || state.timed_out || state.handle_dropped || state.overflow || state.injected_before_catalog {
            continue;
        }
        state.hits = state.hits.saturating_add(1);
        gate.event(&mut state, FaultEventKind::Hit);
        gate.changed.notify_waiters();
        if state.hits == u64::from(gate.selector.occurrence.get()) {
            drop(state);
            return Some(FaultProbe { gate, completed: false });
        }
    }
    None
}

impl FaultProbe {
    /// Called only by the original SDK adapter after Managed upload and full reference verification.
    pub(in crate::vector) fn fail_sdk_candidate(
        mut self,
        prepared: &PreparedCatalogCommit,
        manifest_version: u64,
        references: Vec<CatalogMetadataRef>,
    ) -> Result<(), FaultError> {
        if self.gate.selector.point != FaultPoint::SdkCandidatePrepared
            || prepared.resource_identity() != &self.gate.selector.resource
            || !matches!(prepared.operation(), "put-vectors" | "delete-vectors")
            || !references.contains(prepared.candidate_metadata())
        {
            self.observation_failed();
            return Err(FaultError::InvalidIdentity);
        }
        let observation_token = Uuid::new_v4();
        let mut observation = FaultObservation::new(prepared.expected_head(), manifest_version);
        observation.sdk_candidate = Some(SdkCandidateObservation {
            observation_token,
            commit_id: prepared.commit_id().as_str().to_owned(),
            request_digest_sha256: prepared.request_digest().to_hex(),
            operation: prepared.operation().to_owned(),
            metadata: prepared.candidate_metadata().into(),
            descriptor: prepared.format_descriptor().into(),
            references: references.iter().map(Into::into).collect(),
        });
        let mut state = self.gate.state.lock();
        self.gate.expire(&mut state);
        if state.released || state.timed_out || state.handle_dropped {
            self.completed = true;
            return Ok(());
        }
        if state.overflow
            || state.events.len().checked_add(3).is_none_or(|count| count > MAX_EVENTS)
            || !observation.within_bounds()
        {
            state.overflow = true;
            self.gate.event(&mut state, FaultEventKind::Overflow);
            self.completed = true;
            self.gate.changed.notify_waiters();
            return Err(FaultError::Overflow);
        }
        state.observation = Some(observation);
        state.arrivals += 1;
        self.gate.event(&mut state, FaultEventKind::Arrived);
        state.injected_before_catalog = true;
        self.gate.event(&mut state, FaultEventKind::FailureInjected);
        state.completions += 1;
        self.gate.event(&mut state, FaultEventKind::BoundaryCompleted);
        self.completed = true;
        self.gate.changed.notify_waiters();
        Err(FaultError::InjectedBeforeCatalog { observation_token })
    }

    pub(crate) fn observation_failed(mut self) {
        let mut state = self.gate.state.lock();
        state.observation_failed = true;
        self.gate.event(&mut state, FaultEventKind::ObservationFailed);
        self.completed = true;
        self.gate.changed.notify_waiters();
    }

    /// Retain the caller's actual owners while waiting; never replace its result or cancellation path.
    pub(crate) async fn pause(mut self, observation: FaultObservation) {
        {
            let mut state = self.gate.state.lock();
            if !observation.within_bounds() {
                state.overflow = true;
                self.gate.event(&mut state, FaultEventKind::Overflow);
                self.completed = true;
                self.gate.changed.notify_waiters();
                return;
            }
            state.observation = Some(observation);
            state.arrivals += 1;
            self.gate.event(&mut state, FaultEventKind::Arrived);
            self.gate.changed.notify_waiters();
        }
        loop {
            let notified = self.gate.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.gate.state.lock();
                self.gate.expire(&mut state);
                if state.released || state.timed_out || state.handle_dropped || state.overflow {
                    state.completions += 1;
                    self.gate.event(&mut state, FaultEventKind::BoundaryCompleted);
                    self.completed = true;
                    self.gate.changed.notify_waiters();
                    return;
                }
            }
            let _ = tokio::time::timeout_at(self.gate.deadline, notified).await;
        }
    }
}

impl Drop for FaultProbe {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = self.gate.state.lock();
            state.cancelled = true;
            self.gate.event(&mut state, FaultEventKind::Cancelled);
            self.gate.changed.notify_waiters();
        }
    }
}

#[derive(Clone)]
pub(crate) struct FaultContext {
    pub instance_id: Option<Uuid>,
    pub resource: CatalogResourceIdentity,
    pub observation: FaultObservation,
}

tokio::task_local! {
    static ANN_CONTEXT: FaultContext;
}

pub(crate) async fn with_ann_context<T>(context: FaultContext, future: impl Future<Output = T>) -> T {
    ANN_CONTEXT.scope(context, future).await
}

pub(crate) async fn ann_input_opened(fragment_ids: Vec<u64>, eligible_rows: u64) {
    let Ok(context) = ANN_CONTEXT.try_with(Clone::clone) else {
        return;
    };
    if let Some(probe) = hit(context.instance_id, &context.resource, FaultPoint::AnnInputOpened) {
        let mut observation = context.observation;
        observation.fragment_ids = fragment_ids;
        observation.eligible_rows = Some(eligible_rows);
        probe.pause(observation).await;
    }
}
