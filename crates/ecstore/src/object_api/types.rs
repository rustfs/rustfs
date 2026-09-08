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

use super::*;
use crate::storage_api_contracts::{
    list::VersionMarker,
    object::{
        HTTPPreconditions, ObjectLockRetentionOptions, ObjectPreconditionError, ObjectPreconditionPart, ObjectPreconditionState,
    },
};
use sha2::{Digest, Sha256};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use tokio::sync::{Mutex, Notify, OwnedRwLockReadGuard};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct NamespaceLockFence {
    signals: Arc<Vec<Arc<rustfs_lock::distributed_lock::LockLostSignal>>>,
    #[cfg(test)]
    forced_lost: Arc<Vec<Arc<std::sync::atomic::AtomicBool>>>,
}

impl Debug for NamespaceLockFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceLockFence")
            .field("signal_count", &self.signals.len())
            .finish()
    }
}

impl NamespaceLockFence {
    fn new() -> Self {
        Self {
            signals: Arc::default(),
            #[cfg(test)]
            forced_lost: Arc::new(vec![Arc::new(std::sync::atomic::AtomicBool::new(false))]),
        }
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        #[cfg(test)]
        if self
            .forced_lost
            .iter()
            .any(|lost| lost.load(std::sync::atomic::Ordering::Acquire))
        {
            return true;
        }
        self.signals.iter().any(|signal| signal.is_lost())
    }

    pub(crate) fn add_signal(&mut self, signal: Arc<rustfs_lock::distributed_lock::LockLostSignal>) {
        Arc::make_mut(&mut self.signals).push(signal);
    }

    fn extend(&mut self, other: &Self) {
        if !Arc::ptr_eq(&self.signals, &other.signals) {
            Arc::make_mut(&mut self.signals).extend(other.signals.iter().cloned());
        }
        #[cfg(test)]
        if !Arc::ptr_eq(&self.forced_lost, &other.forced_lost) {
            Arc::make_mut(&mut self.forced_lost).extend(other.forced_lost.iter().cloned());
        }
    }

    #[cfg(test)]
    pub(crate) fn lost_for_test() -> Self {
        let fence = Self::new();
        fence.forced_lost[0].store(true, std::sync::atomic::Ordering::Release);
        fence
    }

    #[cfg(test)]
    pub(crate) fn loss_handle_for_test() -> (Self, Arc<std::sync::atomic::AtomicBool>) {
        let fence = Self::new();
        (fence.clone(), Arc::clone(&fence.forced_lost[0]))
    }
}

#[cfg(test)]
static NAMESPACE_LOCK_SIGNAL_TEST_FENCES: std::sync::OnceLock<std::sync::Mutex<Vec<(usize, NamespaceLockFence)>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) struct NamespaceLockSignalTestFence {
    signal_key: usize,
}

#[cfg(test)]
impl NamespaceLockSignalTestFence {
    pub(crate) fn install_with_loss_handle(
        signal: &Arc<rustfs_lock::distributed_lock::LockLostSignal>,
        loss_handle: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        let fence = NamespaceLockFence {
            signals: Arc::default(),
            forced_lost: Arc::new(vec![loss_handle]),
        };
        let signal_key = Arc::as_ptr(signal) as usize;
        let mut fences = NAMESPACE_LOCK_SIGNAL_TEST_FENCES
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("namespace lock signal test fence should not be poisoned");
        assert!(
            !fences.iter().any(|(key, _)| *key == signal_key),
            "namespace lock signal test fence must be unique"
        );
        fences.push((signal_key, fence));
        Self { signal_key }
    }
}

#[cfg(test)]
impl Drop for NamespaceLockSignalTestFence {
    fn drop(&mut self) {
        let mut fences = NAMESPACE_LOCK_SIGNAL_TEST_FENCES
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("namespace lock signal test fence should not be poisoned");
        fences.retain(|(key, _)| *key != self.signal_key);
    }
}

#[cfg(test)]
pub(crate) fn namespace_lock_signal_test_fence_is_lost(signal: &Arc<rustfs_lock::distributed_lock::LockLostSignal>) -> bool {
    NAMESPACE_LOCK_SIGNAL_TEST_FENCES
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .expect("namespace lock signal test fence should not be poisoned")
        .iter()
        .find(|(key, _)| *key == Arc::as_ptr(signal) as usize)
        .is_some_and(|(_, fence)| fence.is_lock_lost())
}

#[derive(Debug)]
pub struct ObjectLockConfigSnapshot {
    store_id: Option<Uuid>,
    bucket: Option<String>,
    bucket_incarnation_id: Option<Uuid>,
    config_revision: Option<OffsetDateTime>,
    state: crate::bucket::metadata_sys::ObjectLockConfigState,
    lifecycle_fence: NamespaceLockFence,
    _lifecycle_guard: Option<rustfs_lock::NamespaceLockGuard>,
    metadata_transaction_guard: Option<rustfs_lock::NamespaceLockGuard>,
}

impl ObjectLockConfigSnapshot {
    pub(crate) fn new(state: crate::bucket::metadata_sys::ObjectLockConfigState) -> Self {
        Self {
            store_id: None,
            bucket: None,
            bucket_incarnation_id: None,
            config_revision: None,
            state,
            lifecycle_fence: NamespaceLockFence::new(),
            _lifecycle_guard: None,
            metadata_transaction_guard: None,
        }
    }

    pub(crate) fn for_store_bucket(
        store_id: Uuid,
        bucket: &str,
        bucket_incarnation_id: Uuid,
        config_revision: OffsetDateTime,
        state: crate::bucket::metadata_sys::ObjectLockConfigState,
    ) -> Self {
        Self {
            store_id: Some(store_id),
            bucket: Some(bucket.to_string()),
            bucket_incarnation_id: Some(bucket_incarnation_id),
            config_revision: Some(config_revision),
            state,
            lifecycle_fence: NamespaceLockFence::new(),
            _lifecycle_guard: None,
            metadata_transaction_guard: None,
        }
    }

    pub(crate) fn for_guarded_store_bucket(
        store_id: Uuid,
        bucket: &str,
        bucket_incarnation_id: Uuid,
        config_revision: OffsetDateTime,
        state: crate::bucket::metadata_sys::ObjectLockConfigState,
        lifecycle_guard: rustfs_lock::NamespaceLockGuard,
        metadata_transaction_guard: rustfs_lock::NamespaceLockGuard,
    ) -> Self {
        let mut lifecycle_fence = NamespaceLockFence::new();
        if let Some(signal) = lifecycle_guard.lock_lost_signal() {
            lifecycle_fence.add_signal(signal);
        }
        Self {
            store_id: Some(store_id),
            bucket: Some(bucket.to_string()),
            bucket_incarnation_id: Some(bucket_incarnation_id),
            config_revision: Some(config_revision),
            state,
            lifecycle_fence,
            _lifecycle_guard: Some(lifecycle_guard),
            metadata_transaction_guard: Some(metadata_transaction_guard),
        }
    }

    pub(crate) fn for_store_bucket_under_lifecycle_fence(
        store_id: Uuid,
        bucket: &str,
        bucket_incarnation_id: Uuid,
        config_revision: OffsetDateTime,
        state: crate::bucket::metadata_sys::ObjectLockConfigState,
        lifecycle_fence: NamespaceLockFence,
        metadata_transaction_guard: rustfs_lock::NamespaceLockGuard,
    ) -> Self {
        Self {
            store_id: Some(store_id),
            bucket: Some(bucket.to_string()),
            bucket_incarnation_id: Some(bucket_incarnation_id),
            config_revision: Some(config_revision),
            state,
            lifecycle_fence,
            _lifecycle_guard: None,
            metadata_transaction_guard: Some(metadata_transaction_guard),
        }
    }

    #[allow(dead_code, reason = "snapshot-scope predicate asserted by this file's tests (backlog#1823)")]
    pub(crate) fn is_for_store_bucket(
        &self,
        store_id: Uuid,
        bucket: &str,
        bucket_incarnation_id: Uuid,
        config_revision: OffsetDateTime,
    ) -> bool {
        self.store_id == Some(store_id)
            && self.bucket.as_deref() == Some(bucket)
            && self.bucket_incarnation_id == Some(bucket_incarnation_id)
            && self.config_revision == Some(config_revision)
    }

    pub fn state(&self) -> &crate::bucket::metadata_sys::ObjectLockConfigState {
        &self.state
    }

    pub(crate) fn is_valid_for_destructive_put(&self, store_id: Uuid, bucket: &str, bucket_incarnation_id: Uuid) -> bool {
        self.store_id == Some(store_id)
            && self.bucket.as_deref() == Some(bucket)
            && self.bucket_incarnation_id == Some(bucket_incarnation_id)
            && self.config_revision.is_some()
            && !self.lifecycle_fence.is_lock_lost()
            && self
                .metadata_transaction_guard
                .as_ref()
                .is_some_and(|guard| !guard.is_lock_lost())
    }

    pub(crate) fn add_lock_fences(&self, opts: &mut ObjectOptions) {
        opts.bucket_lifecycle_lock_fence
            .get_or_insert_with(NamespaceLockFence::new)
            .extend(&self.lifecycle_fence);
        if let Some(guard) = self.metadata_transaction_guard.as_ref() {
            opts.add_namespace_lock_guard(guard);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaAdmission {
    current_usage: u64,
    quota_limit: u64,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleDeleteAllRequest {
    pub(crate) version_id: Option<Uuid>,
    pub(crate) delete_marker: bool,
    pub(crate) action: rustfs_scanner_metrics::metrics::IlmAction,
    pub(crate) rule_id: String,
    pub(crate) phase: LifecycleDeleteAllPhase,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleDeleteAllPhase {
    Preflight,
    History,
    FinalPreflight,
    Trigger,
}

#[doc(hidden)]
#[derive(Default)]
pub struct LifecycleDeleteAllJournalState {
    mutation_started: bool,
}

impl Debug for LifecycleDeleteAllJournalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LifecycleDeleteAllJournalState")
            .field("mutation_started", &self.mutation_started)
            .finish()
    }
}

impl LifecycleDeleteAllJournalState {
    pub(crate) fn mark_mutation_started(&mut self) {
        self.mutation_started = true;
    }

    #[cfg(test)]
    pub(crate) fn mutation_started(&self) -> bool {
        self.mutation_started
    }
}

impl QuotaAdmission {
    pub(crate) fn current_usage(self) -> u64 {
        self.current_usage
    }

    pub(crate) fn quota_limit(self) -> u64 {
        self.quota_limit
    }

    pub(crate) fn remaining(self) -> u64 {
        self.quota_limit - self.current_usage
    }
}

const SCANNER_PUBLICATION_SCOPE_ADMITTED: u8 = 0;
const SCANNER_PUBLICATION_SCOPE_IN_FLIGHT: u8 = 1;
const SCANNER_PUBLICATION_SCOPE_COMMITTED: u8 = 2;
const SCANNER_PUBLICATION_SCOPE_ABORTED_BEFORE_COMMIT: u8 = 3;
const SCANNER_PUBLICATION_SCOPE_INDETERMINATE: u8 = 4;

/// The terminal result of a storage-owned scanner publication mutation.
///
/// This state is deliberately not serialized. It is the ownership hand-off
/// between the scanner coordinator and the storage mutation task, so a
/// detached rename/cleanup task can retain the movement permit until it has
/// reported a definitive result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerPublicationCommitState {
    Admitted,
    InFlight,
    Committed,
    AbortedBeforeCommit,
    Indeterminate,
}

impl ScannerPublicationCommitState {
    fn as_u8(self) -> u8 {
        match self {
            Self::Admitted => SCANNER_PUBLICATION_SCOPE_ADMITTED,
            Self::InFlight => SCANNER_PUBLICATION_SCOPE_IN_FLIGHT,
            Self::Committed => SCANNER_PUBLICATION_SCOPE_COMMITTED,
            Self::AbortedBeforeCommit => SCANNER_PUBLICATION_SCOPE_ABORTED_BEFORE_COMMIT,
            Self::Indeterminate => SCANNER_PUBLICATION_SCOPE_INDETERMINATE,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            SCANNER_PUBLICATION_SCOPE_IN_FLIGHT => Self::InFlight,
            SCANNER_PUBLICATION_SCOPE_COMMITTED => Self::Committed,
            SCANNER_PUBLICATION_SCOPE_ABORTED_BEFORE_COMMIT => Self::AbortedBeforeCommit,
            SCANNER_PUBLICATION_SCOPE_INDETERMINATE => Self::Indeterminate,
            _ => Self::Admitted,
        }
    }

    /// A caller may release its remote lease only after one of these states.
    /// `Indeterminate` is intentionally excluded: the mutation may have
    /// committed after cancellation or a transport failure.
    pub fn permits_lease_release(self) -> bool {
        matches!(self, Self::Committed | Self::AbortedBeforeCommit)
    }
}

/// Why a storage-owned publication scope could not start its mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerPublicationCommitStartError {
    Cancelled,
    DeadlineExceeded,
    AlreadyStarted,
    Terminal,
}

struct ScannerPublicationCommitScopeInner {
    expected_movement_epoch: u64,
    safe_deadline: tokio::time::Instant,
    remote_lease_tokens: Arc<[Uuid]>,
    cancellation: CancellationToken,
    state: AtomicU8,
    completed: Notify,
    /// Set once a storage mutation task has taken ownership of the scope.
    /// The caller-side RAII guard must not classify cancellation as
    /// indeterminate while that owner can still report a definitive result.
    owner_attached: AtomicBool,
    /// The permit is storage-owned rather than borrowed from the scanner
    /// future. A detached mutation task keeps the scope alive and therefore
    /// keeps this guard alive until it reports a terminal state.
    movement_permit: Mutex<Option<OwnedRwLockReadGuard<()>>>,
    lease_release_safe: Arc<AtomicBool>,
}

/// Storage-owned ownership scope for one fenced scanner metadata mutation.
///
/// The scope is an in-memory capability. It is intentionally carried through
/// [`ObjectOptions`] as a hidden field and never participates in serde, object
/// metadata, RPC wire structures, or on-disk formats.
#[derive(Clone)]
pub struct ScannerPublicationCommitScope {
    inner: Arc<ScannerPublicationCommitScopeInner>,
}

/// RAII fallback for storage paths that return before their commit closure
/// takes ownership. An in-flight scope is never guessed to be aborted: it is
/// marked indeterminate so remote lease release remains blocked.
pub(crate) struct ScannerPublicationCommitScopeGuard {
    scope: Option<ScannerPublicationCommitScope>,
}

impl ScannerPublicationCommitScopeGuard {
    pub(crate) fn new(scope: ScannerPublicationCommitScope) -> Self {
        Self { scope: Some(scope) }
    }

    pub(crate) fn disarm(&mut self) {
        self.scope = None;
    }
}

impl Drop for ScannerPublicationCommitScopeGuard {
    fn drop(&mut self) {
        let Some(scope) = self.scope.as_ref() else {
            return;
        };
        if scope.owner_attached() {
            return;
        }
        match scope.state() {
            ScannerPublicationCommitState::Admitted => {
                let _ = scope.mark_aborted_before_commit();
            }
            ScannerPublicationCommitState::InFlight => {
                let _ = scope.mark_indeterminate();
            }
            ScannerPublicationCommitState::Committed
            | ScannerPublicationCommitState::AbortedBeforeCommit
            | ScannerPublicationCommitState::Indeterminate => {}
        }
    }
}

impl Debug for ScannerPublicationCommitScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScannerPublicationCommitScope")
            .field("expected_movement_epoch", &self.expected_movement_epoch())
            .field("safe_deadline", &self.safe_deadline())
            .field("remote_lease_token_count", &self.remote_lease_tokens().len())
            .field("state", &self.state())
            .finish()
    }
}

impl ScannerPublicationCommitScope {
    /// Construct a scope after the storage layer has acquired its movement
    /// read permit. Callers must keep the scope attached to the actual
    /// mutation owner until [`Self::wait_for_completion`] has resolved.
    pub(crate) fn new_storage_owned(
        expected_movement_epoch: u64,
        safe_deadline: tokio::time::Instant,
        remote_lease_tokens: Vec<Uuid>,
        movement_permit: OwnedRwLockReadGuard<()>,
    ) -> Self {
        Self::new_storage_owned_with_release_flag(
            expected_movement_epoch,
            safe_deadline,
            remote_lease_tokens,
            movement_permit,
            Arc::new(AtomicBool::new(true)),
        )
    }

    pub(crate) fn new_storage_owned_with_release_flag(
        expected_movement_epoch: u64,
        safe_deadline: tokio::time::Instant,
        remote_lease_tokens: Vec<Uuid>,
        movement_permit: OwnedRwLockReadGuard<()>,
        lease_release_safe: Arc<AtomicBool>,
    ) -> Self {
        lease_release_safe.store(false, Ordering::Release);
        Self {
            inner: Arc::new(ScannerPublicationCommitScopeInner {
                expected_movement_epoch,
                safe_deadline,
                remote_lease_tokens: remote_lease_tokens.into(),
                cancellation: CancellationToken::new(),
                state: AtomicU8::new(SCANNER_PUBLICATION_SCOPE_ADMITTED),
                completed: Notify::new(),
                owner_attached: AtomicBool::new(false),
                movement_permit: Mutex::new(Some(movement_permit)),
                lease_release_safe,
            }),
        }
    }

    pub fn expected_movement_epoch(&self) -> u64 {
        self.inner.expected_movement_epoch
    }

    pub fn safe_deadline(&self) -> tokio::time::Instant {
        self.inner.safe_deadline
    }

    pub fn is_expired(&self) -> bool {
        tokio::time::Instant::now() >= self.safe_deadline()
    }

    pub fn remote_lease_tokens(&self) -> &[Uuid] {
        &self.inner.remote_lease_tokens
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancellation.is_cancelled()
    }

    /// Whether a mutation that has already begun may still enter its durable
    /// commit boundary. The storage owner must check this immediately before
    /// starting each irreversible fan-out/rename operation.
    pub fn can_commit(&self) -> bool {
        self.state() == ScannerPublicationCommitState::InFlight && !self.is_cancelled() && !self.is_expired()
    }

    /// Transfer terminal-state responsibility from the caller to a detached
    /// storage mutation owner. Once set, dropping a scanner waiter leaves the
    /// scope in-flight until that owner reports committed or indeterminate.
    pub fn attach_mutation_owner(&self) {
        self.inner.owner_attached.store(true, Ordering::Release);
    }

    fn owner_attached(&self) -> bool {
        self.inner.owner_attached.load(Ordering::Acquire)
    }

    pub fn state(&self) -> ScannerPublicationCommitState {
        ScannerPublicationCommitState::from_u8(self.inner.state.load(Ordering::Acquire))
    }

    /// Request cancellation without claiming that a mutation has stopped.
    /// The owner must still report `AbortedBeforeCommit` or `Indeterminate`.
    pub fn cancel(&self) {
        self.inner.cancellation.cancel();
    }

    pub fn try_begin(&self) -> std::result::Result<(), ScannerPublicationCommitStartError> {
        if self.inner.cancellation.is_cancelled() {
            return Err(ScannerPublicationCommitStartError::Cancelled);
        }
        if self.is_expired() {
            return Err(ScannerPublicationCommitStartError::DeadlineExceeded);
        }
        self.inner
            .state
            .compare_exchange(
                SCANNER_PUBLICATION_SCOPE_ADMITTED,
                SCANNER_PUBLICATION_SCOPE_IN_FLIGHT,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|state| {
                if ScannerPublicationCommitState::from_u8(state).permits_lease_release() {
                    ScannerPublicationCommitStartError::Terminal
                } else {
                    ScannerPublicationCommitStartError::AlreadyStarted
                }
            })
    }

    pub fn mark_committed(&self) -> bool {
        self.mark_terminal(ScannerPublicationCommitState::Committed)
    }

    pub fn mark_aborted_before_commit(&self) -> bool {
        if self
            .inner
            .state
            .compare_exchange(
                SCANNER_PUBLICATION_SCOPE_ADMITTED,
                SCANNER_PUBLICATION_SCOPE_ABORTED_BEFORE_COMMIT,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.inner.lease_release_safe.store(true, Ordering::Release);
            self.inner.completed.notify_waiters();
            return true;
        }
        false
    }

    pub fn mark_indeterminate(&self) -> bool {
        self.mark_terminal(ScannerPublicationCommitState::Indeterminate)
    }

    fn mark_terminal(&self, terminal: ScannerPublicationCommitState) -> bool {
        self.inner
            .state
            .compare_exchange(SCANNER_PUBLICATION_SCOPE_IN_FLIGHT, terminal.as_u8(), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            .then(|| {
                if terminal.permits_lease_release() {
                    self.inner.lease_release_safe.store(true, Ordering::Release);
                }
                self.inner.completed.notify_waiters()
            })
            .is_some()
    }

    /// Wait until the mutation owner has reported a definitive terminal
    /// state. The permit remains owned by this scope until all scope clones are
    /// dropped or [`Self::release_movement_permit`] is called safely.
    pub async fn wait_for_completion(&self) -> ScannerPublicationCommitState {
        loop {
            let notified = self.inner.completed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let state = self.state();
            if state != ScannerPublicationCommitState::Admitted && state != ScannerPublicationCommitState::InFlight {
                return state;
            }
            notified.await;
        }
    }

    /// Release the storage-owned movement permit only after a known-safe
    /// terminal result. Returns `false` for in-flight or indeterminate work.
    pub async fn release_movement_permit(&self) -> bool {
        if !self.state().permits_lease_release() {
            return false;
        }
        self.inner.movement_permit.lock().await.take().is_some()
    }
}

impl Drop for ScannerPublicationCommitScopeInner {
    fn drop(&mut self) {
        if !ScannerPublicationCommitState::from_u8(self.state.load(Ordering::Acquire)).permits_lease_release() {
            self.lease_release_safe.store(false, Ordering::Release);
        }
    }
}

#[derive(Clone, Default)]
#[doc(hidden)]
pub struct DecommissionCapacityOptions {
    pub(crate) expected_data_bytes: Option<usize>,
    pub(crate) operation_id: Option<Uuid>,
    pub(crate) generation: Option<u64>,
    pub(crate) owner_nonce: Option<Uuid>,
    pub(crate) mutation_id: Option<Uuid>,
}

/// Opaque storage-owned collection point for post-commit tier free-version
/// cleanup receipts. This type is public only because workspace crates build
/// [`ObjectOptions`] with struct literals; callers outside `ecstore` must leave
/// the corresponding option unset.
#[doc(hidden)]
#[derive(Clone)]
pub struct TierFreeVersionReceiptSink {
    inner: Arc<parking_lot::Mutex<TierFreeVersionReceiptSinkState>>,
}

struct TierFreeVersionReceiptSinkState {
    receipts: Option<HashMap<TierFreeVersionReceiptIdentity, TierFreeVersionReceiptPayload>>,
}

#[derive(PartialEq, Eq, Hash)]
struct TierFreeVersionReceiptIdentity {
    bucket: String,
    logical_name: String,
    tier: String,
    remote_name: String,
    remote_version_state: TierFreeVersionReceiptVersionState,
    remote_version: String,
    backend_identity: crate::services::tier::tier::TierDestinationId,
}

struct TierFreeVersionReceiptPayload {
    local_free_version_id: Uuid,
    mod_time: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum TierFreeVersionReceiptVersionState {
    KnownDisabled,
    SuspendedNull,
    Exact,
}

impl TierFreeVersionReceiptSink {
    /// Only the delete wrapper may originate a sink. The public type exists so
    /// workspace struct literals can carry it, but external crates cannot
    /// create an undrainable collector accidentally.
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(parking_lot::Mutex::new(TierFreeVersionReceiptSinkState {
                receipts: Some(HashMap::new()),
            })),
        }
    }
}

impl std::fmt::Debug for TierFreeVersionReceiptSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.inner.lock();
        f.debug_struct("TierFreeVersionReceiptSink")
            .field("drained", &state.receipts.is_none())
            .field("receipt_count", &state.receipts.as_ref().map(HashMap::len).unwrap_or_default())
            .finish()
    }
}

impl TierFreeVersionReceiptVersionState {
    fn persisted(self) -> rustfs_filemeta::TransitionVersionState {
        match self {
            Self::KnownDisabled => rustfs_filemeta::TransitionVersionState::KnownDisabled,
            Self::SuspendedNull => rustfs_filemeta::TransitionVersionState::SuspendedNull,
            Self::Exact => rustfs_filemeta::TransitionVersionState::Exact,
        }
    }
}

impl TierFreeVersionReceiptIdentity {
    fn into_object_info(self, payload: TierFreeVersionReceiptPayload) -> ObjectInfo {
        let mut metadata = HashMap::with_capacity(2);
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(self.backend_identity),
        );
        ObjectInfo {
            bucket: self.bucket,
            name: self.logical_name,
            mod_time: payload.mod_time,
            user_defined: Arc::new(metadata),
            version_id: Some(payload.local_free_version_id),
            delete_marker: true,
            transitioned_object: TransitionedObject {
                name: self.remote_name,
                version_id: self.remote_version,
                tier: self.tier,
                free_version: true,
                status: String::new(),
            },
            transition_version_state: self.remote_version_state.persisted(),
            ..Default::default()
        }
    }
}

fn tier_free_version_scheduling_receipt_from_source(
    source: &ObjectInfo,
    local_free_version_id: Uuid,
) -> io::Result<Option<(TierFreeVersionReceiptIdentity, TierFreeVersionReceiptPayload)>> {
    if source.transitioned_object.status != rustfs_filemeta::TRANSITION_COMPLETE
        || source.transitioned_object.free_version
        || source.delete_marker
        || source.bucket.is_empty()
        || source.name.is_empty()
        || source.transitioned_object.tier.is_empty()
        || source.transitioned_object.name.is_empty()
    {
        return Ok(None);
    }
    if local_free_version_id.is_nil() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tier free-version receipt has a nil local version identity",
        ));
    }

    let remote_version = source.transitioned_object.version_id.as_str();
    let remote_version_state = match source.transition_version_state {
        rustfs_filemeta::TransitionVersionState::Unknown => return Ok(None),
        rustfs_filemeta::TransitionVersionState::KnownDisabled if remote_version.is_empty() => {
            TierFreeVersionReceiptVersionState::KnownDisabled
        }
        rustfs_filemeta::TransitionVersionState::SuspendedNull if remote_version == "null" => {
            TierFreeVersionReceiptVersionState::SuspendedNull
        }
        rustfs_filemeta::TransitionVersionState::Exact if !remote_version.is_empty() && remote_version != "null" => {
            TierFreeVersionReceiptVersionState::Exact
        }
        _ => return Ok(None),
    };

    let Some(backend_identity) = crate::services::tier::tier::tier_destination_id_from_metadata(&source.user_defined)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
    else {
        return Ok(None);
    };

    Ok(Some((
        TierFreeVersionReceiptIdentity {
            bucket: source.bucket.clone(),
            logical_name: decode_dir_object(&source.name),
            tier: source.transitioned_object.tier.clone(),
            remote_name: source.transitioned_object.name.clone(),
            remote_version_state,
            remote_version: source.transitioned_object.version_id.clone(),
            backend_identity,
        },
        TierFreeVersionReceiptPayload {
            local_free_version_id,
            mod_time: source.mod_time,
        },
    )))
}

impl TierFreeVersionReceiptSink {
    /// Record one committed free-version cleanup target. Cloned options share
    /// this sink; tuple-equivalent physical copies collapse to one worker task.
    /// `false` means the source cannot safely identify a destructive cleanup.
    pub(crate) fn record(&self, source: &ObjectInfo, local_free_version_id: Uuid) -> io::Result<bool> {
        let Some((identity, payload)) = tier_free_version_scheduling_receipt_from_source(source, local_free_version_id)? else {
            return Ok(false);
        };
        let mut state = self.inner.lock();
        let receipts = state
            .receipts
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "tier free-version receipt sink was already drained"))?;
        receipts.entry(identity).or_insert(payload);
        Ok(true)
    }

    /// Consume every receipt exactly once. A second drain is a caller bug: it
    /// could otherwise make two outer wrappers believe they own the same tasks.
    pub(crate) fn drain(&self) -> io::Result<Vec<ObjectInfo>> {
        let mut state = self.inner.lock();
        let receipts = state
            .receipts
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "tier free-version receipt sink was already drained"))?;
        drop(state);
        Ok(receipts
            .into_iter()
            .map(|(identity, payload)| identity.into_object_info(payload))
            .collect())
    }
}

/// Internal PUT completion boundary; this does not change fsync or write quorum.
#[doc(hidden)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum WriteCompletion {
    /// Return at write quorum when the commit owner can retain its guards.
    #[default]
    Quorum,
    /// Drain the rename fan-out before returning. Minority failures still heal
    /// after a successful quorum commit; this does not require every disk to succeed.
    TailDrained,
}

#[derive(Default, Clone)]
pub struct ObjectOptions {
    // Use the maximum parity (N/2), used when saving server configuration files
    pub max_parity: bool,
    pub mod_time: Option<OffsetDateTime>,
    pub part_number: Option<usize>,

    pub delete_prefix: bool,
    pub delete_prefix_object: bool,
    pub version_id: Option<String>,
    /// Lifecycle-only staged purge request checked under the object write lock.
    #[doc(hidden)]
    pub lifecycle_delete_all: Option<LifecycleDeleteAllRequest>,
    #[doc(hidden)]
    pub lifecycle_delete_all_journal: Option<Arc<parking_lot::Mutex<LifecycleDeleteAllJournalState>>>,
    /// Whole-operation authorization created only by consuming a validated
    /// v6 dispatch-manifest permit. Clones share the authorization, not the
    /// one-shot permit itself.
    #[doc(hidden)]
    pub tier_delete_dispatch_authorization:
        Option<crate::bucket::lifecycle::tier_delete_journal::TierDeleteDispatchAuthorization>,
    /// RustFS-only compare-and-set condition checked under the object write lock.
    pub expected_current_version_id: Option<String>,
    /// Persisted bucket incarnation observed before authorization.
    pub expected_bucket_incarnation_id: Option<Uuid>,
    pub no_lock: bool,
    /// Control-plane writers that immediately read or CAS the same namespace
    /// key use TailDrained without changing namespace lock ownership.
    #[doc(hidden)]
    pub write_completion: WriteCompletion,
    /// True when an upper layer already holds the object read lock before
    /// forwarding a no_lock read to the set layer.
    pub metadata_cache_safe: bool,

    pub versioned: bool,
    pub version_suspended: bool,
    pub incl_free_versions: bool,

    pub skip_decommissioned: bool,
    pub skip_rebalancing: bool,
    pub skip_free_version: bool,

    /// Storage-owned, per-request hand-off for committed tier free-version
    /// cleanup work. The outer delete wrapper installs and drains it; clones
    /// below that boundary share the same opaque sink.
    #[doc(hidden)]
    pub tier_free_version_receipt_sink: Option<TierFreeVersionReceiptSink>,

    /// Cooperative cancellation for an owned PutObject before authoritative
    /// rename begins. Storage ignores it after entering the durable commit.
    #[doc(hidden)]
    pub put_object_cancellation: Option<tokio_util::sync::CancellationToken>,

    /// Storage-owned scanner publication capability. This field is an
    /// in-memory hand-off only; it is never copied into object metadata.
    #[doc(hidden)]
    pub scanner_publication_commit_scope: Option<ScannerPublicationCommitScope>,

    pub data_movement: bool,
    pub raw_data_movement_read: bool,
    /// Durable reservation identity carried only by decommission writes. Other
    /// data-movement users, including rebalance, leave it unset. Keep this
    /// context boxed because `ObjectOptions` is passed by value through deep
    /// storage futures.
    #[doc(hidden)]
    pub decommission_capacity: Option<Box<DecommissionCapacityOptions>>,
    /// Materialize the data-movement per-part checksum sidecar for APIs that
    /// return part checksums. Ordinary object reads leave it encoded.
    pub include_part_checksums: bool,
    pub src_pool_idx: usize,
    pub user_defined: HashMap<String, String>,
    pub preserve_etag: Option<String>,
    pub metadata_chg: bool,
    pub http_preconditions: Option<HTTPPreconditions>,
    /// Internal create-only writes may also preserve an acknowledged deletion.
    /// Evaluated with `http_preconditions` under the namespace commit lock.
    pub preserve_delete_marker: bool,

    pub delete_replication: Option<ReplicationState>,
    pub delete_replication_config_snapshot: Option<Arc<DeleteReplicationConfigSnapshot>>,
    pub namespace_lock_fence: Option<NamespaceLockFence>,
    /// Proves an upper layer holds the bucket lifecycle sentinel. A separate
    /// fence avoids recursively acquiring the read lock behind a queued writer.
    pub bucket_lifecycle_lock_fence: Option<NamespaceLockFence>,
    pub replication_request: bool,
    /// True when the inbound request carried the
    /// `{x-rustfs-,x-minio-}source-proxy-request` header family with the
    /// value "true": the request was already proxied by a replication peer,
    /// so this server must not proxy a local miss onward (anti-loop,
    /// MinIO-compatible). The header only disables proxying — it grants no
    /// capability — so no authorization gate is required to honor it.
    pub proxy_request: bool,
    /// True when the `source-proxy-request` header family was present at
    /// all, regardless of value (MinIO's `ProxyHeaderSet`). A replication
    /// peer sends `source-proxy-request: false` on its worker convergence
    /// HEADs precisely so the receiver answers locally instead of proxying
    /// back — otherwise a proxied 404->200 echo makes the worker believe the
    /// object already converged and it never replicates it.
    pub proxy_header_set: bool,
    /// Source-cluster LWW timestamps carried by an authorized replication
    /// request; None when the source never modified the category. Only the
    /// replication-authorized options builders may set these.
    pub replication_tagging_timestamp: Option<OffsetDateTime>,
    pub replication_retention_timestamp: Option<OffsetDateTime>,
    pub replication_legalhold_timestamp: Option<OffsetDateTime>,
    /// Authorized SSE-C replication passthrough: the body is already
    /// ciphertext, so the write path must not encrypt or compress it and
    /// stores the restored encryption metadata verbatim. Only the
    /// replication-authorized options builders may set this.
    pub preserve_ciphertext: bool,
    pub delete_marker: bool,
    pub synthetic_version_id: bool,

    pub transition: TransitionOptions,
    pub expiration: ExpirationOptions,
    pub lifecycle_audit_event: LcAuditEvent,

    pub eval_metadata: Option<HashMap<String, String>>,
    /// Internal compare-and-set condition for replication workers publishing
    /// terminal status after remote I/O. Storage validates it while holding
    /// the object write lock so an older worker cannot overwrite a newer
    /// mutation's PENDING state. Keep the condition boxed because
    /// `ObjectOptions` is passed by value through deep storage futures.
    #[doc(hidden)]
    pub replication_status_writeback: Option<Box<ReplicationStatusWritebackCondition>>,
    pub object_lock_retention: Option<ObjectLockRetentionOptions>,
    pub object_lock_delete: Option<crate::storage_api_contracts::object::ObjectLockDeleteOptions>,
    /// Authoritative bucket Object Lock snapshot installed inside `ECStore`
    /// before a destructive commit reaches the set layer.
    pub object_lock_config_snapshot: Option<Arc<ObjectLockConfigSnapshot>>,

    pub want_checksum: Option<Checksum>,
    pub skip_verify_bitrot: bool,
    pub capacity_scope_token: Option<Uuid>,
    /// Server-derived bucket-quota snapshot for commit-boundary admission.
    pub quota_admission: Option<QuotaAdmission>,
    /// Storage-owned journal writer used by the atomic delete path. This is
    /// populated only by the `ECStore` wrapper that holds the namespace locks.
    pub tier_delete_journal_api: Option<Arc<crate::store::ECStore>>,
    /// Internal staged-mutation admission supplied by `ECStore`; each local
    /// publish is fenced namespace-first and then by decommission capacity.
    #[doc(hidden)]
    pub decommission_capacity_admission: Option<Arc<crate::store::ECStore>>,
}

#[derive(Clone, Debug, Default)]
#[doc(hidden)]
pub struct ReplicationStatusWritebackCondition {
    pub(crate) expected_generation: ReplicationGenerationSnapshot,
    pub(crate) mode: ReplicationStatusWritebackMode,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[doc(hidden)]
pub enum ReplicationStatusWritebackMode {
    #[default]
    Update,
    ValidateOnly,
}

impl ObjectOptions {
    pub(crate) fn with_capacity_expected_data_bytes(expected_data_bytes: Option<usize>) -> Self {
        Self {
            decommission_capacity: expected_data_bytes.map(|expected_data_bytes| {
                Box::new(DecommissionCapacityOptions {
                    expected_data_bytes: Some(expected_data_bytes),
                    ..Default::default()
                })
            }),
            ..Default::default()
        }
    }

    pub(crate) fn capacity_expected_data_bytes(&self) -> Option<usize> {
        self.decommission_capacity
            .as_deref()
            .and_then(|capacity| capacity.expected_data_bytes)
    }

    pub(crate) fn has_decommission_capacity_reservation(&self) -> bool {
        self.decommission_capacity
            .as_deref()
            .is_some_and(|capacity| capacity.operation_id.is_some())
    }
}

impl std::fmt::Debug for ObjectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectOptions")
            .field("max_parity", &self.max_parity)
            .field("mod_time", &self.mod_time)
            .field("part_number", &self.part_number)
            .field("delete_prefix", &self.delete_prefix)
            .field("delete_prefix_object", &self.delete_prefix_object)
            .field("version_id", &self.version_id.is_some())
            .field("lifecycle_delete_all", &self.lifecycle_delete_all.is_some())
            .field("lifecycle_delete_all_journal", &self.lifecycle_delete_all_journal.is_some())
            .field("tier_delete_dispatch_authorization", &self.tier_delete_dispatch_authorization.is_some())
            .field("expected_current_version_id", &self.expected_current_version_id.is_some())
            .field("expected_bucket_incarnation_id", &self.expected_bucket_incarnation_id)
            .field("no_lock", &self.no_lock)
            .field("metadata_cache_safe", &self.metadata_cache_safe)
            .field("versioned", &self.versioned)
            .field("version_suspended", &self.version_suspended)
            .field("incl_free_versions", &self.incl_free_versions)
            .field("skip_decommissioned", &self.skip_decommissioned)
            .field("skip_rebalancing", &self.skip_rebalancing)
            .field("skip_free_version", &self.skip_free_version)
            .field("tier_free_version_receipt_sink", &self.tier_free_version_receipt_sink)
            .field("put_object_cancellation", &self.put_object_cancellation.is_some())
            .field("scanner_publication_commit_scope", &self.scanner_publication_commit_scope)
            .field("data_movement", &self.data_movement)
            .field("raw_data_movement_read", &self.raw_data_movement_read)
            .field("include_part_checksums", &self.include_part_checksums)
            .field("src_pool_idx", &self.src_pool_idx)
            .field("user_defined_count", &self.user_defined.len())
            .field("preserve_etag", &self.preserve_etag.is_some())
            .field("metadata_chg", &self.metadata_chg)
            .field("http_preconditions", &self.http_preconditions.is_some())
            .field("delete_replication", &self.delete_replication.is_some())
            .field("delete_replication_config_snapshot", &self.delete_replication_config_snapshot)
            .field("namespace_lock_fence", &self.namespace_lock_fence.is_some())
            .field("bucket_lifecycle_lock_fence", &self.bucket_lifecycle_lock_fence.is_some())
            .field("replication_request", &self.replication_request)
            .field("proxy_request", &self.proxy_request)
            .field("proxy_header_set", &self.proxy_header_set)
            .field("replication_tagging_timestamp", &self.replication_tagging_timestamp)
            .field("replication_retention_timestamp", &self.replication_retention_timestamp)
            .field("replication_legalhold_timestamp", &self.replication_legalhold_timestamp)
            .field("preserve_ciphertext", &self.preserve_ciphertext)
            .field("delete_marker", &self.delete_marker)
            .field("synthetic_version_id", &self.synthetic_version_id)
            .field(
                "transition",
                &(self.data_movement
                    || !self.transition.status.is_empty()
                    || !self.transition.tier.is_empty()
                    || self.transition.expected_data_dir.is_some()),
            )
            .field("expiration", &self.expiration)
            .field(
                "lifecycle_audit_event",
                &(!self.lifecycle_audit_event.event.rule_id.is_empty()
                    || !self.lifecycle_audit_event.event.storage_class.is_empty()),
            )
            .field("eval_metadata_count", &self.eval_metadata.as_ref().map(HashMap::len))
            .field("replication_status_writeback", &self.replication_status_writeback.is_some())
            .field("object_lock_retention", &self.object_lock_retention.is_some())
            .field("object_lock_delete", &self.object_lock_delete)
            .field("object_lock_config_snapshot", &self.object_lock_config_snapshot.is_some())
            .field("want_checksum", &self.want_checksum)
            .field("skip_verify_bitrot", &self.skip_verify_bitrot)
            .field("capacity_scope_token", &self.capacity_scope_token)
            .field("quota_admission", &self.quota_admission)
            .field("tier_delete_journal_api", &self.tier_delete_journal_api.is_some())
            .finish()
    }
}

/// Transient scanner-only carrier for target-side publication lease tokens.
/// SetDisks consumes and removes this key before constructing durable
/// FileInfo metadata; it must never appear in an S3-visible object.
pub const SCANNER_PUBLICATION_LEASE_FENCE_METADATA_KEY: &str = "x-rustfs-internal-scanner-publication-lease-fence-v1";

impl ObjectOptions {
    /// Create a new ObjectOptions with modified no_lock field.
    pub fn with_no_lock(&self, no_lock: bool) -> Self {
        let mut opts = self.clone();
        opts.no_lock = no_lock;
        opts
    }

    /// Create commit options from base options (optimized clone).
    pub fn as_commit_opts(&self) -> Self {
        let mut opts = self.clone();
        opts.no_lock = true;
        opts.metadata_cache_safe = false;
        opts.include_part_checksums = true;
        opts
    }

    /// Create read options with include_part_checksums enabled.
    pub fn as_read_opts(&self) -> Self {
        let mut opts = self.clone();
        opts.include_part_checksums = true;
        opts
    }

    pub fn set_quota_admission(&mut self, current_usage: u64, quota_limit: u64) -> bool {
        self.quota_admission = (current_usage <= quota_limit).then_some(QuotaAdmission {
            current_usage,
            quota_limit,
        });
        self.quota_admission.is_some()
    }

    pub(crate) fn overwrites_existing_version(&self) -> bool {
        self.version_id.is_some() || !self.versioned || self.version_suspended
    }

    pub(crate) fn add_namespace_lock_lost_signal(&mut self, signal: Arc<rustfs_lock::distributed_lock::LockLostSignal>) {
        #[cfg(test)]
        let test_fence = NAMESPACE_LOCK_SIGNAL_TEST_FENCES
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("namespace lock signal test fence should not be poisoned")
            .iter()
            .find(|(key, _)| *key == Arc::as_ptr(&signal) as usize)
            .map(|(_, fence)| fence.clone());
        self.namespace_lock_fence
            .get_or_insert_with(NamespaceLockFence::new)
            .add_signal(signal);
        #[cfg(test)]
        if let Some(test_fence) = test_fence {
            self.namespace_lock_fence
                .get_or_insert_with(NamespaceLockFence::new)
                .extend(&test_fence);
        }
    }

    pub(crate) fn ensure_namespace_lock_fence(&mut self) {
        self.namespace_lock_fence.get_or_insert_with(NamespaceLockFence::new);
    }

    #[cfg(test)]
    pub(crate) fn add_namespace_lock_fence(&mut self, fence: &NamespaceLockFence) {
        self.namespace_lock_fence
            .get_or_insert_with(NamespaceLockFence::new)
            .extend(fence);
    }

    pub(crate) fn ensure_lifecycle_delete_all_journal(&mut self) {
        self.lifecycle_delete_all_journal
            .get_or_insert_with(|| Arc::new(parking_lot::Mutex::new(LifecycleDeleteAllJournalState::default())));
    }

    pub(crate) fn lifecycle_delete_all_journal(&self) -> Option<&Arc<parking_lot::Mutex<LifecycleDeleteAllJournalState>>> {
        self.lifecycle_delete_all_journal.as_ref()
    }

    pub fn add_namespace_lock_guard(&mut self, guard: &rustfs_lock::NamespaceLockGuard) {
        if let Some(signal) = guard.lock_lost_signal() {
            self.add_namespace_lock_lost_signal(signal);
        }
    }

    pub fn add_bucket_lifecycle_lock_guard(&mut self, guard: &rustfs_lock::NamespaceLockGuard) {
        let fence = self.bucket_lifecycle_lock_fence.get_or_insert_with(NamespaceLockFence::new);
        if let Some(signal) = guard.lock_lost_signal() {
            fence.add_signal(signal);
        }
    }

    pub fn set_delete_replication_state(&mut self, dsc: ReplicateDecision) {
        let mut rs = ReplicationState {
            replicate_decision_str: dsc.to_string(),
            ..Default::default()
        };
        if self.version_id.is_none() {
            rs.replication_status_internal = dsc.pending_status();
            rs.targets = replication_statuses_map(rs.replication_status_internal.as_deref().unwrap_or_default());
        } else {
            rs.version_purge_status_internal = dsc.pending_status();
            rs.purge_targets = version_purge_statuses_map(rs.version_purge_status_internal.as_deref().unwrap_or_default());
        }

        self.delete_replication = Some(rs)
    }

    pub fn set_replica_status(&mut self, status: ReplicationStatusType) {
        if let Some(rs) = self.delete_replication.as_mut() {
            rs.replica_status = status;
            rs.replica_timestamp = Some(OffsetDateTime::now_utc());
        } else {
            self.delete_replication = Some(ReplicationState {
                replica_status: status,
                replica_timestamp: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            });
        }
    }

    pub fn version_purge_status(&self) -> VersionPurgeStatusType {
        self.delete_replication
            .as_ref()
            .map(|v| v.composite_version_purge_status())
            .unwrap_or(VersionPurgeStatusType::Empty)
    }

    pub fn delete_marker_replication_status(&self) -> ReplicationStatusType {
        self.delete_replication
            .as_ref()
            .map(|v| v.composite_replication_status())
            .unwrap_or(ReplicationStatusType::Empty)
    }

    pub fn put_replication_state(&self) -> ReplicationState {
        if self
            .delete_replication
            .as_ref()
            .is_some_and(|state| !state.replica_status.is_empty())
        {
            return self.delete_replication.clone().unwrap_or_default();
        }

        let rs = match rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_STATUS) {
            Some(v) => v,
            None => return ReplicationState::default(),
        };

        ReplicationState {
            replication_status_internal: Some(rs.to_string()),
            targets: replication_statuses_map(rs.as_str()),
            ..Default::default()
        }
    }

    pub fn precondition_check(&self, obj_info: &ObjectInfo) -> Result<()> {
        let requested_part = self.part_number.and_then(|part_number| {
            if part_number > 1 && !obj_info.parts.is_empty() {
                Some(ObjectPreconditionPart {
                    number: part_number,
                    exists: obj_info.parts.iter().any(|pi| pi.number == part_number),
                })
            } else {
                None
            }
        });
        let state = ObjectPreconditionState {
            etag: obj_info.etag.as_deref(),
            mod_time: obj_info.mod_time,
            requested_part,
        };

        state.check(self.http_preconditions.as_ref()).map_err(|err| match err {
            ObjectPreconditionError::InvalidPartNumber(part_number) => Error::InvalidPartNumber(part_number),
            ObjectPreconditionError::NotModified => Error::NotModified,
            ObjectPreconditionError::PreconditionFailed => Error::PreconditionFailed,
        })
    }
}

fn replication_snapshot_internal_value(
    metadata: &HashMap<String, String>,
    suffix: &str,
) -> std::result::Result<Option<String>, ()> {
    match rustfs_utils::http::get_consistent_str(metadata, suffix) {
        Some(value) => Ok(Some(value.to_string())),
        None if rustfs_utils::http::contains_key_str(metadata, suffix) => Err(()),
        None => Ok(None),
    }
}

fn update_replication_fingerprint_bytes(hasher: &mut Sha256, value: &[u8]) {
    let len = u64::try_from(value.len()).unwrap_or(u64::MAX);
    hasher.update(len.to_le_bytes());
    hasher.update(value);
}

fn update_replication_fingerprint_optional_str(hasher: &mut Sha256, value: Option<&str>) {
    if let Some(value) = value {
        hasher.update([1]);
        update_replication_fingerprint_bytes(hasher, value.as_bytes());
    } else {
        hasher.update([0]);
    }
}

#[derive(Debug, Default)]
pub struct ObjectInfo {
    pub bucket: String,
    pub name: String,
    pub storage_class: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
    pub size: i64,
    // Actual size is the real size of the object uploaded by client.
    pub actual_size: i64,
    pub is_dir: bool,
    pub user_defined: Arc<HashMap<String, String>>,
    pub parity_blocks: usize,
    pub data_blocks: usize,
    pub version_id: Option<Uuid>,
    /// xl.meta directory UUID for this version, regenerated on every body write.
    /// A write-unique token: the object data cache keys on it so an overwrite
    /// cannot be served the previous body under an MD5 collision (backlog#1111).
    pub data_dir: Option<Uuid>,
    pub delete_marker: bool,
    pub transitioned_object: TransitionedObject,
    pub transition_version_state: rustfs_filemeta::TransitionVersionState,
    pub restore_ongoing: bool,
    pub restore_expires: Option<OffsetDateTime>,
    pub user_tags: Arc<String>,
    pub parts: Arc<Vec<ObjectPartInfo>>,
    pub is_latest: bool,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub expires: Option<OffsetDateTime>,
    pub num_versions: usize,
    pub successor_mod_time: Option<OffsetDateTime>,
    pub put_object_reader: Option<PutObjReader>,
    pub etag: Option<String>,
    pub inlined: bool,
    pub metadata_only: bool,
    pub version_only: bool,
    pub replication_status_internal: Option<String>,
    pub replication_status: ReplicationStatusType,
    pub version_purge_status_internal: Option<String>,
    pub version_purge_status: VersionPurgeStatusType,
    pub replication_decision: String,
    pub checksum: Option<Bytes>,
}

impl Clone for ObjectInfo {
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            storage_class: self.storage_class.clone(),
            mod_time: self.mod_time,
            size: self.size,
            actual_size: self.actual_size,
            is_dir: self.is_dir,
            user_defined: self.user_defined.clone(),
            parity_blocks: self.parity_blocks,
            data_blocks: self.data_blocks,
            version_id: self.version_id,
            data_dir: self.data_dir,
            delete_marker: self.delete_marker,
            transitioned_object: self.transitioned_object.clone(),
            transition_version_state: self.transition_version_state,
            restore_ongoing: self.restore_ongoing,
            restore_expires: self.restore_expires,
            user_tags: self.user_tags.clone(),
            parts: self.parts.clone(),
            is_latest: self.is_latest,
            content_type: self.content_type.clone(),
            content_encoding: self.content_encoding.clone(),
            num_versions: self.num_versions,
            successor_mod_time: self.successor_mod_time,
            put_object_reader: None, // reader can not clone
            etag: self.etag.clone(),
            inlined: self.inlined,
            metadata_only: self.metadata_only,
            version_only: self.version_only,
            replication_status_internal: self.replication_status_internal.clone(),
            replication_status: self.replication_status.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            version_purge_status: self.version_purge_status.clone(),
            replication_decision: self.replication_decision.clone(),
            checksum: self.checksum.clone(),
            expires: self.expires,
        }
    }
}

impl ObjectInfo {
    /// Capture the source mutation snapshot used by replication workers when
    /// publishing terminal status. The semantic fingerprint is recomputed at
    /// the storage CAS boundary, so an older writer that preserves an unknown
    /// UUID and collides on the timestamp still cannot hide a payload change.
    pub(crate) fn replication_generation_snapshot(&self) -> ReplicationGenerationSnapshot {
        let timestamp = replication_snapshot_internal_value(&self.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP);
        let mutation_id =
            replication_snapshot_internal_value(&self.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_GENERATION);
        let tagging_timestamp =
            replication_snapshot_internal_value(&self.user_defined, rustfs_utils::http::SUFFIX_TAGGING_TIMESTAMP);
        let retention_timestamp =
            replication_snapshot_internal_value(&self.user_defined, rustfs_utils::http::SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP);
        let legalhold_timestamp =
            replication_snapshot_internal_value(&self.user_defined, rustfs_utils::http::SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP);

        let invalid = timestamp.is_err()
            || mutation_id.is_err()
            || tagging_timestamp.is_err()
            || retention_timestamp.is_err()
            || legalhold_timestamp.is_err();
        let timestamp = timestamp.unwrap_or_default();
        let mutation_id = mutation_id.unwrap_or_default();
        let tagging_timestamp = tagging_timestamp.unwrap_or_default();
        let retention_timestamp = retention_timestamp.unwrap_or_default();
        let legalhold_timestamp = legalhold_timestamp.unwrap_or_default();
        let opaque_timestamp_is_invalid = [
            timestamp.as_deref(),
            tagging_timestamp.as_deref(),
            retention_timestamp.as_deref(),
            legalhold_timestamp.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(str::is_empty);
        let mutation_id_is_invalid = mutation_id.as_deref().is_some_and(|value| {
            Uuid::parse_str(value)
                .ok()
                .filter(|generation| !generation.is_nil())
                .is_none()
        });
        let invalid =
            invalid || opaque_timestamp_is_invalid || mutation_id_is_invalid || (mutation_id.is_some() && timestamp.is_none());

        let payload_fingerprint = (!invalid).then(|| {
            self.replication_payload_fingerprint(
                tagging_timestamp.as_deref(),
                retention_timestamp.as_deref(),
                legalhold_timestamp.as_deref(),
            )
        });

        ReplicationGenerationSnapshot {
            timestamp,
            mutation_id,
            payload_fingerprint,
            invalid,
        }
    }

    fn replication_payload_fingerprint(
        &self,
        tagging_timestamp: Option<&str>,
        retention_timestamp: Option<&str>,
        legalhold_timestamp: Option<&str>,
    ) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"rustfs-replication-payload-v1");

        // A suspended bucket's current null version is represented as
        // `Some(Uuid::nil())` at the request/queue boundary and as `None`
        // when the same xl.meta is reread without versioning flags. They are
        // one persisted identity, so do not let that representation detail
        // make a worker permanently supersede its own terminal write-back.
        if let Some(version_id) = self.version_id.filter(|version_id| !version_id.is_nil()) {
            hasher.update([1]);
            hasher.update(version_id.as_bytes());
        } else {
            hasher.update([0]);
        }
        if let Some(data_dir) = self.data_dir {
            hasher.update([1]);
            hasher.update(data_dir.as_bytes());
        } else {
            hasher.update([0]);
        }
        if let Some(mod_time) = self.mod_time {
            hasher.update([1]);
            hasher.update(mod_time.unix_timestamp_nanos().to_le_bytes());
        } else {
            hasher.update([0]);
        }

        update_replication_fingerprint_optional_str(&mut hasher, self.content_type.as_deref());
        update_replication_fingerprint_optional_str(&mut hasher, self.content_encoding.as_deref());
        update_replication_fingerprint_optional_str(&mut hasher, self.storage_class.as_deref());
        if let Some(expires) = self.expires {
            hasher.update([1]);
            hasher.update(expires.unix_timestamp_nanos().to_le_bytes());
        } else {
            hasher.update([0]);
        }
        update_replication_fingerprint_bytes(&mut hasher, self.user_tags.as_bytes());
        update_replication_fingerprint_optional_str(&mut hasher, tagging_timestamp);
        update_replication_fingerprint_optional_str(&mut hasher, retention_timestamp);
        update_replication_fingerprint_optional_str(&mut hasher, legalhold_timestamp);

        let mut target_arns = self
            .replication_status_internal
            .as_deref()
            .map(replication_statuses_map)
            .unwrap_or_default()
            .into_keys()
            .collect::<Vec<_>>();
        target_arns.sort_unstable();
        hasher.update(u64::try_from(target_arns.len()).unwrap_or(u64::MAX).to_le_bytes());
        for arn in target_arns {
            update_replication_fingerprint_bytes(&mut hasher, arn.as_bytes());
        }
        update_replication_fingerprint_bytes(&mut hasher, self.replication_decision.as_bytes());

        let mut user_metadata = self
            .user_defined
            .iter()
            .filter(|(key, _)| {
                !rustfs_utils::http::is_internal_key(key)
                    && !key.eq_ignore_ascii_case(rustfs_utils::http::AMZ_BUCKET_REPLICATION_STATUS)
            })
            .collect::<Vec<_>>();
        user_metadata.sort_unstable_by(|left, right| left.0.cmp(right.0).then_with(|| left.1.cmp(right.1)));
        hasher.update(u64::try_from(user_metadata.len()).unwrap_or(u64::MAX).to_le_bytes());
        for (key, value) in user_metadata {
            update_replication_fingerprint_bytes(&mut hasher, key.as_bytes());
            update_replication_fingerprint_bytes(&mut hasher, value.as_bytes());
        }

        hasher.finalize().into()
    }

    pub fn is_compressed(&self) -> bool {
        rustfs_utils::http::contains_key_str(&self.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION)
    }

    pub fn is_compressed_ok(&self) -> Result<(CompressionAlgorithm, bool)> {
        let (algorithm, _, compressed) = self.compression_read_plan()?;
        Ok((algorithm, compressed))
    }

    pub fn compression_read_plan(&self) -> Result<(CompressionAlgorithm, crate::io_support::rio::ReadCompressionBackend, bool)> {
        let scheme = rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION);

        if let Some(scheme) = scheme {
            let (algorithm, backend) = crate::io_support::rio::compression_scheme_to_read_plan(&scheme)?;
            Ok((algorithm, backend, true))
        } else {
            Ok((CompressionAlgorithm::None, crate::io_support::rio::ReadCompressionBackend::Legacy, false))
        }
    }

    pub fn is_multipart(&self) -> bool {
        self.parts.len() > 1 || self.etag.as_ref().is_some_and(|v| v.len() != 32)
    }

    pub fn is_encrypted(&self) -> bool {
        self.user_defined
            .keys()
            .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
    }

    /// Historical non-versioned inline size reference.
    ///
    /// Inline admission is layout-specific now; callers must not use this
    /// constant to decide whether an object is eligible for the fast path.
    #[deprecated(note = "inline eligibility is layout-specific; use persisted metadata and the read-path policy")]
    pub const INLINE_MAX_SIZE: i64 = 128 * 1024;

    /// Historical versioned inline size reference.
    ///
    /// Inline admission is layout-specific now; callers must not use this
    /// constant to decide whether an object is eligible for the fast path.
    #[deprecated(note = "inline eligibility is layout-specific; use persisted metadata and the read-path policy")]
    pub const INLINE_MAX_SIZE_VERSIONED: i64 = 16 * 1024;

    /// Returns `true` when this object qualifies for the inline data fast path.
    ///
    /// The inline fast path decodes erasure-coded data entirely in memory,
    /// bypassing disk I/O, duplex pipes, and the disk-read semaphore.
    ///
    /// The persisted `inlined` flag is the canonical size-policy decision. PUT
    /// sets it through the captured storage-class snapshot's effective policy,
    /// which is layout- and version-aware. Reapplying a fixed object-size limit
    /// here would disagree with that policy for wider EC layouts and explicit
    /// inline configurations. The direct-memory reader retains its own bounded
    /// 128 KiB allocation gate at the call site.
    ///
    /// Additional conditions:
    /// - Single part
    /// - Not encrypted
    /// - Not compressed
    /// - Not transitioned to remote tier
    pub fn is_inline_fast_path_eligible(&self) -> bool {
        if !self.inlined {
            return false;
        }
        self.parts.len() == 1
            && self.size >= 0
            && !self.is_encrypted()
            && !self.is_compressed()
            && self.transitioned_object.tier.is_empty()
    }

    pub fn encryption_original_size(&self) -> std::io::Result<Option<i64>> {
        rustfs_utils::http::get_object_encryption_original_size(&self.user_defined)
    }

    pub fn decrypted_size(&self) -> std::io::Result<i64> {
        Ok(self.encryption_original_size()?.unwrap_or(self.size))
    }

    pub fn get_actual_size(&self) -> std::io::Result<i64> {
        if self.actual_size < -1 || (self.actual_size == -1 && !self.is_compressed()) {
            return Err(std::io::Error::other("invalid negative actual size"));
        }
        if self.actual_size > 0 {
            return Ok(self.actual_size);
        }

        if self.is_compressed() {
            if let Some(size_str) = rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE)
                && !size_str.is_empty()
            {
                let size = size_str.parse::<i64>().map_err(|e| std::io::Error::other(e.to_string()))?;
                return Ok(size);
            }
            if self.actual_size == -1 && self.parts.is_empty() {
                return Ok(-1);
            }
            let mut actual_size = 0_i64;
            let mut unknown = false;
            for part in self.parts.iter() {
                match part.actual_size {
                    -1 => unknown = true,
                    size if size >= 0 => {
                        actual_size = actual_size
                            .checked_add(size)
                            .ok_or_else(|| std::io::Error::other("compressed actual size overflow"))?;
                    }
                    _ => return Err(std::io::Error::other("invalid negative compressed part size")),
                }
            }
            if unknown {
                return Ok(-1);
            }
            if actual_size == 0 && actual_size != self.size {
                return Err(std::io::Error::other(format!("invalid decompressed size {} {}", actual_size, self.size)));
            }

            return Ok(actual_size);
        }

        if let Some(size) = self.encryption_original_size()? {
            return Ok(size);
        }

        Ok(self.size)
    }

    /// Returns a non-negative size for client and replication boundaries.
    ///
    /// Compressed legacy metadata can retain the internal `-1` unknown-size
    /// sentinel. Those boundaries cannot emit a negative length, so they use
    /// the persisted physical size while quota accounting keeps the sentinel
    /// distinction in [`crate::data_usage::quota_object_size`].
    pub fn get_actual_size_or_physical(&self) -> i64 {
        self.get_actual_size()
            .map(|size| if size >= 0 { size } else { self.size.max(0) })
            .unwrap_or_else(|_| self.size.max(0))
    }

    pub fn from_file_info(fi: &FileInfo, bucket: &str, object: &str, versioned: bool) -> ObjectInfo {
        let mut version_id = fi.version_id;

        if versioned && version_id.is_none() {
            version_id = Some(Uuid::nil())
        }

        Self::from_file_info_with_version_id(fi, bucket, object, version_id)
    }

    pub(crate) fn from_file_info_with_version_id(
        fi: &FileInfo,
        bucket: &str,
        object: &str,
        version_id: Option<Uuid>,
    ) -> ObjectInfo {
        let name = decode_dir_object(object);

        // etag
        let (content_type, content_encoding, etag) = {
            let content_type = fi.metadata.get("content-type").cloned();
            let content_encoding = fi.metadata.get("content-encoding").cloned();
            let etag = fi.metadata.get("etag").cloned();

            (content_type, content_encoding, etag)
        };

        // tags
        let user_tags: Arc<String> = fi
            .metadata
            .get(AMZ_OBJECT_TAGGING)
            .map(|s| Arc::new(s.clone()))
            .unwrap_or_default();

        let inlined = fi.inline_data();

        // Parse expires from metadata (HTTP date format RFC 7231 or ISO 8601)
        let expires = fi.metadata.get("expires").and_then(|s| {
            // Try parsing as ISO 8601 first
            OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
                .or_else(|_| {
                    // Try RFC 2822 format
                    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc2822)
                })
                .or_else(|_| {
                    // Try RFC 3339 format
                    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                })
                .ok()
        });

        let replication_status_internal = fi
            .replication_state_internal
            .as_ref()
            .and_then(|v| v.replication_status_internal.clone());
        let version_purge_status_internal = fi
            .replication_state_internal
            .as_ref()
            .and_then(|v| v.version_purge_status_internal.clone());
        let replication_decision = fi
            .replication_state_internal
            .as_ref()
            .map(|v| v.replicate_decision_str.clone())
            .unwrap_or_default();

        let mut replication_status = replication_status_from_filemeta(fi.replication_status());
        if replication_status.is_empty()
            && let Some(status) = fi.metadata.get(AMZ_BUCKET_REPLICATION_STATUS).cloned()
            && status == ReplicationStatusType::Replica.as_str()
        {
            replication_status = ReplicationStatusType::Replica;
        }

        let version_purge_status = version_purge_status_from_filemeta(fi.version_purge_status());

        let transitioned_object = TransitionedObject {
            name: fi.transitioned_objname.clone(),
            version_id: fi
                .transition_version
                .clone()
                .or_else(|| fi.transition_version_id.map(|version_id| version_id.to_string()))
                .unwrap_or_default(),
            status: fi.transition_status.clone(),
            free_version: fi.tier_free_version(),
            tier: fi.transition_tier.clone(),
        };

        let metadata = {
            let mut v = fi.metadata.clone();
            clean_metadata(&mut v);
            v
        };

        let storage_class = Some(
            storageclass::effective_class(
                fi.metadata.get(AMZ_STORAGE_CLASS).map(String::as_str),
                (fi.transition_status == rustfs_filemeta::TRANSITION_COMPLETE && !fi.transition_tier.is_empty())
                    .then_some(fi.transition_tier.as_str()),
            )
            .to_string(),
        );

        let mut restore_ongoing = false;
        let mut restore_expires = None;
        if let Some(restore_status) = fi.metadata.get(AMZ_RESTORE).cloned()
            && let Ok(restore_status) = parse_restore_obj_status(&restore_status)
        {
            restore_ongoing = restore_status.on_going();
            restore_expires = restore_status.expiry();
        }

        // Convert parts from rustfs_filemeta::ObjectPartInfo to object_api::ObjectPartInfo
        let parts = fi
            .parts
            .iter()
            .map(|part| ObjectPartInfo {
                etag: part.etag.clone(),
                index: part.index.clone(),
                size: part.size,
                actual_size: part.actual_size,
                mod_time: part.mod_time,
                checksums: part.checksums.clone(),
                number: part.number,
                error: part.error.clone(),
            })
            .collect::<Vec<_>>();

        ObjectInfo {
            bucket: bucket.to_string(),
            name,
            is_dir: object.starts_with('/'),
            parity_blocks: fi.erasure.parity_blocks,
            data_blocks: fi.erasure.data_blocks,
            version_id,
            data_dir: fi.data_dir,
            delete_marker: fi.deleted,
            mod_time: fi.mod_time,
            size: fi.size,
            parts: Arc::new(parts),
            is_latest: fi.is_latest,
            user_tags,
            content_type,
            content_encoding,
            expires,
            num_versions: fi.num_versions,
            successor_mod_time: fi.successor_mod_time,
            etag,
            inlined,
            user_defined: Arc::new(metadata),
            transitioned_object,
            transition_version_state: fi.transition_version_state,
            checksum: fi.checksum.clone(),
            storage_class,
            restore_ongoing,
            restore_expires,
            replication_status_internal,
            replication_status,
            version_purge_status_internal,
            version_purge_status,
            replication_decision,
            ..Default::default()
        }
    }

    pub async fn from_meta_cache_entries_sorted_versions(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_marker: Option<VersionMarker>,
    ) -> Vec<ObjectInfo> {
        Self::from_meta_cache_entries_sorted_versions_with_purge(
            entries,
            bucket,
            prefix,
            delimiter,
            after_version_marker,
            false,
            false,
        )
        .await
        .0
    }

    pub(crate) async fn from_meta_cache_entries_sorted_versions_for_lifecycle(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_marker: Option<VersionMarker>,
    ) -> Vec<ObjectInfo> {
        Self::from_meta_cache_entries_sorted_versions_with_purge(
            entries,
            bucket,
            prefix,
            delimiter,
            after_version_marker,
            true,
            false,
        )
        .await
        .0
    }

    pub(crate) async fn from_meta_cache_entries_sorted_versions_for_recursive_delete(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_marker: Option<VersionMarker>,
    ) -> Result<Vec<ObjectInfo>> {
        let (objects, error) = Self::from_meta_cache_entries_sorted_versions_with_purge(
            entries,
            bucket,
            prefix,
            delimiter,
            after_version_marker,
            true,
            true,
        )
        .await;
        match error {
            Some(error) => Err(error),
            None => Ok(objects),
        }
    }

    async fn from_meta_cache_entries_sorted_versions_with_purge(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_marker: Option<VersionMarker>,
        include_version_purge: bool,
        fail_on_decode_error: bool,
    ) -> (Vec<ObjectInfo>, Option<Error>) {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(entries.entries().len());
        let mut prev_prefix = "";
        let mut after_version_marker = after_version_marker;
        for entry in entries.entries() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    if let Some(idx) = remaining.find(delimiter.as_str()) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let file_infos = match entry.file_info_versions(bucket) {
                    Ok(res) => res,
                    Err(err) => {
                        if fail_on_decode_error {
                            return (objects, Some(err.into()));
                        }
                        warn!("file_info_versions err {:?}", err);
                        continue;
                    }
                };

                let versions = if let Some(marker) = after_version_marker.take() {
                    versions_after_marker(&file_infos, marker)
                } else {
                    &file_infos.versions
                };

                for fi in versions.iter() {
                    if !include_version_purge && !fi.version_purge_status().is_empty() {
                        continue;
                    }

                    let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                    objects.push(ObjectInfo::from_file_info(fi, bucket, &entry.name, versioned));
                }
                continue;
            }

            if entry.is_dir()
                && let Some(delimiter) = &delimiter
                && let Some(idx) = {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    remaining.find(delimiter.as_str())
                }
            {
                let idx = prefix.len() + idx + delimiter.len();
                if let Some(curr_prefix) = entry.name.get(0..idx) {
                    if curr_prefix == prev_prefix {
                        continue;
                    }

                    prev_prefix = curr_prefix;

                    objects.push(ObjectInfo {
                        is_dir: true,
                        bucket: bucket.to_owned(),
                        name: curr_prefix.to_owned(),
                        ..Default::default()
                    });
                }
            }
        }

        (objects, None)
    }

    pub async fn from_meta_cache_entries_sorted_infos(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
    ) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(entries.entries().len());
        let mut prev_prefix = "";
        for entry in entries.entries() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    if let Some(idx) = remaining.find(delimiter.as_str()) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let fi = match entry.to_fileinfo(bucket) {
                    Ok(res) => res,
                    Err(err) => {
                        warn!("file_info_versions err {:?}", err);
                        continue;
                    }
                };

                // TODO(backlog): handle VersionPurgeStatus in object listing
                let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                objects.push(ObjectInfo::from_file_info(&fi, bucket, &entry.name, versioned));

                continue;
            }

            if entry.is_dir()
                && let Some(delimiter) = &delimiter
                && let Some(idx) = {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    remaining.find(delimiter.as_str())
                }
            {
                let idx = prefix.len() + idx + delimiter.len();
                if let Some(curr_prefix) = entry.name.get(0..idx) {
                    if curr_prefix == prev_prefix {
                        continue;
                    }

                    prev_prefix = curr_prefix;

                    objects.push(ObjectInfo {
                        is_dir: true,
                        bucket: bucket.to_owned(),
                        name: curr_prefix.to_owned(),
                        ..Default::default()
                    });
                }
            }
        }

        objects
    }

    pub fn replication_state(&self) -> ReplicationState {
        // Derived from the durable internal keys, not from the wire form: the
        // state's positional encoding skips this map.
        let (target_delete_marker_version_ids, target_delete_marker_version_ids_corrupt) =
            rustfs_utils::http::target_delete_marker_versions(&self.user_defined);
        ReplicationState {
            replication_status_internal: self.replication_status_internal.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            replicate_decision_str: self.replication_decision.clone(),
            targets: replication_statuses_map(self.replication_status_internal.clone().unwrap_or_default().as_str()),
            purge_targets: version_purge_statuses_map(self.version_purge_status_internal.clone().unwrap_or_default().as_str()),
            reset_statuses_map: self
                .user_defined
                .iter()
                .filter_map(|(k, v)| {
                    rustfs_utils::http::internal_key_strip_suffix_prefix(
                        k,
                        rustfs_utils::http::SUFFIX_REPLICATION_RESET_ARN_PREFIX,
                    )
                    .map(|arn| (arn, v.clone()))
                })
                .collect(),
            target_delete_marker_version_ids,
            target_delete_marker_version_ids_corrupt,
            ..Default::default()
        }
    }

    pub fn target_replication_status(&self, arn: &str) -> ReplicationStatusType {
        self.replication_status_internal
            .as_deref()
            .unwrap_or_default()
            .split(';')
            .find_map(|entry| {
                let (target_arn, status) = entry.split_once('=')?;
                (!target_arn.is_empty() && target_arn == arn).then(|| ReplicationStatusType::from(status))
            })
            .unwrap_or_default()
    }

    pub fn decrypt_checksums(&self, part: usize, _headers: &HeaderMap) -> Result<(HashMap<String, String>, bool)> {
        if part > 0
            && let Some(checksums) = self.parts.iter().find(|p| p.number == part).and_then(|p| p.checksums.clone())
        {
            return Ok((checksums, true));
        }

        if let Some(data) = &self.checksum {
            if self.is_encrypted() && get_consistent_str(&self.user_defined, SUFFIX_PLAINTEXT_CHECKSUM) != Some("true") {
                // Object-level encrypted checksum bytes require SSE decrypt material,
                // unless RustFS marked the stored bytes as plaintext. Do not expose
                // unmarked bytes as checksum headers here. The
                // `false` multipart flag feeds the response-path COMPOSITE
                // fallback; callers that need accurate multipart routing must
                // consult `is_multipart()` instead of this value.
                return Ok((HashMap::new(), false));
            }

            let (checksums, is_multipart) = rustfs_rio::read_checksums(data.as_ref(), 0);
            return Ok((checksums, is_multipart));
        }

        Ok((HashMap::new(), false))
    }
}

fn versions_after_marker(file_infos: &rustfs_filemeta::FileInfoVersions, marker: VersionMarker) -> &[FileInfo] {
    let marker_idx = match marker {
        VersionMarker::Null => file_infos.versions.iter().position(|version| version.version_id.is_none()),
        VersionMarker::Version(vid) => file_infos.find_version_index(vid),
    };

    marker_idx
        .map(|idx| &file_infos.versions[idx + 1..])
        .unwrap_or(&file_infos.versions)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_status_writeback_condition_remains_indirected() {
        fn assert_indirected(_: &Option<Box<ReplicationStatusWritebackCondition>>) {}

        assert_indirected(&ObjectOptions::default().replication_status_writeback);
    }

    #[test]
    fn object_lock_config_snapshot_is_bound_to_store_bucket_and_incarnation() {
        let store_id = Uuid::new_v4();
        let incarnation_id = Uuid::new_v4();
        let snapshot = ObjectLockConfigSnapshot::for_store_bucket(
            store_id,
            "source-bucket",
            incarnation_id,
            OffsetDateTime::UNIX_EPOCH,
            crate::bucket::metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
        );

        assert!(snapshot.is_for_store_bucket(store_id, "source-bucket", incarnation_id, OffsetDateTime::UNIX_EPOCH));
        assert!(!snapshot.is_for_store_bucket(Uuid::new_v4(), "source-bucket", incarnation_id, OffsetDateTime::UNIX_EPOCH));
        assert!(!snapshot.is_for_store_bucket(store_id, "other-bucket", incarnation_id, OffsetDateTime::UNIX_EPOCH));
        assert!(!snapshot.is_for_store_bucket(store_id, "source-bucket", Uuid::new_v4(), OffsetDateTime::UNIX_EPOCH));
        assert!(!snapshot.is_for_store_bucket(store_id, "source-bucket", incarnation_id, OffsetDateTime::now_utc()));
    }
    use rustfs_filemeta::{FileInfo, FileMeta, MetaCacheEntry, TRANSITION_COMPLETE};

    #[test]
    fn multipart_identity_uses_stored_parts_and_preserves_the_etag_fallback() {
        let plain_etag = "0123456789abcdef0123456789abcdef";
        let multipart_etag = "0123456789abcdef0123456789abcdef-1";
        for (case, part_count, etag, expected) in [
            ("preserved source ETag", 2, Some(plain_etag), true),
            ("missing ETag", 2, None, true),
            ("ordinary PUT", 1, Some(plain_etag), false),
            ("ordinary PUT without ETag", 1, None, false),
            ("single-part MPU", 1, Some(multipart_etag), true),
            ("legacy MPU without parts", 0, Some(multipart_etag), true),
        ] {
            let object = ObjectInfo {
                etag: etag.map(str::to_string),
                parts: Arc::new(
                    (1..=part_count)
                        .map(|number| ObjectPartInfo {
                            number,
                            ..Default::default()
                        })
                        .collect(),
                ),
                ..Default::default()
            };

            assert_eq!(object.is_multipart(), expected, "{case}");
        }
    }

    fn inline_fast_path_object(size: i64, versioned: bool) -> ObjectInfo {
        ObjectInfo {
            size,
            inlined: true,
            version_id: versioned.then(|| Uuid::from_u128(1)),
            parts: Arc::new(vec![ObjectPartInfo::default()]),
            ..Default::default()
        }
    }

    #[test]
    fn inline_fast_path_eligibility_follows_persisted_marker() {
        for (case, size, versioned, expected) in [
            ("unversioned below", 128 * 1024 - 1, false, true),
            ("unversioned exact", 128 * 1024, false, true),
            ("unversioned above", 128 * 1024 + 1, false, true),
            ("versioned below", 16 * 1024 - 1, true, true),
            ("versioned exact", 16 * 1024, true, true),
            ("versioned above", 16 * 1024 + 1, true, true),
        ] {
            assert_eq!(
                inline_fast_path_object(size, versioned).is_inline_fast_path_eligible(),
                expected,
                "{case}: object_size={size}, versioned={versioned}"
            );
        }
    }

    #[test]
    fn inline_fast_path_marker_allows_ec8_and_ec12_layout_specific_256kib_objects() {
        for data_blocks in [8, 12] {
            let object = ObjectInfo {
                size: 256 * 1024,
                data_blocks,
                parity_blocks: 4,
                inlined: true,
                version_id: Some(Uuid::from_u128(1)),
                parts: Arc::new(vec![ObjectPartInfo::default()]),
                ..Default::default()
            };

            assert!(
                object.is_inline_fast_path_eligible(),
                "the persisted inline marker must be authoritative for EC{data_blocks}+4"
            );
        }
    }

    #[test]
    fn inline_fast_path_eligibility_rejects_incompatible_object_shapes() {
        let mut object = inline_fast_path_object(128 * 1024, false);

        object.inlined = false;
        assert!(!object.is_inline_fast_path_eligible(), "non-inline objects must fall back");

        object.inlined = true;
        object.parts = Arc::new(vec![ObjectPartInfo::default(), ObjectPartInfo::default()]);
        assert!(!object.is_inline_fast_path_eligible(), "multipart objects must fall back");

        object.parts = Arc::new(vec![ObjectPartInfo::default()]);
        object.user_defined = Arc::new(HashMap::from([("x-amz-server-side-encryption".to_string(), "AES256".to_string())]));
        assert!(!object.is_inline_fast_path_eligible(), "encrypted objects must fall back");

        object.user_defined = Arc::new(HashMap::from([(
            rustfs_utils::http::internal_key_rustfs(rustfs_utils::http::SUFFIX_COMPRESSION),
            "zstd".to_string(),
        )]));
        assert!(!object.is_inline_fast_path_eligible(), "compressed objects must fall back");

        object.user_defined = Arc::default();
        object.transitioned_object.tier = "remote-tier".to_string();
        assert!(!object.is_inline_fast_path_eligible(), "transitioned objects must fall back");
    }

    #[test]
    fn minio_internal_encryption_metadata_is_not_treated_as_plaintext() {
        let object = ObjectInfo {
            user_defined: Arc::new(HashMap::from([(
                "X-Minio-Internal-Server-Side-Encryption-Sealed-Key".to_string(),
                "sealed".to_string(),
            )])),
            ..Default::default()
        };

        assert!(object.is_encrypted());
    }

    #[test]
    fn versions_after_marker_handles_null_version_marker() {
        let first_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let last_version = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let file_infos = rustfs_filemeta::FileInfoVersions {
            versions: vec![
                FileInfo {
                    version_id: Some(first_version),
                    ..Default::default()
                },
                FileInfo {
                    version_id: None,
                    ..Default::default()
                },
                FileInfo {
                    version_id: Some(last_version),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let versions = versions_after_marker(&file_infos, VersionMarker::Null);

        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id, Some(last_version));
    }

    #[test]
    fn put_replication_state_preserves_replica_status() {
        let opts = ObjectOptions {
            delete_replication: Some(ReplicationState {
                replica_status: ReplicationStatusType::Replica,
                ..Default::default()
            }),
            ..Default::default()
        };

        let state = opts.put_replication_state();

        assert_eq!(state.composite_replication_status(), ReplicationStatusType::Replica);
    }

    #[test]
    fn object_info_replication_helpers_parse_target_status_and_reset_headers() {
        let reset_key = rustfs_utils::http::internal_key_rustfs("replication-reset-arn:target-a");
        let user_defined = HashMap::from([(reset_key, "reset-id".to_string())]);
        let object = ObjectInfo {
            replication_status_internal: Some("arn:target-a=COMPLETED;arn:target-b=FAILED;".to_string()),
            version_purge_status_internal: Some("arn:target-a=PENDING;".to_string()),
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        let state = object.replication_state();

        assert_eq!(object.target_replication_status("arn:target-a"), ReplicationStatusType::Completed);
        assert_eq!(object.target_replication_status("arn:target-b"), ReplicationStatusType::Failed);
        assert_eq!(object.target_replication_status("arn:missing"), ReplicationStatusType::Empty);
        assert_eq!(state.targets.get("arn:target-b"), Some(&ReplicationStatusType::Failed));
        assert_eq!(state.purge_targets.get("arn:target-a"), Some(&VersionPurgeStatusType::Pending));
        assert_eq!(state.reset_statuses_map.get("arn:target-a"), Some(&"reset-id".to_string()));
    }

    #[test]
    fn versions_after_marker_handles_uuid_version_marker() {
        let first_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let last_version = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let file_infos = rustfs_filemeta::FileInfoVersions {
            versions: vec![
                FileInfo {
                    version_id: Some(first_version),
                    ..Default::default()
                },
                FileInfo {
                    version_id: None,
                    ..Default::default()
                },
                FileInfo {
                    version_id: Some(last_version),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let versions = versions_after_marker(&file_infos, VersionMarker::Version(first_version));

        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version_id, None);
        assert_eq!(versions[1].version_id, Some(last_version));
    }

    #[test]
    fn versions_after_marker_preserves_stale_marker_compatibility() {
        let existing_version =
            Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("existing version UUID should parse");
        let deleted_marker = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").expect("delete marker UUID should parse");
        let file_infos = rustfs_filemeta::FileInfoVersions {
            versions: vec![FileInfo {
                version_id: Some(existing_version),
                ..Default::default()
            }],
            ..Default::default()
        };

        let versions = versions_after_marker(&file_infos, VersionMarker::Version(deleted_marker));

        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id, Some(existing_version));
    }

    #[tokio::test]
    async fn versions_listing_applies_version_marker_only_to_first_entry() {
        let metadata = rustfs_filemeta::test_data::create_real_xlmeta().expect("test metadata should be valid");
        let entries = MetaCacheEntriesSorted {
            o: rustfs_filemeta::MetaCacheEntries(vec![
                Some(rustfs_filemeta::MetaCacheEntry {
                    name: "obj-a".to_owned(),
                    metadata: metadata.clone(),
                    ..Default::default()
                }),
                Some(rustfs_filemeta::MetaCacheEntry {
                    name: "obj-b".to_owned(),
                    metadata,
                    ..Default::default()
                }),
            ]),
            ..Default::default()
        };
        let marker_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();

        let objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &entries,
            "bucket",
            "",
            None,
            Some(VersionMarker::Version(marker_version)),
        )
        .await;

        let obj_a_count = objects.iter().filter(|object| object.name == "obj-a").count();
        let obj_b_count = objects.iter().filter(|object| object.name == "obj-b").count();

        assert_eq!(obj_a_count, 2);
        assert_eq!(obj_b_count, 3);
        assert_eq!(objects.len(), 5);
    }

    #[tokio::test]
    async fn versions_listing_excludes_tier_free_versions_from_delete_marker_count() {
        let object_version_id = Uuid::new_v4();
        let remote_version_id = Uuid::new_v4();
        let free_version_id = Uuid::new_v4();
        let delete_marker_id = Uuid::new_v4();
        let base_time = OffsetDateTime::now_utc();
        let mut fm = FileMeta::new();

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version_id: Some(remote_version_id),
            transition_tier: "WARM".to_string(),
            mod_time: Some(base_time),
            ..Default::default()
        })
        .expect("transitioned object version should be added");

        let mut delete_fi = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_version_id),
            mod_time: Some(base_time),
            ..Default::default()
        };
        delete_fi.set_tier_free_version_id(&free_version_id.to_string());
        fm.delete_version(&delete_fi)
            .expect("transitioned delete should create a free-version record");

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(delete_marker_id),
            deleted: true,
            mod_time: Some(base_time + time::Duration::seconds(1)),
            ..Default::default()
        })
        .expect("delete marker should be added");

        let entries = MetaCacheEntriesSorted {
            o: rustfs_filemeta::MetaCacheEntries(vec![Some(MetaCacheEntry {
                name: "object".to_string(),
                metadata: fm.marshal_msg().expect("metadata should marshal"),
                ..Default::default()
            })]),
            ..Default::default()
        };

        let objects = ObjectInfo::from_meta_cache_entries_sorted_versions(&entries, "bucket", "", None, None).await;

        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].name, "object");
        assert!(objects[0].delete_marker);
        assert!(objects[0].is_latest);
        assert_eq!(objects[0].num_versions, 1);
    }

    #[tokio::test]
    async fn lifecycle_versions_listing_preserves_purge_pending_versions() {
        let visible_version_id = Uuid::new_v4();
        let purge_version_id = Uuid::new_v4();
        let base_time = OffsetDateTime::now_utc();
        let mut fm = FileMeta::new();

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(purge_version_id),
            mod_time: Some(base_time),
            ..Default::default()
        })
        .expect("version pending purge should be added");
        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(visible_version_id),
            mod_time: Some(base_time + time::Duration::seconds(1)),
            ..Default::default()
        })
        .expect("visible version should be added");
        fm.delete_version(&FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(purge_version_id),
            replication_state_internal: Some(crate::bucket::replication::replication_state_to_filemeta(&ReplicationState {
                version_purge_status_internal: Some("arn:target-a=PENDING;".to_string()),
                purge_targets: version_purge_statuses_map("arn:target-a=PENDING;"),
                ..Default::default()
            })),
            ..Default::default()
        })
        .expect("version purge status should be persisted");

        let entries = MetaCacheEntriesSorted {
            o: rustfs_filemeta::MetaCacheEntries(vec![Some(MetaCacheEntry {
                name: "object".to_string(),
                metadata: fm.marshal_msg().expect("metadata should marshal"),
                ..Default::default()
            })]),
            ..Default::default()
        };

        let public_objects = ObjectInfo::from_meta_cache_entries_sorted_versions(&entries, "bucket", "", None, None).await;
        let lifecycle_objects =
            ObjectInfo::from_meta_cache_entries_sorted_versions_for_lifecycle(&entries, "bucket", "", None, None).await;

        assert_eq!(public_objects.len(), 1);
        assert_eq!(public_objects[0].version_id, Some(visible_version_id));
        assert_eq!(public_objects[0].num_versions, 2);
        assert_eq!(lifecycle_objects.len(), 2);
        assert!(
            lifecycle_objects
                .iter()
                .any(|object| object.version_purge_status == VersionPurgeStatusType::Pending)
        );
        assert!(lifecycle_objects.iter().all(|object| object.num_versions == 2));
    }

    #[test]
    fn get_actual_size_prefers_actual_size_field() {
        let info = ObjectInfo {
            size: 5,
            actual_size: 10,
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 10);
    }

    #[test]
    fn get_actual_size_uses_compressed_metadata_size() {
        let user_defined = {
            let mut map = HashMap::new();
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "42".to_string());
            map
        };

        let info = ObjectInfo {
            size: 100,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 42);
    }

    #[test]
    fn get_actual_size_falls_back_to_encrypted_original_size_metadata() {
        let user_defined = {
            let mut map = HashMap::new();
            map.insert("x-amz-server-side-encryption-customer-original-size".to_string(), "77".to_string());
            map
        };

        let info = ObjectInfo {
            size: 100,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 77);
    }

    #[test]
    fn precondition_check_ignores_empty_etag_conditions() {
        let opts = ObjectOptions {
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(String::new()),
                if_none_match: Some(" ".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = ObjectInfo {
            mod_time: Some(OffsetDateTime::now_utc()),
            etag: Some("\"abc\"".to_string()),
            ..Default::default()
        };

        assert!(opts.precondition_check(&info).is_ok());
    }

    #[test]
    fn from_file_info_preserves_replication_decision() {
        let fi = FileInfo {
            replication_state_internal: Some(crate::bucket::replication::replication_state_to_filemeta(&ReplicationState {
                replicate_decision_str: "arn=true;false;arn:replication::1:dest;rule-id".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        };

        let info = ObjectInfo::from_file_info(&fi, "bucket", "object", true);

        assert_eq!(info.replication_decision, "arn=true;false;arn:replication::1:dest;rule-id");
    }

    #[test]
    fn from_file_info_with_version_id_keeps_normalized_absent_version() {
        let fi = FileInfo {
            version_id: Some(Uuid::new_v4()),
            ..Default::default()
        };

        let info = ObjectInfo::from_file_info_with_version_id(&fi, "bucket", "object", None);

        assert_eq!(info.version_id, None, "a normalized absent version must not be rewritten to nil");
    }

    #[test]
    fn from_file_info_reports_effective_storage_class_for_legacy_metadata() {
        for legacy_label in [
            storageclass::STANDARD_IA,
            storageclass::ONEZONE_IA,
            storageclass::INTELLIGENT_TIERING,
            storageclass::GLACIER,
        ] {
            let fi = FileInfo {
                metadata: HashMap::from([(AMZ_STORAGE_CLASS.to_string(), legacy_label.to_string())]),
                ..Default::default()
            };

            let info = ObjectInfo::from_file_info(&fi, "bucket", "legacy-object", true);

            assert_eq!(
                info.storage_class.as_deref(),
                Some(storageclass::STANDARD),
                "{legacy_label} was only a label and must report the effective STANDARD layout"
            );
        }
    }

    #[test]
    fn from_file_info_preserves_transitioned_tier_storage_class() {
        let fi = FileInfo {
            metadata: HashMap::from([(AMZ_STORAGE_CLASS.to_string(), storageclass::STANDARD_IA.to_string())]),
            transition_tier: "WARM-TIER".to_string(),
            transition_status: TRANSITION_COMPLETE.to_string(),
            ..Default::default()
        };

        let info = ObjectInfo::from_file_info(&fi, "bucket", "transitioned-object", true);

        assert_eq!(info.storage_class.as_deref(), Some("WARM-TIER"));
        assert_eq!(info.transitioned_object.tier, "WARM-TIER");
    }

    #[test]
    fn from_file_info_ignores_a_tier_name_without_a_completed_transition() {
        let fi = FileInfo {
            metadata: HashMap::from([(AMZ_STORAGE_CLASS.to_string(), storageclass::STANDARD_IA.to_string())]),
            transition_tier: "WARM-TIER".to_string(),
            ..Default::default()
        };

        let info = ObjectInfo::from_file_info(&fi, "bucket", "incomplete-transition", true);

        assert_eq!(info.storage_class.as_deref(), Some(storageclass::STANDARD));
        assert_eq!(info.transitioned_object.tier, "WARM-TIER");
    }

    #[test]
    fn get_actual_size_uses_compressed_parts_actual_size_when_metadata_missing() {
        let user_defined = {
            let mut map = HashMap::new();
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            map
        };

        let info = ObjectInfo {
            size: 12,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            parts: Arc::new(vec![
                ObjectPartInfo {
                    actual_size: 4,
                    ..Default::default()
                },
                ObjectPartInfo {
                    actual_size: 5,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 9);
    }

    #[test]
    fn get_actual_size_returns_error_when_compressed_parts_missing_and_size_mismatch() {
        let user_defined = {
            let mut map = HashMap::new();
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            map
        };

        let info = ObjectInfo {
            size: 12,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(info.get_actual_size().is_err());
    }

    #[test]
    fn is_encrypted_correct_for_old_version_fileinfo() {
        let mut user_defined: HashMap<String, String> = HashMap::new();

        let metadata = vec![
            ("content-type", "text/plain"),
            ("etag", "e4336b5de4e2180a53fe2e17d03abe4f-4"),
            ("x-minio-internal-actual-size", "67108864"),
            ("x-rustfs-encryption-original-size", "67108864"),
            ("x-rustfs-internal-actual-size", "67108864"),
        ];

        metadata.into_iter().for_each(|(key, value)| {
            user_defined.insert(key.to_string(), value.to_string());
        });

        let info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(!info.is_encrypted());
    }

    #[test]
    fn is_encrypted_returns_true_when_encryption_metadata_present() {
        let mut user_defined: HashMap<String, String> = HashMap::new();

        let metadata = vec![
            ("content-type", "text/plain"),
            ("etag", "f1c9645dbc14efddc7d8a322685f26eb"),
            ("x-amz-server-side-encryption", "AES256"),
            ("x-rustfs-encryption-algorithm", "AES256"),
            ("x-rustfs-encryption-iv", "Fb9moBlEBRE0D14F"),
            (
                "x-rustfs-encryption-key",
                "QUFBQUFBQUFBQUFBQUFBQTpZQk5sNnNJdmJHWWl3QmxZbCtsMTJlVlZCeXVoVml4UlV4b3JPbTNoRk5odUlYVnBPdlpXNWVyT0FTcklXMWJr",
            ),
            ("x-rustfs-encryption-key-id", "default"),
            ("x-rustfs-encryption-original-size", "10485760"),
        ];

        metadata.into_iter().for_each(|(key, value)| {
            user_defined.insert(key.to_string(), value.to_string());
        });

        let info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(info.is_encrypted());
    }

    #[test]
    fn is_encrypted_handles_case_insensitive_rustfs_metadata_keys() {
        let mut user_defined: HashMap<String, String> = HashMap::new();
        user_defined.insert("X-Rustfs-Encryption-Key".to_string(), "encrypted-key".to_string());

        let info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(info.is_encrypted());
    }

    #[test]
    fn decrypt_checksums_reads_plain_object_checksum() {
        let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"plain-object")
            .expect("test checksum should be valid");
        let checksum_key = checksum.checksum_type.to_string();
        let expected_checksum = checksum.encoded.clone();
        let info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            ..Default::default()
        };

        let (checksums, is_multipart) = info
            .decrypt_checksums(0, &HeaderMap::new())
            .expect("plain checksum should decode");

        assert!(!is_multipart);
        assert_eq!(checksums.get(&checksum_key), Some(&expected_checksum));
    }

    #[test]
    fn decrypt_checksums_hides_encrypted_object_checksum_without_decrypt_material() {
        let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"encrypted-object")
            .expect("test checksum should be valid");
        let info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            user_defined: Arc::new(HashMap::from([(
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                "AES256".to_string(),
            )])),
            ..Default::default()
        };

        let (checksums, is_multipart) = info
            .decrypt_checksums(0, &HeaderMap::new())
            .expect("encrypted checksum should fail closed");

        assert!(!is_multipart);
        assert!(checksums.is_empty());
    }

    #[test]
    fn decrypt_checksums_reads_marked_rustfs_encrypted_object_checksum() {
        let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"encrypted-object")
            .expect("test checksum should be valid");
        let checksum_key = checksum.checksum_type.to_string();
        let expected_checksum = checksum.encoded.clone();
        let mut user_defined =
            HashMap::from([(rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "AES256".to_string())]);
        rustfs_utils::http::insert_str(&mut user_defined, SUFFIX_PLAINTEXT_CHECKSUM, "true".to_string());
        assert_eq!(user_defined.get("x-rustfs-internal-plaintext-checksum").map(String::as_str), Some("true"));
        assert_eq!(user_defined.get("x-minio-internal-plaintext-checksum").map(String::as_str), Some("true"));
        let info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        let (checksums, is_multipart) = info
            .decrypt_checksums(0, &HeaderMap::new())
            .expect("marked RustFS checksum should decode");

        assert!(!is_multipart);
        assert_eq!(checksums.get(&checksum_key), Some(&expected_checksum));
    }

    #[test]
    fn decrypt_checksums_keeps_encrypted_multipart_flag_false_for_response_paths() {
        let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"encrypted-object")
            .expect("test checksum should be valid");
        let info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            // Multipart ETag shape: md5-of-md5s with a part-count suffix.
            etag: Some("0123456789abcdef0123456789abcdef-3".to_string()),
            user_defined: Arc::new(HashMap::from([(
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                "AES256".to_string(),
            )])),
            ..Default::default()
        };

        let (checksums, is_multipart) = info
            .decrypt_checksums(0, &HeaderMap::new())
            .expect("encrypted checksum should fail closed");

        // The response path infers COMPOSITE from is_multipart=true when the
        // checksum type is unreadable, so encrypted objects must keep the
        // flag false here even when the object itself is multipart. Callers
        // that need routing (replication) consult is_multipart() directly.
        assert!(checksums.is_empty());
        assert!(!is_multipart);
        assert!(info.is_multipart());
    }

    #[test]
    fn decrypt_checksums_keeps_encrypted_part_checksum_metadata() {
        let checksum = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"encrypted-object")
            .expect("test checksum should be valid");
        let part_checksums = HashMap::from([("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string())]);
        let info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            user_defined: Arc::new(HashMap::from([(
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                "AES256".to_string(),
            )])),
            parts: Arc::new(vec![ObjectPartInfo {
                number: 2,
                checksums: Some(part_checksums.clone()),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let (checksums, is_multipart) = info
            .decrypt_checksums(2, &HeaderMap::new())
            .expect("part checksum metadata should remain readable");

        assert!(is_multipart);
        assert_eq!(checksums, part_checksums);
    }

    #[test]
    fn objectinfo_clone_shares_arc_data_and_is_correct() {
        let mut ud = HashMap::new();
        ud.insert("content-type".to_string(), "application/octet-stream".to_string());
        ud.insert("x-custom-header".to_string(), "custom-value".to_string());

        let original = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "test-object".to_string(),
            user_defined: Arc::new(ud),
            user_tags: Arc::new("env=prod&team=storage".to_string()),
            parts: Arc::new(vec![
                ObjectPartInfo {
                    number: 1,
                    size: 1024,
                    actual_size: 1024,
                    ..Default::default()
                },
                ObjectPartInfo {
                    number: 2,
                    size: 512,
                    actual_size: 512,
                    ..Default::default()
                },
            ]),
            size: 1536,
            etag: Some("abc123".to_string()),
            ..Default::default()
        };

        let cloned = original.clone();

        // Verify cloned values are correct
        assert_eq!(cloned.bucket, "test-bucket");
        assert_eq!(cloned.name, "test-object");
        assert_eq!(cloned.size, 1536);
        assert_eq!(cloned.etag, Some("abc123".to_string()));

        // Verify Arc fields share the same allocation
        assert!(Arc::ptr_eq(&original.user_defined, &cloned.user_defined));
        assert!(Arc::ptr_eq(&original.user_tags, &cloned.user_tags));
        assert!(Arc::ptr_eq(&original.parts, &cloned.parts));

        // Verify Arc-wrapped data is accessible through the clone
        assert_eq!(
            cloned.user_defined.get("content-type").map(String::as_str),
            Some("application/octet-stream")
        );
        assert_eq!(cloned.user_tags.as_str(), "env=prod&team=storage");
        assert_eq!(cloned.parts.len(), 2);
        assert_eq!(cloned.parts[0].number, 1);
        assert_eq!(cloned.parts[1].size, 512);

        // Verify default ObjectInfo clone also works
        let default_obj = ObjectInfo::default();
        let default_cloned = default_obj.clone();
        assert!(default_obj.user_defined.is_empty());
        assert!(default_cloned.user_defined.is_empty());
        assert!(default_cloned.user_tags.is_empty());
        assert!(default_cloned.parts.is_empty());
    }

    fn transitioned_receipt_source(
        bucket: &str,
        object: &str,
        remote_version: &str,
        version_state: rustfs_filemeta::TransitionVersionState,
        identity_hex: Option<&str>,
    ) -> ObjectInfo {
        let mut metadata = HashMap::new();
        if let Some(identity_hex) = identity_hex {
            rustfs_utils::http::metadata_compat::insert_str(
                &mut metadata,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
                identity_hex.to_string(),
            );
        }
        ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            user_defined: Arc::new(metadata),
            transitioned_object: TransitionedObject {
                name: format!("remote/{object}"),
                version_id: remote_version.to_string(),
                tier: "WARM".to_string(),
                status: TRANSITION_COMPLETE.to_string(),
                ..Default::default()
            },
            transition_version_state: version_state,
            ..Default::default()
        }
    }

    #[test]
    fn tier_free_version_receipt_matches_persisted_free_version_worker_fields() {
        let bucket = "receipt-bucket";
        let object = "archive/object.bin";
        let source_version_id = Uuid::from_u128(1);
        let local_free_version_id = Uuid::from_u128(2);
        let remote_version_id = Uuid::from_u128(3);
        let source_mod_time = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(4);
        let identity_hex = "ab".repeat(32);
        let mut source_metadata = HashMap::from([
            ("etag".to_string(), "source-etag".to_string()),
            ("x-amz-meta-private".to_string(), "must-not-enter-receipt".to_string()),
        ]);
        rustfs_utils::http::metadata_compat::insert_str(
            &mut source_metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            identity_hex.clone(),
        );
        let source_file_info = FileInfo {
            volume: bucket.to_string(),
            name: object.to_string(),
            version_id: Some(source_version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/receipt-object".to_string(),
            transition_tier: "WARM".to_string(),
            transition_version_id: Some(remote_version_id),
            transition_version: Some(remote_version_id.to_string()),
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            mod_time: Some(source_mod_time),
            size: 8192,
            data_dir: Some(Uuid::from_u128(4)),
            metadata: source_metadata,
            ..Default::default()
        };
        let source = ObjectInfo::from_file_info(&source_file_info, bucket, object, true);
        let mut persisted = FileMeta::new();
        persisted
            .add_version(source_file_info)
            .expect("transitioned receipt source should be persisted");
        let mut delete_file_info = FileInfo {
            volume: bucket.to_string(),
            name: object.to_string(),
            version_id: Some(source_version_id),
            mod_time: Some(source_mod_time + time::Duration::minutes(1)),
            ..Default::default()
        };
        delete_file_info.set_tier_free_version_id(&local_free_version_id.to_string());
        persisted
            .delete_version(&delete_file_info)
            .expect("transitioned source delete should create a free-version");

        let encoded = persisted.marshal_msg().expect("free-version metadata should encode");
        let decoded = FileMeta::load(&encoded).expect("free-version metadata should decode");
        let persisted_free_version = decoded
            .get_all_file_info_versions(bucket, object, true)
            .expect("decoded free-version should produce FileInfo")
            .versions
            .into_iter()
            .find(|version| version.tier_free_version())
            .expect("decoded metadata should contain the persisted free-version");
        let persisted_object_info = ObjectInfo::from_file_info(&persisted_free_version, bucket, object, true);

        let sink = TierFreeVersionReceiptSink::new();
        assert!(
            sink.record(&source, local_free_version_id)
                .expect("valid transitioned source should produce a receipt")
        );
        let mut receipts = sink.drain().expect("receipt owner should drain exactly once");
        assert_eq!(receipts.len(), 1);
        let receipt = receipts.pop().expect("one receipt should be present");

        assert_eq!(receipt.bucket, persisted_object_info.bucket);
        assert_eq!(receipt.name, persisted_object_info.name);
        assert_eq!(receipt.version_id, persisted_object_info.version_id);
        assert_eq!(receipt.mod_time, persisted_object_info.mod_time);
        assert_eq!(receipt.delete_marker, persisted_object_info.delete_marker);
        assert_eq!(receipt.transitioned_object.name, persisted_object_info.transitioned_object.name);
        assert_eq!(
            receipt.transitioned_object.version_id,
            persisted_object_info.transitioned_object.version_id
        );
        assert_eq!(receipt.transitioned_object.tier, persisted_object_info.transitioned_object.tier);
        assert_eq!(
            receipt.transitioned_object.free_version,
            persisted_object_info.transitioned_object.free_version
        );
        assert_eq!(receipt.transitioned_object.status, persisted_object_info.transitioned_object.status);
        assert_eq!(receipt.transition_version_state, persisted_object_info.transition_version_state);
        assert_eq!(
            crate::services::tier::tier::tier_destination_id_from_metadata(&receipt.user_defined)
                .expect("receipt identity should decode"),
            crate::services::tier::tier::tier_destination_id_from_metadata(&persisted_object_info.user_defined)
                .expect("persisted identity should decode")
        );
        assert_eq!(
            receipt.user_defined.len(),
            2,
            "receipt should carry only the two compatibility identity keys"
        );
        assert_eq!(
            receipt.user_defined.get("x-rustfs-internal-transition-tier-destination-id"),
            Some(&identity_hex)
        );
        assert_eq!(
            receipt.user_defined.get("x-minio-internal-transition-tier-destination-id"),
            Some(&identity_hex)
        );
        assert!(!receipt.user_defined.contains_key("x-amz-meta-private"));
        assert_eq!(receipt.size, 0);
        assert_eq!(receipt.actual_size, 0);
        assert!(receipt.parts.is_empty());
        assert!(receipt.etag.is_none());
        assert!(receipt.checksum.is_none());
        assert!(receipt.data_dir.is_none());
    }

    #[test]
    fn tier_free_version_receipt_sink_deduplicates_remote_target_and_drains_once() {
        let identity_hex = "11".repeat(32);
        let source = transitioned_receipt_source(
            "bucket",
            "object",
            "remote-version",
            rustfs_filemeta::TransitionVersionState::Exact,
            Some(&identity_hex),
        );
        let other_object = transitioned_receipt_source(
            "bucket",
            "other-object",
            "remote-version",
            rustfs_filemeta::TransitionVersionState::Exact,
            Some(&identity_hex),
        );
        let sink = TierFreeVersionReceiptSink::new();
        let clone = sink.clone();

        assert!(
            sink.record(&source, Uuid::from_u128(10))
                .expect("first physical receipt should record")
        );
        assert!(
            clone
                .record(&source, Uuid::from_u128(11))
                .expect("tuple-equivalent physical receipt should be represented")
        );
        assert!(
            clone
                .record(&other_object, Uuid::from_u128(12))
                .expect("a different logical key should retain its own task")
        );

        let mut receipts = sink.drain().expect("owner should drain shared receipts");
        receipts.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0].name, "object");
        assert_eq!(receipts[0].version_id, Some(Uuid::from_u128(10)));
        assert_eq!(receipts[1].name, "other-object");
        assert_eq!(receipts[1].version_id, Some(Uuid::from_u128(12)));
        assert_eq!(
            clone.drain().expect_err("a shared sink must drain only once").kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            clone
                .record(&source, Uuid::from_u128(13))
                .expect_err("recording after drain must fail")
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn tier_free_version_receipt_identity_covers_every_destructive_dimension() {
        let identity_hex = "44".repeat(32);
        let baseline = transitioned_receipt_source(
            "bucket",
            "directory/",
            "remote-version",
            rustfs_filemeta::TransitionVersionState::Exact,
            Some(&identity_hex),
        );
        let mut encoded_duplicate = baseline.clone();
        encoded_duplicate.name = "directory__XLDIR__".to_string();

        let mut variants = Vec::new();
        let mut changed = baseline.clone();
        changed.bucket = "other-bucket".to_string();
        variants.push(changed);
        let mut changed = baseline.clone();
        changed.name = "other-directory/".to_string();
        variants.push(changed);
        let mut changed = baseline.clone();
        changed.transitioned_object.tier = "COLD".to_string();
        variants.push(changed);
        let mut changed = baseline.clone();
        changed.transitioned_object.name = "remote/other-directory/".to_string();
        variants.push(changed);
        let mut changed = baseline.clone();
        changed.transitioned_object.version_id = "other-remote-version".to_string();
        variants.push(changed);
        let mut changed = baseline.clone();
        changed.transition_version_state = rustfs_filemeta::TransitionVersionState::SuspendedNull;
        changed.transitioned_object.version_id = "null".to_string();
        variants.push(changed);
        let mut changed = baseline.clone();
        rustfs_utils::http::metadata_compat::insert_str(
            Arc::make_mut(&mut changed.user_defined),
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            "55".repeat(32),
        );
        variants.push(changed);

        let sink = TierFreeVersionReceiptSink::new();
        assert!(
            sink.record(&baseline, Uuid::from_u128(20))
                .expect("baseline receipt should record")
        );
        assert!(
            sink.record(&encoded_duplicate, Uuid::from_u128(21))
                .expect("the encoded spelling of one logical key should deduplicate")
        );
        for (offset, variant) in variants.iter().enumerate() {
            assert!(
                sink.record(variant, Uuid::from_u128(30 + offset as u128))
                    .expect("each distinct cleanup identity should record")
            );
        }

        let receipts = sink.drain().expect("identity matrix should drain once");
        assert_eq!(receipts.len(), 8, "every destructive identity dimension must prevent deduplication");
        let baseline_receipt = receipts
            .iter()
            .find(|receipt| {
                receipt.bucket == "bucket"
                    && receipt.name == "directory/"
                    && receipt.transitioned_object.tier == "WARM"
                    && receipt.transitioned_object.name == "remote/directory/"
                    && receipt.transitioned_object.version_id == "remote-version"
                    && receipt.transition_version_state == rustfs_filemeta::TransitionVersionState::Exact
                    && crate::services::tier::tier::tier_destination_id_from_metadata(&receipt.user_defined)
                        .is_ok_and(|identity| identity == Some([0x44; 32]))
            })
            .expect("baseline cleanup identity should remain present");
        assert_eq!(
            baseline_receipt.version_id,
            Some(Uuid::from_u128(20)),
            "deduplication must retain the first UUID"
        );
    }

    #[test]
    fn tier_free_version_receipt_source_validation_fails_closed() {
        let identity_hex = "22".repeat(32);
        for (state, remote_version) in [
            (rustfs_filemeta::TransitionVersionState::KnownDisabled, ""),
            (rustfs_filemeta::TransitionVersionState::SuspendedNull, "null"),
            (rustfs_filemeta::TransitionVersionState::Exact, "opaque-version"),
        ] {
            let source = transitioned_receipt_source("bucket", "object", remote_version, state, Some(&identity_hex));
            assert!(
                TierFreeVersionReceiptSink::new()
                    .record(&source, Uuid::new_v4())
                    .expect("canonical remote-version state should be eligible"),
                "state={state:?} remote_version={remote_version:?}"
            );
        }

        let unknown = transitioned_receipt_source(
            "bucket",
            "object",
            "opaque-version",
            rustfs_filemeta::TransitionVersionState::Unknown,
            Some(&identity_hex),
        );
        assert!(
            !TierFreeVersionReceiptSink::new()
                .record(&unknown, Uuid::new_v4())
                .expect("unknown remote version state should defer to recovery")
        );
        let missing_identity = transitioned_receipt_source(
            "bucket",
            "object",
            "opaque-version",
            rustfs_filemeta::TransitionVersionState::Exact,
            None,
        );
        assert!(
            !TierFreeVersionReceiptSink::new()
                .record(&missing_identity, Uuid::new_v4())
                .expect("missing durable identity should defer to recovery")
        );
        let invalid_exact = transitioned_receipt_source(
            "bucket",
            "object",
            "",
            rustfs_filemeta::TransitionVersionState::Exact,
            Some(&identity_hex),
        );
        assert!(
            !TierFreeVersionReceiptSink::new()
                .record(&invalid_exact, Uuid::new_v4())
                .expect("conflicting remote state should defer to recovery")
        );

        let mut conflicting = transitioned_receipt_source(
            "bucket",
            "object",
            "opaque-version",
            rustfs_filemeta::TransitionVersionState::Exact,
            Some(&identity_hex),
        );
        Arc::make_mut(&mut conflicting.user_defined)
            .insert("x-minio-internal-transition-tier-destination-id".to_string(), "33".repeat(32));
        assert_eq!(
            TierFreeVersionReceiptSink::new()
                .record(&conflicting, Uuid::new_v4())
                .expect_err("conflicting identity aliases must fail closed")
                .kind(),
            io::ErrorKind::InvalidData
        );

        let valid = transitioned_receipt_source(
            "bucket",
            "object",
            "opaque-version",
            rustfs_filemeta::TransitionVersionState::Exact,
            Some(&identity_hex),
        );
        assert_eq!(
            TierFreeVersionReceiptSink::new()
                .record(&valid, Uuid::nil())
                .expect_err("nil local free-version identity must be rejected")
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn object_options_default_does_not_allocate_lifecycle_delete_all_journal() {
        let mut opts = ObjectOptions::default();

        assert!(opts.lifecycle_delete_all_journal().is_none());
        assert!(opts.tier_free_version_receipt_sink.is_none());
        opts.ensure_lifecycle_delete_all_journal();
        assert!(opts.lifecycle_delete_all_journal().is_some());
    }
}
