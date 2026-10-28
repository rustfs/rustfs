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

//! Set-wide delete decisions and replay. Decisions remain until every
//! participant and related transaction for the exact key converges.

use crate::disk::error::{DiskError, Result, RpcStatusError};
use crate::disk::{
    DELETE_RECOVERY_PROTOCOL_VERSION, DeleteRecoveryAction, DeleteRecoveryInspection, DeleteRecoveryState, DeleteTransactionId,
    DiskAPI, DiskStore,
};
use crate::error::StorageError;
use crate::set_disk::SetDisks;
use futures::future::join_all;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use uuid::Uuid;

const DISCOVERY_PAGE_SIZE: u32 = 128;
const MAX_DISCOVERED_TRANSACTIONS: usize = 1_000_000;
/// Inline cleanup attempts after a certified decision. Later attempts start
/// from a fresh inspection; the recovery hint retains any remaining work.
const PUBLISHED_CLEANUP_ATTEMPTS: usize = 3;
const EVENT_DELETE_RECOVERY_CLEANUP_DEFERRED: &str = "delete_recovery_cleanup_deferred";

#[cfg(test)]
pub(crate) mod test_hooks {
    use futures::future::BoxFuture;
    use std::sync::{Arc, Mutex};

    type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;
    type TransactionLockHook = Arc<dyn Fn(&rustfs_lock::NamespaceLockGuard) + Send + Sync>;

    static BEFORE_GC_FINALIZE: Mutex<Vec<(String, String, Hook)>> = Mutex::new(Vec::new());
    static CLEANUP_ATTEMPTS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());
    static AFTER_TRANSACTION_LOCK: Mutex<Vec<(String, String, TransactionLockHook)>> = Mutex::new(Vec::new());

    pub(crate) fn after_transaction_lock_acquired(bucket: &str, object: &str, hook: TransactionLockHook) {
        AFTER_TRANSACTION_LOCK
            .lock()
            .expect("transaction lock hooks")
            .push((bucket.to_owned(), object.to_owned(), hook));
    }

    pub(in crate::set_disk) fn run_after_transaction_lock_acquired(
        bucket: &str,
        object: &str,
        guard: &rustfs_lock::NamespaceLockGuard,
    ) {
        let hook = {
            let mut hooks = AFTER_TRANSACTION_LOCK.lock().expect("transaction lock hooks");
            hooks
                .iter()
                .position(|(hook_bucket, hook_object, _)| hook_bucket == bucket && hook_object == object)
                .map(|index| hooks.remove(index).2)
        };
        if let Some(hook) = hook {
            hook(guard);
        }
    }

    pub(super) fn record_cleanup_attempt(bucket: &str, object: &str) {
        CLEANUP_ATTEMPTS
            .lock()
            .expect("cleanup attempts")
            .push((bucket.to_owned(), object.to_owned()));
    }

    /// Inline cleanup attempts made by `publish_delete_commit` for this key.
    pub(crate) fn cleanup_attempts(bucket: &str, object: &str) -> usize {
        CLEANUP_ATTEMPTS
            .lock()
            .expect("cleanup attempts")
            .iter()
            .filter(|(attempt_bucket, attempt_object)| attempt_bucket == bucket && attempt_object == object)
            .count()
    }

    /// Runs `hook` once, after the key-wide GC inspection and before its
    /// finalization, the next time `bucket/object` reaches that point. Hooks
    /// for the same key run in registration order.
    pub(crate) fn before_gc_finalize(bucket: &str, object: &str, hook: Hook) {
        BEFORE_GC_FINALIZE
            .lock()
            .expect("GC finalize hooks")
            .push((bucket.to_owned(), object.to_owned(), hook));
    }

    pub(super) async fn run_before_gc_finalize(bucket: &str, object: &str) {
        let hook = {
            let mut hooks = BEFORE_GC_FINALIZE.lock().expect("GC finalize hooks");
            hooks
                .iter()
                .position(|(hook_bucket, hook_object, _)| hook_bucket == bucket && hook_object == object)
                .map(|index| hooks.remove(index).2)
        };
        if let Some(hook) = hook {
            hook().await;
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct DeleteRecoverySummary {
    pub discovered: usize,
    pub committed: usize,
    pub rolled_back: usize,
    pub blocked: usize,
    /// Groups blocked by a `LegacyAmbiguous` residue. Unlike other blocks this
    /// is deterministic: the residue cannot be classified by retrying.
    pub legacy_ambiguous: usize,
    pub pending: usize,
    pub failed: usize,
    pub reclaimed: usize,
    /// Observed removals for this exact key, not aggregated across prefixes.
    pub removed_versions: HashSet<Uuid>,
    /// Read-only evidence for ordinary missing-key S3 semantics. This never
    /// authorizes settlement while a configured participant is unavailable.
    pub no_transactions_at_quorum: bool,
}

#[derive(Clone)]
pub(crate) struct DeleteRecoveryContext<'a> {
    pub incarnation: Option<Uuid>,
    pub dry_run: bool,
    pub explicit_delete: bool,
    /// Only the locked heal path may repair a live marker-creation source.
    pub repair_marker_source: bool,
    pub namespace_owner: Option<Arc<dyn Send + Sync>>,
    pub transaction_guard: Option<Arc<rustfs_lock::NamespaceLockGuard>>,
    pub recovery_ticket: Option<crate::store::delete_recovery_runtime::DeleteRecoveryTicket>,
    pub published_hint: Option<(Uuid, &'a [Result<DeleteRecoveryInspection>])>,
    pub can_commit: &'a (dyn Fn() -> bool + Sync),
}

impl<'a> DeleteRecoveryContext<'a> {
    pub(crate) fn new(incarnation: Option<Uuid>, can_commit: &'a (dyn Fn() -> bool + Sync)) -> Self {
        Self {
            incarnation,
            dry_run: false,
            explicit_delete: false,
            repair_marker_source: false,
            namespace_owner: None,
            transaction_guard: None,
            recovery_ticket: None,
            published_hint: None,
            can_commit,
        }
    }
}

impl DeleteRecoverySummary {
    pub(crate) fn add(&mut self, other: Self) {
        self.discovered += other.discovered;
        self.committed += other.committed;
        self.rolled_back += other.rolled_back;
        self.blocked += other.blocked;
        self.legacy_ambiguous += other.legacy_ambiguous;
        self.pending += other.pending;
        self.failed += other.failed;
        self.reclaimed += other.reclaimed;
    }

    /// The error a heal must return when recovery left this key unresolved.
    /// An ambiguous legacy residue is permanent and gets its own error; other
    /// pending or blocked groups remain the retryable `SlowDown`.
    pub(crate) fn unresolved_heal_error(&self) -> Option<StorageError> {
        if self.legacy_ambiguous > 0 {
            Some(StorageError::DeleteRecoveryLegacyAmbiguous)
        } else if self.pending > 0 || self.blocked > 0 {
            Some(StorageError::SlowDown)
        } else {
            None
        }
    }
}

pub(in crate::set_disk) fn unsupported_recovery(error: &DiskError) -> bool {
    matches!(error, DiskError::UnsupportedDisk)
        || matches!(error, DiskError::Io(source) if source.get_ref()
            .and_then(|source| source.downcast_ref::<RpcStatusError>())
            .is_some_and(|status| status.status().code() == tonic::Code::Unimplemented))
}

/// Set authority needs generation identities, not the potentially large
/// per-version apply requests carried by returning-drive adoption.
struct DecisionEvidence {
    transaction: Uuid,
    deleted_versions: HashSet<Uuid>,
    original_versions: HashSet<crate::disk::DeleteRecoveryVersionIdentity>,
    predecessors: HashSet<Uuid>,
    target_absent: bool,
}

fn receipt(state: DeleteRecoveryState) -> bool {
    matches!(state, DeleteRecoveryState::QuorumCommitted | DeleteRecoveryState::CommitFinished)
}

fn validate(inspection: &DeleteRecoveryInspection, object: &str, transaction: Uuid) -> Result<()> {
    if inspection.protocol_version != DELETE_RECOVERY_PROTOCOL_VERSION
        || inspection.id.object != object
        || inspection.id.transaction != transaction
    {
        return Err(DiskError::FileCorrupt);
    }
    Ok(())
}

async fn inspect(
    disks: &[Option<DiskStore>],
    bucket: &str,
    object: &str,
    transaction: Uuid,
) -> Vec<Result<DeleteRecoveryInspection>> {
    join_all(disks.iter().map(|disk| async move {
        let state = disk
            .as_ref()
            .ok_or(DiskError::DiskNotFound)?
            .inspect_delete_transaction(bucket, object, transaction)
            .await?;
        validate(&state, object, transaction)?;
        Ok(state)
    }))
    .await
}

async fn discover(disk: &DiskStore, bucket: &str, object: Option<&str>) -> Result<Vec<DeleteTransactionId>> {
    let mut cursor = None;
    let mut ids = BTreeSet::new();
    loop {
        let page = disk
            .discover_delete_transactions(bucket, object, cursor, DISCOVERY_PAGE_SIZE)
            .await?;
        if page.transactions.len() > usize::try_from(DISCOVERY_PAGE_SIZE).map_err(|_| DiskError::FileCorrupt)? {
            return Err(DiskError::FileCorrupt);
        }
        for id in page.transactions {
            if id.transaction.is_nil() || object.is_some_and(|object| object != id.object) {
                return Err(DiskError::FileCorrupt);
            }
            ids.insert((id.object, id.transaction));
            if ids.len() > MAX_DISCOVERED_TRANSACTIONS {
                return Err(DiskError::other("delete recovery transaction limit exceeded"));
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    Ok(ids
        .into_iter()
        .map(|(object, transaction)| DeleteTransactionId { object, transaction })
        .collect())
}

async fn settle(
    disk: &DiskStore,
    bucket: &str,
    state: &DeleteRecoveryInspection,
    action: DeleteRecoveryAction,
    can_commit: &(impl Fn() -> bool + Sync + ?Sized),
    namespace_owner: Option<&Arc<dyn Send + Sync>>,
) -> Result<DeleteRecoveryInspection> {
    if !can_commit() {
        return Err(DiskError::other("delete recovery namespace fence was lost"));
    }
    let result = disk
        .settle_delete_transaction_with_namespace_owner(
            bucket,
            &state.id.object,
            state.id.transaction,
            state.fingerprint,
            action,
            namespace_owner.cloned(),
        )
        .await;
    let next = result?;
    validate(&next, &state.id.object, state.id.transaction)?;
    Ok(next)
}

/// Each phase fans out across independent drives, and awaits all started work
/// before evaluating the next decision or cleanup barrier.
async fn settle_group(
    disks: &[Option<DiskStore>],
    bucket: &str,
    copies: &mut [DeleteRecoveryInspection],
    action: impl Fn(&DeleteRecoveryInspection) -> Option<DeleteRecoveryAction> + Sync,
    can_commit: &(impl Fn() -> bool + Sync + ?Sized),
    namespace_owner: Option<&Arc<dyn Send + Sync>>,
    removed_versions: &mut HashSet<Uuid>,
) -> Result<bool> {
    let results = join_all(disks.iter().zip(copies.iter()).map(|(disk, copy)| {
        let action = action(copy);
        async move {
            match action {
                Some(action) => settle(
                    disk.as_ref().ok_or(DiskError::DiskNotFound)?,
                    bucket,
                    copy,
                    action,
                    can_commit,
                    namespace_owner,
                )
                .await
                .map(Some),
                None => Ok(None),
            }
        }
    }))
    .await;
    let mut pending = false;
    for (copy, result) in copies.iter_mut().zip(results) {
        match result {
            Ok(Some(next)) => {
                if copy.current_digest != next.current_digest {
                    let retained: HashSet<_> = next
                        .current_versions
                        .iter()
                        .map(|v| v.version_id.unwrap_or_default())
                        .collect();
                    removed_versions.extend(
                        copy.current_versions
                            .iter()
                            .map(|v| v.version_id.unwrap_or_default())
                            .filter(|id| !retained.contains(id)),
                    );
                }
                *copy = next;
            }
            Ok(None) => {}
            Err(DiskError::DiskOngoingReq) => pending = true,
            Err(error) => return Err(error),
        }
    }
    Ok(pending)
}

/// A later decision may require settling its prepared predecessor before an
/// untouched participant can adopt that decision. Ordering provides no commit
/// authority; reject contradictory causal records before any settlement.
fn causal_transaction_order(states: &BTreeMap<Uuid, Vec<DeleteRecoveryInspection>>) -> Result<Vec<Uuid>> {
    let mut indegrees: BTreeMap<_, usize> = states.keys().map(|id| (*id, 0)).collect();
    let mut successors: BTreeMap<Uuid, BTreeSet<Uuid>> = BTreeMap::new();
    for (transaction, copies) in states {
        for predecessor in copies
            .iter()
            .filter_map(|copy| copy.journal.as_ref())
            .flat_map(|journal| &journal.predecessors)
        {
            if states.contains_key(predecessor) && successors.entry(*predecessor).or_default().insert(*transaction) {
                let degree = indegrees.get_mut(transaction).ok_or(DiskError::FileCorrupt)?;
                *degree = degree.checked_add(1).ok_or(DiskError::FileCorrupt)?;
            }
        }
    }
    let mut ready: BTreeSet<_> = indegrees
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
        .collect();
    let mut order = Vec::with_capacity(states.len());
    while let Some(transaction) = ready.pop_first() {
        order.push(transaction);
        if let Some(children) = successors.get(&transaction) {
            for child in children {
                let degree = indegrees.get_mut(child).ok_or(DiskError::FileCorrupt)?;
                *degree = degree.checked_sub(1).ok_or(DiskError::FileCorrupt)?;
                if *degree == 0 {
                    ready.insert(*child);
                }
            }
        }
    }
    if order.len() != states.len() {
        return Err(DiskError::FileCorrupt);
    }
    Ok(order)
}

impl SetDisks {
    /// Publication follows the ordinary metadata quorum. Once a publication is
    /// attempted, response loss must never trigger blind rollback.
    pub(in crate::set_disk) async fn publish_delete_commit(
        &self,
        bucket: &str,
        object: &str,
        transaction: Uuid,
        disks: &[Option<DiskStore>],
        write_quorum: usize,
        mut context: DeleteRecoveryContext<'_>,
    ) -> Result<()> {
        let transaction_guard = match context.transaction_guard.take() {
            Some(guard) => guard,
            None => self
                .acquire_delete_transaction_lock(bucket, object)
                .await
                .map_err(DiskError::other)?,
        };
        let outer_can_commit = context.can_commit;
        let can_commit = || outer_can_commit() && !transaction_guard.is_lock_lost();
        context.can_commit = &can_commit;
        context.transaction_guard = Some(Arc::clone(&transaction_guard));
        let owner = context
            .namespace_owner
            .take()
            .unwrap_or_else(|| self.ctx.begin_namespace_commit());
        context.namespace_owner = Some(Arc::new((owner, Arc::clone(&transaction_guard))));
        let can_commit = context.can_commit;
        let states = inspect(disks, bucket, object, transaction).await;
        let untouched_absence = |state: &Result<DeleteRecoveryInspection>| matches!(state, Ok(state) if state.state == DeleteRecoveryState::Absent && state.current_digest.is_none());
        if states.iter().filter(|state| untouched_absence(state)).count() >= write_quorum
            && states.iter().all(|state| {
                untouched_absence(state) || matches!(state, Err(error) if unavailable(error) && !unsupported_recovery(error))
            })
        {
            // An ordinary missing-key no-op requires no recovery decision.
            // Do not manufacture witness transactions while a drive is offline.
            if let Some(ticket) = context.recovery_ticket.as_ref() {
                self.ctx.delete_recovery_hints().ack(ticket);
            }
            return Ok(());
        }
        let published = join_all(disks.iter().zip(&states).map(|(disk, state)| async {
            let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
            let state = state.as_ref().map_err(Clone::clone)?;
            if state.state == DeleteRecoveryState::LegacyAmbiguous {
                return Err(DiskError::FileCorrupt);
            }
            settle(
                disk,
                bucket,
                state,
                DeleteRecoveryAction::PublishCommit,
                can_commit,
                context.namespace_owner.as_ref(),
            )
            .await
        }))
        .await;
        if published
            .iter()
            .filter(|state| matches!(state, Ok(state) if receipt(state.state)))
            .count()
            < write_quorum
        {
            return Err(DiskError::ErasureWriteQuorum);
        }
        // Physical cleanup is retryable after the certified decision. An
        // unavailable participant retains the decision for its eventual return.
        let ticket = context.recovery_ticket.clone();
        let mut attempt = 1;
        let cleanup = loop {
            #[cfg(test)]
            test_hooks::record_cleanup_attempt(bucket, object);
            let mut attempt_context = context.clone();
            attempt_context.published_hint = (attempt == 1).then_some((transaction, published.as_slice()));
            match self.recover_delete_key(bucket, object, attempt_context).await {
                // A stale settlement fingerprint only means the snapshot raced
                // another writer; each attempt re-inspects every participant.
                Err(DiskError::OutdatedXLMeta) if attempt < PUBLISHED_CLEANUP_ATTEMPTS && can_commit() => attempt += 1,
                result => break result,
            }
        };
        match cleanup {
            Ok(summary) => {
                if summary.pending == 0
                    && summary.blocked == 0
                    && summary.failed == 0
                    && let Some(ticket) = ticket.as_ref()
                {
                    self.ctx.delete_recovery_hints().ack(ticket);
                }
            }
            Err(error) => {
                // The decision is durable; the retained hint completes cleanup.
                tracing::warn!(
                    event = EVENT_DELETE_RECOVERY_CLEANUP_DEFERRED,
                    component = crate::set_disk::LOG_COMPONENT_ECSTORE,
                    subsystem = crate::set_disk::LOG_SUBSYSTEM_SET_DISK,
                    state = "deferred",
                    bucket,
                    object,
                    %transaction,
                    attempts = attempt,
                    error = %error,
                    "Delete decision published but inline cleanup was deferred"
                );
            }
        }
        Ok(())
    }

    pub(crate) async fn recover_delete_bucket(
        &self,
        bucket: &str,
        context: DeleteRecoveryContext<'_>,
    ) -> Result<DeleteRecoverySummary> {
        self.recover_delete_prefix(bucket, "", context).await
    }

    pub(crate) async fn recover_delete_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        context: DeleteRecoveryContext<'_>,
    ) -> Result<DeleteRecoverySummary> {
        let disks = self.disk_inventory().await;
        if disks.len() != self.set_drive_count || disks.iter().any(Option::is_none) {
            return Ok(DeleteRecoverySummary {
                pending: 1,
                ..Default::default()
            });
        }
        let pages = join_all(disks.iter().flatten().map(|disk| discover(disk, bucket, None))).await;
        let mut keys = BTreeSet::new();
        let mut summary = DeleteRecoverySummary::default();
        for page in pages {
            match page {
                Ok(ids) => {
                    keys.extend(
                        ids.into_iter()
                            .map(|id| id.object)
                            .filter(|object| object.starts_with(prefix)),
                    );
                }
                Err(error) if unavailable(&error) => {
                    summary.pending += 1;
                }
                Err(DiskError::FileCorrupt) => summary.failed += 1,
                Err(error) => return Err(error),
            }
        }
        // Every key requires complete participation. Avoid taking each key's
        // lock and repeating discovery while this sweep already lacks a peer.
        if summary.pending > 0 {
            return Ok(summary);
        }
        // Lock order: caller's bucket lifecycle fence, then one exact key.
        for object in keys {
            match self.recover_delete_object(bucket, &object, context.clone()).await {
                Ok(result) => summary.add(result),
                Err(DiskError::FileCorrupt) => summary.failed += 1,
                Err(error) => return Err(error),
            }
        }
        Ok(summary)
    }

    pub(crate) async fn recover_delete_object(
        &self,
        bucket: &str,
        object: &str,
        mut context: DeleteRecoveryContext<'_>,
    ) -> Result<DeleteRecoverySummary> {
        // Lock order: caller's bucket lifecycle fence, then one exact key.
        let guard = Arc::new(
            self.acquire_write_lock_diag("recover_delete", bucket, object)
                .await
                .map_err(DiskError::other)?,
        );
        if !context.dry_run {
            let namespace_owner = context
                .namespace_owner
                .clone()
                .unwrap_or_else(|| self.ctx.begin_namespace_commit());
            context.namespace_owner = Some(Arc::new((namespace_owner, guard.clone())));
        }
        let can_commit = context.can_commit;
        let fence = || can_commit() && !guard.is_lock_lost();
        context.can_commit = &fence;
        self.recover_delete_key(bucket, object, context).await
    }

    /// Caller retains the exact key's write lock and its lifecycle owner.
    pub(in crate::set_disk) async fn recover_delete_key(
        &self,
        bucket: &str,
        object: &str,
        mut context: DeleteRecoveryContext<'_>,
    ) -> Result<DeleteRecoverySummary> {
        let transaction_guard = match context.transaction_guard.take() {
            Some(guard) => guard,
            None => self
                .acquire_delete_transaction_lock(bucket, object)
                .await
                .map_err(DiskError::other)?,
        };
        let outer_can_commit = context.can_commit;
        let can_commit = || outer_can_commit() && !transaction_guard.is_lock_lost();
        context.can_commit = &can_commit;
        if !context.dry_run {
            let owner = context
                .namespace_owner
                .take()
                .unwrap_or_else(|| self.ctx.begin_namespace_commit());
            context.namespace_owner = Some(Arc::new((owner, Arc::clone(&transaction_guard))));
        }
        let DeleteRecoveryContext {
            incarnation,
            dry_run,
            explicit_delete,
            repair_marker_source,
            namespace_owner,
            published_hint,
            can_commit,
            recovery_ticket: _,
            transaction_guard: _,
        } = context;
        let disks = self.disk_inventory().await;
        let complete_inventory = disks.len() == self.set_drive_count && disks.iter().all(Option::is_some);
        let pages = join_all(disks.iter().flatten().map(|disk| discover(disk, bucket, Some(object)))).await;
        let mut transactions = BTreeSet::new();
        let mut inspected = 0;
        let mut offline = !complete_inventory;
        let mut unsupported = false;
        for page in pages {
            match page {
                Ok(ids) => {
                    inspected += 1;
                    transactions.extend(ids.into_iter().map(|id| id.transaction));
                }
                Err(error) if unavailable(&error) => {
                    offline = true;
                    unsupported |= unsupported_recovery(&error);
                }
                Err(error) => return Err(error),
            }
        }
        if offline {
            return Ok(DeleteRecoverySummary {
                discovered: transactions.len(),
                pending: 1,
                no_transactions_at_quorum: transactions.is_empty() && !unsupported && inspected > self.set_drive_count / 2,
                ..Default::default()
            });
        }
        let mut summary = DeleteRecoverySummary {
            discovered: transactions.len(),
            ..Default::default()
        };
        let mut all_states = BTreeMap::new();
        for transaction in transactions {
            let mut complete = Vec::with_capacity(disks.len());
            let inspections = if let Some((published_tx, published_states)) = published_hint
                && !repair_marker_source
                && !dry_run
                && published_tx == transaction
                && published_states.len() == disks.len()
                && published_states.iter().all(Result::is_ok)
            {
                published_states.to_vec()
            } else {
                inspect(&disks, bucket, object, transaction).await
            };
            for state in inspections {
                match state {
                    Ok(state) => {
                        if let Some(journal) = &state.journal {
                            let witness = journal.original_digest.is_none()
                                && journal.target_digest.is_none()
                                && journal.requests.is_empty();
                            if incarnation.is_some_and(|expected| journal.bucket_incarnation != Some(expected) && !witness) {
                                summary.blocked += 1;
                                return Ok(summary);
                            }
                        }
                        complete.push(state);
                    }
                    Err(error) if unavailable(&error) => {
                        summary.pending += 1;
                        return Ok(summary);
                    }
                    Err(error) => return Err(error),
                }
            }
            all_states.insert(transaction, complete);
        }
        // Validate causal authority before prerequisite repair can publish metadata.
        let transaction_order = causal_transaction_order(&all_states)?;
        let prerequisite = if repair_marker_source && !dry_run {
            let mut active = all_states
                .iter()
                .filter(|(_, copies)| copies.iter().any(|copy| copy.state != DeleteRecoveryState::Absent));
            let first = active.next();
            if active.next().is_none() {
                first.and_then(|(&transaction, copies)| {
                    marker_source_to_repair(copies, incarnation).map(|source| (transaction, source))
                })
            } else {
                None
            }
        } else {
            None
        };
        if let Some((transaction, source)) = prerequisite {
            Box::pin(self.heal_delete_recovery_source(bucket, object, &source, can_commit, namespace_owner.as_ref())).await?;
            let refreshed = inspect(&disks, bucket, object, transaction)
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            if !can_commit() {
                return Err(DiskError::DiskOngoingReq);
            }
            if refreshed.iter().any(|copy| {
                copy.journal
                    .as_ref()
                    .is_some_and(|journal| journal.bucket_incarnation != incarnation)
            }) {
                return Err(DiskError::OutdatedXLMeta);
            }
            all_states.insert(transaction, refreshed);
        }
        // A fresh authorized removal may discard safe legacy residue beside
        // certified absence receipts. Every participant must have no live metadata;
        // the receipts remain behind the key-wide barrier until all groups settle.
        let explicit_legacy_removal = explicit_delete
            && all_states.values().flatten().all(|copy| {
                copy.current_digest.is_none()
                    && match copy.state {
                        DeleteRecoveryState::Absent => true,
                        DeleteRecoveryState::LegacyAmbiguous => copy.explicit_delete_safe,
                        DeleteRecoveryState::QuorumCommitted | DeleteRecoveryState::CommitFinished => {
                            copy.journal.as_ref().is_some_and(|journal| journal.target_digest.is_none())
                        }
                        DeleteRecoveryState::NewPrepared => false,
                    }
            });
        // This is a fresh authorized delete of an absent unversioned key,
        // rather than evidence about the outcome of a historical transaction.
        let fresh_absence_decision = explicit_delete
            && all_states.values().flatten().all(|copy| {
                copy.current_digest.is_none()
                    && match copy.state {
                        DeleteRecoveryState::Absent => true,
                        DeleteRecoveryState::NewPrepared => copy.explicit_delete_safe,
                        DeleteRecoveryState::QuorumCommitted | DeleteRecoveryState::CommitFinished => {
                            copy.journal.as_ref().is_some_and(|journal| journal.target_digest.is_none())
                        }
                        DeleteRecoveryState::LegacyAmbiguous => false,
                    }
            });
        // Receipt cleanup alone does not mutate xl.meta. Returning peers and
        // undecided preparations still need fences before and after recovery.
        let metadata_may_change = all_states.values().any(|copies| {
            !copies.iter().all(|copy| copy.state == DeleteRecoveryState::Absent)
                && copies.iter().any(|copy| {
                    copy.state == DeleteRecoveryState::NewPrepared
                        || (copy.state == DeleteRecoveryState::Absent && copy.current_digest.is_some())
                        || copy
                            .journal
                            .as_ref()
                            .is_some_and(|journal| copy.current_digest != journal.target_digest)
                })
        });
        let namespace_owner = if dry_run {
            None
        } else {
            Some(namespace_owner.unwrap_or_else(|| self.ctx.begin_namespace_commit()))
        };
        if !dry_run && metadata_may_change {
            self.invalidate_get_object_metadata_cache(bucket, object).await;
        }
        let authorities = all_states
            .values()
            .flatten()
            .filter(|state| receipt(state.state))
            .filter_map(|state| state.journal.as_ref())
            .map(|journal| DecisionEvidence {
                transaction: journal.transaction,
                deleted_versions: journal.deleted_versions.iter().copied().collect(),
                original_versions: journal.original_versions.iter().cloned().collect(),
                predecessors: journal.predecessors.iter().copied().collect(),
                target_absent: journal.target_digest.is_none(),
            })
            .collect::<Vec<_>>();
        let mut unresolved = false;
        let mut mutated = false;
        for transaction in transaction_order {
            let copies = all_states.get_mut(&transaction).ok_or(DiskError::FileCorrupt)?;
            if mutated {
                // A sibling may have restored metadata or reclaimed shared
                // evidence. Refresh fingerprints before acting on this group.
                *copies = inspect(&disks, bucket, object, transaction)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
            }
            if copies.iter().all(|state| state.state == DeleteRecoveryState::Absent) {
                continue;
            }
            if copies.iter().any(|state| state.state == DeleteRecoveryState::LegacyAmbiguous) {
                if explicit_legacy_removal {
                    if dry_run {
                        summary.reclaimed += 1;
                        continue;
                    }
                    mutated = true;
                    if settle_group(
                        &disks,
                        bucket,
                        copies,
                        |copy| {
                            (copy.state == DeleteRecoveryState::LegacyAmbiguous).then_some(DeleteRecoveryAction::DiscardLegacy)
                        },
                        can_commit,
                        namespace_owner.as_ref(),
                        &mut summary.removed_versions,
                    )
                    .await?
                    {
                        summary.pending += 1;
                        unresolved = true;
                    } else {
                        summary.reclaimed += 1;
                    }
                } else {
                    summary.blocked += 1;
                    summary.legacy_ambiguous += 1;
                    unresolved = true;
                }
                continue;
            }
            let authority = copies
                .iter()
                .filter(|state| receipt(state.state))
                .filter_map(|state| state.journal.as_ref())
                .max_by_key(|journal| !journal.requests.is_empty())
                .cloned();
            let prepared = copies.iter().find_map(|state| state.journal.as_ref());
            let covered = prepared
                .filter(|journal| !journal.deleted_versions.is_empty())
                .is_some_and(|journal| {
                    authorities.iter().any(|authority| {
                        journal.deleted_versions.iter().all(|version| {
                            authority.deleted_versions.contains(version)
                                && (!version.is_nil()
                                    || journal
                                        .original_versions
                                        .iter()
                                        .filter(|source| source.version_id.is_none_or(|id| id.is_nil()))
                                        .all(|source| authority.original_versions.contains(source))
                                    || (authority.predecessors.contains(&journal.transaction)
                                        && journal.target_digest.is_none()
                                        && authority.target_absent
                                        && copies.iter().all(|copy| {
                                            copy.current_digest.is_none() || copy.current_versions == journal.original_versions
                                        })))
                        })
                    })
                });
            let original_quorum = prepared.is_some_and(|journal| {
                copies
                    .iter()
                    .filter(|copy| copy.current_versions == journal.original_versions)
                    .count()
                    > self.set_drive_count / 2
            });
            let all_decisions_precede = !authorities.is_empty()
                && authorities.iter().all(|authority| {
                    copies
                        .iter()
                        .filter_map(|copy| copy.journal.as_ref())
                        .any(|journal| journal.predecessors.contains(&authority.transaction))
                });
            if authority.is_some() || covered || fresh_absence_decision {
                let certified_same_transaction = authority.is_some();
                let reference = authority
                    .or_else(|| copies.iter().find_map(|state| state.journal.clone()))
                    .ok_or(DiskError::FileCorrupt)?;
                if dry_run {
                    if copies.iter().any(|copy| {
                        if certified_same_transaction
                            && copy.state == DeleteRecoveryState::NewPrepared
                            && copy.current_digest.is_none()
                            && !copy.commit_data_safe
                        {
                            copy.journal
                                .as_ref()
                                .is_none_or(|local| !local.matches_prepared_authority(&reference))
                        } else if copy.state == DeleteRecoveryState::Absent {
                            copy.current_digest.is_some() && !reference.matches_adoption_source(&copy.current_versions)
                        } else {
                            !copy.commit_data_safe
                        }
                    }) {
                        summary.blocked += 1;
                    } else {
                        summary.committed += 1;
                    }
                    continue;
                }
                mutated = true;
                let mut pending = settle_group(
                    &disks,
                    bucket,
                    copies,
                    |copy| {
                        if receipt(copy.state) && copy.commit_data_safe {
                            return None;
                        }
                        Some(
                            if copy.state == DeleteRecoveryState::Absent
                                || (certified_same_transaction
                                    && copy.state == DeleteRecoveryState::NewPrepared
                                    && copy.current_digest.is_none()
                                    && !copy.commit_data_safe)
                            {
                                DeleteRecoveryAction::AdoptCommit {
                                    authority: Box::new(reference.clone()),
                                }
                            } else {
                                DeleteRecoveryAction::PublishCommit
                            },
                        )
                    },
                    can_commit,
                    namespace_owner.as_ref(),
                    &mut summary.removed_versions,
                )
                .await?;
                if !pending {
                    pending = settle_group(
                        &disks,
                        bucket,
                        copies,
                        |copy| {
                            (copy.state != DeleteRecoveryState::CommitFinished || !copy.commit_data_safe)
                                .then_some(DeleteRecoveryAction::FinishCommit)
                        },
                        can_commit,
                        namespace_owner.as_ref(),
                        &mut summary.removed_versions,
                    )
                    .await?;
                }
                if pending {
                    summary.pending += 1;
                    unresolved = true;
                } else {
                    summary.committed += 1;
                }
            } else {
                // A sibling decision or foreign generation can supersede this
                // snapshot. Never guess by restoring the whole old backup.
                if (!authorities.is_empty() && !original_quorum && !all_decisions_precede)
                    || copies
                        .iter()
                        .any(|state| state.state != DeleteRecoveryState::Absent && !state.rollback_data_complete)
                {
                    summary.blocked += 1;
                    unresolved = true;
                    continue;
                }
                let originals = copies
                    .iter()
                    .filter_map(|state| state.journal.as_ref())
                    .flat_map(|journal| journal.original_versions.iter())
                    .collect::<HashSet<_>>();
                if copies.iter().any(|state| {
                    state.state == DeleteRecoveryState::Absent
                        && state.current_versions.iter().any(|version| !originals.contains(version))
                }) {
                    summary.blocked += 1;
                    unresolved = true;
                    continue;
                }
                if dry_run {
                    summary.rolled_back += 1;
                    continue;
                }
                mutated = true;
                if settle_group(
                    &disks,
                    bucket,
                    copies,
                    |copy| (copy.state == DeleteRecoveryState::NewPrepared).then_some(DeleteRecoveryAction::Rollback),
                    can_commit,
                    namespace_owner.as_ref(),
                    &mut summary.removed_versions,
                )
                .await?
                {
                    summary.pending += 1;
                    unresolved = true;
                } else {
                    summary.rolled_back += 1;
                }
            }
        }
        if !unresolved && !dry_run {
            // GC is a key-wide barrier: every predecessor must have settled
            // before the last surviving commit witness can disappear.
            let mut settled = Vec::with_capacity(all_states.len());
            if all_states.len() == 1 {
                let copies = all_states.into_values().next().ok_or(DiskError::FileCorrupt)?;
                if copies
                    .iter()
                    .any(|state| !matches!(state.state, DeleteRecoveryState::Absent | DeleteRecoveryState::CommitFinished))
                {
                    summary.pending += 1;
                    return Ok(summary);
                }
                settled.push(copies);
            } else {
                for transaction in all_states.keys() {
                    let fresh = inspect(&disks, bucket, object, *transaction).await;
                    if fresh.iter().any(|state| !matches!(state, Ok(state) if matches!(state.state, DeleteRecoveryState::Absent | DeleteRecoveryState::CommitFinished))) {
                        summary.pending += 1;
                        return Ok(summary);
                    }
                    settled.push(fresh.into_iter().collect::<Result<Vec<_>>>()?);
                }
            }
            for (index, mut copies) in settled.into_iter().enumerate() {
                if index > 0 {
                    copies = inspect(&disks, bucket, object, copies[0].id.transaction)
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>>>()?;
                    if copies
                        .iter()
                        .any(|copy| !matches!(copy.state, DeleteRecoveryState::Absent | DeleteRecoveryState::CommitFinished))
                    {
                        summary.pending += 1;
                        return Ok(summary);
                    }
                }
                #[cfg(test)]
                test_hooks::run_before_gc_finalize(bucket, object).await;
                if settle_group(
                    &disks,
                    bucket,
                    &mut copies,
                    |copy| (copy.state == DeleteRecoveryState::CommitFinished).then_some(DeleteRecoveryAction::FinalizeCommit),
                    can_commit,
                    namespace_owner.as_ref(),
                    &mut summary.removed_versions,
                )
                .await?
                {
                    summary.pending += 1;
                }
            }
        }

        if !dry_run && metadata_may_change {
            self.invalidate_get_object_metadata_cache(bucket, object).await;
        }
        Ok(summary)
    }
}

// A lagging member may have the complete older suffix of a marker creation's
// live source. Repair that source before applying the original strict matcher.
fn marker_source_to_repair(
    copies: &[DeleteRecoveryInspection],
    incarnation: Option<Uuid>,
) -> Option<crate::disk::DeleteRecoveryVersionIdentity> {
    let expected = incarnation.filter(|id| !id.is_nil())?;
    let authority = copies
        .iter()
        .filter(|copy| receipt(copy.state))
        .find_map(|copy| copy.journal.as_ref())?;
    let source = authority.original_versions.first()?;
    let request = authority.requests.first()?;
    if authority.bucket_incarnation != Some(expected)
        || authority.requests.len() != 1
        || !request.deleted
        || !request.mark_deleted
        || request.version_id.is_none_or(|id| id.is_nil())
        || authority
            .original_versions
            .iter()
            .any(|version| version.version_id == request.version_id)
        || !authority.deleted_versions.is_empty()
        || !authority.data_dirs.is_empty()
        || authority.target_digest.is_none()
        || source.version_id.is_none_or(|id| id.is_nil())
        || source.deleted
    {
        return None;
    }
    let mut missing_source = false;
    for copy in copies {
        match copy.state {
            DeleteRecoveryState::QuorumCommitted | DeleteRecoveryState::CommitFinished => {
                let journal = copy.journal.as_ref()?;
                if journal.bucket_incarnation != Some(expected)
                    || journal.original_versions != authority.original_versions
                    || !journal.matches_prepared_authority(authority)
                    || copy.current_digest != journal.target_digest
                {
                    return None;
                }
            }
            DeleteRecoveryState::Absent => {
                if copy.journal.is_some()
                    || copy.current_versions.is_empty()
                    || !authority.original_versions.ends_with(&copy.current_versions)
                {
                    return None;
                }
                missing_source |= copy.current_versions.first() != Some(source);
            }
            _ => return None,
        }
    }
    missing_source.then(|| source.clone())
}

fn unavailable(error: &DiskError) -> bool {
    if unsupported_recovery(error) {
        return true;
    }
    match error {
        DiskError::DiskNotFound
        | DiskError::FaultyDisk
        | DiskError::DiskOngoingReq
        | DiskError::Timeout
        | DiskError::RemoteClientUnavailable(_) => true,
        DiskError::Io(source) => {
            if let Some(status) = crate::cluster::rpc::client::embedded_tonic_status(source) {
                return crate::cluster::rpc::client::is_network_like_status(status);
            }
            matches!(
                source.kind(),
                std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::UnexpectedEof
            )
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata_sys::{self, init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};

    #[test]
    fn unresolved_heal_error_distinguishes_legacy_ambiguity_from_transient_blocks() {
        // rustfs/rustfs#8608: a legacy residue is deterministic and must not be
        // reported as request-rate pressure.
        let resolved = DeleteRecoverySummary::default();
        assert!(resolved.unresolved_heal_error().is_none());
        let pending = DeleteRecoverySummary {
            pending: 1,
            ..Default::default()
        };
        assert!(matches!(pending.unresolved_heal_error(), Some(StorageError::SlowDown)));
        let blocked = DeleteRecoverySummary {
            blocked: 1,
            ..Default::default()
        };
        assert!(matches!(blocked.unresolved_heal_error(), Some(StorageError::SlowDown)));
        let ambiguous = DeleteRecoverySummary {
            blocked: 1,
            legacy_ambiguous: 1,
            pending: 1,
            ..Default::default()
        };
        assert!(matches!(
            ambiguous.unresolved_heal_error(),
            Some(StorageError::DeleteRecoveryLegacyAmbiguous)
        ));
        let mut total = DeleteRecoverySummary::default();
        total.add(ambiguous);
        assert_eq!(total.legacy_ambiguous, 1);
    }
    use crate::disk::{DeleteOptions, ReadOptions, STORAGE_FORMAT_FILE};
    use crate::object_api::{ObjectOptions, PutObjReader};
    use crate::storage_api_contracts::bucket::{BucketOperations, DeleteBucketOptions, MakeBucketOptions};
    use crate::storage_api_contracts::object::{ObjectIO, ObjectOperations};
    use crate::store::ECStore;
    use rustfs_filemeta::FileInfo;
    use rustfs_heal_contracts::heal_channel::HealOpts;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    async fn fixture(bucket: &str) -> (Vec<tempfile::TempDir>, Arc<ECStore>, Vec<DiskStore>) {
        let (dirs, store) = isolated_store_over_temp_disks().await;
        // Manual recovery assertions need sole ownership of the seeded journals.
        store.background_cancel_token().expect("fixture background token").cancel();
        tokio::task::yield_now().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create authoritative recovery bucket");
        let disks = store.pools[0].disk_set[0]
            .disk_inventory()
            .await
            .into_iter()
            .map(|disk| disk.expect("fixture drive is online"))
            .collect();
        (dirs, store, disks)
    }

    async fn put(store: &Arc<ECStore>, bucket: &str, body: &[u8], versioned: bool) -> Option<Uuid> {
        let mut reader = PutObjReader::from_vec(body.to_vec());
        store
            .put_object(
                bucket,
                "object",
                &mut reader,
                &ObjectOptions {
                    versioned,
                    write_completion: crate::object_api::WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("put recovery source")
            .version_id
    }

    async fn applied(store: &Arc<ECStore>, disks: &[DiskStore], bucket: &str, version: Option<Uuid>, count: usize) -> Uuid {
        applied_at(store, disks, bucket, version, count, Uuid::new_v4()).await
    }

    async fn applied_at(
        store: &Arc<ECStore>,
        disks: &[DiskStore],
        bucket: &str,
        version: Option<Uuid>,
        count: usize,
        transaction: Uuid,
    ) -> Uuid {
        let incarnation = store
            .bucket_incarnation_id_from_disk(bucket)
            .await
            .expect("source bucket identity");
        for disk in disks.iter().take(count) {
            disk.delete_version_recoverable(
                bucket,
                "object",
                FileInfo {
                    name: "object".to_string(),
                    version_id: version,
                    ..Default::default()
                },
                false,
                DeleteOptions {
                    old_data_dir: Some(transaction),
                    delete_recovery_bucket_incarnation: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("apply prepared delete without publishing a set decision");
        }
        transaction
    }

    async fn publish(disk: &DiskStore, bucket: &str, transaction: Uuid) {
        let state = disk
            .inspect_delete_transaction(bucket, "object", transaction)
            .await
            .expect("inspect prepared source");
        disk.settle_delete_transaction(bucket, "object", transaction, state.fingerprint, DeleteRecoveryAction::PublishCommit)
            .await
            .expect("persist a decision after the simulated metadata quorum");
    }

    async fn body(store: &Arc<ECStore>, bucket: &str, version: Option<Uuid>) -> Vec<u8> {
        let opts = ObjectOptions {
            version_id: version.map(|version| version.to_string()),
            ..Default::default()
        };
        let mut reader = store
            .get_object_reader(bucket, "object", None, http::HeaderMap::new(), &opts)
            .await
            .expect("open recovered object");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("read exact recovered bytes");
        body
    }

    #[tokio::test]
    async fn delete_recovery_set_rolls_back_undecided_inline_bytes() {
        let bucket = "recover-undecided";
        let (_dirs, store, disks) = fixture(bucket).await;
        let original = vec![21; 4096];
        put(&store, bucket, &original, false).await;
        applied(&store, &disks, bucket, None, disks.len()).await;
        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("recover undisputed prepared delete");
        assert!(
            report.detail.contains("rolled_back=1"),
            "actual rollback must be reported: {}",
            report.detail
        );
        assert_eq!(body(&store, bucket, None).await, original);
    }

    #[tokio::test]
    async fn delete_recovery_deferred_batch_discards_released_alias_fence_and_preserves_caller_fence() {
        use crate::bucket::metadata_sys::ObjectLockConfigState;
        use crate::object_api::{NamespaceLockFence, NamespaceLockSignalTestFence, ObjectLockConfigSnapshot};
        use crate::storage_api_contracts::object::ObjectToDelete;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicBool, Ordering};

        for lose_caller in [false, true] {
            let bucket = if lose_caller {
                "deferred-batch-lost-caller"
            } else {
                "deferred-batch-released-alias"
            };
            let (_dirs, store, disks) = fixture(bucket).await;
            put(&store, bucket, &[27; 4096], false).await;
            let transaction = applied(&store, &disks, bucket, None, disks.len()).await;
            let before = join_all(
                disks
                    .iter()
                    .map(|disk| disk.inspect_delete_transaction(bucket, "object", transaction)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .expect("capture pending recovery evidence");
            let mut set = (*store.pools[0].disk_set[0]).clone();
            set.lockers = vec![Arc::new(rustfs_lock::LocalClient::with_manager(Arc::new(
                rustfs_lock::GlobalLockManager::Enabled(Arc::new(rustfs_lock::FastObjectLockManager::new())),
            )))];
            set.ctx
                .update_erasure_type(crate::layout::endpoints::SetupType::DistErasure)
                .await;

            let (caller_fence, caller_lost) = NamespaceLockFence::loss_handle_for_test();
            let released_alias_lost = Arc::new(AtomicBool::new(false));
            let signal_fence = Arc::new(Mutex::new(None));
            let captured_signal_fence = Arc::clone(&signal_fence);
            let captured_alias_lost = Arc::clone(&released_alias_lost);
            test_hooks::after_transaction_lock_acquired(
                bucket,
                "object",
                Arc::new(move |guard| {
                    let signal = guard.lock_lost_signal().expect("distributed alias signal");
                    let fence = NamespaceLockSignalTestFence::install_with_loss_handle(&signal, Arc::clone(&captured_alias_lost));
                    // Retain the signal so a later allocation cannot reuse its
                    // registry identity after the original guard is dropped.
                    *captured_signal_fence.lock().expect("captured alias fence") = Some((fence, signal));
                }),
            );
            let replay_started = Arc::new(AtomicBool::new(false));
            let captured_replay_started = Arc::clone(&replay_started);
            let expired_alias = Arc::clone(&released_alias_lost);
            test_hooks::after_transaction_lock_acquired(
                bucket,
                "object",
                Arc::new(move |_| {
                    // Acquiring the same transaction key again proves the first
                    // lease was released. Only its copied fence becomes stale.
                    expired_alias.store(true, Ordering::Release);
                    caller_lost.store(lose_caller, Ordering::Release);
                    captured_replay_started.store(true, Ordering::Release);
                }),
            );

            let (deleted, errors) = set
                .delete_objects(
                    bucket,
                    vec![ObjectToDelete {
                        object_name: "object".into(),
                        ..Default::default()
                    }],
                    ObjectOptions {
                        namespace_lock_fence: Some(caller_fence),
                        object_lock_config_snapshot: Some(Arc::new(ObjectLockConfigSnapshot::new(
                            ObjectLockConfigState::ConfirmedAbsent,
                        ))),
                        ..Default::default()
                    },
                )
                .await;
            assert!(
                replay_started.load(Ordering::Acquire),
                "missing metadata must take the deferred replay path"
            );
            assert!(
                released_alias_lost.load(Ordering::Acquire),
                "the completed batch alias must be marked lost"
            );
            if lose_caller {
                assert!(errors[0].is_some(), "losing the original caller fence must reject deferred replay");
                for (disk, original) in disks.iter().zip(before) {
                    let after = disk
                        .inspect_delete_transaction(bucket, "object", transaction)
                        .await
                        .expect("retained proof");
                    assert_eq!(
                        after.fingerprint, original.fingerprint,
                        "lost caller authority must preserve every prepared journal"
                    );
                }
            } else {
                assert!(
                    errors.iter().all(Option::is_none),
                    "released alias loss must not reject fresh recovery: {errors:?}"
                );
                assert!(
                    !deleted[0].found,
                    "fresh unversioned recovery commits absence before the idempotent batch retry"
                );
                for disk in &disks {
                    assert!(
                        discover(disk, bucket, Some("object"))
                            .await
                            .expect("final recovery evidence")
                            .is_empty()
                    );
                    assert_eq!(
                        disk.read_version("", bucket, "object", "", &ReadOptions::default())
                            .await
                            .expect_err("replayed delete must remove the source"),
                        DiskError::FileNotFound
                    );
                }
            }
            drop(signal_fence);
        }
    }

    #[tokio::test]
    async fn delete_recovery_corrupt_bucket_does_not_prevent_other_bucket_rollback() {
        let bad_bucket = "recover-corrupt-bucket";
        let good_bucket = "recover-good-bucket";
        let (dirs, store, disks) = fixture(bad_bucket).await;
        store
            .make_bucket(good_bucket, &MakeBucketOptions::default())
            .await
            .expect("create healthy bucket");
        put(&store, bad_bucket, &[61; 4096], false).await;
        let bad_transaction = applied(&store, &disks, bad_bucket, None, disks.len()).await;
        let good_bytes = vec![62; 4096];
        put(&store, good_bucket, &good_bytes, false).await;
        applied(&store, &disks, good_bucket, None, disks.len()).await;
        for dir in &dirs {
            std::fs::write(
                dir.path()
                    .join(bad_bucket)
                    .join("object")
                    .join(bad_transaction.to_string())
                    .join("delete-prepared"),
                b"{broken",
            )
            .expect("inject malformed preparation journal");
        }

        let failed = store
            .recover_delete_transactions(bad_bucket, &HealOpts::default())
            .await
            .expect("bucket corruption is an explicit partial recovery result");
        assert_eq!(failed.delete_recovery.expect("recovery status").failed, 1);
        let error = store
            .recover_delete_transactions_at_incarnation(bad_bucket, Some("object"), None, &HealOpts::default())
            .await
            .expect_err("exact object recovery must retain typed corruption");
        assert!(matches!(error, crate::error::StorageError::FileCorrupt));
        let recovered = store
            .recover_delete_transactions(good_bucket, &HealOpts::default())
            .await
            .expect("another bucket must remain recoverable");
        assert!(recovered.detail.contains("rolled_back=1"));
        assert!(!recovered.delete_recovery.expect("healthy recovery status").has_unresolved());
        assert_eq!(body(&store, good_bucket, None).await, good_bytes);
    }

    #[tokio::test]
    async fn delete_recovery_corrupt_key_does_not_prevent_other_key_rollback() {
        let bucket = "recover-corrupt-key";
        let (dirs, store, disks) = fixture(bucket).await;
        let healthy_bytes = vec![63; 4096];
        put(&store, bucket, &healthy_bytes, false).await;
        applied(&store, &disks, bucket, None, disks.len()).await;
        let mut reader = PutObjReader::from_vec(vec![64; 4096]);
        store
            .put_object(
                bucket,
                "a-corrupt",
                &mut reader,
                &ObjectOptions {
                    write_completion: crate::object_api::WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("write corrupt-key fixture");
        let transaction = Uuid::new_v4();
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("bucket identity");
        for disk in &disks {
            disk.delete_version_recoverable(
                bucket,
                "a-corrupt",
                FileInfo {
                    name: "a-corrupt".to_owned(),
                    ..Default::default()
                },
                false,
                DeleteOptions {
                    old_data_dir: Some(transaction),
                    delete_recovery_bucket_incarnation: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("prepare corrupt-key fixture");
        }
        let damaged = dirs[0]
            .path()
            .join(bucket)
            .join("a-corrupt")
            .join(transaction.to_string())
            .join(crate::disk::STORAGE_FORMAT_FILE_BACKUP);
        std::fs::write(&damaged, b"corrupt metadata").expect("damage one backup while retaining discoverable journal");

        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("per-key corruption must not abort the remaining prefix");

        assert_eq!(report.delete_recovery.expect("recovery status").failed, 1);
        assert!(report.detail.contains("rolled_back=1"));
        assert_eq!(std::fs::read(damaged).expect("corrupt proof is retained"), b"corrupt metadata");
        assert_eq!(body(&store, bucket, None).await, healthy_bytes);
    }

    #[tokio::test]
    async fn delete_recovery_offline_bucket_reports_pending_without_hiding_exact_object_error() {
        let bucket = "recover-offline-status";
        let (_dirs, store, _disks) = fixture(bucket).await;
        store.pools[0].disk_set[0].disks.write().await[3] = None;

        let result = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("offline recovery must expose a pending summary even with no discovered transactions");
        let status = result.delete_recovery.expect("offline status is retained");
        assert_eq!(status.pending, 1);
        assert_eq!(status.failed, 0);
        assert!(status.has_unresolved());
        let error = store
            .recover_delete_transactions_at_incarnation(bucket, Some("object"), None, &HealOpts::default())
            .await
            .expect_err("exact-object workers require a typed retry error");
        assert!(matches!(error, crate::error::StorageError::ErasureReadQuorum));
    }

    fn snapshot(root: &std::path::Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
        let mut files = BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    files.insert(path.strip_prefix(root).unwrap().to_path_buf(), std::fs::read(path).unwrap());
                }
            }
        }
        files
    }

    #[tokio::test]
    async fn delete_recovery_marker_source_heal_preserves_foreign_history_and_damaged_data() {
        for shape in ["live-backup", "foreign", "partial-history", "damaged-data", "causal-cycle"] {
            let bucket = "recover-marker-source-guards";
            let (dirs, store) = isolated_store_over_temp_disks().await;
            init_bucket_metadata_sys(store.clone(), Vec::new()).await;
            store
                .make_bucket(
                    bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.unwrap();
            let set = &store.pools[0].disk_set[0];
            let disks: Vec<_> = set.disk_inventory().await.into_iter().map(Option::unwrap).collect();
            let original = vec![71; 1024 * 1024];
            let oldest = put(&store, bucket, &original, true).await.unwrap();
            let first = put(&store, bucket, &original, true).await.unwrap();
            set.disks.write().await[3] = None;
            let payload = vec![72; 1024 * 1024];
            let source = put(&store, bucket, &payload, true).await.unwrap();
            store
                .delete_object(
                    bucket,
                    "object",
                    ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("publish marker while one member lacks the source version");
            set.disks.write().await[3] = Some(disks[3].clone());
            let mut first_info = disks[3]
                .read_version("", bucket, "object", &first.to_string(), &ReadOptions::default())
                .await
                .unwrap();
            match shape {
                "live-backup" => {
                    let root = dirs[3].path().join(bucket).join("object");
                    let backup = root.join(first_info.data_dir.unwrap().to_string()).join("xl.meta.bkp");
                    tokio::fs::write(backup, tokio::fs::read(root.join("xl.meta")).await.unwrap())
                        .await
                        .unwrap();
                }
                "foreign" => {
                    first_info.version_id = Some(Uuid::new_v4());
                    first_info.mod_time = Some(time::OffsetDateTime::now_utc());
                    disks[3].write_metadata("", bucket, "object", first_info).await.unwrap();
                }
                "partial-history" => {
                    disks[3]
                        .delete_version(
                            bucket,
                            "object",
                            FileInfo {
                                name: "object".into(),
                                version_id: Some(oldest),
                                ..Default::default()
                            },
                            false,
                            DeleteOptions::default(),
                        )
                        .await
                        .unwrap();
                }
                "damaged-data" => {
                    // Three metadata sources meet write quorum, but corrupting
                    // two shards leaves fewer than EC2+2's two required readers.
                    for (index, disk) in disks.iter().take(2).enumerate() {
                        let info = disk
                            .read_version("", bucket, "object", &source.to_string(), &ReadOptions::default())
                            .await
                            .unwrap();
                        let part = dirs[index]
                            .path()
                            .join(bucket)
                            .join("object")
                            .join(info.data_dir.unwrap().to_string())
                            .join("part.1");
                        let mut bytes = tokio::fs::read(&part).await.unwrap();
                        *bytes.last_mut().unwrap() ^= 1;
                        tokio::fs::write(part, bytes).await.unwrap();
                    }
                }
                "causal-cycle" => {
                    for dir in dirs.iter().take(3) {
                        let root = dir.path().join(bucket).join("object");
                        let mut found = 0;
                        for entry in std::fs::read_dir(root).unwrap() {
                            let entry = entry.unwrap();
                            let prepared = entry.path().join("delete-prepared");
                            if !prepared.is_file() {
                                continue;
                            }
                            let transaction = entry.file_name().to_str().unwrap().to_owned();
                            let mut journal: serde_json::Value =
                                serde_json::from_slice(&std::fs::read(&prepared).unwrap()).unwrap();
                            journal["predecessors"] = serde_json::json!([transaction]);
                            std::fs::write(prepared, serde_json::to_vec(&journal).unwrap()).unwrap();
                            let receipt = entry.path().join(crate::disk::local::DELETE_QUORUM_COMMITTED_FILE);
                            if receipt.is_file() {
                                let mut value: serde_json::Value =
                                    serde_json::from_slice(&std::fs::read(&receipt).unwrap()).unwrap();
                                value["journal"] = journal;
                                std::fs::write(receipt, serde_json::to_vec(&value).unwrap()).unwrap();
                            }
                            found += 1;
                        }
                        assert_eq!(found, 1, "one active marker journal must exercise prerequisite rejection");
                    }
                }
                _ => unreachable!(),
            }
            let before: Vec<_> = dirs
                .iter()
                .map(|dir| snapshot(&dir.path().join(bucket).join("object")))
                .collect();
            let result = store
                .heal_object_at_incarnation(
                    bucket,
                    "object",
                    &source.to_string(),
                    incarnation,
                    &HealOpts {
                        scan_mode: rustfs_heal_contracts::heal_channel::HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await;
            if shape == "live-backup" {
                assert!(result.unwrap().error.is_none(), "plain live backups cannot block source repair");
                assert_eq!(body(&store, bucket, Some(source)).await, payload);
                assert_eq!(body(&store, bucket, Some(first)).await, original);
            } else {
                assert!(result.is_err() || result.unwrap().error.is_some(), "{shape} must fail safely");
                for (dir, expected) in dirs.iter().zip(before) {
                    assert_eq!(snapshot(&dir.path().join(bucket).join("object")), expected, "{shape}");
                }
            }
        }
    }

    #[tokio::test]
    async fn delete_recovery_admin_prefix_excludes_other_keys_and_restores_hidden_match() {
        let bucket = "recover-prefix-scope";
        let (dirs, store, disks) = fixture(bucket).await;
        let original = vec![27; 4096];
        put(&store, bucket, &original, false).await;
        let transaction = applied(&store, &disks, bucket, None, disks.len()).await;
        let outside = store
            .recover_delete_prefix_at_incarnation(bucket, "other/", None, &HealOpts::default())
            .await
            .expect("inspect unrelated prefix");
        assert!(outside.is_none());
        for dir in &dirs {
            assert!(
                dir.path()
                    .join(bucket)
                    .join("object")
                    .join(transaction.to_string())
                    .join("xl.meta.bkp")
                    .exists()
            );
        }
        let matching = store
            .recover_delete_prefix_at_incarnation(bucket, "obj", None, &HealOpts::default())
            .await
            .expect("recover matching hidden key")
            .expect("report matching transaction");
        assert!(matching.detail.contains("rolled_back=1"));
        assert_eq!(body(&store, bucket, None).await, original);
    }

    #[tokio::test]
    async fn delete_recovery_set_commits_receipt_and_unblocks_bucket() {
        let bucket = "recover-committed";
        let (_dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![22; 1024 * 1024], false).await;
        let transaction = applied(&store, &disks, bucket, None, disks.len()).await;
        publish(&disks[0], bucket, transaction).await;
        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("complete certified delete");
        assert!(
            report.detail.contains("committed=1"),
            "certified replay must be reported: {}",
            report.detail
        );
        assert!(
            store
                .get_object_info(bucket, "object", &ObjectOptions::default())
                .await
                .is_err()
        );
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("non-force bucket deletion must succeed after proof GC");
    }

    #[tokio::test]
    async fn delete_recovery_committed_generation_finishes_beside_stale_write_backup() {
        let bucket = "recover-stale-write-backup";
        let (dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![81; 1024 * 1024], false).await;
        let originals: Vec<_> = dirs
            .iter()
            .map(|dir| snapshot(&dir.path().join(bucket).join("object")))
            .collect();
        let metadata: Vec<_> = originals
            .iter()
            .map(|files| {
                files
                    .get(std::path::Path::new(STORAGE_FORMAT_FILE))
                    .expect("old generation metadata")
                    .clone()
            })
            .collect();
        let old_data_dir = rustfs_filemeta::FileMeta::load(&metadata[0])
            .expect("decode old generation")
            .get_data_dirs()
            .expect("old generation data directory")
            .into_iter()
            .flatten()
            .next()
            .expect("external payload data directory");
        put(&store, bucket, &vec![82; 1024 * 1024], false).await;
        for (index, dir) in dirs.iter().enumerate() {
            let root = dir.path().join(bucket).join("object");
            if index == disks.len() - 1 {
                tokio::fs::remove_dir_all(&root)
                    .await
                    .expect("replace returning member with its old generation");
            }
            for (relative, bytes) in &originals[index] {
                if index != disks.len() - 1 && !relative.starts_with(old_data_dir.to_string()) {
                    continue;
                }
                let path = root.join(relative);
                tokio::fs::create_dir_all(path.parent().expect("source file parent"))
                    .await
                    .expect("restore old generation directory");
                tokio::fs::write(path, bytes).await.expect("restore old generation bytes");
            }
            if index != disks.len() - 1 {
                tokio::fs::write(root.join(old_data_dir.to_string()).join("xl.meta.bkp"), &metadata[index])
                    .await
                    .expect("retain stale write backup beside older payload");
            }
        }
        let transaction = applied(&store, &disks, bucket, None, disks.len()).await;
        for disk in &disks {
            publish(disk, bucket, transaction).await;
        }
        let incarnation = store
            .bucket_incarnation_id_from_disk(bucket)
            .await
            .expect("recovery bucket identity");
        let before_preview: Vec<_> = dirs
            .iter()
            .map(|dir| snapshot(&dir.path().join(bucket).join("object")))
            .collect();
        let mut preview_context = DeleteRecoveryContext::new(Some(incarnation), &|| true);
        preview_context.explicit_delete = true;
        preview_context.dry_run = true;
        let preview = store.pools[0].disk_set[0]
            .recover_delete_object(bucket, "object", preview_context)
            .await
            .expect("preview fresh removal beside unfinished certified receipts");
        assert_eq!(preview.reclaimed, 1, "fresh intent covers eligible legacy residue beside QuorumCommitted");
        assert_eq!(preview.committed, 1);
        assert_eq!(preview.blocked, 0);
        assert_eq!(preview.pending, 0);
        for (index, dir) in dirs.iter().enumerate() {
            assert_eq!(
                snapshot(&dir.path().join(bucket).join("object")),
                before_preview[index],
                "dry-run member {index}"
            );
        }
        let report = store.pools[0].disk_set[0]
            .recover_delete_object(bucket, "object", DeleteRecoveryContext::new(Some(incarnation), &|| true))
            .await
            .expect("an unrelated backup cannot prevent the certified transaction from finishing");
        assert_eq!(report.committed, 1, "the shared transaction finishes on all members");
        assert_eq!(report.pending, 0, "no certified payload remains pending");
        assert_eq!(report.blocked, 1, "the uncertified older backup remains explicit recovery work");
        for (index, disk) in disks.iter().enumerate() {
            let state = disk
                .inspect_delete_transaction(bucket, "object", transaction)
                .await
                .expect("inspect completed certified transaction");
            assert_eq!(state.state, DeleteRecoveryState::CommitFinished, "member {index}");
            let root = dirs[index].path().join(bucket).join("object");
            assert!(
                !root.join(STORAGE_FORMAT_FILE).exists(),
                "deleted metadata remains absent on member {index}"
            );
            if index != disks.len() - 1 {
                assert_eq!(
                    tokio::fs::read(root.join(old_data_dir.to_string()).join("xl.meta.bkp"))
                        .await
                        .expect("uncertified write backup survives"),
                    metadata[index],
                    "member {index} preserves the ambiguous older generation"
                );
            } else {
                assert!(
                    !root.join(old_data_dir.to_string()).exists(),
                    "the certified older member reclaims its payload"
                );
            }
        }
        for (index, dir) in dirs.iter().enumerate().take(disks.len() - 1) {
            let source = rustfs_filemeta::FileMeta::load(&metadata[index]).expect("decode backup before protection");
            let mut protected = rustfs_filemeta::FileMeta::new();
            for version in &source.versions {
                let mut info = version.into_fileinfo(bucket, "object", true).expect("backup version");
                info.metadata.insert("x-amz-object-lock-legal-hold".into(), "ON".into());
                protected.add_version(info).expect("protect ambiguous backup");
            }
            tokio::fs::write(
                dir.path()
                    .join(bucket)
                    .join("object")
                    .join(old_data_dir.to_string())
                    .join("xl.meta.bkp"),
                protected.marshal_msg().expect("encode protected backup"),
            )
            .await
            .expect("persist protected ambiguous generation");
        }
        let protected: Vec<_> = dirs
            .iter()
            .map(|dir| snapshot(&dir.path().join(bucket).join("object")))
            .collect();
        assert!(
            store.delete_object(bucket, "object", ObjectOptions::default()).await.is_err(),
            "an existing delete decision cannot authorize removing protected legacy metadata"
        );
        for (index, dir) in dirs.iter().enumerate() {
            assert_eq!(
                snapshot(&dir.path().join(bucket).join("object")),
                protected[index],
                "protected member {index}"
            );
        }
        for (index, dir) in dirs.iter().enumerate().take(disks.len() - 1) {
            tokio::fs::write(
                dir.path()
                    .join(bucket)
                    .join("object")
                    .join(old_data_dir.to_string())
                    .join("xl.meta.bkp"),
                &metadata[index],
            )
            .await
            .expect("restore unprotected backup fixture");
        }
        store
            .delete_object(bucket, "object", ObjectOptions::default())
            .await
            .expect("a fresh authorized delete reclaims safe absent-key legacy residue");
        for disk in &disks {
            assert!(
                discover(disk, bucket, Some("object"))
                    .await
                    .expect("inspect final recovery residue")
                    .is_empty(),
                "the fresh delete finalizes every retained decision"
            );
        }
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("the fully reclaimed object no longer blocks bucket removal");
    }

    #[tokio::test]
    async fn delete_recovery_set_handles_prepared_subset() {
        let bucket = "recover-subset";
        let (_dirs, store, disks) = fixture(bucket).await;
        let original = vec![23; 4096];
        put(&store, bucket, &original, false).await;
        applied(&store, &disks, bucket, None, 1).await;
        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("untouched peers remain valid evidence");
        assert!(report.detail.contains("rolled_back=1"));
        assert_eq!(body(&store, bucket, None).await, original);
    }

    #[tokio::test]
    async fn delete_recovery_set_adopts_untouched_returning_peer() {
        let bucket = "recover-untouched";
        let (_dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![24; 1024 * 1024], false).await;
        let transaction = applied(&store, &disks, bucket, None, 3).await;
        for disk in disks.iter().take(3) {
            publish(disk, bucket, transaction).await;
        }
        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("matching untouched generation must converge");
        assert!(report.detail.contains("committed=1"));
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("returning old replica must not leave bucket residue");
    }

    #[tokio::test]
    async fn delete_recovery_set_replays_partial_gc_with_empty_transaction_directory() {
        let bucket = "recover-empty-gc";
        let (dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &[24; 4096], false).await;
        let transaction = applied(&store, &disks, bucket, None, disks.len()).await;
        for (index, disk) in disks.iter().enumerate() {
            publish(disk, bucket, transaction).await;
            let state = disk.inspect_delete_transaction(bucket, "object", transaction).await.unwrap();
            let finished = disk
                .settle_delete_transaction(bucket, "object", transaction, state.fingerprint, DeleteRecoveryAction::FinishCommit)
                .await
                .unwrap();
            if index > 0 {
                disk.settle_delete_transaction(
                    bucket,
                    "object",
                    transaction,
                    finished.fingerprint,
                    DeleteRecoveryAction::FinalizeCommit,
                )
                .await
                .unwrap();
            }
        }
        // A crash can persist proof removal before the final empty rmdir.
        let empty = dirs[3].path().join(bucket).join("object").join(transaction.to_string());
        tokio::fs::create_dir_all(&empty).await.unwrap();
        for _ in 0..2 {
            store
                .recover_delete_transactions(bucket, &HealOpts::default())
                .await
                .expect("the remaining receipt must settle every participant without a fresh delete");
            for disk in &disks {
                assert!(discover(disk, bucket, Some("object")).await.unwrap().is_empty());
                assert!(
                    disk.inspect_delete_transaction(bucket, "object", transaction)
                        .await
                        .unwrap()
                        .current_digest
                        .is_none()
                );
            }
        }
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("ordinary bucket removal must succeed after replay");
    }

    #[tokio::test]
    async fn delete_recovery_tracks_only_observed_removed_version() {
        let bucket = "recover-actual-removal";
        let (_dirs, store, disks) = fixture(bucket).await;
        let target = put(&store, bucket, &[41; 4096], true).await.unwrap();
        let retained = put(&store, bucket, &[42; 4096], true).await.unwrap();
        let transaction = applied(&store, &disks, bucket, Some(target), 3).await;
        publish(&disks[0], bucket, transaction).await;
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.unwrap();
        let set = &store.pools[0].disk_set[0];
        let report = set
            .recover_delete_object(bucket, "object", DeleteRecoveryContext::new(Some(incarnation), &|| true))
            .await
            .unwrap();
        assert_eq!(report.pending, 0);
        assert_eq!(report.blocked, 0);
        assert_eq!(report.removed_versions, HashSet::from([target]));
        assert_eq!(body(&store, bucket, Some(retained)).await, vec![42; 4096]);
        let replay = set
            .recover_delete_object(bucket, "object", DeleteRecoveryContext::new(Some(incarnation), &|| true))
            .await
            .unwrap();
        assert!(replay.removed_versions.is_empty(), "receipt replay cannot claim a second removal");
    }

    #[tokio::test]
    async fn delete_recovery_set_retains_decisions_until_offline_peer_returns() {
        let bucket = "recover-offline";
        let (dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![25; 4096], false).await;
        let transaction = applied(&store, &disks, bucket, None, 4).await;
        let set = &store.pools[0].disk_set[0];
        set.disks.write().await[3] = None;
        let available = set.disk_inventory().await;
        set.publish_delete_commit(bucket, "object", transaction, &available, 3, DeleteRecoveryContext::new(None, &|| true))
            .await
            .expect("commit quorum remains available");
        for dir in dirs.iter().take(3) {
            assert!(
                dir.path()
                    .join(bucket)
                    .join("object")
                    .join(transaction.to_string())
                    .join(crate::disk::local::DELETE_QUORUM_COMMITTED_FILE)
                    .exists(),
                "decision must survive an unavailable participant"
            );
        }
        set.disks.write().await[3] = Some(disks[3].clone());
        store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("returning prepared peer must roll forward");
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("proof collection follows complete convergence");
    }

    #[tokio::test]
    async fn delete_recovery_set_never_resurrects_prepared_old_null_after_new_delete() {
        let bucket = "recover-null-history";
        let (_dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![26; 4096], false).await;
        applied(&store, &disks, bucket, None, 4).await;
        let set = &store.pools[0].disk_set[0];
        set.disks.write().await[3] = None;
        put(&store, bucket, &vec![27; 4096], false).await;
        store
            .delete_object(bucket, "object", ObjectOptions::default())
            .await
            .expect("later visible generation can commit with quorum");
        set.disks.write().await[3] = Some(disks[3].clone());
        store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("later deletion must supersede hidden old null residue");
        assert!(
            store
                .get_object_info(bucket, "object", &ObjectOptions::default())
                .await
                .is_err(),
            "old bytes must never become visible again"
        );
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("both generations' transactions converge");
    }

    #[tokio::test]
    async fn delete_recovery_set_missing_version_does_not_ack_hidden_rollback() {
        let bucket = "recover-version-history";
        let (_dirs, store, disks) = fixture(bucket).await;
        metadata_sys::update_in(&store.ctx, bucket, crate::bucket::metadata::BUCKET_VERSIONING_CONFIG,
            br#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#.to_vec())
            .await.expect("enable versioning");
        let first = put(&store, bucket, &vec![28; 4096], true).await.expect("first version");
        let survivor = vec![29; 4096];
        let second = put(&store, bucket, &survivor, true).await.expect("second version");
        applied(&store, &disks, bucket, Some(first), 4).await;
        let set = &store.pools[0].disk_set[0];
        set.disks.write().await[3] = None;
        let opts = ObjectOptions {
            version_id: Some(first.to_string()),
            versioned: true,
            ..Default::default()
        };
        assert!(
            store.delete_object(bucket, "object", opts.clone()).await.is_err(),
            "hidden prepared version cannot receive an idempotent success while a peer is unavailable"
        );
        set.disks.write().await[3] = Some(disks[3].clone());
        store
            .delete_object(bucket, "object", opts)
            .await
            .expect("recover then execute the currently authorized exact-version delete");
        assert!(
            store
                .get_object_info(
                    bucket,
                    "object",
                    &ObjectOptions {
                        version_id: Some(first.to_string()),
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(body(&store, bucket, Some(second)).await, survivor);
    }

    #[tokio::test]
    async fn delete_recovery_set_dry_run_preserves_every_file() {
        let bucket = "recover-dry-run";
        let (dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![30; 4096], false).await;
        let transaction = applied(&store, &disks, bucket, None, 4).await;
        let before = disks[0]
            .inspect_delete_transaction(bucket, "object", transaction)
            .await
            .expect("before inspection");
        let report = store
            .recover_delete_transactions(
                bucket,
                &HealOpts {
                    dry_run: true,
                    ..Default::default()
                },
            )
            .await
            .expect("dry-run report");
        assert!(report.detail.contains("dry run"));
        let after = disks[0]
            .inspect_delete_transaction(bucket, "object", transaction)
            .await
            .expect("after inspection");
        assert_eq!(before.fingerprint, after.fingerprint, "dry run may not modify recovery evidence");
        for dir in dirs {
            assert!(!dir.path().join(bucket).join("object").join(STORAGE_FORMAT_FILE).exists());
        }
    }
    #[tokio::test]
    async fn delete_recovery_set_admin_object_restores_hidden_metadata() {
        let bucket = "recover-admin-object";
        let (_dirs, store, disks) = fixture(bucket).await;
        let payload = vec![41; 4096];
        put(&store, bucket, &payload, false).await;
        applied(&store, &disks, bucket, None, 4).await;
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("bucket identity");
        let result = store
            .heal_object_at_incarnation(bucket, "object", "", incarnation, &HealOpts::default())
            .await
            .expect("admin object recovery");
        assert!(result.error.is_none(), "recovered object should heal cleanly: {:?}", result.error);
        assert_eq!(body(&store, bucket, None).await, payload);
    }

    #[tokio::test]
    async fn delete_recovery_current_version_delete_preserves_legacy_on_missing_participant() {
        use crate::storage_api_contracts::object::ObjectToDelete;
        for batch in [false, true] {
            let bucket = "recover-current-with-legacy";
            let (dirs, store, disks) = fixture(bucket).await;
            let original = vec![44; 1024 * 1024];
            let old_version = put(&store, bucket, &original, true).await.expect("historical version");
            let legacy = Uuid::new_v4();
            disks[3]
                .delete_version(
                    bucket,
                    "object",
                    FileInfo {
                        name: "object".into(),
                        version_id: Some(old_version),
                        ..Default::default()
                    },
                    false,
                    DeleteOptions {
                        old_data_dir: Some(legacy),
                        ..Default::default()
                    },
                )
                .await
                .expect("historical disk-local mutation without a set decision");
            let backup = dirs[3]
                .path()
                .join(bucket)
                .join("object")
                .join(legacy.to_string())
                .join("xl.meta.bkp");
            let before = tokio::fs::read(&backup).await.expect("historical backup");
            let set = &store.pools[0].disk_set[0];
            set.disks.write().await[3] = None;
            let current = put(&store, bucket, &vec![45; 4096], true)
                .await
                .expect("current version at quorum");
            set.disks.write().await[3] = Some(disks[3].clone());
            if batch {
                let (_, errors) = store
                    .delete_objects(
                        bucket,
                        vec![ObjectToDelete {
                            object_name: "object".into(),
                            version_id: Some(current),
                            ..Default::default()
                        }],
                        ObjectOptions {
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await;
                assert!(errors.iter().all(Option::is_none), "current deletion at quorum: {errors:?}");
            } else {
                store
                    .delete_object(
                        bucket,
                        "object",
                        ObjectOptions {
                            versioned: true,
                            version_id: Some(current.to_string()),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("current deletion at quorum");
            }
            assert_eq!(tokio::fs::read(&backup).await.expect("legacy backup retained"), before);
            let state = disks[3]
                .inspect_delete_transaction(bucket, "object", legacy)
                .await
                .expect("legacy inspection");
            assert_eq!(state.state, DeleteRecoveryState::LegacyAmbiguous);
            assert_eq!(body(&store, bucket, Some(old_version)).await, original);
        }
    }

    #[tokio::test]
    async fn delete_recovery_batch_with_missing_version_recovers_after_preparation_failure() {
        use crate::disk::RUSTFS_META_TMP_BUCKET;
        use crate::storage_api_contracts::object::ObjectToDelete;

        let bucket = "recover-batch-missing-version";
        let (dirs, store, disks) = fixture(bucket).await;
        let deleted = put(&store, bucket, &vec![48; 4096], true).await.expect("version to delete");
        let remaining = vec![49; 4096];
        let retained = put(&store, bucket, &remaining, true).await.expect("version to retain");
        let missing = Uuid::new_v4();
        let temporary = dirs[3].path().join(RUSTFS_META_TMP_BUCKET);
        let saved = dirs[3].path().join("saved-delete-tmp");
        tokio::fs::rename(&temporary, &saved)
            .await
            .expect("save returning disk's staging directory");
        tokio::fs::write(&temporary, b"block preparation")
            .await
            .expect("fail staging after the recovery preflight");

        let (objects, errors) = store
            .delete_objects(
                bucket,
                [deleted, missing]
                    .into_iter()
                    .map(|version| ObjectToDelete {
                        object_name: "object".into(),
                        version_id: Some(version),
                        ..Default::default()
                    })
                    .collect(),
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(objects.len(), 2);
        assert!(errors.iter().all(Option::is_none), "batch deletion succeeds at quorum: {errors:?}");
        assert!(
            disks[3]
                .discover_delete_transactions(bucket, Some("object"), None, 100)
                .await
                .expect("inspect failed preparation")
                .transactions
                .is_empty(),
            "returning disk must adopt the quorum decision without a local preparation"
        );
        tokio::fs::remove_file(&temporary).await.expect("remove preparation failure");
        tokio::fs::rename(&saved, &temporary)
            .await
            .expect("restore returning disk staging");

        let result = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("recover batch containing a missing non-null version");
        assert!(result.detail.contains("blocked=0"), "{}", result.detail);
        assert_eq!(body(&store, bucket, Some(retained)).await, remaining);
        for version in [deleted, missing] {
            assert_eq!(
                disks[3]
                    .read_version(bucket, bucket, "object", &version.to_string(), &ReadOptions::default())
                    .await
                    .expect_err("returning disk must not retain or create a deleted version"),
                DiskError::FileVersionNotFound
            );
        }
        for disk in &disks {
            assert!(
                disk.discover_delete_transactions(bucket, Some("object"), None, 100)
                    .await
                    .expect("inspect converged batch")
                    .transactions
                    .is_empty(),
                "every participant must converge and release the decision"
            );
        }
    }

    #[tokio::test]
    async fn delete_recovery_dry_run_matches_adoption_of_already_absent_non_null_version() {
        let bucket = "recover-preview-non-null";
        let (_dirs, store, disks) = fixture(bucket).await;
        let first = put(&store, bucket, &vec![46; 4096], true).await.expect("first version");
        let remaining = vec![47; 4096];
        let second = put(&store, bucket, &remaining, true).await.expect("remaining version");
        disks[3]
            .delete_version(
                bucket,
                "object",
                FileInfo {
                    name: "object".into(),
                    version_id: Some(first),
                    ..Default::default()
                },
                false,
                DeleteOptions::default(),
            )
            .await
            .expect("returning peer already lacks the explicit version");
        let transaction = applied(&store, &disks, bucket, Some(first), 3).await;
        publish(&disks[0], bucket, transaction).await;
        let preview = store
            .recover_delete_transactions(
                bucket,
                &HealOpts {
                    dry_run: true,
                    ..Default::default()
                },
            )
            .await
            .expect("preview certified recovery");
        assert!(
            preview.detail.contains("committed=1") && preview.detail.contains("blocked=0"),
            "{}",
            preview.detail
        );
        let actual = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("recover certified removal");
        assert!(
            actual.detail.contains("committed=1") && actual.detail.contains("blocked=0"),
            "{}",
            actual.detail
        );
        assert_eq!(body(&store, bucket, Some(second)).await, remaining);
        assert!(
            store
                .get_object_info(
                    bucket,
                    "object",
                    &ObjectOptions {
                        version_id: Some(first.to_string()),
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn delete_recovery_set_legacy_requires_explicit_removal() {
        let bucket = "recover-legacy-remove";
        let (dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![42; 4096], false).await;
        let transaction = Uuid::new_v4();
        for disk in &disks {
            disk.delete_version(
                bucket,
                "object",
                FileInfo {
                    name: "object".to_owned(),
                    ..Default::default()
                },
                false,
                DeleteOptions {
                    old_data_dir: Some(transaction),
                    ..Default::default()
                },
            )
            .await
            .expect("create undecided historical delete shape");
        }
        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("automatic report");
        assert!(report.detail.contains("blocked=1"));
        for dir in &dirs {
            assert!(
                dir.path()
                    .join(bucket)
                    .join("object")
                    .join(transaction.to_string())
                    .join("xl.meta.bkp")
                    .exists()
            );
        }
        let dry = store
            .recover_delete_transactions(
                bucket,
                &HealOpts {
                    dry_run: true,
                    remove: true,
                    ..Default::default()
                },
            )
            .await
            .expect("authorized-removal dry run");
        assert!(dry.detail.contains("reclaimed=1"));
        for dir in &dirs {
            assert!(dir.path().join(bucket).join("object").exists());
        }
        let report = store
            .recover_delete_transactions(
                bucket,
                &HealOpts {
                    remove: true,
                    ..Default::default()
                },
            )
            .await
            .expect("explicit admin removal");
        assert!(report.detail.contains("reclaimed=1"));
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("non-force bucket deletion");
    }

    #[tokio::test]
    async fn delete_recovery_set_old_null_decision_cannot_commit_new_undecided_generation() {
        let bucket = "recover-inverse-null-history";
        let (_dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![51; 4096], false).await;
        let old_transaction = applied(&store, &disks, bucket, None, 4).await;
        let set = &store.pools[0].disk_set[0];
        set.disks.write().await[3] = None;
        set.publish_delete_commit(
            bucket,
            "object",
            old_transaction,
            &set.disk_inventory().await,
            3,
            DeleteRecoveryContext::new(None, &|| true),
        )
        .await
        .expect("old generation decision while peer offline");
        let survivor = vec![52; 4096];
        put(&store, bucket, &survivor, false).await;
        let new_transaction = applied(&store, &disks, bucket, None, 3).await;
        set.disks.write().await[3] = Some(disks[3].clone());
        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("causal recovery across old receipt");
        assert!(report.detail.contains("rolled_back=1"));
        assert_eq!(
            body(&store, bucket, None).await,
            survivor,
            "an old null-version receipt cannot authorize deleting a later generation"
        );
        for disk in &disks {
            assert_eq!(
                disk.inspect_delete_transaction(bucket, "object", new_transaction)
                    .await
                    .expect("new transaction settled")
                    .state,
                DeleteRecoveryState::Absent
            );
        }
        store
            .delete_object(bucket, "object", ObjectOptions::default())
            .await
            .expect("delete recovered generation explicitly");
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("all proof is eventually collected");
    }

    #[tokio::test]
    async fn delete_recovery_set_ordinary_missing_single_and_batch_keep_quorum_semantics() {
        let bucket = "recover-missing-quorum";
        let (_dirs, store, disks) = fixture(bucket).await;
        let set = &store.pools[0].disk_set[0];
        set.disks.write().await[3] = None;
        let error = store
            .delete_object(bucket, "single-missing", ObjectOptions::default())
            .await
            .expect_err("ordinary missing storage result");
        assert!(
            crate::error::is_err_object_not_found(&error),
            "S3 may translate missing key into idempotent204; not a bucket/quorum error: {error}"
        );
        let objects = ["batch-one", "batch-two"]
            .into_iter()
            .map(|name| crate::storage_api_contracts::object::ObjectToDelete {
                object_name: name.to_owned(),
                ..Default::default()
            })
            .collect();
        let (deleted, errors) = store.delete_objects(bucket, objects, ObjectOptions::default()).await;
        assert!(
            errors.iter().all(Option::is_none),
            "ordinary batch no-op must succeed at available quorum: {errors:?}"
        );
        assert_eq!(deleted.len(), 2);
        for disk in disks.iter().take(3) {
            for key in ["single-missing", "batch-one", "batch-two"] {
                assert!(
                    disk.discover_delete_transactions(bucket, Some(key), None, 128)
                        .await
                        .expect("no-op leaves no recovery journals")
                        .transactions
                        .is_empty()
                );
            }
        }
        set.disks.write().await[3] = Some(disks[3].clone());
        let payload = vec![53; 4096];
        put(&store, bucket, &payload, false).await;
        assert_eq!(body(&store, bucket, None).await, payload);
    }
    #[tokio::test]
    async fn delete_recovery_set_fresh_delete_resolves_unknown_null_predecessor() {
        let bucket = "recover-unknown-null-history";
        let (_dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![54; 4096], false).await;
        let transaction = Uuid::new_v4();
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("bucket identity");
        disks[3]
            .delete_version_recoverable(
                bucket,
                "object",
                FileInfo {
                    name: "object".to_owned(),
                    ..Default::default()
                },
                false,
                DeleteOptions {
                    old_data_dir: Some(transaction),
                    delete_recovery_bucket_incarnation: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("only future-offline peer prepared old delete");
        let set = &store.pools[0].disk_set[0];
        set.disks.write().await[3] = None;
        put(&store, bucket, &vec![55; 4096], false).await;
        store
            .delete_object(bucket, "object", ObjectOptions::default())
            .await
            .expect("later generation deleted at quorum");
        set.disks.write().await[3] = Some(disks[3].clone());
        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("unknown causal history is reported");
        assert!(
            report.detail.contains("blocked=1"),
            "automatic recovery cannot invent causality: {}",
            report.detail
        );
        store
            .delete_object(bucket, "object", ObjectOptions::default())
            .await
            .expect("fresh authorized delete certifies current absence");
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("fresh intent drains all proven unprotected null history");
    }
    #[test]
    fn delete_recovery_network_errors_do_not_mask_application_corruption() {
        assert!(unavailable(&DiskError::RemoteClientUnavailable(
            "transport error: connection refused".to_owned()
        )));
        assert!(unavailable(&DiskError::from(tonic::Status::unavailable("peer disconnected"))));
        assert!(
            unavailable(&DiskError::FaultyDisk),
            "a drive disabled by health cannot vote in recovery inspection"
        );
        assert!(!unavailable(&DiskError::FileCorrupt));
        assert!(!unavailable(&DiskError::other("invalid journal containing connection refused")));
        assert!(!unavailable(&DiskError::from(tonic::Status::internal("connection refused"))));
        assert!(!unavailable(&DiskError::other("bucket unavailable-logs has corrupt metadata")));
    }

    #[tokio::test]
    async fn delete_recovery_chained_committed_versions_finish_after_current_metadata_disappears() {
        let bucket = "recover-chained-versions";
        let (dirs, store, disks) = fixture(bucket).await;
        let first = put(&store, bucket, &vec![61; 1024 * 1024], true)
            .await
            .expect("first version");
        let second = put(&store, bucket, &vec![62; 4096], true).await.expect("second version");
        let set = &store.pools[0].disk_set[0];
        let returning_key = dirs[3].path().join(bucket).join("object");
        let before_outage = snapshot(&returning_key);
        // A missing slot can be renewed by the endpoint monitor. Keep its
        // original identity attached while rejecting I/O until explicit return.
        disks[3].close().await.expect("stop returning drive's health monitor");
        disks[3].force_offline_for_test();
        set.connect_disks().await;
        assert!(Arc::ptr_eq(set.disks.read().await[3].as_ref().unwrap(), &disks[3]));
        assert_eq!(disks[3].runtime_state(), crate::disk::health_state::RuntimeDriveHealthState::Offline);
        assert!(matches!(discover(&disks[3], bucket, Some("object")).await, Err(DiskError::FaultyDisk)));
        for version in [first, second] {
            store
                .delete_object(
                    bucket,
                    "object",
                    ObjectOptions {
                        versioned: true,
                        version_id: Some(version.to_string()),
                        ..Default::default()
                    },
                )
                .await
                .expect("commit each explicit version at quorum while peer is absent");
        }
        assert!(
            store
                .get_object_info(bucket, "object", &ObjectOptions::default())
                .await
                .is_err()
        );
        assert_eq!(snapshot(&returning_key), before_outage, "offline peer retains both deleted versions");
        assert_eq!(disks[3].runtime_state(), crate::disk::health_state::RuntimeDriveHealthState::Offline);
        disks[3].reset_health_for_store_init_retry();
        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("older certified target may be superseded by a later certified absence");
        assert!(
            report.detail.contains("committed=2"),
            "both retained decisions must converge: {}",
            report.detail
        );
        for version in [first, second] {
            assert!(
                store
                    .get_object_info(
                        bucket,
                        "object",
                        &ObjectOptions {
                            version_id: Some(version.to_string()),
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err(),
                "certified cleanup must not resurrect either deleted version"
            );
        }
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("no retained version decision may keep the empty bucket nonempty");
    }

    #[tokio::test]
    async fn delete_recovery_partial_batch_receipt_handles_missing_versions() {
        use crate::disk::RUSTFS_META_TMP_BUCKET;
        use crate::set_disk::ops::object::DeleteCleanupBarrier;
        use crate::storage_api_contracts::object::ObjectToDelete;

        for include_missing in [false, true] {
            let bucket = "recover-prepared-batch-missing-versions";
            let (dirs, store, disks) = fixture(bucket).await;
            metadata_sys::update_in(
                &store.ctx,
                bucket,
                crate::bucket::metadata::BUCKET_VERSIONING_CONFIG,
                b"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
            )
            .await
            .expect("enable supported versioned bucket");
            let first = put(&store, bucket, &vec![81; 1024 * 1024], true)
                .await
                .expect("first version");
            let second = put(&store, bucket, &vec![82; 4096], true).await.expect("retained version");
            let missing = Uuid::new_v4();
            let barrier = DeleteCleanupBarrier::install(bucket);
            let deleting = store.clone();
            let task = tokio::spawn(async move {
                let mut versions = vec![first];
                if include_missing {
                    versions.push(missing);
                }
                deleting
                    .delete_objects(
                        bucket,
                        versions
                            .into_iter()
                            .map(|version| ObjectToDelete {
                                object_name: "object".into(),
                                version_id: Some(version),
                                ..Default::default()
                            })
                            .collect(),
                        ObjectOptions {
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
            });
            barrier.wait_until_paused().await;
            let transactions = disks[0]
                .discover_delete_transactions(bucket, Some("object"), None, 100)
                .await
                .expect("discover batch preparation")
                .transactions;
            assert_eq!(transactions.len(), 1);
            let transaction = transactions[0].transaction;
            for disk in &disks {
                let state = disk
                    .inspect_delete_transaction(bucket, "object", transaction)
                    .await
                    .expect("inspect prepared batch");
                assert_eq!(state.state, DeleteRecoveryState::NewPrepared);
                let journal = state.journal.expect("prepared journal");
                assert!(journal.original_digest.is_some());
                assert!(journal.original_versions.iter().all(|v| v.version_id != Some(missing)));
            }
            // Fail durable receipt staging on two participants after metadata quorum.
            for dir in dirs.iter().skip(2) {
                tokio::fs::rename(dir.path().join(RUSTFS_META_TMP_BUCKET), dir.path().join("saved-receipt-staging"))
                    .await
                    .expect("save receipt staging directory");
                tokio::fs::write(dir.path().join(RUSTFS_META_TMP_BUCKET), b"receipt staging I/O failure")
                    .await
                    .expect("inject receipt staging failure");
            }
            barrier.release();
            let (_, errors) = task.await.expect("batch deletion must join");
            drop(barrier);
            assert_eq!(errors.len(), if include_missing { 2 } else { 1 });
            assert!(errors.iter().all(Option::is_some), "partial receipts are indeterminate: {errors:?}");
            let mut receipts = 0;
            let mut prepared = 0;
            for disk in &disks {
                match disk
                    .inspect_delete_transaction(bucket, "object", transaction)
                    .await
                    .expect("partial decision")
                    .state
                {
                    DeleteRecoveryState::QuorumCommitted => receipts += 1,
                    DeleteRecoveryState::NewPrepared => prepared += 1,
                    state => panic!("unexpected post-publication state: {state:?}"),
                }
            }
            assert_eq!((receipts, prepared), (2, 2));
            for dir in dirs.iter().skip(2) {
                tokio::fs::remove_file(dir.path().join(RUSTFS_META_TMP_BUCKET))
                    .await
                    .expect("remove injected staging failure");
                tokio::fs::rename(dir.path().join("saved-receipt-staging"), dir.path().join(RUSTFS_META_TMP_BUCKET))
                    .await
                    .expect("restore receipt staging");
            }
            store
                .delete_object(
                    bucket,
                    "object",
                    ObjectOptions {
                        versioned: true,
                        version_id: Some(second.to_string()),
                        ..Default::default()
                    },
                )
                .await
                .expect("later version deletion succeeds");
            store
                .recover_delete_transactions(bucket, &HealOpts::default())
                .await
                .expect("certified batch must recover after later version deletion");
            for disk in &disks {
                assert!(
                    disk.discover_delete_transactions(bucket, Some("object"), None, 100)
                        .await
                        .expect("settled transaction inventory")
                        .transactions
                        .is_empty()
                );
            }
            store
                .delete_bucket(bucket, &DeleteBucketOptions::default())
                .await
                .expect("settled receipts must not block removal of the empty bucket");
        }
    }

    #[tokio::test]
    async fn delete_recovery_partial_receipt_converges_after_later_version_absence() {
        let bucket = "recover-partial-chained-versions";
        let (dirs, store, disks) = fixture(bucket).await;
        let first = put(&store, bucket, &vec![63; 1024 * 1024], true)
            .await
            .expect("first version");
        let second = put(&store, bucket, &vec![64; 4096], true).await.expect("second version");
        let transaction = applied(&store, &disks, bucket, Some(first), 4).await;
        // A crash after the first durable decision leaves other copies prepared.
        publish(&disks[0], bucket, transaction).await;
        let later = applied(&store, &disks, bucket, Some(second), 4).await;
        publish(&disks[0], bucket, later).await;
        let dry = store
            .recover_delete_transactions(
                bucket,
                &HealOpts {
                    dry_run: true,
                    ..Default::default()
                },
            )
            .await
            .expect("dry-run uses the same certified prepared-copy authority");
        assert!(
            dry.detail.contains("committed=2") && dry.detail.contains("blocked=0"),
            "accurate dry-run: {}",
            dry.detail
        );
        for dir in &dirs {
            for id in [transaction, later] {
                assert!(
                    dir.path()
                        .join(bucket)
                        .join("object")
                        .join(id.to_string())
                        .join("delete-prepared")
                        .exists(),
                    "dry-run must retain every preparation record"
                );
            }
        }
        store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("same-transaction receipt certifies each prepared absent copy");
        for dir in &dirs {
            assert!(!dir.path().join(bucket).join("object").exists(), "all related proof must converge");
        }
        for version in [first, second] {
            assert!(
                store
                    .get_object_info(
                        bucket,
                        "object",
                        &ObjectOptions {
                            version_id: Some(version.to_string()),
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err(),
                "no erased version may be recreated by receipt adoption"
            );
        }
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("non-force bucket removal");
    }

    #[tokio::test]
    async fn delete_recovery_causal_cycle_rejects_before_mutation_and_retains_proof() {
        let bucket = "recover-causal-cycle";
        let (dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![58; 4096], false).await;
        let first = applied(&store, &disks, bucket, None, 4).await;
        put(&store, bucket, &vec![59; 4096], false).await;
        let second = applied(&store, &disks, bucket, None, 4).await;
        let mut evidence = Vec::new();
        for (dir, disk) in dirs.iter().zip(&disks) {
            let root = dir.path().join(bucket).join("object");
            let mut journal = disk
                .inspect_delete_transaction(bucket, "object", first)
                .await
                .expect("inspect first preparation")
                .journal
                .expect("original journal");
            journal.predecessors.push(second);
            let path = root.join(first.to_string()).join("delete-prepared");
            std::fs::write(&path, serde_json::to_vec(&journal).expect("encode cyclic fixture")).expect("write cyclic edge");
            for transaction in [first, second] {
                for file in ["delete-prepared", "xl.meta.bkp"] {
                    let path = root.join(transaction.to_string()).join(file);
                    evidence.push((path.clone(), std::fs::read(path).expect("capture untouched proof")));
                }
            }
        }
        let error = store
            .recover_delete_transactions_at_incarnation(bucket, Some("object"), None, &HealOpts::default())
            .await
            .expect_err("cyclic history cannot settle");
        assert!(
            matches!(error, crate::error::StorageError::FileCorrupt),
            "typed corruption must reject cyclic evidence: {error:?}"
        );
        let report = store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("a bucket report must retain its corrupt-key status");
        assert_eq!(report.delete_recovery.expect("corrupt-key status").failed, 1);
        for (path, bytes) in evidence {
            assert_eq!(std::fs::read(path).expect("all proof retained"), bytes);
        }
        for dir in &dirs {
            assert!(
                !dir.path().join(bucket).join("object").join(STORAGE_FORMAT_FILE).exists(),
                "cycle rejection must not restore or publish metadata"
            );
        }
    }

    #[tokio::test]
    async fn delete_recovery_set_later_null_decision_covers_unapplied_original_peer() {
        let bucket = "recover-unapplied-null-history";
        let (_dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![56; 1024 * 1024], false).await;
        // Every generated v4 UUID sorts before MAX, so the former UUID order
        // deterministically tried the newer decision before its predecessor.
        applied_at(&store, &disks, bucket, None, 3, Uuid::from_u128(u128::MAX)).await;
        let set = &store.pools[0].disk_set[0];
        set.disks.write().await[3] = None;
        put(&store, bucket, &vec![57; 1024 * 1024], false).await;
        store
            .delete_object(bucket, "object", ObjectOptions::default())
            .await
            .expect("later null decision");
        set.disks.write().await[3] = Some(disks[3].clone());
        store
            .recover_delete_transactions(bucket, &HealOpts::default())
            .await
            .expect("causal older original can be removed");
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("no stale old body or receipts remain");
    }

    #[tokio::test]
    async fn heal_mrf_inline_overwrite_does_not_strand_rollback_backup() {
        use crate::disk::STORAGE_FORMAT_FILE_BACKUP;
        use crate::disk::local::{DurabilityMode, durability_mode, effective_durability, inline_metadata_rollback_dir};
        use crate::layout::endpoint::Endpoint;
        use crate::layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
        use crate::layout::format::FormatV3;
        use crate::object_api::ShardIntegrityWriteMode;
        use crate::store::init_format::load_format_erasure;
        use rustfs_filemeta::FileMeta;
        use rustfs_heal_contracts::heal_channel::{HealScanMode, LEGACY_OBJECT_IDENTITY_UNVERIFIED_DETAIL};

        const CHILD_MANIFEST: &str = "RUSTFS_HEAL_MRF_RESTART_MANIFEST";
        const CHILD_PHASE: &str = "RUSTFS_HEAL_MRF_RESTART_PHASE";
        const TEST_NAME: &str =
            "set_disk::ops::delete_recovery::tests::heal_mrf_inline_overwrite_does_not_strand_rollback_backup";

        fn object_tree(root: &std::path::Path) -> BTreeMap<std::path::PathBuf, Option<Vec<u8>>> {
            let mut tree = BTreeMap::new();
            let mut pending = vec![root.to_path_buf()];
            while let Some(directory) = pending.pop() {
                for entry in std::fs::read_dir(directory).expect("read object tree") {
                    let path = entry.expect("object tree entry").path();
                    let relative = path
                        .strip_prefix(root)
                        .expect("object tree stays below its root")
                        .to_path_buf();
                    if path.is_dir() {
                        tree.insert(relative, None);
                        pending.push(path);
                    } else {
                        tree.insert(relative, Some(std::fs::read(path).expect("read original object bytes")));
                    }
                }
            }
            tree
        }

        #[derive(serde::Serialize, serde::Deserialize)]
        struct RestartDisk {
            root: std::path::PathBuf,
            slot: [i32; 3],
            disk_id: Uuid,
            format: FormatV3,
            transaction: Uuid,
            evidence: DeleteRecoveryInspection,
            tree: BTreeMap<std::path::PathBuf, Option<Vec<u8>>>,
        }

        #[derive(serde::Serialize, serde::Deserialize)]
        struct RestartFixture {
            writer_pid: u32,
            receipt_nonce: Uuid,
            incoming_inline: bool,
            legacy: bool,
            set_count: usize,
            drives_per_set: usize,
            cmd_line: String,
            platform: String,
            bucket: String,
            object: String,
            incarnation: Uuid,
            reads: Vec<(String, Option<String>, Vec<u8>)>,
            disks: Vec<RestartDisk>,
        }

        fn read_restart_fixture(path: &std::path::Path) -> RestartFixture {
            rmp_serde::from_slice(&std::fs::read(path).expect("read persisted MRF fixture"))
                .expect("decode persisted MRF fixture")
        }

        fn assert_owned_disk_root(owned_root: &std::path::Path, disk_root: &std::path::Path) {
            let owned_root = owned_root.canonicalize().expect("supervisor root must exist");
            let disk_root = disk_root.canonicalize().expect("original disk directory must exist");
            assert_ne!(disk_root, owned_root, "each disk must have its own directory");
            assert!(
                disk_root.starts_with(&owned_root),
                "restart disk must remain below the supervisor-owned root"
            );
        }

        async fn run_restart_process(phase: &str, owned_root: &std::path::Path, manifest_path: &std::path::Path) -> u32 {
            let child_phase = phase.to_string();
            let owned_root = owned_root.to_path_buf();
            let manifest_path = manifest_path.to_path_buf();
            let (child_pid, output, timed_out) = tokio::task::spawn_blocking(move || {
                let mut child = std::process::Command::new(std::env::current_exe().expect("current test executable"))
                    .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
                    .env(CHILD_PHASE, child_phase)
                    .env(CHILD_MANIFEST, manifest_path)
                    .env("TMPDIR", &owned_root)
                    .env("TMP", &owned_root)
                    .env("TEMP", &owned_root)
                    .env("RUSTFS_DURABILITY_MODE", "strict")
                    .env("RUSTFS_NEW_BUCKET_DURABILITY_MODE", "strict")
                    .env("RUSTFS_EXPERIMENTAL_DST_DIR_FSYNC_GROUP_COMMIT_ENABLE", "false")
                    .env("RUSTFS_EXPERIMENTAL_FILE_FDATASYNC_GROUP_COMMIT_ENABLE", "false")
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                    .expect("start independent MRF restart process");
                let child_pid = child.id();
                let mut stdout = child.stdout.take().expect("captured child stdout");
                let stdout = std::thread::spawn(move || {
                    let mut bytes = Vec::new();
                    std::io::Read::read_to_end(&mut stdout, &mut bytes).expect("drain child stdout");
                    bytes
                });
                let mut stderr = child.stderr.take().expect("captured child stderr");
                let stderr = std::thread::spawn(move || {
                    let mut bytes = Vec::new();
                    std::io::Read::read_to_end(&mut stderr, &mut bytes).expect("drain child stderr");
                    bytes
                });
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
                let (status, timed_out) = loop {
                    if let Some(status) = child.try_wait().expect("observe restart process") {
                        break (status, false);
                    }
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        break (child.wait().expect("reap timed-out restart process"), true);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(25));
                };
                (
                    child_pid,
                    std::process::Output {
                        status,
                        stdout: stdout.join().expect("child stdout reader"),
                        stderr: stderr.join().expect("child stderr reader"),
                    },
                    timed_out,
                )
            })
            .await
            .expect("restart process supervisor");
            println!("MRF {phase} child {child_pid}:\n{}", String::from_utf8_lossy(&output.stdout));
            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
            assert!(!timed_out, "MRF {phase} process exceeded its 60-second deadline");
            assert!(output.status.success(), "MRF {phase} process failed: {}", output.status);
            assert_ne!(child_pid, std::process::id(), "fixture must execute in a separate process");
            child_pid
        }

        let Some(phase) = std::env::var_os(CHILD_PHASE) else {
            for (phase, incoming_inline) in [("writer-inline", true), ("writer-external", false)] {
                let restart_dir = tempfile::tempdir().expect("supervisor-owned restart directory");
                let owned_root = restart_dir.path().to_path_buf();
                let manifest_path = owned_root.join("fixture.msgpack");
                assert!(!manifest_path.exists(), "each writer must produce a fresh manifest");
                let writer_pid = run_restart_process(phase, &owned_root, &manifest_path).await;
                let persisted = read_restart_fixture(&manifest_path);
                assert_eq!(persisted.writer_pid, writer_pid, "the manifest must come from the exited writer");
                assert_eq!(persisted.incoming_inline, incoming_inline, "each destination branch must run");
                for disk in &persisted.disks {
                    assert_owned_disk_root(&owned_root, &disk.root);
                }
                println!("MRF {phase} writer {writer_pid} exited successfully before verifier start");
                let verifier_pid = run_restart_process("verify", &owned_root, &manifest_path).await;
                assert_ne!(verifier_pid, writer_pid, "the verifier must not be the exited writer");
                assert_eq!(
                    std::fs::read_to_string(manifest_path.with_extension("verified"))
                        .expect("verifier must finish the selected test and write its unique receipt"),
                    format!("{}:{writer_pid}:{verifier_pid}", persisted.receipt_nonce)
                );
                for disk in &persisted.disks {
                    assert_eq!(
                        object_tree(&disk.root.join(&persisted.bucket).join(&persisted.object)),
                        disk.tree,
                        "writer exit and verifier exit must leave every original object file and directory unchanged"
                    );
                }
                println!(
                    "MRF_RESTART_RECEIPT {}",
                    serde_json::json!({
                        "case": phase,
                        "writer_pid": writer_pid,
                        "writer_exit": "success",
                        "writer_exit_before_verifier_start": true,
                        "verifier_pid": verifier_pid,
                        "verifier_exit": "success",
                        "nonce": persisted.receipt_nonce,
                    })
                );
            }
            return;
        };
        let phase = phase.to_str().expect("restart phase must be UTF-8");
        let manifest_path =
            std::path::PathBuf::from(std::env::var_os(CHILD_MANIFEST).expect("restart child requires its supervisor manifest"));
        let owned_root = manifest_path
            .parent()
            .expect("manifest must have a supervisor-owned parent")
            .to_path_buf();
        assert!(owned_root.is_dir(), "supervisor root must exist");

        if phase == "verify" {
            let persisted = read_restart_fixture(&manifest_path);
            for disk in &persisted.disks {
                assert_owned_disk_root(&owned_root, &disk.root);
            }
            assert_ne!(persisted.writer_pid, std::process::id(), "reopen must use a fresh process");
            assert_eq!(durability_mode(), DurabilityMode::Strict, "restarted process must actually use strict");
            let endpoints = EndpointServerPools(vec![PoolEndpoints {
                legacy: persisted.legacy,
                set_count: persisted.set_count,
                drives_per_set: persisted.drives_per_set,
                endpoints: Endpoints::from(
                    persisted
                        .disks
                        .iter()
                        .map(|disk| {
                            let mut endpoint = Endpoint::try_from(disk.root.to_str().expect("original UTF-8 disk path"))
                                .expect("original local endpoint");
                            endpoint.set_pool_index(usize::try_from(disk.slot[0]).expect("original pool index"));
                            endpoint.set_set_index(usize::try_from(disk.slot[1]).expect("original set index"));
                            endpoint.set_disk_index(usize::try_from(disk.slot[2]).expect("original disk index"));
                            endpoint
                        })
                        .collect::<Vec<_>>(),
                ),
                cmd_line: persisted.cmd_line.clone(),
                platform: persisted.platform.clone(),
            }]);
            let context = Arc::new(crate::runtime::instance::InstanceContext::new());
            crate::store::init_local_disks_with_instance_ctx(&context, endpoints.clone())
                .await
                .expect("reopen persisted disks with original pool, set and disk indices");
            let reopened = ECStore::new_with_instance_ctx(
                "127.0.0.1:0".parse().expect("test address"),
                endpoints.clone(),
                tokio_util::sync::CancellationToken::new(),
                context,
            )
            .await
            .expect("reopen persisted store in a fresh process");
            reopened
                .background_cancel_token()
                .expect("reopened background token")
                .cancel();
            tokio::task::yield_now().await;
            init_bucket_metadata_sys(reopened.clone(), Vec::new()).await;
            let bucket = persisted.bucket.as_str();
            let object = persisted.object.as_str();
            assert_eq!(
                reopened
                    .bucket_incarnation_id_from_disk(bucket)
                    .await
                    .expect("reopened bucket identity"),
                persisted.incarnation
            );
            assert_eq!(durability_mode(), DurabilityMode::Strict, "reopened process must remain strict");
            assert_eq!(effective_durability(bucket), DurabilityMode::Strict, "reopened bucket must remain strict");
            let set = &reopened.pools[0].disk_set[0];
            let disks: Vec<_> = set
                .disk_inventory()
                .await
                .into_iter()
                .map(|disk| disk.expect("reopened disk"))
                .collect();
            assert_eq!(disks.len(), persisted.disks.len());
            for (index, (disk, expected)) in disks.iter().zip(&persisted.disks).enumerate() {
                assert_eq!(disk.get_disk_id().await.expect("reopened disk identity"), Some(expected.disk_id));
                assert_eq!(disk.endpoint(), endpoints.0[0].endpoints.as_ref()[index]);
                assert_eq!(
                    load_format_erasure(disk, false)
                        .await
                        .expect("reopened persisted disk format"),
                    expected.format,
                    "restart must retain the original deployment and set membership"
                );
                let evidence = disk
                    .inspect_delete_transaction(bucket, object, expected.transaction)
                    .await
                    .expect("query retained C-class backup by its exact UUID");
                assert_eq!(evidence, expected.evidence, "startup must preserve the original C fingerprint");
                assert_eq!(
                    evidence.state,
                    DeleteRecoveryState::Absent,
                    "live backup must not become a delete blocker"
                );
                assert!(
                    evidence.journal.is_none() && evidence.backup_digest.is_some(),
                    "no historical commit authority"
                );
                assert!(!evidence.explicit_delete_safe, "historical versions cannot grant fresh legacy discard");
                assert_eq!(
                    object_tree(&expected.root.join(bucket).join(object)),
                    expected.tree,
                    "startup and inspection preserve the entire original object tree"
                );
            }
            let healthy = reopened
                .heal_mrf_object_at_incarnation(
                    bucket,
                    object,
                    "",
                    persisted.incarnation,
                    &HealOpts {
                        scan_mode: HealScanMode::Normal,
                        ..Default::default()
                    },
                )
                .await
                .expect("restarted MRF check must not create another backup");
            assert!(healthy.error.is_none(), "restarted MRF check: {:?}", healthy.error);
            assert_eq!(healthy.item.drives_healed(), Some(0));
            for (disk, expected) in disks.iter().zip(&persisted.disks) {
                assert_eq!(
                    disk.inspect_delete_transaction(bucket, object, expected.transaction)
                        .await
                        .expect("C evidence after restarted MRF"),
                    expected.evidence,
                    "follow-up MRF must preserve the exact C fingerprint"
                );
                assert_eq!(
                    object_tree(&expected.root.join(bucket).join(object)),
                    expected.tree,
                    "follow-up MRF must not create or remove any backup"
                );
            }
            let inventory = set.disks.read().await.clone();
            *set.disks.write().await = vec![None, None, Some(disks[2].clone()), Some(disks[3].clone())];
            for (key, version_id, expected) in &persisted.reads {
                let mut reader = set
                    .get_object_reader(
                        bucket,
                        key,
                        None,
                        Default::default(),
                        &ObjectOptions {
                            version_id: version_id.clone(),
                            no_lock: true,
                            suppress_read_repair: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("restarted exact-quorum GET using the repaired disk");
                let mut actual = Vec::new();
                reader.read_to_end(&mut actual).await.expect("read complete persisted object");
                assert_eq!(&actual, expected, "restarted content for {key}");
            }
            *set.disks.write().await = inventory;
            std::fs::write(
                manifest_path.with_extension("verified"),
                format!("{}:{}:{}", persisted.receipt_nonce, persisted.writer_pid, std::process::id()),
            )
            .expect("write child completion receipt after every assertion");
            return;
        }

        let incoming_inline = match phase {
            "writer-inline" => true,
            "writer-external" => false,
            _ => panic!("unexpected MRF restart phase: {phase}"),
        };
        assert_eq!(durability_mode(), DurabilityMode::Strict, "writer process must actually use strict");
        let bucket = if incoming_inline {
            "heal-inline-rollback"
        } else {
            "heal-inline-to-external-rollback"
        };
        let (dirs, store, disks) = temp_env::async_with_vars(
            [
                ("RUSTFS_DURABILITY_MODE", Some("strict")),
                ("RUSTFS_NEW_BUCKET_DURABILITY_MODE", Some("strict")),
            ],
            fixture(bucket),
        )
        .await;
        assert_eq!(durability_mode(), DurabilityMode::Strict, "process durability must be strict");
        assert_eq!(effective_durability(bucket), DurabilityMode::Strict, "bucket durability must be strict");
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("bucket identity");
        let synthetic = inline_metadata_rollback_dir(Uuid::nil(), &FileMeta::new());
        let object = "object";
        let child = format!("{object}/{synthetic}");
        let child_body = b"child bytes must survive parent repair".to_vec();
        let write_options = ObjectOptions {
            shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
            write_completion: crate::object_api::WriteCompletion::TailDrained,
            ..Default::default()
        };
        let historical_body = vec![0x41; 1024 * 1024];
        let historical_version = store
            .put_object(
                bucket,
                object,
                &mut PutObjReader::from_vec(historical_body.clone()),
                &ObjectOptions {
                    versioned: true,
                    ..write_options.clone()
                },
            )
            .await
            .expect("seed old external version")
            .version_id
            .expect("historical version has a UUID");
        for (key, body) in [(child.as_str(), child_body.as_slice()), (object, b"inline-gen-1".as_slice())] {
            store
                .put_object(bucket, key, &mut PutObjReader::from_vec(body.to_vec()), &write_options)
                .await
                .expect("seed initial inline objects");
        }
        let mut historical_backups = Vec::new();
        for (index, disk) in disks.iter().enumerate() {
            let historical = disk
                .read_version("", bucket, object, &historical_version.to_string(), &ReadOptions::default())
                .await
                .expect("read historical external version");
            assert!(!historical.inline_data(), "historical version must use real data shards");
            let data_dir = historical.data_dir.expect("historical data directory");
            let mut backup = FileMeta::new();
            backup.add_version(historical).expect("retain historical version evidence");
            let bytes = backup.marshal_msg().expect("encode historical backup");
            let backup_path = dirs[index]
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join(STORAGE_FORMAT_FILE_BACKUP);
            tokio::fs::write(&backup_path, &bytes)
                .await
                .expect("seed unreceipted historical backup");
            historical_backups.push((data_dir, bytes));
        }
        let read_options = ReadOptions {
            read_data: true,
            ..Default::default()
        };
        let old_child = disks[3]
            .read_version("", bucket, &child, "", &read_options)
            .await
            .expect("physical child before repair");
        let set = &store.pools[0].disk_set[0];
        set.disks.write().await[3] = None;
        let payload = if incoming_inline {
            b"inline-gen-2-updated".to_vec()
        } else {
            vec![0x53; 1024 * 1024]
        };
        store
            .put_object(bucket, object, &mut PutObjReader::from_vec(payload.clone()), &write_options)
            .await
            .expect("degraded overwrite with a real stale target");
        set.disks.write().await[3] = Some(disks[3].clone());
        let donor = disks[0]
            .read_version("", bucket, object, "", &read_options)
            .await
            .expect("current donor generation");
        assert_eq!(donor.data.is_some(), incoming_inline, "fixture must use the intended commit branch");
        let heal_opts = HealOpts {
            scan_mode: HealScanMode::Normal,
            ..Default::default()
        };
        let first = store
            .heal_mrf_object_at_incarnation(bucket, object, "", incarnation, &heal_opts)
            .await
            .expect("first MRF repair");
        assert!(first.error.is_none(), "first MRF repair: {:?}", first.error);
        assert_eq!(first.item.drives_healed(), Some(1));
        assert!(!first.item.repair_verified);
        assert_eq!(first.item.detail, LEGACY_OBJECT_IDENTITY_UNVERIFIED_DETAIL);
        let target = disks[3]
            .read_version("", bucket, object, "", &read_options)
            .await
            .expect("repaired target generation");
        assert_eq!(target.mod_time, donor.mod_time);
        assert_eq!(target.data_dir, donor.data_dir);
        assert_eq!(target.size, donor.size);
        assert_eq!(target.version_id, donor.version_id);
        let child_after = disks[3]
            .read_version("", bucket, &child, "", &read_options)
            .await
            .expect("physical child survives parent cleanup");
        assert_eq!(child_after.data, old_child.data);
        assert!(child_after.data.is_some(), "child fixture must carry its physical inline bytes");
        let child_dir = dirs[3].path().join(bucket).join(object).join(synthetic.to_string());
        assert!(!child_dir.join(STORAGE_FORMAT_FILE_BACKUP).exists());
        assert!(child_dir.join(STORAGE_FORMAT_FILE).exists());
        let second = store
            .heal_mrf_object_at_incarnation(bucket, object, "", incarnation, &heal_opts)
            .await
            .expect("follow-up MRF check must not return SlowDown");
        assert!(second.error.is_none(), "second MRF check: {:?}", second.error);
        assert_eq!(second.item.drives_healed(), Some(0));
        assert_eq!(second.item.detail, LEGACY_OBJECT_IDENTITY_UNVERIFIED_DETAIL);

        // EC2+2: force the repaired shard to participate in the exact read quorum.
        let inventory = set.disks.read().await.clone();
        *set.disks.write().await = vec![None, None, Some(disks[2].clone()), Some(disks[3].clone())];
        for (key, expected) in [(object, payload.clone()), (child.as_str(), child_body.clone())] {
            let mut reader = set
                .get_object_reader(
                    bucket,
                    key,
                    None,
                    Default::default(),
                    &ObjectOptions {
                        no_lock: true,
                        suppress_read_repair: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("exact-quorum GET using repaired target");
            let mut body = Vec::new();
            reader.read_to_end(&mut body).await.expect("read complete parent/child body");
            assert_eq!(body, expected, "exact-quorum body for {key}");
        }
        *set.disks.write().await = inventory;

        set.disks.write().await[3] = None;
        let final_payload = if incoming_inline {
            b"inline-gen-3-updated-again".to_vec()
        } else {
            vec![0x54; 1024 * 1024]
        };
        store
            .put_object(bucket, object, &mut PutObjReader::from_vec(final_payload.clone()), &write_options)
            .await
            .expect("second degraded overwrite");
        set.disks.write().await[3] = Some(disks[3].clone());
        let second_repair = store
            .heal_mrf_object_at_incarnation(bucket, object, "", incarnation, &heal_opts)
            .await
            .expect("second actual MRF repair");
        assert!(second_repair.error.is_none(), "second repair: {:?}", second_repair.error);
        assert_eq!(second_repair.item.drives_healed(), Some(1));

        assert_eq!(store.pools.len(), 1, "restart fixture retains its original single pool");
        let endpoints = EndpointServerPools(store.pools.iter().map(|pool| pool.endpoints.clone()).collect());
        let disk_ids = join_all(disks.iter().map(|disk| disk.get_disk_id()))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .expect("original disk identities");
        let formats = join_all(disks.iter().map(|disk| load_format_erasure(disk, false)))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .expect("original persisted disk formats");
        let mut restart_disks = Vec::with_capacity(disks.len());
        for (index, (disk, format)) in disks.iter().zip(&formats).enumerate() {
            let endpoint = disk.endpoint();
            let disk_id = disk_ids[index].expect("formatted disk has a UUID");
            assert_eq!(endpoint, endpoints.0[0].endpoints.as_ref()[index]);
            assert!(endpoint.is_local, "restart must reopen the original local disk");
            assert_eq!(std::path::Path::new(&endpoint.get_file_path()), dirs[index].path());
            assert_eq!(format.id, store.pools[0].format.id);
            assert_eq!(format.erasure.sets, store.pools[0].format.erasure.sets);
            assert_eq!(format.erasure.this, disk_id);
            let set_index = usize::try_from(endpoint.set_idx).expect("original endpoint set index");
            let disk_index = usize::try_from(endpoint.disk_idx).expect("original endpoint disk index");
            assert_eq!(format.erasure.sets[set_index][disk_index], disk_id);
            let (transaction, backup) = &historical_backups[index];
            let tree = object_tree(&dirs[index].path().join(bucket).join(object));
            assert_eq!(
                tree.get(&std::path::PathBuf::from(format!("{transaction}/{STORAGE_FORMAT_FILE_BACKUP}")))
                    .and_then(Option::as_ref),
                Some(backup),
                "both repairs must retain the original historical backup"
            );
            let evidence = disk
                .inspect_delete_transaction(bucket, object, *transaction)
                .await
                .expect("capture exact pre-restart C evidence");
            assert_eq!(evidence.state, DeleteRecoveryState::Absent);
            assert!(evidence.journal.is_none() && evidence.backup_digest.is_some());
            assert!(!evidence.explicit_delete_safe);
            assert_eq!(object_tree(&dirs[index].path().join(bucket).join(object)), tree);
            restart_disks.push(RestartDisk {
                root: dirs[index].path().to_path_buf(),
                slot: [endpoint.pool_idx, endpoint.set_idx, endpoint.disk_idx],
                disk_id,
                format: format.clone(),
                transaction: *transaction,
                evidence,
                tree,
            });
        }
        let pool = &endpoints.0[0];
        let receipt_nonce = Uuid::new_v4();
        let persisted = RestartFixture {
            writer_pid: std::process::id(),
            receipt_nonce,
            incoming_inline,
            legacy: pool.legacy,
            set_count: pool.set_count,
            drives_per_set: pool.drives_per_set,
            cmd_line: pool.cmd_line.clone(),
            platform: pool.platform.clone(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            incarnation,
            reads: vec![
                (object.to_string(), None, final_payload),
                (child, None, child_body),
                (object.to_string(), Some(historical_version.to_string()), historical_body),
            ],
            disks: restart_disks,
        };
        for dir in dirs {
            assert_owned_disk_root(&owned_root, dir.path());
            let retained = dir.keep();
            assert_owned_disk_root(&owned_root, &retained);
        }
        drop(disks);
        drop(store);
        std::fs::write(
            &manifest_path,
            rmp_serde::to_vec_named(&persisted).expect("encode exact original restart state"),
        )
        .expect("publish writer manifest after every repair assertion");
    }

    #[tokio::test]
    async fn delete_recovery_batch_delete_of_prefix_sharing_keys_leaves_no_proof() {
        // CI s3-tests `test_bucket_list(v2)_delimiter_basic`: one DeleteObjects
        // removes `foo/bar` and its child key `foo/bar/xyzzy` under separate
        // exact-key locks. The child's finalization removes `foo/bar/xyzzy/`
        // between the parent's GC inspection and finalization.
        let bucket = "recover-prefix-sharing-batch";
        let (_dirs, store, disks) = fixture(bucket).await;
        let keys = ["asdf", "boo/bar", "foo/bar", "foo/bar/xyzzy", "quux/thud"];
        for key in keys {
            let mut reader = PutObjReader::from_vec(vec![7; 64]);
            store
                .put_object(
                    bucket,
                    key,
                    &mut reader,
                    &ObjectOptions {
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("put prefix-sharing key");
        }
        let parent_inspected = Arc::new(tokio::sync::Notify::new());
        let parent_paused = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let parent_inspected = parent_inspected.clone();
            let parent_paused = parent_paused.clone();
            let disks = disks.clone();
            test_hooks::before_gc_finalize(
                bucket,
                "foo/bar",
                Arc::new(move || {
                    let parent_inspected = parent_inspected.clone();
                    let parent_paused = parent_paused.clone();
                    let disks = disks.clone();
                    Box::pin(async move {
                        parent_paused.store(true, std::sync::atomic::Ordering::SeqCst);
                        parent_inspected.notify_one();
                        let child_finalized = async {
                            loop {
                                let mut residue = false;
                                for disk in &disks {
                                    // The probe races the child's own cleanup;
                                    // a vanishing directory is retried.
                                    residue |= disk
                                        .discover_delete_transactions(bucket, Some("foo/bar/xyzzy"), None, 128)
                                        .await
                                        .map_or(true, |page| !page.transactions.is_empty());
                                }
                                if !residue {
                                    break;
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                            }
                        };
                        tokio::time::timeout(std::time::Duration::from_secs(30), child_finalized)
                            .await
                            .expect("child key finalizes while the parent is paused");
                    })
                }),
            );
        }
        {
            let parent_inspected = parent_inspected.clone();
            test_hooks::before_gc_finalize(
                bucket,
                "foo/bar/xyzzy",
                Arc::new(move || {
                    let parent_inspected = parent_inspected.clone();
                    Box::pin(async move {
                        tokio::time::timeout(std::time::Duration::from_secs(30), parent_inspected.notified())
                            .await
                            .expect("parent key reaches its GC inspection");
                    })
                }),
            );
        }
        let objects = keys
            .into_iter()
            .map(|name| crate::storage_api_contracts::object::ObjectToDelete {
                object_name: name.to_owned(),
                ..Default::default()
            })
            .collect();
        let (deleted, errors) = store.delete_objects(bucket, objects, ObjectOptions::default()).await;
        assert!(errors.iter().all(Option::is_none), "batch delete must succeed: {errors:?}");
        assert_eq!(deleted.len(), keys.len());
        assert!(
            parent_paused.load(std::sync::atomic::Ordering::SeqCst),
            "the interleaving under test must have been exercised"
        );
        for disk in &disks {
            for key in keys {
                assert!(
                    disk.discover_delete_transactions(bucket, Some(key), None, 128)
                        .await
                        .expect("discover delete proof")
                        .transactions
                        .is_empty(),
                    "inline cleanup must not leave a proof for {key}"
                );
            }
        }
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("an emptied bucket is deletable without background recovery");
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("captured logs").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct PublishedCleanup {
        attempts: usize,
        gc_reached: usize,
        proof_retained: bool,
        hint_pending: bool,
        logs: String,
    }

    /// Publishes a certified delete while the first `stale_attempts` inline
    /// cleanup attempts each find a fresh change of this key's own evidence
    /// between GC inspection and finalization.
    async fn published_cleanup(bucket: &str, stale_attempts: usize, lose_fence_after_first: bool) -> PublishedCleanup {
        use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
        use tracing_subscriber::layer::SubscriberExt as _;

        let (dirs, store, disks) = fixture(bucket).await;
        put(&store, bucket, &vec![61; 4096], false).await;
        let transaction = applied(&store, &disks, bucket, None, disks.len()).await;
        let set = &store.pools[0].disk_set[0];
        let key = crate::store::delete_recovery_runtime::DeleteRecoveryKey {
            pool: 0,
            set: 0,
            bucket: bucket.to_owned(),
            object: "object".to_owned(),
            incarnation: None,
        };
        let ticket = set.ctx.delete_recovery_hints().register(key.clone());
        let attempts = Arc::new(AtomicUsize::new(0));
        let fence_armed = Arc::new(AtomicBool::new(false));
        let root = dirs[0].path().join(bucket).join("object");
        // One hook per attempt: the stale ones, then one more that observes a
        // retry when the bound allows it.
        for stale in 0..stale_attempts + usize::from(!lose_fence_after_first && stale_attempts < PUBLISHED_CLEANUP_ATTEMPTS) {
            let attempts = attempts.clone();
            let fence_armed = fence_armed.clone();
            let root = root.clone();
            test_hooks::before_gc_finalize(
                bucket,
                "object",
                Arc::new(move || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if stale < stale_attempts {
                        // A root file is this key's own evidence.
                        std::fs::write(root.join(format!("stale-{attempt}")), b"x").expect("change own evidence");
                        if lose_fence_after_first {
                            fence_armed.store(true, Ordering::SeqCst);
                        }
                    }
                    Box::pin(async {})
                }),
            );
        }
        // Once armed, the fence survives this attempt's per-drive settlements
        // and is lost before the retry decision.
        let settlements = AtomicIsize::new(isize::try_from(disks.len()).expect("drive count"));
        let can_commit = || !fence_armed.load(Ordering::SeqCst) || settlements.fetch_sub(1, Ordering::SeqCst) > 0;
        let logs = CapturedLogs::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .without_time(),
        );
        let _guard = tracing::subscriber::set_default(subscriber);
        let _callsite_pin = crate::test_tracing::pin_callsite_interest_for_test();
        set.publish_delete_commit(
            bucket,
            "object",
            transaction,
            &set.disk_inventory().await,
            set.set_drive_count / 2 + 1,
            DeleteRecoveryContext {
                recovery_ticket: ticket,
                ..DeleteRecoveryContext::new(None, &can_commit)
            },
        )
        .await
        .expect("a certified decision reports success regardless of cleanup");
        let mut proof_retained = false;
        for disk in &disks {
            proof_retained |= !disk
                .discover_delete_transactions(bucket, Some("object"), None, 128)
                .await
                .expect("discover delete proof")
                .transactions
                .is_empty();
        }
        let logs = String::from_utf8(logs.0.lock().expect("captured logs").clone()).expect("UTF-8 logs");
        PublishedCleanup {
            attempts: test_hooks::cleanup_attempts(bucket, "object"),
            gc_reached: attempts.load(Ordering::SeqCst),
            proof_retained,
            hint_pending: set.ctx.delete_recovery_hints().is_pending(&key),
            logs,
        }
    }

    #[tokio::test]
    async fn delete_recovery_published_cleanup_retries_stale_fingerprint() {
        let outcome = published_cleanup("recover-cleanup-retry", 1, false).await;
        assert_eq!(outcome.attempts, 2, "the stale attempt is retried from a fresh inspection");
        assert_eq!(outcome.gc_reached, 2);
        assert!(!outcome.proof_retained, "the retry finalizes the decision");
        assert!(!outcome.hint_pending, "a converged key acknowledges its hint");
        assert!(!outcome.logs.contains(EVENT_DELETE_RECOVERY_CLEANUP_DEFERRED), "{}", outcome.logs);
    }

    #[tokio::test]
    async fn delete_recovery_published_cleanup_retry_is_bounded() {
        let outcome = published_cleanup("recover-cleanup-bounded", PUBLISHED_CLEANUP_ATTEMPTS, false).await;
        assert_eq!(outcome.attempts, PUBLISHED_CLEANUP_ATTEMPTS);
        assert_eq!(outcome.gc_reached, PUBLISHED_CLEANUP_ATTEMPTS, "every attempt met a stale snapshot");
        assert!(outcome.proof_retained, "the decision proof remains for background recovery");
        assert!(outcome.hint_pending, "unfinished cleanup keeps its hint");
        assert!(
            outcome.logs.contains(EVENT_DELETE_RECOVERY_CLEANUP_DEFERRED) && outcome.logs.contains("outdated XL meta"),
            "deferred cleanup must be visible: {}",
            outcome.logs
        );
    }

    #[tokio::test]
    async fn delete_recovery_published_cleanup_does_not_retry_after_fence_loss() {
        let outcome = published_cleanup("recover-cleanup-fence-lost", 1, true).await;
        assert_eq!(outcome.attempts, 1, "a lost fence stops further attempts");
        assert_eq!(outcome.gc_reached, 1);
        assert!(outcome.proof_retained);
        assert!(outcome.hint_pending);
        assert!(
            outcome.logs.contains(EVENT_DELETE_RECOVERY_CLEANUP_DEFERRED) && outcome.logs.contains("outdated XL meta"),
            "the stale snapshot, not a retry under a lost fence, is reported: {}",
            outcome.logs
        );
    }
}
