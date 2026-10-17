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
use crate::set_disk::SetDisks;
use futures::future::join_all;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use uuid::Uuid;

const DISCOVERY_PAGE_SIZE: u32 = 128;
const MAX_DISCOVERED_TRANSACTIONS: usize = 1_000_000;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct DeleteRecoverySummary {
    pub discovered: usize,
    pub committed: usize,
    pub rolled_back: usize,
    pub blocked: usize,
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
    pub recovery_ticket: Option<crate::store::delete_recovery_runtime::DeleteRecoveryTicket>,
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
            recovery_ticket: None,
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
        self.pending += other.pending;
        self.failed += other.failed;
        self.reclaimed += other.reclaimed;
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
        if context.namespace_owner.is_none() {
            context.namespace_owner = Some(self.ctx.begin_namespace_commit());
        }
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
        if let Ok(summary) = self.recover_delete_key(bucket, object, context).await
            && summary.pending == 0
            && summary.blocked == 0
            && summary.failed == 0
            && let Some(ticket) = ticket.as_ref()
        {
            self.ctx.delete_recovery_hints().ack(ticket);
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
        context: DeleteRecoveryContext<'_>,
    ) -> Result<DeleteRecoverySummary> {
        let DeleteRecoveryContext {
            incarnation,
            dry_run,
            explicit_delete,
            repair_marker_source,
            namespace_owner,
            can_commit,
            recovery_ticket: _,
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
            for state in inspect(&disks, bucket, object, transaction).await {
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
            Box::pin(self.heal_delete_recovery_source(bucket, object, &source, can_commit)).await?;
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
        // Legacy snapshots contain no set decision. A fresh authorized removal
        // applies only after the entire key is absent and every raw shape is safe.
        let legacy_only = explicit_delete
            && all_states.values().flatten().all(|copy| {
                copy.current_digest.is_none()
                    && matches!(copy.state, DeleteRecoveryState::Absent | DeleteRecoveryState::LegacyAmbiguous)
                    && (copy.state != DeleteRecoveryState::LegacyAmbiguous || copy.explicit_delete_safe)
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
                if legacy_only {
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
                        |_| Some(DeleteRecoveryAction::FinishCommit),
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
            for transaction in all_states.keys() {
                let fresh = inspect(&disks, bucket, object, *transaction).await;
                if fresh.iter().any(|state| !matches!(state, Ok(state) if matches!(state.state, DeleteRecoveryState::Absent | DeleteRecoveryState::CommitFinished))) {
                    summary.pending += 1;
                    return Ok(summary);
                }
                settled.push(fresh.into_iter().collect::<Result<Vec<_>>>()?);
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
        use crate::disk::local::inline_metadata_rollback_dir;
        use crate::object_api::ShardIntegrityWriteMode;
        use rustfs_filemeta::FileMeta;
        use rustfs_heal_contracts::heal_channel::{HealScanMode, LEGACY_OBJECT_IDENTITY_UNVERIFIED_DETAIL};

        let bucket = "heal-inline-rollback-reclaim";
        let (dirs, store, disks) = fixture(bucket).await;
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("bucket identity");
        let synthetic = inline_metadata_rollback_dir(Uuid::nil(), &FileMeta::new());
        let object = "object";
        let child_object = format!("{object}/{synthetic}");

        for (key, body) in [(&child_object[..], &b"child"[..]), (object, b"inline-gen-1")] {
            let mut reader = PutObjReader::from_vec(body.to_vec());
            store
                .put_object(
                    bucket,
                    key,
                    &mut reader,
                    &ObjectOptions {
                        shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("put initial inline object");
        }

        let set = &store.pools[0].disk_set[0];
        set.disks.write().await[3] = None;
        let mut reader = PutObjReader::from_vec(b"inline-gen-2-updated".to_vec());
        store
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Legacy),
                    write_completion: crate::object_api::WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("put degraded inline overwrite");
        set.disks.write().await[3] = Some(disks[3].clone());

        let heal_opts = HealOpts {
            scan_mode: HealScanMode::Normal,
            ..Default::default()
        };
        let first = store
            .heal_mrf_object_at_incarnation(bucket, object, "", incarnation, &heal_opts)
            .await
            .expect("first MRF repair");
        assert!(first.error.is_none(), "first MRF repair error: {:?}", first.error);
        assert_eq!(first.item.drives_healed(), Some(1));
        assert!(!first.item.repair_verified);
        assert_eq!(first.item.detail, LEGACY_OBJECT_IDENTITY_UNVERIFIED_DETAIL);

        let disk3_child_dir = dirs[3].path().join(bucket).join(object).join(synthetic.to_string());
        assert!(!disk3_child_dir.join(STORAGE_FORMAT_FILE_BACKUP).exists());
        assert!(disk3_child_dir.join(STORAGE_FORMAT_FILE).exists());

        let second = store
            .heal_mrf_object_at_incarnation(bucket, object, "", incarnation, &heal_opts)
            .await
            .expect("follow-up MRF check must not fail with SlowDown");
        assert!(second.error.is_none(), "second MRF check error: {:?}", second.error);
        assert_eq!(second.item.drives_healed(), Some(0));
        assert_eq!(second.item.detail, LEGACY_OBJECT_IDENTITY_UNVERIFIED_DETAIL);
    }
}
