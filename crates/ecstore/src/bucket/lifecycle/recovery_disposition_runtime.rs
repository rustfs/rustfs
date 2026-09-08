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

use std::collections::HashSet;
use std::future::Future;
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use futures::{StreamExt, stream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;

use super::config_boundary;
use super::recovery_control::{IlmRecoveryClassification, IlmRecoveryProtocol, list_recovery_controls};
use super::recovery_disposition::{
    CreatedIlmRecoveryDisposition, ILM_RECOVERY_DISPOSITION_PREFIX, IlmRecoveryDisposition, IlmRecoveryDispositionState,
    ObservedIlmRecoveryDisposition, create_recovery_disposition_if_absent, load_recovery_disposition,
    load_recovery_disposition_no_lock, recovery_disposition_id_from_record_object_name, recovery_disposition_record_object_name,
    resume_recovery_disposition,
};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::object_api::ObjectOptions;
use crate::storage_api_contracts::{list::ListOperations as _, namespace::NamespaceLocking as _};
use crate::store::ECStore;

pub(crate) const MAX_ILM_RECOVERY_DISPOSITIONS: usize = 10_000;
pub(crate) const MAX_ILM_RECOVERY_DISPOSITION_BYTES: u64 = 10_000_u64 * 16_u64 * 1024_u64;
pub(crate) const MAX_ACTOR_DISPOSITIONS_PER_MINUTE: usize = 10;
pub(crate) const MAX_CLUSTER_DISPOSITIONS_PER_MINUTE: usize = 100;
pub(crate) const MAX_ACTIVE_RECOVERY_DISPOSITIONS: usize = 8;
pub(crate) const DEFAULT_RECOVERY_DISPOSITION_PASS_LIMIT: usize = MAX_ACTIVE_RECOVERY_DISPOSITIONS;

const RECOVERY_DISPOSITION_ADMISSION_LOCK: &str = "ilm/recovery-admission/disposition.lock";
const RECOVERY_DISPOSITION_RECENT_WINDOW_NANOS: i64 = 60 * 1_000_000_000;
const RECOVERY_DISPOSITION_PAGE_LIST_TIMEOUT: Duration = Duration::from_secs(15);
const RECOVERY_DISPOSITION_ENTRY_TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY_DISPOSITION_INVENTORY_TIMEOUT: Duration = Duration::from_secs(60);
const RECOVERY_DISPOSITION_PASS_TIMEOUT: Duration = Duration::from_secs(60);
const RECOVERY_DISPOSITION_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
const RECOVERY_DISPOSITION_METRICS_INTERVAL: Duration = Duration::from_secs(5 * 60);
const METRIC_RECOVERY_DISPOSITION_RECORDS: &str = "rustfs_ilm_recovery_disposition_records";
const METRIC_RECOVERY_DISPOSITION_BYTES: &str = "rustfs_ilm_recovery_disposition_bytes";
const METRIC_RECOVERY_DISPOSITION_INFLIGHT: &str = "rustfs_ilm_recovery_disposition_inflight";
const METRIC_RECOVERY_DISPOSITION_OPERATIONS_TOTAL: &str = "rustfs_ilm_recovery_disposition_operations_total";
const METRIC_RECOVERY_CONTROLS: &str = "rustfs_ilm_recovery_controls";
const METRIC_RECOVERY_CONTROL_SCAN_INCOMPLETE: &str = "rustfs_ilm_recovery_control_scan_incomplete";

static RECOVERY_DISPOSITION_EXECUTION_LIMIT: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_ACTIVE_RECOVERY_DISPOSITIONS)));
static RECOVERY_DISPOSITION_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IlmRecoveryDispositionInventory {
    pub(crate) count: usize,
    pub(crate) bytes: u64,
    pub(crate) prepared: usize,
    pub(crate) applying: usize,
    pub(crate) completed: usize,
    pub(crate) corrupt: usize,
    pub(crate) incomplete: bool,
    creations: Vec<(i64, String)>,
}

impl IlmRecoveryDispositionInventory {
    fn active(&self) -> usize {
        self.prepared.saturating_add(self.applying)
    }

    fn check_admission(&self, actor_sha256: &str, candidate_len: usize, now_unix_nanos: i64) -> Result<()> {
        if self.incomplete || self.corrupt > 0 || self.creations.iter().any(|(created_at, _)| *created_at > now_unix_nanos) {
            return Err(Error::PreconditionFailed);
        }
        let recent_after = now_unix_nanos.saturating_sub(RECOVERY_DISPOSITION_RECENT_WINDOW_NANOS);
        let cluster_recent = self
            .creations
            .iter()
            .filter(|(created_at, _)| *created_at > recent_after)
            .count();
        let actor_recent = self
            .creations
            .iter()
            .filter(|(created_at, actor)| *created_at > recent_after && actor == actor_sha256)
            .count();
        check_recovery_disposition_admission(self.count, self.bytes, self.active(), actor_recent, cluster_recent, candidate_len)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct IlmRecoveryControlClassificationCounts {
    pub(crate) retrying: usize,
    pub(crate) retained_ambiguous: usize,
    pub(crate) corrupt: usize,
    pub(crate) operator_required: usize,
    pub(crate) abandoned: usize,
    pub(crate) terminal: usize,
    pub(crate) incomplete: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RecoveryDispositionMaintenanceStats {
    pub(crate) scanned: u64,
    pub(crate) resumed: u64,
    pub(crate) completed: u64,
    pub(crate) replayed: u64,
    pub(crate) garbage_collected: u64,
    pub(crate) retained: u64,
    pub(crate) corrupt: u64,
    pub(crate) failed: u64,
    pub(crate) next_marker: Option<String>,
    pub(crate) truncated: bool,
}

#[derive(Debug)]
enum BoundedEntryTaskResult<T> {
    Completed(T),
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryDispositionMaintenanceEntryOutcome {
    Missing,
    Corrupt,
    Retained,
    GarbageCollected,
    Failed,
    Resumed,
    Completed,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveryDispositionMaintenanceCursor {
    marker: Option<String>,
    wait_for_interval: bool,
}

pub(crate) struct RecoveryDispositionExecutionPermit<'a> {
    _permit: OwnedSemaphorePermit,
    inflight: &'a AtomicUsize,
    record_metrics: bool,
}

impl Drop for RecoveryDispositionExecutionPermit<'_> {
    fn drop(&mut self) {
        let inflight = self.inflight.fetch_sub(1, Ordering::AcqRel).saturating_sub(1);
        if self.record_metrics {
            metrics::gauge!(METRIC_RECOVERY_DISPOSITION_INFLIGHT).set(metric_count(inflight));
        }
    }
}

fn try_acquire_recovery_disposition_execution_permit(
    execution_limit: Arc<Semaphore>,
    inflight: &AtomicUsize,
    record_metrics: bool,
) -> Result<RecoveryDispositionExecutionPermit<'_>> {
    let permit = execution_limit.try_acquire_owned().map_err(|err| match err {
        TryAcquireError::NoPermits => {
            if record_metrics {
                metrics::counter!(
                    METRIC_RECOVERY_DISPOSITION_OPERATIONS_TOTAL,
                    "operation" => "execution_admission",
                    "outcome" => "rejected"
                )
                .increment(1);
            }
            Error::SlowDown
        }
        TryAcquireError::Closed => Error::other("ILM recovery disposition execution limiter is closed"),
    })?;
    let current_inflight = inflight.fetch_add(1, Ordering::AcqRel).saturating_add(1);
    if record_metrics {
        metrics::gauge!(METRIC_RECOVERY_DISPOSITION_INFLIGHT).set(metric_count(current_inflight));
    }
    Ok(RecoveryDispositionExecutionPermit {
        _permit: permit,
        inflight,
        record_metrics,
    })
}

pub(crate) async fn acquire_recovery_disposition_execution_permit() -> Result<RecoveryDispositionExecutionPermit<'static>> {
    try_acquire_recovery_disposition_execution_permit(
        Arc::clone(&RECOVERY_DISPOSITION_EXECUTION_LIMIT),
        &RECOVERY_DISPOSITION_INFLIGHT,
        true,
    )
}

async fn run_bounded_entry_tasks<I, F, T>(
    tasks: I,
    concurrency: usize,
    timeout: Duration,
    cancel_token: &CancellationToken,
) -> Result<Vec<BoundedEntryTaskResult<T>>>
where
    I: IntoIterator<Item = F> + Send + 'static,
    I::IntoIter: Send,
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    if concurrency == 0 {
        return Err(Error::other("ILM recovery disposition task concurrency must be positive"));
    }
    let mut pending = stream::iter(tasks)
        .map(|task| async move {
            match tokio::time::timeout(timeout, task).await {
                Ok(result) => BoundedEntryTaskResult::Completed(result),
                Err(_) => BoundedEntryTaskResult::TimedOut,
            }
        })
        .buffer_unordered(concurrency);
    let mut results = Vec::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return Err(Error::OperationCanceled),
            next = pending.next() => next,
        };
        match next {
            Some(result) => results.push(result),
            None => return Ok(results),
        }
    }
}

/// Creates or replays a disposition under the cluster-wide admission lock.
///
/// Lock order is admission, then the control/source locks acquired by
/// `create_recovery_disposition_if_absent`. Existing records bypass inventory
/// admission so an idempotent replay can still converge when the namespace is
/// at its limit.
pub(crate) async fn create_recovery_disposition_with_admission(
    api: Arc<ECStore>,
    disposition: &IlmRecoveryDisposition,
    now_unix_nanos: i64,
) -> Result<CreatedIlmRecoveryDisposition> {
    disposition.validate().map_err(Error::other)?;
    let protocol = disposition.identity.protocol;
    let disposition_id = &disposition.identity.disposition_id;
    match load_recovery_disposition(api.clone(), protocol, disposition_id).await {
        Ok(_) => return create_recovery_disposition_if_absent(api, disposition).await,
        Err(err) if disposition_is_missing(&err) => {}
        Err(err) => return Err(err),
    }

    let lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, RECOVERY_DISPOSITION_ADMISSION_LOCK)
        .await?;
    let admission_guard = lock.get_write_lock(crate::set_disk::get_lock_acquire_timeout()).await?;
    if admission_guard.is_lock_lost() {
        return Err(Error::PreconditionFailed);
    }
    match load_recovery_disposition(api.clone(), protocol, disposition_id).await {
        Ok(_) => return create_recovery_disposition_if_absent(api, disposition).await,
        Err(err) if disposition_is_missing(&err) => {}
        Err(err) => return Err(err),
    }

    let inventory = tokio::time::timeout(
        RECOVERY_DISPOSITION_INVENTORY_TIMEOUT,
        collect_recovery_disposition_inventory(api.clone()),
    )
    .await
    .map_err(|_| Error::Timeout)??;
    record_recovery_disposition_inventory_metrics(&inventory);
    let candidate_len = disposition.encode().map_err(Error::other)?.len();
    if let Err(err) = inventory.check_admission(&disposition.identity.actor_sha256, candidate_len, now_unix_nanos) {
        metrics::counter!(
            METRIC_RECOVERY_DISPOSITION_OPERATIONS_TOTAL,
            "operation" => "create_admission",
            "outcome" => "rejected"
        )
        .increment(1);
        return Err(err);
    }
    if admission_guard.is_lock_lost() {
        return Err(Error::PreconditionFailed);
    }
    let result = create_recovery_disposition_if_absent(api, disposition).await;
    metrics::counter!(
        METRIC_RECOVERY_DISPOSITION_OPERATIONS_TOTAL,
        "operation" => "create",
        "outcome" => if result.is_ok() { "success" } else { "failed" }
    )
    .increment(1);
    result
}

pub(crate) async fn collect_recovery_disposition_inventory(api: Arc<ECStore>) -> Result<IlmRecoveryDispositionInventory> {
    let mut marker = None;
    let mut seen_markers = HashSet::new();
    let mut inventory = IlmRecoveryDispositionInventory::default();
    loop {
        let page = api
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                &format!("{ILM_RECOVERY_DISPOSITION_PREFIX}/"),
                marker.clone(),
                None,
                1_000,
                false,
                None,
                false,
            )
            .await?;
        let mut load_candidates = Vec::with_capacity(page.objects.len());
        for object in page.objects {
            inventory.count = inventory
                .count
                .checked_add(1)
                .ok_or_else(|| Error::other("ILM recovery disposition count overflow"))?;
            if object.size < 0 {
                inventory.incomplete = true;
            } else {
                inventory.bytes = inventory
                    .bytes
                    .checked_add(
                        u64::try_from(object.size)
                            .map_err(|_| Error::other("ILM recovery disposition object size does not fit u64"))?,
                    )
                    .ok_or_else(|| Error::other("ILM recovery disposition byte total overflow"))?;
            }
            let Ok((protocol, disposition_id)) = recovery_disposition_id_from_record_object_name(&object.name) else {
                inventory.corrupt = inventory.corrupt.saturating_add(1);
                inventory.incomplete = true;
                continue;
            };
            load_candidates.push((protocol, disposition_id));
        }
        let inventory_cancel = CancellationToken::new();
        let page_api = api.clone();
        let loads = load_candidates.into_iter().map(move |(protocol, disposition_id)| {
            let api = page_api.clone();
            async move { load_recovery_disposition(api, protocol, &disposition_id).await }
        });
        for result in run_bounded_entry_tasks(
            loads,
            MAX_ACTIVE_RECOVERY_DISPOSITIONS,
            RECOVERY_DISPOSITION_ENTRY_TIMEOUT,
            &inventory_cancel,
        )
        .await?
        {
            match result {
                BoundedEntryTaskResult::Completed(Ok(observed)) => {
                    inventory.creations.push((
                        observed.disposition.created_at_unix_nanos,
                        observed.disposition.identity.actor_sha256.clone(),
                    ));
                    match observed.disposition.state {
                        IlmRecoveryDispositionState::Prepared => inventory.prepared = inventory.prepared.saturating_add(1),
                        IlmRecoveryDispositionState::Applying => inventory.applying = inventory.applying.saturating_add(1),
                        IlmRecoveryDispositionState::Completed => inventory.completed = inventory.completed.saturating_add(1),
                    }
                }
                BoundedEntryTaskResult::Completed(Err(err)) if disposition_is_missing(&err) => {
                    inventory.incomplete = true;
                }
                BoundedEntryTaskResult::Completed(Err(_)) => {
                    inventory.corrupt = inventory.corrupt.saturating_add(1);
                    inventory.incomplete = true;
                }
                BoundedEntryTaskResult::TimedOut => inventory.incomplete = true,
            }
        }
        if !page.is_truncated {
            break;
        }
        let next = page
            .next_continuation_token
            .ok_or_else(|| Error::other("ILM recovery disposition inventory omitted its continuation marker"))?;
        marker = Some(record_inventory_marker(&mut seen_markers, next)?);
    }
    Ok(inventory)
}

pub(crate) async fn refresh_recovery_control_metrics(api: Arc<ECStore>) -> Result<IlmRecoveryControlClassificationCounts> {
    let mut counts = IlmRecoveryControlClassificationCounts::default();
    for protocol in IlmRecoveryProtocol::all() {
        let mut marker = None;
        let mut seen_markers = HashSet::new();
        loop {
            let page = list_recovery_controls(api.clone(), protocol, None, 1_000, marker.clone()).await?;
            counts.incomplete |= page.incomplete;
            for record in page.records {
                match record.classification {
                    IlmRecoveryClassification::Retrying => counts.retrying = counts.retrying.saturating_add(1),
                    IlmRecoveryClassification::RetainedAmbiguous => {
                        counts.retained_ambiguous = counts.retained_ambiguous.saturating_add(1)
                    }
                    IlmRecoveryClassification::Corrupt => counts.corrupt = counts.corrupt.saturating_add(1),
                    IlmRecoveryClassification::OperatorRequired => {
                        counts.operator_required = counts.operator_required.saturating_add(1)
                    }
                    IlmRecoveryClassification::Abandoned => counts.abandoned = counts.abandoned.saturating_add(1),
                    IlmRecoveryClassification::Terminal => counts.terminal = counts.terminal.saturating_add(1),
                }
            }
            if !page.truncated {
                break;
            }
            let next = page
                .next_marker
                .ok_or_else(|| Error::other("ILM recovery control metrics omitted its continuation marker"))?;
            marker = Some(record_control_metrics_marker(&mut seen_markers, next)?);
        }
    }
    metrics::gauge!(METRIC_RECOVERY_CONTROLS, "classification" => "retrying").set(metric_count(counts.retrying));
    metrics::gauge!(METRIC_RECOVERY_CONTROLS, "classification" => "retained_ambiguous")
        .set(metric_count(counts.retained_ambiguous));
    metrics::gauge!(METRIC_RECOVERY_CONTROLS, "classification" => "corrupt").set(metric_count(counts.corrupt));
    metrics::gauge!(METRIC_RECOVERY_CONTROLS, "classification" => "operator_required")
        .set(metric_count(counts.operator_required));
    metrics::gauge!(METRIC_RECOVERY_CONTROLS, "classification" => "abandoned").set(metric_count(counts.abandoned));
    metrics::gauge!(METRIC_RECOVERY_CONTROLS, "classification" => "terminal").set(metric_count(counts.terminal));
    metrics::gauge!(METRIC_RECOVERY_CONTROL_SCAN_INCOMPLETE).set(if counts.incomplete { 1.0 } else { 0.0 });
    Ok(counts)
}

async fn process_recovery_disposition_maintenance_entry(
    api: Arc<ECStore>,
    object_name: String,
    now_unix_nanos: i64,
) -> RecoveryDispositionMaintenanceEntryOutcome {
    let (protocol, disposition_id) = match recovery_disposition_id_from_record_object_name(&object_name) {
        Ok(parsed) => parsed,
        Err(_) => return RecoveryDispositionMaintenanceEntryOutcome::Corrupt,
    };
    let observed = match load_recovery_disposition(api.clone(), protocol, &disposition_id).await {
        Ok(observed) => observed,
        Err(err) if disposition_is_missing(&err) => return RecoveryDispositionMaintenanceEntryOutcome::Missing,
        Err(_) => return RecoveryDispositionMaintenanceEntryOutcome::Corrupt,
    };
    if observed.disposition.state == IlmRecoveryDispositionState::Completed {
        if !disposition_is_gc_eligible(observed.disposition.state, observed.disposition.retain_until_unix_nanos, now_unix_nanos) {
            return RecoveryDispositionMaintenanceEntryOutcome::Retained;
        }
        return match garbage_collect_completed_recovery_disposition(api, &observed, now_unix_nanos).await {
            Ok(true) => RecoveryDispositionMaintenanceEntryOutcome::GarbageCollected,
            Ok(false) => RecoveryDispositionMaintenanceEntryOutcome::Retained,
            Err(_) => RecoveryDispositionMaintenanceEntryOutcome::Failed,
        };
    }

    let _permit = match acquire_recovery_disposition_execution_permit().await {
        Ok(permit) => permit,
        Err(_) => return RecoveryDispositionMaintenanceEntryOutcome::Failed,
    };
    match resume_recovery_disposition(api, protocol, &disposition_id, now_unix_nanos).await {
        Ok(execution) => {
            use super::recovery_disposition::IlmRecoveryDispositionExecutionOutcome::{AcceptedForRecovery, Completed, Replayed};
            match execution.outcome {
                AcceptedForRecovery => RecoveryDispositionMaintenanceEntryOutcome::Resumed,
                Completed => RecoveryDispositionMaintenanceEntryOutcome::Completed,
                Replayed => RecoveryDispositionMaintenanceEntryOutcome::Replayed,
            }
        }
        Err(_) => RecoveryDispositionMaintenanceEntryOutcome::Failed,
    }
}

fn record_maintenance_entry_outcome(
    stats: &mut RecoveryDispositionMaintenanceStats,
    outcome: RecoveryDispositionMaintenanceEntryOutcome,
) {
    match outcome {
        RecoveryDispositionMaintenanceEntryOutcome::Missing => {}
        RecoveryDispositionMaintenanceEntryOutcome::Corrupt => stats.corrupt = stats.corrupt.saturating_add(1),
        RecoveryDispositionMaintenanceEntryOutcome::Retained => stats.retained = stats.retained.saturating_add(1),
        RecoveryDispositionMaintenanceEntryOutcome::GarbageCollected => {
            stats.garbage_collected = stats.garbage_collected.saturating_add(1)
        }
        RecoveryDispositionMaintenanceEntryOutcome::Failed => stats.failed = stats.failed.saturating_add(1),
        RecoveryDispositionMaintenanceEntryOutcome::Resumed => stats.resumed = stats.resumed.saturating_add(1),
        RecoveryDispositionMaintenanceEntryOutcome::Completed => stats.completed = stats.completed.saturating_add(1),
        RecoveryDispositionMaintenanceEntryOutcome::Replayed => stats.replayed = stats.replayed.saturating_add(1),
    }
}

fn advance_recovery_disposition_maintenance_cursor(
    stats: &RecoveryDispositionMaintenanceStats,
    seen_markers: &mut HashSet<String>,
) -> Result<RecoveryDispositionMaintenanceCursor> {
    if !stats.truncated {
        seen_markers.clear();
        return Ok(RecoveryDispositionMaintenanceCursor {
            marker: None,
            wait_for_interval: true,
        });
    }
    let next = stats
        .next_marker
        .clone()
        .ok_or_else(|| Error::other("ILM recovery disposition maintenance omitted its continuation marker"))?;
    Ok(RecoveryDispositionMaintenanceCursor {
        marker: Some(record_maintenance_marker(seen_markers, next)?),
        wait_for_interval: false,
    })
}

pub(crate) async fn run_recovery_disposition_maintenance_pass(
    api: Arc<ECStore>,
    cancel_token: &CancellationToken,
    limit: usize,
    marker: Option<String>,
    now_unix_nanos: i64,
) -> Result<RecoveryDispositionMaintenanceStats> {
    if !(1..=1_000).contains(&limit) {
        return Err(Error::other("ILM recovery disposition pass limit must be between 1 and 1000"));
    }
    let page = tokio::time::timeout(
        RECOVERY_DISPOSITION_PAGE_LIST_TIMEOUT,
        api.clone().list_objects_v2(
            RUSTFS_META_BUCKET,
            &format!("{ILM_RECOVERY_DISPOSITION_PREFIX}/"),
            marker,
            None,
            i32::try_from(limit).unwrap_or(1_000),
            false,
            None,
            false,
        ),
    )
    .await
    .map_err(|_| Error::Timeout)??;
    if page.is_truncated && page.next_continuation_token.is_none() {
        return Err(Error::other("ILM recovery disposition pass omitted its continuation marker"));
    }
    let scanned =
        u64::try_from(page.objects.len()).map_err(|_| Error::other("ILM recovery disposition page length does not fit u64"))?;
    let mut stats = RecoveryDispositionMaintenanceStats {
        scanned,
        next_marker: page.next_continuation_token,
        truncated: page.is_truncated,
        ..Default::default()
    };
    let entries = page.objects.into_iter().map(move |object| {
        let api = api.clone();
        async move { process_recovery_disposition_maintenance_entry(api, object.name, now_unix_nanos).await }
    });
    for result in run_bounded_entry_tasks(
        entries,
        MAX_ACTIVE_RECOVERY_DISPOSITIONS,
        RECOVERY_DISPOSITION_ENTRY_TIMEOUT,
        cancel_token,
    )
    .await?
    {
        match result {
            BoundedEntryTaskResult::Completed(outcome) => record_maintenance_entry_outcome(&mut stats, outcome),
            BoundedEntryTaskResult::TimedOut => stats.failed = stats.failed.saturating_add(1),
        }
    }
    metrics::counter!(
        METRIC_RECOVERY_DISPOSITION_OPERATIONS_TOTAL,
        "operation" => "maintenance_pass",
        "outcome" => if stats.failed == 0 { "success" } else { "failed" }
    )
    .increment(1);
    Ok(stats)
}

pub(crate) async fn run_recovery_disposition_maintenance_loop(api: Arc<ECStore>, cancel_token: CancellationToken) {
    let mut interval = tokio::time::interval(RECOVERY_DISPOSITION_MAINTENANCE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut marker = None;
    let mut seen_markers = HashSet::new();
    let mut last_metrics_refresh = None;
    let mut wait_for_interval = true;
    loop {
        if wait_for_interval {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => return,
                _ = interval.tick() => {},
            }
        } else if cancel_token.is_cancelled() {
            return;
        }
        let now_unix_nanos = match now_unix_nanos() {
            Ok(now) => now,
            Err(_) => {
                metrics::counter!(
                    METRIC_RECOVERY_DISPOSITION_OPERATIONS_TOTAL,
                    "operation" => "maintenance_loop",
                    "outcome" => "clock_error"
                )
                .increment(1);
                wait_for_interval = true;
                interval.reset();
                continue;
            }
        };
        let pass = run_recovery_disposition_maintenance_pass(
            api.clone(),
            &cancel_token,
            DEFAULT_RECOVERY_DISPOSITION_PASS_LIMIT,
            marker.clone(),
            now_unix_nanos,
        );
        match tokio::time::timeout(RECOVERY_DISPOSITION_PASS_TIMEOUT, pass).await {
            Ok(Ok(stats)) => match advance_recovery_disposition_maintenance_cursor(&stats, &mut seen_markers) {
                Ok(cursor) => {
                    marker = cursor.marker;
                    wait_for_interval = cursor.wait_for_interval;
                }
                Err(_) => {
                    metrics::counter!(
                        METRIC_RECOVERY_DISPOSITION_OPERATIONS_TOTAL,
                        "operation" => "maintenance_loop",
                        "outcome" => "marker_cycle"
                    )
                    .increment(1);
                    seen_markers.clear();
                    marker = None;
                    wait_for_interval = true;
                }
            },
            Ok(Err(_)) | Err(_) => {
                metrics::counter!(
                    METRIC_RECOVERY_DISPOSITION_OPERATIONS_TOTAL,
                    "operation" => "maintenance_loop",
                    "outcome" => "failed"
                )
                .increment(1);
                seen_markers.clear();
                marker = None;
                wait_for_interval = true;
            }
        }
        let metrics_refresh_due =
            last_metrics_refresh.is_none_or(|last: tokio::time::Instant| last.elapsed() >= RECOVERY_DISPOSITION_METRICS_INTERVAL);
        if marker.is_none() && metrics_refresh_due {
            if let Ok(Ok(inventory)) = tokio::time::timeout(
                RECOVERY_DISPOSITION_INVENTORY_TIMEOUT,
                collect_recovery_disposition_inventory(api.clone()),
            )
            .await
            {
                record_recovery_disposition_inventory_metrics(&inventory);
            }
            let _ =
                tokio::time::timeout(RECOVERY_DISPOSITION_INVENTORY_TIMEOUT, refresh_recovery_control_metrics(api.clone())).await;
            last_metrics_refresh = Some(tokio::time::Instant::now());
        }
        if wait_for_interval {
            // A full round (or a failed page) gets one fresh backoff. Resetting
            // after metrics work avoids an overdue interval tick turning the
            // next round into an immediate retry.
            interval.reset();
        }
    }
}

pub(crate) async fn garbage_collect_completed_recovery_disposition(
    api: Arc<ECStore>,
    observed: &ObservedIlmRecoveryDisposition,
    now_unix_nanos: i64,
) -> Result<bool> {
    if !disposition_is_gc_eligible(observed.disposition.state, observed.disposition.retain_until_unix_nanos, now_unix_nanos) {
        return Ok(false);
    }
    let protocol = observed.disposition.identity.protocol;
    let disposition_id = &observed.disposition.identity.disposition_id;
    let object = recovery_disposition_record_object_name(protocol, disposition_id).map_err(Error::other)?;
    let lock = api.new_ns_lock(RUSTFS_META_BUCKET, &object).await?;
    let guard = lock.get_write_lock(crate::set_disk::get_lock_acquire_timeout()).await?;
    let authoritative_result = load_recovery_disposition_no_lock(api.clone(), protocol, disposition_id).await;
    if guard.is_lock_lost() {
        return Err(Error::PreconditionFailed);
    }
    let authoritative = match authoritative_result {
        Ok(authoritative) => authoritative,
        Err(err) if disposition_is_missing(&err) => return Ok(true),
        Err(err) => return Err(err),
    };
    if &authoritative != observed
        || authoritative.disposition.state != IlmRecoveryDispositionState::Completed
        || authoritative.disposition.retain_until_unix_nanos > now_unix_nanos
        || guard.is_lock_lost()
    {
        return Err(Error::PreconditionFailed);
    }
    api.record_durable_ilm_decommission_terminal(&object, &authoritative.encoded)
        .await?;
    if guard.is_lock_lost() {
        return Err(Error::PreconditionFailed);
    }
    let mut delete_options = ObjectOptions {
        delete_prefix: true,
        delete_prefix_object: true,
        no_lock: true,
        ..Default::default()
    };
    delete_options.add_namespace_lock_guard(&guard);
    let delete_result =
        config_boundary::delete_config_if_match_with_opts(api.clone(), &object, &authoritative.etag, delete_options).await;
    if guard.is_lock_lost() {
        return Err(Error::PreconditionFailed);
    }
    let confirmation = load_recovery_disposition_no_lock(api, protocol, disposition_id).await;
    if guard.is_lock_lost() {
        return Err(Error::PreconditionFailed);
    }
    match confirmation {
        Err(err) if disposition_is_missing(&err) => {
            metrics::counter!(
                METRIC_RECOVERY_DISPOSITION_OPERATIONS_TOTAL,
                "operation" => "gc",
                "outcome" => "deleted"
            )
            .increment(1);
            Ok(true)
        }
        Ok(_) => Err(delete_result.err().unwrap_or(Error::PreconditionFailed)),
        Err(confirm_err) => Err(delete_result.err().unwrap_or(confirm_err)),
    }
}

fn check_recovery_disposition_admission(
    count: usize,
    bytes: u64,
    active: usize,
    actor_recent: usize,
    cluster_recent: usize,
    candidate_len: usize,
) -> Result<()> {
    let candidate_len =
        u64::try_from(candidate_len).map_err(|_| Error::other("ILM recovery disposition size does not fit u64"))?;
    if count >= MAX_ILM_RECOVERY_DISPOSITIONS
        || bytes
            .checked_add(candidate_len)
            .is_none_or(|total| total > MAX_ILM_RECOVERY_DISPOSITION_BYTES)
        || active >= MAX_ACTIVE_RECOVERY_DISPOSITIONS
        || actor_recent >= MAX_ACTOR_DISPOSITIONS_PER_MINUTE
        || cluster_recent >= MAX_CLUSTER_DISPOSITIONS_PER_MINUTE
    {
        return Err(Error::SlowDown);
    }
    Ok(())
}

fn disposition_is_gc_eligible(state: IlmRecoveryDispositionState, retain_until_unix_nanos: i64, now_unix_nanos: i64) -> bool {
    state == IlmRecoveryDispositionState::Completed && retain_until_unix_nanos <= now_unix_nanos
}

fn record_recovery_disposition_inventory_metrics(inventory: &IlmRecoveryDispositionInventory) {
    metrics::gauge!(METRIC_RECOVERY_DISPOSITION_RECORDS, "state" => "prepared").set(metric_count(inventory.prepared));
    metrics::gauge!(METRIC_RECOVERY_DISPOSITION_RECORDS, "state" => "applying").set(metric_count(inventory.applying));
    metrics::gauge!(METRIC_RECOVERY_DISPOSITION_RECORDS, "state" => "completed").set(metric_count(inventory.completed));
    metrics::gauge!(METRIC_RECOVERY_DISPOSITION_RECORDS, "state" => "corrupt").set(metric_count(inventory.corrupt));
    metrics::gauge!(METRIC_RECOVERY_DISPOSITION_BYTES).set(metric_u64(inventory.bytes));
}

fn metric_count(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn metric_u64(value: u64) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(f64::MAX)
}

fn record_inventory_marker(seen_markers: &mut HashSet<String>, next: String) -> Result<String> {
    if !seen_markers.insert(next.clone()) {
        return Err(Error::other("ILM recovery disposition inventory repeated its continuation marker"));
    }
    Ok(next)
}

fn record_control_metrics_marker(seen_markers: &mut HashSet<String>, next: String) -> Result<String> {
    if !seen_markers.insert(next.clone()) {
        return Err(Error::other("ILM recovery control metrics repeated its continuation marker"));
    }
    Ok(next)
}

fn record_maintenance_marker(seen_markers: &mut HashSet<String>, next: String) -> Result<String> {
    if !seen_markers.insert(next.clone()) {
        return Err(Error::other("ILM recovery disposition maintenance repeated its continuation marker"));
    }
    Ok(next)
}

fn now_unix_nanos() -> Result<i64> {
    i64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos())
        .map_err(|_| Error::other("ILM recovery disposition timestamp does not fit i64"))
}

fn disposition_is_missing(err: &Error) -> bool {
    matches!(
        err,
        Error::ConfigNotFound | Error::FileNotFound | Error::ObjectNotFound(_, _) | Error::VersionNotFound(_, _, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_maintenance_page_fits_strictly_inside_pass_deadline() {
        const {
            assert!(DEFAULT_RECOVERY_DISPOSITION_PASS_LIMIT <= MAX_ACTIVE_RECOVERY_DISPOSITIONS);
        }
        assert!(RECOVERY_DISPOSITION_PAGE_LIST_TIMEOUT + RECOVERY_DISPOSITION_ENTRY_TIMEOUT < RECOVERY_DISPOSITION_PASS_TIMEOUT);
    }

    #[test]
    fn truncated_maintenance_pages_advance_without_per_page_interval_waits() {
        let page_count = MAX_ILM_RECOVERY_DISPOSITIONS.div_ceil(DEFAULT_RECOVERY_DISPOSITION_PASS_LIMIT);
        let mut seen_markers = HashSet::new();
        let mut interval_waits = 0;
        for page_index in 0..page_count {
            let truncated = page_index + 1 < page_count;
            let stats = RecoveryDispositionMaintenanceStats {
                next_marker: truncated.then(|| format!("page-{}", page_index + 1)),
                truncated,
                ..Default::default()
            };
            let cursor = advance_recovery_disposition_maintenance_cursor(&stats, &mut seen_markers)
                .expect("unique maintenance markers should advance");
            interval_waits += usize::from(cursor.wait_for_interval);
            if truncated {
                assert!(cursor.marker.is_some(), "a truncated page must continue immediately");
            } else {
                assert!(cursor.marker.is_none(), "a completed round must restart from the beginning");
            }
        }
        assert_eq!(interval_waits, 1, "ten thousand records must incur one interval wait per full round");
    }

    #[test]
    fn timed_out_entry_does_not_add_a_page_interval_before_tail_or_retry() {
        let mut stats = RecoveryDispositionMaintenanceStats {
            failed: 1,
            next_marker: Some("tail-page".to_string()),
            truncated: true,
            ..Default::default()
        };
        let mut seen_markers = HashSet::new();
        let tail = advance_recovery_disposition_maintenance_cursor(&stats, &mut seen_markers)
            .expect("a timed-out entry must not block pagination");
        assert!(!tail.wait_for_interval);
        assert_eq!(tail.marker.as_deref(), Some("tail-page"));

        stats.next_marker = None;
        stats.truncated = false;
        let next_round = advance_recovery_disposition_maintenance_cursor(&stats, &mut seen_markers)
            .expect("the completed round should schedule one bounded retry interval");
        assert!(next_round.wait_for_interval);
        assert!(next_round.marker.is_none());
    }

    #[test]
    fn execution_permit_rejects_saturation_and_releases_capacity_on_drop() {
        let execution_limit = Arc::new(Semaphore::new(2));
        let inflight = AtomicUsize::new(0);

        let first = try_acquire_recovery_disposition_execution_permit(Arc::clone(&execution_limit), &inflight, false)
            .expect("first execution permit should be available");
        let second = try_acquire_recovery_disposition_execution_permit(Arc::clone(&execution_limit), &inflight, false)
            .expect("second execution permit should be available");
        assert_eq!(inflight.load(Ordering::Acquire), 2);

        let saturated = try_acquire_recovery_disposition_execution_permit(Arc::clone(&execution_limit), &inflight, false);
        assert!(matches!(saturated, Err(Error::SlowDown)));
        assert_eq!(inflight.load(Ordering::Acquire), 2);

        drop(first);
        assert_eq!(inflight.load(Ordering::Acquire), 1);
        let replacement = try_acquire_recovery_disposition_execution_permit(execution_limit, &inflight, false)
            .expect("dropping a permit should immediately restore capacity");
        assert_eq!(inflight.load(Ordering::Acquire), 2);

        drop(second);
        drop(replacement);
        assert_eq!(inflight.load(Ordering::Acquire), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_tasks_complete_fast_peer_and_release_timed_out_permit() {
        type EntryTask = std::pin::Pin<Box<dyn Future<Output = usize> + Send + 'static>>;

        let execution_limit = Arc::new(Semaphore::new(1));
        let inflight = Arc::new(AtomicUsize::new(0));
        let fast_completed = Arc::new(AtomicUsize::new(0));
        let slow_started = Arc::new(tokio::sync::Notify::new());
        let fast_finished = Arc::new(tokio::sync::Notify::new());
        let slow_limit = Arc::clone(&execution_limit);
        let slow_inflight = Arc::clone(&inflight);
        let slow_signal = Arc::clone(&slow_started);
        let slow: EntryTask = Box::pin(async move {
            let _permit = try_acquire_recovery_disposition_execution_permit(slow_limit, slow_inflight.as_ref(), false)
                .expect("slow task should acquire its isolated execution permit");
            slow_signal.notify_one();
            std::future::pending::<usize>().await
        });
        let fast_signal = Arc::clone(&fast_completed);
        let fast_notification = Arc::clone(&fast_finished);
        let fast: EntryTask = Box::pin(async move {
            fast_signal.store(1, Ordering::Release);
            fast_notification.notify_one();
            7
        });
        let cancel_token = CancellationToken::new();
        let runner = run_bounded_entry_tasks(vec![slow, fast], 2, RECOVERY_DISPOSITION_ENTRY_TIMEOUT, &cancel_token);
        tokio::pin!(runner);

        tokio::select! {
            _ = slow_started.notified() => {}
            result = &mut runner => panic!("bounded runner completed before the slow task started: {result:?}"),
        }
        tokio::select! {
            _ = fast_finished.notified() => {}
            result = &mut runner => panic!("bounded runner completed before the fast peer: {result:?}"),
        }
        assert_eq!(fast_completed.load(Ordering::Acquire), 1);
        assert_eq!(inflight.load(Ordering::Acquire), 1);

        tokio::time::advance(RECOVERY_DISPOSITION_ENTRY_TIMEOUT).await;
        let results = runner.await.expect("entry timeout should be reported as an outcome");
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, BoundedEntryTaskResult::Completed(7)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, BoundedEntryTaskResult::TimedOut))
                .count(),
            1
        );
        assert_eq!(inflight.load(Ordering::Acquire), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_task_cancellation_drops_execution_permit() {
        let execution_limit = Arc::new(Semaphore::new(1));
        let inflight = Arc::new(AtomicUsize::new(0));
        let task_started = Arc::new(tokio::sync::Notify::new());
        let task_limit = Arc::clone(&execution_limit);
        let task_inflight = Arc::clone(&inflight);
        let task_signal = Arc::clone(&task_started);
        let pending = async move {
            let _permit = try_acquire_recovery_disposition_execution_permit(task_limit, task_inflight.as_ref(), false)
                .expect("pending task should acquire its isolated execution permit");
            task_signal.notify_one();
            std::future::pending::<()>().await;
        };
        let cancel_token = CancellationToken::new();
        let runner = run_bounded_entry_tasks(std::iter::once(pending), 1, RECOVERY_DISPOSITION_ENTRY_TIMEOUT, &cancel_token);
        tokio::pin!(runner);

        tokio::select! {
            _ = task_started.notified() => {}
            result = &mut runner => panic!("bounded runner completed before the pending task started: {result:?}"),
        }
        assert_eq!(inflight.load(Ordering::Acquire), 1);
        cancel_token.cancel();
        let result = runner.await;
        assert!(matches!(result, Err(Error::OperationCanceled)));
        assert_eq!(inflight.load(Ordering::Acquire), 0);
    }

    #[test]
    fn disposition_admission_enforces_each_exact_boundary() {
        assert!(
            check_recovery_disposition_admission(
                MAX_ILM_RECOVERY_DISPOSITIONS - 1,
                MAX_ILM_RECOVERY_DISPOSITION_BYTES - 1,
                MAX_ACTIVE_RECOVERY_DISPOSITIONS - 1,
                MAX_ACTOR_DISPOSITIONS_PER_MINUTE - 1,
                MAX_CLUSTER_DISPOSITIONS_PER_MINUTE - 1,
                1,
            )
            .is_ok()
        );
        assert!(check_recovery_disposition_admission(MAX_ILM_RECOVERY_DISPOSITIONS, 0, 0, 0, 0, 1).is_err());
        assert!(check_recovery_disposition_admission(0, MAX_ILM_RECOVERY_DISPOSITION_BYTES, 0, 0, 0, 1).is_err());
        assert!(check_recovery_disposition_admission(0, 0, MAX_ACTIVE_RECOVERY_DISPOSITIONS, 0, 0, 1).is_err());
        assert!(check_recovery_disposition_admission(0, 0, 0, MAX_ACTOR_DISPOSITIONS_PER_MINUTE, 0, 1).is_err());
        assert!(check_recovery_disposition_admission(0, 0, 0, 0, MAX_CLUSTER_DISPOSITIONS_PER_MINUTE, 1).is_err());
    }

    #[test]
    fn disposition_admission_enforces_exact_byte_boundary_and_overflow() {
        let candidate_len = 16 * 1024;
        assert!(
            check_recovery_disposition_admission(
                0,
                MAX_ILM_RECOVERY_DISPOSITION_BYTES - u64::try_from(candidate_len).expect("candidate fits u64"),
                0,
                0,
                0,
                candidate_len,
            )
            .is_ok()
        );
        assert!(matches!(
            check_recovery_disposition_admission(
                0,
                MAX_ILM_RECOVERY_DISPOSITION_BYTES - u64::try_from(candidate_len).expect("candidate fits u64") + 1,
                0,
                0,
                0,
                candidate_len,
            ),
            Err(Error::SlowDown)
        ));
        assert!(matches!(
            check_recovery_disposition_admission(0, u64::MAX, 0, 0, 0, 1),
            Err(Error::SlowDown)
        ));
    }

    #[test]
    fn inventory_enforces_actor_and_cluster_rate_limits_independently() {
        let now = 120 * 1_000_000_000;
        let recent = now - RECOVERY_DISPOSITION_RECENT_WINDOW_NANOS + 1;
        let actor_below_limit = IlmRecoveryDispositionInventory {
            creations: vec![(recent, "actor-below-limit".to_string()); MAX_ACTOR_DISPOSITIONS_PER_MINUTE - 1],
            ..Default::default()
        };
        assert!(actor_below_limit.check_admission("actor-below-limit", 1, now).is_ok());

        let actor_at_limit = IlmRecoveryDispositionInventory {
            creations: vec![(recent, "actor-at-limit".to_string()); MAX_ACTOR_DISPOSITIONS_PER_MINUTE],
            ..Default::default()
        };
        assert!(matches!(actor_at_limit.check_admission("actor-at-limit", 1, now), Err(Error::SlowDown)));
        assert!(actor_at_limit.check_admission("different-actor", 1, now).is_ok());

        let cluster_below_limit = IlmRecoveryDispositionInventory {
            creations: (0..MAX_CLUSTER_DISPOSITIONS_PER_MINUTE - 1)
                .map(|index| (recent, format!("actor-{index}")))
                .collect(),
            ..Default::default()
        };
        assert!(cluster_below_limit.check_admission("new-actor", 1, now).is_ok());

        let cluster_at_limit = IlmRecoveryDispositionInventory {
            creations: (0..MAX_CLUSTER_DISPOSITIONS_PER_MINUTE)
                .map(|index| (recent, format!("actor-{index}")))
                .collect(),
            ..Default::default()
        };
        assert!(matches!(cluster_at_limit.check_admission("new-actor", 1, now), Err(Error::SlowDown)));
    }

    #[test]
    fn inventory_rejects_future_creation_even_when_rate_limits_are_clear() {
        let now = 120 * 1_000_000_000;
        let inventory = IlmRecoveryDispositionInventory {
            creations: vec![(now + 1, "other-actor".to_string())],
            ..Default::default()
        };

        assert!(matches!(inventory.check_admission("actor", 1, now), Err(Error::PreconditionFailed)));
    }

    #[test]
    fn inventory_marker_rejects_cycles() {
        let mut seen = HashSet::new();
        assert_eq!(
            record_inventory_marker(&mut seen, "next".to_string()).expect("new marker should pass"),
            "next"
        );
        assert!(record_inventory_marker(&mut seen, "next".to_string()).is_err());
    }

    #[test]
    fn control_metrics_marker_rejects_cycles() {
        let mut seen = HashSet::new();
        assert_eq!(
            record_control_metrics_marker(&mut seen, "next".to_string()).expect("new marker should pass"),
            "next"
        );
        assert!(record_control_metrics_marker(&mut seen, "next".to_string()).is_err());
    }

    #[test]
    fn incomplete_inventory_fails_closed() {
        let inventory = IlmRecoveryDispositionInventory {
            incomplete: true,
            ..Default::default()
        };
        assert!(inventory.check_admission("actor", 1, 1_000_000_000).is_err());
    }

    #[test]
    fn inventory_rate_window_is_exact_and_rejects_future_timestamps() {
        let now = 120 * 1_000_000_000;
        let mut inventory = IlmRecoveryDispositionInventory {
            creations: vec![(now - RECOVERY_DISPOSITION_RECENT_WINDOW_NANOS, "actor".to_string())],
            ..Default::default()
        };
        assert!(inventory.check_admission("actor", 1, now).is_ok());

        inventory.creations = vec![(now - RECOVERY_DISPOSITION_RECENT_WINDOW_NANOS + 1, "actor".to_string()); 10];
        assert!(inventory.check_admission("actor", 1, now).is_err());

        inventory.creations = vec![(now + 1, "actor".to_string())];
        assert!(inventory.check_admission("actor", 1, now).is_err());
    }

    #[test]
    fn garbage_collection_requires_terminal_state_and_elapsed_retention() {
        assert!(!disposition_is_gc_eligible(IlmRecoveryDispositionState::Prepared, 10, 11));
        assert!(!disposition_is_gc_eligible(IlmRecoveryDispositionState::Applying, 10, 11));
        assert!(!disposition_is_gc_eligible(IlmRecoveryDispositionState::Completed, 11, 10));
        assert!(disposition_is_gc_eligible(IlmRecoveryDispositionState::Completed, 10, 10));
    }
}
