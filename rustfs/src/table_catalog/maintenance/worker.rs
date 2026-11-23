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

use super::super::*;

pub(crate) fn maintenance_timestamp(now: OffsetDateTime) -> String {
    now.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| now.unix_timestamp().to_string())
}

pub(crate) fn default_table_maintenance_worker_lease_timeout_seconds() -> u64 {
    TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_DEFAULT_SECONDS
}

fn parse_maintenance_timestamp(timestamp: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339).ok()
}

pub(crate) fn table_maintenance_quarantine_operator_reason(action: &str, reason: Option<&str>) -> String {
    let reason = reason.map(str::trim).filter(|reason| !reason.is_empty());
    match reason {
        Some(reason) => format!("maintenance quarantine {action} by operator: {reason}"),
        None => format!("maintenance quarantine {action} by operator"),
    }
}

pub(crate) fn push_table_maintenance_audit_event(
    report: &mut TableMetadataMaintenanceReport,
    timestamp: OffsetDateTime,
    actor: TableMaintenanceAuditActor,
    action: TableMaintenanceAuditAction,
    reason: Option<String>,
    before_status: Option<TableMetadataMaintenanceJobStatus>,
    before_quarantined_object_count: Option<usize>,
) {
    report.audit_events.push(TableMaintenanceAuditEvent {
        timestamp: maintenance_timestamp(timestamp),
        actor,
        action,
        reason,
        before_status,
        after_status: Some(report.job.status.clone()),
        before_quarantined_object_count,
        after_quarantined_object_count: Some(report.job.quarantined_object_count),
        recommended_actions: report.job.recommended_actions.clone(),
    });
}

fn table_maintenance_recommended_actions(job: &TableMetadataMaintenanceJob) -> Vec<TableMaintenanceRecommendedAction> {
    let mut actions = Vec::new();
    match job.status {
        TableMetadataMaintenanceJobStatus::NotYetRun => {}
        TableMetadataMaintenanceJobStatus::Queued => {
            actions.push(TableMaintenanceRecommendedAction::RunMaintenanceWorker);
        }
        TableMetadataMaintenanceJobStatus::Running => {
            actions.push(TableMaintenanceRecommendedAction::WaitForActiveWorker);
        }
        TableMetadataMaintenanceJobStatus::Successful => {
            if matches!(job.operation, TableMetadataMaintenanceOperation::DryRun)
                && (job.deletable_metadata_file_count > 0 || job.deletable_object_count > 0)
            {
                actions.push(TableMaintenanceRecommendedAction::ReviewAndRunDelete);
            } else {
                actions.push(TableMaintenanceRecommendedAction::NoActionRequired);
            }
        }
        TableMetadataMaintenanceJobStatus::Failed => {
            if job
                .failure_reason
                .as_deref()
                .is_some_and(|reason| reason == TABLE_MAINTENANCE_DELETE_DISABLED_REASON)
            {
                actions.push(TableMaintenanceRecommendedAction::EnableDelete);
            }
            if job.quarantine_enabled && job.quarantined_object_count > 0 {
                actions.push(TableMaintenanceRecommendedAction::ReviewQuarantine);
            }
            if job.next_retry_after.is_some() {
                actions.push(TableMaintenanceRecommendedAction::WaitForRetryBackoff);
            }
            if actions.is_empty() {
                actions.push(TableMaintenanceRecommendedAction::InvestigateFailure);
            }
        }
        TableMetadataMaintenanceJobStatus::Disabled => {
            actions.push(TableMaintenanceRecommendedAction::EnableBackgroundMaintenance);
        }
        TableMetadataMaintenanceJobStatus::Paused => {
            actions.push(TableMaintenanceRecommendedAction::ResumeMaintenanceWorker);
        }
    }
    actions
}

pub(crate) fn push_unique_maintenance_action(
    actions: &mut Vec<TableMaintenanceRecommendedAction>,
    action: TableMaintenanceRecommendedAction,
) {
    if !actions.contains(&action) {
        actions.push(action);
    }
}

pub(crate) fn table_maintenance_report_order_timestamp(report: &TableMetadataMaintenanceReport) -> String {
    report
        .job
        .finished_at
        .clone()
        .or_else(|| report.job.heartbeat_at.clone())
        .or_else(|| report.job.started_at.clone())
        .or_else(|| report.job.scheduled_at.clone())
        .unwrap_or_default()
}

pub(crate) fn table_maintenance_scheduler_job_summary(
    report: &TableMetadataMaintenanceReport,
) -> TableMaintenanceSchedulerJobSummary {
    TableMaintenanceSchedulerJobSummary {
        job_id: report.job.job_id.clone(),
        operation: report.job.operation.clone(),
        status: report.job.status.clone(),
        scheduler_id: report.job.scheduler_id.clone(),
        scheduled_at: report.job.scheduled_at.clone(),
        worker_id: report.job.worker_id.clone(),
        attempt: report.job.attempt,
        started_at: report.job.started_at.clone(),
        finished_at: report.job.finished_at.clone(),
        heartbeat_at: report.job.heartbeat_at.clone(),
        next_retry_after: report.job.next_retry_after.clone(),
        recommended_actions: report.job.recommended_actions.clone(),
        audit_events: report.audit_events.clone(),
    }
}

pub(crate) fn table_maintenance_scheduler_quarantine_boundary(
    config: &TableMaintenanceConfig,
    reports: &[TableMetadataMaintenanceReport],
) -> TableMaintenanceSchedulerQuarantineBoundary {
    let source = reports
        .iter()
        .find(|report| report.job.quarantine_enabled && report.job.quarantined_object_count > 0);
    TableMaintenanceSchedulerQuarantineBoundary {
        enabled: config.quarantine_enabled,
        active: source.is_some(),
        retention_seconds: source.map_or(config.quarantine_retention_seconds, |report| report.job.quarantine_retention_seconds),
        quarantined_object_count: source.map_or(0, |report| report.job.quarantined_object_count),
        source_job_id: source.map(|report| report.job.job_id.clone()),
    }
}

pub(crate) fn refresh_table_maintenance_report_recommended_actions(report: &mut TableMetadataMaintenanceReport) {
    report.job.recommended_actions = table_maintenance_recommended_actions(&report.job);
}

pub(crate) fn table_maintenance_report_with_recommended_actions(
    mut report: TableMetadataMaintenanceReport,
) -> TableMetadataMaintenanceReport {
    refresh_table_maintenance_report_recommended_actions(&mut report);
    report
}

pub(crate) fn table_maintenance_scheduler_lease_is_active(
    job: &TableMetadataMaintenanceJob,
    scheduler_lease_timeout_seconds: u64,
    now: OffsetDateTime,
) -> bool {
    let Some(scheduled_at) = job.scheduled_at.as_deref().and_then(parse_maintenance_timestamp) else {
        return false;
    };
    let timeout_seconds = i64::try_from(scheduler_lease_timeout_seconds).unwrap_or(i64::MAX);
    scheduled_at.saturating_add(Duration::seconds(timeout_seconds)) > now
}

pub(crate) fn table_maintenance_job_lease_is_active(
    job: &TableMetadataMaintenanceJob,
    worker_lease_timeout_seconds: u64,
    now: OffsetDateTime,
) -> bool {
    let Some(heartbeat_at) = job.heartbeat_at.as_deref().and_then(parse_maintenance_timestamp) else {
        return false;
    };
    let timeout_seconds = i64::try_from(worker_lease_timeout_seconds).unwrap_or(i64::MAX);
    heartbeat_at.saturating_add(Duration::seconds(timeout_seconds)) > now
}

pub(crate) fn table_maintenance_job_retry_is_pending(job: &TableMetadataMaintenanceJob, now: OffsetDateTime) -> bool {
    if !matches!(job.status, TableMetadataMaintenanceJobStatus::Failed) {
        return false;
    }
    let Some(next_retry_after) = job.next_retry_after.as_deref().and_then(parse_maintenance_timestamp) else {
        return false;
    };
    next_retry_after > now
}

pub(crate) fn apply_maintenance_retry_after(
    job: &mut TableMetadataMaintenanceJob,
    config: &TableMaintenanceConfig,
    now: OffsetDateTime,
) {
    if config.max_retry_attempts == 0 || job.attempt >= config.max_retry_attempts {
        job.next_retry_after = None;
        return;
    }
    let attempt_index = u32::from(job.attempt.saturating_sub(1));
    let multiplier = 1_u64.checked_shl(attempt_index).unwrap_or(u64::MAX);
    let delay_seconds = config
        .retry_initial_backoff_seconds
        .saturating_mul(multiplier)
        .min(config.retry_max_backoff_seconds);
    let delay_seconds = i64::try_from(delay_seconds).unwrap_or(i64::MAX);
    job.next_retry_after = Some(maintenance_timestamp(now.saturating_add(Duration::seconds(delay_seconds))));
}

pub(crate) fn validate_table_maintenance_worker_condition(
    worker_id: &str,
    condition: &TableMaintenanceWorkerCondition,
) -> TableCatalogStoreResult<()> {
    IdentifierSegment::parse(condition.expected_job_id.clone())
        .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid expected maintenance job ID: {err}")))?;
    if matches!(
        condition.expected_job_id.as_str(),
        MAINTENANCE_JOB_ALIAS_CURRENT | MAINTENANCE_JOB_ALIAS_LATEST
    ) {
        return Err(TableCatalogStoreError::Invalid(
            "expected maintenance job ID cannot be an alias".to_string(),
        ));
    }
    if worker_id.trim().is_empty() || condition.expected_attempt == u16::MAX {
        return Err(TableCatalogStoreError::Invalid(
            "conditional maintenance requires a nonempty worker ID and an incrementable expected attempt".to_string(),
        ));
    }
    Ok(())
}

/// True means that this request already claimed the recorded attempt. It must
/// return that report without executing its candidates again.
pub(crate) fn conditional_table_maintenance_is_replay(
    report: &TableMetadataMaintenanceReport,
    worker_id: &str,
    condition: &TableMaintenanceWorkerCondition,
) -> bool {
    report.job.job_id == condition.expected_job_id
        && condition.expected_attempt.checked_add(1) == Some(report.job.attempt)
        && report.job.worker_id.as_deref() == Some(worker_id)
        && !report.job.lease_id.is_empty()
        && matches!(
            report.job.status,
            TableMetadataMaintenanceJobStatus::Running
                | TableMetadataMaintenanceJobStatus::Successful
                | TableMetadataMaintenanceJobStatus::Failed
        )
}

pub(crate) fn validate_conditional_table_maintenance_claim(
    report: &TableMetadataMaintenanceReport,
    entry: &TableEntry,
    effective: &TableMaintenanceEffectiveConfig,
    worker_id: &str,
    condition: &TableMaintenanceWorkerCondition,
    now: OffsetDateTime,
) -> TableCatalogStoreResult<bool> {
    let job = &report.job;
    if job.job_id != condition.expected_job_id
        || job.table_id != entry.table_id
        || report.current_metadata_location != entry.metadata_location
        || job.current_metadata_location != entry.metadata_location
        || job.current_generation != entry.generation
    {
        return Err(TableCatalogStoreError::Conflict(
            "maintenance job identity or catalog state changed before conditional worker claim".to_string(),
        ));
    }
    if conditional_table_maintenance_is_replay(report, worker_id, condition) {
        if matches!(job.status, TableMetadataMaintenanceJobStatus::Running) {
            if !table_maintenance_job_lease_is_active(job, effective.config.worker_lease_timeout_seconds, now) {
                return Err(TableCatalogStoreError::Conflict(
                    "conditional maintenance does not take over an expired worker; inspect the persisted job".to_string(),
                ));
            }
        } else if job.finished_at.as_deref().and_then(parse_maintenance_timestamp).is_none() {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance terminal report is incomplete; inspect the persisted job".to_string(),
            ));
        }
        return Ok(true);
    }
    if job.attempt != condition.expected_attempt {
        return Err(TableCatalogStoreError::Conflict(
            "maintenance attempt changed before conditional worker claim".to_string(),
        ));
    }
    let config = &effective.config;
    if !config.background_enabled || config.worker_paused {
        return Err(TableCatalogStoreError::Conflict(
            "conditional maintenance is disabled or paused".to_string(),
        ));
    }
    if job.retain_recent_metadata_files != config.retain_recent_metadata_files
        || job.quarantine_enabled != config.quarantine_enabled
        || job.quarantine_retention_seconds != config.quarantine_retention_seconds
        || job.config_source != effective.source
        || (matches!(job.operation, TableMetadataMaintenanceOperation::Delete) && !config.delete_enabled)
    {
        return Err(TableCatalogStoreError::Conflict(
            "maintenance policy changed before conditional worker claim".to_string(),
        ));
    }
    if job.quarantined_object_count > 0 || job.max_retry_attempts > 10 {
        return Err(TableCatalogStoreError::Conflict(
            "maintenance quarantine or invalid persisted attempt budget requires operator recovery".to_string(),
        ));
    }
    match job.status {
        TableMetadataMaintenanceJobStatus::Queued
            if job.attempt == 0
                && job.worker_id.is_none()
                && job.lease_id.is_empty()
                && job.started_at.is_none()
                && job.finished_at.is_none()
                && table_maintenance_scheduler_lease_is_active(job, config.worker_lease_timeout_seconds, now) => {}
        TableMetadataMaintenanceJobStatus::Failed if job.attempt > 0 && job.attempt < job.max_retry_attempts => {
            let finished_at = job
                .finished_at
                .as_deref()
                .and_then(parse_maintenance_timestamp)
                .ok_or_else(|| {
                    TableCatalogStoreError::Conflict(
                        "maintenance failed report is incomplete; inspect the persisted job".to_string(),
                    )
                })?;
            let retry_after = job
                .next_retry_after
                .as_deref()
                .and_then(parse_maintenance_timestamp)
                .ok_or_else(|| {
                    TableCatalogStoreError::Conflict("maintenance retry is not scheduled; inspect the persisted job".to_string())
                })?;
            if retry_after < finished_at || job.lease_id.is_empty() || job.worker_id.as_deref().is_none_or(str::is_empty) {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance failed attempt has invalid retry or lease state; inspect the persisted job".to_string(),
                ));
            }
            if retry_after > now {
                return Err(TableCatalogStoreError::Conflict("maintenance retry backoff has not elapsed".to_string()));
            }
        }
        _ => {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance job is not eligible for a conditional claim; inspect its status and persisted attempt budget"
                    .to_string(),
            ));
        }
    }
    Ok(false)
}

/// Only a completed transition from this exact persisted Running report can
/// repair its aliases. A partial claim is deliberately not resumable here.
pub(crate) fn maintenance_terminal_report_continues_running(
    terminal: &TableMetadataMaintenanceReport,
    running: &TableMetadataMaintenanceReport,
) -> bool {
    let finished = &terminal.job;
    let started = &running.job;
    let terminal_action = match finished.status {
        TableMetadataMaintenanceJobStatus::Successful => TableMaintenanceAuditAction::WorkerSucceeded,
        TableMetadataMaintenanceJobStatus::Failed => TableMaintenanceAuditAction::WorkerFailed,
        _ => return false,
    };
    matches!(started.status, TableMetadataMaintenanceJobStatus::Running)
        && finished
            .finished_at
            .as_deref()
            .and_then(parse_maintenance_timestamp)
            .is_some()
        && finished.job_id == started.job_id
        && finished.table_bucket == started.table_bucket
        && finished.namespace == started.namespace
        && finished.table == started.table
        && finished.table_id == started.table_id
        && finished.operation == started.operation
        && finished.config_source == started.config_source
        && finished.worker_id == started.worker_id
        && !finished.lease_id.is_empty()
        && finished.lease_id == started.lease_id
        && finished.attempt == started.attempt
        && finished.max_retry_attempts == started.max_retry_attempts
        && finished.current_metadata_location == started.current_metadata_location
        && finished.current_generation == started.current_generation
        && finished.retain_recent_metadata_files == started.retain_recent_metadata_files
        && finished.safety_window_seconds == started.safety_window_seconds
        && finished.cleanup_watermark_unix_seconds == started.cleanup_watermark_unix_seconds
        && finished.quarantine_enabled == started.quarantine_enabled
        && finished.quarantine_retention_seconds == started.quarantine_retention_seconds
        && finished.started_at == started.started_at
        && finished.heartbeat_at == started.heartbeat_at
        && finished.scheduler_id == started.scheduler_id
        && finished.scheduler_lease_id == started.scheduler_lease_id
        && finished.scheduled_at == started.scheduled_at
        && terminal.current_metadata_location == running.current_metadata_location
        && terminal.cleanup_candidate_locations == running.cleanup_candidate_locations
        && terminal.deletable_metadata_locations == running.deletable_metadata_locations
        && terminal.cleanup_object_candidate_locations == running.cleanup_object_candidate_locations
        && terminal.deletable_object_locations == running.deletable_object_locations
        && terminal.audit_events.len() == running.audit_events.len().saturating_add(1)
        && terminal.audit_events.starts_with(&running.audit_events)
        && terminal.audit_events.last().is_some_and(|event| {
            event.actor == TableMaintenanceAuditActor::Worker
                && event.action == terminal_action
                && event.before_status == Some(TableMetadataMaintenanceJobStatus::Running)
                && event.after_status.as_ref() == Some(&finished.status)
        })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VectorMaintenanceExecutionError {
    #[error("vector maintenance job finished in a failed state")]
    FailedJob,
    #[error("vector maintenance worker did not complete")]
    Join(#[source] tokio::task::JoinError),
    #[error(transparent)]
    Budget(#[from] VectorMaintenanceBudgetError),
    #[error(transparent)]
    Catalog(#[from] TableCatalogStoreError),
    #[error("vector maintenance executor failed")]
    Executor {
        category: VectorMaintenanceFailureCode,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl VectorMaintenanceExecutionError {
    /// A normally returned task error keeps its resource's durable recovery path available.
    /// Join failure cannot establish normal owner completion and stops this runtime.
    pub(crate) fn requires_runtime_stop(&self) -> bool {
        matches!(self, Self::Join(_))
    }

    pub(crate) fn category(&self) -> VectorMaintenanceFailureCode {
        match self {
            Self::Budget(VectorMaintenanceBudgetError::Stopped) => VectorMaintenanceFailureCode::Interrupted,
            Self::Budget(_) => VectorMaintenanceFailureCode::CapacityExceeded,
            Self::Catalog(TableCatalogStoreError::Io {
                classification: CatalogIoErrorKind::Denied,
                ..
            }) => VectorMaintenanceFailureCode::IdentityDenied,
            Self::Catalog(TableCatalogStoreError::Conflict(_)) => VectorMaintenanceFailureCode::Conflict,
            Self::Catalog(TableCatalogStoreError::Invalid(_)) => VectorMaintenanceFailureCode::CorruptRecord,
            Self::Catalog(_) => VectorMaintenanceFailureCode::StorageUnavailable,
            Self::Join(_) => VectorMaintenanceFailureCode::UnknownOutcome,
            Self::FailedJob => VectorMaintenanceFailureCode::UnknownOutcome,
            Self::Executor { category, .. } => *category,
        }
    }
}

#[derive(Debug)]
pub(crate) struct VectorMaintenanceRuntimeFailure {
    pub(crate) task_failure: Option<VectorMaintenanceExecutionError>,
    pub(crate) fatal_failure: Option<VectorMaintenanceExecutionError>,
}

impl std::fmt::Display for VectorMaintenanceRuntimeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("vector maintenance retained a worker failure")
    }
}

impl std::error::Error for VectorMaintenanceRuntimeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.fatal_failure
            .as_ref()
            .or(self.task_failure.as_ref())
            .map(|error| error as _)
    }
}

#[async_trait::async_trait]
pub(crate) trait VectorMaintenanceExecutor<B: TableCatalogObjectBackend>: Send + Sync {
    /// Run before the worker installs any dispatch or heartbeat owner. The control has no dispatch guard.
    /// The fixed adapter owns each proof scope: drain readers and release every lifecycle/dispatch/GC
    /// owner before native replay, then reacquire and revalidate before admitting a sealed result.
    /// Return only after all admitted native work and all temporary owners have completed.
    async fn recover_native(
        &self,
        store: Arc<ObjectTableCatalogStore<B>>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<(), VectorMaintenanceExecutionError>;

    /// Return only after every admitted engine and Managed operation has completed.
    async fn execute(
        &self,
        store: Arc<ObjectTableCatalogStore<B>>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError>;

    async fn prepare(
        &self,
        store: Arc<ObjectTableCatalogStore<B>>,
        action: VectorMaintenancePreparationAction,
        control: VectorMaintenanceControl,
    ) -> Result<(), VectorMaintenanceExecutionError>;
}

struct VectorMaintenanceOwnedResult<T> {
    output: T,
    revision: Uuid,
    heartbeat_error: Option<TableCatalogStoreError>,
}

/// Native replay acquires lifecycle protection itself. This phase has no worker-owned guard or heartbeat.
async fn coordinate_vector_native_recovery<F>(execution: F, control: VectorMaintenanceControl) -> F::Output
where
    F: std::future::Future,
{
    tokio::pin!(execution);
    tokio::select! {
        biased;
        _ = control.stop_new_work.cancelled() => {}
        _ = tokio::time::sleep_until(control.admission_deadline) => control.stop_new_work.cancel(),
        output = &mut execution => return output,
    }
    execution.await
}

/// Cancellation changes admission while the same execution future retains its owners.
async fn coordinate_vector_execution<F, H, HF>(
    execution: F,
    mut heartbeat: H,
    control: VectorMaintenanceControl,
    heartbeat_interval: std::time::Duration,
    mut revision: Uuid,
) -> VectorMaintenanceOwnedResult<F::Output>
where
    F: std::future::Future,
    H: FnMut(Uuid) -> HF,
    HF: std::future::Future<Output = TableCatalogStoreResult<Uuid>>,
{
    tokio::pin!(execution);
    let mut heartbeat_timer = tokio::time::interval_at(tokio::time::Instant::now() + heartbeat_interval, heartbeat_interval);
    heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut stopping = false;
    let mut heartbeat_error = None;
    loop {
        tokio::select! {
            biased;
            _ = control.stop_new_work.cancelled(), if !stopping => { stopping = true; }
            _ = tokio::time::sleep_until(control.admission_deadline), if !stopping => {
                control.stop_new_work.cancel();
                stopping = true;
            }
            output = &mut execution => return VectorMaintenanceOwnedResult { output, revision, heartbeat_error },
            _ = heartbeat_timer.tick(), if heartbeat_error.is_none() => {
                match heartbeat(revision).await {
                    Ok(next) => revision = next,
                    Err(error) => {
                        heartbeat_error = Some(error);
                        control.stop_new_work.cancel();
                        stopping = true;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum VectorMaintenanceWorkerResult {
    Finished(VectorMaintenanceJobState),
    Replay(VectorMaintenanceJobState),
    Skipped(VectorMaintenanceNotEligible),
    Prepared,
}

pub(crate) struct VectorMaintenanceExecutionContext<B: TableCatalogObjectBackend> {
    pub(crate) identity: VectorMaintenanceExecutionIdentityV1,
    pub(crate) source: Arc<dyn super::periodic::VectorMaintenanceSource>,
    pub(crate) executor: Arc<dyn VectorMaintenanceExecutor<B>>,
}

#[cfg(test)]
use super::fault_observation::{self, VectorMaintenanceObservedPhase as ObservedPhase};

pub(crate) async fn execute_vector_maintenance_job<B: TableCatalogObjectBackend>(
    store: ObjectTableCatalogStore<B>,
    resource: VectorMaintenanceResourceV1,
    job_id: Uuid,
    worker: VectorMaintenanceWorker,
    context: VectorMaintenanceExecutionContext<B>,
    control: VectorMaintenanceControl,
) -> Result<VectorMaintenanceWorkerResult, VectorMaintenanceExecutionError> {
    let VectorMaintenanceExecutionContext {
        identity,
        source,
        executor,
    } = context;
    if control.dispatch_guard().is_ok() {
        return Err(TableCatalogStoreError::Invalid("maintenance worker requires an unowned control".into()).into());
    }
    let _record_memory =
        control.reserve_memory((2 * VECTOR_MAINTENANCE_RECORD_MAX_BYTES + 8 * VECTOR_MAINTENANCE_JOB_MAX_BYTES) as u64)?;
    let store = Arc::new(store);
    control.check_admission()?;
    source.check_admission(&identity).await?;
    let resource_identity = resource.identity()?;
    let namespace = Namespace::parse(resource.namespace()).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
    let name = CatalogName::parse(resource.name().clone()).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
    let snapshot = store
        .read_vector_maintenance_job(&resource_identity, job_id)
        .await?
        .ok_or_else(|| TableCatalogStoreError::NotFound("Vector maintenance job".into()))?;
    if snapshot.job().resource() != &resource {
        return Ok(VectorMaintenanceWorkerResult::Skipped(VectorMaintenanceNotEligible::ResourceChanged));
    }
    if snapshot.job().execution_identity() != &identity {
        return Ok(VectorMaintenanceWorkerResult::Skipped(VectorMaintenanceNotEligible::IdentityDenied));
    }
    control.check_admission()?;
    let observed = store
        .read_vector_maintenance_resource(&resource_identity, &namespace, &name)
        .await?;
    let condition = VectorMaintenanceClaimCondition::from_snapshot(&snapshot, observed);
    control.check_admission()?;
    source.check_admission(&identity).await?;
    #[cfg(test)]
    fault_observation::phase(ObservedPhase::Claim);
    let work = match store
        .claim_vector_maintenance_job(&resource_identity, job_id, condition, worker, Uuid::new_v4())
        .await?
    {
        VectorMaintenanceClaimOutcome::Acquired(work) => *work,
        VectorMaintenanceClaimOutcome::Replay(snapshot) => {
            return Ok(VectorMaintenanceWorkerResult::Replay(*snapshot.job().state()));
        }
        VectorMaintenanceClaimOutcome::NotEligible(reason) => return Ok(VectorMaintenanceWorkerResult::Skipped(reason)),
    };
    #[cfg(test)]
    {
        fault_observation::claimed(&work);
        fault_observation::phase(ObservedPhase::NativeRecovery);
    }
    // No worker-owned lifecycle, dispatch, GC, authorizer, or heartbeat exists during native replay.
    // The same admitted callback must drain even when cancellation stops subsequent admissions.
    let recovery_error: Option<VectorMaintenanceExecutionError> = match control.check_admission() {
        Err(error) => Some(error.into()),
        Ok(()) => {
            source.check_admission(&identity).await?;
            coordinate_vector_native_recovery(
                executor.recover_native(store.clone(), work.clone(), control.clone()),
                control.clone(),
            )
            .await
            .err()
        }
    };
    #[cfg(test)]
    {
        if let Some(error) = &recovery_error {
            fault_observation::diagnostic(ObservedPhase::NativeRecovery, error);
        }
        fault_observation::phase(ObservedPhase::Readmission);
    }
    // Revoke new admissions on identity changes. An expired lease remains unresolved and cannot
    // establish that a prior native operation ended. begin_dispatch validates the current lease.
    source.check_admission(&identity).await?;
    let guard = Arc::new(store.begin_vector_maintenance_dispatch(work.lease()).await?);
    let current = store.read_vector_maintenance_job_with_guard(&guard).await?;
    if current.job().resource() != &resource || current.job().execution_identity() != &identity {
        return Err(TableCatalogStoreError::Conflict("maintenance resource or identity changed after recovery".into()).into());
    }
    source.check_admission(&identity).await?;
    let policy = work.snapshot().job().policy();
    let heartbeat_interval = std::time::Duration::from_secs(*policy.heartbeat_interval_seconds());
    let revision = *current.job().revision();
    let lease = work.lease().clone();
    let control = control.with_dispatch(guard.clone());
    let (outcome, diagnostic) = if let Some(error) = recovery_error {
        (
            VectorMaintenanceExecutionOutcome::Unknown {
                reason: error.category(),
            },
            Some(error),
        )
    } else if control.check_admission().is_err() {
        (
            VectorMaintenanceExecutionOutcome::Unknown {
                reason: VectorMaintenanceFailureCode::Interrupted,
            },
            None,
        )
    } else {
        #[cfg(test)]
        fault_observation::phase(ObservedPhase::Executor);
        let result = coordinate_vector_execution(
            executor.execute(store.clone(), work, control.clone()),
            |revision| {
                let store = &store;
                let guard = &guard;
                let source = &source;
                let identity = &identity;
                async move {
                    source.check_admission(identity).await?;
                    guard.check_held()?;
                    store
                        .heartbeat_vector_maintenance_job(guard, revision)
                        .await
                        .map(|snapshot| *snapshot.job().revision())
                }
            },
            control,
            heartbeat_interval,
            revision,
        )
        .await;
        // The coordinator serializes revision changes; the final read also resolves a lost response.
        let _latest_heartbeat_revision = result.revision;
        let diagnostic = result.heartbeat_error.map(|error| {
            let error = VectorMaintenanceExecutionError::Catalog(error);
            #[cfg(test)]
            fault_observation::diagnostic(ObservedPhase::Heartbeat, &error);
            error
        });
        match result.output {
            Ok(outcome) => (outcome, diagnostic),
            Err(error) => {
                #[cfg(test)]
                fault_observation::diagnostic(ObservedPhase::Executor, &error);
                (
                    VectorMaintenanceExecutionOutcome::Unknown {
                        reason: error.category(),
                    },
                    Some(error),
                )
            }
        }
    };
    #[cfg(test)]
    fault_observation::phase(ObservedPhase::FinishRead);
    guard.check_held()?;
    // Refresh after the serialized heartbeat; never race terminal publication against it.
    // The admitted owner records its exact completed result even after new admissions are revoked.
    let latest = store.read_vector_maintenance_job_with_guard(&guard).await?;
    if *latest.job().attempt() != lease.attempt() || latest.job().execution_identity() != &identity {
        return Err(TableCatalogStoreError::Conflict("maintenance attempt changed before finish".into()).into());
    }
    #[cfg(test)]
    fault_observation::phase(ObservedPhase::Finish);
    match store
        .finish_vector_maintenance_job(&guard, *latest.job().revision(), outcome)
        .await
    {
        Ok(finished) => {
            #[cfg(test)]
            fault_observation::durable_job(&finished);
            match diagnostic {
                Some(error) => {
                    #[cfg(test)]
                    fault_observation::returning_diagnostic();
                    Err(error)
                }
                None => Ok(VectorMaintenanceWorkerResult::Finished(*finished.job().state())),
            }
        }
        Err(error) => {
            let error = VectorMaintenanceExecutionError::Catalog(error);
            #[cfg(test)]
            {
                fault_observation::capture(ObservedPhase::Finish, &error);
                fault_observation::phase(ObservedPhase::FinishReadback);
            }
            // One accurate read can recognize an accepted terminal write whose response was lost.
            let recovered = store
                .read_vector_maintenance_job_with_guard(&guard)
                .await
                .map_err(VectorMaintenanceExecutionError::Catalog);
            #[cfg(test)]
            if let Err(error) = &recovered {
                fault_observation::capture(ObservedPhase::FinishReadback, error);
            }
            if let Ok(recovered) = recovered
                && *recovered.job().attempt() == lease.attempt()
                && recovered.job().execution_identity() == &identity
                && matches!(
                    recovered.job().state(),
                    VectorMaintenanceJobState::Succeeded
                        | VectorMaintenanceJobState::Failed
                        | VectorMaintenanceJobState::ManualReview
                        | VectorMaintenanceJobState::PartialRecovered
                )
            {
                #[cfg(test)]
                fault_observation::durable_job(&recovered);
                return match diagnostic {
                    Some(error) => {
                        #[cfg(test)]
                        fault_observation::returning_diagnostic();
                        Err(error)
                    }
                    None => Ok(VectorMaintenanceWorkerResult::Finished(*recovered.job().state())),
                };
            }
            #[cfg(test)]
            fault_observation::phase(ObservedPhase::Finish);
            Err(error)
        }
    }
}

pub(crate) async fn execute_vector_maintenance_preparation<B: TableCatalogObjectBackend>(
    store: ObjectTableCatalogStore<B>,
    resource: VectorMaintenanceResourceV1,
    kind: super::periodic::VectorMaintenancePreparationKind,
    expected: CatalogMetadataRefRecordV1,
    context: VectorMaintenanceExecutionContext<B>,
    control: VectorMaintenanceControl,
) -> Result<VectorMaintenanceWorkerResult, VectorMaintenanceExecutionError> {
    let VectorMaintenanceExecutionContext {
        identity,
        source,
        executor,
    } = context;
    let _record_memory = control.reserve_memory((6 * VECTOR_MAINTENANCE_RECORD_MAX_BYTES) as u64)?;
    let store = Arc::new(store);
    control.check_admission()?;
    source.check_admission(&identity).await?;
    source.authorize_preparation(&identity, &resource).await?;
    let resource_identity = resource.identity()?;
    let namespace = Namespace::parse(resource.namespace()).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
    let name = CatalogName::parse(resource.name().clone()).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
    let expected: CatalogMetadataRef = (&expected).try_into()?;
    control.check_admission()?;
    let snapshot = store
        .read_vector_maintenance_resource(&resource_identity, &namespace, &name)
        .await?;
    if snapshot.observed_object() != &expected {
        return Ok(VectorMaintenanceWorkerResult::Skipped(VectorMaintenanceNotEligible::ResourceChanged));
    }
    match kind {
        super::periodic::VectorMaintenancePreparationKind::Current => {
            control.check_admission()?;
            let bucket = resource.bucket().to_handle()?;
            let (guard, entry) = store.begin_vector_operation(&bucket, &namespace, &name).await?;
            let head = VectorMaintenanceHeadV1::from_entry(&entry)?;
            if entry.head().resource_identity() != &resource_identity || snapshot.head() != Some(&head) {
                return Ok(VectorMaintenanceWorkerResult::Skipped(VectorMaintenanceNotEligible::ResourceChanged));
            }
            if *head.entry_version() != 3 {
                return Ok(VectorMaintenanceWorkerResult::Skipped(
                    VectorMaintenanceNotEligible::MaintenanceEnrollmentRequired,
                ));
            }
            guard.check_held()?;
            drop(guard);
            let commit_id = vector_maintenance_enroll_commit_id(&resource_identity, &head)?;
            control.check_admission()?;
            source.authorize_preparation(&identity, &resource).await?;
            control.check_admission()?;
            #[cfg(test)]
            fault_observation::phase(ObservedPhase::Preparation);
            executor
                .prepare(
                    store.clone(),
                    VectorMaintenancePreparationAction::EnrollCurrent {
                        expected: Box::new(entry),
                        commit_id,
                    },
                    control.clone(),
                )
                .await?;
        }
        super::periodic::VectorMaintenancePreparationKind::Deleted => {
            let update_id = vector_maintenance_enroll_deleted_update_id(&resource_identity, &expected)?;
            control.check_admission()?;
            source.authorize_preparation(&identity, &resource).await?;
            control.check_admission()?;
            #[cfg(test)]
            fault_observation::phase(ObservedPhase::Preparation);
            executor
                .prepare(
                    store.clone(),
                    VectorMaintenancePreparationAction::EnrollDeleted {
                        resource: resource_identity,
                        expected_intent: expected,
                        update_id,
                    },
                    control.clone(),
                )
                .await?;
        }
    }
    Ok(VectorMaintenanceWorkerResult::Prepared)
}

pub(crate) async fn execute_vector_maintenance_action<B: TableCatalogObjectBackend>(
    store: ObjectTableCatalogStore<B>,
    action: VectorMaintenancePreparationAction,
    context: VectorMaintenanceExecutionContext<B>,
    control: VectorMaintenanceControl,
) -> Result<VectorMaintenanceWorkerResult, VectorMaintenanceExecutionError> {
    let VectorMaintenanceExecutionContext {
        identity,
        source,
        executor,
    } = context;
    control.check_admission()?;
    source.check_admission(&identity).await?;
    control.check_admission()?;
    // The fixed preparation executor checks current action-specific permissions and
    // revalidates the exact source/head while holding the real Catalog protections.
    #[cfg(test)]
    fault_observation::phase(ObservedPhase::Preparation);
    executor.prepare(Arc::new(store), action, control).await?;
    Ok(VectorMaintenanceWorkerResult::Prepared)
}

#[cfg(test)]
mod vector_worker_tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    struct DropOwner(Arc<AtomicBool>);
    impl Drop for DropOwner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn vector_worker_deadline_retains_execute_owner() {
        let token = CancellationToken::new();
        let budgets = VectorMaintenanceBudgets::new(&VectorMaintenanceRuntimeConfig::default(), None).unwrap();
        let control = VectorMaintenanceControl::new(token.clone(), Instant::now() + Duration::from_secs(5), budgets);
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = DropOwner(dropped.clone());
        let (entered, started) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        let task = tokio::spawn(coordinate_vector_execution(
            async move {
                let _owner = owner;
                entered.send(()).unwrap();
                resume.await.unwrap();
                17u8
            },
            |_| async { Ok(Uuid::from_u128(2)) },
            control,
            Duration::from_secs(60),
            Uuid::from_u128(1),
        ));
        started.await.unwrap();
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::timeout(Duration::from_secs(1), token.cancelled()).await.unwrap();
        assert!(token.is_cancelled());
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(!task.is_finished());
        release.send(()).unwrap();
        let result = task.await.unwrap();
        assert_eq!(result.output, 17);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn vector_worker_heartbeat_conflict_stops_new_work_and_drains() {
        let token = CancellationToken::new();
        let budgets = VectorMaintenanceBudgets::new(&VectorMaintenanceRuntimeConfig::default(), None).unwrap();
        let control = VectorMaintenanceControl::new(token.clone(), Instant::now() + Duration::from_secs(900), budgets);
        let calls = Arc::new(AtomicUsize::new(0));
        let heartbeat_calls = calls.clone();
        let (heartbeat_tx, mut heartbeat_rx) = tokio::sync::mpsc::channel(2);
        let (release, resume) = oneshot::channel();
        let (entered, started) = oneshot::channel();
        let task = tokio::spawn(coordinate_vector_execution(
            async move {
                entered.send(()).unwrap();
                resume.await.unwrap()
            },
            move |revision| {
                let count = heartbeat_calls.fetch_add(1, Ordering::SeqCst);
                let heartbeat_tx = heartbeat_tx.clone();
                async move {
                    heartbeat_tx.send(count).await.unwrap();
                    if count == 0 {
                        assert_eq!(revision, Uuid::from_u128(1));
                        Ok(Uuid::from_u128(2))
                    } else {
                        assert_eq!(revision, Uuid::from_u128(2));
                        Err(TableCatalogStoreError::Conflict("changed revision".into()))
                    }
                }
            },
            control,
            Duration::from_secs(60),
            Uuid::from_u128(1),
        ));
        started.await.unwrap();
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(heartbeat_rx.recv().await, Some(0));
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(heartbeat_rx.recv().await, Some(1));
        tokio::time::timeout(Duration::from_secs(1), token.cancelled()).await.unwrap();
        assert!(token.is_cancelled());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!task.is_finished());
        release.send(()).unwrap();
        let result = task.await.unwrap();
        assert_eq!(result.revision, Uuid::from_u128(2));
        assert!(result.heartbeat_error.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn vector_worker_native_recovery_deadline_drains_original_future() {
        let token = CancellationToken::new();
        let budgets = VectorMaintenanceBudgets::new(&VectorMaintenanceRuntimeConfig::default(), None).unwrap();
        let control = VectorMaintenanceControl::new(token.clone(), Instant::now() + Duration::from_secs(5), budgets);
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = DropOwner(dropped.clone());
        let (entered, started) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        let task = tokio::spawn(coordinate_vector_native_recovery(
            async move {
                let _owner = owner;
                entered.send(()).unwrap();
                resume.await.unwrap();
                Err::<(), _>(TableCatalogStoreError::Unavailable("native recovery unresolved".into()))
            },
            control,
        ));
        started.await.unwrap();
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::timeout(Duration::from_secs(1), token.cancelled()).await.unwrap();
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(!task.is_finished());
        release.send(()).unwrap();
        assert!(matches!(task.await.unwrap(), Err(TableCatalogStoreError::Unavailable(_))));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn vector_worker_native_recovery_cancellation_retains_io_permit() {
        let token = CancellationToken::new();
        let config = VectorMaintenanceRuntimeConfig {
            io_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        };
        let budgets = VectorMaintenanceBudgets::new(&config, None).unwrap();
        let control = VectorMaintenanceControl::new(token.clone(), Instant::now() + Duration::from_secs(900), budgets.clone());
        assert!(control.dispatch_guard().is_err());
        let io = control.acquire_io().await.unwrap();
        let (entered, started) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        let task = tokio::spawn(coordinate_vector_native_recovery(
            async move {
                let _io = io;
                entered.send(()).unwrap();
                resume.await.unwrap();
            },
            control,
        ));
        started.await.unwrap();
        token.cancel();
        assert!(budgets.try_acquire_io().is_err());
        assert!(!task.is_finished());
        release.send(()).unwrap();
        task.await.unwrap();
        assert!(budgets.try_acquire_io().is_ok());
    }
}
