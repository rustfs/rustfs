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
