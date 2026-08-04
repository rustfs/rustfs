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
