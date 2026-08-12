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

use std::collections::{BTreeMap, BTreeSet};

use super::super::*;

pub(crate) fn commit_log_matches_request(commit_log: &CommitLogEntry, request: &TableCommitRequest, table_id: &str) -> bool {
    commit_log.version == TABLE_CATALOG_ENTRY_VERSION
        && commit_log.commit_id == request.commit_id
        && commit_log.idempotency_key == request.idempotency_key
        && commit_log.table_id == table_id
        && commit_log.operation == request.operation
        && commit_log.expected_version_token == request.expected_version_token
        && commit_log.previous_metadata_location == request.expected_metadata_location
        && commit_log.new_metadata_location == request.new_metadata_location
        && commit_log.requirements == request.requirements
        && commit_log.writer == request.writer
}

pub(crate) fn table_matches_committed_log(table: &TableEntry, commit_log: &CommitLogEntry) -> bool {
    table.table_id == commit_log.table_id
        && table.metadata_location == commit_log.new_metadata_location
        && table.version_token == commit_log.new_version_token
}

pub(crate) fn table_matches_staged_base(table: &TableEntry, commit_log: &CommitLogEntry) -> bool {
    table.table_id == commit_log.table_id
        && table.metadata_location == commit_log.previous_metadata_location
        && table.version_token == commit_log.expected_version_token
}

pub(crate) struct TableCommitHistoryIndex<'a> {
    table_id: &'a str,
    reachable_states: BTreeSet<(&'a str, &'a str)>,
    ambiguous_states: BTreeSet<(&'a str, &'a str)>,
    cycle_detected: bool,
}

impl<'a> TableCommitHistoryIndex<'a> {
    pub(crate) fn new(table: &'a TableEntry, commits: impl IntoIterator<Item = &'a CommitLogEntry>) -> Self {
        let mut by_new_state = BTreeMap::<(&str, &str), Option<(&str, &str)>>::new();
        let mut ambiguous_states = BTreeSet::new();
        for commit in commits
            .into_iter()
            .filter(|commit| commit.table_id == table.table_id && !matches!(commit.status, CommitLogStatus::Failed))
        {
            let key = (commit.new_metadata_location.as_str(), commit.new_version_token.as_str());
            let previous = (commit.previous_metadata_location.as_str(), commit.expected_version_token.as_str());
            by_new_state
                .entry(key)
                .and_modify(|candidate| {
                    *candidate = None;
                    ambiguous_states.insert(key);
                })
                .or_insert(Some(previous));
        }

        let mut reachable_states = BTreeSet::new();
        let mut state = (table.metadata_location.as_str(), table.version_token.as_str());
        let cycle_detected = loop {
            if !reachable_states.insert(state) {
                break true;
            }
            let Some(Some(previous)) = by_new_state.get(&state) else {
                break false;
            };
            state = *previous;
        };
        Self {
            table_id: &table.table_id,
            reachable_states,
            ambiguous_states,
            cycle_detected,
        }
    }

    pub(crate) fn proves_committed(&self, target: &CommitLogEntry) -> bool {
        !self.cycle_detected
            && self.table_id == target.table_id.as_str()
            && !matches!(target.status, CommitLogStatus::Failed)
            && !self
                .ambiguous_states
                .contains(&(target.new_metadata_location.as_str(), target.new_version_token.as_str()))
            && self
                .reachable_states
                .contains(&(target.new_metadata_location.as_str(), target.new_version_token.as_str()))
    }
}

pub(crate) fn table_catalog_recovery_summary(
    metadata_status: &TableMetadataPointerStatus,
    commit_recovery: &TableCommitRecoveryReport,
) -> (TableCatalogRecoveryStatus, Vec<TableCatalogRecoveryAction>) {
    let mut actions = Vec::new();
    let metadata_status = match metadata_status {
        TableMetadataPointerStatus::Valid => None,
        TableMetadataPointerStatus::MissingObject => {
            actions.push(TableCatalogRecoveryAction::RestoreCurrentMetadataObject);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
        TableMetadataPointerStatus::InvalidJson => {
            actions.push(TableCatalogRecoveryAction::FixCurrentMetadataJson);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
        TableMetadataPointerStatus::InvalidLocation => {
            actions.push(TableCatalogRecoveryAction::MoveCurrentMetadataInsideTable);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
    };

    if commit_recovery.manual_review_count > 0 {
        actions.push(TableCatalogRecoveryAction::ReviewCommitLog);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::ManualReviewRequired), actions);
    }
    if commit_recovery.finalization_required_count > 0 || commit_recovery.idempotency_repair_required_count > 0 {
        actions.push(TableCatalogRecoveryAction::RunCommitRecovery);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Recoverable), actions);
    }
    if commit_recovery.staged_before_table_update_count > 0 {
        actions.push(TableCatalogRecoveryAction::RetryCommit);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Recoverable), actions);
    }

    (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Healthy), actions)
}

pub(crate) fn commit_logs_share_recovery_payload(left: &CommitLogEntry, right: &CommitLogEntry) -> bool {
    left.version == right.version
        && left.commit_id == right.commit_id
        && left.idempotency_key == right.idempotency_key
        && left.table_id == right.table_id
        && left.operation == right.operation
        && left.expected_version_token == right.expected_version_token
        && left.new_version_token == right.new_version_token
        && left.previous_metadata_location == right.previous_metadata_location
        && left.new_metadata_location == right.new_metadata_location
        && left.requirements == right.requirements
        && left.writer == right.writer
}

fn commit_idempotency_index_status(
    commit_log: &CommitLogEntry,
    idempotency_commit: Option<&CommitLogEntry>,
) -> TableCommitIdempotencyIndexStatus {
    match (commit_log.idempotency_key.as_ref(), idempotency_commit) {
        (None, _) => TableCommitIdempotencyIndexStatus::NotRequired,
        (Some(_), None) => TableCommitIdempotencyIndexStatus::Missing,
        (Some(_), Some(indexed)) if indexed == commit_log => TableCommitIdempotencyIndexStatus::Matches,
        (Some(_), Some(indexed)) if commit_logs_share_recovery_payload(indexed, commit_log) => {
            TableCommitIdempotencyIndexStatus::Stale
        }
        (Some(_), Some(_)) => TableCommitIdempotencyIndexStatus::Conflicting,
    }
}

pub(crate) fn table_commit_recovery_entry(
    table: &TableEntry,
    commit_log: &CommitLogEntry,
    idempotency_commit: Option<&CommitLogEntry>,
    historically_committed: bool,
) -> TableCommitRecoveryEntry {
    let idempotency_index_status = commit_idempotency_index_status(commit_log, idempotency_commit);
    let idempotency_index_present = matches!(
        idempotency_index_status,
        TableCommitIdempotencyIndexStatus::Matches
            | TableCommitIdempotencyIndexStatus::Stale
            | TableCommitIdempotencyIndexStatus::Conflicting
    );
    let idempotency_index_repair_required = matches!(
        idempotency_index_status,
        TableCommitIdempotencyIndexStatus::Missing | TableCommitIdempotencyIndexStatus::Stale
    );

    let (recovery_state, reason) = if matches!(idempotency_index_status, TableCommitIdempotencyIndexStatus::Conflicting) {
        (
            TableCommitRecoveryState::ManualReview,
            "idempotency index points at a different commit payload".to_string(),
        )
    } else if matches!(commit_log.status, CommitLogStatus::Failed) {
        (
            TableCommitRecoveryState::ManualReview,
            "failed commit log cannot be finalized automatically".to_string(),
        )
    } else if table_matches_committed_log(table, commit_log) {
        if matches!(commit_log.status, CommitLogStatus::Committed) {
            if idempotency_index_repair_required {
                (
                    TableCommitRecoveryState::IdempotencyIndexRepairRequired,
                    "committed table pointer is durable but idempotency index needs repair".to_string(),
                )
            } else {
                (
                    TableCommitRecoveryState::Committed,
                    "commit log and current table pointer agree".to_string(),
                )
            }
        } else {
            (
                TableCommitRecoveryState::FinalizationRequired,
                "current table pointer already advanced but commit log is not finalized".to_string(),
            )
        }
    } else if matches!(commit_log.status, CommitLogStatus::Staged) && historically_committed {
        (
            TableCommitRecoveryState::FinalizationRequired,
            "a later committed pointer proves this staged commit is part of table history".to_string(),
        )
    } else if matches!(commit_log.status, CommitLogStatus::Committed) && historically_committed {
        if idempotency_index_repair_required {
            (
                TableCommitRecoveryState::IdempotencyIndexRepairRequired,
                "historical committed log needs idempotency index repair".to_string(),
            )
        } else {
            (
                TableCommitRecoveryState::Committed,
                "commit is finalized and may be older than the current table pointer".to_string(),
            )
        }
    } else if matches!(commit_log.status, CommitLogStatus::Committed) {
        (
            TableCommitRecoveryState::ManualReview,
            "committed log is not reachable from the current table pointer".to_string(),
        )
    } else if table_matches_staged_base(table, commit_log) {
        (
            TableCommitRecoveryState::StagedBeforeTableUpdate,
            "staged commit exists but table pointer has not advanced".to_string(),
        )
    } else {
        (
            TableCommitRecoveryState::ManualReview,
            "staged commit no longer matches the current table pointer or its expected base".to_string(),
        )
    };

    TableCommitRecoveryEntry {
        commit_id: commit_log.commit_id.clone(),
        idempotency_key: commit_log.idempotency_key.clone(),
        operation: commit_log.operation.clone(),
        status: commit_log.status.clone(),
        recovery_state,
        previous_metadata_location: commit_log.previous_metadata_location.clone(),
        new_metadata_location: commit_log.new_metadata_location.clone(),
        expected_version_token: commit_log.expected_version_token.clone(),
        new_version_token: commit_log.new_version_token.clone(),
        idempotency_index_present,
        idempotency_index_status,
        reason,
    }
}

pub(crate) fn record_table_commit_attempt(operation: &str) {
    counter!("rustfs_table_catalog_commit_attempts_total", "operation" => operation.to_string()).increment(1);
}

fn table_catalog_store_result_label<T>(result: &TableCatalogStoreResult<T>) -> &'static str {
    match result {
        Ok(_) => "success",
        Err(TableCatalogStoreError::Conflict(_) | TableCatalogStoreError::AlreadyExists(_)) => "conflict",
        Err(TableCatalogStoreError::Invalid(_)) => "invalid",
        Err(
            TableCatalogStoreError::NotFound(_)
            | TableCatalogStoreError::NamespaceNotFound(_)
            | TableCatalogStoreError::TableNotFound(_),
        ) => "not_found",
        Err(TableCatalogStoreError::Unsupported(_)) => "unsupported",
        Err(TableCatalogStoreError::Internal(_)) => "failure",
    }
}

fn duration_millis_u64(duration: StdDuration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn record_table_commit_cas_result(operation: &str, started: Instant, result: &TableCatalogStoreResult<()>) {
    let elapsed = started.elapsed();
    let result_label = table_catalog_store_result_label(result);
    counter!(
        "rustfs_table_catalog_commit_cas_results_total",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .increment(1);
    histogram!(
        "rustfs_table_catalog_commit_cas_duration_seconds",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .record(elapsed.as_secs_f64());
}

fn record_table_commit_result(
    table_bucket: &str,
    namespace: &str,
    table: &str,
    commit_id: &str,
    operation: &str,
    started: Instant,
    result: &TableCatalogStoreResult<TableCommitResult>,
) {
    let elapsed = started.elapsed();
    let result_label = table_catalog_store_result_label(result);
    counter!(
        "rustfs_table_catalog_commit_results_total",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .increment(1);
    if matches!(result, Err(TableCatalogStoreError::Conflict(_))) {
        counter!("rustfs_table_catalog_commit_conflicts_total", "operation" => operation.to_string()).increment(1);
    }
    histogram!(
        "rustfs_table_catalog_commit_duration_seconds",
        "operation" => operation.to_string(),
        "result" => result_label.to_string()
    )
    .record(elapsed.as_secs_f64());

    match result {
        Ok(commit) if elapsed >= TABLE_COMMIT_SLOW_LOG_THRESHOLD => {
            tracing::warn!(
                table_bucket,
                namespace,
                table,
                commit_id,
                operation,
                generation = commit.table.generation,
                duration_ms = duration_millis_u64(elapsed),
                "slow table catalog commit"
            );
        }
        Ok(commit) => {
            tracing::debug!(
                table_bucket,
                namespace,
                table,
                commit_id,
                operation,
                generation = commit.table.generation,
                duration_ms = duration_millis_u64(elapsed),
                "table catalog commit completed"
            );
        }
        Err(error) => {
            tracing::warn!(
                table_bucket,
                namespace,
                table,
                commit_id,
                operation,
                result = result_label,
                duration_ms = duration_millis_u64(elapsed),
                error = %error,
                "table catalog commit did not complete"
            );
        }
    }
}

pub(crate) fn table_commit_result(
    table_bucket: &str,
    namespace: &str,
    table: &str,
    commit_id: &str,
    operation: &str,
    started: Instant,
    result: TableCatalogStoreResult<TableCommitResult>,
) -> TableCatalogStoreResult<TableCommitResult> {
    record_table_commit_result(table_bucket, namespace, table, commit_id, operation, started, &result);
    result
}
