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

use super::*;

fn vector_resource_successor_matches(table: &TableEntry, log: &CommitLogEntry) -> bool {
    match (
        table.version,
        log.version,
        table.vector_index_definition.as_ref(),
        log.vector_resource_mutation.as_ref(),
    ) {
        (3 | 4, 3 | 4, Some(definition), Some(mutation)) => {
            table.version == mutation.entry_version()
                && log.version == mutation.entry_version()
                && mutation.validate().is_ok()
                && definition == mutation.next_definition()
                && &table.state == mutation.next_state()
                && table.vector_maintenance_binding.as_ref() == mutation.next_maintenance()
        }
        (1, 1, None, None) | (2, 2, None, None) => true,
        _ => false,
    }
}

fn vector_resource_predecessor_matches(table: &TableEntry, log: &CommitLogEntry) -> bool {
    match (
        table.version,
        log.version,
        table.vector_index_definition.as_ref(),
        log.vector_resource_mutation.as_ref(),
    ) {
        (3 | 4, 3 | 4, Some(definition), Some(mutation)) => {
            log.version == mutation.entry_version()
                && table.version
                    == if mutation.kind() == VectorResourceMutationKind::Enroll {
                        3
                    } else {
                        log.version
                    }
                && mutation.validate().is_ok()
                && definition == mutation.expected_definition()
                && &table.state == mutation.expected_state()
                && table.vector_maintenance_binding.as_ref() == mutation.expected_maintenance()
        }
        (1, 1, None, None) | (2, 2, None, None) => true,
        _ => false,
    }
}

pub(crate) fn table_matches_committed_log(table: &TableEntry, commit_log: &CommitLogEntry) -> bool {
    table.table_id == commit_log.table_id
        && table.metadata_location == commit_log.new_metadata_location
        && table.version_token == commit_log.new_version_token
        && vector_resource_successor_matches(table, commit_log)
        && match (&table.vector_head_binding, &commit_log.vector_commit_binding) {
            (None, None) => true,
            (Some(head), Some(commit)) => {
                head.bucket == commit.bucket
                    && head.table_id == commit.table_id
                    && head.metadata == commit.candidate_metadata
                    && head.descriptor == commit.descriptor
                    && commit.expected_generation.checked_add(1) == Some(table.generation)
            }
            _ => false,
        }
}

pub(crate) fn table_matches_staged_base(table: &TableEntry, commit_log: &CommitLogEntry) -> bool {
    table.table_id == commit_log.table_id
        && table.metadata_location == commit_log.previous_metadata_location
        && table.version_token == commit_log.expected_version_token
        && vector_resource_predecessor_matches(table, commit_log)
        && match (&table.vector_head_binding, &commit_log.vector_commit_binding) {
            (None, None) => true,
            (Some(head), Some(commit)) => {
                head.bucket == commit.bucket
                    && head.table_id == commit.table_id
                    && commit.expected_metadata.as_ref() == Some(&head.metadata)
                    && commit.expected_descriptor.as_ref() == Some(&head.descriptor)
                    && commit.expected_generation == table.generation
            }
            _ => false,
        }
}

pub(crate) struct TableCommitHistoryIndex<'a> {
    table_id: &'a str,
    reachable_states: BTreeSet<(&'a str, &'a str)>,
    reachable_version_tokens: BTreeSet<&'a str>,
    committed_successors: BTreeMap<(&'a str, &'a str), &'a CommitLogEntry>,
    ambiguous_states: BTreeSet<(&'a str, &'a str)>,
    cycle_detected: bool,
    version_token_reused: bool,
    vector_entry: bool,
    vector_history_valid: bool,
    vector_commits: BTreeMap<&'a str, &'a CommitLogEntry>,
}

impl<'a> TableCommitHistoryIndex<'a> {
    pub(crate) fn new(table: &'a TableEntry, commits: impl IntoIterator<Item = &'a CommitLogEntry>) -> Self {
        let mut by_new_state = BTreeMap::<(&str, &str), Option<&CommitLogEntry>>::new();
        let mut ambiguous_states = BTreeSet::new();
        for commit in commits
            .into_iter()
            .filter(|commit| commit.table_id == table.table_id && !matches!(commit.status, CommitLogStatus::Failed))
        {
            let key = (commit.new_metadata_location.as_str(), commit.new_version_token.as_str());
            by_new_state
                .entry(key)
                .and_modify(|candidate| {
                    *candidate = None;
                    ambiguous_states.insert(key);
                })
                .or_insert(Some(commit));
        }

        let mut reachable_states = BTreeSet::new();
        let mut reachable_version_tokens = BTreeSet::new();
        let mut committed_successors = BTreeMap::new();
        let mut version_token_reused = false;
        let vector_entry = matches!(table.version, 3 | 4);
        let mut vector_cursor = VectorHistoryCursor::from_entry(table);
        let mut vector_history_valid = !vector_entry || vector_cursor.is_some();
        let mut vector_commits = BTreeMap::new();
        let mut state = (table.metadata_location.as_str(), table.version_token.as_str());
        let cycle_detected = loop {
            if !reachable_states.insert(state) {
                break true;
            }
            version_token_reused |= !reachable_version_tokens.insert(state.1);
            let Some(commit) = by_new_state.get(&state).copied().flatten() else {
                break false;
            };
            if vector_entry
                && (!vector_cursor.as_mut().is_some_and(|cursor| cursor.accept(commit))
                    || vector_commits.insert(commit.commit_id.as_str(), commit).is_some())
            {
                vector_history_valid = false;
                break false;
            }
            let previous = (commit.previous_metadata_location.as_str(), commit.expected_version_token.as_str());
            if matches!(commit.status, CommitLogStatus::Committed) {
                committed_successors.insert(previous, commit);
            }
            state = previous;
        };
        if vector_entry {
            vector_history_valid &=
                vector_cursor.is_some_and(|cursor| cursor.is_initial()) && !version_token_reused && !cycle_detected;
        }
        Self {
            table_id: &table.table_id,
            reachable_states,
            reachable_version_tokens,
            committed_successors,
            ambiguous_states,
            cycle_detected,
            version_token_reused,
            vector_entry,
            vector_history_valid,
            vector_commits,
        }
    }

    pub(crate) fn proves_committed(&self, target: &CommitLogEntry) -> bool {
        if self.vector_entry {
            return self.vector_history_valid
                && !matches!(target.status, CommitLogStatus::Failed)
                && self
                    .vector_commits
                    .get(target.commit_id.as_str())
                    .is_some_and(|committed| commit_logs_share_recovery_payload(committed, target));
        }
        !matches!(target.version, 3 | 4)
            && !self.cycle_detected
            && self.table_id == target.table_id.as_str()
            && !matches!(target.status, CommitLogStatus::Failed)
            && !self
                .ambiguous_states
                .contains(&(target.new_metadata_location.as_str(), target.new_version_token.as_str()))
            && self
                .reachable_states
                .contains(&(target.new_metadata_location.as_str(), target.new_version_token.as_str()))
    }

    fn proven_superseding_commit(&self, target: &CommitLogEntry) -> Option<&CommitLogEntry> {
        if (self.vector_entry && !self.vector_history_valid)
            || (!self.vector_entry && matches!(target.version, 3 | 4))
            || self.cycle_detected
            || self.version_token_reused
            || !self.ambiguous_states.is_empty()
            || self.table_id != target.table_id
            || !matches!(target.status, CommitLogStatus::Staged)
            || self.reachable_version_tokens.contains(target.new_version_token.as_str())
        {
            return None;
        }
        let base = (target.previous_metadata_location.as_str(), target.expected_version_token.as_str());
        self.committed_successors.get(&base).copied().filter(|winner| {
            winner.commit_id != target.commit_id && (!self.vector_entry || vector_recovery_predecessors_match(winner, target))
        })
    }
}

/// Classifies an object-backed staged loser without rewriting its receipt.
///
/// A reachable finalized successor from the same pointer/token base proves that
/// the old request lost CAS. Its STAGED record and idempotency binding remain
/// available, and replay of that old request must still conflict. Missing or
/// contradictory proof remains a manual-review state.
pub(crate) fn table_commit_recovery_entry_from_history(
    table: &TableEntry,
    commit_log: &CommitLogEntry,
    idempotency_commit: Option<&CommitLogEntry>,
    history: &TableCommitHistoryIndex<'_>,
) -> TableCommitRecoveryEntry {
    let mut entry = table_commit_recovery_entry(table, commit_log, idempotency_commit, history.proves_committed(commit_log));
    if entry.recovery_state == TableCommitRecoveryState::ManualReview
        && matches!(
            entry.idempotency_index_status,
            TableCommitIdempotencyIndexStatus::NotRequired | TableCommitIdempotencyIndexStatus::Matches
        )
        && let Some(winner) = history.proven_superseding_commit(commit_log)
    {
        entry.recovery_state = TableCommitRecoveryState::Superseded;
        entry.reason = format!("staged request lost CAS to reachable committed successor {}", winner.commit_id);
    }
    entry
}

pub(crate) fn commit_logs_share_recovery_payload(left: &CommitLogEntry, right: &CommitLogEntry) -> bool {
    left.version == right.version
        && left.vector_commit_binding == right.vector_commit_binding
        && left.vector_resource_mutation == right.vector_resource_mutation
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
        Err(TableCatalogStoreError::Unavailable(_) | TableCatalogStoreError::LockUnavailable { .. }) => "unavailable",
        Err(TableCatalogStoreError::Internal(_)) => "failure",
        Err(TableCatalogStoreError::Io { classification, .. }) => match classification {
            CatalogIoErrorKind::Denied => "denied",
            CatalogIoErrorKind::Conflict => "conflict",
            CatalogIoErrorKind::Unavailable => "unavailable",
            CatalogIoErrorKind::Internal => "failure",
        },
    }
}

fn duration_millis_u64(duration: StdDuration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn record_table_commit_cas_result<T>(operation: &str, started: Instant, result: &TableCatalogStoreResult<T>) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_catalog_unavailable_result_label_is_stable() {
        for error in [
            TableCatalogStoreError::Unavailable("catalog publication authority is temporarily unavailable".to_string()),
            TableCatalogStoreError::LockUnavailable {
                message: "catalog publication authority is temporarily unavailable".to_string(),
                source: Box::new(rustfs_lock::LockError::timeout("catalog/lock", StdDuration::from_secs(5))),
            },
        ] {
            let result: TableCatalogStoreResult<()> = Err(error);
            assert_eq!(table_catalog_store_result_label(&result), "unavailable");
        }
    }
}

pub(crate) fn validate_catalog_entry_version(kind: &str, version: u16) -> TableCatalogStoreResult<()> {
    if version != TABLE_CATALOG_ENTRY_VERSION {
        return Err(TableCatalogStoreError::Invalid(format!("unsupported {kind} entry version")));
    }
    Ok(())
}

struct CatalogDigestEncoder(Sha256);

impl CatalogDigestEncoder {
    fn bytes(&mut self, value: &[u8]) -> TableCatalogStoreResult<()> {
        self.0.update([1]);
        self.0.update(
            u64::try_from(value.len())
                .map_err(|_| TableCatalogStoreError::Invalid("catalog digest input too large".to_string()))?
                .to_be_bytes(),
        );
        self.0.update(value);
        Ok(())
    }
    fn text(&mut self, value: &str) -> TableCatalogStoreResult<()> {
        self.0.update([2]);
        self.bytes(value.as_bytes())
    }
    fn number(&mut self, value: u64) {
        self.0.update([3]);
        self.0.update(value.to_be_bytes());
    }
    fn present(&mut self, present: bool) {
        self.0.update([4, u8::from(present)]);
    }
    fn optional_text(&mut self, value: Option<&str>) -> TableCatalogStoreResult<()> {
        self.present(value.is_some());
        if let Some(value) = value {
            self.text(value)?;
        }
        Ok(())
    }
    fn metadata(&mut self, metadata: &CatalogMetadataRef) -> TableCatalogStoreResult<()> {
        self.0.update([5]);
        self.text(metadata.object_key().as_str())?;
        self.number(metadata.content_length());
        self.bytes(metadata.content_sha256().as_bytes())?;
        self.present(metadata.body_revision().is_some());
        if let Some(revision) = metadata.body_revision() {
            self.bytes(revision.as_uuid().as_bytes())?;
        }
        self.optional_text(metadata.etag())
    }
    fn descriptor(&mut self, descriptor: &CatalogFormatDescriptor) -> TableCatalogStoreResult<()> {
        self.0.update([6]);
        self.number(u64::from(descriptor.encoding_version()));
        self.text(descriptor.format().as_str())?;
        self.number(u64::from(descriptor.format_version()));
        self.metadata(descriptor.metadata())?;
        self.bytes(&Sha256::digest(descriptor.payload()))
    }
}

pub(in crate::table_catalog) fn catalog_prepared_request_digest(
    prepared: &PreparedCatalogCommit,
) -> TableCatalogStoreResult<CatalogRequestDigest> {
    let mut encoding = CatalogDigestEncoder(Sha256::new());
    encoding.text("rustfs.catalog.prepared-commit.v1")?;
    let resource = prepared.resource_identity();
    encoding.text(match resource.format() {
        CatalogFormat::Iceberg => "table",
        CatalogFormat::Lance => "vector",
    })?;
    encoding.text(resource.storage_bucket_id())?;
    encoding.bytes(resource.incarnation().as_bytes())?;
    if let Some(bucket) = resource.vector_bucket() {
        encoding.text(bucket.account_namespace_id().as_str())?;
        encoding.text(bucket.region())?;
        encoding.text(bucket.public_name().as_str())?;
    }
    encoding.text(resource.table_id().as_str())?;
    encoding.text(resource.format().as_str())?;
    encoding.number(
        u64::try_from(prepared.namespace().segments().len())
            .map_err(|_| TableCatalogStoreError::Invalid("namespace segment count overflow".to_string()))?,
    );
    for segment in prepared.namespace().segments() {
        encoding.text(segment.as_str())?;
    }
    encoding.text(prepared.name().as_str())?;
    let expected = prepared.expected_head();
    encoding.text(expected.version_token().as_str())?;
    encoding.number(expected.generation().get());
    encoding.present(expected.metadata().is_some());
    if let Some(metadata) = expected.metadata() {
        encoding.metadata(metadata)?;
    }
    encoding.present(expected.format_descriptor().is_some());
    if let Some(descriptor) = expected.format_descriptor() {
        encoding.descriptor(descriptor)?;
    }
    encoding.metadata(prepared.candidate_metadata())?;
    encoding.descriptor(prepared.format_descriptor())?;
    encoding.text(prepared.commit_id().as_str())?;
    encoding.text(prepared.operation())?;
    encoding.optional_text(prepared.idempotency_key())?;
    encoding.optional_text(prepared.writer())?;
    encoding.optional_text(prepared.entry_updates().warehouse_location())?;
    encoding.present(prepared.entry_updates().format_version().is_some());
    if let Some(version) = prepared.entry_updates().format_version() {
        encoding.number(u64::from(version));
    }
    encoding.bytes(prepared.legacy_requirements())?;
    Ok(CatalogRequestDigest::from_bytes(encoding.0.finalize().into()))
}

pub(crate) fn commit_log_matches_prepared(log: &CommitLogEntry, prepared: &PreparedCatalogCommit, table_id: &str) -> bool {
    let request = CatalogCommitFields::from_prepared(prepared);
    let fields_match = log.commit_id == request.commit_id
        && log.idempotency_key == request.idempotency_key
        && log.table_id == table_id
        && log.operation == request.operation
        && log.expected_version_token == request.expected_version_token
        && log.previous_metadata_location == request.expected_metadata_location
        && log.new_metadata_location == request.new_metadata_location
        && log.writer == request.writer;
    if !fields_match {
        return false;
    }
    match prepared.resource_identity().format() {
        CatalogFormat::Iceberg => {
            log.version == TABLE_CATALOG_ENTRY_VERSION
                && log.vector_commit_binding.is_none()
                && log.vector_resource_mutation.is_none()
                && prepared
                    .compatibility_json_array()
                    .is_ok_and(|payload| payload.matches_values(&log.requirements))
        }
        CatalogFormat::Lance => {
            log.version
                == prepared
                    .vector_resource_mutation()
                    .map_or(2, VectorResourceMutationBinding::entry_version)
                && log.vector_resource_mutation.as_ref() == prepared.vector_resource_mutation()
                && log.requirements.is_empty()
                && VectorCommitBindingV1::from_prepared(prepared).ok().as_ref() == log.vector_commit_binding.as_ref()
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(in crate::table_catalog) struct CatalogCommitFields {
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub commit_id: String,
    pub idempotency_key: Option<String>,
    pub operation: String,
    pub expected_version_token: String,
    pub expected_metadata_location: String,
    pub new_metadata_location: String,
    pub writer: Option<String>,
}
impl CatalogCommitFields {
    pub(in crate::table_catalog) fn from_prepared(prepared: &PreparedCatalogCommit) -> Self {
        Self {
            table_bucket: prepared.resource_identity().storage_bucket_id().to_string(),
            namespace: prepared.namespace().public_name(),
            table: prepared.name().as_str().to_string(),
            commit_id: prepared.commit_id().as_str().to_string(),
            idempotency_key: prepared.idempotency_key().map(str::to_string),
            operation: prepared.operation().to_string(),
            expected_version_token: prepared.expected_head().version_token().as_str().to_string(),
            expected_metadata_location: prepared
                .expected_head()
                .metadata()
                .map(|metadata| metadata.object_key().as_str())
                .unwrap_or_default()
                .to_string(),
            new_metadata_location: prepared.candidate_metadata().object_key().as_str().to_string(),
            writer: prepared.writer().map(str::to_string),
        }
    }
}

pub(in crate::table_catalog) fn vector_committed_history_versions(
    entry: &TableEntry,
    records: &[CommitLogEntry],
) -> TableCatalogStoreResult<Vec<CatalogCommittedVersion>> {
    let head = CatalogHead::from_vector_entry(entry)?;
    let history_index = TableCommitHistoryIndex::new(entry, records.iter());
    let mut metadata = head.metadata().clone();
    let mut descriptor = head.format_descriptor().clone();
    let mut token = head.version_token().as_str().to_string();
    let mut generation = head.generation().get();
    let mut definition = entry.vector_index_definition.as_ref();
    let mut record_version = entry.version;
    let mut maintenance = entry.vector_maintenance_binding.as_ref();
    let mut state = &entry.state;
    let mut seen_tokens = BTreeSet::new();
    let mut versions = Vec::new();
    loop {
        if !seen_tokens.insert(token.clone()) {
            return Err(TableCatalogStoreError::Invalid("Vector history reuses a version token".to_string()));
        }
        let candidates = records
            .iter()
            .filter(|log| {
                !matches!(log.status, CommitLogStatus::Failed)
                    && log.new_version_token == token
                    && log.new_metadata_location == metadata.object_key().as_str()
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            if generation != 1 || (matches!(entry.version, 3 | 4) && state != &TableCatalogEntryState::Active) {
                return Err(TableCatalogStoreError::Invalid("Vector history is incomplete".to_string()));
            }
            versions.push(CatalogCommittedVersion::new(metadata, descriptor, None)?);
            break;
        }
        if candidates.len() != 1 {
            return Err(TableCatalogStoreError::Invalid("Vector history has ambiguous predecessors".to_string()));
        }
        let log = candidates[0];
        if !history_index.proves_committed(log) {
            return Err(TableCatalogStoreError::Invalid(
                "Vector history cannot prove committed ancestry".to_string(),
            ));
        }
        let binding = log
            .vector_commit_binding
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Vector history record has no binding".to_string()))?;
        binding.validate_log(log, &entry.table_bucket, &entry.table_id)?;
        match (record_version, log.version, definition, log.vector_resource_mutation.as_ref()) {
            (3 | 4, 3 | 4, Some(current), Some(mutation))
                if record_version == log.version
                    && log.version == mutation.entry_version()
                    && current == mutation.next_definition()
                    && state == mutation.next_state()
                    && maintenance == mutation.next_maintenance() =>
            {
                definition = Some(mutation.expected_definition());
                state = mutation.expected_state();
                maintenance = mutation.expected_maintenance();
                if mutation.kind() == VectorResourceMutationKind::Enroll {
                    record_version = 3;
                }
            }
            (2, 2, None, None) => {}
            _ => {
                return Err(TableCatalogStoreError::Invalid(
                    "Vector history resource mutation contradicts its successor".into(),
                ));
            }
        }
        if head.resource_identity().vector_bucket() != Some(&binding.bucket.to_handle()?)
            || binding.expected_generation.checked_add(1) != Some(generation)
            || CatalogMetadataRef::try_from(&binding.candidate_metadata)? != metadata
            || CatalogFormatDescriptor::try_from(&binding.descriptor)? != descriptor
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector history binding contradicts its successor".to_string(),
            ));
        }
        versions.push(CatalogCommittedVersion::new(
            metadata,
            descriptor,
            Some(CatalogCommitId::try_new(log.commit_id.clone())?),
        )?);
        metadata = CatalogMetadataRef::try_from(
            binding
                .expected_metadata
                .as_ref()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector history has no initial metadata".to_string()))?,
        )?;
        descriptor = CatalogFormatDescriptor::try_from(
            binding
                .expected_descriptor
                .as_ref()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector history has no initial descriptor".to_string()))?,
        )?;
        token.clone_from(&log.expected_version_token);
        generation = binding.expected_generation;
    }
    Ok(versions)
}

#[async_trait::async_trait]
pub(in crate::table_catalog) trait CatalogCommitPreparation: Send + Sync {
    async fn prepare(
        &self,
        current: &TableEntry,
        reader: &dyn CatalogObjectReader,
    ) -> TableCatalogStoreResult<PreparedCatalogCommit>;
}

pub(in crate::table_catalog) enum CatalogCommitInput<'a> {
    Prepared(Box<PreparedCatalogCommit>),
    Pending {
        fields: Box<CatalogCommitFields>,
        compatibility: CatalogJsonArray,
        preparation: &'a dyn CatalogCommitPreparation,
    },
}

impl CatalogCommitInput<'_> {
    pub(in crate::table_catalog) fn fields(&self) -> CatalogCommitFields {
        match self {
            Self::Prepared(prepared) => CatalogCommitFields::from_prepared(prepared),
            Self::Pending { fields, .. } => fields.as_ref().clone(),
        }
    }

    pub(in crate::table_catalog) fn prepared(&self) -> Option<&PreparedCatalogCommit> {
        match self {
            Self::Prepared(prepared) => Some(prepared),
            Self::Pending { .. } => None,
        }
    }

    pub(in crate::table_catalog) fn matches_log(&self, log: &CommitLogEntry, table_id: &str) -> bool {
        match self {
            Self::Prepared(prepared) => commit_log_matches_prepared(log, prepared, table_id),
            Self::Pending {
                fields, compatibility, ..
            } => {
                log.version == TABLE_CATALOG_ENTRY_VERSION
                    && log.vector_commit_binding.is_none()
                    && log.commit_id == fields.commit_id
                    && log.idempotency_key == fields.idempotency_key
                    && log.table_id == table_id
                    && log.operation == fields.operation
                    && log.expected_version_token == fields.expected_version_token
                    && log.previous_metadata_location == fields.expected_metadata_location
                    && log.new_metadata_location == fields.new_metadata_location
                    && log.writer == fields.writer
                    && compatibility.matches_values(&log.requirements)
            }
        }
    }

    pub(in crate::table_catalog) async fn prepare(
        self,
        current: &TableEntry,
        reader: Option<&dyn CatalogObjectReader>,
    ) -> TableCatalogStoreResult<PreparedCatalogCommit> {
        match self {
            Self::Prepared(prepared) => Ok(*prepared),
            Self::Pending {
                fields,
                compatibility,
                preparation,
            } => {
                let reader = reader
                    .ok_or_else(|| TableCatalogStoreError::Unsupported("catalog preparation needs an object reader".into()))?;
                let prepared = preparation.prepare(current, reader).await?;
                if CatalogCommitFields::from_prepared(&prepared) != *fields
                    || prepared.compatibility_json_array()? != compatibility
                {
                    return Err(TableCatalogStoreError::Invalid(
                        "catalog preparation changed its request identity".to_string(),
                    ));
                }
                Ok(prepared)
            }
        }
    }
}

struct VectorHistoryCursor<'a> {
    version: u16,
    maintenance: Option<&'a VectorMaintenanceBindingV1>,
    bucket: &'a VectorBucketBindingV1,
    table_id: &'a str,
    metadata: &'a CatalogMetadataRefRecordV1,
    descriptor: &'a CatalogFormatDescriptorRecordV1,
    generation: u64,
    definition: &'a VectorIndexDefinitionV1,
    state: &'a TableCatalogEntryState,
}

impl<'a> VectorHistoryCursor<'a> {
    fn from_entry(entry: &'a TableEntry) -> Option<Self> {
        if !matches!(entry.version, 3 | 4) || CatalogHead::from_vector_entry(entry).is_err() {
            return None;
        }
        let head = entry.vector_head_binding.as_ref()?;
        Some(Self {
            version: entry.version,
            maintenance: entry.vector_maintenance_binding.as_ref(),
            bucket: &head.bucket,
            table_id: &entry.table_id,
            metadata: &head.metadata,
            descriptor: &head.descriptor,
            generation: entry.generation,
            definition: entry.vector_index_definition.as_ref()?,
            state: &entry.state,
        })
    }

    fn accept(&mut self, commit: &'a CommitLogEntry) -> bool {
        let (Some(binding), Some(mutation)) = (&commit.vector_commit_binding, &commit.vector_resource_mutation) else {
            return false;
        };
        if commit.version != self.version
            || commit.version != mutation.entry_version()
            || self.maintenance != mutation.next_maintenance()
            || binding
                .validate_log(commit, &self.bucket.storage_bucket_id, self.table_id)
                .is_err()
            || &binding.bucket != self.bucket
            || &binding.candidate_metadata != self.metadata
            || &binding.descriptor != self.descriptor
            || binding.expected_generation.checked_add(1) != Some(self.generation)
            || mutation.next_definition() != self.definition
            || mutation.next_state() != self.state
        {
            return false;
        }
        let (Some(metadata), Some(descriptor)) = (&binding.expected_metadata, &binding.expected_descriptor) else {
            return false;
        };
        self.metadata = metadata;
        self.descriptor = descriptor;
        self.generation = binding.expected_generation;
        self.definition = mutation.expected_definition();
        self.state = mutation.expected_state();
        self.maintenance = mutation.expected_maintenance();
        if mutation.kind() == VectorResourceMutationKind::Enroll {
            self.version = 3;
        }
        true
    }

    fn is_initial(&self) -> bool {
        self.generation == 1 && self.version == 3 && self.maintenance.is_none() && self.state == &TableCatalogEntryState::Active
    }
}

fn vector_recovery_predecessors_match(left: &CommitLogEntry, right: &CommitLogEntry) -> bool {
    let (Some(left_binding), Some(right_binding), Some(left_mutation), Some(right_mutation)) = (
        &left.vector_commit_binding,
        &right.vector_commit_binding,
        &left.vector_resource_mutation,
        &right.vector_resource_mutation,
    ) else {
        return false;
    };
    matches!(left.version, 3 | 4)
        && left.version == right.version
        && left_binding
            .validate_log(left, &left_binding.bucket.storage_bucket_id, &left.table_id)
            .is_ok()
        && right_binding
            .validate_log(right, &right_binding.bucket.storage_bucket_id, &right.table_id)
            .is_ok()
        && left_binding.bucket == right_binding.bucket
        && left_binding.table_id == right_binding.table_id
        && left_binding.expected_generation == right_binding.expected_generation
        && left_binding.expected_metadata == right_binding.expected_metadata
        && left_binding.expected_descriptor == right_binding.expected_descriptor
        && left_mutation.expected_definition() == right_mutation.expected_definition()
        && left_mutation.expected_state() == right_mutation.expected_state()
        && left_mutation.expected_maintenance() == right_mutation.expected_maintenance()
}
