// Copyright 2026 RustFS Team
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

use super::super::object::{validate_commit_idempotency_entry_object, validate_commit_log_entry_object};
use super::*;

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    /// Rejects a captured snapshot that still requires commit/index recovery.
    ///
    /// This reads only the supplied snapshot. Callers hold the backup/publication
    /// fences and validate ordinary artifact hashes and metadata separately.
    /// Recovery classification is shared with the normal commit-recovery API;
    /// this check never finalizes a log, repairs an index, or changes a pointer.
    pub(super) fn validate_backup_commit_recovery(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<()> {
        let mut active = BTreeMap::<String, TableEntry>::new();
        let mut logs = BTreeMap::<String, BTreeMap<String, CommitLogEntry>>::new();
        let mut indexes = BTreeMap::<String, BTreeMap<String, CommitLogEntry>>::new();
        let mut seen_paths = BTreeSet::new();
        let root = self.paths.table_bucket_root_prefix(table_bucket);
        let commit_root = format!("{root}{COMMIT_LOG_ROOT}/");
        let index_root = format!("{root}{COMMIT_IDEMPOTENCY_ROOT}/");

        for object in objects.iter().filter(|object| object.bucket == self.catalog_bucket()) {
            if !seen_paths.insert(object.object.as_str()) {
                return Err(TableCatalogStoreError::Invalid("catalog backup has a duplicate object path".to_string()));
            }
            if object.object.ends_with(TABLE_ENTRY_FILE) {
                let entry: TableEntry = serde_json::from_slice(&object.data)
                    .map_err(|_| TableCatalogStoreError::Invalid("invalid table owner in backup commit state".to_string()))?;
                validate_table_entry_object(&self.paths, &object.object, &entry)?;
                if entry.table_bucket != table_bucket {
                    return Err(TableCatalogStoreError::Invalid(
                        "backup commit owner belongs to another bucket".to_string(),
                    ));
                }
                if matches!(entry.state, TableCatalogEntryState::Renaming | TableCatalogEntryState::Deleting) {
                    return Err(backup_commit_recovery_required("table owner has a pending operation"));
                }
                if entry.state == TableCatalogEntryState::Active && active.insert(entry.table_id.clone(), entry).is_some() {
                    return Err(backup_commit_recovery_required("more than one active catalog pointer owns a table id"));
                }
            } else if object.object.starts_with(&commit_root) {
                let commit: CommitLogEntry = serde_json::from_slice(&object.data)
                    .map_err(|_| TableCatalogStoreError::Invalid("invalid commit log in catalog backup".to_string()))?;
                validate_commit_log_entry_object(&self.paths, &object.object, table_bucket, &commit.table_id, &commit)?;
                if logs
                    .entry(commit.table_id.clone())
                    .or_default()
                    .insert(commit.commit_id.clone(), commit)
                    .is_some()
                {
                    return Err(TableCatalogStoreError::Invalid("duplicate backup commit lookup identity".to_string()));
                }
            } else if object.object.starts_with(&index_root) {
                let commit: CommitLogEntry = serde_json::from_slice(&object.data)
                    .map_err(|_| TableCatalogStoreError::Invalid("invalid idempotency index in catalog backup".to_string()))?;
                validate_commit_idempotency_entry_object(&self.paths, &object.object, table_bucket, &commit.table_id, &commit)?;
                let key = commit
                    .idempotency_key
                    .as_ref()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("backup idempotency index has no lookup key".to_string()))?;
                if indexes
                    .entry(commit.table_id.clone())
                    .or_default()
                    .insert(key.clone(), commit)
                    .is_some()
                {
                    return Err(TableCatalogStoreError::Invalid(
                        "duplicate backup idempotency lookup identity".to_string(),
                    ));
                }
            }
        }

        for (table_id, entry) in &active {
            let Some(commits) = logs.get(table_id) else {
                continue;
            };
            // Commit records are keyed by immutable table id, not its logical
            // name. A rename source tombstone is not a second current pointer.
            let history = TableCommitHistoryIndex::new(entry, commits.values());
            for commit in commits.values() {
                let indexed = commit
                    .idempotency_key
                    .as_ref()
                    .and_then(|key| indexes.get(table_id).and_then(|records| records.get(key)));
                let recovery = table_commit_recovery_entry_from_history(entry, commit, indexed, &history);
                if !matches!(
                    recovery.recovery_state,
                    TableCommitRecoveryState::Committed | TableCommitRecoveryState::Superseded
                ) {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "commit recovery required before backup or restore: {}/{}/{} commit {} is {:?}: {}",
                        entry.table_bucket,
                        entry.namespace,
                        entry.table,
                        recovery.commit_id,
                        recovery.recovery_state,
                        recovery.reason,
                    )));
                }
            }
        }

        // Check both directions. Looking up indexes only while iterating logs
        // would miss an index whose commit record disappeared. Dropped-table
        // history can remain in object backing; retain valid paired records
        // without inventing a current pointer or comparing a rename tombstone.
        for (table_id, commits) in &logs {
            for commit in commits.values() {
                let Some(key) = commit.idempotency_key.as_ref() else {
                    continue;
                };
                if indexes.get(table_id).and_then(|records| records.get(key)) != Some(commit) {
                    return Err(backup_commit_recovery_required("commit log has no identical idempotency receipt"));
                }
            }
        }
        for (table_id, records) in &indexes {
            for indexed in records.values() {
                if logs.get(table_id).and_then(|commits| commits.get(&indexed.commit_id)) != Some(indexed) {
                    return Err(backup_commit_recovery_required("idempotency receipt has no identical commit log"));
                }
            }
        }
        Ok(())
    }
}

fn backup_commit_recovery_required(reason: &str) -> TableCatalogStoreError {
    TableCatalogStoreError::Conflict(format!(
        "commit recovery required before backup or restore: {reason}; inspect the persisted commit and idempotency records, recover the existing request, then capture a stable backup"
    ))
}

#[cfg(test)]
#[path = "object_backup_commit_recovery_tests.rs"]
mod tests;
