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

use super::object::{next_table_catalog_update_time, validate_commit_idempotency_entry_object, validate_commit_log_entry_object};
use super::*;

pub(in crate::table_catalog) struct TableBucketDisablement {
    pub(in crate::table_catalog) entry: TableBucketEntry,
    guards: Vec<TableCatalogLockGuard>,
}

impl TableBucketDisablement {
    pub(in crate::table_catalog) fn extend_mutation_fence(&self, fence: &mut TableCatalogObjectMutationFence) {
        fence
            .signals
            .extend(self.guards.iter().filter_map(TableCatalogLockGuard::lock_lost_signal));
    }
}

/// An in-memory, fenced cleanup plan. No mutation occurs until every object has
/// been validated and the caller has authorized all metadata deletions.
pub(crate) struct EmptyTableBucketCleanup {
    disablement: TableBucketDisablement,
    metadata: Vec<(String, String)>,
    indexes: Vec<(String, String)>,
    fence: TableCatalogObjectMutationFence,
}

impl EmptyTableBucketCleanup {
    pub(in crate::table_catalog) fn from_validated_references(
        disablement: TableBucketDisablement,
        metadata: Vec<(String, String)>,
        indexes: Vec<(String, String)>,
        fence: TableCatalogObjectMutationFence,
    ) -> Self {
        Self {
            disablement,
            metadata,
            indexes,
            fence,
        }
    }

    pub(crate) fn metadata_objects(&self) -> impl Iterator<Item = &str> {
        self.metadata.iter().map(|(key, _)| key.as_str())
    }

    pub(crate) fn ensure_held(&self) -> TableCatalogStoreResult<()> {
        self.fence.ensure_held()
    }

    pub(crate) fn mutation_fence(&self) -> TableCatalogObjectMutationFence {
        self.fence.clone()
    }
}

pub(in crate::table_catalog) fn cleanup_conflict(message: impl Into<String>) -> TableCatalogStoreError {
    TableCatalogStoreError::Conflict(message.into())
}

impl TableCatalogCleanupObject {
    pub(in crate::table_catalog) fn ensure_unversioned_catalog_record(&self) -> TableCatalogStoreResult<()> {
        if self.delete_marker || self.version_id.as_deref().is_some_and(|v| !v.is_empty() && v != "null") {
            return Err(cleanup_conflict("catalog contains versioned or deleted records"));
        }
        Ok(())
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(in crate::table_catalog) async fn commit_history_cleanup_etag(
        &self,
        bucket: &str,
        key: &str,
        idempotency: bool,
    ) -> TableCatalogStoreResult<String> {
        let value = self
            .backend
            .read_object_limited(self.catalog_bucket(), key, TABLE_METADATA_JSON_MAX_SIZE)
            .await?
            .ok_or_else(|| cleanup_conflict("commit history changed during cleanup"))?;
        let record: CommitLogEntry = serde_json::from_slice(&value.data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid commit history: {err}")))?;
        if record.table_id.is_empty() || record.commit_id.is_empty() {
            return Err(cleanup_conflict("commit history has no table or commit identity"));
        }
        if idempotency {
            validate_commit_idempotency_entry_object(&self.paths, key, bucket, &record.table_id, &record)?;
        } else {
            validate_commit_log_entry_object(&self.paths, key, bucket, &record.table_id, &record)?;
        }
        value.etag.ok_or_else(|| cleanup_conflict("commit history has no ETag"))
    }

    pub(super) async fn prepare_dropped_table_commit_cleanup(
        &self,
        entry: &TableEntry,
    ) -> TableCatalogStoreResult<Vec<(String, String)>> {
        let bucket = &entry.table_bucket;
        let mut records = Vec::new();
        for (prefix, idempotency) in [
            (self.paths.commit_log_entries_prefix(bucket, &entry.table_id), false),
            (self.paths.commit_idempotency_entries_prefix(bucket, &entry.table_id), true),
        ] {
            for object in self.backend.list_objects_for_cleanup(self.catalog_bucket(), &prefix).await? {
                object.ensure_unversioned_catalog_record()?;
                if !object.key.starts_with(&prefix) {
                    return Err(cleanup_conflict("commit history listing returned a different table"));
                }
                let etag = self.commit_history_cleanup_etag(bucket, &object.key, idempotency).await?;
                records.push((object.key, etag));
            }
        }
        Ok(records)
    }

    pub(in crate::table_catalog) async fn begin_table_bucket_disablement(
        &self,
        mut entry: TableBucketEntry,
        publication: Option<&dyn TableCommitPublication>,
    ) -> TableCatalogStoreResult<TableBucketDisablement> {
        validate_table_bucket_entry(&entry)?;
        let mut guards = Vec::new();
        if publication.is_some_and(|publication| publication.catalog_migration_read_permit_status() == Some(true)) {
            self.reject_legacy_catalog_global_state().await?;
            self.ensure_object_backed_catalog_write_permit_after_lock(&entry.table_bucket)
                .await?;
        } else {
            guards.push(self.acquire_table_bucket_registry_write_permit().await?);
            guards.push(self.acquire_object_backed_catalog_write_permit(&entry.table_bucket).await?);
        }
        let object = self.paths.table_bucket_entry_path(&entry.table_bucket);
        guards.push(self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?);
        if let Some((current, _)) = self.read_table_bucket_with_etag_unlocked(&entry.table_bucket).await? {
            if current.active_rename_id.is_some()
                || !matches!(current.state, TableCatalogEntryState::Active | TableCatalogEntryState::Deleted)
            {
                return Err(cleanup_conflict("table bucket has an operation in progress"));
            }
            entry = current;
        }
        Ok(TableBucketDisablement { entry, guards })
    }

    pub(super) async fn finish_table_bucket_disablement(
        &self,
        disablement: &mut TableBucketDisablement,
    ) -> TableCatalogStoreResult<()> {
        let mut entry = disablement.entry.clone();
        entry.state = TableCatalogEntryState::Deleted;
        entry.updated_at = Some(next_table_catalog_update_time(entry.updated_at.as_deref()));
        self.write_entry_unlocked(
            self.catalog_bucket(),
            &self.paths.table_bucket_entry_path(&entry.table_bucket),
            &entry,
            TableCatalogPutPrecondition::Any,
            disablement
                .guards
                .iter()
                .flat_map(TableCatalogLockGuard::write_commit_guards)
                .collect(),
        )
        .await
    }

    async fn execute_empty_table_bucket_cleanup(
        &self,
        cleanup: &mut EmptyTableBucketCleanup,
        incarnation: Uuid,
    ) -> TableCatalogStoreResult<()> {
        let bucket = &cleanup.disablement.entry.table_bucket;
        for (key, etag) in &cleanup.metadata {
            cleanup.ensure_held()?;
            self.backend
                .delete_object_if_match_fenced_for_incarnation(bucket, key, etag, incarnation, &cleanup.fence)
                .await?;
        }
        // A retry inventories the remaining metadata. Keep warehouse protection
        // until all user objects and uploads have been checked again.
        if !self.backend.list_objects_for_cleanup(bucket, "").await?.is_empty() {
            return Err(cleanup_conflict("table bucket changed during cleanup"));
        }
        self.backend.ensure_no_multipart_uploads(bucket).await?;
        for (key, etag) in &cleanup.indexes {
            cleanup.ensure_held()?;
            self.backend
                .delete_object_if_match_fenced(self.catalog_bucket(), key, etag, &cleanup.fence)
                .await?;
        }
        cleanup.ensure_held()?;
        self.finish_table_bucket_disablement(&mut cleanup.disablement).await
    }
}

impl<B: TableCatalogObjectBackend> ConfiguredTableCatalogStore<B> {
    pub(crate) async fn execute_empty_table_bucket_cleanup(
        &self,
        cleanup: &mut EmptyTableBucketCleanup,
        incarnation: Uuid,
    ) -> TableCatalogStoreResult<()> {
        let Self::ObjectBacked(store) = self;
        store.execute_empty_table_bucket_cleanup(cleanup, incarnation).await
    }
}
