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

use super::object::{
    next_table_catalog_update_time, table_warehouse_index_state_ready, validate_table_warehouse_index_entry_object,
};
use super::*;

pub(super) struct TableBucketDisablement {
    pub(super) entry: TableBucketEntry,
    guards: Vec<TableCatalogLockGuard>,
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

fn cleanup_conflict(message: impl Into<String>) -> TableCatalogStoreError {
    TableCatalogStoreError::Conflict(message.into())
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(super) async fn begin_table_bucket_disablement(
        &self,
        mut entry: TableBucketEntry,
        publication: Option<&dyn TableCommitPublication>,
    ) -> TableCatalogStoreResult<TableBucketDisablement> {
        validate_table_bucket_entry(&entry)?;
        let mut guards = Vec::new();
        if publication.is_some_and(|publication| publication.catalog_migration_read_permit_status() == Some(true)) {
            if self
                .backend
                .read_object(self.catalog_bucket(), &self.paths.backing_migration_global_fence_path())
                .await?
                .is_some()
            {
                return Err(cleanup_conflict("table bucket registry migration is in progress"));
            }
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

    async fn prepare_empty_table_bucket_cleanup(
        &self,
        entry: TableBucketEntry,
        publication: &impl TableCatalogObjectBackend,
    ) -> TableCatalogStoreResult<EmptyTableBucketCleanup> {
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(cleanup_conflict("empty table bucket cleanup requires a publication fence"));
        }
        let disablement = self.begin_table_bucket_disablement(entry, Some(publication)).await?;
        let bucket = &disablement.entry.table_bucket;
        let mut fence = publication
            .object_mutation_fence()
            .ok_or_else(|| cleanup_conflict("cleanup requires storage fencing"))?;
        fence
            .signals
            .extend(disablement.guards.iter().filter_map(TableCatalogLockGuard::lock_lost_signal));
        fence.ensure_held()?;

        let catalog_objects = self
            .backend
            .list_objects_for_cleanup(self.catalog_bucket(), &self.paths.table_bucket_root_prefix(bucket))
            .await?;
        let bucket_path = self.paths.table_bucket_entry_path(bucket);
        let index_state_path = self.paths.warehouse_index_state_path(bucket);
        let index_prefix = self.paths.warehouse_index_entries_prefix(bucket);
        let mut indexes = Vec::new();
        for object in catalog_objects {
            if object.delete_marker
                || object
                    .version_id
                    .as_deref()
                    .is_some_and(|version| !version.is_empty() && version != "null")
            {
                return Err(cleanup_conflict("catalog contains versioned or deleted records"));
            }
            if object.key == bucket_path {
                continue;
            }
            let etag = if object.key == index_state_path {
                let (state, etag) = self
                    .read_entry::<TableWarehouseIndexStateEntry>(self.catalog_bucket(), &object.key)
                    .await?
                    .ok_or_else(|| cleanup_conflict("warehouse index state changed during cleanup"))?;
                table_warehouse_index_state_ready(&state, bucket)?;
                if !matches!(state.state, TableCatalogEntryState::Active | TableCatalogEntryState::Deleted) {
                    return Err(cleanup_conflict("warehouse index has an operation in progress"));
                }
                etag
            } else if object.key.starts_with(&index_prefix) {
                let (index, etag) = self
                    .read_entry::<TableWarehouseIndexEntry>(self.catalog_bucket(), &object.key)
                    .await?
                    .ok_or_else(|| cleanup_conflict("warehouse index changed during cleanup"))?;
                validate_table_warehouse_index_entry_object(&self.paths, &object.key, &index)?;
                if index.table_bucket != *bucket || index.state != TableCatalogEntryState::Deleted {
                    return Err(cleanup_conflict("table bucket contains an active warehouse index"));
                }
                Namespace::parse(&index.namespace).map_err(|err| cleanup_conflict(err.to_string()))?;
                IdentifierSegment::parse(index.table.clone()).map_err(|err| cleanup_conflict(err.to_string()))?;
                if Uuid::parse_str(&index.table_id).ok().filter(|id| !id.is_nil()).is_none() {
                    return Err(cleanup_conflict("warehouse index table identity is invalid"));
                }
                let expected = format!("tables/{}/", index.table_id);
                if index.warehouse_object_prefix != expected {
                    return Err(cleanup_conflict("cleanup requires the default table warehouse location"));
                }
                etag
            } else {
                return Err(cleanup_conflict("table bucket contains namespaces or retained catalog resources"));
            };
            let etag = etag.ok_or_else(|| cleanup_conflict("catalog cleanup object has no etag"))?;
            indexes.push((object.key, etag));
        }
        self.backend.ensure_no_multipart_uploads(bucket).await?;
        let mut metadata = Vec::new();
        for object in self.backend.list_objects_for_cleanup(bucket, "").await? {
            if object.delete_marker
                || object
                    .version_id
                    .as_deref()
                    .is_some_and(|version| !version.is_empty() && version != "null")
            {
                return Err(cleanup_conflict("table bucket contains object versions or delete markers"));
            }
            let value = publication
                .read_object_limited(bucket, &object.key, TABLE_METADATA_JSON_MAX_SIZE)
                .await?
                .ok_or_else(|| cleanup_conflict("metadata object changed during cleanup"))?;
            crate::table_catalog::cleanup_metadata::validate_empty_table_cleanup_metadata(bucket, &object.key, &value.data)?;
            let etag = value
                .etag
                .ok_or_else(|| cleanup_conflict("metadata cleanup object has no etag"))?;
            metadata.push((object.key, etag));
        }
        fence.ensure_held()?;
        Ok(EmptyTableBucketCleanup {
            disablement,
            metadata,
            indexes,
            fence,
        })
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
    pub(crate) async fn prepare_empty_table_bucket_cleanup(
        &self,
        entry: TableBucketEntry,
        publication: &impl TableCatalogObjectBackend,
    ) -> TableCatalogStoreResult<EmptyTableBucketCleanup> {
        match self {
            Self::ObjectBacked(store) => store.prepare_empty_table_bucket_cleanup(entry, publication).await,
            Self::DurableStrong(_) => Err(TableCatalogStoreError::Unsupported(
                "empty table bucket cleanup requires object-backed catalog storage".to_string(),
            )),
        }
    }

    pub(crate) async fn execute_empty_table_bucket_cleanup(
        &self,
        cleanup: &mut EmptyTableBucketCleanup,
        incarnation: Uuid,
    ) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.execute_empty_table_bucket_cleanup(cleanup, incarnation).await,
            Self::DurableStrong(_) => Err(TableCatalogStoreError::Unsupported(
                "empty table bucket cleanup requires object-backed catalog storage".to_string(),
            )),
        }
    }
}
