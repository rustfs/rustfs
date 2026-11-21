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

use crate::table_catalog::store::cleanup::{EmptyTableBucketCleanup, cleanup_conflict};
use crate::table_catalog::store::object::{
    table_warehouse_index_state_ready, validate_table_maintenance_report_path, validate_table_warehouse_index_entry_object,
};
use crate::table_catalog::*;

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
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
        disablement.extend_mutation_fence(&mut fence);
        fence.ensure_held()?;

        let catalog_objects = self
            .backend
            .list_objects_for_cleanup(self.catalog_bucket(), &self.paths.table_bucket_root_prefix(bucket))
            .await?;
        let bucket_path = self.paths.table_bucket_entry_path(bucket);
        let index_state_path = self.paths.warehouse_index_state_path(bucket);
        let index_prefix = self.paths.warehouse_index_entries_prefix(bucket);
        let directory_prefix = self.paths.table_directory_prefix(bucket);
        let catalog_root = self.paths.table_bucket_root_prefix(bucket);
        let commits = format!("{catalog_root}{COMMIT_LOG_ROOT}/");
        let receipts = format!("{catalog_root}{COMMIT_IDEMPOTENCY_ROOT}/");
        let mut indexes = Vec::new();
        for object in catalog_objects {
            object.ensure_unversioned_catalog_record()?;
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
            } else if object.key.starts_with(&directory_prefix) {
                // Registration/rename share the held bucket publication fence.
                // Remove only the exact immutable candidate of a vanished entry.
                let (target, etag) = self.directory_candidate_target(bucket, &object.key).await?;
                if self.backend.object_exists(self.catalog_bucket(), &target).await? {
                    return Err(cleanup_conflict("table directory candidate references a retained table entry"));
                }
                etag
            } else if object.key.starts_with(&commits) || object.key.starts_with(&receipts) {
                Some(
                    self.commit_history_cleanup_etag(bucket, &object.key, object.key.starts_with(&receipts))
                        .await?,
                )
            } else if let Some((_, suffix)) = self.paths.historical_table_maintenance_path(bucket, &object.key) {
                // The full inventory rejects every retained table entry before any deletion.
                let value = self
                    .backend
                    .read_object_limited(self.catalog_bucket(), &object.key, TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_OBJECT_SIZE)
                    .await?
                    .ok_or_else(|| cleanup_conflict("table maintenance record changed during cleanup"))?;
                if suffix == MAINTENANCE_CONFIG_FILE {
                    let config = serde_json::from_slice(&value.data)
                        .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid table maintenance config: {err}")))?;
                    validate_table_maintenance_config(&config)?;
                } else {
                    let report: TableMetadataMaintenanceReport = serde_json::from_slice(&value.data)
                        .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid table maintenance report: {err}")))?;
                    validate_table_maintenance_report_path(&self.paths, bucket, &object.key, suffix, &report)?;
                    if matches!(
                        report.job.status,
                        TableMetadataMaintenanceJobStatus::Queued | TableMetadataMaintenanceJobStatus::Running
                    ) {
                        return Err(cleanup_conflict("table bucket contains a queued or running maintenance job"));
                    }
                }
                value.etag
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
            super::cleanup_metadata::validate_empty_table_cleanup_metadata(bucket, &object.key, &value.data)?;
            let etag = value
                .etag
                .ok_or_else(|| cleanup_conflict("metadata cleanup object has no etag"))?;
            metadata.push((object.key, etag));
        }
        fence.ensure_held()?;
        Ok(EmptyTableBucketCleanup::from_validated_references(disablement, metadata, indexes, fence))
    }
}

impl<B: TableCatalogObjectBackend> ConfiguredTableCatalogStore<B> {
    pub(crate) async fn prepare_empty_table_bucket_cleanup(
        &self,
        entry: TableBucketEntry,
        publication: &impl TableCatalogObjectBackend,
    ) -> TableCatalogStoreResult<EmptyTableBucketCleanup> {
        let Self::ObjectBacked(store) = self;
        store.prepare_empty_table_bucket_cleanup(entry, publication).await
    }
}
