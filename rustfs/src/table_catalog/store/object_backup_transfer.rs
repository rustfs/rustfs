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

use super::*;

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    /// Returns the saved v1 artifact bytes without reencoding or applying them.
    ///
    /// This does not export external manifest/data/delete files. Pending restore
    /// or legacy control state is rejected; cancellation leaves catalog state unchanged.
    pub(crate) async fn export_object_catalog_backup(
        &self,
        table_bucket: &str,
        backup_id: &str,
    ) -> TableCatalogStoreResult<Vec<u8>> {
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let fence = Self::object_backup_mutation_fence(&guards);
        fence.ensure_held()?;
        self.ensure_no_active_restore(table_bucket).await?;
        let path = self.paths.catalog_backup_path(table_bucket, backup_id);
        let artifact = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup {backup_id}")))?;
        let backup = self.decode_object_catalog_backup(table_bucket, &artifact.data)?;
        if backup.backup_id != backup_id {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity does not match its request".to_string(),
            ));
        }
        let _validated_tables = self.validate_catalog_backup_metadata_identities(table_bucket, &backup.objects)?;
        fence.ensure_held()?;
        Ok(artifact.data)
    }

    /// Persists one validated v1 artifact without publishing catalog or metadata objects.
    ///
    /// Only the artifact key is conditionally created. Exact retries keep its bytes,
    /// ETag and original creation timestamp; conflicting content is never replaced.
    /// A response lost after storage can be retried to confirm the saved artifact.
    /// Referenced objects must be prepared and validated separately before restore.
    pub(crate) async fn import_object_catalog_backup(
        &self,
        table_bucket: &str,
        data: Vec<u8>,
    ) -> TableCatalogStoreResult<TableCatalogBackupReport> {
        let backup = self.decode_object_catalog_backup(table_bucket, &data)?;
        let _validated_tables = self.validate_catalog_backup_metadata_identities(table_bucket, &backup.objects)?;
        let path = self.paths.catalog_backup_path(table_bucket, &backup.backup_id);
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let fence = Self::object_backup_mutation_fence(&guards);
        fence.ensure_held()?;
        self.ensure_no_active_restore(table_bucket).await?;

        let existing = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .await?;
        let status = if let Some(existing) = existing {
            if existing.data != data {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog backup identity is already occupied by different bytes".to_string(),
                ));
            }
            TableCatalogBackupStatus::AlreadyPresent
        } else {
            let result = self
                .backend
                .put_object_fenced(self.catalog_bucket(), &path, data.clone(), TableCatalogPutPrecondition::IfAbsent, &fence)
                .await;
            fence.ensure_held()?;
            let saved = self
                .backend
                .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
                .await?;
            match saved {
                Some(saved) if saved.data == data => {
                    if result.is_ok() {
                        TableCatalogBackupStatus::Created
                    } else {
                        // The read proves an ambiguous write or identical concurrent
                        // import completed; it does not authorize another put.
                        TableCatalogBackupStatus::AlreadyPresent
                    }
                }
                Some(_) => {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup identity is already occupied by different bytes".to_string(),
                    ));
                }
                None => {
                    result?;
                    return Err(TableCatalogStoreError::Unavailable("catalog backup disappeared after import".to_string()));
                }
            }
        };
        fence.ensure_held()?;
        Ok(TableCatalogBackupReport {
            table_bucket: table_bucket.to_string(),
            backup_id: backup.backup_id,
            status,
            backup_path: path,
            source_snapshot_etag: backup.source_catalog_etag.clone(),
            source_snapshot_version: OBJECT_CATALOG_BACKUP_VERSION,
            catalog_fingerprint: backup.source_catalog_etag,
            object_count: backup.objects.len(),
            verified_object_count: backup.objects.len(),
            verified_bytes: backup.objects.iter().map(|object| object.size_bytes).sum(),
            maintenance_object_count: 0,
            created_at: backup.created_at,
        })
    }
}
