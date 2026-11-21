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

#[cfg(test)]
use super::super::backup_types::TableCatalogBackupObjectPreparationStatus;
use super::*;

pub(in crate::table_catalog) enum BackupObjectPreparationRestoreGate {
    Unblocked { version: Option<String> },
    Pending { controls: [(String, String); 2] },
}

fn preparation_pending_phases_match(state: ObjectCatalogRestoreStateKind, intent: ObjectCatalogRestoreStateKind) -> bool {
    matches!(
        (state, intent),
        (ObjectCatalogRestoreStateKind::Prepared, ObjectCatalogRestoreStateKind::Prepared)
            | (ObjectCatalogRestoreStateKind::Applying, ObjectCatalogRestoreStateKind::Prepared)
            | (ObjectCatalogRestoreStateKind::Applying, ObjectCatalogRestoreStateKind::CatalogApplied)
            | (
                ObjectCatalogRestoreStateKind::CatalogApplied,
                ObjectCatalogRestoreStateKind::CatalogApplied
            )
            | (ObjectCatalogRestoreStateKind::CatalogApplied, ObjectCatalogRestoreStateKind::Completed)
    )
}

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub(in crate::table_catalog) async fn backup_object_preparation_restore_gate(
        &self,
        table_bucket: &str,
        backup: &ObjectCatalogBackup,
    ) -> TableCatalogStoreResult<BackupObjectPreparationRestoreGate> {
        self.reject_legacy_catalog_control_state(table_bucket).await?;
        let state = self.read_object_catalog_restore_state(table_bucket).await?;
        let Some((state, state_etag)) = state else {
            self.ensure_no_active_restore(table_bucket).await?;
            return Ok(BackupObjectPreparationRestoreGate::Unblocked { version: None });
        };
        if state.state == ObjectCatalogRestoreStateKind::Completed {
            self.ensure_no_active_restore(table_bucket).await?;
            return Ok(BackupObjectPreparationRestoreGate::Unblocked {
                version: Some(state_etag),
            });
        }
        if state.backup_id != backup.backup_id {
            return Err(TableCatalogStoreError::Unavailable(
                "backup object preparation cannot cross another backup's pending restore".to_string(),
            ));
        }
        let (intent, intent_etag) = self
            .read_object_catalog_restore_intent(table_bucket, &state.restore_id)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Unavailable("pending restore is missing its durable intent".to_string()))?;
        if state.restore_epoch != intent.restore_epoch
            || state.restore_id != intent.restore_id
            || state.backup_id != intent.backup_id
            || state.request_fingerprint != intent.request_fingerprint
            || state.completed_intent_etag.is_some()
            || state.backup_etag.is_some()
            || !preparation_pending_phases_match(state.state, intent.state)
        {
            return Err(TableCatalogStoreError::Unavailable(
                "pending restore does not authorize backup object preparation for this request".to_string(),
            ));
        }
        self.validate_restore_plan(backup, &intent)?;
        Ok(BackupObjectPreparationRestoreGate::Pending {
            controls: [
                (self.paths.catalog_restore_state_path(table_bucket), state_etag),
                (self.paths.catalog_restore_intent_path(table_bucket, &state.restore_id), intent_etag),
            ],
        })
    }

    pub(in crate::table_catalog) async fn finish_backup_object_preparation_gate(
        &self,
        table_bucket: &str,
        gate: &BackupObjectPreparationRestoreGate,
    ) -> TableCatalogStoreResult<()> {
        match gate {
            BackupObjectPreparationRestoreGate::Unblocked { version } => {
                self.finish_catalog_restore_read(table_bucket, version.as_deref()).await
            }
            BackupObjectPreparationRestoreGate::Pending { controls } => {
                for (object, expected) in controls {
                    let actual = self.backend.object_metadata(self.catalog_bucket(), object).await?;
                    if actual.as_ref().and_then(|actual| actual.etag.as_deref()) != Some(expected.as_str()) {
                        return Err(TableCatalogStoreError::Unavailable(
                            "pending restore controls changed during backup object preparation".to_string(),
                        ));
                    }
                }
                Ok(())
            }
        }
    }

    pub(in crate::table_catalog) async fn read_stable_preparation_object(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let Some(before) = self.backend.object_metadata(table_bucket, object).await? else {
            return Ok(None);
        };
        let max_size = u64::try_from(OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE)
            .map_err(|_| TableCatalogStoreError::Internal("backup preparation size limit is invalid".to_string()))?;
        if before.size > max_size || before.etag.as_deref().is_none_or(str::is_empty) {
            return Err(TableCatalogStoreError::Conflict(
                "existing preparation object cannot be safely compared within the size and ETag limits".to_string(),
            ));
        }
        let stored = self
            .backend
            .read_object_limited(table_bucket, object, OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Conflict("preparation object disappeared during its read".to_string()))?;
        let after = self
            .backend
            .object_metadata(table_bucket, object)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Conflict("preparation object disappeared after its read".to_string()))?;
        if stored.etag != before.etag
            || after.etag != before.etag
            || after.size != before.size
            || u64::try_from(stored.data.len()).ok() != Some(before.size)
        {
            return Err(TableCatalogStoreError::Conflict("preparation object changed during its read".to_string()));
        }
        Ok(Some(stored))
    }
}

#[cfg(test)]
#[path = "object_backup_prepare_tests.rs"]
mod tests;
