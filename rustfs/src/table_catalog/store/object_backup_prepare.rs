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

use super::super::backup_types::{TableCatalogBackupObjectPreparationReport, TableCatalogBackupObjectPreparationStatus};
use super::inventory::{
    backup_reference_metadata_log_locations, validate_backup_reference_table_metadata, validate_backup_reference_view_metadata,
};
use super::*;

const BACKUP_PREPARATION_MAX_HISTORY_SCOPES: usize = 1024;

enum BackupObjectPreparationRestoreGate {
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

fn preparation_graph_scope_matches(
    entry: &TableEntry,
    metadata: &serde_json::Value,
    reference: &ObjectCatalogBackupReference,
) -> TableCatalogStoreResult<bool> {
    let prefix = table_warehouse_object_prefix(entry)?;
    let expected_kind = match reference.kind {
        ObjectCatalogBackupReferenceKind::ManifestList => TableMetadataMaintenanceObjectKind::ManifestList,
        ObjectCatalogBackupReferenceKind::ManifestFile => TableMetadataMaintenanceObjectKind::ManifestFile,
        ObjectCatalogBackupReferenceKind::DataFile => TableMetadataMaintenanceObjectKind::DataFile,
        ObjectCatalogBackupReferenceKind::DeleteFile => TableMetadataMaintenanceObjectKind::DeleteFile,
        ObjectCatalogBackupReferenceKind::TableStatistics | ObjectCatalogBackupReferenceKind::PartitionStatistics => {
            for statistics in table_statistics_object_references(metadata)? {
                let kind_matches = matches!(
                    (reference.kind, statistics.kind),
                    (ObjectCatalogBackupReferenceKind::TableStatistics, IcebergStatisticsFileKind::Table)
                        | (
                            ObjectCatalogBackupReferenceKind::PartitionStatistics,
                            IcebergStatisticsFileKind::Partition
                        )
                );
                if kind_matches
                    && reference.object.starts_with(&prefix)
                    && table_catalog_object_key_from_location(&entry.table_bucket, &statistics.location).as_deref()
                        == Some(reference.object.as_str())
                {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        ObjectCatalogBackupReferenceKind::TableMetadata | ObjectCatalogBackupReferenceKind::ViewMetadata => return Ok(false),
    };
    Ok(table_maintenance_object_kind_for_entry(entry, Some(&prefix), &reference.object)
        .is_some_and(|actual| table_maintenance_object_kind_matches_reference(&actual, &expected_kind)))
}

fn validate_preparation_warehouse_owner(
    entry: &TableEntry,
    tables: &[(TableEntry, serde_json::Value)],
) -> TableCatalogStoreResult<()> {
    let prefix = table_warehouse_object_prefix(entry)?;
    for (other, _) in tables {
        if other.table_id != entry.table_id && warehouse_object_prefixes_overlap(&prefix, &table_warehouse_object_prefix(other)?)
        {
            return Err(TableCatalogStoreError::Conflict(
                "backup object preparation scope overlaps another active table".to_string(),
            ));
        }
    }
    Ok(())
}

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    async fn backup_object_preparation_restore_gate(
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

    async fn finish_backup_object_preparation_gate(
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

    async fn read_stable_preparation_object(
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

    async fn validate_backup_object_preparation_scope(
        &self,
        table_bucket: &str,
        backup: &ObjectCatalogBackup,
        references: &[ObjectCatalogBackupReference],
        reference: &ObjectCatalogBackupReference,
        data: &[u8],
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let views = self.backup_reference_view_entries(table_bucket, &backup.objects)?;
        let tables = self.validate_catalog_backup_metadata_identities(table_bucket, &backup.objects)?;
        for (entry, metadata) in &tables {
            let current = table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).ok_or_else(|| {
                TableCatalogStoreError::Invalid("backup table current metadata location is invalid".to_string())
            })?;
            validate_backup_reference_table_metadata(entry, &current, metadata, true)?;
            validate_preparation_warehouse_owner(entry, &tables)?;
            if reference.kind == ObjectCatalogBackupReferenceKind::TableMetadata {
                let historical = backup_reference_metadata_log_locations(metadata, entry)?;
                if reference.object == current || historical.contains(&reference.object) {
                    if reference.object == current {
                        let embedded = backup
                            .objects
                            .iter()
                            .find(|object| object.bucket == table_bucket && object.object == current)
                            .ok_or_else(|| {
                                TableCatalogStoreError::Invalid("backup is missing its current metadata payload".to_string())
                            })?;
                        if embedded.data.as_slice() != data {
                            return Err(TableCatalogStoreError::Invalid(
                                "current metadata preparation differs from the embedded backup payload".to_string(),
                            ));
                        }
                    }
                    let prepared = decode_table_metadata_json(&reference.object, data)?;
                    validate_backup_reference_table_metadata(entry, &reference.object, &prepared, reference.object == current)?;
                    let mut prepared_entry = entry.clone();
                    prepared_entry.warehouse_location = table_metadata_location(&prepared)?.to_string();
                    validate_preparation_warehouse_owner(&prepared_entry, &tables)?;
                    return Ok(());
                }
            } else if preparation_graph_scope_matches(entry, metadata, reference)? {
                return Ok(());
            }
        }
        if reference.kind == ObjectCatalogBackupReferenceKind::ViewMetadata {
            for entry in views {
                if table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).as_deref()
                    == Some(reference.object.as_str())
                {
                    let metadata = decode_table_metadata_json(&reference.object, data)?;
                    validate_backup_reference_view_metadata(
                        &entry,
                        &parse_namespace_for_store(&entry.namespace)?,
                        &parse_table_for_store(&entry.view)?,
                        &reference.object,
                        &metadata,
                    )?;
                    return Ok(());
                }
            }
        }
        if matches!(
            reference.kind,
            ObjectCatalogBackupReferenceKind::TableMetadata | ObjectCatalogBackupReferenceKind::ViewMetadata
        ) {
            return Err(TableCatalogStoreError::Invalid(
                "backup metadata preparation is outside an active entry's direct metadata scope".to_string(),
            ));
        }

        let mut inspected = 0usize;
        let mut read_bytes = 0u64;
        let mut missing_history = false;
        let max_read_bytes = u64::try_from(OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .map_err(|_| TableCatalogStoreError::Internal("backup preparation history budget is invalid".to_string()))?;
        for (entry, metadata) in &tables {
            let current = table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).ok_or_else(|| {
                TableCatalogStoreError::Invalid("backup table current metadata location is invalid".to_string())
            })?;
            for object in backup_reference_metadata_log_locations(metadata, entry)? {
                fence.ensure_held()?;
                if object == current {
                    continue;
                }
                inspected += 1;
                if inspected > BACKUP_PREPARATION_MAX_HISTORY_SCOPES {
                    return Err(TableCatalogStoreError::Unavailable(
                        "historical preparation scope lookup exceeds its 1024-object budget".to_string(),
                    ));
                }
                let saved = references
                    .binary_search_by(|candidate| {
                        candidate
                            .bucket
                            .as_str()
                            .cmp(table_bucket)
                            .then_with(|| candidate.object.cmp(&object))
                    })
                    .ok()
                    .and_then(|index| references.get(index))
                    .ok_or_else(|| {
                        TableCatalogStoreError::Invalid(
                            "direct history metadata is missing from the backup reference inventory".to_string(),
                        )
                    })?;
                if saved.kind != ObjectCatalogBackupReferenceKind::TableMetadata {
                    return Err(TableCatalogStoreError::Invalid(
                        "direct history has a conflicting backup reference kind".to_string(),
                    ));
                }
                read_bytes = read_bytes
                    .checked_add(saved.size_bytes)
                    .filter(|bytes| *bytes <= max_read_bytes)
                    .ok_or_else(|| {
                        TableCatalogStoreError::Unavailable(
                            "historical preparation scope lookup exceeds its byte budget".to_string(),
                        )
                    })?;
                let Some(stored) = self.read_stable_preparation_object(table_bucket, &object).await? else {
                    missing_history = true;
                    continue;
                };
                if u64::try_from(stored.data.len()).ok() != Some(saved.size_bytes)
                    || saved.sha256.as_deref() != Some(backup_sha256(&stored.data).as_str())
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "prepared historical metadata does not match its backup watermark".to_string(),
                    ));
                }
                let metadata = decode_table_metadata_json(&object, &stored.data)?;
                validate_backup_reference_table_metadata(entry, &object, &metadata, false)?;
                let mut historical_entry = entry.clone();
                historical_entry.warehouse_location = table_metadata_location(&metadata)?.to_string();
                validate_preparation_warehouse_owner(&historical_entry, &tables)?;
                if preparation_graph_scope_matches(&historical_entry, &metadata, reference)? {
                    return Ok(());
                }
            }
        }
        if missing_history {
            return Err(TableCatalogStoreError::Unavailable(
                "prepare the referenced direct history metadata before preparing an object outside current warehouse scope"
                    .to_string(),
            ));
        }
        Err(TableCatalogStoreError::Invalid(
            "backup object preparation is outside its active entries and prepared direct history scopes".to_string(),
        ))
    }

    /// Prepares one small object referenced by a saved v2 backup without publishing catalog state.
    ///
    /// Only an absent key is written. Exact-byte retries keep the target ETag; different existing bytes are never
    /// replaced. An ambiguous put is confirmed by a stable read of the original bytes. Cancellation may leave that
    /// immutable object prepared; retrying the same input confirms it. Pending restore controls are never changed.
    pub(crate) async fn prepare_object_catalog_backup_object(
        &self,
        table_bucket: &str,
        backup_id: &str,
        object: &str,
        data: Vec<u8>,
    ) -> TableCatalogStoreResult<TableCatalogBackupObjectPreparationReport> {
        if table_bucket == self.catalog_bucket() || !safe_object_key(object) || data.len() > OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE
        {
            return Err(TableCatalogStoreError::Invalid(
                "backup object preparation has an invalid bucket, key, or body size".to_string(),
            ));
        }
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let fence = Self::object_backup_mutation_fence(&guards);
        fence.ensure_held()?;
        let backup_path = self.paths.catalog_backup_path(table_bucket, backup_id);
        let artifact = self
            .backend
            .read_object_limited(self.catalog_bucket(), &backup_path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup {backup_id}")))?;
        let artifact_etag = artifact.etag.as_deref().filter(|etag| !etag.is_empty()).ok_or_else(|| {
            TableCatalogStoreError::Unavailable("catalog backup has no stable ETag for preparation".to_string())
        })?;
        let backup = self.decode_object_catalog_backup(table_bucket, &artifact.data)?;
        if backup.backup_id != backup_id {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity does not match its preparation request".to_string(),
            ));
        }
        let references = backup
            .references
            .as_deref()
            .filter(|_| backup.version == OBJECT_CATALOG_BACKUP_INVENTORY_VERSION)
            .ok_or_else(|| {
                TableCatalogStoreError::Unsupported("object preparation requires a v2 catalog backup inventory".to_string())
            })?;
        validate_object_catalog_backup_reference_shape(table_bucket, references)?;
        let index = references
            .binary_search_by(|reference| {
                reference
                    .bucket
                    .as_str()
                    .cmp(table_bucket)
                    .then_with(|| reference.object.as_str().cmp(object))
            })
            .map_err(|_| {
                TableCatalogStoreError::Invalid("object is not present in this catalog backup reference inventory".to_string())
            })?;
        let reference = &references[index];
        let sha256 = backup_sha256(&data);
        if u64::try_from(data.len()).ok() != Some(reference.size_bytes) || reference.sha256.as_deref() != Some(sha256.as_str()) {
            return Err(TableCatalogStoreError::Invalid(
                "prepared object body does not match its backup size and SHA-256 watermark".to_string(),
            ));
        }
        let gate = self.backup_object_preparation_restore_gate(table_bucket, &backup).await?;
        self.validate_backup_object_preparation_scope(table_bucket, &backup, references, reference, &data, &fence)
            .await?;
        match reference.kind {
            ObjectCatalogBackupReferenceKind::ManifestList => {
                decode_manifest_list_avro_async(data.clone()).await?;
            }
            ObjectCatalogBackupReferenceKind::ManifestFile => {
                decode_manifest_avro_async(data.clone()).await?;
            }
            _ => {}
        }
        fence.ensure_held()?;
        self.finish_backup_object_preparation_gate(table_bucket, &gate).await?;
        let (status, stored) = if let Some(stored) = self.read_stable_preparation_object(table_bucket, object).await? {
            if stored.data != data {
                return Err(TableCatalogStoreError::Conflict(
                    "backup preparation never replaces different existing object bytes".to_string(),
                ));
            }
            (TableCatalogBackupObjectPreparationStatus::AlreadyPresent, stored)
        } else {
            let result = self
                .backend
                .put_object_fenced(table_bucket, object, data.clone(), TableCatalogPutPrecondition::IfAbsent, &fence)
                .await;
            fence.ensure_held()?;
            let saved = self.read_stable_preparation_object(table_bucket, object).await?;
            match saved {
                Some(saved) if saved.data == data => (
                    if result.is_ok() {
                        TableCatalogBackupObjectPreparationStatus::Prepared
                    } else {
                        TableCatalogBackupObjectPreparationStatus::AlreadyPresent
                    },
                    saved,
                ),
                Some(_) => {
                    return Err(TableCatalogStoreError::Conflict(
                        "backup preparation key is occupied by different object bytes".to_string(),
                    ));
                }
                None => {
                    result?;
                    return Err(TableCatalogStoreError::Unavailable(
                        "prepared object disappeared after its conditional write".to_string(),
                    ));
                }
            }
        };
        fence.ensure_held()?;
        self.finish_backup_object_preparation_gate(table_bucket, &gate).await?;
        let final_artifact = self.backend.object_metadata(self.catalog_bucket(), &backup_path).await?;
        if final_artifact.as_ref().and_then(|metadata| metadata.etag.as_deref()) != Some(artifact_etag) {
            return Err(TableCatalogStoreError::Unavailable(
                "catalog backup changed during object preparation".to_string(),
            ));
        }
        fence.ensure_held()?;
        Ok(TableCatalogBackupObjectPreparationReport {
            table_bucket: table_bucket.to_string(),
            backup_id: backup_id.to_string(),
            object: object.to_string(),
            status,
            size_bytes: reference.size_bytes,
            sha256,
            etag: stored.etag,
        })
    }
}

#[cfg(test)]
#[path = "object_backup_prepare_tests.rs"]
mod tests;
