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

use super::backup_types::TableCatalogRestoreStatus;
use super::object::{
    ObjectTableCatalogStore, validate_namespace_entry_object, validate_table_bucket_entry_object, validate_table_entry_object,
    validate_view_entry_object,
};
use super::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[path = "object_backup_commit_recovery.rs"]
mod commit_recovery;
#[path = "object_backup_inventory.rs"]
mod inventory;
#[path = "object_backup_maintenance.rs"]
mod maintenance;
#[path = "object_backup_prepare.rs"]
mod prepare;

pub(in crate::table_catalog) use inventory::validate_object_catalog_backup_reference_shape;
pub(in crate::table_catalog) use inventory::{
    OBJECT_CATALOG_BACKUP_REFERENCE_HASH_MAX_SIZE, OBJECT_CATALOG_BACKUP_REFERENCE_MAX_OBJECTS, ObjectCatalogBackupReference,
    ObjectCatalogBackupReferenceKind,
};

const OBJECT_CATALOG_BACKUP_VERSION: u16 = 1;
pub(in crate::table_catalog) const OBJECT_CATALOG_BACKUP_INVENTORY_VERSION: u16 = 2;
pub(in crate::table_catalog) const OBJECT_CATALOG_BACKUP_MAX_SIZE: usize = 64 * 1024 * 1024;
const OBJECT_CATALOG_BACKUP_MAX_OBJECTS: usize = 1_000_000;
pub(in crate::table_catalog) const OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE: usize = 16 * 1024 * 1024;
pub(in crate::table_catalog) const OBJECT_CATALOG_RESTORE_STATE_VERSION: u16 = 2;
const OBJECT_CATALOG_RESTORE_STATE_MAX_SIZE: usize = 16 * 1024;
const OBJECT_CATALOG_RESTORE_INTENT_MAX_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::table_catalog) struct ObjectCatalogBackup {
    pub(in crate::table_catalog) version: u16,
    pub(in crate::table_catalog) backup_id: String,
    pub(in crate::table_catalog) table_bucket: String,
    pub(in crate::table_catalog) created_at: String,
    pub(in crate::table_catalog) source_catalog_etag: String,
    pub(in crate::table_catalog) objects: Vec<ObjectCatalogBackupObject>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_backup_references"
    )]
    pub(in crate::table_catalog) references: Option<Vec<ObjectCatalogBackupReference>>,
}

fn deserialize_backup_references<'de, D>(deserializer: D) -> Result<Option<Vec<ObjectCatalogBackupReference>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Missing is the historical v1 shape; an explicitly null inventory is invalid.
    Vec::<ObjectCatalogBackupReference>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::table_catalog) struct ObjectCatalogBackupObject {
    pub(in crate::table_catalog) bucket: String,
    pub(in crate::table_catalog) object: String,
    size_bytes: u64,
    etag: Option<String>,
    sha256: String,
    pub(in crate::table_catalog) data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(in crate::table_catalog) enum ObjectCatalogRestoreStateKind {
    Prepared,
    Applying,
    CatalogApplied,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::table_catalog) struct ObjectCatalogRestoreWrite {
    pub(in crate::table_catalog) bucket: String,
    pub(in crate::table_catalog) object: String,
    pub(in crate::table_catalog) expected_etag: Option<String>,
    pub(in crate::table_catalog) already_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::table_catalog) struct ObjectCatalogRestoreDelete {
    pub(in crate::table_catalog) object: String,
    pub(in crate::table_catalog) expected_etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::table_catalog) struct ObjectCatalogRestoreState {
    pub(in crate::table_catalog) version: u16,
    pub(in crate::table_catalog) table_bucket: String,
    pub(in crate::table_catalog) restore_epoch: String,
    pub(in crate::table_catalog) restore_id: String,
    pub(in crate::table_catalog) backup_id: String,
    pub(in crate::table_catalog) request_fingerprint: String,
    pub(in crate::table_catalog) state: ObjectCatalogRestoreStateKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::table_catalog) completed_intent_etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::table_catalog) backup_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::table_catalog) struct ObjectCatalogRestoreIntent {
    pub(in crate::table_catalog) version: u16,
    pub(in crate::table_catalog) table_bucket: String,
    pub(in crate::table_catalog) restore_epoch: String,
    pub(in crate::table_catalog) restore_id: String,
    pub(in crate::table_catalog) backup_id: String,
    pub(in crate::table_catalog) request_fingerprint: String,
    pub(in crate::table_catalog) expected_catalog_etag: Option<String>,
    pub(in crate::table_catalog) allow_replace: bool,
    pub(in crate::table_catalog) source_catalog_etag: String,
    pub(in crate::table_catalog) state: ObjectCatalogRestoreStateKind,
    pub(in crate::table_catalog) previous_snapshot_etag: Option<String>,
    pub(in crate::table_catalog) restored_snapshot_etag: String,
    pub(in crate::table_catalog) object_count: usize,
    pub(in crate::table_catalog) maintenance_object_count: usize,
    pub(in crate::table_catalog) writes: Vec<ObjectCatalogRestoreWrite>,
    pub(in crate::table_catalog) deletes: Vec<ObjectCatalogRestoreDelete>,
    pub(in crate::table_catalog) created_at: String,
    pub(in crate::table_catalog) updated_at: String,
}

pub(in crate::table_catalog) fn backup_sha256(data: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(data), hex_simd::AsciiCase::Lower)
}

pub(in crate::table_catalog) fn safe_object_key(object: &str) -> bool {
    !object.is_empty()
        && !object.starts_with('/')
        && !object.contains("..")
        && !object.contains('\\')
        && !object.bytes().any(|byte| byte.is_ascii_control())
}

pub(in crate::table_catalog) fn catalog_backup_fingerprint(
    objects: &[ObjectCatalogBackupObject],
) -> TableCatalogStoreResult<String> {
    let identity = serde_json::to_vec(
        &objects
            .iter()
            .map(|object| (&object.bucket, &object.object, &object.sha256))
            .collect::<Vec<_>>(),
    )
    .map_err(|error| TableCatalogStoreError::Internal(format!("failed to encode catalog backup identity: {error}")))?;
    Ok(backup_sha256(&identity))
}

fn catalog_backup_identity(
    table_bucket: &str,
    fingerprint: &str,
    objects: &[ObjectCatalogBackupObject],
) -> TableCatalogStoreResult<String> {
    let data = serde_json::to_vec(&(OBJECT_CATALOG_BACKUP_VERSION, table_bucket, fingerprint, objects))
        .map_err(|error| TableCatalogStoreError::Internal(format!("failed to encode catalog backup: {error}")))?;
    Ok(backup_sha256(&data))
}

pub(in crate::table_catalog) fn catalog_backup_inventory_identity(
    table_bucket: &str,
    fingerprint: &str,
    objects: &[ObjectCatalogBackupObject],
    references: &[ObjectCatalogBackupReference],
) -> TableCatalogStoreResult<String> {
    let data = serde_json::to_vec(&(OBJECT_CATALOG_BACKUP_INVENTORY_VERSION, table_bucket, fingerprint, objects, references))
        .map_err(|error| TableCatalogStoreError::Internal(format!("failed to encode catalog backup inventory: {error}")))?;
    Ok(backup_sha256(&data))
}

pub(in crate::table_catalog) fn catalog_backup_verification_totals(
    backup: &ObjectCatalogBackup,
) -> TableCatalogStoreResult<(usize, u64)> {
    let add_size = |total: u64, size: u64| {
        total
            .checked_add(size)
            .ok_or_else(|| TableCatalogStoreError::Invalid("catalog backup byte total overflows u64".to_string()))
    };
    let (count, bytes) = if let Some(references) = &backup.references {
        (
            references.len(),
            references
                .iter()
                .map(|reference| reference.size_bytes)
                .try_fold(0u64, add_size),
        )
    } else {
        (
            backup.objects.len(),
            backup.objects.iter().map(|object| object.size_bytes).try_fold(0u64, add_size),
        )
    };
    Ok((count, bytes?))
}

pub(in crate::table_catalog) fn append_backup_object(
    objects: &mut Vec<ObjectCatalogBackupObject>,
    encoded_bytes: &mut usize,
    object: ObjectCatalogBackupObject,
) -> TableCatalogStoreResult<()> {
    if objects.len() == OBJECT_CATALOG_BACKUP_MAX_OBJECTS {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup object count exceeds its limit".to_string(),
        ));
    }
    let size = serde_json::to_vec(&object)
        .map_err(|error| TableCatalogStoreError::Internal(format!("failed to measure catalog backup payload: {error}")))?
        .len();
    let total = encoded_bytes
        .checked_add(size)
        .and_then(|total| total.checked_add(1))
        .filter(|total| *total <= OBJECT_CATALOG_BACKUP_MAX_SIZE)
        .ok_or_else(|| TableCatalogStoreError::Invalid("catalog backup payload exceeds its encoded byte budget".to_string()))?;
    *encoded_bytes = total;
    objects.push(object);
    Ok(())
}

pub(in crate::table_catalog) fn restore_request_fingerprint(
    table_bucket: &str,
    restore_id: &str,
    backup_id: &str,
    expected_catalog_etag: Option<&str>,
    allow_replace: bool,
) -> TableCatalogStoreResult<String> {
    let encoded = serde_json::to_vec(&(table_bucket, restore_id, backup_id, expected_catalog_etag, allow_replace))
        .map_err(|error| TableCatalogStoreError::Internal(format!("failed to encode catalog restore request: {error}")))?;
    Ok(backup_sha256(&encoded))
}

#[cfg(test)]
#[path = "object_backup_version_tests.rs"]
mod version_tests;

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub(in crate::table_catalog) fn decode_object_catalog_backup(
        &self,
        table_bucket: &str,
        data: &[u8],
    ) -> TableCatalogStoreResult<ObjectCatalogBackup> {
        if data.len() > OBJECT_CATALOG_BACKUP_MAX_SIZE {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup exceeds the encoded size limit".to_string(),
            ));
        }
        let backup: ObjectCatalogBackup = serde_json::from_slice(data).map_err(|error| {
            TableCatalogStoreError::Invalid(format!("invalid catalog backup at line {} column {}", error.line(), error.column()))
        })?;
        if !matches!(backup.version, OBJECT_CATALOG_BACKUP_VERSION | OBJECT_CATALOG_BACKUP_INVENTORY_VERSION)
            || backup.table_bucket != table_bucket
            || (backup.version == OBJECT_CATALOG_BACKUP_VERSION) != backup.references.is_none()
        {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity or version does not match its request".to_string(),
            ));
        }
        self.validate_object_catalog_backup_objects(table_bucket, &backup.objects)?;
        let expected_identity = if let Some(references) = &backup.references {
            validate_object_catalog_backup_reference_shape(table_bucket, references)?;
            for object in backup.objects.iter().filter(|object| object.bucket == table_bucket) {
                let reference = references
                    .binary_search_by(|reference| reference.object.cmp(&object.object))
                    .ok()
                    .map(|index| &references[index])
                    .ok_or_else(|| {
                        TableCatalogStoreError::Invalid(
                            "catalog backup current metadata is missing from its inventory".to_string(),
                        )
                    })?;
                if reference.kind != ObjectCatalogBackupReferenceKind::TableMetadata
                    || reference.size_bytes != object.size_bytes
                    || reference.sha256.as_deref() != Some(object.sha256.as_str())
                {
                    return Err(TableCatalogStoreError::Invalid(
                        "catalog backup embedded metadata does not match its inventory".to_string(),
                    ));
                }
            }
            catalog_backup_inventory_identity(table_bucket, &backup.source_catalog_etag, &backup.objects, references)?
        } else {
            catalog_backup_identity(table_bucket, &backup.source_catalog_etag, &backup.objects)?
        };
        // Reject malformed declared sizes before raw import can persist a candidate.
        catalog_backup_verification_totals(&backup)?;
        if catalog_backup_fingerprint(&backup.objects)? != backup.source_catalog_etag || expected_identity != backup.backup_id {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup content does not match its identity".to_string(),
            ));
        }
        Ok(backup)
    }

    pub(in crate::table_catalog) fn is_catalog_directory_sidecar(&self, table_bucket: &str, object: &str) -> bool {
        object == self.paths.backing_migration_fence_path(table_bucket)
            || object.starts_with(&self.paths.table_directory_prefix(table_bucket))
    }

    pub(in crate::table_catalog) fn validate_object_catalog_backup_objects(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<()> {
        if objects.is_empty()
            || objects
                .windows(2)
                .any(|pair| (&pair[0].bucket, &pair[0].object) >= (&pair[1].bucket, &pair[1].object))
        {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup objects must be sorted, unique, and nonempty".to_string(),
            ));
        }
        let prefix = self.paths.table_bucket_root_prefix(table_bucket);
        let bucket_path = self.paths.table_bucket_entry_path(table_bucket);
        let mut has_bucket = false;
        let mut metadata_paths = BTreeSet::new();
        // Validate every byte and destination before any restore mutation. A late
        // corrupt object must not leave earlier catalog objects installed.
        for object in objects {
            if !safe_object_key(&object.object)
                || object.data.len() > OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE
                || u64::try_from(object.data.len()).ok() != Some(object.size_bytes)
                || backup_sha256(&object.data) != object.sha256
            {
                return Err(TableCatalogStoreError::Invalid("catalog backup object verification failed".to_string()));
            }
            if object.bucket != self.catalog_bucket() {
                if object.bucket != table_bucket {
                    return Err(TableCatalogStoreError::Invalid(
                        "catalog backup object belongs to another bucket".to_string(),
                    ));
                }
                continue;
            }
            if !object.object.starts_with(&prefix) || self.is_catalog_directory_sidecar(table_bucket, &object.object) {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog backup object is outside its catalog prefix".to_string(),
                ));
            }
            if object.object == bucket_path {
                let entry: TableBucketEntry = serde_json::from_slice(&object.data)
                    .map_err(|_| TableCatalogStoreError::Invalid("invalid table bucket in catalog backup".to_string()))?;
                validate_table_bucket_entry_object(&self.paths, &object.object, &entry)?;
                if entry.state != TableCatalogEntryState::Active || entry.active_rename_id.is_some() {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup requires a stable active table bucket".to_string(),
                    ));
                }
                has_bucket = true;
            } else if object.object.ends_with(NAMESPACE_ENTRY_FILE) {
                let entry: NamespaceEntry = serde_json::from_slice(&object.data)
                    .map_err(|_| TableCatalogStoreError::Invalid("invalid namespace in catalog backup".to_string()))?;
                validate_namespace_entry_object(&self.paths, &object.object, &entry)?;
            } else if object.object.ends_with(TABLE_ENTRY_FILE) {
                let entry: TableEntry = serde_json::from_slice(&object.data)
                    .map_err(|_| TableCatalogStoreError::Invalid("invalid table in catalog backup".to_string()))?;
                validate_table_entry_object(&self.paths, &object.object, &entry)?;
                if matches!(entry.state, TableCatalogEntryState::Renaming | TableCatalogEntryState::Deleting) {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup contains a pending table operation".to_string(),
                    ));
                }
                if entry.state == TableCatalogEntryState::Active {
                    let path =
                        table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).ok_or_else(|| {
                            TableCatalogStoreError::Invalid("invalid table metadata location in catalog backup".to_string())
                        })?;
                    metadata_paths.insert(path);
                }
            } else if object.object.ends_with(VIEW_ENTRY_FILE) {
                let entry: ViewEntry = serde_json::from_slice(&object.data)
                    .map_err(|_| TableCatalogStoreError::Invalid("invalid view in catalog backup".to_string()))?;
                validate_view_entry_object(&self.paths, &object.object, &entry)?;
                if matches!(entry.state, TableCatalogEntryState::Renaming | TableCatalogEntryState::Deleting) {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup contains a pending view operation".to_string(),
                    ));
                }
            }
        }
        if !has_bucket {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup is missing its table bucket entry".to_string(),
            ));
        }
        for object in objects.iter().filter(|object| object.bucket != self.catalog_bucket()) {
            if !metadata_paths.contains(&object.object) {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog backup payload is not current table metadata".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn decode_object_catalog_restore_state(
        &self,
        table_bucket: &str,
        data: &[u8],
    ) -> TableCatalogStoreResult<ObjectCatalogRestoreState> {
        if data.len() > OBJECT_CATALOG_RESTORE_STATE_MAX_SIZE {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore state exceeds the encoded size limit".to_string(),
            ));
        }
        let state: ObjectCatalogRestoreState = serde_json::from_slice(data).map_err(|error| {
            TableCatalogStoreError::Invalid(format!(
                "invalid catalog restore state at line {} column {}",
                error.line(),
                error.column()
            ))
        })?;
        if !matches!(state.version, 1 | OBJECT_CATALOG_RESTORE_STATE_VERSION)
            || state.table_bucket != table_bucket
            || Uuid::parse_str(&state.restore_epoch).is_err()
            || state.restore_id.is_empty()
            || state.restore_id.len() > 256
            || state.backup_id.is_empty()
            || state.request_fingerprint.len() != 64
            || state.completed_intent_etag.as_deref().is_some_and(str::is_empty)
            || state.backup_etag.as_deref().is_some_and(str::is_empty)
            || state.completed_intent_etag.is_some() != state.backup_etag.is_some()
        {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore state identity or version is invalid".to_string(),
            ));
        }
        Ok(state)
    }

    pub(in crate::table_catalog) async fn read_object_catalog_restore_intent(
        &self,
        table_bucket: &str,
        restore_id: &str,
    ) -> TableCatalogStoreResult<Option<(ObjectCatalogRestoreIntent, String)>> {
        let path = self.paths.catalog_restore_intent_path(table_bucket, restore_id);
        let Some(stored) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_RESTORE_INTENT_MAX_SIZE)
            .await?
        else {
            return Ok(None);
        };
        let intent: ObjectCatalogRestoreIntent = serde_json::from_slice(&stored.data)
            .map_err(|_| TableCatalogStoreError::Invalid("invalid catalog restore intent".to_string()))?;
        if !matches!(intent.version, 1 | OBJECT_CATALOG_RESTORE_STATE_VERSION)
            || intent.table_bucket != table_bucket
            || intent.restore_id != restore_id
            || Uuid::parse_str(&intent.restore_epoch).is_err()
            || intent.backup_id.is_empty()
            || intent.source_catalog_etag.is_empty()
            || intent.restored_snapshot_etag != intent.source_catalog_etag
            || intent.request_fingerprint
                != restore_request_fingerprint(
                    table_bucket,
                    restore_id,
                    &intent.backup_id,
                    intent.expected_catalog_etag.as_deref(),
                    intent.allow_replace,
                )?
        {
            return Err(TableCatalogStoreError::Invalid("catalog restore intent identity is invalid".to_string()));
        }
        let etag = stored
            .etag
            .filter(|value| !value.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Unavailable("catalog restore intent has no usable ETag".to_string()))?;
        Ok(Some((intent, etag)))
    }

    pub(super) async fn reject_legacy_catalog_global_state(&self) -> TableCatalogStoreResult<()> {
        let paths = &self.paths;
        for (label, object) in [
            ("legacy global catalog fence", paths.legacy_backing_migration_global_fence_path()),
            ("legacy catalog snapshot", paths.legacy_catalog_snapshot_path()),
        ] {
            if self.backend.object_exists(self.catalog_bucket(), &object).await? {
                return Err(TableCatalogStoreError::Unavailable(format!(
                    "{label} is present; explicit catalog recovery is required"
                )));
            }
        }
        Ok(())
    }

    pub(in crate::table_catalog) async fn reject_legacy_catalog_control_state(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<()> {
        self.reject_legacy_catalog_global_state().await?;
        let path = self.paths.legacy_backing_migration_fence_path(table_bucket);
        if self.backend.object_exists(self.catalog_bucket(), &path).await?
            && let Some(object) = self
                .backend
                .read_object_limited(self.catalog_bucket(), &path, super::directory_index::DIRECTORY_STATE_MAX_SIZE)
                .await?
            && !matches!(self.table_directory_state_from_object(table_bucket, &object), Ok(Some(_)))
        {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "legacy per-bucket catalog fence is present for table bucket {table_bucket}; explicit catalog recovery is required"
            )));
        }
        let path = self.paths.legacy_catalog_restore_intent_path(table_bucket);
        if self.backend.object_exists(self.catalog_bucket(), &path).await? {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "legacy catalog restore intent is present for table bucket {table_bucket}; explicit catalog recovery is required"
            )));
        }
        Ok(())
    }

    pub(in crate::table_catalog) async fn read_object_catalog_restore_state(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Option<(ObjectCatalogRestoreState, String)>> {
        let path = self.paths.catalog_restore_state_path(table_bucket);
        if !self.backend.object_exists(self.catalog_bucket(), &path).await? {
            return Ok(None);
        }
        let Some(stored) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_RESTORE_STATE_MAX_SIZE)
            .await?
        else {
            return Err(TableCatalogStoreError::Unavailable(
                "catalog restore state disappeared during observation".to_string(),
            ));
        };
        let etag = stored
            .etag
            .filter(|value| !value.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Unavailable("catalog restore state has no usable ETag".to_string()))?;
        Ok(Some((self.decode_object_catalog_restore_state(table_bucket, &stored.data)?, etag)))
    }

    pub(in crate::table_catalog) async fn write_object_catalog_restore_state(
        &self,
        table_bucket: &str,
        state: &ObjectCatalogRestoreState,
        precondition: TableCatalogPutPrecondition,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<String> {
        let path = self.paths.catalog_restore_state_path(table_bucket);
        let encoded = serde_json::to_vec(state)
            .map_err(|error| TableCatalogStoreError::Internal(format!("failed to encode catalog restore state: {error}")))?;
        if encoded.len() > OBJECT_CATALOG_RESTORE_STATE_MAX_SIZE {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore state exceeds the encoded size limit".to_string(),
            ));
        }
        self.write_restore_record(&path, encoded, precondition, fence).await
    }

    pub(in crate::table_catalog) async fn write_object_catalog_restore_intent(
        &self,
        intent: &ObjectCatalogRestoreIntent,
        precondition: TableCatalogPutPrecondition,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<String> {
        let path = self
            .paths
            .catalog_restore_intent_path(&intent.table_bucket, &intent.restore_id);
        let encoded = serde_json::to_vec(intent)
            .map_err(|error| TableCatalogStoreError::Internal(format!("failed to encode catalog restore intent: {error}")))?;
        if encoded.len() > OBJECT_CATALOG_RESTORE_INTENT_MAX_SIZE {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore intent exceeds the encoded size limit".to_string(),
            ));
        }
        self.write_restore_record(&path, encoded, precondition, fence).await
    }

    async fn write_restore_record(
        &self,
        path: &str,
        encoded: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<String> {
        self.backend
            .put_object_fenced(self.catalog_bucket(), path, encoded.clone(), precondition, fence)
            .await?;
        let stored = self
            .backend
            .read_object_limited(self.catalog_bucket(), path, OBJECT_CATALOG_RESTORE_INTENT_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Unavailable("catalog restore record disappeared after write".to_string()))?;
        if stored.data != encoded {
            return Err(TableCatalogStoreError::Conflict("catalog restore record changed after write".to_string()));
        }
        stored
            .etag
            .filter(|value| !value.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Unavailable("catalog restore record has no usable ETag".to_string()))
    }

    pub(in crate::table_catalog) async fn ensure_no_active_restore(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        self.catalog_restore_read_version(table_bucket).await.map(|_| ())
    }

    pub(super) async fn catalog_restore_read_version(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<String>> {
        self.reject_legacy_catalog_control_state(table_bucket).await?;
        match self.read_object_catalog_restore_state(table_bucket).await? {
            None => Ok(None),
            Some((state, etag)) if state.state == ObjectCatalogRestoreStateKind::Completed => {
                // These immutable object versions were fully validated before
                // publication. Avoid decoding a large backup on every table read.
                if let (Some(intent_etag), Some(backup_etag)) =
                    (state.completed_intent_etag.as_deref(), state.backup_etag.as_deref())
                {
                    for (path, expected) in [
                        (self.paths.catalog_restore_intent_path(table_bucket, &state.restore_id), intent_etag),
                        (self.paths.catalog_backup_path(table_bucket, &state.backup_id), backup_etag),
                    ] {
                        let metadata = self.backend.object_metadata(self.catalog_bucket(), &path).await?;
                        if metadata.as_ref().and_then(|metadata| metadata.etag.as_deref()) != Some(expected) {
                            return Err(TableCatalogStoreError::Unavailable(
                                "completed catalog restore control object changed or disappeared".to_string(),
                            ));
                        }
                    }
                    return Ok(Some(etag));
                }
                // Earlier restore barriers have no sealed object versions; keep
                // their full validation path so no state migration is required.
                let Some((intent, _)) = self
                    .read_object_catalog_restore_intent(table_bucket, &state.restore_id)
                    .await?
                else {
                    return Err(TableCatalogStoreError::Unavailable(
                        "completed catalog restore barrier is missing its durable intent".to_string(),
                    ));
                };
                if intent.state != ObjectCatalogRestoreStateKind::Completed
                    || intent.restore_epoch != state.restore_epoch
                    || intent.restore_id != state.restore_id
                    || intent.backup_id != state.backup_id
                    || intent.request_fingerprint != state.request_fingerprint
                {
                    return Err(TableCatalogStoreError::Unavailable(
                        "completed catalog restore barrier does not match its durable intent".to_string(),
                    ));
                }
                let backup_path = self.paths.catalog_backup_path(table_bucket, &state.backup_id);
                let backup = self
                    .backend
                    .read_object_limited(self.catalog_bucket(), &backup_path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
                    .await?
                    .ok_or_else(|| {
                        TableCatalogStoreError::Unavailable("completed catalog restore backup is missing".to_string())
                    })?;
                let backup = self.decode_object_catalog_backup(table_bucket, &backup.data)?;
                self.validate_restore_plan(&backup, &intent)?;
                Ok(Some(etag))
            }
            Some((state, _)) => Err(TableCatalogStoreError::Unavailable(format!(
                "table bucket {table_bucket} has an interrupted catalog restore epoch {}; retry restore-id {} with backup-id {}",
                state.restore_epoch, state.restore_id, state.backup_id
            ))),
        }
    }

    pub(super) async fn finish_catalog_restore_read(
        &self,
        table_bucket: &str,
        expected_version: Option<&str>,
    ) -> TableCatalogStoreResult<()> {
        if self.catalog_restore_read_version(table_bucket).await?.as_deref() != expected_version {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "table bucket {table_bucket} completed a catalog restore while reading"
            )));
        }
        Ok(())
    }

    pub(in crate::table_catalog) fn restore_report_from_state(
        &self,
        state: &ObjectCatalogRestoreIntent,
        status: TableCatalogRestoreStatus,
    ) -> TableCatalogRestoreReport {
        TableCatalogRestoreReport {
            table_bucket: state.table_bucket.clone(),
            backup_id: state.backup_id.clone(),
            status,
            previous_snapshot_etag: state.previous_snapshot_etag.clone(),
            restored_snapshot_etag: state.restored_snapshot_etag.clone(),
            object_count: state.object_count,
            maintenance_object_count: state.maintenance_object_count,
        }
    }

    pub(in crate::table_catalog) async fn acquire_object_backup_fence(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Vec<TableCatalogLockGuard>> {
        let paths = TableCatalogObjectPaths::default();
        let global = self
            .backend
            .acquire_write_lock(RUSTFS_META_BUCKET, &paths.catalog_backup_global_fence_lock_path())
            .await?;
        let bucket = self
            .backend
            .acquire_write_lock(RUSTFS_META_BUCKET, &paths.catalog_backup_fence_lock_path(table_bucket))
            .await?;
        let publication = self
            .backend
            .acquire_write_lock(table_bucket, &default_table_bucket_publication_lock_path())
            .await?;
        Ok(vec![global, bucket, publication])
    }

    pub(in crate::table_catalog) fn object_backup_mutation_fence(
        guards: &[TableCatalogLockGuard],
    ) -> TableCatalogObjectMutationFence {
        TableCatalogObjectMutationFence::from_signals(guards.iter().map(TableCatalogLockGuard::lock_lost_signal))
    }

    pub(in crate::table_catalog) async fn capture_backup_object(
        &self,
        bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<ObjectCatalogBackupObject> {
        if !safe_object_key(object) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog backup object key is invalid: {bucket}/{object}"
            )));
        }
        let value = self
            .backend
            .read_object_limited(bucket, object, OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup object disappeared: {bucket}/{object}")))?;
        Ok(ObjectCatalogBackupObject {
            bucket: bucket.to_string(),
            object: object.to_string(),
            size_bytes: value
                .data
                .len()
                .try_into()
                .map_err(|_| TableCatalogStoreError::Invalid("catalog backup object size exceeds u64".to_string()))?,
            etag: value.etag,
            sha256: backup_sha256(&value.data),
            data: value.data,
        })
    }

    pub(in crate::table_catalog) fn validate_restore_plan(
        &self,
        backup: &ObjectCatalogBackup,
        intent: &ObjectCatalogRestoreIntent,
    ) -> TableCatalogStoreResult<()> {
        let prefix = self.paths.table_bucket_root_prefix(&intent.table_bucket);
        // Version 1 receipts predate maintenance counting. Preserve their
        // recorded result; version 2 binds the actual sidecars in the plan.
        let maintenance_object_count = if intent.version == 1 {
            0
        } else {
            self.backup_maintenance_object_count(&intent.table_bucket, &backup.objects)
        };
        if intent.object_count != backup.objects.len()
            || intent.maintenance_object_count != maintenance_object_count
            || intent.source_catalog_etag != backup.source_catalog_etag
            || intent.backup_id != backup.backup_id
            || intent.writes.len() != backup.objects.len()
            || intent.writes.iter().zip(&backup.objects).any(|(write, object)| {
                write.bucket != object.bucket
                    || write.object != object.object
                    || write.expected_etag.as_deref().is_some_and(str::is_empty)
                    || (!write.already_present
                        && write.expected_etag.is_some()
                        && (!intent.allow_replace || write.bucket != self.catalog_bucket()))
            })
            || intent.deletes.windows(2).any(|pair| pair[0].object >= pair[1].object)
            || intent.deletes.iter().any(|delete| {
                !intent.allow_replace
                    || !delete.object.starts_with(&prefix)
                    || !safe_object_key(&delete.object)
                    || self.is_catalog_directory_sidecar(&intent.table_bucket, &delete.object)
                    || delete.expected_etag.is_empty()
                    || backup
                        .objects
                        .iter()
                        .any(|object| object.bucket == self.catalog_bucket() && object.object == delete.object)
            })
        {
            return Err(TableCatalogStoreError::Invalid("catalog restore intent plan is invalid".to_string()));
        }
        Ok(())
    }

    pub(in crate::table_catalog) async fn apply_object_catalog_restore_plan(
        &self,
        backup: &ObjectCatalogBackup,
        state: &ObjectCatalogRestoreIntent,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let backup_objects = backup
            .objects
            .iter()
            .map(|object| ((object.bucket.clone(), object.object.clone()), object))
            .collect::<BTreeMap<_, _>>();
        for write in &state.writes {
            let object = backup_objects
                .get(&(write.bucket.clone(), write.object.clone()))
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("catalog restore state references an unknown object".to_string())
                })?;
            let precondition = match self
                .backend
                .read_object_limited(&write.bucket, &write.object, OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE)
                .await?
            {
                None if write.expected_etag.is_none() && !write.already_present => TableCatalogPutPrecondition::IfAbsent,
                None => {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog restore target disappeared: {}/{}",
                        write.bucket, write.object
                    )));
                }
                Some(existing) if existing.data == object.data => continue,
                Some(_) if write.already_present => {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog restore previously identical target changed: {}/{}",
                        write.bucket, write.object
                    )));
                }
                Some(existing) => match write.expected_etag.as_deref() {
                    Some(expected) if existing.etag.as_deref() == Some(expected) => {
                        TableCatalogPutPrecondition::IfMatch(expected.to_string())
                    }
                    Some(_) => {
                        return Err(TableCatalogStoreError::Conflict(format!(
                            "catalog restore target object changed: {}/{}",
                            write.bucket, write.object
                        )));
                    }
                    None => {
                        return Err(TableCatalogStoreError::Conflict(format!(
                            "catalog restore target object appeared: {}/{}",
                            write.bucket, write.object
                        )));
                    }
                },
            };
            fence.ensure_held()?;
            self.backend
                .put_object_fenced(&write.bucket, &write.object, object.data.clone(), precondition, fence)
                .await?;
        }
        for delete in &state.deletes {
            let Some(metadata) = self.backend.object_metadata(self.catalog_bucket(), &delete.object).await? else {
                continue;
            };
            let Some(etag) = metadata.etag.filter(|value| !value.is_empty()) else {
                return Err(TableCatalogStoreError::Unavailable(
                    "catalog restore deletion target has no usable ETag".to_string(),
                ));
            };
            if etag != delete.expected_etag {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog restore deletion target changed: {}",
                    delete.object
                )));
            }
            fence.ensure_held()?;
            self.backend
                .delete_object_if_match_fenced(self.catalog_bucket(), &delete.object, &etag, fence)
                .await?;
        }
        fence.ensure_held()
    }
}
