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

use super::backup_types::{TableCatalogBackupStatus, TableCatalogRestoreStatus};
use super::object::{
    ObjectTableCatalogStore, validate_namespace_entry_object, validate_table_bucket_entry_object, validate_table_entry_object,
    validate_view_entry_object,
};
use super::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[path = "object_backup_transfer.rs"]
mod transfer;

const OBJECT_CATALOG_BACKUP_VERSION: u16 = 1;
const OBJECT_CATALOG_BACKUP_MAX_SIZE: usize = 64 * 1024 * 1024;
const OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE: usize = 16 * 1024 * 1024;
const OBJECT_CATALOG_RESTORE_STATE_VERSION: u16 = 1;
const OBJECT_CATALOG_RESTORE_STATE_MAX_SIZE: usize = 16 * 1024;
const OBJECT_CATALOG_RESTORE_INTENT_MAX_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectCatalogBackup {
    version: u16,
    backup_id: String,
    table_bucket: String,
    created_at: String,
    source_catalog_etag: String,
    objects: Vec<ObjectCatalogBackupObject>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectCatalogBackupObject {
    bucket: String,
    object: String,
    size_bytes: u64,
    etag: Option<String>,
    sha256: String,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ObjectCatalogRestoreStateKind {
    Prepared,
    Applying,
    CatalogApplied,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectCatalogRestoreWrite {
    bucket: String,
    object: String,
    expected_etag: Option<String>,
    already_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectCatalogRestoreDelete {
    object: String,
    expected_etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectCatalogRestoreState {
    version: u16,
    table_bucket: String,
    restore_epoch: String,
    restore_id: String,
    backup_id: String,
    request_fingerprint: String,
    state: ObjectCatalogRestoreStateKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed_intent_etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backup_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectCatalogRestoreIntent {
    version: u16,
    table_bucket: String,
    restore_epoch: String,
    restore_id: String,
    backup_id: String,
    request_fingerprint: String,
    expected_catalog_etag: Option<String>,
    allow_replace: bool,
    source_catalog_etag: String,
    state: ObjectCatalogRestoreStateKind,
    previous_snapshot_etag: Option<String>,
    restored_snapshot_etag: String,
    object_count: usize,
    maintenance_object_count: usize,
    writes: Vec<ObjectCatalogRestoreWrite>,
    deletes: Vec<ObjectCatalogRestoreDelete>,
    created_at: String,
    updated_at: String,
}

fn backup_sha256(data: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(data), hex_simd::AsciiCase::Lower)
}

fn safe_object_key(object: &str) -> bool {
    !object.is_empty()
        && !object.starts_with('/')
        && !object.contains("..")
        && !object.contains('\\')
        && !object.bytes().any(|byte| byte.is_ascii_control())
}

fn catalog_backup_fingerprint(objects: &[ObjectCatalogBackupObject]) -> TableCatalogStoreResult<String> {
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

fn restore_request_fingerprint(
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

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    fn decode_object_catalog_backup(&self, table_bucket: &str, data: &[u8]) -> TableCatalogStoreResult<ObjectCatalogBackup> {
        if data.len() > OBJECT_CATALOG_BACKUP_MAX_SIZE {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup exceeds the encoded size limit".to_string(),
            ));
        }
        let backup: ObjectCatalogBackup = serde_json::from_slice(data).map_err(|error| {
            TableCatalogStoreError::Invalid(format!("invalid catalog backup at line {} column {}", error.line(), error.column()))
        })?;
        if backup.version != OBJECT_CATALOG_BACKUP_VERSION || backup.table_bucket != table_bucket {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity or version does not match its request".to_string(),
            ));
        }
        self.validate_object_catalog_backup_objects(table_bucket, &backup.objects)?;
        if catalog_backup_fingerprint(&backup.objects)? != backup.source_catalog_etag
            || catalog_backup_identity(table_bucket, &backup.source_catalog_etag, &backup.objects)? != backup.backup_id
        {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup content does not match its identity".to_string(),
            ));
        }
        Ok(backup)
    }

    fn validate_object_catalog_backup_objects(
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
            if !object.object.starts_with(&prefix) {
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
        if state.version != OBJECT_CATALOG_RESTORE_STATE_VERSION
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

    async fn read_object_catalog_restore_intent(
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
        if intent.version != OBJECT_CATALOG_RESTORE_STATE_VERSION
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

    async fn reject_legacy_catalog_control_state(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        self.reject_legacy_catalog_global_state().await?;
        let paths = &self.paths;
        for (label, object) in [
            ("legacy per-bucket catalog fence", paths.legacy_backing_migration_fence_path(table_bucket)),
            ("legacy catalog restore intent", paths.legacy_catalog_restore_intent_path(table_bucket)),
        ] {
            if self.backend.object_exists(self.catalog_bucket(), &object).await? {
                return Err(TableCatalogStoreError::Unavailable(format!(
                    "{label} is present for table bucket {table_bucket}; explicit catalog recovery is required"
                )));
            }
        }
        Ok(())
    }

    async fn read_object_catalog_restore_state(
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

    async fn write_object_catalog_restore_state(
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

    async fn write_object_catalog_restore_intent(
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

    pub(super) async fn ensure_no_active_restore(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
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

    fn restore_report_from_state(
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

    async fn acquire_object_backup_fence(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableCatalogLockGuard>> {
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

    fn object_backup_mutation_fence(guards: &[TableCatalogLockGuard]) -> TableCatalogObjectMutationFence {
        TableCatalogObjectMutationFence::from_signals(guards.iter().map(TableCatalogLockGuard::lock_lost_signal))
    }

    async fn capture_backup_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<ObjectCatalogBackupObject> {
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

    async fn capture_object_catalog_backup_objects(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Vec<ObjectCatalogBackupObject>> {
        let prefix = self.paths.table_bucket_root_prefix(table_bucket);
        let mut objects = Vec::new();
        for object in self.backend.list_objects(self.catalog_bucket(), &prefix).await? {
            objects.push(self.capture_backup_object(self.catalog_bucket(), &object).await?);
        }
        let catalog_objects = objects.clone();
        for object in catalog_objects {
            if !object.object.ends_with(TABLE_ENTRY_FILE) {
                continue;
            }
            let entry: TableEntry = serde_json::from_slice(&object.data)
                .map_err(|error| TableCatalogStoreError::Invalid(format!("table catalog entry is invalid: {error}")))?;
            if entry.state != TableCatalogEntryState::Active {
                continue;
            }
            let metadata_object = table_catalog_object_key_from_location(&entry.table_bucket, &entry.metadata_location)
                .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata location is not an object path".to_string()))?;
            if !objects
                .iter()
                .any(|candidate| candidate.bucket == entry.table_bucket && candidate.object == metadata_object)
            {
                objects.push(self.capture_backup_object(&entry.table_bucket, &metadata_object).await?);
            }
        }
        objects.sort_by(|left, right| left.bucket.cmp(&right.bucket).then_with(|| left.object.cmp(&right.object)));
        if objects.is_empty() {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket catalog {table_bucket}")));
        }
        Ok(objects)
    }

    fn validate_catalog_backup_metadata_identities(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<Vec<(TableEntry, serde_json::Value)>> {
        let mut tables = Vec::new();
        for object in objects
            .iter()
            .filter(|object| object.bucket == RUSTFS_META_BUCKET && object.object.ends_with(TABLE_ENTRY_FILE))
        {
            let entry: TableEntry = serde_json::from_slice(&object.data)
                .map_err(|error| TableCatalogStoreError::Invalid(format!("table catalog entry is invalid: {error}")))?;
            if entry.state != TableCatalogEntryState::Active {
                continue;
            }
            if entry.table_bucket != table_bucket {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog backup contains an entry for another table bucket".to_string(),
                ));
            }
            let metadata_object = table_catalog_object_key_from_location(&entry.table_bucket, &entry.metadata_location)
                .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata location is not an object path".to_string()))?;
            let metadata = objects
                .iter()
                .find(|candidate| candidate.bucket == entry.table_bucket && candidate.object == metadata_object)
                .ok_or_else(|| {
                    TableCatalogStoreError::NotFound(format!(
                        "catalog backup is missing current metadata object: {}/{}",
                        entry.table_bucket, metadata_object
                    ))
                })?;
            let metadata = decode_table_metadata_json(&entry.metadata_location, &metadata.data)?;
            validate_supported_table_metadata(&metadata)?;
            if metadata.get("table-uuid").and_then(serde_json::Value::as_str) != Some(entry.table_uuid.as_str())
                || metadata.get("location").and_then(serde_json::Value::as_str) != Some(entry.warehouse_location.as_str())
                || metadata
                    .get("format-version")
                    .and_then(serde_json::Value::as_u64)
                    .is_none_or(|version| version < u64::from(entry.format_version))
            {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog backup metadata does not match its table identity".to_string(),
                ));
            }
            tables.push((entry, metadata));
        }
        Ok(tables)
    }

    async fn validate_catalog_backup_snapshot_graph(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<()> {
        for (entry, metadata) in self.validate_catalog_backup_metadata_identities(table_bucket, objects)? {
            let context = TableSnapshotGraphValidationContext::new(&self.backend, table_bucket, &entry);
            validate_table_snapshot_changes(&context, None, &metadata).await?;
        }
        Ok(())
    }

    pub(crate) async fn create_object_catalog_backup(
        &self,
        table_bucket: &str,
        expected_catalog_etag: Option<&str>,
    ) -> TableCatalogStoreResult<TableCatalogBackupReport> {
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let mutation_fence = Self::object_backup_mutation_fence(&guards);
        mutation_fence.ensure_held()?;
        self.ensure_no_active_restore(table_bucket).await?;
        let objects = self.capture_object_catalog_backup_objects(table_bucket).await?;
        self.validate_object_catalog_backup_objects(table_bucket, &objects)?;
        mutation_fence.ensure_held()?;
        self.validate_catalog_backup_snapshot_graph(table_bucket, &objects).await?;
        mutation_fence.ensure_held()?;
        let source_catalog_etag = catalog_backup_fingerprint(&objects)?;
        if expected_catalog_etag.is_some_and(|expected| expected != source_catalog_etag) {
            return Err(TableCatalogStoreError::Conflict(
                "catalog changed before object backup capture".to_string(),
            ));
        }
        let mut created_at = OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|error| TableCatalogStoreError::Internal(format!("failed to format backup timestamp: {error}")))?;
        let backup_id = catalog_backup_identity(table_bucket, &source_catalog_etag, &objects)?;
        let backup = ObjectCatalogBackup {
            version: OBJECT_CATALOG_BACKUP_VERSION,
            backup_id: backup_id.clone(),
            table_bucket: table_bucket.to_string(),
            created_at: created_at.clone(),
            source_catalog_etag: source_catalog_etag.clone(),
            objects,
        };
        let encoded = serde_json::to_vec(&backup)
            .map_err(|error| TableCatalogStoreError::Internal(format!("failed to encode object catalog backup: {error}")))?;
        if encoded.len() > OBJECT_CATALOG_BACKUP_MAX_SIZE {
            return Err(TableCatalogStoreError::Invalid(format!(
                "object catalog backup exceeds the maximum encoded size of {OBJECT_CATALOG_BACKUP_MAX_SIZE} bytes"
            )));
        }
        let path = self.paths.catalog_backup_path(table_bucket, &backup_id);
        let status = match self
            .backend
            .put_object_fenced(RUSTFS_META_BUCKET, &path, encoded, TableCatalogPutPrecondition::IfAbsent, &mutation_fence)
            .await
        {
            Ok(()) => TableCatalogBackupStatus::Created,
            Err(TableCatalogStoreError::Conflict(_)) => {
                let existing = self
                    .backend
                    .read_object_limited(RUSTFS_META_BUCKET, &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
                    .await?
                    .ok_or_else(|| TableCatalogStoreError::Conflict("catalog backup disappeared after conflict".to_string()))?;
                let existing = self.decode_object_catalog_backup(table_bucket, &existing.data)?;
                let same_identity = existing.version == backup.version
                    && existing.backup_id == backup.backup_id
                    && existing.table_bucket == backup.table_bucket
                    && existing.source_catalog_etag == backup.source_catalog_etag
                    && existing.objects == backup.objects;
                if !same_identity {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup identity is already occupied by different content".to_string(),
                    ));
                }
                created_at = existing.created_at;
                TableCatalogBackupStatus::AlreadyPresent
            }
            Err(error) => return Err(error),
        };
        mutation_fence.ensure_held()?;
        let verified_bytes = backup.objects.iter().map(|object| object.size_bytes).sum();
        Ok(TableCatalogBackupReport {
            table_bucket: table_bucket.to_string(),
            backup_id,
            status,
            backup_path: path,
            source_snapshot_etag: source_catalog_etag.clone(),
            source_snapshot_version: OBJECT_CATALOG_BACKUP_VERSION,
            catalog_fingerprint: source_catalog_etag,
            object_count: backup.objects.len(),
            verified_object_count: backup.objects.len(),
            verified_bytes,
            maintenance_object_count: 0,
            created_at,
        })
    }

    async fn preflight_object_catalog_restore(
        &self,
        table_bucket: &str,
        backup: &ObjectCatalogBackup,
        allow_replace: bool,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(Vec<ObjectCatalogRestoreWrite>, Vec<ObjectCatalogRestoreDelete>)> {
        self.validate_catalog_backup_snapshot_graph(table_bucket, &backup.objects)
            .await?;
        fence.ensure_held()?;
        let target_prefix = self.paths.table_bucket_root_prefix(table_bucket);
        let current = self.backend.list_objects(self.catalog_bucket(), &target_prefix).await?;
        let target_paths = backup
            .objects
            .iter()
            .filter(|object| object.bucket == self.catalog_bucket())
            .map(|object| object.object.as_str())
            .collect::<BTreeSet<_>>();
        if !allow_replace && current.iter().any(|object| !target_paths.contains(object.as_str())) {
            return Err(TableCatalogStoreError::Conflict(
                "catalog restore would replace existing catalog objects; set allow-replace".to_string(),
            ));
        }
        let mut writes = Vec::new();
        for object in &backup.objects {
            let (expected_etag, already_present) = match self
                .backend
                .read_object_limited(&object.bucket, &object.object, OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE)
                .await?
            {
                None => (None, false),
                Some(existing) if existing.data == object.data => (existing.etag, true),
                Some(existing) => {
                    // Replacement applies only to internal catalog state. Iceberg
                    // metadata is immutable, including during disaster recovery.
                    if !allow_replace || object.bucket != self.catalog_bucket() {
                        return Err(TableCatalogStoreError::Conflict(format!(
                            "catalog restore target object changed: {}/{}",
                            object.bucket, object.object
                        )));
                    }
                    (
                        Some(existing.etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                            TableCatalogStoreError::Unavailable("catalog restore target has no usable ETag".to_string())
                        })?),
                        false,
                    )
                }
            };
            writes.push(ObjectCatalogRestoreWrite {
                bucket: object.bucket.clone(),
                object: object.object.clone(),
                expected_etag,
                already_present,
            });
        }
        let mut deletes = Vec::new();
        if allow_replace {
            for object in current {
                if target_paths.contains(object.as_str()) {
                    continue;
                }
                let metadata = self
                    .backend
                    .object_metadata(self.catalog_bucket(), &object)
                    .await?
                    .ok_or_else(|| {
                        TableCatalogStoreError::Conflict("catalog restore target disappeared during preflight".to_string())
                    })?;
                let expected_etag = metadata.etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                    TableCatalogStoreError::Unavailable("catalog restore deletion target has no usable ETag".to_string())
                })?;
                deletes.push(ObjectCatalogRestoreDelete { object, expected_etag });
            }
        }
        deletes.sort_by(|left, right| left.object.cmp(&right.object));
        Ok((writes, deletes))
    }

    fn validate_restore_plan(
        &self,
        backup: &ObjectCatalogBackup,
        intent: &ObjectCatalogRestoreIntent,
    ) -> TableCatalogStoreResult<()> {
        let prefix = self.paths.table_bucket_root_prefix(&intent.table_bucket);
        if intent.object_count != backup.objects.len()
            || intent.maintenance_object_count != 0
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

    async fn verify_restored_catalog(&self, backup: &ObjectCatalogBackup) -> TableCatalogStoreResult<()> {
        for object in &backup.objects {
            let current = self
                .backend
                .read_object_limited(&object.bucket, &object.object, OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE)
                .await?;
            if current.is_none_or(|current| current.data != object.data) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog restore target differs from the backup: {}/{}",
                    object.bucket, object.object
                )));
            }
        }
        let actual = self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.table_bucket_root_prefix(&backup.table_bucket))
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let expected = backup
            .objects
            .iter()
            .filter(|object| object.bucket == self.catalog_bucket())
            .map(|object| object.object.clone())
            .collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(TableCatalogStoreError::Conflict("catalog restore target inventory changed".to_string()));
        }
        self.validate_catalog_backup_snapshot_graph(&backup.table_bucket, &backup.objects)
            .await
    }

    async fn apply_object_catalog_restore_plan(
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

    async fn current_object_catalog_fingerprint(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<String>> {
        let current = self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.table_bucket_root_prefix(table_bucket))
            .await?;
        if current.is_empty() {
            return Ok(None);
        }
        let objects = self.capture_object_catalog_backup_objects(table_bucket).await?;
        catalog_backup_fingerprint(&objects).map(Some)
    }

    #[cfg(test)]
    pub(crate) async fn restore_object_catalog_backup(
        &self,
        table_bucket: &str,
        backup_id: &str,
        expected_catalog_etag: Option<&str>,
        allow_replace: bool,
    ) -> TableCatalogStoreResult<TableCatalogRestoreReport> {
        self.restore_object_catalog_backup_with_id(table_bucket, backup_id, expected_catalog_etag, allow_replace, None)
            .await
    }

    pub(crate) async fn restore_object_catalog_backup_with_id(
        &self,
        table_bucket: &str,
        backup_id: &str,
        expected_catalog_etag: Option<&str>,
        allow_replace: bool,
        restore_id: Option<&str>,
    ) -> TableCatalogStoreResult<TableCatalogRestoreReport> {
        let restore_id = restore_id.unwrap_or(backup_id);
        if restore_id.is_empty()
            || restore_id.len() > 256
            || !restore_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._".contains(&byte))
            || expected_catalog_etag.is_some_and(str::is_empty)
        {
            return Err(TableCatalogStoreError::Invalid("catalog restore request identity is invalid".to_string()));
        }
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let fence = Self::object_backup_mutation_fence(&guards);
        fence.ensure_held()?;
        self.reject_legacy_catalog_control_state(table_bucket).await?;
        let path = self.paths.catalog_backup_path(table_bucket, backup_id);
        let stored = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup {backup_id}")))?;
        let backup = self.decode_object_catalog_backup(table_bucket, &stored.data)?;
        let backup_etag = stored
            .etag
            .filter(|etag| !etag.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Unavailable("catalog backup has no usable ETag".to_string()))?;
        if backup.backup_id != backup_id {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity does not match its request".to_string(),
            ));
        }
        let request_fingerprint =
            restore_request_fingerprint(table_bucket, restore_id, backup_id, expected_catalog_etag, allow_replace)?;
        let current_state = self.read_object_catalog_restore_state(table_bucket).await?;
        let saved = self.read_object_catalog_restore_intent(table_bucket, restore_id).await?;
        if let Some((intent, _)) = saved.as_ref() {
            if intent.request_fingerprint != request_fingerprint || intent.backup_id != backup_id {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog restore-id is already bound to a different request".to_string(),
                ));
            }
            self.validate_restore_plan(&backup, intent)?;
            if intent.state == ObjectCatalogRestoreStateKind::Completed
                && current_state.as_ref().is_some_and(|(state, _)| {
                    state.restore_epoch != intent.restore_epoch || state.state == ObjectCatalogRestoreStateKind::Completed
                })
            {
                // A receipt is a result of this request, not permission to roll back
                // legitimate catalog writes made after its publication.
                fence.ensure_held()?;
                return Ok(self.restore_report_from_state(intent, TableCatalogRestoreStatus::AlreadyRestored));
            }
        }
        if let Some((state, _)) = current_state.as_ref()
            && state.state != ObjectCatalogRestoreStateKind::Completed
            && (state.restore_id != restore_id || state.request_fingerprint != request_fingerprint)
        {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "table bucket {table_bucket} has an active catalog restore for another request"
            )));
        }
        if let Some((state, _)) = current_state.as_ref()
            && state.restore_id == restore_id
            && saved.is_none()
        {
            return Err(TableCatalogStoreError::Unavailable(
                "catalog restore barrier is missing its durable intent".to_string(),
            ));
        }

        let (mut intent, mut intent_etag) = match saved {
            Some(saved) => saved,
            None => {
                let previous_snapshot_etag = self.current_object_catalog_fingerprint(table_bucket).await?;
                if expected_catalog_etag.is_some_and(|expected| Some(expected) != previous_snapshot_etag.as_deref()) {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog restore target watermark does not match".to_string(),
                    ));
                }
                let (writes, deletes) = self
                    .preflight_object_catalog_restore(table_bucket, &backup, allow_replace, &fence)
                    .await?;
                let now = OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .map_err(|error| TableCatalogStoreError::Internal(format!("failed to format restore timestamp: {error}")))?;
                let intent = ObjectCatalogRestoreIntent {
                    version: OBJECT_CATALOG_RESTORE_STATE_VERSION,
                    table_bucket: table_bucket.to_string(),
                    restore_epoch: Uuid::new_v4().to_string(),
                    restore_id: restore_id.to_string(),
                    backup_id: backup_id.to_string(),
                    request_fingerprint: request_fingerprint.clone(),
                    expected_catalog_etag: expected_catalog_etag.map(str::to_string),
                    allow_replace,
                    source_catalog_etag: backup.source_catalog_etag.clone(),
                    state: ObjectCatalogRestoreStateKind::Prepared,
                    previous_snapshot_etag,
                    restored_snapshot_etag: backup.source_catalog_etag.clone(),
                    object_count: backup.objects.len(),
                    maintenance_object_count: 0,
                    writes,
                    deletes,
                    created_at: now.clone(),
                    updated_at: now,
                };
                self.validate_restore_plan(&backup, &intent)?;
                let etag = self
                    .write_object_catalog_restore_intent(&intent, TableCatalogPutPrecondition::IfAbsent, &fence)
                    .await?;
                (intent, etag)
            }
        };
        let (mut state, mut state_etag) = match current_state {
            Some((state, etag)) if state.state != ObjectCatalogRestoreStateKind::Completed => {
                if state.restore_epoch != intent.restore_epoch || state.backup_id != intent.backup_id {
                    return Err(TableCatalogStoreError::Unavailable(
                        "catalog restore intent does not own the active barrier".to_string(),
                    ));
                }
                (state, etag)
            }
            previous => {
                // An intent may have persisted immediately before a crash. No target
                // mutation is authorized until its original target is revalidated.
                if intent.state != ObjectCatalogRestoreStateKind::Prepared
                    || self.current_object_catalog_fingerprint(table_bucket).await? != intent.previous_snapshot_etag
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog restore target changed before barrier activation".to_string(),
                    ));
                }
                let state = ObjectCatalogRestoreState {
                    version: OBJECT_CATALOG_RESTORE_STATE_VERSION,
                    table_bucket: table_bucket.to_string(),
                    restore_epoch: intent.restore_epoch.clone(),
                    restore_id: restore_id.to_string(),
                    backup_id: backup_id.to_string(),
                    request_fingerprint,
                    state: ObjectCatalogRestoreStateKind::Prepared,
                    completed_intent_etag: None,
                    backup_etag: None,
                };
                let precondition = previous.map_or(TableCatalogPutPrecondition::IfAbsent, |(_, etag)| {
                    TableCatalogPutPrecondition::IfMatch(etag)
                });
                let etag = self
                    .write_object_catalog_restore_state(table_bucket, &state, precondition, &fence)
                    .await?;
                (state, etag)
            }
        };
        if state.state == ObjectCatalogRestoreStateKind::Prepared {
            state.state = ObjectCatalogRestoreStateKind::Applying;
            state_etag = self
                .write_object_catalog_restore_state(
                    table_bucket,
                    &state,
                    TableCatalogPutPrecondition::IfMatch(state_etag),
                    &fence,
                )
                .await?;
        }
        if intent.state == ObjectCatalogRestoreStateKind::Prepared {
            // Recheck immutable Iceberg references before resuming, including after
            // a process restart. Every mutation retains its first observed ETag.
            self.validate_catalog_backup_snapshot_graph(table_bucket, &backup.objects)
                .await?;
            self.apply_object_catalog_restore_plan(&backup, &intent, &fence).await?;
            self.verify_restored_catalog(&backup).await?;
            intent.state = ObjectCatalogRestoreStateKind::CatalogApplied;
            intent.updated_at = OffsetDateTime::now_utc().to_string();
            intent_etag = self
                .write_object_catalog_restore_intent(&intent, TableCatalogPutPrecondition::IfMatch(intent_etag), &fence)
                .await?;
        } else {
            self.verify_restored_catalog(&backup).await?;
        }
        if state.state != ObjectCatalogRestoreStateKind::CatalogApplied {
            state.state = ObjectCatalogRestoreStateKind::CatalogApplied;
            state_etag = self
                .write_object_catalog_restore_state(
                    table_bucket,
                    &state,
                    TableCatalogPutPrecondition::IfMatch(state_etag),
                    &fence,
                )
                .await?;
        }
        if intent.state != ObjectCatalogRestoreStateKind::Completed {
            intent.state = ObjectCatalogRestoreStateKind::Completed;
            intent.updated_at = OffsetDateTime::now_utc().to_string();
            intent_etag = self
                .write_object_catalog_restore_intent(&intent, TableCatalogPutPrecondition::IfMatch(intent_etag), &fence)
                .await?;
        }
        // Publication is the final durable write. Until it succeeds all readers
        // and writers reject the bucket, even if every catalog object was applied.
        state.state = ObjectCatalogRestoreStateKind::Completed;
        state.completed_intent_etag = Some(intent_etag);
        state.backup_etag = Some(backup_etag);
        self.write_object_catalog_restore_state(table_bucket, &state, TableCatalogPutPrecondition::IfMatch(state_etag), &fence)
            .await?;
        fence.ensure_held()?;
        Ok(self.restore_report_from_state(&intent, TableCatalogRestoreStatus::Restored))
    }
}
