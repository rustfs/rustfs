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
use std::collections::BTreeSet;

const OBJECT_CATALOG_BACKUP_VERSION: u16 = 1;
const OBJECT_CATALOG_BACKUP_MAX_SIZE: usize = 64 * 1024 * 1024;
const OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE: usize = 16 * 1024 * 1024;

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

    async fn validate_catalog_backup_snapshot_graph(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<()> {
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

    pub(crate) async fn restore_object_catalog_backup(
        &self,
        table_bucket: &str,
        backup_id: &str,
        expected_catalog_etag: Option<&str>,
        allow_replace: bool,
    ) -> TableCatalogStoreResult<TableCatalogRestoreReport> {
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let mutation_fence = Self::object_backup_mutation_fence(&guards);
        mutation_fence.ensure_held()?;
        let path = self.paths.catalog_backup_path(table_bucket, backup_id);
        let stored = self
            .backend
            .read_object_limited(RUSTFS_META_BUCKET, &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup {backup_id}")))?;
        let backup = self.decode_object_catalog_backup(table_bucket, &stored.data)?;
        if backup.backup_id != backup_id {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity does not match its request".to_string(),
            ));
        }
        if expected_catalog_etag.is_some_and(|expected| expected != backup.source_catalog_etag) {
            return Err(TableCatalogStoreError::Conflict(
                "catalog backup source watermark does not match".to_string(),
            ));
        }
        self.validate_catalog_backup_snapshot_graph(table_bucket, &backup.objects)
            .await?;
        mutation_fence.ensure_held()?;
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
            let precondition = match self
                .backend
                .read_object_limited(&object.bucket, &object.object, OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE)
                .await?
            {
                None => TableCatalogPutPrecondition::IfAbsent,
                Some(existing) if existing.data == object.data => continue,
                Some(existing) => {
                    // Replacement applies only to internal catalog state. Iceberg
                    // metadata is immutable, including during disaster recovery.
                    if !allow_replace || object.bucket != self.catalog_bucket() {
                        return Err(TableCatalogStoreError::Conflict(format!(
                            "catalog restore target object changed: {}/{}",
                            object.bucket, object.object
                        )));
                    }
                    let etag = existing.etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                        TableCatalogStoreError::Unavailable("catalog restore target has no usable ETag".to_string())
                    })?;
                    TableCatalogPutPrecondition::IfMatch(etag)
                }
            };
            writes.push((object, precondition));
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
                let etag = metadata.etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                    TableCatalogStoreError::Unavailable("catalog restore deletion target has no usable ETag".to_string())
                })?;
                deletes.push((object, etag));
            }
        }
        let status = if writes.is_empty() && deletes.is_empty() {
            TableCatalogRestoreStatus::AlreadyRestored
        } else {
            TableCatalogRestoreStatus::Restored
        };
        // Bind each mutation to its preflight observation. Re-reading a fresh
        // ETag here would authorize overwriting a concurrent replacement.
        for (object, precondition) in writes {
            mutation_fence.ensure_held()?;
            self.backend
                .put_object_fenced(&object.bucket, &object.object, object.data.clone(), precondition, &mutation_fence)
                .await?;
        }
        for (object, etag) in deletes {
            mutation_fence.ensure_held()?;
            self.backend
                .delete_object_if_match_fenced(self.catalog_bucket(), &object, &etag, &mutation_fence)
                .await?;
        }
        mutation_fence.ensure_held()?;
        Ok(TableCatalogRestoreReport {
            table_bucket: table_bucket.to_string(),
            backup_id: backup_id.to_string(),
            status,
            previous_snapshot_etag: stored.etag,
            restored_snapshot_etag: backup.source_catalog_etag,
            object_count: backup.objects.len(),
            maintenance_object_count: 0,
        })
    }
}
