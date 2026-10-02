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

use super::strong::{
    StrongSnapshotWritePostcondition, StrongTableCatalogBucketSnapshot, StrongTableCatalogSnapshot, StrongTableCatalogStore,
    table_catalog_bucket_snapshot_fingerprint,
};
use super::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrongTableCatalogBackup {
    version: u16,
    backup_id: String,
    table_bucket: String,
    created_at: String,
    source_snapshot_etag: String,
    source_snapshot_version: u16,
    catalog_fingerprint: String,
    snapshot: StrongTableCatalogBucketSnapshot,
    objects: Vec<StrongTableCatalogBackupObject>,
    maintenance_objects: Vec<StrongTableCatalogBackupMaintenanceObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrongTableCatalogBackupObject {
    bucket: String,
    object: String,
    kind: StrongTableCatalogBackupObjectKind,
    size_bytes: u64,
    etag: Option<String>,
    sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StrongTableCatalogBackupObjectKind {
    TableMetadata,
    ViewMetadata,
    ManifestList,
    ManifestFile,
    DataFile,
    DeleteFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrongTableCatalogBackupMaintenanceObject {
    object: String,
    size_bytes: u64,
    etag: Option<String>,
    sha256: String,
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrongTableCatalogRestoreIntent {
    version: u16,
    table_bucket: String,
    backup_id: String,
    expected_snapshot_etag: Option<String>,
    restored_snapshot_etag: Option<String>,
    state: StrongTableCatalogRestoreIntentState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StrongTableCatalogRestoreIntentState {
    Prepared,
    CatalogApplied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogBackupStatus {
    Created,
    AlreadyPresent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogBackupReport {
    pub table_bucket: String,
    pub backup_id: String,
    pub status: TableCatalogBackupStatus,
    pub backup_path: String,
    pub source_snapshot_etag: String,
    pub source_snapshot_version: u16,
    pub catalog_fingerprint: String,
    pub object_count: usize,
    pub verified_object_count: usize,
    pub verified_bytes: u64,
    pub maintenance_object_count: usize,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogRestoreStatus {
    Restored,
    AlreadyRestored,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogRestoreReport {
    pub table_bucket: String,
    pub backup_id: String,
    pub status: TableCatalogRestoreStatus,
    pub previous_snapshot_etag: Option<String>,
    pub restored_snapshot_etag: String,
    pub object_count: usize,
    pub maintenance_object_count: usize,
}

impl StrongTableCatalogBackupObjectKind {
    fn from_maintenance_kind(kind: &TableMetadataMaintenanceObjectKind) -> Self {
        match kind {
            TableMetadataMaintenanceObjectKind::ManifestList => Self::ManifestList,
            TableMetadataMaintenanceObjectKind::ManifestFile => Self::ManifestFile,
            TableMetadataMaintenanceObjectKind::DataFile => Self::DataFile,
            TableMetadataMaintenanceObjectKind::DeleteFile => Self::DeleteFile,
            TableMetadataMaintenanceObjectKind::MetadataFile => Self::TableMetadata,
        }
    }
}

fn backup_sha256(data: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(data), hex_simd::AsciiCase::Lower)
}

fn catalog_backup_id(
    table_bucket: &str,
    source_snapshot_etag: &str,
    source_snapshot_version: u16,
    catalog_fingerprint: &str,
    objects: &[StrongTableCatalogBackupObject],
    maintenance_objects: &[StrongTableCatalogBackupMaintenanceObject],
) -> TableCatalogStoreResult<String> {
    let payload = serde_json::to_vec(&(
        table_bucket,
        source_snapshot_etag,
        source_snapshot_version,
        catalog_fingerprint,
        objects,
        maintenance_objects,
    ))
    .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode backup identity: {err}")))?;
    Ok(backup_sha256(&payload))
}

fn backup_object_key_is_safe(object: &str) -> bool {
    !object.is_empty()
        && !object.starts_with('/')
        && !object.contains("..")
        && !object.contains('\\')
        && !object.bytes().any(|byte| byte.is_ascii_control())
}

fn maintenance_object_is_active(data: &[u8]) -> TableCatalogStoreResult<bool> {
    let value = serde_json::from_slice::<serde_json::Value>(data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("maintenance backup object is not valid JSON: {err}")))?;
    let job = value.get("job").unwrap_or(&value);
    let status = job.get("status").and_then(serde_json::Value::as_str);
    let active_status = matches!(status, Some("QUEUED" | "RUNNING"));
    let terminal_status = matches!(status, Some("NOT_YET_RUN" | "SUCCESSFUL" | "FAILED" | "DISABLED" | "PAUSED"));
    let active_lease = ["lease-id", "lease_id", "scheduler-lease-id", "scheduler_lease_id"]
        .into_iter()
        .any(|field| {
            job.get(field)
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.is_empty())
        });
    Ok(active_status || (!terminal_status && active_lease))
}

impl<B> StrongTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    fn ensure_backup_fence_held(
        bucket_guard: &TableCatalogLockGuard,
        global_guard: &TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<()> {
        if bucket_guard.is_lock_lost() || global_guard.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable(
                "catalog backup or restore lost its fencing lock".to_string(),
            ));
        }
        Ok(())
    }

    async fn acquire_backup_fence(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<(TableCatalogLockGuard, TableCatalogLockGuard)> {
        // Mutating catalog operations acquire the table-bucket fence before the global snapshot read lock. Keep the same order here to avoid a writer/fence deadlock.
        let bucket_guard = self
            .object_backend
            .acquire_write_lock(table_bucket, &default_table_bucket_publication_lock_path())
            .await?;
        let global_guard = self
            .object_backend
            .acquire_write_lock(
                RUSTFS_META_BUCKET,
                &TableCatalogObjectPaths::default().backing_migration_global_fence_lock_path(),
            )
            .await?;
        Ok((bucket_guard, global_guard))
    }

    async fn backup_object_fingerprint(
        &self,
        bucket: &str,
        object: &str,
        kind: StrongTableCatalogBackupObjectKind,
    ) -> TableCatalogStoreResult<StrongTableCatalogBackupObject> {
        if !backup_object_key_is_safe(object) {
            return Err(TableCatalogStoreError::Invalid(format!("backup object key is invalid: {object}")));
        }
        let metadata = self
            .object_backend
            .object_metadata(bucket, object)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("backup object {bucket}/{object}")))?;
        let sha256 = if metadata.size <= TABLE_CATALOG_BACKUP_HASH_MAX_SIZE {
            let object_data = self
                .object_backend
                .read_object_limited(bucket, object, usize::try_from(TABLE_CATALOG_BACKUP_HASH_MAX_SIZE).unwrap_or(usize::MAX))
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound(format!("backup object {bucket}/{object}")))?;
            if object_data.data.len() as u64 != metadata.size {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "backup object {bucket}/{object} changed while its watermark was collected"
                )));
            }
            Some(backup_sha256(&object_data.data))
        } else if metadata.etag.is_none() {
            return Err(TableCatalogStoreError::Invalid(format!(
                "backup object {bucket}/{object} is too large for a digest and has no etag watermark"
            )));
        } else {
            None
        };
        Ok(StrongTableCatalogBackupObject {
            bucket: bucket.to_string(),
            object: object.to_string(),
            kind,
            size_bytes: metadata.size,
            etag: metadata.etag,
            sha256,
        })
    }

    async fn collect_maintenance_backup_objects(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Vec<StrongTableCatalogBackupMaintenanceObject>> {
        let paths = TableCatalogObjectPaths::default();
        let maintenance_marker = format!("/{MAINTENANCE_ROOT}/");
        let table_bucket_prefix = paths.table_bucket_root_prefix(table_bucket);
        let mut total_bytes = 0usize;
        let mut objects = Vec::new();
        for object in self
            .object_backend
            .list_objects(RUSTFS_META_BUCKET, &table_bucket_prefix)
            .await?
            .into_iter()
            .filter(|object| object.contains(&maintenance_marker))
        {
            if objects.len() >= TABLE_CATALOG_BACKUP_MAX_OBJECTS {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup contains more than {TABLE_CATALOG_BACKUP_MAX_OBJECTS} maintenance objects"
                )));
            }
            let value = self
                .object_backend
                .read_object_limited(RUSTFS_META_BUCKET, &object, TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_OBJECT_SIZE)
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound(format!("maintenance backup object {object}")))?;
            if maintenance_object_is_active(&value.data)? {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance backup requires all scheduler and worker leases to be drained".to_string(),
                ));
            }
            total_bytes = total_bytes.saturating_add(value.data.len());
            if total_bytes > TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_BYTES {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "maintenance backup exceeds the maximum of {TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_BYTES} bytes"
                )));
            }
            objects.push(StrongTableCatalogBackupMaintenanceObject {
                object,
                size_bytes: value.data.len() as u64,
                etag: value.etag,
                sha256: backup_sha256(&value.data),
                data: value.data,
            });
        }
        Ok(objects)
    }

    async fn collect_backup_objects(
        &self,
        snapshot: &StrongTableCatalogBucketSnapshot,
    ) -> TableCatalogStoreResult<Vec<StrongTableCatalogBackupObject>> {
        let mut keys = BTreeMap::<(String, String), StrongTableCatalogBackupObjectKind>::new();
        for entry in &snapshot.tables {
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let table = parse_table_for_store(&entry.table)?;
            if !is_valid_table_metadata_location_for_entry(entry, &entry.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "table backup metadata location is invalid for {}/{}/{}",
                    entry.table_bucket, entry.namespace, entry.table
                )));
            }
            let current_metadata = read_table_metadata_value(&self.object_backend, &entry.table_bucket, &entry.metadata_location)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::NotFound(format!("current metadata object {}", entry.metadata_location))
                })?;
            let mut metadata_locations = metadata_log_locations(&current_metadata, &entry.table_bucket, &namespace, &table);
            metadata_locations.insert(entry.metadata_location.clone());
            for metadata_location in metadata_locations {
                if !is_valid_table_metadata_location_for_entry(entry, &metadata_location) {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "table backup metadata log location is outside the table: {metadata_location}"
                    )));
                }
                keys.insert(
                    (entry.table_bucket.clone(), metadata_location.clone()),
                    StrongTableCatalogBackupObjectKind::TableMetadata,
                );
                let metadata = if metadata_location == entry.metadata_location {
                    current_metadata.clone()
                } else {
                    read_table_metadata_value(&self.object_backend, &entry.table_bucket, &metadata_location)
                        .await?
                        .ok_or_else(|| TableCatalogStoreError::NotFound(format!("metadata log object {metadata_location}")))?
                };
                let warehouse_prefix = table_warehouse_object_prefix(entry)?;
                let referenced = metadata_maintenance_referenced_object_reports(
                    &self.object_backend,
                    &entry.table_bucket,
                    &namespace,
                    &table,
                    Some(warehouse_prefix.as_str()),
                    &metadata,
                    std::slice::from_ref(&metadata_location),
                )
                .await?;
                for report in referenced {
                    if report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired {
                        return Err(TableCatalogStoreError::Conflict(format!(
                            "table backup cannot seal an unresolved object reference: {}",
                            report.object_location
                        )));
                    }
                    keys.insert(
                        (entry.table_bucket.clone(), report.object_location),
                        StrongTableCatalogBackupObjectKind::from_maintenance_kind(&report.object_kind),
                    );
                }
            }
        }
        for entry in &snapshot.views {
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let view = parse_table_for_store(&entry.view)?;
            if !is_valid_view_metadata_location(&namespace, &view, &entry.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "view backup metadata location is invalid for {}/{}/{}",
                    entry.table_bucket, entry.namespace, entry.view
                )));
            }
            keys.insert(
                (entry.table_bucket.clone(), entry.metadata_location.clone()),
                StrongTableCatalogBackupObjectKind::ViewMetadata,
            );
        }
        if keys.len() > TABLE_CATALOG_BACKUP_MAX_OBJECTS {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog backup references more than {TABLE_CATALOG_BACKUP_MAX_OBJECTS} objects"
            )));
        }
        let mut objects = Vec::with_capacity(keys.len());
        for ((bucket, object), kind) in keys {
            objects.push(self.backup_object_fingerprint(&bucket, &object, kind).await?);
        }
        Ok(objects)
    }

    async fn read_backup(&self, table_bucket: &str, backup_id: &str) -> TableCatalogStoreResult<StrongTableCatalogBackup> {
        let path = TableCatalogObjectPaths::default().catalog_backup_path(table_bucket, backup_id);
        let object = self
            .object_backend
            .read_object_limited(RUSTFS_META_BUCKET, &path, TABLE_CATALOG_BACKUP_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup {backup_id}")))?;
        let backup = serde_json::from_slice::<StrongTableCatalogBackup>(&object.data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to decode catalog backup: {err}")))?;
        if backup.version != TABLE_CATALOG_BACKUP_VERSION || backup.backup_id != backup_id || backup.table_bucket != table_bucket
        {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity or version does not match the restore request".to_string(),
            ));
        }
        if backup.snapshot.table_bucket.table_bucket != table_bucket {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup snapshot belongs to a different table bucket".to_string(),
            ));
        }
        if backup.objects.len() > TABLE_CATALOG_BACKUP_MAX_OBJECTS {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog backup references more than {TABLE_CATALOG_BACKUP_MAX_OBJECTS} objects"
            )));
        }
        if backup.maintenance_objects.len() > TABLE_CATALOG_BACKUP_MAX_OBJECTS {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog backup contains more than {TABLE_CATALOG_BACKUP_MAX_OBJECTS} maintenance objects"
            )));
        }
        let fingerprint = table_catalog_bucket_snapshot_fingerprint(&backup.snapshot)?;
        if fingerprint != backup.catalog_fingerprint {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup snapshot fingerprint does not match its payload".to_string(),
            ));
        }
        let expected_backup_id = catalog_backup_id(
            &backup.table_bucket,
            &backup.source_snapshot_etag,
            backup.source_snapshot_version,
            &backup.catalog_fingerprint,
            &backup.objects,
            &backup.maintenance_objects,
        )?;
        if expected_backup_id != backup.backup_id {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup content does not match its backup identity".to_string(),
            ));
        }
        StrongTableCatalogStore::<B>::state_from_snapshot(
            StrongTableCatalogSnapshot {
                version: backup.source_snapshot_version,
                table_buckets: vec![backup.snapshot.table_bucket.clone()],
                namespaces: backup.snapshot.namespaces.clone(),
                tables: backup.snapshot.tables.clone(),
                views: backup.snapshot.views.clone(),
                commits: backup.snapshot.commits.clone(),
                idempotency: backup.snapshot.idempotency.clone(),
            },
            None,
        )?;
        Ok(backup)
    }

    fn validate_maintenance_backup_objects(
        table_bucket: &str,
        objects: &[StrongTableCatalogBackupMaintenanceObject],
    ) -> TableCatalogStoreResult<()> {
        let paths = TableCatalogObjectPaths::default();
        let prefix = paths.table_bucket_root_prefix(table_bucket);
        let marker = format!("/{MAINTENANCE_ROOT}/");
        let mut total_bytes = 0usize;
        for object in objects {
            if !backup_object_key_is_safe(&object.object)
                || !object.object.starts_with(&prefix)
                || !object.object.contains(&marker)
                || object.data.len() > TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_OBJECT_SIZE
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup maintenance object is outside the protected maintenance root: {}",
                    object.object
                )));
            }
            if object.data.len() as u64 != object.size_bytes || backup_sha256(&object.data) != object.sha256 {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup maintenance object checksum is invalid: {}",
                    object.object
                )));
            }
            total_bytes = total_bytes.saturating_add(object.data.len());
            if total_bytes > TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_BYTES {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup exceeds the maximum of {TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_BYTES} maintenance bytes"
                )));
            }
        }
        Ok(())
    }

    async fn verify_backup_objects(&self, backup: &StrongTableCatalogBackup) -> TableCatalogStoreResult<()> {
        for object in &backup.objects {
            if !backup_object_key_is_safe(&object.object) || object.bucket != backup.table_bucket {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup contains an object outside its protected buckets: {}/{}",
                    object.bucket, object.object
                )));
            }
            if object.size_bytes <= TABLE_CATALOG_BACKUP_HASH_MAX_SIZE && object.sha256.is_none() {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup is missing a content watermark: {}/{}",
                    object.bucket, object.object
                )));
            }
            let metadata = self
                .object_backend
                .object_metadata(&object.bucket, &object.object)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::Conflict(format!(
                        "catalog backup object is missing: {}/{}",
                        object.bucket, object.object
                    ))
                })?;
            if metadata.size != object.size_bytes || metadata.etag != object.etag {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog backup object watermark changed: {}/{}",
                    object.bucket, object.object
                )));
            }
            if let Some(expected_sha256) = object.sha256.as_deref() {
                let data = self
                    .object_backend
                    .read_object_limited(
                        &object.bucket,
                        &object.object,
                        usize::try_from(TABLE_CATALOG_BACKUP_HASH_MAX_SIZE).unwrap_or(usize::MAX),
                    )
                    .await?
                    .ok_or_else(|| {
                        TableCatalogStoreError::Conflict(format!("catalog backup object disappeared: {}", object.object))
                    })?;
                if backup_sha256(&data.data) != expected_sha256 {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog backup object checksum changed: {}/{}",
                        object.bucket, object.object
                    )));
                }
            }
        }
        Ok(())
    }

    async fn read_restore_intent(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Option<(StrongTableCatalogRestoreIntent, Option<String>)>> {
        let path = TableCatalogObjectPaths::default().catalog_backup_restore_intent_path(table_bucket);
        let Some(object) = self
            .object_backend
            .read_object_limited(RUSTFS_META_BUCKET, &path, 64 * 1024)
            .await?
        else {
            return Ok(None);
        };
        let intent = serde_json::from_slice::<StrongTableCatalogRestoreIntent>(&object.data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to decode catalog restore intent: {err}")))?;
        if intent.version != TABLE_CATALOG_BACKUP_VERSION || intent.table_bucket != table_bucket {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore intent has an invalid identity or version".to_string(),
            ));
        }
        Ok(Some((intent, object.etag)))
    }

    async fn write_restore_intent(
        &self,
        table_bucket: &str,
        intent: &StrongTableCatalogRestoreIntent,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        let data = serde_json::to_vec(intent)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode catalog restore intent: {err}")))?;
        self.object_backend
            .put_object(
                RUSTFS_META_BUCKET,
                &TableCatalogObjectPaths::default().catalog_backup_restore_intent_path(table_bucket),
                data,
                precondition,
            )
            .await
    }

    async fn restore_maintenance_objects(
        &self,
        table_bucket: &str,
        objects: &[StrongTableCatalogBackupMaintenanceObject],
    ) -> TableCatalogStoreResult<()> {
        Self::validate_maintenance_backup_objects(table_bucket, objects)?;
        let paths = TableCatalogObjectPaths::default();
        let prefix = paths.table_bucket_root_prefix(table_bucket);
        let marker = format!("/{MAINTENANCE_ROOT}/");
        for object in self
            .object_backend
            .list_objects(RUSTFS_META_BUCKET, &prefix)
            .await?
            .into_iter()
            .filter(|object| object.contains(&marker))
        {
            self.object_backend.delete_object(RUSTFS_META_BUCKET, &object).await?;
        }
        for object in objects {
            self.object_backend
                .put_object(RUSTFS_META_BUCKET, &object.object, object.data.clone(), TableCatalogPutPrecondition::Any)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn create_durable_catalog_backup(
        &self,
        table_bucket: &str,
        expected_snapshot_etag: Option<&str>,
    ) -> TableCatalogStoreResult<TableCatalogBackupReport> {
        let (bucket_guard, global_guard) = self.acquire_backup_fence(table_bucket).await?;
        Self::ensure_backup_fence_held(&bucket_guard, &global_guard)?;
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let (snapshot, source_snapshot_etag, source_snapshot_version) = {
            let state = self.state.lock().await;
            let current_etag = state
                .snapshot_etag
                .clone()
                .ok_or_else(|| TableCatalogStoreError::Internal("durable strong catalog snapshot has no etag".to_string()))?;
            if expected_snapshot_etag.is_some_and(|expected| expected != current_etag) {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog snapshot changed before backup capture".to_string(),
                ));
            }
            let snapshot = StrongTableCatalogStore::<B>::bucket_snapshot_from_state_locked(&state, table_bucket)
                .ok_or_else(|| TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")))?;
            if snapshot.table_bucket.state != TableCatalogEntryState::Active || snapshot.table_bucket.active_rename_id.is_some() {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog backup requires an active table bucket without a pending rename".to_string(),
                ));
            }
            for entry in &snapshot.tables {
                if entry.state == TableCatalogEntryState::Active {
                    let recovery = StrongTableCatalogStore::<B>::table_commit_recovery_report_for_entry_locked(&state, entry);
                    if recovery.staged_before_table_update_count > 0
                        || recovery.finalization_required_count > 0
                        || recovery.idempotency_repair_required_count > 0
                        || recovery.manual_review_count > 0
                    {
                        return Err(TableCatalogStoreError::Conflict(format!(
                            "table backup requires commit recovery before sealing table {}",
                            entry.table
                        )));
                    }
                }
            }
            let version = state
                .snapshot_version
                .ok_or_else(|| TableCatalogStoreError::Internal("durable strong catalog snapshot has no version".to_string()))?;
            (snapshot, current_etag, version)
        };
        let catalog_fingerprint = table_catalog_bucket_snapshot_fingerprint(&snapshot)?;
        let objects = self.collect_backup_objects(&snapshot).await?;
        let maintenance_objects = self.collect_maintenance_backup_objects(table_bucket).await?;
        Self::ensure_backup_fence_held(&bucket_guard, &global_guard)?;
        let backup_id = catalog_backup_id(
            table_bucket,
            &source_snapshot_etag,
            source_snapshot_version,
            &catalog_fingerprint,
            &objects,
            &maintenance_objects,
        )?;
        let created_at = maintenance_timestamp(OffsetDateTime::now_utc());
        let backup = StrongTableCatalogBackup {
            version: TABLE_CATALOG_BACKUP_VERSION,
            backup_id: backup_id.clone(),
            table_bucket: table_bucket.to_string(),
            created_at: created_at.clone(),
            source_snapshot_etag: source_snapshot_etag.clone(),
            source_snapshot_version,
            catalog_fingerprint: catalog_fingerprint.clone(),
            snapshot,
            objects,
            maintenance_objects,
        };
        let data = serde_json::to_vec(&backup)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode catalog backup: {err}")))?;
        if data.len() > TABLE_CATALOG_BACKUP_MAX_SIZE {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog backup exceeds the maximum encoded size of {TABLE_CATALOG_BACKUP_MAX_SIZE} bytes"
            )));
        }
        let path = TableCatalogObjectPaths::default().catalog_backup_path(table_bucket, &backup_id);
        Self::ensure_backup_fence_held(&bucket_guard, &global_guard)?;
        let (status, persisted_created_at) = match self
            .object_backend
            .put_object(RUSTFS_META_BUCKET, &path, data, TableCatalogPutPrecondition::IfAbsent)
            .await
        {
            Ok(()) => (TableCatalogBackupStatus::Created, created_at.clone()),
            Err(TableCatalogStoreError::Conflict(_)) => {
                let existing = self
                    .object_backend
                    .read_object_limited(RUSTFS_META_BUCKET, &path, TABLE_CATALOG_BACKUP_MAX_SIZE)
                    .await?
                    .ok_or_else(|| {
                        TableCatalogStoreError::Conflict("catalog backup disappeared after a write conflict".to_string())
                    })?;
                let existing = serde_json::from_slice::<StrongTableCatalogBackup>(&existing.data)
                    .map_err(|err| TableCatalogStoreError::Invalid(format!("existing catalog backup is invalid: {err}")))?;
                let mut comparable = backup.clone();
                comparable.created_at = existing.created_at.clone();
                if existing != comparable {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup identity is already occupied by different content".to_string(),
                    ));
                }
                (TableCatalogBackupStatus::AlreadyPresent, existing.created_at)
            }
            Err(err) => return Err(err),
        };
        let verified_bytes = backup
            .objects
            .iter()
            .fold(0u64, |total, object| total.saturating_add(object.size_bytes));
        Ok(TableCatalogBackupReport {
            table_bucket: table_bucket.to_string(),
            backup_id,
            status,
            backup_path: path,
            source_snapshot_etag,
            source_snapshot_version,
            catalog_fingerprint,
            object_count: backup.objects.len(),
            verified_object_count: backup.objects.len(),
            verified_bytes,
            maintenance_object_count: backup.maintenance_objects.len(),
            created_at: persisted_created_at,
        })
    }

    pub(crate) async fn restore_durable_catalog_backup(
        &self,
        table_bucket: &str,
        backup_id: &str,
        expected_snapshot_etag: Option<&str>,
        allow_replace: bool,
    ) -> TableCatalogStoreResult<TableCatalogRestoreReport> {
        let (bucket_guard, global_guard) = self.acquire_backup_fence(table_bucket).await?;
        Self::ensure_backup_fence_held(&bucket_guard, &global_guard)?;
        let _write_guard = self.write_lock.lock().await;
        let backup = self.read_backup(table_bucket, backup_id).await?;
        self.verify_backup_objects(&backup).await?;
        let current_objects = self.collect_backup_objects(&backup.snapshot).await?;
        if current_objects != backup.objects {
            return Err(TableCatalogStoreError::Conflict(
                "catalog backup object graph no longer matches the saved catalog snapshot".to_string(),
            ));
        }
        Self::validate_maintenance_backup_objects(table_bucket, &backup.maintenance_objects)?;

        self.hydrate_state().await?;
        let intent_path = TableCatalogObjectPaths::default().catalog_backup_restore_intent_path(table_bucket);
        let existing_intent = self.read_restore_intent(table_bucket).await?;
        if let Some((intent, _)) = existing_intent.as_ref()
            && intent.backup_id != backup_id
        {
            return Err(TableCatalogStoreError::Conflict(
                "another catalog restore is already pending for this table bucket".to_string(),
            ));
        }
        let (current_snapshot_etag, current_bucket) = {
            let state = self.state.lock().await;
            (
                state.snapshot_etag.clone(),
                StrongTableCatalogStore::<B>::bucket_snapshot_from_state_locked(&state, table_bucket),
            )
        };
        if expected_snapshot_etag.is_some_and(|expected| current_snapshot_etag.as_deref() != Some(expected)) {
            return Err(TableCatalogStoreError::Conflict(
                "target catalog snapshot does not match expected-snapshot-etag".to_string(),
            ));
        }
        let already_restored = current_bucket.as_ref() == Some(&backup.snapshot);
        if !already_restored && current_bucket.is_some() && !allow_replace {
            return Err(TableCatalogStoreError::Conflict(
                "restoring over an existing table bucket requires allow-replace".to_string(),
            ));
        }
        if !already_restored && current_bucket.is_none() && expected_snapshot_etag.is_some() {
            return Err(TableCatalogStoreError::Conflict(
                "expected-snapshot-etag cannot be used when the target table bucket is absent".to_string(),
            ));
        }

        let mut restored_snapshot_etag = current_snapshot_etag.clone();
        if !already_restored {
            let intent = StrongTableCatalogRestoreIntent {
                version: TABLE_CATALOG_BACKUP_VERSION,
                table_bucket: table_bucket.to_string(),
                backup_id: backup_id.to_string(),
                expected_snapshot_etag: expected_snapshot_etag.map(str::to_string),
                restored_snapshot_etag: None,
                state: StrongTableCatalogRestoreIntentState::Prepared,
            };
            if existing_intent.is_none() {
                self.write_restore_intent(table_bucket, &intent, TableCatalogPutPrecondition::IfAbsent)
                    .await?;
            }
            let (snapshot, precondition) = {
                let state = self.state.lock().await;
                let precondition = state
                    .snapshot_etag
                    .clone()
                    .map_or(TableCatalogPutPrecondition::IfAbsent, TableCatalogPutPrecondition::IfMatch);
                let mut draft_state = state.clone();
                StrongTableCatalogStore::<B>::remove_bucket_from_state_locked(&mut draft_state, table_bucket);
                StrongTableCatalogStore::<B>::insert_bucket_snapshot_locked(&mut draft_state, backup.snapshot.clone())?;
                let snapshot = StrongTableCatalogStore::<B>::snapshot_from_mutated_state_locked(
                    &mut draft_state,
                    self.snapshot_write_version,
                )?;
                (snapshot, precondition)
            };
            Self::ensure_backup_fence_held(&bucket_guard, &global_guard)?;
            self.verify_backup_objects(&backup).await?;
            self.finalize_snapshot_write(
                snapshot,
                precondition,
                StrongSnapshotWritePostcondition::BucketSnapshotPresent(backup.snapshot.clone()),
            )
            .await?;
            self.hydrate_state().await?;
            let restored_etag = self
                .state
                .lock()
                .await
                .snapshot_etag
                .clone()
                .ok_or_else(|| TableCatalogStoreError::Internal("restored catalog snapshot has no etag".to_string()))?;
            restored_snapshot_etag = Some(restored_etag.clone());
            let intent = StrongTableCatalogRestoreIntent {
                version: TABLE_CATALOG_BACKUP_VERSION,
                table_bucket: table_bucket.to_string(),
                backup_id: backup_id.to_string(),
                expected_snapshot_etag: expected_snapshot_etag.map(str::to_string),
                restored_snapshot_etag: Some(restored_etag),
                state: StrongTableCatalogRestoreIntentState::CatalogApplied,
            };
            Self::ensure_backup_fence_held(&bucket_guard, &global_guard)?;
            self.write_restore_intent(table_bucket, &intent, TableCatalogPutPrecondition::Any)
                .await?;
        } else if restored_snapshot_etag.is_none() {
            return Err(TableCatalogStoreError::Internal(
                "already-restored catalog snapshot has no etag".to_string(),
            ));
        }

        Self::ensure_backup_fence_held(&bucket_guard, &global_guard)?;
        self.restore_maintenance_objects(table_bucket, &backup.maintenance_objects)
            .await?;
        self.object_backend.delete_object(RUSTFS_META_BUCKET, &intent_path).await?;
        Ok(TableCatalogRestoreReport {
            table_bucket: table_bucket.to_string(),
            backup_id: backup_id.to_string(),
            status: if already_restored {
                TableCatalogRestoreStatus::AlreadyRestored
            } else {
                TableCatalogRestoreStatus::Restored
            },
            previous_snapshot_etag: current_snapshot_etag,
            restored_snapshot_etag: restored_snapshot_etag
                .ok_or_else(|| TableCatalogStoreError::Internal("restored catalog snapshot etag disappeared".to_string()))?,
            object_count: backup.objects.len(),
            maintenance_object_count: backup.maintenance_objects.len(),
        })
    }
}
