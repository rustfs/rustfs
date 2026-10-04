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

use super::object::validate_table_maintenance_report_owner;
use super::strong::{
    StrongSnapshotWritePostcondition, StrongTableCatalogBucketSnapshot, StrongTableCatalogSnapshot, StrongTableCatalogStore,
    table_catalog_bucket_snapshot_fingerprint,
};
use super::*;
use std::collections::{BTreeMap, BTreeSet};

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
    TableStatistics,
    PartitionStatistics,
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
    #[serde(default)]
    target_snapshot_etag: Option<String>,
    #[serde(default = "missing_restore_target_bucket_fingerprint")]
    target_bucket_fingerprint: String,
    #[serde(default)]
    target_bucket_present: Option<bool>,
    #[serde(default)]
    allow_replace: Option<bool>,
    #[serde(default)]
    source_maintenance_fingerprint: Option<String>,
    restored_snapshot_etag: Option<String>,
    state: StrongTableCatalogRestoreIntentState,
}

fn missing_restore_target_bucket_fingerprint() -> String {
    String::new()
}

const ABSENT_RESTORE_TARGET_BUCKET_FINGERPRINT: &str = "ABSENT";

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

    fn from_statistics_kind(kind: IcebergStatisticsFileKind) -> Self {
        match kind {
            IcebergStatisticsFileKind::Table => Self::TableStatistics,
            IcebergStatisticsFileKind::Partition => Self::PartitionStatistics,
        }
    }
}

fn backup_sha256(data: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(data), hex_simd::AsciiCase::Lower)
}

fn has_usable_etag(etag: Option<&str>) -> bool {
    etag.is_some_and(|etag| !etag.is_empty())
}

fn catalog_backup_id(
    table_bucket: &str,
    backup_version: u16,
    source_snapshot_etag: &str,
    source_snapshot_version: u16,
    catalog_fingerprint: &str,
    objects: &[StrongTableCatalogBackupObject],
    maintenance_objects: &[StrongTableCatalogBackupMaintenanceObject],
) -> TableCatalogStoreResult<String> {
    let payload = serde_json::to_vec(&(
        table_bucket,
        backup_version,
        source_snapshot_etag,
        source_snapshot_version,
        catalog_fingerprint,
        objects,
        maintenance_objects,
    ))
    .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode backup identity: {err}")))?;
    Ok(backup_sha256(&payload))
}

fn maintenance_objects_fingerprint(objects: &[StrongTableCatalogBackupMaintenanceObject]) -> TableCatalogStoreResult<String> {
    let mut identity = objects
        .iter()
        .map(|object| (object.object.as_str(), object.size_bytes, object.sha256.as_str()))
        .collect::<Vec<_>>();
    identity.sort_unstable();
    let payload = serde_json::to_vec(&identity)
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode maintenance backup identity: {err}")))?;
    Ok(backup_sha256(&payload))
}

fn backup_object_key_is_safe(object: &str) -> bool {
    !object.is_empty()
        && !object.starts_with('/')
        && !object.contains("..")
        && !object.contains('\\')
        && !object.bytes().any(|byte| byte.is_ascii_control())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaintenanceBackupObjectType {
    Config,
    Job,
}

#[derive(Debug)]
struct MaintenanceBackupObjectPath {
    object_type: MaintenanceBackupObjectType,
    namespace: Option<Namespace>,
    table: Option<IdentifierSegment>,
    table_id_hash: Option<String>,
    job_id_hash: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct MaintenanceBackupPaths {
    exact_paths: BTreeSet<String>,
    config_paths: BTreeSet<String>,
    job_prefixes: BTreeSet<String>,
    allow_all_table_paths: bool,
}

impl MaintenanceBackupPaths {
    fn extend(&mut self, other: Self) {
        self.exact_paths.extend(other.exact_paths);
        self.config_paths.extend(other.config_paths);
        self.job_prefixes.extend(other.job_prefixes);
        self.allow_all_table_paths |= other.allow_all_table_paths;
    }
}

fn is_table_catalog_path_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn maintenance_backup_object_path(table_bucket: &str, object: &str) -> Option<MaintenanceBackupObjectPath> {
    let root = TableCatalogObjectPaths::default().table_bucket_root_prefix(table_bucket);
    let relative = object.strip_prefix(&root)?;
    if relative == format!("{MAINTENANCE_ROOT}/{MAINTENANCE_CONFIG_FILE}") {
        return Some(MaintenanceBackupObjectPath {
            object_type: MaintenanceBackupObjectType::Config,
            namespace: None,
            table: None,
            table_id_hash: None,
            job_id_hash: None,
        });
    }

    let relative = relative.strip_prefix(&format!("{NAMESPACE_ROOT}/"))?;
    let (namespace_storage_id, table_path) = relative.rsplit_once(&format!("/{TABLE_ROOT}/"))?;
    let namespace = Namespace::from_segments(namespace_storage_id.split('/').map(str::to_string).collect()).ok()?;
    let (table_name, maintenance_path) = table_path.split_once(&format!("/{MAINTENANCE_ROOT}/"))?;
    let table = IdentifierSegment::parse(table_name.to_string()).ok()?;
    let mut segments = maintenance_path.split('/');
    let table_id_hash = segments.next()?.to_string();
    if !is_table_catalog_path_hash(&table_id_hash) {
        return None;
    }
    match (segments.next(), segments.next(), segments.next()) {
        (Some(file), None, None)
            if matches!(file, MAINTENANCE_CONFIG_FILE | MAINTENANCE_LATEST_JOB_FILE | MAINTENANCE_CURRENT_JOB_FILE) =>
        {
            Some(MaintenanceBackupObjectPath {
                object_type: if file == MAINTENANCE_CONFIG_FILE {
                    MaintenanceBackupObjectType::Config
                } else {
                    MaintenanceBackupObjectType::Job
                },
                namespace: Some(namespace),
                table: Some(table),
                table_id_hash: Some(table_id_hash),
                job_id_hash: None,
            })
        }
        (Some(MAINTENANCE_JOB_ROOT), Some(job_file), None) => {
            let job_hash = job_file.strip_suffix(".json")?;
            is_table_catalog_path_hash(job_hash).then_some(MaintenanceBackupObjectPath {
                object_type: MaintenanceBackupObjectType::Job,
                namespace: Some(namespace),
                table: Some(table),
                table_id_hash: Some(table_id_hash),
                job_id_hash: Some(job_hash.to_string()),
            })
        }
        _ => None,
    }
}

fn maintenance_backup_structural_object_type(table_bucket: &str, object: &str) -> Option<MaintenanceBackupObjectType> {
    maintenance_backup_object_path(table_bucket, object).map(|path| path.object_type)
}

fn maintenance_backup_object_type(
    table_bucket: &str,
    object: &str,
    paths: &MaintenanceBackupPaths,
) -> Option<MaintenanceBackupObjectType> {
    if paths.config_paths.contains(object) {
        return Some(MaintenanceBackupObjectType::Config);
    }
    if paths.exact_paths.contains(object)
        || paths.job_prefixes.iter().any(|prefix| {
            object
                .strip_prefix(prefix)
                .and_then(|suffix| suffix.strip_suffix(".json"))
                .is_some_and(is_table_catalog_path_hash)
        })
    {
        return Some(MaintenanceBackupObjectType::Job);
    }
    paths
        .allow_all_table_paths
        .then(|| maintenance_backup_structural_object_type(table_bucket, object))
        .flatten()
}

fn maintenance_restore_paths() -> MaintenanceBackupPaths {
    MaintenanceBackupPaths {
        allow_all_table_paths: true,
        ..Default::default()
    }
}

fn maintenance_backup_paths(
    table_bucket: &str,
    snapshot: Option<&StrongTableCatalogBucketSnapshot>,
) -> TableCatalogStoreResult<MaintenanceBackupPaths> {
    let paths = TableCatalogObjectPaths::default();
    let bucket_config_path = paths.table_bucket_maintenance_config_path(table_bucket);
    let mut result = MaintenanceBackupPaths {
        exact_paths: BTreeSet::from([bucket_config_path.clone()]),
        config_paths: BTreeSet::from([bucket_config_path]),
        job_prefixes: BTreeSet::new(),
        allow_all_table_paths: false,
    };
    if let Some(snapshot) = snapshot {
        for entry in &snapshot.tables {
            if entry.table_bucket != table_bucket {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup maintenance snapshot contains table {} from another table bucket",
                    entry.table
                )));
            }
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let table = parse_table_for_store(&entry.table)?;
            let config_path = paths.table_maintenance_config_path(table_bucket, &namespace, &table, &entry.table_id);
            result.exact_paths.insert(config_path.clone());
            result.config_paths.insert(config_path);
            result
                .exact_paths
                .insert(paths.table_maintenance_latest_job_path(table_bucket, &namespace, &table, &entry.table_id));
            result.exact_paths.insert(paths.table_maintenance_current_job_path(
                table_bucket,
                &namespace,
                &table,
                &entry.table_id,
            ));
            result
                .job_prefixes
                .insert(paths.table_maintenance_jobs_prefix(table_bucket, &namespace, &table, &entry.table_id));
        }
    }
    Ok(result)
}

fn maintenance_backup_object_key_is_safe(table_bucket: &str, object: &str, paths: &MaintenanceBackupPaths) -> bool {
    backup_object_key_is_safe(object) && maintenance_backup_object_type(table_bucket, object, paths).is_some()
}

fn maintenance_object_is_active(
    table_bucket: &str,
    object: &str,
    data: &[u8],
    paths: &MaintenanceBackupPaths,
) -> TableCatalogStoreResult<bool> {
    let object_type = maintenance_backup_object_type(table_bucket, object, paths)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("maintenance backup object has an invalid path: {object}")))?;
    if object_type == MaintenanceBackupObjectType::Config {
        let config = serde_json::from_slice::<TableMaintenanceConfig>(data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("maintenance backup config is not valid: {object}: {err}")))?;
        validate_table_maintenance_config(&config)?;
        return Ok(false);
    }
    let path = maintenance_backup_object_path(table_bucket, object)
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("maintenance backup report has an invalid path: {object}")))?;
    let namespace = path
        .namespace
        .as_ref()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("maintenance backup report has no table owner: {object}")))?;
    let table = path
        .table
        .as_ref()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("maintenance backup report has no table owner: {object}")))?;
    let table_id_hash = path
        .table_id_hash
        .as_deref()
        .ok_or_else(|| TableCatalogStoreError::Invalid(format!("maintenance backup report has no table identity: {object}")))?;
    let report = serde_json::from_slice::<TableMetadataMaintenanceReport>(data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("maintenance backup report is not valid: {object}: {err}")))?;
    validate_table_maintenance_report_owner(&report, table_bucket, namespace, table, &report.job.table_id)?;
    if table_catalog_path_hash(&report.job.table_id) != table_id_hash {
        return Err(TableCatalogStoreError::Invalid(format!(
            "maintenance backup report table identity does not match its object path: {object}"
        )));
    }
    if let Some(job_id_hash) = path.job_id_hash.as_deref()
        && table_catalog_path_hash(&report.job.job_id) != job_id_hash
    {
        return Err(TableCatalogStoreError::Invalid(format!(
            "maintenance backup report job identity does not match its object path: {object}"
        )));
    }
    Ok(matches!(
        report.job.status,
        TableMetadataMaintenanceJobStatus::Queued | TableMetadataMaintenanceJobStatus::Running
    ))
}

fn backup_metadata_log_locations(metadata: &serde_json::Value, entry: &TableEntry) -> TableCatalogStoreResult<BTreeSet<String>> {
    let Some(metadata_log) = metadata.get("metadata-log") else {
        return Ok(BTreeSet::new());
    };
    let entries = metadata_log
        .as_array()
        .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata-log must be an array for a durable backup".to_string()))?;
    let mut locations = BTreeSet::new();
    for (index, metadata_log_entry) in entries.iter().enumerate() {
        let location = metadata_log_entry
            .get("metadata-file")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!(
                    "table metadata-log entry {index} must identify a metadata-file for a durable backup"
                ))
            })?;
        let object = table_catalog_object_key_from_location(&entry.table_bucket, location).ok_or_else(|| {
            TableCatalogStoreError::Invalid(format!(
                "table metadata-log entry {index} points outside table bucket {}",
                entry.table_bucket
            ))
        })?;
        if !is_valid_table_metadata_location_for_entry(entry, &object) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "table metadata-log entry {index} points outside the table metadata directory: {object}"
            )));
        }
        locations.insert(object);
    }
    Ok(locations)
}

fn validate_backup_table_metadata(
    entry: &TableEntry,
    metadata_location: &str,
    metadata: &serde_json::Value,
    current: bool,
) -> TableCatalogStoreResult<()> {
    validate_supported_table_metadata(metadata)?;
    if table_metadata_uuid(metadata)? != entry.table_uuid {
        return Err(TableCatalogStoreError::Invalid(format!(
            "table metadata UUID does not match catalog entry for {}/{}/{}",
            entry.table_bucket, entry.namespace, entry.table
        )));
    }
    let location = table_metadata_location(metadata)?;
    validate_table_warehouse_location(&entry.table_bucket, location)?;
    if current && location != entry.warehouse_location {
        return Err(TableCatalogStoreError::Invalid(format!(
            "current table metadata location does not match catalog warehouse location for {}/{}/{}",
            entry.table_bucket, entry.namespace, entry.table
        )));
    }
    if current && table_metadata_format_version(metadata)? != entry.format_version {
        return Err(TableCatalogStoreError::Invalid(format!(
            "current table metadata format version does not match catalog entry for {}/{}/{}",
            entry.table_bucket, entry.namespace, entry.table
        )));
    }
    if !is_valid_table_metadata_location_for_entry(entry, metadata_location) {
        return Err(TableCatalogStoreError::Invalid(format!(
            "table metadata object is outside the catalog metadata directory: {metadata_location}"
        )));
    }
    Ok(())
}

fn validate_backup_view_metadata(
    entry: &ViewEntry,
    namespace: &Namespace,
    view: &IdentifierSegment,
    metadata_location: &str,
    metadata: &serde_json::Value,
) -> TableCatalogStoreResult<()> {
    validate_supported_view_metadata(metadata)?;
    let view_uuid = metadata
        .get("view-uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|uuid| !uuid.is_empty())
        .ok_or_else(|| TableCatalogStoreError::Invalid("view metadata is missing view-uuid".to_string()))?;
    if view_uuid != entry.view_uuid {
        return Err(TableCatalogStoreError::Invalid(format!(
            "view metadata UUID does not match catalog entry for {}/{}/{}",
            entry.table_bucket, entry.namespace, entry.view
        )));
    }
    let location = metadata
        .get("location")
        .and_then(serde_json::Value::as_str)
        .filter(|location| !location.is_empty())
        .ok_or_else(|| TableCatalogStoreError::Invalid("view metadata is missing location".to_string()))?;
    validate_view_warehouse_location(&entry.table_bucket, location)?;
    if location != entry.warehouse_location {
        return Err(TableCatalogStoreError::Invalid(format!(
            "current view metadata location does not match catalog warehouse location for {}/{}/{}",
            entry.table_bucket, entry.namespace, entry.view
        )));
    }
    let format_version = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| TableCatalogStoreError::Invalid("view metadata is missing format-version".to_string()))?;
    if format_version != i64::from(entry.format_version) {
        return Err(TableCatalogStoreError::Invalid(format!(
            "view metadata format version does not match catalog entry for {}/{}/{}",
            entry.table_bucket, entry.namespace, entry.view
        )));
    }
    if !is_valid_view_metadata_location(namespace, view, metadata_location) {
        return Err(TableCatalogStoreError::Invalid(format!(
            "view metadata object is outside the catalog metadata directory: {metadata_location}"
        )));
    }
    Ok(())
}

fn backup_metadata_graph_context(
    entry: &TableEntry,
    metadata: &serde_json::Value,
) -> TableCatalogStoreResult<(TableEntry, String)> {
    let mut metadata_entry = entry.clone();
    metadata_entry.warehouse_location = table_metadata_location(metadata)?.to_string();
    let warehouse_prefix = table_warehouse_object_prefix(&metadata_entry)?;
    Ok((metadata_entry, warehouse_prefix))
}

fn validate_backup_metadata_warehouse_owner(
    snapshot: &StrongTableCatalogBucketSnapshot,
    entry: &TableEntry,
    warehouse_prefix: &str,
) -> TableCatalogStoreResult<()> {
    for other in &snapshot.tables {
        if other.state != TableCatalogEntryState::Active || other.table_id == entry.table_id {
            continue;
        }
        let other_prefix = table_warehouse_object_prefix(other)?;
        if warehouse_object_prefixes_overlap(warehouse_prefix, &other_prefix) {
            return Err(TableCatalogStoreError::Conflict(format!(
                "table backup metadata warehouse overlaps another active table: {warehouse_prefix}"
            )));
        }
    }
    Ok(())
}

fn insert_backup_object_kind(
    keys: &mut BTreeMap<(String, String), StrongTableCatalogBackupObjectKind>,
    bucket: &str,
    object: &str,
    kind: StrongTableCatalogBackupObjectKind,
) -> TableCatalogStoreResult<()> {
    let key = (bucket.to_string(), object.to_string());
    if let Some(existing) = keys.get(&key) {
        if existing != &kind {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog backup object has conflicting kinds: {bucket}/{object}"
            )));
        }
        return Ok(());
    }
    keys.insert(key, kind);
    Ok(())
}

struct StrongTableCatalogBackupFence {
    table_bucket_publication: TableCatalogLockGuard,
    table_bucket_migration: TableCatalogLockGuard,
    global_migration: TableCatalogLockGuard,
}

impl StrongTableCatalogBackupFence {
    fn ensure_held(&self) -> TableCatalogStoreResult<()> {
        if self.table_bucket_publication.is_lock_lost()
            || self.global_migration.is_lock_lost()
            || self.table_bucket_migration.is_lock_lost()
        {
            return Err(TableCatalogStoreError::Unavailable(
                "catalog backup or restore lost its fencing lock".to_string(),
            ));
        }
        Ok(())
    }

    fn mutation_fence(&self) -> TableCatalogObjectMutationFence {
        TableCatalogObjectMutationFence::from_signals([
            self.table_bucket_publication.lock_lost_signal(),
            self.global_migration.lock_lost_signal(),
            self.table_bucket_migration.lock_lost_signal(),
        ])
    }
}

impl<B> StrongTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    async fn acquire_backup_fence(&self, table_bucket: &str) -> TableCatalogStoreResult<StrongTableCatalogBackupFence> {
        // Keep global migration -> per-bucket migration -> publication ordering.
        let global_migration = self
            .object_backend
            .acquire_write_lock(
                RUSTFS_META_BUCKET,
                &TableCatalogObjectPaths::default().backing_migration_global_fence_lock_path(),
            )
            .await?;
        let table_bucket_migration = self
            .object_backend
            .acquire_write_lock(
                RUSTFS_META_BUCKET,
                &TableCatalogObjectPaths::default().backing_migration_fence_lock_path(table_bucket),
            )
            .await?;
        let table_bucket_publication = self
            .object_backend
            .acquire_write_lock(table_bucket, &default_table_bucket_publication_lock_path())
            .await?;
        Ok(StrongTableCatalogBackupFence {
            table_bucket_publication,
            global_migration,
            table_bucket_migration,
        })
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
            if object_data.etag != metadata.etag {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "backup object {bucket}/{object} changed while its watermark was collected"
                )));
            }
            Some(backup_sha256(&object_data.data))
        } else if !has_usable_etag(metadata.etag.as_deref()) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "backup object {bucket}/{object} is too large for a digest and has no etag watermark"
            )));
        } else {
            None
        };
        let final_metadata = self.object_backend.object_metadata(bucket, object).await?.ok_or_else(|| {
            TableCatalogStoreError::Conflict(format!(
                "backup object {bucket}/{object} disappeared while its watermark was collected"
            ))
        })?;
        if final_metadata.size != metadata.size || final_metadata.etag != metadata.etag {
            return Err(TableCatalogStoreError::Conflict(format!(
                "backup object {bucket}/{object} changed while its watermark was collected"
            )));
        }
        Ok(StrongTableCatalogBackupObject {
            bucket: bucket.to_string(),
            object: object.to_string(),
            kind,
            size_bytes: metadata.size,
            etag: metadata.etag,
            sha256,
        })
    }

    async fn collect_maintenance_objects(
        &self,
        table_bucket: &str,
        allowed_paths: &MaintenanceBackupPaths,
    ) -> TableCatalogStoreResult<Vec<StrongTableCatalogBackupMaintenanceObject>> {
        let object_paths = TableCatalogObjectPaths::default();
        let maintenance_marker = format!("/{MAINTENANCE_ROOT}/");
        let table_bucket_prefix = object_paths.table_bucket_root_prefix(table_bucket);
        let mut total_bytes = 0usize;
        let mut objects = Vec::new();
        for object in self
            .object_backend
            .list_objects(RUSTFS_META_BUCKET, &table_bucket_prefix)
            .await?
            .into_iter()
        {
            if !object.contains(&maintenance_marker) {
                continue;
            }
            if !maintenance_backup_object_key_is_safe(table_bucket, &object, allowed_paths) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup maintenance object is outside the catalog snapshot paths: {object}"
                )));
            }
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
            let etag = value.etag.clone().filter(|etag| !etag.is_empty()).ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!(
                    "maintenance backup object {object} has no etag watermark required for conditional restore"
                ))
            })?;
            let final_metadata = self
                .object_backend
                .object_metadata(RUSTFS_META_BUCKET, &object)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::Conflict(format!(
                        "maintenance backup object {object} disappeared while its watermark was collected"
                    ))
                })?;
            if final_metadata.size != value.data.len() as u64 || final_metadata.etag.as_deref() != Some(etag.as_str()) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "maintenance backup object {object} changed while its watermark was collected"
                )));
            }
            if maintenance_object_is_active(table_bucket, &object, &value.data, allowed_paths)? {
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
                etag: Some(etag),
                sha256: backup_sha256(&value.data),
                data: value.data,
            });
        }
        objects.sort_by(|left, right| left.object.cmp(&right.object));
        Ok(objects)
    }

    async fn collect_maintenance_backup_objects(
        &self,
        table_bucket: &str,
        snapshot: Option<&StrongTableCatalogBucketSnapshot>,
    ) -> TableCatalogStoreResult<Vec<StrongTableCatalogBackupMaintenanceObject>> {
        let paths = maintenance_backup_paths(table_bucket, snapshot)?;
        self.collect_maintenance_objects(table_bucket, &paths).await
    }

    async fn collect_backup_objects(
        &self,
        snapshot: &StrongTableCatalogBucketSnapshot,
    ) -> TableCatalogStoreResult<Vec<StrongTableCatalogBackupObject>> {
        let mut keys = BTreeMap::<(String, String), StrongTableCatalogBackupObjectKind>::new();
        for entry in &snapshot.tables {
            match &entry.state {
                TableCatalogEntryState::Deleted => continue,
                TableCatalogEntryState::Renaming | TableCatalogEntryState::Deleting => {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog backup requires table {} to leave its transient state before sealing",
                        entry.table
                    )));
                }
                TableCatalogEntryState::Active => {}
            }
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let table = parse_table_for_store(&entry.table)?;
            if !is_valid_table_metadata_location_for_entry(entry, &entry.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "table backup metadata location is invalid for {}/{}/{}",
                    entry.table_bucket, entry.namespace, entry.table
                )));
            }
            let current_metadata_location = table_catalog_object_key_from_location(&entry.table_bucket, &entry.metadata_location)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid(format!(
                        "table backup metadata location cannot be converted to an object key for {}/{}/{}",
                        entry.table_bucket, entry.namespace, entry.table
                    ))
                })?;
            let current_metadata =
                read_table_metadata_value(&self.object_backend, &entry.table_bucket, &current_metadata_location)
                    .await?
                    .ok_or_else(|| {
                        TableCatalogStoreError::NotFound(format!("current metadata object {current_metadata_location}"))
                    })?;
            validate_backup_table_metadata(entry, &current_metadata_location, &current_metadata, true)?;
            let mut metadata_locations = backup_metadata_log_locations(&current_metadata, entry)?;
            metadata_locations.insert(current_metadata_location.clone());
            for metadata_location in metadata_locations {
                if !is_valid_table_metadata_location_for_entry(entry, &metadata_location) {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "table backup metadata log location is outside the table: {metadata_location}"
                    )));
                }
                insert_backup_object_kind(
                    &mut keys,
                    &entry.table_bucket,
                    &metadata_location,
                    StrongTableCatalogBackupObjectKind::TableMetadata,
                )?;
                let metadata = if metadata_location == current_metadata_location {
                    current_metadata.clone()
                } else {
                    read_table_metadata_value(&self.object_backend, &entry.table_bucket, &metadata_location)
                        .await?
                        .ok_or_else(|| TableCatalogStoreError::NotFound(format!("metadata log object {metadata_location}")))?
                };
                validate_backup_table_metadata(
                    entry,
                    &metadata_location,
                    &metadata,
                    metadata_location == current_metadata_location,
                )?;
                let (metadata_entry, warehouse_prefix) = backup_metadata_graph_context(entry, &metadata)?;
                validate_backup_metadata_warehouse_owner(snapshot, entry, &warehouse_prefix)?;
                let context =
                    TableSnapshotGraphValidationContext::new(&self.object_backend, &entry.table_bucket, &metadata_entry);
                // A durable backup must be able to replay the complete Iceberg
                // snapshot graph, not merely preserve the metadata locations.
                // The maintenance reachability walk is intentionally tolerant
                // of unsupported objects so it can report candidates; backup
                // sealing must reject malformed or incomplete graphs instead.
                validate_table_snapshot_changes(&context, None, &metadata).await?;
                for reference in table_statistics_object_references(&metadata)? {
                    let object_key = table_catalog_object_key_from_location(&entry.table_bucket, &reference.location)
                        .ok_or_else(|| {
                            TableCatalogStoreError::Invalid(format!(
                                "table backup statistics location cannot be converted to an object key: {}",
                                reference.location
                            ))
                        })?;
                    if !object_key.starts_with(&warehouse_prefix) {
                        return Err(TableCatalogStoreError::Invalid(format!(
                            "table backup statistics object is outside the table warehouse: {object_key}"
                        )));
                    }
                    let kind = StrongTableCatalogBackupObjectKind::from_statistics_kind(reference.kind);
                    insert_backup_object_kind(&mut keys, &entry.table_bucket, &object_key, kind)?;
                }
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
                    insert_backup_object_kind(
                        &mut keys,
                        &entry.table_bucket,
                        &report.object_location,
                        StrongTableCatalogBackupObjectKind::from_maintenance_kind(&report.object_kind),
                    )?;
                }
            }
        }
        for entry in &snapshot.views {
            match &entry.state {
                TableCatalogEntryState::Deleted => continue,
                TableCatalogEntryState::Renaming | TableCatalogEntryState::Deleting => {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog backup requires view {} to leave its transient state before sealing",
                        entry.view
                    )));
                }
                TableCatalogEntryState::Active => {}
            }
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let view = parse_table_for_store(&entry.view)?;
            let current_metadata_location = table_catalog_object_key_from_location(&entry.table_bucket, &entry.metadata_location)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid(format!(
                        "view backup metadata location cannot be converted to an object key for {}/{}/{}",
                        entry.table_bucket, entry.namespace, entry.view
                    ))
                })?;
            if !is_valid_view_metadata_location(&namespace, &view, &current_metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "view backup metadata location is invalid for {}/{}/{}",
                    entry.table_bucket, entry.namespace, entry.view
                )));
            }
            let metadata = read_table_metadata_value(&self.object_backend, &entry.table_bucket, &current_metadata_location)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::NotFound(format!("current view metadata object {current_metadata_location}"))
                })?;
            validate_backup_view_metadata(entry, &namespace, &view, &current_metadata_location, &metadata)?;
            insert_backup_object_kind(
                &mut keys,
                &entry.table_bucket,
                &current_metadata_location,
                StrongTableCatalogBackupObjectKind::ViewMetadata,
            )?;
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
        if backup.source_snapshot_etag.is_empty() {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup has no source snapshot ETag watermark".to_string(),
            ));
        }
        if backup.snapshot.table_bucket.table_bucket != table_bucket {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup snapshot belongs to a different table bucket".to_string(),
            ));
        }
        if backup.snapshot.table_bucket.state != TableCatalogEntryState::Active
            || backup.snapshot.table_bucket.active_rename_id.is_some()
        {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup snapshot must contain an active table bucket without a pending rename".to_string(),
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
            backup.version,
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
        snapshot: &StrongTableCatalogBucketSnapshot,
        objects: &[StrongTableCatalogBackupMaintenanceObject],
    ) -> TableCatalogStoreResult<()> {
        let paths = maintenance_backup_paths(table_bucket, Some(snapshot))?;
        if objects
            .windows(2)
            .any(|pair| pair[0].object.as_str() >= pair[1].object.as_str())
        {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup maintenance objects must be sorted and unique".to_string(),
            ));
        }
        let mut total_bytes = 0usize;
        for object in objects {
            if !maintenance_backup_object_key_is_safe(table_bucket, &object.object, &paths)
                || object.data.len() > TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_OBJECT_SIZE
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup maintenance object is outside the catalog snapshot paths: {}",
                    object.object
                )));
            }
            if object.etag.as_deref().is_none_or(str::is_empty) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup maintenance object has no etag watermark required for conditional restore: {}",
                    object.object
                )));
            }
            if object.data.len() as u64 != object.size_bytes || backup_sha256(&object.data) != object.sha256 {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup maintenance object checksum is invalid: {}",
                    object.object
                )));
            }
            if maintenance_object_is_active(table_bucket, &object.object, &object.data, &paths)? {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup contains active maintenance state: {}",
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
            if object.size_bytes > TABLE_CATALOG_BACKUP_HASH_MAX_SIZE && !has_usable_etag(object.etag.as_deref()) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog backup is missing an etag watermark for a large object: {}/{}",
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
                if data.data.len() as u64 != object.size_bytes
                    || data.etag != object.etag
                    || backup_sha256(&data.data) != expected_sha256
                {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog backup object checksum changed: {}/{}",
                        object.bucket, object.object
                    )));
                }
            }
            let final_metadata = self
                .object_backend
                .object_metadata(&object.bucket, &object.object)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::Conflict(format!(
                        "catalog backup object disappeared while it was verified: {}/{}",
                        object.bucket, object.object
                    ))
                })?;
            if final_metadata.size != object.size_bytes || final_metadata.etag != object.etag {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog backup object watermark changed: {}/{}",
                    object.bucket, object.object
                )));
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
        let intent_etag = object
            .etag
            .filter(|etag| !etag.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Invalid("catalog restore intent has no usable ETag watermark".to_string()))?;
        for (name, etag) in [
            ("expected snapshot", intent.expected_snapshot_etag.as_deref()),
            ("target snapshot", intent.target_snapshot_etag.as_deref()),
            ("restored snapshot", intent.restored_snapshot_etag.as_deref()),
        ] {
            if etag.is_some_and(str::is_empty) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog restore intent has an empty {name} ETag watermark"
                )));
            }
        }
        Ok(Some((intent, Some(intent_etag))))
    }

    async fn write_restore_intent(
        &self,
        table_bucket: &str,
        intent: &StrongTableCatalogRestoreIntent,
        precondition: TableCatalogPutPrecondition,
        mutation_fence: Option<&TableCatalogObjectMutationFence>,
    ) -> TableCatalogStoreResult<()> {
        let data = serde_json::to_vec(intent)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode catalog restore intent: {err}")))?;
        if data.len() > 64 * 1024 {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore intent exceeds the maximum encoded size of 65536 bytes".to_string(),
            ));
        }
        let path = TableCatalogObjectPaths::default().catalog_backup_restore_intent_path(table_bucket);
        match mutation_fence {
            Some(fence) => {
                self.object_backend
                    .put_object_fenced(RUSTFS_META_BUCKET, &path, data, precondition, fence)
                    .await
            }
            None => {
                self.object_backend
                    .put_object(RUSTFS_META_BUCKET, &path, data, precondition)
                    .await
            }
        }
    }

    async fn delete_restore_intent_if_unchanged(
        &self,
        table_bucket: &str,
        expected_etag: &str,
        mutation_fence: Option<&TableCatalogObjectMutationFence>,
    ) -> TableCatalogStoreResult<()> {
        let path = TableCatalogObjectPaths::default().catalog_backup_restore_intent_path(table_bucket);
        match mutation_fence {
            Some(fence) => {
                self.object_backend
                    .delete_object_if_match_fenced(RUSTFS_META_BUCKET, &path, expected_etag, fence)
                    .await
            }
            None => {
                self.object_backend
                    .delete_object_if_match(RUSTFS_META_BUCKET, &path, expected_etag)
                    .await
            }
        }
    }

    fn validate_restore_intent_replay(
        intent: &StrongTableCatalogRestoreIntent,
        expected_snapshot_etag: Option<&str>,
        allow_replace: bool,
        current_snapshot_etag: Option<&str>,
        current_bucket: Option<&StrongTableCatalogBucketSnapshot>,
        backup: &StrongTableCatalogBackup,
    ) -> TableCatalogStoreResult<()> {
        let Some(target_bucket_present) = intent.target_bucket_present else {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore intent lacks a persisted target-presence precondition; manual recovery is required".to_string(),
            ));
        };
        let Some(intent_allow_replace) = intent.allow_replace else {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore intent lacks a persisted replacement policy; manual recovery is required".to_string(),
            ));
        };
        if intent.source_maintenance_fingerprint.is_none() {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore intent lacks a persisted maintenance precondition; manual recovery is required".to_string(),
            ));
        }
        if intent.expected_snapshot_etag.as_deref() != expected_snapshot_etag {
            return Err(TableCatalogStoreError::Conflict(
                "catalog restore retry does not match the persisted request precondition".to_string(),
            ));
        }
        if intent_allow_replace != allow_replace {
            return Err(TableCatalogStoreError::Conflict(
                "catalog restore retry does not match the persisted allow-replace policy".to_string(),
            ));
        }
        if target_bucket_present && intent.target_snapshot_etag.is_none() {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore intent has a bucket target without a snapshot precondition".to_string(),
            ));
        }
        if intent.target_bucket_fingerprint.is_empty() {
            return Err(TableCatalogStoreError::Invalid(
                "catalog restore intent lacks a persisted target bucket fingerprint; manual recovery is required".to_string(),
            ));
        }
        let catalog_matches_backup = current_bucket == Some(&backup.snapshot);
        if intent.state == StrongTableCatalogRestoreIntentState::CatalogApplied {
            if intent.restored_snapshot_etag.is_none() {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog restore intent marks the catalog applied without a restored snapshot ETag".to_string(),
                ));
            }
            if !catalog_matches_backup {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog restore intent records an applied snapshot but the target catalog changed".to_string(),
                ));
            }
            return Ok(());
        }
        // The catalog CAS may have succeeded immediately before a crash or an
        // intent-finalization error. Once the exact target bucket is present,
        // the target preconditions describe the state before publication and
        // must not reject replay of the already-applied restore.
        if catalog_matches_backup {
            return Ok(());
        }
        let current_target_bucket_fingerprint = current_bucket
            .map(table_catalog_bucket_snapshot_fingerprint)
            .transpose()?
            .unwrap_or_else(|| ABSENT_RESTORE_TARGET_BUCKET_FINGERPRINT.to_string());
        if intent.target_bucket_fingerprint != current_target_bucket_fingerprint {
            return Err(TableCatalogStoreError::Conflict(
                "catalog restore target bucket changed while the restore intent was pending".to_string(),
            ));
        }
        if intent.expected_snapshot_etag.is_some() && intent.target_snapshot_etag.as_deref() != current_snapshot_etag {
            return Err(TableCatalogStoreError::Conflict(
                "catalog restore global snapshot changed while the expected-snapshot-etag precondition was pending".to_string(),
            ));
        }
        if target_bucket_present != current_bucket.is_some() {
            return Err(TableCatalogStoreError::Conflict(
                "catalog restore target changed while the restore intent was pending".to_string(),
            ));
        }
        Ok(())
    }

    async fn restore_maintenance_objects(
        &self,
        table_bucket: &str,
        snapshot: &StrongTableCatalogBucketSnapshot,
        objects: &[StrongTableCatalogBackupMaintenanceObject],
        mutation_fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        Self::validate_maintenance_backup_objects(table_bucket, snapshot, objects)?;
        let allowed_paths = maintenance_restore_paths();
        let existing_objects = self
            .collect_maintenance_objects(table_bucket, &allowed_paths)
            .await?
            .into_iter()
            .map(|object| (object.object.clone(), object))
            .collect::<BTreeMap<_, _>>();
        let target_paths = objects.iter().map(|object| object.object.as_str()).collect::<BTreeSet<_>>();

        // Publish target objects before removing stale state. Each replacement is
        // conditional so a concurrent change cannot be overwritten silently.
        for object in objects {
            mutation_fence.ensure_held()?;
            match existing_objects.get(&object.object) {
                Some(existing) if existing.data == object.data => {}
                Some(existing) => {
                    let expected_etag = existing.etag.as_deref().ok_or_else(|| {
                        TableCatalogStoreError::Invalid(format!(
                            "catalog restore maintenance object has no etag watermark required for conditional replacement: {}",
                            object.object
                        ))
                    })?;
                    self.object_backend
                        .put_object_fenced(
                            RUSTFS_META_BUCKET,
                            &object.object,
                            object.data.clone(),
                            TableCatalogPutPrecondition::IfMatch(expected_etag.to_string()),
                            mutation_fence,
                        )
                        .await?;
                }
                None => {
                    self.object_backend
                        .put_object_fenced(
                            RUSTFS_META_BUCKET,
                            &object.object,
                            object.data.clone(),
                            TableCatalogPutPrecondition::IfAbsent,
                            mutation_fence,
                        )
                        .await?;
                }
            }
            mutation_fence.ensure_held()?;
        }

        // Only remove objects that are outside the complete target set. The
        // ETag check makes a stale inventory fail closed instead of deleting a
        // newer maintenance record.
        for (path, object) in existing_objects {
            if target_paths.contains(path.as_str()) {
                continue;
            }
            mutation_fence.ensure_held()?;
            let expected_etag = object.etag.as_deref().ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!(
                    "catalog restore maintenance object has no etag watermark required for conditional deletion: {path}"
                ))
            })?;
            self.object_backend
                .delete_object_if_match_fenced(RUSTFS_META_BUCKET, &path, expected_etag, mutation_fence)
                .await?;
            mutation_fence.ensure_held()?;
        }
        Ok(())
    }

    pub(crate) async fn create_durable_catalog_backup(
        &self,
        table_bucket: &str,
        expected_snapshot_etag: Option<&str>,
    ) -> TableCatalogStoreResult<TableCatalogBackupReport> {
        let fence = self.acquire_backup_fence(table_bucket).await?;
        fence.ensure_held()?;
        let mutation_fence = fence.mutation_fence();
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        if self.read_restore_intent(table_bucket).await?.is_some() {
            return Err(TableCatalogStoreError::Conflict(
                "catalog backup requires pending restore recovery before sealing".to_string(),
            ));
        }
        let (snapshot, source_snapshot_etag, source_snapshot_version) = {
            let state = self.state.lock().await;
            let current_etag = state
                .snapshot_etag
                .clone()
                .filter(|etag| !etag.is_empty())
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
        let maintenance_objects = self.collect_maintenance_backup_objects(table_bucket, Some(&snapshot)).await?;
        fence.ensure_held()?;
        let backup_id = catalog_backup_id(
            table_bucket,
            TABLE_CATALOG_BACKUP_VERSION,
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
        fence.ensure_held()?;
        let (status, persisted_created_at) = match self
            .object_backend
            .put_object_fenced(RUSTFS_META_BUCKET, &path, data, TableCatalogPutPrecondition::IfAbsent, &mutation_fence)
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
        fence.ensure_held()?;
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
        let fence = self.acquire_backup_fence(table_bucket).await?;
        fence.ensure_held()?;
        let mutation_fence = fence.mutation_fence();
        let _write_guard = self.write_lock.lock().await;
        let backup = self.read_backup(table_bucket, backup_id).await?;
        let current_objects = self.collect_backup_objects(&backup.snapshot).await?;
        if current_objects != backup.objects {
            return Err(TableCatalogStoreError::Conflict(
                "catalog backup object graph no longer matches the saved catalog snapshot".to_string(),
            ));
        }
        self.verify_backup_objects(&backup).await?;
        Self::validate_maintenance_backup_objects(table_bucket, &backup.snapshot, &backup.maintenance_objects)?;

        self.hydrate_state().await?;
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
        let already_restored = current_bucket.as_ref() == Some(&backup.snapshot);
        if let Some((intent, _)) = existing_intent.as_ref() {
            Self::validate_restore_intent_replay(
                intent,
                expected_snapshot_etag,
                allow_replace,
                current_snapshot_etag.as_deref(),
                current_bucket.as_ref(),
                &backup,
            )?;
        }
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

        let source_paths = maintenance_restore_paths();
        let source_objects = self.collect_maintenance_objects(table_bucket, &source_paths).await?;
        let source_fingerprint = maintenance_objects_fingerprint(&source_objects)?;
        let target_fingerprint = maintenance_objects_fingerprint(&backup.maintenance_objects)?;
        let fully_restored = already_restored && source_fingerprint == target_fingerprint;
        if existing_intent.is_none()
            && !fully_restored
            && expected_snapshot_etag.is_some_and(|expected| current_snapshot_etag.as_deref() != Some(expected))
        {
            return Err(TableCatalogStoreError::Conflict(
                "target catalog snapshot does not match expected-snapshot-etag".to_string(),
            ));
        }
        let target_bucket_fingerprint = current_bucket
            .as_ref()
            .map(table_catalog_bucket_snapshot_fingerprint)
            .transpose()?
            .unwrap_or_else(|| ABSENT_RESTORE_TARGET_BUCKET_FINGERPRINT.to_string());
        if let Some((intent, _)) = existing_intent.as_ref() {
            let persisted_source = intent.source_maintenance_fingerprint.as_deref().ok_or_else(|| {
                TableCatalogStoreError::Invalid(
                    "catalog restore intent lacks a persisted maintenance precondition; manual recovery is required".to_string(),
                )
            })?;
            if source_fingerprint != persisted_source && source_fingerprint != target_fingerprint {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog restore maintenance state changed while the restore was pending".to_string(),
                ));
            }
        }

        let mut intent =
            existing_intent
                .as_ref()
                .map(|(intent, _)| intent.clone())
                .unwrap_or_else(|| StrongTableCatalogRestoreIntent {
                    version: TABLE_CATALOG_BACKUP_VERSION,
                    table_bucket: table_bucket.to_string(),
                    backup_id: backup_id.to_string(),
                    expected_snapshot_etag: expected_snapshot_etag.map(str::to_string),
                    target_snapshot_etag: current_snapshot_etag.clone(),
                    target_bucket_fingerprint,
                    target_bucket_present: Some(current_bucket.is_some()),
                    allow_replace: Some(allow_replace),
                    source_maintenance_fingerprint: Some(source_fingerprint.clone()),
                    restored_snapshot_etag: already_restored.then(|| current_snapshot_etag.clone()).flatten(),
                    state: if already_restored {
                        StrongTableCatalogRestoreIntentState::CatalogApplied
                    } else {
                        StrongTableCatalogRestoreIntentState::Prepared
                    },
                });
        let mut intent_etag = existing_intent.as_ref().and_then(|(_, etag)| etag.clone());
        if existing_intent.is_none() {
            fence.ensure_held()?;
            self.write_restore_intent(table_bucket, &intent, TableCatalogPutPrecondition::IfAbsent, Some(&mutation_fence))
                .await?;
            fence.ensure_held()?;
            let (persisted, etag) = self.read_restore_intent(table_bucket).await?.ok_or_else(|| {
                TableCatalogStoreError::Internal("catalog restore intent disappeared after it was written".to_string())
            })?;
            if persisted != intent {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog restore intent changed while it was being persisted".to_string(),
                ));
            }
            intent = persisted;
            intent_etag = Some(etag.ok_or_else(|| {
                TableCatalogStoreError::Internal("catalog restore intent has no etag after it was written".to_string())
            })?);
        }

        let initially_already_restored = already_restored;
        let mut restored_snapshot_etag = current_snapshot_etag.clone();
        if intent.state == StrongTableCatalogRestoreIntentState::Prepared {
            if !already_restored {
                let persisted_source = intent.source_maintenance_fingerprint.as_deref().ok_or_else(|| {
                    TableCatalogStoreError::Invalid(
                        "catalog restore intent lacks a persisted maintenance precondition; manual recovery is required"
                            .to_string(),
                    )
                })?;
                if source_fingerprint != persisted_source {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog restore source maintenance state changed before catalog publication".to_string(),
                    ));
                }
            }
            if !already_restored {
                let (snapshot, precondition) = {
                    let state = self.state.lock().await;
                    let precondition = state
                        .snapshot_etag
                        .clone()
                        .map_or(TableCatalogPutPrecondition::IfAbsent, TableCatalogPutPrecondition::IfMatch);
                    let mut draft_state = state.clone();
                    StrongTableCatalogStore::<B>::remove_bucket_from_state_locked(&mut draft_state, table_bucket);
                    StrongTableCatalogStore::<B>::insert_bucket_snapshot_locked(&mut draft_state, backup.snapshot.clone())?;
                    // A restore must never downgrade a snapshot format that this process has already observed.
                    // The backup version is part of the source snapshot contract even though the bucket payload
                    // itself is version-independent.
                    draft_state.snapshot_version = Some(
                        draft_state
                            .snapshot_version
                            .unwrap_or(STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION)
                            .max(backup.source_snapshot_version),
                    );
                    let snapshot = StrongTableCatalogStore::<B>::snapshot_from_mutated_state_locked(
                        &mut draft_state,
                        self.snapshot_write_version,
                    )?;
                    (snapshot, precondition)
                };
                fence.ensure_held()?;
                self.verify_backup_objects(&backup).await?;
                self.finalize_snapshot_write_with_fence(
                    snapshot,
                    precondition,
                    StrongSnapshotWritePostcondition::BucketSnapshotPresent(backup.snapshot.clone()),
                    Some(&mutation_fence),
                )
                .await?;
                fence.ensure_held()?;
                self.hydrate_state().await?;
                restored_snapshot_etag =
                    Some(
                        self.state.lock().await.snapshot_etag.clone().ok_or_else(|| {
                            TableCatalogStoreError::Internal("restored catalog snapshot has no etag".to_string())
                        })?,
                    );
            } else {
                restored_snapshot_etag = current_snapshot_etag.clone();
            }
            intent.restored_snapshot_etag = restored_snapshot_etag.clone();
            intent.state = StrongTableCatalogRestoreIntentState::CatalogApplied;
            let current_intent_etag = intent_etag.clone().ok_or_else(|| {
                TableCatalogStoreError::Internal("catalog restore intent has no etag before finalization".to_string())
            })?;
            fence.ensure_held()?;
            self.write_restore_intent(
                table_bucket,
                &intent,
                TableCatalogPutPrecondition::IfMatch(current_intent_etag),
                Some(&mutation_fence),
            )
            .await?;
            fence.ensure_held()?;
            let (persisted, etag) = self.read_restore_intent(table_bucket).await?.ok_or_else(|| {
                TableCatalogStoreError::Internal("catalog restore intent disappeared after finalization".to_string())
            })?;
            if persisted != intent {
                return Err(TableCatalogStoreError::Invalid(
                    "finalized catalog restore intent changed while it was being persisted".to_string(),
                ));
            }
            intent_etag = Some(etag.ok_or_else(|| {
                TableCatalogStoreError::Internal("catalog restore intent has no etag after finalization".to_string())
            })?);
        } else if restored_snapshot_etag.is_none() {
            return Err(TableCatalogStoreError::Internal(
                "already-restored catalog snapshot has no etag".to_string(),
            ));
        }

        let mut allowed_paths = source_paths;
        allowed_paths.extend(maintenance_backup_paths(table_bucket, Some(&backup.snapshot))?);
        let current_after_catalog = self.collect_maintenance_objects(table_bucket, &allowed_paths).await?;
        let current_after_catalog_fingerprint = maintenance_objects_fingerprint(&current_after_catalog)?;
        if current_after_catalog_fingerprint != target_fingerprint {
            let persisted_source = intent.source_maintenance_fingerprint.as_deref().ok_or_else(|| {
                TableCatalogStoreError::Invalid(
                    "catalog restore intent lacks a persisted maintenance precondition; manual recovery is required".to_string(),
                )
            })?;
            if current_after_catalog_fingerprint != persisted_source {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog restore maintenance state changed while the restore was pending".to_string(),
                ));
            }
            fence.ensure_held()?;
            self.restore_maintenance_objects(table_bucket, &backup.snapshot, &backup.maintenance_objects, &mutation_fence)
                .await?;
            fence.ensure_held()?;
        }
        let restored_maintenance = self
            .collect_maintenance_backup_objects(table_bucket, Some(&backup.snapshot))
            .await?;
        if maintenance_objects_fingerprint(&restored_maintenance)? != target_fingerprint {
            return Err(TableCatalogStoreError::Conflict(
                "catalog restore maintenance state does not match the backup after publication".to_string(),
            ));
        }
        let intent_etag = intent_etag
            .ok_or_else(|| TableCatalogStoreError::Internal("catalog restore intent has no etag before cleanup".to_string()))?;
        fence.ensure_held()?;
        self.delete_restore_intent_if_unchanged(table_bucket, &intent_etag, Some(&mutation_fence))
            .await?;
        fence.ensure_held()?;
        Ok(TableCatalogRestoreReport {
            table_bucket: table_bucket.to_string(),
            backup_id: backup_id.to_string(),
            status: if initially_already_restored {
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
