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

use super::*;
use futures::{StreamExt, TryStreamExt, stream};

pub(in crate::table_catalog) const TABLE_REGISTRATION_READ_CONCURRENCY: usize = 8;
const TABLE_MAINTENANCE_RENAME_MAX_OBJECTS: usize = 4096;
const TABLE_MAINTENANCE_SCHEDULER_HISTORY_MAX_OBJECTS: usize = 4096;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TableMaintenanceRenameObject {
    etag: String,
    data: serde_json::Value,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TableMaintenanceRenameIntent {
    version: u16,
    pub(super) source: TableEntry,
    pub(super) destination: TableEntry,
    pub(super) source_snapshot_etag: String,
    source_objects: BTreeMap<String, TableMaintenanceRenameObject>,
    destination_etags: BTreeMap<String, String>,
}

fn maintenance_rename_intent_path(table_bucket: &str) -> String {
    format!(
        "{}maintenance-rename-intent.json",
        TableCatalogObjectPaths::default().table_bucket_root_prefix(table_bucket)
    )
}

pub(super) fn maintenance_rename_recovery_error(mut error: TableCatalogStoreError) -> TableCatalogStoreError {
    let message = match &mut error {
        TableCatalogStoreError::NotFound(message)
        | TableCatalogStoreError::NamespaceNotFound(message)
        | TableCatalogStoreError::TableNotFound(message)
        | TableCatalogStoreError::AlreadyExists(message)
        | TableCatalogStoreError::Conflict(message)
        | TableCatalogStoreError::Invalid(message)
        | TableCatalogStoreError::Unsupported(message)
        | TableCatalogStoreError::Unavailable(message)
        | TableCatalogStoreError::LockUnavailable { message, .. }
        | TableCatalogStoreError::Internal(message) => message,
        #[cfg(test)]
        TableCatalogStoreError::Io { context, .. } => {
            *context = "maintenance rename recovery required; retry after resolving the I/O failure";
            return error;
        }
    };
    *message = format!(
        "maintenance rename recovery required; retry the same rename after transient errors, or repair invalid persisted state before retrying: {message}"
    );
    error
}

fn maintenance_rename_prefix(entry: &TableEntry) -> TableCatalogStoreResult<String> {
    let namespace = parse_namespace_for_store(&entry.namespace)?;
    let table = parse_table_for_store(&entry.table)?;
    let path = TableCatalogObjectPaths::default().table_maintenance_config_path(
        &entry.table_bucket,
        &namespace,
        &table,
        &entry.table_id,
    );
    Ok(path.trim_end_matches(MAINTENANCE_CONFIG_FILE).to_string())
}

fn maintenance_rename_suffix_is_valid(suffix: &str) -> bool {
    matches!(
        suffix,
        MAINTENANCE_CONFIG_FILE | MAINTENANCE_CURRENT_JOB_FILE | MAINTENANCE_LATEST_JOB_FILE
    ) || suffix
        .strip_prefix(&format!("{MAINTENANCE_JOB_ROOT}/"))
        .and_then(|suffix| suffix.strip_suffix(".json"))
        .is_some_and(|hash| {
            hash.len() == 64
                && hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

fn maintenance_rename_data(
    entry: &TableEntry,
    destination: &TableEntry,
    suffix: &str,
    data: &serde_json::Value,
) -> TableCatalogStoreResult<serde_json::Value> {
    if !maintenance_rename_suffix_is_valid(suffix) {
        return Err(TableCatalogStoreError::Invalid(
            "unexpected table maintenance rename object path".to_string(),
        ));
    }
    if suffix == MAINTENANCE_CONFIG_FILE {
        let config = serde_json::from_value::<TableMaintenanceConfig>(data.clone())
            .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid maintenance rename config: {err}")))?;
        validate_table_maintenance_config(&config)?;
        return Ok(data.clone());
    }
    let report = serde_json::from_value::<TableMetadataMaintenanceReport>(data.clone())
        .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid maintenance rename report: {err}")))?;
    let namespace = parse_namespace_for_store(&entry.namespace)?;
    let table = parse_table_for_store(&entry.table)?;
    validate_table_maintenance_report_owner(&report, &entry.table_bucket, &namespace, &table, &entry.table_id)?;
    if suffix.starts_with(&format!("{MAINTENANCE_JOB_ROOT}/"))
        && suffix != format!("{MAINTENANCE_JOB_ROOT}/{}.json", table_catalog_path_hash(&report.job.job_id))
    {
        return Err(TableCatalogStoreError::Invalid(
            "maintenance rename job does not match its object path".to_string(),
        ));
    }
    // Preserve every report field, including audit records and unknown additive fields.
    // Only catalog owner identifiers change; physical locations and generations do not.
    let mut renamed = data.clone();
    for field in ["job", "snapshot-expiration", "compaction"] {
        let Some(owner) = renamed.get_mut(field).filter(|value| !value.is_null()) else {
            continue;
        };
        if owner.get("table_bucket").and_then(serde_json::Value::as_str) != Some(entry.table_bucket.as_str())
            || owner.get("namespace").and_then(serde_json::Value::as_str) != Some(entry.namespace.as_str())
            || owner.get("table").and_then(serde_json::Value::as_str) != Some(entry.table.as_str())
            || owner.get("table_id").and_then(serde_json::Value::as_str) != Some(entry.table_id.as_str())
        {
            return Err(TableCatalogStoreError::Invalid(
                "maintenance rename nested report owner does not match its table".to_string(),
            ));
        }
        owner["namespace"] = serde_json::Value::String(destination.namespace.clone());
        owner["table"] = serde_json::Value::String(destination.table.clone());
    }
    Ok(renamed)
}

pub(in crate::table_catalog) fn ensure_table_maintenance_publication_held(
    publication: Option<&(dyn TableCommitPublication + Sync)>,
    entry: &TableEntry,
    stage: &str,
) -> TableCatalogStoreResult<()> {
    if publication.is_some_and(|publication| {
        !publication.holds_table_bucket(&entry.table_bucket)
            || !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.table)
    }) {
        return Err(TableCatalogStoreError::Internal(format!(
            "table maintenance publication fence was lost {stage}"
        )));
    }
    Ok(())
}

pub(in crate::table_catalog) fn table_maintenance_mutation_fence(
    publication: Option<&(dyn TableCommitPublication + Sync)>,
    entry: &TableEntry,
) -> TableCatalogStoreResult<Option<TableCatalogObjectMutationFence>> {
    ensure_table_maintenance_publication_held(publication, entry, "before storage mutation")?;
    publication
        .map(|publication| {
            publication.object_mutation_fence().ok_or_else(|| {
                TableCatalogStoreError::Internal("table maintenance requires a storage mutation fence".to_string())
            })
        })
        .transpose()
}

pub(in crate::table_catalog) fn validate_table_bucket_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    entry: &TableBucketEntry,
) -> TableCatalogStoreResult<()> {
    paths.validate_bucket_entry(entry)?;
    if paths.table_bucket_entry_path(&entry.table_bucket) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog table bucket entry identity does not match its object path".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::table_catalog) fn validate_namespace_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    entry: &NamespaceEntry,
) -> TableCatalogStoreResult<()> {
    let namespace = validate_namespace_entry_identity(entry)?;
    if paths.namespace_entry_path(&entry.table_bucket, &namespace) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog namespace entry identity does not match its object path".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::table_catalog) fn validate_table_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    entry: &TableEntry,
) -> TableCatalogStoreResult<Namespace> {
    paths.validate_table_entry(entry)?;
    let namespace = parse_namespace_for_store(&entry.namespace)?;
    let table = paths.parse_name(&entry.table)?;
    if paths.table_entry_path(&entry.table_bucket, &namespace, &table) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog table entry identity does not match its object path".to_string(),
        ));
    }
    Ok(namespace)
}

fn validate_table_rename_intent_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    intent: &TableRenameIntent,
) -> TableCatalogStoreResult<()> {
    if intent.version != TABLE_RENAME_INTENT_VERSION
        || intent.rename_id.is_empty()
        || intent.source_etag.is_empty()
        || intent.destination_etag.as_deref().is_some_and(str::is_empty)
        || intent.warehouse_index_etag.is_empty()
        || intent.created_at.is_empty()
        || intent.updated_at.is_empty()
    {
        return Err(TableCatalogStoreError::Invalid(
            "catalog table rename intent has invalid required fields".to_string(),
        ));
    }
    if paths.table_rename_intent_path(&intent.table_bucket, &intent.rename_id) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog table rename intent identity does not match its object path".to_string(),
        ));
    }
    validate_table_entry_version_and_id(&intent.source)?;
    validate_table_entry_version_and_id(&intent.destination)?;
    if intent.source.table_bucket != intent.table_bucket
        || intent.destination.table_bucket != intent.table_bucket
        || intent.source.state != TableCatalogEntryState::Active
        || intent.destination.state != TableCatalogEntryState::Active
    {
        return Err(TableCatalogStoreError::Invalid(
            "catalog table rename intent has invalid table ownership or state".to_string(),
        ));
    }
    let mut expected_destination = intent.source.clone();
    expected_destination.namespace.clone_from(&intent.destination.namespace);
    expected_destination.table.clone_from(&intent.destination.table);
    expected_destination.updated_at.clone_from(&intent.destination.updated_at);
    if expected_destination != intent.destination
        || intent.destination.updated_at.as_deref() != Some(intent.created_at.as_str())
        || (intent.source.namespace == intent.destination.namespace && intent.source.table == intent.destination.table)
    {
        return Err(TableCatalogStoreError::Invalid(
            "catalog table rename intent changes fields other than the table identifier and update time".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn next_table_catalog_update_time(previous: Option<&str>) -> String {
    let now = OffsetDateTime::now_utc();
    let update_time = previous
        .and_then(|value| OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok())
        .filter(|previous| *previous >= now)
        .map_or(now, |previous| previous.saturating_add(Duration::nanoseconds(1)));
    update_time
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| update_time.to_string())
}

pub(super) fn validate_view_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    entry: &ViewEntry,
) -> TableCatalogStoreResult<Namespace> {
    validate_view_entry_version_and_id(entry)?;
    let namespace = parse_namespace_for_store(&entry.namespace)?;
    let view = parse_table_for_store(&entry.view)?;
    if paths.view_entry_path(&entry.table_bucket, &namespace, &view) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog view entry identity does not match its object path".to_string(),
        ));
    }
    Ok(namespace)
}

pub(super) fn validate_commit_log_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    table_bucket: &str,
    table_id: &str,
    entry: &CommitLogEntry,
) -> TableCatalogStoreResult<()> {
    paths.validate_commit_entry(entry, table_bucket, table_id)?;
    if entry.table_id != table_id || paths.commit_log_entry_path(table_bucket, table_id, &entry.commit_id) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog commit log identity does not match its object path".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_commit_idempotency_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    table_bucket: &str,
    table_id: &str,
    entry: &CommitLogEntry,
) -> TableCatalogStoreResult<()> {
    paths.validate_commit_entry(entry, table_bucket, table_id)?;
    let idempotency_key = entry
        .idempotency_key
        .as_deref()
        .ok_or_else(|| TableCatalogStoreError::Invalid("catalog commit idempotency index has no idempotency key".to_string()))?;
    if entry.table_id != table_id || paths.commit_idempotency_entry_path(table_bucket, table_id, idempotency_key) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog commit idempotency identity does not match its object path".to_string(),
        ));
    }
    Ok(())
}

fn validate_external_catalog_bridge_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    entry: &ExternalCatalogBridgeEntry,
) -> TableCatalogStoreResult<()> {
    validate_catalog_entry_version("external catalog bridge", entry.version)?;
    let namespace = parse_namespace_for_store(&entry.namespace)?;
    let table = parse_table_for_store(&entry.table)?;
    if paths.external_catalog_bridge_path(&entry.table_bucket, &namespace, &table) != object {
        return Err(TableCatalogStoreError::Invalid(
            "external catalog bridge identity does not match its object path".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::table_catalog) fn validate_table_maintenance_report_owner(
    report: &TableMetadataMaintenanceReport,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    table_id: &str,
) -> TableCatalogStoreResult<()> {
    if report.job.job_id.is_empty()
        || report.job.table_bucket != table_bucket
        || report.job.namespace != namespace.public_name()
        || report.job.table != table.as_str()
        || report.job.table_id != table_id
    {
        return Err(TableCatalogStoreError::Invalid(
            "table maintenance report identity does not match its catalog owner".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::table_catalog) fn validate_table_maintenance_report_path(
    paths: &TableCatalogObjectPaths,
    bucket: &str,
    key: &str,
    suffix: &str,
    report: &TableMetadataMaintenanceReport,
) -> TableCatalogStoreResult<()> {
    let job = &report.job;
    let namespace = parse_namespace_for_store(&job.namespace)?;
    let table = parse_table_for_store(&job.table)?;
    validate_table_maintenance_report_owner(report, bucket, &namespace, &table, &job.table_id)?;
    let expected = match suffix {
        MAINTENANCE_CURRENT_JOB_FILE => paths.table_maintenance_current_job_path(bucket, &namespace, &table, &job.table_id),
        MAINTENANCE_LATEST_JOB_FILE => paths.table_maintenance_latest_job_path(bucket, &namespace, &table, &job.table_id),
        _ => paths.table_maintenance_job_path(bucket, &namespace, &table, &job.table_id, &job.job_id),
    };
    if job.table_id.is_empty() || key != expected {
        return Err(TableCatalogStoreError::Invalid(
            "maintenance report does not match its object path".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::table_catalog) fn validate_table_warehouse_index_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    index: &TableWarehouseIndexEntry,
) -> TableCatalogStoreResult<()> {
    validate_catalog_entry_version("warehouse index", index.version)?;
    if paths.warehouse_index_entry_path(&index.table_bucket, &index.warehouse_object_prefix) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog warehouse index identity does not match its object path".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::table_catalog) fn table_warehouse_index_state_ready(
    state: &TableWarehouseIndexStateEntry,
    table_bucket: &str,
) -> TableCatalogStoreResult<bool> {
    if state.version == 0 || state.version > TABLE_WAREHOUSE_INDEX_STATE_VERSION {
        return Err(TableCatalogStoreError::Invalid(format!(
            "unsupported warehouse index state version: {}",
            state.version
        )));
    }
    if state.table_bucket != table_bucket {
        return Err(TableCatalogStoreError::Invalid(
            "warehouse index state identity does not match its object path".to_string(),
        ));
    }
    Ok(state.version == TABLE_WAREHOUSE_INDEX_STATE_VERSION && state.state == TableCatalogEntryState::Active)
}

struct ActiveNamespaceEvidence {
    namespace: Namespace,
    explicit_entry: Option<NamespaceEntry>,
}

enum TableWarehouseIndexResolution {
    Active(TableDataPlaneResource),
    Deleted(TableDataPlaneResource),
    Missing,
}

enum TableWarehouseIndexBackfillMode {
    InitializeIfNeeded,
    ScanAllTables,
}

pub(in crate::table_catalog) fn bounded_table_entry_objects_for_data_plane_scan(
    objects: Vec<String>,
    is_truncated: bool,
    max_catalog_objects: usize,
) -> TableCatalogStoreResult<Vec<String>> {
    // Only table entries count: maintenance reports, bridges and other
    // per-table objects share the prefix and are controlled by table-scoped
    // principals, so they must not be able to trip the data-plane limit.
    let entries: Vec<String> = objects
        .into_iter()
        .filter(|object| object.ends_with(TABLE_ENTRY_FILE))
        .collect();
    if is_truncated || entries.len() > max_catalog_objects {
        return Err(TableCatalogStoreError::Unavailable(format!(
            "table data-plane warehouse-index miss scan exceeds the {max_catalog_objects}-catalog-object safety limit"
        )));
    }
    Ok(entries)
}

#[derive(Clone)]
pub(crate) struct ObjectTableCatalogStore<B> {
    pub(in crate::table_catalog) backend: B,
    pub(in crate::table_catalog) paths: TableCatalogObjectPaths,
    vector_bucket: Option<BucketHandle>,
    vector_owner: Arc<()>,
}

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            paths: TableCatalogObjectPaths::default(),
            vector_bucket: None,
            vector_owner: Arc::new(()),
        }
    }

    #[cfg(test)]
    pub(crate) async fn for_vector_scope(backend: B, bucket: BucketHandle) -> TableCatalogStoreResult<Self> {
        if bucket.kind() != BucketKind::Vector {
            return Err(TableCatalogStoreError::Invalid("Vector catalog requires a Vector bucket".to_string()));
        }
        backend.revalidate_bucket_handle(&bucket).await?;
        Ok(Self {
            backend,
            paths: TableCatalogObjectPaths::for_vector(),
            vector_bucket: Some(bucket),
            vector_owner: Arc::new(()),
        })
    }

    fn vector_bucket_for(&self, bucket: &str) -> TableCatalogStoreResult<&BucketHandle> {
        self.vector_bucket
            .as_ref()
            .filter(|expected| expected.storage_bucket_id().as_str() == bucket)
            .ok_or_else(|| TableCatalogStoreError::Conflict("bucket does not match this Vector catalog".to_string()))
    }

    fn validate_store_bucket_entry(&self, entry: &TableBucketEntry) -> TableCatalogStoreResult<()> {
        self.paths.validate_bucket_entry(entry)?;
        if self.paths.scope == CatalogScope::Vector {
            let actual = entry
                .vector_bucket_binding
                .as_ref()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector bucket binding is required".to_string()))?
                .to_handle()?;
            if &actual != self.vector_bucket_for(&entry.table_bucket)? {
                return Err(TableCatalogStoreError::Conflict("stored Vector bucket identity changed".to_string()));
            }
        }
        Ok(())
    }

    pub(in crate::table_catalog) fn catalog_bucket(&self) -> &'static str {
        RUSTFS_META_BUCKET
    }

    pub(super) async fn acquire_table_bucket_registry_write_permit(&self) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        if self.paths.scope == CatalogScope::Vector {
            return self
                .backend
                .acquire_read_lock(self.catalog_bucket(), "s3vectors/catalog/registry.lock")
                .await;
        }
        self.reject_legacy_catalog_global_state().await?;
        self.backend
            .acquire_read_lock(self.catalog_bucket(), &self.paths.catalog_backup_global_fence_lock_path())
            .await
    }

    pub(in crate::table_catalog) async fn acquire_object_backed_catalog_write_permit(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        if self.paths.scope == CatalogScope::Vector {
            return self
                .backend
                .acquire_bucket_lifecycle_read(self.vector_bucket_for(table_bucket)?)
                .await;
        }
        let guard = self
            .backend
            .acquire_read_lock(self.catalog_bucket(), &self.paths.catalog_backup_fence_lock_path(table_bucket))
            .await?;
        self.ensure_no_active_restore(table_bucket).await?;
        Ok(guard)
    }

    pub(in crate::table_catalog) async fn ensure_object_backed_catalog_write_permit_after_lock(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<()> {
        if self.paths.scope == CatalogScope::Vector {
            return self
                .backend
                .revalidate_bucket_handle(self.vector_bucket_for(table_bucket)?)
                .await;
        }
        self.ensure_no_active_restore(table_bucket).await
    }

    pub(in crate::table_catalog) async fn with_catalog_restore_read<T>(
        &self,
        table_bucket: &str,
        read: impl std::future::Future<Output = TableCatalogStoreResult<T>>,
    ) -> TableCatalogStoreResult<T> {
        if self.paths.scope == CatalogScope::Vector {
            let bucket = self.vector_bucket_for(table_bucket)?;
            let guard = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
            let result = read.await?;
            self.backend.revalidate_bucket_handle(bucket).await?;
            if guard.is_lock_lost() {
                return Err(TableCatalogStoreError::Unavailable("Vector bucket lifecycle guard was lost".to_string()));
            }
            return Ok(result);
        }
        // A persisted epoch also detects a restore that starts and finishes
        // between these observations, even when catalog bytes return to an
        // earlier value. No nested read lease is needed on this read path.
        let restore_version = self.catalog_restore_read_version(table_bucket).await?;
        let result = read.await?;
        self.finish_catalog_restore_read(table_bucket, restore_version.as_deref())
            .await?;
        Ok(result)
    }

    pub(in crate::table_catalog) async fn with_table_maintenance_read<T>(
        &self,
        table_bucket: &str,
        read: impl std::future::Future<Output = TableCatalogStoreResult<T>>,
    ) -> TableCatalogStoreResult<T> {
        self.with_catalog_restore_read(table_bucket, async {
            let version = self.table_rename_read_version(table_bucket).await?;
            let result = read.await?;
            self.finish_table_rename_read(table_bucket, version.as_deref()).await?;
            Ok(result)
        })
        .await
    }

    async fn acquire_table_maintenance_publication_permit(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        let guard = self
            .backend
            .acquire_read_lock(table_bucket, &default_table_bucket_publication_lock_path())
            .await?;
        self.ensure_no_active_table_rename(table_bucket).await?;
        Ok(guard)
    }

    pub(in crate::table_catalog) async fn metadata_maintenance_publication(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<TableCommitLockPublication<'_, B>> {
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        publication.attach_catalog_migration_read_guard(self.acquire_object_backed_catalog_write_permit(table_bucket).await?)?;
        let bucket_guard = self.acquire_table_maintenance_publication_permit(table_bucket).await?;
        publication.attach_maintenance_bucket_read_guard(table_bucket, bucket_guard)?;
        Ok(publication)
    }

    async fn list_entry_page<T, P, V>(
        &self,
        prefix: &str,
        entry_file: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
        include: P,
        validate: V,
    ) -> TableCatalogStoreResult<TableCatalogListPage<T>>
    where
        T: DeserializeOwned,
        P: Fn(&T) -> bool,
        V: Fn(&str, &T) -> TableCatalogStoreResult<()>,
    {
        let cursor = catalog_list_cursor(cursor, OBJECT_CATALOG_LIST_CURSOR_PREFIX)?;
        if cursor.is_some_and(|cursor| !cursor.starts_with(prefix)) {
            return Err(TableCatalogStoreError::Invalid(
                "page cursor does not match this table catalog list operation".to_string(),
            ));
        }

        let scan_limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS)
            .ok_or_else(|| TableCatalogStoreError::Internal("catalog object scan limit must be positive".to_string()))?;
        let mut entries = Vec::with_capacity(limit.get());
        let mut last_entry_path = None;

        let page = self
            .backend
            .list_objects_page(self.catalog_bucket(), prefix, cursor, scan_limit)
            .await?;
        let last_scanned_path = page.objects.last().cloned();
        if page.is_truncated && last_scanned_path.is_none() {
            return Err(TableCatalogStoreError::Internal("catalog object pagination made no progress".to_string()));
        }
        if cursor
            .zip(last_scanned_path.as_deref())
            .is_some_and(|(cursor, last)| last <= cursor)
        {
            return Err(TableCatalogStoreError::Internal("catalog object pagination did not advance".to_string()));
        }

        for object in page.objects {
            if !object.ends_with(entry_file) {
                continue;
            }
            if entries.len() == limit.get() {
                let last_entry_path = last_entry_path.ok_or_else(|| {
                    TableCatalogStoreError::Internal("catalog page cursor is missing its last entry".to_string())
                })?;
                return Ok(TableCatalogListPage {
                    entries,
                    next_cursor: Some(format!("{OBJECT_CATALOG_LIST_CURSOR_PREFIX}{last_entry_path}")),
                });
            }
            let Some((entry, _)) = self.read_entry::<T>(self.catalog_bucket(), &object).await? else {
                continue;
            };
            validate(&object, &entry)?;
            if !include(&entry) {
                continue;
            }
            last_entry_path = Some(object);
            entries.push(entry);
        }

        let next_cursor = if page.is_truncated {
            last_scanned_path.map(|path| format!("{OBJECT_CATALOG_LIST_CURSOR_PREFIX}{path}"))
        } else {
            None
        };
        Ok(TableCatalogListPage { entries, next_cursor })
    }

    async fn read_active_namespace_evidence(&self, object: &str) -> TableCatalogStoreResult<Option<ActiveNamespaceEvidence>> {
        if object.ends_with(NAMESPACE_ENTRY_FILE) {
            let Some((entry, _)) = self.read_entry::<NamespaceEntry>(self.catalog_bucket(), object).await? else {
                return Ok(None);
            };
            validate_namespace_entry_object(&self.paths, object, &entry)?;
            if entry.state != TableCatalogEntryState::Active {
                return Ok(None);
            }
            return Ok(Some(ActiveNamespaceEvidence {
                namespace: parse_namespace_for_store(&entry.namespace)?,
                explicit_entry: Some(entry),
            }));
        }
        if object.ends_with(TABLE_ENTRY_FILE) {
            let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), object).await? else {
                return Ok(None);
            };
            let namespace = validate_table_entry_object(&self.paths, object, &entry)?;
            return Ok((entry.state == TableCatalogEntryState::Active).then_some(ActiveNamespaceEvidence {
                namespace,
                explicit_entry: None,
            }));
        }
        if object.ends_with(VIEW_ENTRY_FILE) {
            let Some((entry, _)) = self.read_entry::<ViewEntry>(self.catalog_bucket(), object).await? else {
                return Ok(None);
            };
            let namespace = validate_view_entry_object(&self.paths, object, &entry)?;
            return Ok((entry.state == TableCatalogEntryState::Active).then_some(ActiveNamespaceEvidence {
                namespace,
                explicit_entry: None,
            }));
        }
        Ok(None)
    }

    async fn has_active_namespace_object(&self, table_bucket: &str, namespace: &Namespace) -> TableCatalogStoreResult<bool> {
        let scan_limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS)
            .ok_or_else(|| TableCatalogStoreError::Internal("catalog object scan limit must be positive".to_string()))?;
        for prefix in [
            self.paths.table_entries_prefix(table_bucket, namespace),
            self.paths.view_entries_prefix(table_bucket, namespace),
        ] {
            let mut cursor = None;
            loop {
                let page = self
                    .backend
                    .list_objects_page(self.catalog_bucket(), &prefix, cursor.as_deref(), scan_limit)
                    .await?;
                let last_scanned = page.objects.last().cloned();
                for object in page.objects {
                    if self
                        .read_active_namespace_evidence(&object)
                        .await?
                        .is_some_and(|evidence| evidence.namespace == *namespace)
                    {
                        return Ok(true);
                    }
                }
                if !page.is_truncated {
                    break;
                }
                let next = last_scanned.ok_or_else(|| {
                    TableCatalogStoreError::Internal("catalog namespace object scan made no progress".to_string())
                })?;
                if cursor.as_deref().is_some_and(|cursor| next.as_str() <= cursor) {
                    return Err(TableCatalogStoreError::Internal(
                        "catalog namespace object scan did not advance".to_string(),
                    ));
                }
                cursor = Some(next);
            }
        }
        Ok(false)
    }

    async fn has_namespace_resource_entry(&self, table_bucket: &str, namespace: &Namespace) -> TableCatalogStoreResult<bool> {
        let scan_limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS)
            .ok_or_else(|| TableCatalogStoreError::Internal("catalog object scan limit must be positive".to_string()))?;
        for (prefix, entry_file) in [
            (self.paths.table_entries_prefix(table_bucket, namespace), TABLE_ENTRY_FILE),
            (self.paths.view_entries_prefix(table_bucket, namespace), VIEW_ENTRY_FILE),
        ] {
            let mut cursor = None;
            loop {
                let page = self
                    .backend
                    .list_objects_page(self.catalog_bucket(), &prefix, cursor.as_deref(), scan_limit)
                    .await?;
                let last_scanned = page.objects.last().cloned();
                for object in page.objects.iter().filter(|object| object.ends_with(entry_file)) {
                    let state = if entry_file == TABLE_ENTRY_FILE {
                        let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), object).await? else {
                            continue;
                        };
                        validate_table_entry_object(&self.paths, object, &entry)?;
                        entry.state
                    } else {
                        let Some((entry, _)) = self.read_entry::<ViewEntry>(self.catalog_bucket(), object).await? else {
                            continue;
                        };
                        validate_view_entry_object(&self.paths, object, &entry)?;
                        entry.state
                    };
                    // Retain tombstones for conditional identifier reuse. Pending
                    // publications and malformed records must still block deletion.
                    if state != TableCatalogEntryState::Deleted {
                        return Ok(true);
                    }
                }
                if !page.is_truncated {
                    break;
                }
                let next = last_scanned.ok_or_else(|| {
                    TableCatalogStoreError::Internal("catalog namespace resource scan made no progress".to_string())
                })?;
                if cursor.as_deref().is_some_and(|cursor| next.as_str() <= cursor) {
                    return Err(TableCatalogStoreError::Internal(
                        "catalog namespace resource scan did not advance".to_string(),
                    ));
                }
                cursor = Some(next);
            }
        }
        Ok(false)
    }

    async fn has_active_namespace_descendant(&self, table_bucket: &str, namespace: &Namespace) -> TableCatalogStoreResult<bool> {
        let parent = namespace.public_name();
        let namespace_path = self.paths.namespace_entry_path(table_bucket, namespace);
        let descendant_prefix = format!("{}{}/", self.paths.namespace_entries_prefix(table_bucket), namespace.storage_id());
        let scan_limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS)
            .ok_or_else(|| TableCatalogStoreError::Internal("catalog object scan limit must be positive".to_string()))?;
        let mut cursor = None;
        loop {
            let page = self
                .backend
                .list_objects_page(self.catalog_bucket(), &descendant_prefix, cursor.as_deref(), scan_limit)
                .await?;
            let last_scanned = page.objects.last().cloned();
            for object in page.objects {
                if object == namespace_path {
                    continue;
                }
                if self
                    .read_active_namespace_evidence(&object)
                    .await?
                    .is_some_and(|evidence| namespace_is_descendant(&evidence.namespace.public_name(), &parent))
                {
                    return Ok(true);
                }
            }
            if !page.is_truncated {
                return Ok(false);
            }
            let next = last_scanned.ok_or_else(|| {
                TableCatalogStoreError::Internal("catalog namespace descendant scan made no progress".to_string())
            })?;
            if cursor.as_deref().is_some_and(|cursor| next.as_str() <= cursor) {
                return Err(TableCatalogStoreError::Internal(
                    "catalog namespace descendant scan did not advance".to_string(),
                ));
            }
            cursor = Some(next);
        }
    }

    async fn require_active_namespace_unlocked(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        namespace_path: &str,
    ) -> TableCatalogStoreResult<()> {
        let current = self
            .read_entry_unlocked::<NamespaceEntry>(self.catalog_bucket(), namespace_path)
            .await?;
        if let Some((entry, _)) = current.as_ref() {
            validate_namespace_entry_object(&self.paths, namespace_path, entry)?;
            if entry.state == TableCatalogEntryState::Active {
                return Ok(());
            }
        }
        if !self.has_active_namespace_object(table_bucket, namespace).await?
            && !self.has_active_namespace_descendant(table_bucket, namespace).await?
        {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {table_bucket}/{}",
                namespace.public_name()
            )));
        }
        Ok(())
    }

    async fn list_namespace_children_page_inner(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        let parent = parent.map(parse_namespace_for_store).transpose()?;
        if let Some(parent) = parent.as_ref()
            && self
                .get_namespace(table_bucket, &parent.public_name())
                .await?
                .is_none_or(|entry| entry.state != TableCatalogEntryState::Active)
        {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {table_bucket}/{}",
                parent.public_name()
            )));
        }

        let namespace_prefix = self.paths.namespace_entries_prefix(table_bucket);
        let scan_prefix = match parent.as_ref() {
            Some(parent) => format!("{namespace_prefix}{}/", parent.storage_id()),
            None => namespace_prefix,
        };
        let mut scan_cursor = catalog_list_cursor(cursor, OBJECT_CATALOG_LIST_CURSOR_PREFIX)?.map(str::to_string);
        if scan_cursor.as_ref().is_some_and(|cursor| !cursor.starts_with(&scan_prefix)) {
            return Err(TableCatalogStoreError::Invalid(
                "page cursor does not match this namespace child list operation".to_string(),
            ));
        }
        let scan_limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS)
            .ok_or_else(|| TableCatalogStoreError::Internal("catalog object scan limit must be positive".to_string()))?;
        let mut children = Vec::with_capacity(limit.get().saturating_add(1));

        loop {
            let page = self
                .backend
                .list_objects_page(self.catalog_bucket(), &scan_prefix, scan_cursor.as_deref(), scan_limit)
                .await?;
            let last_scanned = page.objects.last().cloned();
            let mut current_segment = None;
            let mut current_segment_visible = false;

            for object in page.objects {
                let Some(relative) = object.strip_prefix(&scan_prefix) else {
                    return Err(TableCatalogStoreError::Invalid(
                        "catalog namespace child object is outside its list prefix".to_string(),
                    ));
                };
                let Some((segment, _)) = relative.split_once('/') else {
                    continue;
                };
                if current_segment.as_deref() != Some(segment) {
                    current_segment = Some(segment.to_string());
                    current_segment_visible = false;
                }
                if current_segment_visible {
                    continue;
                }
                let Some(evidence) = self.read_active_namespace_evidence(&object).await? else {
                    continue;
                };
                let namespace = evidence.namespace;
                if parent
                    .as_ref()
                    .is_some_and(|parent| !namespace.segments().starts_with(parent.segments()))
                {
                    continue;
                }
                let parent_depth = parent.as_ref().map_or(0, |parent| parent.segments().len());
                if namespace.segments().len() <= parent_depth || namespace.segments()[parent_depth].as_str() != segment {
                    continue;
                }
                let child = Namespace::from_segments(
                    namespace.segments()[..=parent_depth]
                        .iter()
                        .map(|segment| segment.as_str().to_string())
                        .collect(),
                )
                .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid catalog namespace child: {err}")))?;
                let child_entry = evidence
                    .explicit_entry
                    .filter(|_| namespace == child)
                    .unwrap_or_else(|| synthetic_namespace_entry(table_bucket, &child));
                let child_cursor = format!("{OBJECT_CATALOG_LIST_CURSOR_PREFIX}{scan_prefix}{segment}/\u{10ffff}");
                children.push((child_entry, child_cursor));
                current_segment_visible = true;
                if children.len() > limit.get() {
                    let next_cursor = children.get(limit.get().saturating_sub(1)).map(|(_, cursor)| cursor.clone());
                    children.truncate(limit.get());
                    return Ok(TableCatalogListPage {
                        entries: children.into_iter().map(|(entry, _)| entry).collect(),
                        next_cursor,
                    });
                }
            }

            if !page.is_truncated {
                return Ok(TableCatalogListPage {
                    entries: children.into_iter().map(|(entry, _)| entry).collect(),
                    next_cursor: None,
                });
            }
            let next = match (current_segment_visible, current_segment) {
                (true, Some(segment)) => format!("{scan_prefix}{segment}/\u{10ffff}"),
                _ => last_scanned.ok_or_else(|| {
                    TableCatalogStoreError::Internal("catalog namespace child scan made no progress".to_string())
                })?,
            };
            if scan_cursor.as_deref().is_some_and(|cursor| next.as_str() <= cursor) {
                return Err(TableCatalogStoreError::Internal(
                    "catalog namespace child scan did not advance".to_string(),
                ));
            }
            scan_cursor = Some(next);
        }
    }

    async fn list_active_namespaces_with_prefix(&self, prefix: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        let mut entries = Vec::new();
        for object in self.backend.list_objects(self.catalog_bucket(), prefix).await? {
            if !object.ends_with(NAMESPACE_ENTRY_FILE) {
                continue;
            }
            if let Some((entry, _)) = self.read_entry::<NamespaceEntry>(self.catalog_bucket(), &object).await? {
                validate_namespace_entry_object(&self.paths, &object, &entry)?;
                if entry.state == TableCatalogEntryState::Active {
                    entries.push(entry);
                }
            }
        }
        entries.sort_by(|left, right| left.namespace.cmp(&right.namespace));
        Ok(entries)
    }

    async fn collect_table_maintenance_rename_objects(
        &self,
        entry: &TableEntry,
    ) -> TableCatalogStoreResult<BTreeMap<String, TableMaintenanceRenameObject>> {
        let prefix = maintenance_rename_prefix(entry)?;
        let limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS)
            .ok_or_else(|| TableCatalogStoreError::Internal("maintenance rename page limit must be positive".to_string()))?;
        let mut cursor: Option<String> = None;
        let mut objects = BTreeMap::new();
        let mut total_bytes = 0usize;
        loop {
            let page = self
                .backend
                .list_objects_page(self.catalog_bucket(), &prefix, cursor.as_deref(), limit)
                .await?;
            if page.objects.len() > limit.get() || (page.is_truncated && page.objects.is_empty()) {
                return Err(TableCatalogStoreError::Invalid("invalid maintenance rename listing page".to_string()));
            }
            for object in page.objects {
                if cursor.as_ref().is_some_and(|cursor| object <= *cursor) {
                    return Err(TableCatalogStoreError::Invalid("maintenance rename listing did not advance".to_string()));
                }
                let suffix = object
                    .strip_prefix(&prefix)
                    .filter(|suffix| maintenance_rename_suffix_is_valid(suffix))
                    .ok_or_else(|| {
                        TableCatalogStoreError::Invalid("maintenance rename listing escaped its owner paths".to_string())
                    })?;
                if objects.len() >= TABLE_MAINTENANCE_RENAME_MAX_OBJECTS {
                    return Err(TableCatalogStoreError::Invalid(
                        "maintenance rename object count exceeds its limit".to_string(),
                    ));
                }
                let value = self
                    .backend
                    .read_object_limited(self.catalog_bucket(), &object, TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_OBJECT_SIZE)
                    .await?
                    .ok_or_else(|| {
                        TableCatalogStoreError::Conflict("maintenance rename object disappeared during collection".to_string())
                    })?;
                total_bytes = total_bytes.saturating_add(value.data.len());
                if total_bytes > TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_BYTES {
                    return Err(TableCatalogStoreError::Invalid(
                        "maintenance rename content exceeds its byte limit".to_string(),
                    ));
                }
                let etag = value
                    .etag
                    .filter(|etag| !etag.is_empty())
                    .ok_or_else(|| TableCatalogStoreError::Conflict("maintenance rename object has no ETag".to_string()))?;
                let data = serde_json::from_slice(&value.data)
                    .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid maintenance rename JSON: {err}")))?;
                maintenance_rename_data(entry, entry, suffix, &data)?;
                objects.insert(suffix.to_string(), TableMaintenanceRenameObject { etag, data });
                cursor = Some(object);
            }
            if !page.is_truncated {
                return Ok(objects);
            }
        }
    }

    pub(super) async fn read_table_maintenance_rename_intent(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Option<(TableMaintenanceRenameIntent, String)>> {
        let object = maintenance_rename_intent_path(table_bucket);
        let Some(value) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &object, TABLE_CATALOG_BACKUP_MAX_SIZE)
            .await?
        else {
            return Ok(None);
        };
        let intent: TableMaintenanceRenameIntent = serde_json::from_slice(&value.data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid maintenance rename intent: {err}")))?;
        let etag = value
            .etag
            .filter(|etag| !etag.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Conflict("maintenance rename intent has no ETag".to_string()))?;
        validate_table_entry_version_and_id(&intent.source)?;
        validate_table_entry_version_and_id(&intent.destination)?;
        let mut expected_destination = intent.source.clone();
        expected_destination.namespace.clone_from(&intent.destination.namespace);
        expected_destination.table.clone_from(&intent.destination.table);
        expected_destination.updated_at.clone_from(&intent.destination.updated_at);
        if intent.version != 1
            || intent.source_snapshot_etag.is_empty()
            || intent.source.table_bucket != table_bucket
            || intent.source.state != TableCatalogEntryState::Active
            || intent.destination != expected_destination
            || (intent.source.namespace == intent.destination.namespace && intent.source.table == intent.destination.table)
            || intent.source_objects.len() > TABLE_MAINTENANCE_RENAME_MAX_OBJECTS
            || intent.destination_etags.len() > TABLE_MAINTENANCE_RENAME_MAX_OBJECTS
        {
            return Err(TableCatalogStoreError::Invalid(
                "invalid maintenance rename intent identity, version, or count".to_string(),
            ));
        }
        maintenance_rename_prefix(&intent.source)?;
        maintenance_rename_prefix(&intent.destination)?;
        let mut total_bytes = 0usize;
        for (suffix, value) in &intent.source_objects {
            let size = serde_json::to_vec(&value.data)
                .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid maintenance rename data: {err}")))?
                .len();
            total_bytes = total_bytes.saturating_add(size);
            if value.etag.is_empty()
                || size > TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_OBJECT_SIZE
                || total_bytes > TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_BYTES
            {
                return Err(TableCatalogStoreError::Invalid(
                    "maintenance rename intent object exceeds its limits".to_string(),
                ));
            }
            maintenance_rename_data(&intent.source, &intent.destination, suffix, &value.data)?;
        }
        if intent
            .destination_etags
            .iter()
            .any(|(suffix, etag)| !maintenance_rename_suffix_is_valid(suffix) || etag.is_empty())
        {
            return Err(TableCatalogStoreError::Invalid(
                "invalid maintenance rename destination watermark".to_string(),
            ));
        }
        Ok(Some((intent, etag)))
    }

    pub(super) async fn prepare_table_maintenance_rename(
        &self,
        source: &TableEntry,
        destination: &TableEntry,
        source_snapshot_etag: &str,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<Option<(TableMaintenanceRenameIntent, String)>> {
        fence.ensure_held()?;
        let saved = self.read_table_maintenance_rename_intent(&source.table_bucket).await?;
        let (intent, intent_etag) = if let Some((intent, etag)) = saved {
            if intent.source != *source
                || intent.destination != *destination
                || intent.source_snapshot_etag != source_snapshot_etag
            {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance rename intent belongs to a different catalog rename".to_string(),
                ));
            }
            (intent, etag)
        } else {
            let source_objects = self.collect_table_maintenance_rename_objects(source).await?;
            let destination_objects = self.collect_table_maintenance_rename_objects(destination).await?;
            // Persist even an empty plan so a missing intent cannot be mistaken
            // for a rename whose source never had maintenance state.
            let intent = TableMaintenanceRenameIntent {
                version: 1,
                source: source.clone(),
                destination: destination.clone(),
                source_snapshot_etag: source_snapshot_etag.to_string(),
                source_objects,
                destination_etags: destination_objects
                    .into_iter()
                    .map(|(key, value)| (key, value.etag))
                    .collect(),
            };
            let object = maintenance_rename_intent_path(&source.table_bucket);
            let data = serde_json::to_vec(&intent).map_err(|err| {
                TableCatalogStoreError::Internal(format!("failed to serialize maintenance rename intent: {err}"))
            })?;
            if data.len() > TABLE_CATALOG_BACKUP_MAX_SIZE {
                return Err(TableCatalogStoreError::Invalid(
                    "maintenance rename intent exceeds its byte limit".to_string(),
                ));
            }
            self.backend
                .put_object_fenced(self.catalog_bucket(), &object, data, TableCatalogPutPrecondition::IfAbsent, fence)
                .await?;
            let (persisted, etag) = self
                .read_table_maintenance_rename_intent(&source.table_bucket)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Conflict("maintenance rename intent disappeared".to_string()))?;
            if persisted != intent {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance rename intent changed during preparation".to_string(),
                ));
            }
            (intent, etag)
        };
        Ok(Some((intent, intent_etag)))
    }

    async fn copy_table_maintenance_rename(
        &self,
        intent: &TableMaintenanceRenameIntent,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let source = &intent.source;
        let destination = &intent.destination;
        let current_source = self.collect_table_maintenance_rename_objects(source).await?;
        if current_source != intent.source_objects {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance rename source changed before catalog publication".to_string(),
            ));
        }
        let destination_prefix = maintenance_rename_prefix(destination)?;
        for (suffix, value) in &intent.source_objects {
            let renamed = maintenance_rename_data(source, destination, suffix, &value.data)?;
            let object = format!("{destination_prefix}{suffix}");
            let current = self
                .backend
                .read_object_limited(self.catalog_bucket(), &object, TABLE_CATALOG_BACKUP_MAX_MAINTENANCE_OBJECT_SIZE)
                .await?;
            let precondition = match current {
                Some(current) => {
                    let data: serde_json::Value = serde_json::from_slice(&current.data).map_err(|_| {
                        TableCatalogStoreError::Conflict("maintenance rename destination changed before publication".to_string())
                    })?;
                    if data == renamed {
                        continue;
                    }
                    let expected = intent.destination_etags.get(suffix).ok_or_else(|| {
                        TableCatalogStoreError::Conflict("maintenance rename destination appeared after preparation".to_string())
                    })?;
                    if current.etag.as_deref() != Some(expected.as_str()) {
                        return Err(TableCatalogStoreError::Conflict(
                            "maintenance rename destination watermark changed".to_string(),
                        ));
                    }
                    TableCatalogPutPrecondition::IfMatch(expected.clone())
                }
                None if !intent.destination_etags.contains_key(suffix) => TableCatalogPutPrecondition::IfAbsent,
                None => {
                    return Err(TableCatalogStoreError::Conflict(
                        "maintenance rename destination disappeared after preparation".to_string(),
                    ));
                }
            };
            let data = serde_json::to_vec(&renamed).map_err(|err| {
                TableCatalogStoreError::Internal(format!("failed to serialize renamed maintenance object: {err}"))
            })?;
            self.backend
                .put_object_fenced(self.catalog_bucket(), &object, data, precondition, fence)
                .await?;
        }
        for (suffix, expected) in &intent.destination_etags {
            if intent.source_objects.contains_key(suffix) {
                continue;
            }
            let object = format!("{destination_prefix}{suffix}");
            let Some(current) = self.backend.object_metadata(self.catalog_bucket(), &object).await? else {
                continue;
            };
            if current.etag.as_deref() != Some(expected.as_str()) {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance rename stale destination watermark changed".to_string(),
                ));
            }
            self.backend
                .delete_object_if_match_fenced(self.catalog_bucket(), &object, expected, fence)
                .await?;
        }
        if self.collect_table_maintenance_rename_objects(source).await? != intent.source_objects {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance rename source changed before catalog publication".to_string(),
            ));
        }
        self.verify_table_maintenance_rename_destination(intent).await?;
        fence.ensure_held()
    }

    async fn verify_table_maintenance_rename_destination(
        &self,
        intent: &TableMaintenanceRenameIntent,
    ) -> TableCatalogStoreResult<()> {
        let current = self.collect_table_maintenance_rename_objects(&intent.destination).await?;
        if current.len() != intent.source_objects.len() {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance rename destination is not a complete mirror".to_string(),
            ));
        }
        for (suffix, saved) in &intent.source_objects {
            if current.get(suffix).map(|value| &value.data)
                != Some(&maintenance_rename_data(&intent.source, &intent.destination, suffix, &saved.data)?)
            {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance rename destination changed before publication".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn finish_table_maintenance_rename(
        &self,
        intent: &TableMaintenanceRenameIntent,
        committed: bool,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        if committed {
            self.verify_table_maintenance_rename_destination(intent).await?;
        }
        let authoritative = if committed { &intent.destination } else { &intent.source };
        let authoritative_objects = self.collect_table_maintenance_rename_objects(authoritative).await?;
        if intent
            .source_objects
            .keys()
            .any(|suffix| !authoritative_objects.contains_key(suffix))
        {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance rename authoritative records are incomplete; repair persisted state before retrying rename"
                    .to_string(),
            ));
        }
        let entry = if committed { &intent.source } else { &intent.destination };
        let prefix = maintenance_rename_prefix(entry)?;
        let current = self.collect_table_maintenance_rename_objects(entry).await?;
        // Validate the whole cleanup set before the first delete. A failed or interrupted copy
        // is recognizable by its exact planned JSON; every deletion still uses the observed ETag.
        for (suffix, value) in &current {
            let allowed = if committed {
                intent
                    .source_objects
                    .get(suffix)
                    .is_some_and(|saved| saved.etag == value.etag)
            } else if intent.destination_etags.get(suffix) == Some(&value.etag) {
                true
            } else if let Some(saved) = intent.source_objects.get(suffix) {
                value.data == maintenance_rename_data(&intent.source, &intent.destination, suffix, &saved.data)?
            } else {
                false
            };
            if !allowed {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance rename cleanup encountered a changed object watermark".to_string(),
                ));
            }
        }
        fence.ensure_held()?;
        begin_publication_mutation()?;
        for (suffix, value) in current {
            self.backend
                .delete_object_if_match_fenced(self.catalog_bucket(), &format!("{prefix}{suffix}"), &value.etag, fence)
                .await?;
        }
        fence.ensure_held()
    }

    async fn delete_table_maintenance_rename_intent(
        &self,
        intent: &TableMaintenanceRenameIntent,
        intent_etag: &str,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let object = maintenance_rename_intent_path(&intent.source.table_bucket);
        match self
            .backend
            .delete_object_if_match_fenced(self.catalog_bucket(), &object, intent_etag, fence)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                fence.ensure_held()?;
                if !self.backend.object_exists(self.catalog_bucket(), &object).await? {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub(in crate::table_catalog) async fn read_entry<T>(
        &self,
        bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<(T, Option<String>)>>
    where
        T: DeserializeOwned,
    {
        self.read_entry_with(bucket, object, |backend, bucket, object| {
            Box::pin(async move { backend.read_object(bucket, object).await })
        })
        .await
    }

    pub(in crate::table_catalog) async fn read_entry_unlocked<T>(
        &self,
        bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<(T, Option<String>)>>
    where
        T: DeserializeOwned,
    {
        self.read_entry_with(bucket, object, |backend, bucket, object| {
            Box::pin(async move { backend.read_object_unlocked(bucket, object).await })
        })
        .await
    }

    async fn read_entry_with<'a, T, F>(
        &'a self,
        bucket: &'a str,
        object: &'a str,
        read_object: F,
    ) -> TableCatalogStoreResult<Option<(T, Option<String>)>>
    where
        T: DeserializeOwned,
        F: FnOnce(
            &'a B,
            &'a str,
            &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TableCatalogStoreResult<Option<TableCatalogObject>>> + Send + 'a>,
        >,
    {
        let Some(object_data) = read_object(&self.backend, bucket, object).await? else {
            return Ok(None);
        };

        let entry = serde_json::from_slice(&object_data.data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to parse catalog entry {object}: {err}")))?;
        Ok(Some((entry, object_data.etag)))
    }

    pub(in crate::table_catalog) async fn write_entry<T>(
        &self,
        bucket: &str,
        object: &str,
        entry: &T,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()>
    where
        T: Serialize,
    {
        let data = serde_json::to_vec(entry)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to serialize catalog entry {object}: {err}")))?;
        begin_publication_mutation()?;
        self.backend.put_object(bucket, object, data, precondition).await
    }

    async fn write_maintenance_entry<T>(
        &self,
        object: &str,
        value: &T,
        entry: &TableEntry,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<()>
    where
        T: Serialize,
    {
        let Some(fence) = table_maintenance_mutation_fence(publication, entry)? else {
            return self
                .write_entry(self.catalog_bucket(), object, value, TableCatalogPutPrecondition::Any)
                .await;
        };
        let precondition = match self.backend.object_metadata(self.catalog_bucket(), object).await? {
            None => TableCatalogPutPrecondition::IfAbsent,
            Some(current) => TableCatalogPutPrecondition::IfMatch(current.etag.ok_or_else(|| {
                TableCatalogStoreError::Conflict(format!("maintenance sidecar has no conditional-write ETag: {object}"))
            })?),
        };
        let data = serde_json::to_vec(value)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to serialize catalog entry {object}: {err}")))?;
        self.backend
            .put_object_fenced(self.catalog_bucket(), object, data, precondition, &fence)
            .await
    }

    pub(in crate::table_catalog) async fn write_entry_unlocked<T>(
        &self,
        bucket: &str,
        object: &str,
        entry: &T,
        precondition: TableCatalogPutPrecondition,
        write_guards: Vec<WriteCommitGuard>,
    ) -> TableCatalogStoreResult<()>
    where
        T: Serialize,
    {
        self.write_entry_unlocked_with_fence(bucket, object, entry, precondition, write_guards, None)
            .await
    }

    async fn write_entry_unlocked_with_fence<T>(
        &self,
        bucket: &str,
        object: &str,
        entry: &T,
        precondition: TableCatalogPutPrecondition,
        write_guards: Vec<WriteCommitGuard>,
        fence: Option<&TableCatalogObjectMutationFence>,
    ) -> TableCatalogStoreResult<()>
    where
        T: Serialize,
    {
        let data = serde_json::to_vec(entry)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to serialize catalog entry {object}: {err}")))?;
        if let Some(fence) = fence {
            fence.ensure_held()?;
        }
        begin_publication_mutation()?;
        self.prepare_table_directory_candidate(bucket, object, &data).await?;
        match fence {
            Some(fence) => {
                self.backend
                    .put_object_unlocked_fenced(bucket, object, data, precondition, write_guards, fence)
                    .await
            }
            None => {
                self.backend
                    .put_object_unlocked(bucket, object, data, precondition, write_guards)
                    .await
            }
        }
    }

    async fn write_exact_entry_unlocked<T>(
        &self,
        bucket: &str,
        object: &str,
        entry: &T,
        precondition: TableCatalogPutPrecondition,
        write_guards: Vec<WriteCommitGuard>,
    ) -> TableCatalogStoreResult<String>
    where
        T: DeserializeOwned + PartialEq + Serialize,
    {
        self.write_exact_entry_unlocked_with_fence(bucket, object, entry, precondition, write_guards, None)
            .await
    }

    async fn write_exact_entry_unlocked_with_fence<T>(
        &self,
        bucket: &str,
        object: &str,
        entry: &T,
        precondition: TableCatalogPutPrecondition,
        write_guards: Vec<WriteCommitGuard>,
        fence: Option<&TableCatalogObjectMutationFence>,
    ) -> TableCatalogStoreResult<String>
    where
        T: DeserializeOwned + PartialEq + Serialize,
    {
        let write_result = self
            .write_entry_unlocked_with_fence(bucket, object, entry, precondition, write_guards, fence)
            .await;
        let current = self.read_entry_unlocked::<T>(bucket, object).await?;
        match current {
            Some((current, Some(etag))) if current == *entry => Ok(etag),
            Some((current, None)) if current == *entry => Err(TableCatalogStoreError::Internal(format!(
                "catalog entry has no etag after write: {object}"
            ))),
            _ => match write_result {
                Ok(()) => Err(TableCatalogStoreError::Internal(format!(
                    "catalog entry does not match the completed write: {object}"
                ))),
                Err(err) => Err(err),
            },
        }
    }

    pub(super) async fn read_table_bucket_with_etag_unlocked(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Option<(TableBucketEntry, String)>> {
        let object = self.paths.table_bucket_entry_path(table_bucket);
        let Some((entry, etag)) = self
            .read_entry_unlocked::<TableBucketEntry>(self.catalog_bucket(), &object)
            .await?
        else {
            return Ok(None);
        };
        validate_table_bucket_entry_object(&self.paths, &object, &entry)?;
        self.validate_store_bucket_entry(&entry)?;
        let Some(etag) = etag else {
            return Err(TableCatalogStoreError::Internal(format!(
                "catalog table bucket entry has no etag: {object}"
            )));
        };
        Ok(Some((entry, etag)))
    }

    pub(in crate::table_catalog) async fn ensure_no_active_table_rename(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<()> {
        self.table_rename_read_version(table_bucket).await.map(|_| ())
    }

    async fn table_rename_read_version(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<String>> {
        self.table_rename_read_snapshot(table_bucket)
            .await
            .map(|snapshot| snapshot.map(|(_, etag)| etag))
    }

    async fn table_rename_read_snapshot(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Option<(TableBucketEntry, String)>> {
        let object = self.paths.table_bucket_entry_path(table_bucket);
        let Some((entry, etag)) = self.read_entry::<TableBucketEntry>(self.catalog_bucket(), &object).await? else {
            return Ok(None);
        };
        validate_table_bucket_entry_object(&self.paths, &object, &entry)?;
        self.validate_store_bucket_entry(&entry)?;
        if let Some(rename_id) = entry.active_rename_id {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "table bucket {table_bucket} has an active table rename {rename_id}"
            )));
        }
        let etag =
            etag.ok_or_else(|| TableCatalogStoreError::Internal(format!("catalog table bucket entry has no etag: {object}")))?;
        Ok(Some((entry, etag)))
    }

    async fn table_data_plane_read_version(&self, table_bucket: &str) -> TableCatalogStoreResult<String> {
        let Some((entry, etag)) = self.table_rename_read_snapshot(table_bucket).await? else {
            return Err(TableCatalogStoreError::Internal(format!(
                "object-backed catalog has no entry for table-enabled bucket {table_bucket}"
            )));
        };
        if entry.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::Internal(format!(
                "table-enabled bucket {table_bucket} has an inactive object-backed catalog entry"
            )));
        }
        Ok(etag)
    }

    async fn finish_table_rename_read(&self, table_bucket: &str, expected_version: Option<&str>) -> TableCatalogStoreResult<()> {
        let object = self.paths.table_bucket_entry_path(table_bucket);
        let current_version =
            match self.backend.object_metadata(self.catalog_bucket(), &object).await? {
                None => None,
                Some(metadata) => Some(metadata.etag.ok_or_else(|| {
                    TableCatalogStoreError::Internal(format!("catalog table bucket entry has no etag: {object}"))
                })?),
            };
        if current_version.as_deref() != expected_version {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "table bucket {table_bucket} changed while reading the table catalog"
            )));
        }
        Ok(())
    }

    async fn acquire_catalog_write_locks(&self, mut objects: Vec<String>) -> TableCatalogStoreResult<Vec<TableCatalogLockGuard>> {
        objects.sort_unstable();
        objects.dedup();
        let mut guards = Vec::with_capacity(objects.len());
        for object in objects {
            guards.push(self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?);
        }
        Ok(guards)
    }

    async fn advance_table_rename_intent_unlocked(
        &self,
        object: &str,
        intent: &mut TableRenameIntent,
        etag: String,
        state: TableRenameIntentState,
        write_guards: Vec<WriteCommitGuard>,
    ) -> TableCatalogStoreResult<String> {
        if intent.state >= state {
            return Ok(etag);
        }
        intent.state = state;
        intent.updated_at = OffsetDateTime::now_utc().to_string();
        self.write_exact_entry_unlocked(
            self.catalog_bucket(),
            object,
            intent,
            TableCatalogPutPrecondition::IfMatch(etag),
            write_guards,
        )
        .await
    }

    /// Advances by bucket directories, regardless of the number of table objects.
    /// The cursor advances before decoding so one invalid bucket cannot starve later buckets.
    pub(crate) async fn next_table_bucket_for_rename_recovery(
        &self,
        cursor: &mut Option<String>,
    ) -> TableCatalogStoreResult<Option<TableBucketEntry>> {
        if self.paths.scope != CatalogScope::Table {
            return Err(TableCatalogStoreError::Unsupported(
                "table rename recovery requires Table scope".to_string(),
            ));
        }
        let prefix = self.paths.table_bucket_entries_prefix();
        let page = self
            .backend
            .list_objects_page(self.catalog_bucket(), &prefix, cursor.as_deref(), NonZeroUsize::MIN)
            .await?;
        let Some(object) = page.objects.first() else {
            *cursor = None;
            return Ok(None);
        };
        if cursor.as_deref().is_some_and(|previous| object.as_str() <= previous) {
            return Err(TableCatalogStoreError::Internal("catalog bucket scan did not advance".to_string()));
        }
        *cursor = Some(object.clone());
        let directory = object
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.split_once('/'))
            .map(|(directory, _)| directory)
            .ok_or_else(|| TableCatalogStoreError::Invalid("invalid catalog bucket object path".to_string()))?;
        *cursor = Some(format!("{prefix}{directory}/\u{10ffff}"));
        let entry_object = format!("{prefix}{directory}/{TABLE_BUCKET_ENTRY_FILE}");
        let (entry, _) = self
            .read_entry::<TableBucketEntry>(self.catalog_bucket(), &entry_object)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Invalid("catalog bucket entry missing during discovery".to_string()))?;
        validate_table_bucket_entry_object(&self.paths, &entry_object, &entry)?;
        Ok(Some(entry))
    }

    /// Uses the same restore fence, publication lease and state machine as RenameTable.
    pub(crate) fn recover_pending_table_rename<'a>(
        &'a self,
        table_bucket: &'a str,
    ) -> futures::future::BoxFuture<'a, TableCatalogStoreResult<()>> {
        // Keep the publication future opaque to the background concurrency adapters.
        Box::pin(async move {
            if self.paths.scope != CatalogScope::Table {
                return Err(TableCatalogStoreError::Unsupported(
                    "table rename recovery requires Table scope".to_string(),
                ));
            }
            let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
            publication
                .attach_catalog_migration_read_guard(self.acquire_object_backed_catalog_write_permit(table_bucket).await?)?;
            let _completion = TableCommitPublicationCompletion::new(&publication);
            let _migration_guard = self
                .begin_table_bucket_with_migration_permit(table_bucket, &publication)
                .await?;
            self.recover_active_table_rename(table_bucket, &publication).await
        })
    }

    pub(in crate::table_catalog) async fn recover_active_table_rename(
        &self,
        table_bucket: &str,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        publication_recovery(self.recover_active_table_rename_inner(table_bucket, publication)).await
    }

    async fn recover_active_table_rename_inner(
        &self,
        table_bucket: &str,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        if !publication.holds_table_bucket(table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "table rename recovery requires a table-bucket publication fence".to_string(),
            ));
        }
        let bucket_object = self.paths.table_bucket_entry_path(table_bucket);
        let Some((observed_bucket, _)) = self
            .read_entry::<TableBucketEntry>(self.catalog_bucket(), &bucket_object)
            .await?
        else {
            // Recovery only owns an active rename advertised by the bucket
            // entry. Callers retain their existing validation when no entry
            // exists.
            return Ok(());
        };
        validate_table_bucket_entry_object(&self.paths, &bucket_object, &observed_bucket)?;
        if observed_bucket.active_rename_id.is_none() {
            return Ok(());
        }
        let _bucket_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &bucket_object).await?;
        let Some((bucket_entry, _)) = self.read_table_bucket_with_etag_unlocked(table_bucket).await? else {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
        };
        let Some(rename_id) = bucket_entry.active_rename_id.clone() else {
            return Ok(());
        };
        let intent_object = self.paths.table_rename_intent_path(table_bucket, &rename_id);
        let _intent_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &intent_object).await?;
        let Some((mut intent, mut intent_etag)) = self
            .read_entry_unlocked::<TableRenameIntent>(self.catalog_bucket(), &intent_object)
            .await?
        else {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "active table rename is missing its durable intent: {intent_object}"
            )));
        };
        let Some(mut intent_etag) = intent_etag.take() else {
            return Err(TableCatalogStoreError::Internal(format!(
                "catalog table rename intent has no etag: {intent_object}"
            )));
        };
        validate_table_rename_intent_object(&self.paths, &intent_object, &intent)?;
        if intent.table_bucket != table_bucket || intent.rename_id != rename_id {
            return Err(TableCatalogStoreError::Invalid(
                "active table rename does not belong to its table bucket fence".to_string(),
            ));
        }
        if bucket_entry.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::Conflict(format!(
                "table bucket {table_bucket} became inactive during table rename recovery"
            )));
        }

        let source_namespace = parse_namespace_for_store(&intent.source.namespace)?;
        let source_table = parse_table_for_store(&intent.source.table)?;
        let destination_namespace = parse_namespace_for_store(&intent.destination.namespace)?;
        let destination_table = parse_table_for_store(&intent.destination.table)?;
        let source_object = self.paths.table_entry_path(table_bucket, &source_namespace, &source_table);
        let destination_object = self
            .paths
            .table_entry_path(table_bucket, &destination_namespace, &destination_table);
        let destination_view_object = self
            .paths
            .view_entry_path(table_bucket, &destination_namespace, &destination_table);
        let index = table_warehouse_index_entry(&intent.source)?;
        let index_object = self
            .paths
            .warehouse_index_entry_path(table_bucket, &index.warehouse_object_prefix);
        let _catalog_guards = self
            .acquire_catalog_write_locks(vec![
                self.paths.namespace_entry_path(table_bucket, &source_namespace),
                self.paths.namespace_entry_path(table_bucket, &destination_namespace),
                source_object.clone(),
                destination_object.clone(),
                destination_view_object.clone(),
                index_object.clone(),
            ])
            .await?;
        if !publication.holds_table_bucket(table_bucket) {
            return Err(TableCatalogStoreError::Unavailable(
                "table-bucket publication fence was lost during table rename recovery".to_string(),
            ));
        }
        self.require_active_namespace_unlocked(
            table_bucket,
            &source_namespace,
            &self.paths.namespace_entry_path(table_bucket, &source_namespace),
        )
        .await
        .map_err(|err| match err {
            TableCatalogStoreError::NotFound(_) => {
                TableCatalogStoreError::Conflict("table rename source namespace disappeared during recovery".to_string())
            }
            err => err,
        })?;
        self.require_active_namespace_unlocked(
            table_bucket,
            &destination_namespace,
            &self.paths.namespace_entry_path(table_bucket, &destination_namespace),
        )
        .await
        .map_err(|err| match err {
            TableCatalogStoreError::NotFound(_) => {
                TableCatalogStoreError::Conflict("table rename destination namespace disappeared during recovery".to_string())
            }
            err => err,
        })?;
        if self
            .read_entry_unlocked::<ViewEntry>(self.catalog_bucket(), &destination_view_object)
            .await?
            .is_some()
        {
            return Err(TableCatalogStoreError::Conflict(
                "table rename destination became a view during recovery".to_string(),
            ));
        }

        let mutation_fence = publication
            .object_mutation_fence()
            .ok_or_else(|| TableCatalogStoreError::Internal("table rename requires a storage mutation fence".to_string()))?;
        let maintenance = if intent.state == TableRenameIntentState::Prepared {
            self.prepare_table_maintenance_rename(&intent.source, &intent.destination, &intent.source_etag, &mutation_fence)
                .await
                .map_err(maintenance_rename_recovery_error)?
        } else {
            let saved = self
                .read_table_maintenance_rename_intent(table_bucket)
                .await
                .map_err(maintenance_rename_recovery_error)?;
            if let Some((saved, _)) = saved.as_ref() {
                if saved.source != intent.source
                    || saved.destination != intent.destination
                    || saved.source_snapshot_etag != intent.source_etag
                {
                    return Err(maintenance_rename_recovery_error(TableCatalogStoreError::Conflict(
                        "maintenance rename intent does not match the active catalog rename".to_string(),
                    )));
                }
            } else if intent.state != TableRenameIntentState::Completed {
                return Err(maintenance_rename_recovery_error(TableCatalogStoreError::Unavailable(
                    "active catalog rename is missing its maintenance intent".to_string(),
                )));
            }
            saved
        };
        if intent.state == TableRenameIntentState::Prepared {
            // This phase distinguishes a never-prepared rename from a lost
            // sidecar intent after copying has become authorized.
            intent_etag = self
                .advance_table_rename_intent_unlocked(
                    &intent_object,
                    &mut intent,
                    intent_etag,
                    TableRenameIntentState::MaintenancePrepared,
                    _intent_guard.write_commit_guards(),
                )
                .await?;
        }
        if intent.state < TableRenameIntentState::DestinationPublished
            && let Some((maintenance, _)) = maintenance.as_ref()
        {
            self.copy_table_maintenance_rename(maintenance, &mutation_fence)
                .await
                .map_err(maintenance_rename_recovery_error)?;
        }

        let mut source_fence = intent.source.clone();
        source_fence.state = TableCatalogEntryState::Renaming;
        // Keep the source object as a conditional-replacement tombstone instead of relying on an unconditional delete.
        let mut source_tombstone = intent.source.clone();
        source_tombstone.state = TableCatalogEntryState::Deleted;
        source_tombstone.updated_at = Some(intent.created_at.clone());
        match self
            .read_table_with_etag_unlocked(table_bucket, &source_namespace, &source_table)
            .await?
        {
            Some((current, _)) if current == source_fence || current == source_tombstone => {}
            Some((current, current_etag)) if current == intent.source && current_etag == intent.source_etag => {
                self.write_exact_entry_unlocked_with_fence(
                    self.catalog_bucket(),
                    &source_object,
                    &source_fence,
                    TableCatalogPutPrecondition::IfMatch(current_etag),
                    _catalog_guards
                        .iter()
                        .flat_map(TableCatalogLockGuard::write_commit_guards)
                        .collect(),
                    Some(&mutation_fence),
                )
                .await?;
            }
            _ => {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table rename source changed during recovery: {table_bucket}/{}/{}",
                    source_namespace.public_name(),
                    source_table.as_str()
                )));
            }
        }
        intent_etag = self
            .advance_table_rename_intent_unlocked(
                &intent_object,
                &mut intent,
                intent_etag,
                TableRenameIntentState::SourceFenced,
                _intent_guard.write_commit_guards(),
            )
            .await?;

        let mut destination_fence = intent.destination.clone();
        destination_fence.state = TableCatalogEntryState::Renaming;
        match self
            .read_table_with_etag_unlocked(table_bucket, &destination_namespace, &destination_table)
            .await?
        {
            Some((current, _)) if current == destination_fence || current == intent.destination => {}
            None if intent.destination_etag.is_none() => {
                self.write_exact_entry_unlocked_with_fence(
                    self.catalog_bucket(),
                    &destination_object,
                    &destination_fence,
                    TableCatalogPutPrecondition::IfAbsent,
                    _catalog_guards
                        .iter()
                        .flat_map(TableCatalogLockGuard::write_commit_guards)
                        .collect(),
                    Some(&mutation_fence),
                )
                .await?;
            }
            Some((current, current_etag))
                if current.state == TableCatalogEntryState::Deleted
                    && Some(current_etag.as_str()) == intent.destination_etag.as_deref() =>
            {
                self.write_exact_entry_unlocked_with_fence(
                    self.catalog_bucket(),
                    &destination_object,
                    &destination_fence,
                    TableCatalogPutPrecondition::IfMatch(current_etag),
                    _catalog_guards
                        .iter()
                        .flat_map(TableCatalogLockGuard::write_commit_guards)
                        .collect(),
                    Some(&mutation_fence),
                )
                .await?;
            }
            _ => {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table rename destination changed during recovery: {table_bucket}/{}/{}",
                    destination_namespace.public_name(),
                    destination_table.as_str()
                )));
            }
        }
        intent_etag = self
            .advance_table_rename_intent_unlocked(
                &intent_object,
                &mut intent,
                intent_etag,
                TableRenameIntentState::DestinationWritten,
                _intent_guard.write_commit_guards(),
            )
            .await?;

        match self
            .read_table_with_etag_unlocked(table_bucket, &source_namespace, &source_table)
            .await?
        {
            Some((current, _)) if current == source_tombstone => {}
            Some((current, current_etag)) if current == source_fence => {
                self.write_exact_entry_unlocked_with_fence(
                    self.catalog_bucket(),
                    &source_object,
                    &source_tombstone,
                    TableCatalogPutPrecondition::IfMatch(current_etag),
                    _catalog_guards
                        .iter()
                        .flat_map(TableCatalogLockGuard::write_commit_guards)
                        .collect(),
                    Some(&mutation_fence),
                )
                .await?;
            }
            _ => {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table rename source changed during recovery: {table_bucket}/{}/{}",
                    source_namespace.public_name(),
                    source_table.as_str()
                )));
            }
        }
        intent_etag = self
            .advance_table_rename_intent_unlocked(
                &intent_object,
                &mut intent,
                intent_etag,
                TableRenameIntentState::SourceTombstoned,
                _intent_guard.write_commit_guards(),
            )
            .await?;

        let destination_index = table_warehouse_index_entry(&intent.destination)?;
        let Some((current_index, current_index_etag)) = self
            .read_entry_unlocked::<TableWarehouseIndexEntry>(self.catalog_bucket(), &index_object)
            .await?
        else {
            return Err(TableCatalogStoreError::Conflict(
                "table rename warehouse index disappeared during recovery".to_string(),
            ));
        };
        validate_table_warehouse_index_entry_object(&self.paths, &index_object, &current_index)?;
        if current_index != destination_index {
            if current_index != index || current_index_etag.as_deref() != Some(intent.warehouse_index_etag.as_str()) {
                return Err(TableCatalogStoreError::Conflict(
                    "table rename warehouse index changed during recovery".to_string(),
                ));
            }
            self.write_exact_entry_unlocked_with_fence(
                self.catalog_bucket(),
                &index_object,
                &destination_index,
                TableCatalogPutPrecondition::IfMatch(intent.warehouse_index_etag.clone()),
                _catalog_guards
                    .iter()
                    .flat_map(TableCatalogLockGuard::write_commit_guards)
                    .collect(),
                Some(&mutation_fence),
            )
            .await?;
        }
        intent_etag = self
            .advance_table_rename_intent_unlocked(
                &intent_object,
                &mut intent,
                intent_etag,
                TableRenameIntentState::IndexPublished,
                _intent_guard.write_commit_guards(),
            )
            .await?;
        match self
            .read_table_with_etag_unlocked(table_bucket, &destination_namespace, &destination_table)
            .await?
        {
            Some((current, _)) if current == intent.destination => {}
            Some((current, current_etag)) if current == destination_fence => {
                self.write_exact_entry_unlocked_with_fence(
                    self.catalog_bucket(),
                    &destination_object,
                    &intent.destination,
                    TableCatalogPutPrecondition::IfMatch(current_etag),
                    _catalog_guards
                        .iter()
                        .flat_map(TableCatalogLockGuard::write_commit_guards)
                        .collect(),
                    Some(&mutation_fence),
                )
                .await?;
            }
            _ => {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table rename destination changed during recovery: {table_bucket}/{}/{}",
                    destination_namespace.public_name(),
                    destination_table.as_str()
                )));
            }
        }
        intent_etag = self
            .advance_table_rename_intent_unlocked(
                &intent_object,
                &mut intent,
                intent_etag,
                TableRenameIntentState::DestinationPublished,
                _intent_guard.write_commit_guards(),
            )
            .await?;
        if let Some((maintenance, _)) = maintenance.as_ref() {
            self.finish_table_maintenance_rename(maintenance, true, &mutation_fence)
                .await
                .map_err(maintenance_rename_recovery_error)?;
        }
        self.advance_table_rename_intent_unlocked(
            &intent_object,
            &mut intent,
            intent_etag,
            TableRenameIntentState::Completed,
            _intent_guard.write_commit_guards(),
        )
        .await?;
        // The completed record makes an interrupted intent deletion recognizable.
        // Readers remain fenced until sidecar cleanup and finalization are durable.
        if let Some((maintenance, etag)) = maintenance.as_ref() {
            self.delete_table_maintenance_rename_intent(maintenance, etag, &mutation_fence)
                .await
                .map_err(maintenance_rename_recovery_error)?;
        }

        let Some((mut bucket_entry, bucket_etag)) = self.read_table_bucket_with_etag_unlocked(table_bucket).await? else {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
        };
        if bucket_entry.active_rename_id.as_deref() != Some(rename_id.as_str()) {
            return Err(TableCatalogStoreError::Conflict("table rename fence changed during recovery".to_string()));
        }
        bucket_entry.active_rename_id = None;
        bucket_entry.updated_at = Some(next_table_catalog_update_time(bucket_entry.updated_at.as_deref()));
        self.write_exact_entry_unlocked_with_fence(
            self.catalog_bucket(),
            &bucket_object,
            &bucket_entry,
            TableCatalogPutPrecondition::IfMatch(bucket_etag),
            _bucket_guard.write_commit_guards(),
            Some(&mutation_fence),
        )
        .await?;
        Ok(())
    }

    async fn write_warehouse_index_state_unlocked(
        &self,
        table_bucket: &str,
        write_guards: Vec<WriteCommitGuard>,
    ) -> TableCatalogStoreResult<()> {
        let state = TableWarehouseIndexStateEntry {
            version: TABLE_WAREHOUSE_INDEX_STATE_VERSION,
            table_bucket: table_bucket.to_string(),
            state: TableCatalogEntryState::Active,
        };
        self.write_entry_unlocked(
            self.catalog_bucket(),
            &self.paths.warehouse_index_state_path(table_bucket),
            &state,
            TableCatalogPutPrecondition::Any,
            write_guards,
        )
        .await
    }

    pub(in crate::table_catalog) async fn warehouse_index_ready(&self, table_bucket: &str) -> TableCatalogStoreResult<bool> {
        self.with_catalog_restore_read(table_bucket, async {
            let Some((state, _)) = self
                .read_entry::<TableWarehouseIndexStateEntry>(
                    self.catalog_bucket(),
                    &self.paths.warehouse_index_state_path(table_bucket),
                )
                .await?
            else {
                return Ok(false);
            };
            table_warehouse_index_state_ready(&state, table_bucket)
        })
        .await
    }

    async fn warehouse_index_entry_has_active_owner(&self, index: &TableWarehouseIndexEntry) -> TableCatalogStoreResult<bool> {
        if index.state != TableCatalogEntryState::Active {
            return Ok(false);
        }
        let Some(table) = self
            .load_table_entry(&index.table_bucket, &index.namespace, &index.table)
            .await?
        else {
            return Ok(false);
        };
        if table.state != TableCatalogEntryState::Active {
            return Ok(false);
        }
        let current_prefix = table_warehouse_object_prefix(&table)?;
        Ok(table.table_id == index.table_id && current_prefix == index.warehouse_object_prefix)
    }

    async fn delete_warehouse_index_object(
        &self,
        object: &str,
        index: &TableWarehouseIndexEntry,
        reason: &'static str,
    ) -> TableCatalogStoreResult<bool> {
        validate_table_warehouse_index_entry_object(&self.paths, object, index)?;
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), object).await?;
        let Some((current, _)) = self
            .read_entry_unlocked::<TableWarehouseIndexEntry>(self.catalog_bucket(), object)
            .await?
        else {
            return Ok(false);
        };
        validate_table_warehouse_index_entry_object(&self.paths, object, &current)?;
        if current != *index {
            tracing::warn!(
                table_bucket = %index.table_bucket,
                namespace = %index.namespace,
                table = %index.table,
                table_id = %index.table_id,
                warehouse_object_prefix = %index.warehouse_object_prefix,
                current_namespace = %current.namespace,
                current_table = %current.table,
                current_table_id = %current.table_id,
                reason = %reason,
                "skipped deleting table warehouse index because owner changed"
            );
            return Ok(false);
        }
        self.delete_warehouse_index_object_unlocked(object, index, reason).await?;
        Ok(true)
    }

    async fn delete_warehouse_index_object_unlocked(
        &self,
        object: &str,
        index: &TableWarehouseIndexEntry,
        reason: &'static str,
    ) -> TableCatalogStoreResult<()> {
        begin_publication_mutation()?;
        self.backend.delete_object_unlocked(self.catalog_bucket(), object).await?;
        tracing::warn!(
            table_bucket = %index.table_bucket,
            namespace = %index.namespace,
            table = %index.table,
            table_id = %index.table_id,
            warehouse_object_prefix = %index.warehouse_object_prefix,
            reason = %reason,
            "deleted table warehouse index"
        );
        Ok(())
    }

    async fn replace_table_warehouse_index(
        &self,
        object: &str,
        stale: &TableWarehouseIndexEntry,
        replacement: &TableWarehouseIndexEntry,
    ) -> TableCatalogStoreResult<bool> {
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), object).await?;
        let Some((current, current_etag)) = self
            .read_entry_unlocked::<TableWarehouseIndexEntry>(self.catalog_bucket(), object)
            .await?
        else {
            return Ok(false);
        };
        validate_table_warehouse_index_entry_object(&self.paths, object, &current)?;
        if current != *stale {
            return Ok(false);
        }
        let current_etag = current_etag
            .ok_or_else(|| TableCatalogStoreError::Internal(format!("catalog warehouse index has no etag: {object}")))?;
        // A missing index would expose residual data during failed prefix reuse.
        self.write_entry_unlocked(
            self.catalog_bucket(),
            object,
            replacement,
            TableCatalogPutPrecondition::IfMatch(current_etag),
            _guard.write_commit_guards(),
        )
        .await?;
        Ok(true)
    }

    async fn ensure_table_warehouse_prefix_available(
        &self,
        entry: &TableEntry,
        locked_table_entry: Option<&str>,
    ) -> TableCatalogStoreResult<()> {
        let candidate = table_warehouse_index_entry(entry)?;
        validate_table_entry_version_and_id(entry)?;
        for existing in self
            .list_all_table_entries_with_limit(
                &candidate.table_bucket,
                None,
                locked_table_entry,
                TABLE_REGISTRATION_READ_CONCURRENCY,
            )
            .await?
        {
            if existing.state != TableCatalogEntryState::Active {
                continue;
            }
            if existing.table_id == candidate.table_id {
                if existing.namespace != candidate.namespace || existing.table != candidate.table {
                    return Err(TableCatalogStoreError::Conflict(
                        "table id is already registered in this table bucket".to_string(),
                    ));
                }
                continue;
            }
            let existing_prefix = table_warehouse_object_prefix(&existing)?;
            if warehouse_object_prefixes_overlap(&existing_prefix, &candidate.warehouse_object_prefix) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table warehouse location overlaps an active table: {}",
                    candidate.warehouse_object_prefix
                )));
            }
        }
        let state_object = self.paths.warehouse_index_state_path(&candidate.table_bucket);
        let index_objects = self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.warehouse_index_entries_prefix(&candidate.table_bucket))
            .await?;
        stream::iter(index_objects.into_iter().filter(|object| object != &state_object))
            .map(|object| {
                let candidate = &candidate;
                async move {
                    let Some((existing, _)) = self
                        .read_entry::<TableWarehouseIndexEntry>(self.catalog_bucket(), &object)
                        .await?
                    else {
                        return Ok(());
                    };
                    validate_table_warehouse_index_entry_object(&self.paths, &object, &existing)?;
                    if existing.table_id == candidate.table_id || existing.state != TableCatalogEntryState::Active {
                        return Ok(());
                    }
                    if warehouse_object_prefixes_overlap(&existing.warehouse_object_prefix, &candidate.warehouse_object_prefix)
                        && self.warehouse_index_entry_has_active_owner(&existing).await?
                    {
                        return Err(TableCatalogStoreError::Conflict(format!(
                            "table warehouse location overlaps an active table: {}",
                            candidate.warehouse_object_prefix
                        )));
                    }
                    Ok::<_, TableCatalogStoreError>(())
                }
            })
            .buffered(TABLE_REGISTRATION_READ_CONCURRENCY)
            .try_for_each(|()| async { Ok(()) })
            .await
    }

    async fn reserve_table_warehouse_index(
        &self,
        entry: &TableEntry,
        prefix_already_checked: bool,
        locked_table_entry: &str,
    ) -> TableCatalogStoreResult<WarehouseIndexReservation> {
        let index = table_warehouse_index_entry(entry)?;
        let object = self
            .paths
            .warehouse_index_entry_path(&index.table_bucket, &index.warehouse_object_prefix);
        if let Some((existing, _)) = self
            .read_entry::<TableWarehouseIndexEntry>(self.catalog_bucket(), &object)
            .await?
        {
            validate_table_warehouse_index_entry_object(&self.paths, &object, &existing)?;
            if existing == index {
                return Ok(WarehouseIndexReservation::AlreadyReserved);
            }
        }
        // Registration already checked the prefix while holding the bucket publication fence.
        if !prefix_already_checked {
            self.ensure_table_warehouse_prefix_available(entry, Some(locked_table_entry))
                .await?;
        }
        loop {
            match self
                .write_entry(self.catalog_bucket(), &object, &index, TableCatalogPutPrecondition::IfAbsent)
                .await
            {
                Ok(()) => return Ok(WarehouseIndexReservation::Created),
                Err(TableCatalogStoreError::Conflict(_)) => {
                    let Some((existing, _)) = self
                        .read_entry::<TableWarehouseIndexEntry>(self.catalog_bucket(), &object)
                        .await?
                    else {
                        continue;
                    };
                    validate_table_warehouse_index_entry_object(&self.paths, &object, &existing)?;
                    if existing == index {
                        return Ok(WarehouseIndexReservation::AlreadyReserved);
                    }
                    if existing.table_bucket != index.table_bucket
                        || existing.warehouse_object_prefix != index.warehouse_object_prefix
                        || self.warehouse_index_entry_has_active_owner(&existing).await?
                    {
                        return Err(TableCatalogStoreError::Conflict(format!(
                            "table warehouse location is already registered: {}",
                            index.warehouse_object_prefix
                        )));
                    }
                    if self.replace_table_warehouse_index(&object, &existing, &index).await? {
                        return Ok(WarehouseIndexReservation::Replaced(existing));
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn delete_stale_table_warehouse_index(
        &self,
        object: &str,
        index: &TableWarehouseIndexEntry,
        reason: &'static str,
    ) -> TableCatalogStoreResult<()> {
        self.delete_warehouse_index_object(object, index, reason)
            .await
            .map(|_| ())
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to delete stale warehouse index {object}: {err}")))
    }

    async fn fail_closed_for_broken_warehouse_index<T>(
        &self,
        object: &str,
        index: &TableWarehouseIndexEntry,
        reason: &'static str,
    ) -> TableCatalogStoreResult<Option<T>> {
        Err(TableCatalogStoreError::Internal(format!(
            "active warehouse index {object} for {}/{}/{} ({}) is inconsistent: {reason}",
            index.table_bucket, index.namespace, index.table, index.table_id
        )))
    }

    async fn resolve_table_data_plane_resource_from_index_entry(
        &self,
        index_object: &str,
        index: TableWarehouseIndexEntry,
        repair_index: bool,
    ) -> TableCatalogStoreResult<Option<(TableCatalogEntryState, TableDataPlaneResource)>> {
        validate_table_warehouse_index_entry_object(&self.paths, index_object, &index)?;
        if index.state == TableCatalogEntryState::Deleted {
            let resource = table_data_plane_resource_from_warehouse_index(&index)?;
            return Ok(Some((TableCatalogEntryState::Deleted, resource)));
        }
        if index.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::Internal(format!(
                "warehouse index {index_object} for {}/{}/{} has an invalid transient state while the index is authoritative",
                index.table_bucket, index.namespace, index.table
            )));
        }
        let Some(table) = self
            .load_table_entry(&index.table_bucket, &index.namespace, &index.table)
            .await?
        else {
            return self
                .fail_closed_for_broken_warehouse_index(index_object, &index, "referenced table entry is missing")
                .await;
        };
        if table.state != TableCatalogEntryState::Active {
            if repair_index {
                self.delete_stale_table_warehouse_index(index_object, &index, "referenced table is inactive")
                    .await?;
            }
            return Ok(None);
        }
        let current_prefix = table_warehouse_object_prefix(&table).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("warehouse index table entry has invalid location {index_object}: {err}"))
        })?;
        if current_prefix != index.warehouse_object_prefix {
            if repair_index {
                self.delete_stale_table_warehouse_index(index_object, &index, "referenced table moved warehouse prefix")
                    .await?;
            }
            return Ok(None);
        }
        if table.table_id != index.table_id {
            return self
                .fail_closed_for_broken_warehouse_index(index_object, &index, "referenced table identity changed")
                .await;
        }
        Ok(Some((
            TableCatalogEntryState::Active,
            table_data_plane_resource_from_entry(table, current_prefix),
        )))
    }

    async fn read_warehouse_index_state_unlocked(&self, table_bucket: &str) -> TableCatalogStoreResult<bool> {
        let Some((state, _)) = self
            .read_entry_unlocked::<TableWarehouseIndexStateEntry>(
                self.catalog_bucket(),
                &self.paths.warehouse_index_state_path(table_bucket),
            )
            .await?
        else {
            return Ok(false);
        };
        table_warehouse_index_state_ready(&state, table_bucket)
    }

    async fn rollback_table_warehouse_index_reservation(
        &self,
        entry: &TableEntry,
        reservation: WarehouseIndexReservation,
        reason: &'static str,
    ) {
        let result = async {
            match reservation {
                WarehouseIndexReservation::AlreadyReserved => Ok(()),
                WarehouseIndexReservation::Created => self.delete_table_warehouse_index(entry).await,
                WarehouseIndexReservation::Replaced(previous) => {
                    let index = table_warehouse_index_entry(entry)?;
                    // An error response can follow a committed catalog write.
                    if let Some(current) = self
                        .load_table_entry(&entry.table_bucket, &entry.namespace, &entry.table)
                        .await?
                        && table_warehouse_index_entry(&current)? == index
                    {
                        return Ok(());
                    }
                    self.replace_table_warehouse_index(
                        &self
                            .paths
                            .warehouse_index_entry_path(&index.table_bucket, &index.warehouse_object_prefix),
                        &index,
                        &previous,
                    )
                    .await
                    .map(|_| ())
                }
            }
        }
        .await;
        let warehouse_object_prefix = table_warehouse_object_prefix(entry).ok();
        if let Err(err) = result {
            tracing::warn!(
                table_bucket = %entry.table_bucket,
                namespace = %entry.namespace,
                table = %entry.table,
                table_id = %entry.table_id,
                warehouse_object_prefix = warehouse_object_prefix.as_deref().unwrap_or(""),
                reason = %reason,
                error = %err,
                "failed to roll back table warehouse index reservation"
            );
        }
    }

    pub(in crate::table_catalog) async fn delete_table_warehouse_index(&self, entry: &TableEntry) -> TableCatalogStoreResult<()> {
        let Ok(index) = table_warehouse_index_entry(entry) else {
            return Ok(());
        };
        self.delete_warehouse_index_object(
            &self
                .paths
                .warehouse_index_entry_path(&index.table_bucket, &index.warehouse_object_prefix),
            &index,
            "table warehouse index owner removed",
        )
        .await
        .map(|_| ())
    }

    pub(in crate::table_catalog) async fn tombstone_table_warehouse_index_for_drop(
        &self,
        entry: &TableEntry,
        authoritative_strong_owner: bool,
    ) -> TableCatalogStoreResult<()> {
        let index = table_warehouse_index_entry(entry)?;
        let mut tombstone = index.clone();
        tombstone.state = TableCatalogEntryState::Deleted;
        let object = self
            .paths
            .warehouse_index_entry_path(&index.table_bucket, &index.warehouse_object_prefix);
        validate_table_warehouse_index_entry_object(&self.paths, &object, &index)?;
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?;
        let Some((current, current_etag)) = self
            .read_entry_unlocked::<TableWarehouseIndexEntry>(self.catalog_bucket(), &object)
            .await?
        else {
            return self
                .write_entry_unlocked(
                    self.catalog_bucket(),
                    &object,
                    &tombstone,
                    TableCatalogPutPrecondition::IfAbsent,
                    _guard.write_commit_guards(),
                )
                .await;
        };
        validate_table_warehouse_index_entry_object(&self.paths, &object, &current)?;
        if current == tombstone {
            return Ok(());
        }
        // Materialized indexes can retain the old identifier after a strong-catalog rename.
        let same_stable_owner = current.state == TableCatalogEntryState::Active
            && current.table_bucket == index.table_bucket
            && current.warehouse_object_prefix == index.warehouse_object_prefix
            && current.table_id == index.table_id;
        if current != index
            && !(authoritative_strong_owner && (current.state == TableCatalogEntryState::Deleted || same_stable_owner))
        {
            return Err(TableCatalogStoreError::Conflict(format!(
                "table warehouse index owner changed before drop: {}",
                index.warehouse_object_prefix
            )));
        }
        let current_etag = current_etag
            .ok_or_else(|| TableCatalogStoreError::Internal(format!("catalog warehouse index has no etag: {object}")))?;
        self.write_entry_unlocked(
            self.catalog_bucket(),
            &object,
            &tombstone,
            TableCatalogPutPrecondition::IfMatch(current_etag),
            _guard.write_commit_guards(),
        )
        .await
    }

    async fn restore_table_warehouse_index_after_failed_drop(
        &self,
        entry: &TableEntry,
        locked_table_entry: &str,
        reason: &'static str,
    ) {
        if let Err(err) = self.reserve_table_warehouse_index(entry, false, locked_table_entry).await {
            tracing::warn!(
                table_bucket = %entry.table_bucket,
                namespace = %entry.namespace,
                table = %entry.table,
                table_id = %entry.table_id,
                reason,
                error = %err,
                "failed to restore table warehouse index after table drop stopped"
            );
        }
    }

    async fn delete_table_warehouse_index_if_changed(&self, current: &TableEntry, next: &TableEntry) {
        let Ok(current_index) = table_warehouse_index_entry(current) else {
            return;
        };
        let Ok(next_index) = table_warehouse_index_entry(next) else {
            return;
        };
        if current_index.warehouse_object_prefix == next_index.warehouse_object_prefix {
            return;
        }
        if let Err(err) = self.delete_table_warehouse_index(current).await {
            tracing::warn!(
                table_bucket = %current.table_bucket,
                namespace = %current.namespace,
                table = %current.table,
                table_id = %current.table_id,
                warehouse_object_prefix = %current_index.warehouse_object_prefix,
                error = %err,
                "failed to delete stale table warehouse index"
            );
        }
    }

    async fn resolve_table_data_plane_resource_from_index(
        &self,
        table_bucket: &str,
        object: &str,
        repair_index: bool,
    ) -> TableCatalogStoreResult<TableWarehouseIndexResolution> {
        let mut active: Option<TableDataPlaneResource> = None;
        let mut deleted: Option<TableDataPlaneResource> = None;
        let mut deleted_overlap = None;
        for warehouse_object_prefix in warehouse_index_candidate_prefixes(object) {
            let index_object = self.paths.warehouse_index_entry_path(table_bucket, warehouse_object_prefix);
            let Some((index, _)) = self
                .read_entry::<TableWarehouseIndexEntry>(self.catalog_bucket(), &index_object)
                .await?
            else {
                continue;
            };
            if index.table_bucket != table_bucket || index.warehouse_object_prefix != warehouse_object_prefix {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "warehouse index entry does not match indexed prefix: {index_object}"
                )));
            }
            if let Some((state, resource)) = self
                .resolve_table_data_plane_resource_from_index_entry(&index_object, index, repair_index)
                .await?
            {
                let matched = match &state {
                    TableCatalogEntryState::Active => &mut active,
                    TableCatalogEntryState::Deleted => {
                        if let Some(current) = deleted.as_ref() {
                            deleted_overlap = Some((current.warehouse_object_prefix.clone(), resource.warehouse_object_prefix));
                            continue;
                        }
                        &mut deleted
                    }
                    TableCatalogEntryState::Renaming | TableCatalogEntryState::Deleting => {
                        unreachable!("transient indexes are rejected above")
                    }
                };
                if let Some(current) = matched.as_ref() {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "object {object} matches overlapping active table warehouse indexes {} and {}",
                        current.warehouse_object_prefix, resource.warehouse_object_prefix
                    )));
                }
                *matched = Some(resource);
            }
        }
        if let Some(resource) = active {
            Ok(TableWarehouseIndexResolution::Active(resource))
        } else if let Some((left, right)) = deleted_overlap {
            Err(TableCatalogStoreError::Invalid(format!(
                "object {object} matches overlapping deleted table warehouse indexes {left} and {right}"
            )))
        } else if let Some(resource) = deleted {
            Ok(TableWarehouseIndexResolution::Deleted(resource))
        } else {
            Ok(TableWarehouseIndexResolution::Missing)
        }
    }

    /// Resolve authorization without repairing indexes or acquiring catalog mutation permits.
    /// The caller may already hold a publication read lock needed by backup and restore.
    pub(crate) async fn resolve_table_data_plane_resource_for_authorization(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        if table_bucket.is_empty() || object.is_empty() {
            return Ok(None);
        }
        self.with_catalog_restore_read(table_bucket, async {
            let read_version = self.table_data_plane_read_version(table_bucket).await?;
            let resource = if self.warehouse_index_ready(table_bucket).await? {
                self.resolve_table_data_plane_resource_with_scan(table_bucket, object, false)
                    .await
            } else {
                let indexed = self
                    .resolve_table_data_plane_resource_from_index(table_bucket, object, false)
                    .await?;
                let scanned = self
                    .scan_table_data_plane_resource_for_index_miss(table_bucket, object, false)
                    .await?;
                match indexed {
                    TableWarehouseIndexResolution::Active(resource) if scanned.as_ref() != Some(&resource) => Err(
                        TableCatalogStoreError::Unavailable("warehouse index changed during authorization lookup".to_string()),
                    ),
                    TableWarehouseIndexResolution::Deleted(tombstone) => Ok(scanned.or(Some(tombstone))),
                    _ => Ok(scanned),
                }
            }?;
            self.finish_table_rename_read(table_bucket, Some(&read_version)).await?;
            Ok(resource)
        })
        .await
    }

    async fn resolve_table_data_plane_resource_with_scan(
        &self,
        table_bucket: &str,
        object: &str,
        repair_index: bool,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        match self
            .resolve_table_data_plane_resource_from_index(table_bucket, object, repair_index)
            .await?
        {
            TableWarehouseIndexResolution::Active(resource) => Ok(Some(resource)),
            TableWarehouseIndexResolution::Deleted(tombstone) => Ok(self
                .scan_table_data_plane_resource_for_index_miss(table_bucket, object, repair_index)
                .await?
                .or(Some(tombstone))),
            TableWarehouseIndexResolution::Missing => {
                self.scan_table_data_plane_resource_for_index_miss(table_bucket, object, repair_index)
                    .await
            }
        }
    }

    async fn scan_table_data_plane_resource_for_index_miss(
        &self,
        table_bucket: &str,
        object: &str,
        repair_index: bool,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        let mut matched: Option<TableDataPlaneResource> = None;
        for table in self
            .list_all_table_entries_with_limit(table_bucket, Some(TABLE_DATA_PLANE_INDEX_MISS_SCAN_MAX_CATALOG_OBJECTS), None, 1)
            .await?
        {
            if table.state != TableCatalogEntryState::Active {
                continue;
            }
            let warehouse_object_prefix = table_warehouse_object_prefix(&table)?;
            if !object.starts_with(&warehouse_object_prefix) {
                continue;
            }
            if let Some(current) = matched.as_ref() {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "object {object} matches overlapping active table warehouse prefixes {} and {warehouse_object_prefix}",
                    current.warehouse_object_prefix
                )));
            }
            matched = Some(table_data_plane_resource_from_entry(table, warehouse_object_prefix));
        }
        if repair_index && let Some(resource) = matched.as_ref() {
            let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
            self.backfill_active_table_warehouse_index_with_prefix_check(
                &resource.table_bucket,
                &resource.namespace,
                &resource.table,
                true,
            )
            .await?;
        }
        Ok(matched)
    }

    #[cfg(test)]
    pub(in crate::table_catalog) async fn backfill_active_table_warehouse_index(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<()> {
        self.backfill_active_table_warehouse_index_with_prefix_check(table_bucket, namespace, table, false)
            .await
    }

    async fn backfill_active_table_warehouse_index_with_prefix_check(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        prefix_already_checked: bool,
    ) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        let Some((current, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
            return Ok(());
        };
        if current.state != TableCatalogEntryState::Active {
            return Ok(());
        }
        self.reserve_table_warehouse_index(&current, prefix_already_checked, &table_path)
            .await
            .map(|_| ())
    }

    pub(crate) async fn backfill_table_warehouse_index(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        self.backfill_table_warehouse_index_with_limit(table_bucket, None, TableWarehouseIndexBackfillMode::ScanAllTables)
            .await
    }

    async fn backfill_table_warehouse_index_for_data_plane(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        self.backfill_table_warehouse_index_with_limit(
            table_bucket,
            Some(TABLE_DATA_PLANE_INDEX_MISS_SCAN_MAX_CATALOG_OBJECTS),
            TableWarehouseIndexBackfillMode::InitializeIfNeeded,
        )
        .await
    }

    async fn backfill_table_warehouse_index_with_limit(
        &self,
        table_bucket: &str,
        max_catalog_objects: Option<usize>,
        mode: TableWarehouseIndexBackfillMode,
    ) -> TableCatalogStoreResult<()> {
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let state_object = self.paths.warehouse_index_state_path(table_bucket);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &state_object).await?;
        let index_ready = self.read_warehouse_index_state_unlocked(table_bucket).await?;
        if matches!(mode, TableWarehouseIndexBackfillMode::InitializeIfNeeded) && index_ready {
            return Ok(());
        }
        let tables = self
            .list_all_table_entries_with_limit(table_bucket, max_catalog_objects, None, 1)
            .await?
            .into_iter()
            .filter(|table| table.state == TableCatalogEntryState::Active)
            .collect::<Vec<_>>();
        let mut table_ids = BTreeSet::new();
        if let Some(table) = tables.iter().find(|table| !table_ids.insert(table.table_id.as_str())) {
            return Err(TableCatalogStoreError::Conflict(format!(
                "table id {} is registered by multiple tables in table bucket {table_bucket}",
                table.table_id
            )));
        }
        let mut active_prefixes = Vec::new();
        for table in &tables {
            active_prefixes.push((table_warehouse_object_prefix(table)?, table.table_id.as_str()));
        }
        active_prefixes.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        if let Some(window) = active_prefixes
            .windows(2)
            .find(|window| window[0].1 != window[1].1 && warehouse_object_prefixes_overlap(&window[0].0, &window[1].0))
        {
            return Err(TableCatalogStoreError::Conflict(format!(
                "active table warehouse locations overlap: {} and {}",
                window[0].0, window[1].0
            )));
        }
        for table in tables {
            self.backfill_active_table_warehouse_index_with_prefix_check(
                &table.table_bucket,
                &table.namespace,
                &table.table,
                true,
            )
            .await?;
        }
        self.write_warehouse_index_state_unlocked(table_bucket, _guard.write_commit_guards())
            .await
    }

    pub(super) async fn require_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        if self
            .get_table_bucket(table_bucket)
            .await?
            .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
        {
            return Ok(());
        }
        Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")))
    }

    pub(in crate::table_catalog) async fn read_table_with_etag(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &impl AsRef<str>,
    ) -> TableCatalogStoreResult<Option<(TableEntry, String)>> {
        let table_path = self.paths.table_entry_path(table_bucket, namespace, table);
        let Some((entry, etag)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Ok(None);
        };
        validate_table_entry_object(&self.paths, &table_path, &entry)?;
        if self.paths.scope == CatalogScope::Vector
            && CatalogHead::from_vector_entry(&entry)?.resource_identity().vector_bucket()
                != Some(self.vector_bucket_for(table_bucket)?)
        {
            return Err(TableCatalogStoreError::Conflict("stored Vector table identity changed".to_string()));
        }
        let Some(etag) = etag else {
            return Err(TableCatalogStoreError::Internal(format!("catalog table entry has no etag: {table_path}")));
        };
        Ok(Some((entry, etag)))
    }

    async fn load_table_entry(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<Option<TableEntry>> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = self.paths.parse_name(table)?;
        self.read_table_with_etag(table_bucket, &namespace, &table)
            .await
            .map(|entry| entry.map(|(table, _)| table))
    }

    async fn list_all_table_entries(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        self.list_all_table_entries_with_limit(table_bucket, None, None, 1).await
    }

    pub(in crate::table_catalog) async fn list_table_entry_objects_for_data_plane_scan(
        &self,
        table_bucket: &str,
        max_catalog_objects: usize,
    ) -> TableCatalogStoreResult<Vec<String>> {
        let mut objects = Vec::new();
        let mut cursor: Option<String> = None;
        let prefix = self.paths.namespace_entries_prefix(table_bucket);
        let limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS)
            .ok_or_else(|| TableCatalogStoreError::Internal("table catalog listing page size must be positive".to_string()))?;
        loop {
            let page = self
                .backend
                .list_table_entry_objects_page(self.catalog_bucket(), &prefix, cursor.as_deref(), limit)
                .await?;
            let next_cursor = page.objects.last().cloned();
            objects.extend(page.objects);
            if !page.is_truncated {
                return bounded_table_entry_objects_for_data_plane_scan(objects, false, max_catalog_objects);
            }
            if objects.len() > max_catalog_objects {
                return bounded_table_entry_objects_for_data_plane_scan(objects, true, max_catalog_objects);
            }
            let Some(next_cursor) = next_cursor else {
                return Err(TableCatalogStoreError::Internal(
                    "catalog object pagination did not advance during table data-plane index-miss scan".to_string(),
                ));
            };
            if cursor.as_deref().is_some_and(|current| next_cursor.as_str() <= current) {
                return Err(TableCatalogStoreError::Internal(
                    "catalog object pagination did not advance during table data-plane index-miss scan".to_string(),
                ));
            }
            cursor = Some(next_cursor);
        }
    }

    pub(in crate::table_catalog) async fn list_all_table_entries_with_limit(
        &self,
        table_bucket: &str,
        max_catalog_objects: Option<usize>,
        locked_table_entry: Option<&str>,
        read_concurrency: usize,
    ) -> TableCatalogStoreResult<Vec<TableEntry>> {
        let directory = self.table_directory_objects(table_bucket, max_catalog_objects).await?;
        let (table_objects, directory_epoch) = match directory {
            Some(scan) => (scan.objects, Some(scan.activation_id)),
            None => {
                let objects = match max_catalog_objects {
                    Some(limit) => self.list_table_entry_objects_for_data_plane_scan(table_bucket, limit).await?,
                    None => self
                        .backend
                        .list_objects(self.catalog_bucket(), &self.paths.namespace_entries_prefix(table_bucket))
                        .await?
                        .into_iter()
                        .filter(|object| object.ends_with(TABLE_ENTRY_FILE))
                        .collect(),
                };
                (objects, None)
            }
        };
        let mut entries = stream::iter(table_objects)
            .map(|object| async move {
                // Only the exact entry protected by the caller's write lock can bypass a read lock.
                let entry = if locked_table_entry == Some(object.as_str()) {
                    self.read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &object).await?
                } else {
                    self.read_entry::<TableEntry>(self.catalog_bucket(), &object).await?
                };
                let Some((entry, _)) = entry else {
                    return Ok(None);
                };
                validate_table_entry_object(&self.paths, &object, &entry)?;
                Ok::<_, TableCatalogStoreError>(Some(entry))
            })
            .buffered(read_concurrency)
            .try_filter_map(|entry| async move { Ok(entry) })
            .try_collect::<Vec<_>>()
            .await?;
        if let Some(epoch) = directory_epoch {
            self.ensure_table_directory_epoch(table_bucket, epoch).await?;
        }
        entries.sort_by(|left, right| (&left.namespace, &left.table).cmp(&(&right.namespace, &right.table)));
        Ok(entries)
    }

    pub(in crate::table_catalog) async fn read_table_with_etag_unlocked(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &impl AsRef<str>,
    ) -> TableCatalogStoreResult<Option<(TableEntry, String)>> {
        let table_path = self.paths.table_entry_path(table_bucket, namespace, table);
        let Some((entry, etag)) = self
            .read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &table_path)
            .await?
        else {
            return Ok(None);
        };
        validate_table_entry_object(&self.paths, &table_path, &entry)?;
        if self.paths.scope == CatalogScope::Vector
            && CatalogHead::from_vector_entry(&entry)?.resource_identity().vector_bucket()
                != Some(self.vector_bucket_for(table_bucket)?)
        {
            return Err(TableCatalogStoreError::Conflict("stored Vector table identity changed".to_string()));
        }
        let Some(etag) = etag else {
            return Err(TableCatalogStoreError::Internal(format!("catalog table entry has no etag: {table_path}")));
        };
        Ok(Some((entry, etag)))
    }

    pub(in crate::table_catalog) async fn read_view_with_etag_unlocked(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        view: &IdentifierSegment,
    ) -> TableCatalogStoreResult<Option<(ViewEntry, String)>> {
        let view_path = self.paths.view_entry_path(table_bucket, namespace, view);
        let Some((entry, etag)) = self
            .read_entry_unlocked::<ViewEntry>(self.catalog_bucket(), &view_path)
            .await?
        else {
            return Ok(None);
        };
        validate_view_entry_object(&self.paths, &view_path, &entry)?;
        let Some(etag) = etag else {
            return Err(TableCatalogStoreError::Internal(format!("catalog view entry has no etag: {view_path}")));
        };
        Ok(Some((entry, etag)))
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    async fn write_table_entry(
        &self,
        entry: TableEntry,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        self.write_table_entry_with_publication(entry, precondition, &publication)
            .await
    }

    pub(in crate::table_catalog) async fn begin_table_bucket_with_migration_permit(
        &self,
        table_bucket: &str,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<Option<TableCatalogLockGuard>> {
        if self.paths.scope == CatalogScope::Vector {
            let guard = self
                .backend
                .acquire_bucket_lifecycle_read(self.vector_bucket_for(table_bucket)?)
                .await?;
            publication.begin_table_bucket(table_bucket).await?;
            if guard.is_lock_lost() {
                publication.complete();
                return Err(TableCatalogStoreError::Unavailable("Vector lifecycle guard was lost".to_string()));
            }
            return Ok(Some(guard));
        }
        let permit_before_publication = publication.catalog_migration_read_permit_status();
        if permit_before_publication == Some(false) {
            return Err(TableCatalogStoreError::Conflict(
                "table-bucket catalog migration read permit was lost".to_string(),
            ));
        }

        publication.begin_table_bucket(table_bucket).await?;
        let result = match (permit_before_publication, publication.catalog_migration_read_permit_status()) {
            (Some(true), Some(true)) | (None, Some(true)) => self
                .ensure_object_backed_catalog_write_permit_after_lock(table_bucket)
                .await
                .map(|()| None),
            (None, None) => self.acquire_object_backed_catalog_write_permit(table_bucket).await.map(Some),
            (Some(true), Some(false)) | (None, Some(false)) => Err(TableCatalogStoreError::Conflict(
                "table-bucket catalog migration read permit was lost".to_string(),
            )),
            (Some(true), None) => Err(TableCatalogStoreError::Internal(
                "table-bucket catalog migration read permit disappeared during publication".to_string(),
            )),
            (Some(false), _) => unreachable!("a lost migration permit is rejected before publication"),
        };
        if result.is_err() {
            publication.complete();
        }
        result
    }

    async fn write_table_entry_with_publication(
        &self,
        entry: TableEntry,
        precondition: TableCatalogPutPrecondition,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        publication_preparation(publication, async {
            self.paths.validate_table_entry(&entry)?;
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let table = self.paths.parse_name(&entry.table)?;
            if self.paths.scope == CatalogScope::Table {
                validate_table_warehouse_location(&entry.table_bucket, &entry.warehouse_location)?;
            }
            self.require_table_bucket(&entry.table_bucket).await?;
            let mut _migration_guard = self
                .begin_table_bucket_with_migration_permit(&entry.table_bucket, publication)
                .await?;
            if !publication.holds_table_bucket(&entry.table_bucket) {
                return Err(TableCatalogStoreError::Internal(
                    "table registration requires a table-bucket publication fence".to_string(),
                ));
            }
            retain_publication_preparation_fence(publication);
            let _publication_completion = TableCommitPublicationCompletion::new(publication);
            self.recover_active_table_rename(&entry.table_bucket, publication).await?;
            let namespace_path = self.paths.namespace_entry_path(&entry.table_bucket, &namespace);
            let _namespace_guard = self
                .backend
                .acquire_write_lock(self.catalog_bucket(), &namespace_path)
                .await?;
            self.require_active_namespace_unlocked(&entry.table_bucket, &namespace, &namespace_path)
                .await?;
            let table_path = self.paths.table_entry_path(&entry.table_bucket, &namespace, &table);
            let _table_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            if self.paths.scope == CatalogScope::Table {
                let view_path =
                    self.paths
                        .view_entry_path(&entry.table_bucket, &namespace, &parse_table_for_store(table.as_str())?);
                if self
                    .read_entry_unlocked::<ViewEntry>(self.catalog_bucket(), &view_path)
                    .await?
                    .is_some()
                {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog object already exists: view {}/{}/{}",
                        entry.table_bucket, entry.namespace, entry.table
                    )));
                }
            }
            let mut precondition = precondition;
            if matches!(precondition, TableCatalogPutPrecondition::IfAbsent)
                && let Some((current, etag)) = self
                    .read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &table_path)
                    .await?
            {
                validate_table_entry_object(&self.paths, &table_path, &current)?;
                if current.state != TableCatalogEntryState::Deleted {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog object already exists: table {}/{}/{}",
                        entry.table_bucket, entry.namespace, entry.table
                    )));
                }
                precondition = etag
                    .map(TableCatalogPutPrecondition::IfMatch)
                    .ok_or_else(|| TableCatalogStoreError::Internal(format!("catalog table entry has no etag: {table_path}")))?;
            }
            // Preserve catalog -> publication -> object lock order across rolling upgrades.
            publication
                .prepare(&entry.table_bucket, &entry.namespace, &entry.table)
                .await?;
            if !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.table) {
                return Err(TableCatalogStoreError::Internal(
                    "table registration requires a table publication fence".to_string(),
                ));
            }
            retain_publication_preparation_fence(publication);
            let reservation = if self.paths.scope == CatalogScope::Table {
                self.check_registration_ownership(&entry, &table_path, publication).await?;
                self.reserve_table_warehouse_index(&entry, true, &table_path).await?
            } else {
                let head = CatalogHead::from_vector_entry(&entry)?;
                if entry.generation != 1 {
                    return Err(TableCatalogStoreError::Invalid("initial Vector generation must be one".to_string()));
                }
                if head.resource_identity().vector_bucket() != Some(self.vector_bucket_for(&entry.table_bucket)?) {
                    return Err(TableCatalogStoreError::Conflict("initial Vector head bucket binding changed".to_string()));
                }
                let entries = self
                    .list_all_table_entries_with_limit(&entry.table_bucket, None, Some(&table_path), 1)
                    .await?;
                if entries.iter().any(|other| other.table_id == entry.table_id) {
                    return Err(TableCatalogStoreError::Conflict("Vector table id cannot be reused".to_string()));
                }
                let limit = usize::try_from(head.metadata().content_length())
                    .ok()
                    .and_then(|size| size.checked_add(1))
                    .ok_or_else(|| TableCatalogStoreError::Invalid("initial metadata size exceeds address space".to_string()))?;
                let object = self
                    .backend
                    .read_managed_candidate(
                        self.vector_bucket_for(&entry.table_bucket)?,
                        head.metadata(),
                        limit,
                        _migration_guard.as_mut().ok_or_else(|| {
                            TableCatalogStoreError::Internal("Vector registration has no lifecycle guard".to_string())
                        })?,
                    )
                    .await?
                    .ok_or_else(|| TableCatalogStoreError::NotFound("initial Vector manifest is missing".to_string()))?;
                head.metadata().verify_read(&object)?;
                WarehouseIndexReservation::AlreadyReserved
            };
            if !publication.holds_table_bucket(&entry.table_bucket)
                || !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.table)
            {
                self.rollback_table_warehouse_index_reservation(&entry, reservation, "table publication fence lost")
                    .await;
                return Err(TableCatalogStoreError::Internal(
                    "table registration publication fence was lost before catalog update".to_string(),
                ));
            }
            let mut mutation_fence = publication.object_mutation_fence().unwrap_or_default();
            mutation_fence.extend_signals([_migration_guard.as_ref().and_then(TableCatalogLockGuard::lock_lost_signal)]);
            mutation_fence.ensure_held()?;
            let result = self
                .write_entry_unlocked_with_fence(
                    self.catalog_bucket(),
                    &table_path,
                    &entry,
                    precondition,
                    _table_guard.write_commit_guards(),
                    Some(&mutation_fence),
                )
                .await;
            if result.is_err() {
                self.rollback_table_warehouse_index_reservation(&entry, reservation, "table entry write failed")
                    .await;
            }
            result?;
            Ok(())
        })
        .await
    }

    async fn write_view_entry(&self, entry: ViewEntry, precondition: TableCatalogPutPrecondition) -> TableCatalogStoreResult<()> {
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        self.write_view_entry_with_publication(entry, precondition, &publication)
            .await
    }

    async fn write_view_entry_with_publication(
        &self,
        entry: ViewEntry,
        precondition: TableCatalogPutPrecondition,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        validate_view_entry_version_and_id(&entry)?;
        self.require_table_bucket(&entry.table_bucket).await?;
        let _migration_guard = self
            .begin_table_bucket_with_migration_permit(&entry.table_bucket, publication)
            .await?;
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "view creation requires a table-bucket publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let view = parse_table_for_store(&entry.view)?;
        validate_view_warehouse_location(&entry.table_bucket, &entry.warehouse_location)?;
        self.recover_active_table_rename(&entry.table_bucket, publication).await?;
        let namespace_path = self.paths.namespace_entry_path(&entry.table_bucket, &namespace);
        let _namespace_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &namespace_path)
            .await?;
        self.require_active_namespace_unlocked(&entry.table_bucket, &namespace, &namespace_path)
            .await?;
        let view_path = self.paths.view_entry_path(&entry.table_bucket, &namespace, &view);
        let _view_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &view_path).await?;
        let table_path = self.paths.table_entry_path(&entry.table_bucket, &namespace, &view);
        if self
            .read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &table_path)
            .await?
            .is_some()
        {
            return Err(TableCatalogStoreError::Conflict(format!(
                "catalog object already exists: table {}/{}/{}",
                entry.table_bucket, entry.namespace, entry.view
            )));
        }
        // Preserve catalog -> publication -> object lock order across rolling upgrades.
        publication
            .prepare(&entry.table_bucket, &entry.namespace, &entry.view)
            .await?;
        if !publication.holds_table_bucket(&entry.table_bucket)
            || !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.view)
        {
            return Err(TableCatalogStoreError::Internal(
                "view creation publication fence was lost before catalog update".to_string(),
            ));
        }
        self.write_entry_unlocked(self.catalog_bucket(), &view_path, &entry, precondition, _view_guard.write_commit_guards())
            .await
    }

    pub(crate) async fn get_external_catalog_bridge(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<Option<ExternalCatalogBridgeEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            self.require_table_bucket(table_bucket).await?;
            let namespace = parse_namespace_for_store(namespace)?;
            let table = parse_table_for_store(table)?;
            if self.get_namespace(table_bucket, &namespace.public_name()).await?.is_none() {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "namespace {}/{}",
                    table_bucket,
                    namespace.public_name()
                )));
            }
            let bridge_path = self.paths.external_catalog_bridge_path(table_bucket, &namespace, &table);
            let Some((entry, _)) = self
                .read_entry::<ExternalCatalogBridgeEntry>(self.catalog_bucket(), &bridge_path)
                .await?
            else {
                return Ok(None);
            };
            validate_external_catalog_bridge_entry_object(&self.paths, &bridge_path, &entry)?;
            Ok(Some(entry))
        })
        .await
    }

    pub(crate) async fn put_external_catalog_bridge(
        &self,
        entry: ExternalCatalogBridgeEntry,
    ) -> TableCatalogStoreResult<ExternalCatalogBridgeEntry> {
        validate_catalog_entry_version("external catalog bridge", entry.version)?;
        self.require_table_bucket(&entry.table_bucket).await?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&entry.table_bucket).await?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        if self.get_namespace(&entry.table_bucket, &entry.namespace).await?.is_none() {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {}/{}",
                entry.table_bucket, entry.namespace
            )));
        }
        let bridge_path = self
            .paths
            .external_catalog_bridge_path(&entry.table_bucket, &namespace, &table);
        validate_external_catalog_bridge_entry_object(&self.paths, &bridge_path, &entry)?;
        self.write_entry(self.catalog_bucket(), &bridge_path, &entry, TableCatalogPutPrecondition::Any)
            .await?;
        Ok(entry)
    }

    async fn read_commit_by_path(&self, object: &str) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        self.read_entry::<CommitLogEntry>(self.catalog_bucket(), object)
            .await
            .map(|entry| entry.map(|(commit, _)| commit))
    }

    async fn read_commit_log_entry(
        &self,
        table_bucket: &str,
        table_id: &str,
        commit_id: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        let object = self.paths.commit_log_entry_path(table_bucket, table_id, commit_id);
        let Some(commit) = self.read_commit_by_path(&object).await? else {
            return Ok(None);
        };
        validate_commit_log_entry_object(&self.paths, &object, table_bucket, table_id, &commit)?;
        Ok(Some(commit))
    }

    async fn read_commit_idempotency_entry(
        &self,
        table_bucket: &str,
        table_id: &str,
        idempotency_key: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        let object = self
            .paths
            .commit_idempotency_entry_path(table_bucket, table_id, idempotency_key);
        let Some(commit) = self.read_commit_by_path(&object).await? else {
            return Ok(None);
        };
        validate_commit_idempotency_entry_object(&self.paths, &object, table_bucket, table_id, &commit)?;
        Ok(Some(commit))
    }

    async fn read_table_commit_logs(&self, entry: &TableEntry) -> TableCatalogStoreResult<Vec<(String, CommitLogEntry)>> {
        let commit_prefix = self.paths.commit_log_entries_prefix(&entry.table_bucket, &entry.table_id);
        let mut commits = Vec::new();
        for object in self.backend.list_objects(self.catalog_bucket(), &commit_prefix).await? {
            if !object.ends_with(".json") {
                continue;
            }
            if let Some(commit_log) = self.read_commit_by_path(&object).await? {
                validate_commit_log_entry_object(&self.paths, &object, &entry.table_bucket, &entry.table_id, &commit_log)?;
                commits.push((object, commit_log));
            }
        }
        Ok(commits)
    }

    async fn finalize_commit_log(
        &self,
        commit_path: &str,
        idempotency_path: Option<&str>,
        commit_log: &CommitLogEntry,
    ) -> TableCatalogStoreResult<()> {
        self.write_entry(self.catalog_bucket(), commit_path, commit_log, TableCatalogPutPrecondition::Any)
            .await?;
        if let Some(idempotency_path) = idempotency_path {
            self.write_entry(self.catalog_bucket(), idempotency_path, commit_log, TableCatalogPutPrecondition::Any)
                .await?;
        }
        Ok(())
    }

    pub(in crate::table_catalog) async fn table_commit_recovery_report_for_entry(
        &self,
        entry: &TableEntry,
        finalized_count: usize,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
        let commit_logs_with_paths = self.read_table_commit_logs(entry).await?;
        let history = TableCommitHistoryIndex::new(entry, commit_logs_with_paths.iter().map(|(_, commit_log)| commit_log));
        let mut commits = Vec::with_capacity(commit_logs_with_paths.len());
        for (_, commit_log) in &commit_logs_with_paths {
            let idempotency_commit = match commit_log.idempotency_key.as_deref() {
                Some(idempotency_key) => {
                    self.read_commit_idempotency_entry(&entry.table_bucket, &entry.table_id, idempotency_key)
                        .await?
                }
                None => None,
            };
            commits.push(table_commit_recovery_entry_from_history(
                entry,
                commit_log,
                idempotency_commit.as_ref(),
                &history,
            ));
        }
        commits.sort_by(|left, right| left.commit_id.cmp(&right.commit_id));

        let finalization_required_count = commits
            .iter()
            .filter(|commit| matches!(commit.recovery_state, TableCommitRecoveryState::FinalizationRequired))
            .count();
        let idempotency_repair_required_count = commits
            .iter()
            .filter(|commit| matches!(commit.recovery_state, TableCommitRecoveryState::IdempotencyIndexRepairRequired))
            .count();
        let staged_before_table_update_count = commits
            .iter()
            .filter(|commit| matches!(commit.recovery_state, TableCommitRecoveryState::StagedBeforeTableUpdate))
            .count();
        let manual_review_count = commits
            .iter()
            .filter(|commit| matches!(commit.recovery_state, TableCommitRecoveryState::ManualReview))
            .count();

        Ok(TableCommitRecoveryReport {
            table_bucket: entry.table_bucket.clone(),
            namespace: entry.namespace.clone(),
            table: entry.table.clone(),
            table_id: entry.table_id.clone(),
            current_metadata_location: entry.metadata_location.clone(),
            current_version_token: entry.version_token.clone(),
            current_generation: entry.generation,
            commits,
            staged_before_table_update_count,
            finalization_required_count,
            idempotency_repair_required_count,
            manual_review_count,
            finalized_count,
        })
    }

    #[cfg(test)]
    pub(crate) async fn plan_table_commit_recovery(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
        self.with_catalog_restore_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            let table = self.paths.parse_name(table)?;
            let Some((entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    table.as_str()
                )));
            };
            self.table_commit_recovery_report_for_entry(&entry, 0).await
        })
        .await
    }

    pub(crate) async fn recover_table_commits(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = self.paths.parse_name(table)?;
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        publication.begin_table_bucket(table_bucket).await?;
        let _publication_completion = TableCommitPublicationCompletion::new(&publication);
        self.recover_active_table_rename(table_bucket, &publication).await?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };

        let commit_logs_with_paths = self.read_table_commit_logs(&entry).await?;
        let history = TableCommitHistoryIndex::new(&entry, commit_logs_with_paths.iter().map(|(_, commit_log)| commit_log));
        let mut finalized_count = 0;
        for (commit_path, commit_log) in &commit_logs_with_paths {
            let idempotency_key = commit_log.idempotency_key.as_deref();
            let idempotency_path = idempotency_key.map(|idempotency_key| {
                self.paths
                    .commit_idempotency_entry_path(table_bucket, &entry.table_id, idempotency_key)
            });
            let idempotency_commit = match idempotency_key {
                Some(idempotency_key) => {
                    self.read_commit_idempotency_entry(table_bucket, &entry.table_id, idempotency_key)
                        .await?
                }
                None => None,
            };
            let recovery_entry =
                table_commit_recovery_entry_from_history(&entry, commit_log, idempotency_commit.as_ref(), &history);
            if matches!(
                recovery_entry.recovery_state,
                TableCommitRecoveryState::FinalizationRequired | TableCommitRecoveryState::IdempotencyIndexRepairRequired
            ) {
                let mut committed = commit_log.clone();
                committed.status = CommitLogStatus::Committed;
                self.finalize_commit_log(commit_path, idempotency_path.as_deref(), &committed)
                    .await?;
                finalized_count += 1;
            }
        }

        self.table_commit_recovery_report_for_entry(&entry, finalized_count).await
    }

    pub(crate) async fn get_table_maintenance_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        self.with_table_maintenance_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            let table = parse_table_for_store(table)?;
            let Some((entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    table.as_str()
                )));
            };

            self.get_table_maintenance_config_for_entry(&entry).await
        })
        .await
    }

    pub(super) async fn get_table_maintenance_config_for_entry(
        &self,
        entry: &TableEntry,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;

        let config_path = self
            .paths
            .table_maintenance_config_path(&entry.table_bucket, &namespace, &table, &entry.table_id);
        let config = self
            .read_entry::<TableMaintenanceConfig>(self.catalog_bucket(), &config_path)
            .await?
            .map(|(config, _)| config)
            .unwrap_or_default();
        validate_table_maintenance_config(&config)?;
        Ok(config)
    }

    pub(crate) async fn get_effective_table_maintenance_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableMaintenanceEffectiveConfig> {
        self.with_table_maintenance_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            let table = parse_table_for_store(table)?;
            let Some((entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    table.as_str()
                )));
            };

            self.get_effective_table_maintenance_config_for_entry_unlocked(table_bucket, &namespace, &table, &entry)
                .await
        })
        .await
    }

    pub(super) async fn get_effective_table_maintenance_config_for_entry_unlocked(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        entry: &TableEntry,
    ) -> TableCatalogStoreResult<TableMaintenanceEffectiveConfig> {
        let table_config_path = self
            .paths
            .table_maintenance_config_path(table_bucket, namespace, table, &entry.table_id);
        if let Some((config, _)) = self
            .read_entry_unlocked::<TableMaintenanceConfig>(self.catalog_bucket(), &table_config_path)
            .await?
        {
            validate_table_maintenance_config(&config)?;
            return Ok(TableMaintenanceEffectiveConfig {
                config,
                source: TableMaintenanceConfigSource::TableOverride,
            });
        }

        let bucket_config_path = self.paths.table_bucket_maintenance_config_path(table_bucket);
        if let Some((config, _)) = self
            .read_entry_unlocked::<TableMaintenanceConfig>(self.catalog_bucket(), &bucket_config_path)
            .await?
        {
            validate_table_maintenance_config(&config)?;
            return Ok(TableMaintenanceEffectiveConfig {
                config,
                source: TableMaintenanceConfigSource::TableBucketDefault,
            });
        }

        Ok(TableMaintenanceEffectiveConfig {
            config: TableMaintenanceConfig::default(),
            source: TableMaintenanceConfigSource::Default,
        })
    }

    pub(crate) async fn put_table_maintenance_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableMaintenanceConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        validate_table_maintenance_config(&config)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let _publication_guard = self.acquire_table_maintenance_publication_permit(table_bucket).await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let Some((entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };

        self.put_table_maintenance_config_for_entry(&entry, config).await
    }

    pub(super) async fn put_table_maintenance_config_for_entry(
        &self,
        entry: &TableEntry,
        config: TableMaintenanceConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        self.put_table_maintenance_config_for_entry_with_publication(entry, config, None)
            .await
    }

    pub(super) async fn put_table_maintenance_config_for_entry_with_publication(
        &self,
        entry: &TableEntry,
        config: TableMaintenanceConfig,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        validate_table_maintenance_config(&config)?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        let config_path = self
            .paths
            .table_maintenance_config_path(&entry.table_bucket, &namespace, &table, &entry.table_id);
        ensure_table_maintenance_publication_held(publication, entry, "before config write")?;
        self.write_maintenance_entry(&config_path, &config, entry, publication)
            .await?;
        ensure_table_maintenance_publication_held(publication, entry, "after config write")?;
        Ok(config)
    }

    pub(crate) async fn put_table_metadata_maintenance_report(
        &self,
        report: &TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<()> {
        let _migration_guard = self
            .acquire_object_backed_catalog_write_permit(&report.job.table_bucket)
            .await?;
        let _publication_guard = self
            .acquire_table_maintenance_publication_permit(&report.job.table_bucket)
            .await?;
        self.put_table_metadata_maintenance_report_unfenced(report).await
    }

    pub(in crate::table_catalog) async fn put_table_metadata_maintenance_report_unfenced(
        &self,
        report: &TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(&report.job.namespace)?;
        let table = parse_table_for_store(&report.job.table)?;
        let Some((entry, _)) = self
            .read_table_with_etag(&report.job.table_bucket, &namespace, &table)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                report.job.table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        self.put_table_metadata_maintenance_report_for_entry(report, &entry).await
    }

    pub(super) async fn put_table_metadata_maintenance_report_for_entry(
        &self,
        report: &TableMetadataMaintenanceReport,
        entry: &TableEntry,
    ) -> TableCatalogStoreResult<()> {
        self.put_table_metadata_maintenance_report_for_entry_with_publication(report, entry, None)
            .await
    }

    pub(in crate::table_catalog) async fn put_table_metadata_maintenance_report_for_entry_with_publication(
        &self,
        report: &TableMetadataMaintenanceReport,
        entry: &TableEntry,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<()> {
        let report = table_maintenance_report_with_recommended_actions(report.clone());
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        validate_table_maintenance_report_owner(&report, &entry.table_bucket, &namespace, &table, &entry.table_id)?;
        let job_path =
            self.paths
                .table_maintenance_job_path(&entry.table_bucket, &namespace, &table, &entry.table_id, &report.job.job_id);
        let latest_job_path =
            self.paths
                .table_maintenance_latest_job_path(&entry.table_bucket, &namespace, &table, &entry.table_id);
        let current_job_path =
            self.paths
                .table_maintenance_current_job_path(&entry.table_bucket, &namespace, &table, &entry.table_id);
        ensure_table_maintenance_publication_held(publication, entry, "before job report write")?;
        self.write_maintenance_entry(&job_path, &report, entry, publication).await?;
        ensure_table_maintenance_publication_held(publication, entry, "after job report write")?;
        ensure_table_maintenance_publication_held(publication, entry, "before latest report write")?;
        self.write_maintenance_entry(&latest_job_path, &report, entry, publication)
            .await?;
        ensure_table_maintenance_publication_held(publication, entry, "after latest report write")?;
        ensure_table_maintenance_publication_held(publication, entry, "before current report write")?;
        self.write_maintenance_entry(&current_job_path, &report, entry, publication)
            .await?;
        ensure_table_maintenance_publication_held(publication, entry, "after current report write")
    }

    pub(crate) async fn get_table_metadata_maintenance_report(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        job_id: &str,
    ) -> TableCatalogStoreResult<Option<TableMetadataMaintenanceReport>> {
        self.with_table_maintenance_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            let table = parse_table_for_store(table)?;
            let Some((entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    table.as_str()
                )));
            };

            self.get_table_metadata_maintenance_report_for_entry_unlocked(
                table_bucket,
                &namespace,
                &table,
                &entry.table_id,
                job_id,
            )
            .await
        })
        .await
    }

    pub(super) async fn get_table_metadata_maintenance_report_for_entry_unlocked(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
        job_id: &str,
    ) -> TableCatalogStoreResult<Option<TableMetadataMaintenanceReport>> {
        let job_path = self.table_metadata_maintenance_report_path(table_bucket, namespace, table, table_id, job_id);
        let Some((report, _)) = self
            .read_entry_unlocked::<TableMetadataMaintenanceReport>(self.catalog_bucket(), &job_path)
            .await?
        else {
            return Ok(None);
        };
        validate_table_maintenance_report_owner(&report, table_bucket, namespace, table, table_id)?;
        Ok(Some(table_maintenance_report_with_recommended_actions(report)))
    }

    fn table_metadata_maintenance_report_path(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
        job_id: &str,
    ) -> String {
        match job_id {
            MAINTENANCE_JOB_ALIAS_LATEST => {
                self.paths
                    .table_maintenance_latest_job_path(table_bucket, namespace, table, table_id)
            }
            MAINTENANCE_JOB_ALIAS_CURRENT => {
                self.paths
                    .table_maintenance_current_job_path(table_bucket, namespace, table, table_id)
            }
            _ => self
                .paths
                .table_maintenance_job_path(table_bucket, namespace, table, table_id, job_id),
        }
    }

    pub(crate) async fn get_table_maintenance_scheduler_report(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerReport> {
        self.get_table_maintenance_scheduler_report_at(table_bucket, namespace, table, OffsetDateTime::now_utc())
            .await
    }

    pub(in crate::table_catalog) async fn get_table_maintenance_scheduler_report_at(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        now: OffsetDateTime,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerReport> {
        self.with_table_maintenance_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            let table = parse_table_for_store(table)?;
            let Some((entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    table.as_str()
                )));
            };
            self.get_table_maintenance_scheduler_report_for_entry_at(&entry, now).await
        })
        .await
    }

    pub(super) async fn get_table_maintenance_scheduler_report_for_entry_at(
        &self,
        entry: &TableEntry,
        now: OffsetDateTime,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerReport> {
        self.get_table_maintenance_scheduler_report_with_audit_reports_for_entry_at(entry, now, None)
            .await
    }

    pub(super) async fn get_table_maintenance_scheduler_report_with_audit_reports_for_entry_at(
        &self,
        entry: &TableEntry,
        now: OffsetDateTime,
        reports: Option<Vec<TableMetadataMaintenanceReport>>,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerReport> {
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        let effective = self
            .get_effective_table_maintenance_config_for_entry_unlocked(&entry.table_bucket, &namespace, &table, entry)
            .await?;
        let current = self
            .get_table_metadata_maintenance_report_for_entry_unlocked(
                &entry.table_bucket,
                &namespace,
                &table,
                &entry.table_id,
                MAINTENANCE_JOB_ALIAS_CURRENT,
            )
            .await?;
        let reports = match reports {
            Some(reports) => reports,
            None => {
                let objects = self.list_table_metadata_maintenance_audit_objects_for_entry(entry).await?;
                self.read_table_metadata_maintenance_audit_reports_for_entry(entry, &objects)
                    .await?
            }
        };
        let quarantine = table_maintenance_scheduler_quarantine_boundary(&effective.config, &reports);
        let mut recommended_actions = Vec::new();

        let status = if !effective.config.background_enabled {
            push_unique_maintenance_action(
                &mut recommended_actions,
                TableMaintenanceRecommendedAction::EnableBackgroundMaintenance,
            );
            TableMaintenanceSchedulerStatus::Disabled
        } else if effective.config.worker_paused {
            push_unique_maintenance_action(&mut recommended_actions, TableMaintenanceRecommendedAction::ResumeMaintenanceWorker);
            TableMaintenanceSchedulerStatus::Paused
        } else if let Some(current) = current.as_ref()
            && matches!(current.job.status, TableMetadataMaintenanceJobStatus::Running)
            && table_maintenance_job_lease_is_active(&current.job, effective.config.worker_lease_timeout_seconds, now)
        {
            push_unique_maintenance_action(&mut recommended_actions, TableMaintenanceRecommendedAction::WaitForActiveWorker);
            TableMaintenanceSchedulerStatus::Backpressured
        } else if let Some(current) = current.as_ref()
            && matches!(current.job.status, TableMetadataMaintenanceJobStatus::Queued)
            && table_maintenance_scheduler_lease_is_active(&current.job, effective.config.worker_lease_timeout_seconds, now)
        {
            push_unique_maintenance_action(&mut recommended_actions, TableMaintenanceRecommendedAction::RunMaintenanceWorker);
            TableMaintenanceSchedulerStatus::Queued
        } else if let Some(current) = current.as_ref()
            && table_maintenance_job_retry_is_pending(&current.job, now)
        {
            push_unique_maintenance_action(&mut recommended_actions, TableMaintenanceRecommendedAction::WaitForRetryBackoff);
            TableMaintenanceSchedulerStatus::RetryDeferred
        } else if quarantine.active {
            push_unique_maintenance_action(&mut recommended_actions, TableMaintenanceRecommendedAction::ReviewQuarantine);
            push_unique_maintenance_action(&mut recommended_actions, TableMaintenanceRecommendedAction::InvestigateFailure);
            TableMaintenanceSchedulerStatus::Quarantined
        } else {
            push_unique_maintenance_action(&mut recommended_actions, TableMaintenanceRecommendedAction::NoActionRequired);
            TableMaintenanceSchedulerStatus::Ready
        };

        Ok(TableMaintenanceSchedulerReport {
            table_bucket: entry.table_bucket.clone(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            table_id: entry.table_id.clone(),
            status,
            config_source: effective.source,
            background_enabled: effective.config.background_enabled,
            worker_paused: effective.config.worker_paused,
            delete_enabled: effective.config.delete_enabled,
            worker_lease_timeout_seconds: effective.config.worker_lease_timeout_seconds,
            max_retry_attempts: effective.config.max_retry_attempts,
            retry_initial_backoff_seconds: effective.config.retry_initial_backoff_seconds,
            retry_max_backoff_seconds: effective.config.retry_max_backoff_seconds,
            recommended_actions,
            current_job: current.as_ref().map(table_maintenance_scheduler_job_summary),
            quarantine,
            audit_timeline: reports.iter().map(table_maintenance_scheduler_job_summary).collect(),
        })
    }

    pub(crate) async fn run_table_maintenance_scheduler_once(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        scheduler_id: String,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerRunResult> {
        self.run_table_maintenance_scheduler_once_at(table_bucket, namespace, table, scheduler_id, OffsetDateTime::now_utc())
            .await
    }

    pub(in crate::table_catalog) async fn run_table_maintenance_scheduler_once_at(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        scheduler_id: String,
        now: OffsetDateTime,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerRunResult> {
        self.run_table_maintenance_scheduler_once_with_guard_at(table_bucket, namespace, table, scheduler_id, now, None)
            .await
    }

    pub(super) async fn run_table_maintenance_scheduler_once_with_guard_at(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        scheduler_id: String,
        now: OffsetDateTime,
        periodic_guard: Option<&PeriodicTableMaintenanceQueueGuard<'_>>,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerRunResult> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let _publication_guard = self.acquire_table_maintenance_publication_permit(table_bucket).await?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let namespace_name = namespace.public_name();
        let table_name = table.as_str().to_string();

        let preflight = {
            let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket, namespace_name, table_name
                )));
            };
            let effective = self
                .get_effective_table_maintenance_config_for_entry_unlocked(table_bucket, &namespace, &table, &entry)
                .await?;
            if let Some(periodic_guard) = periodic_guard {
                self.validate_periodic_table_maintenance_queue_guard(
                    TableMaintenancePreflightContext {
                        table_bucket,
                        namespace: &namespace,
                        table: &table,
                        entry: &entry,
                    },
                    &effective,
                    periodic_guard,
                )
                .await?;
            }
            self.table_metadata_maintenance_scheduler_preflight(
                TableMaintenancePreflightContext {
                    table_bucket,
                    namespace: &namespace,
                    table: &table,
                    entry: &entry,
                },
                &scheduler_id,
                now,
                effective,
            )
            .await?
        };
        let effective = match preflight {
            TableMaintenanceSchedulerPreflight::Ready(effective) => effective,
            TableMaintenanceSchedulerPreflight::Complete(report) => {
                let scheduler = self
                    .get_table_maintenance_scheduler_report_at(table_bucket, &namespace_name, &table_name, now)
                    .await?;
                return Ok(TableMaintenanceSchedulerRunResult {
                    report: *report,
                    scheduler,
                });
            }
        };

        let mut report = self
            .plan_table_metadata_maintenance(
                table_bucket,
                &namespace_name,
                &table_name,
                effective.config.retain_recent_metadata_files,
            )
            .await?;

        let report = {
            let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket, namespace_name, table_name
                )));
            };
            let effective = self
                .get_effective_table_maintenance_config_for_entry_unlocked(table_bucket, &namespace, &table, &entry)
                .await?;
            if let Some(periodic_guard) = periodic_guard {
                self.validate_periodic_table_maintenance_queue_guard(
                    TableMaintenancePreflightContext {
                        table_bucket,
                        namespace: &namespace,
                        table: &table,
                        entry: &entry,
                    },
                    &effective,
                    periodic_guard,
                )
                .await?;
            }
            match self
                .table_metadata_maintenance_scheduler_preflight(
                    TableMaintenancePreflightContext {
                        table_bucket,
                        namespace: &namespace,
                        table: &table,
                        entry: &entry,
                    },
                    &scheduler_id,
                    now,
                    effective,
                )
                .await?
            {
                TableMaintenanceSchedulerPreflight::Ready(effective) => {
                    if report.job.retain_recent_metadata_files != effective.config.retain_recent_metadata_files {
                        return Err(TableCatalogStoreError::Conflict(
                            "maintenance config changed before scheduler claim".to_string(),
                        ));
                    }
                    if entry.metadata_location != report.current_metadata_location {
                        return Err(TableCatalogStoreError::Conflict(
                            "current metadata location changed before maintenance scheduler claim".to_string(),
                        ));
                    }

                    let before_status = Some(report.job.status.clone());
                    let before_quarantined_object_count = Some(report.job.quarantined_object_count);
                    let scheduled_at = maintenance_timestamp(now);
                    report.job.operation = if effective.config.delete_enabled {
                        TableMetadataMaintenanceOperation::Delete
                    } else {
                        TableMetadataMaintenanceOperation::DryRun
                    };
                    report.job.status = TableMetadataMaintenanceJobStatus::Queued;
                    report.job.failure_reason = None;
                    report.job.config_source = effective.source;
                    report.job.scheduler_id = Some(scheduler_id);
                    report.job.scheduler_lease_id = Uuid::new_v4().to_string();
                    report.job.scheduled_at = Some(scheduled_at);
                    report.job.worker_id = None;
                    report.job.lease_id = String::new();
                    report.job.attempt = 0;
                    report.job.max_retry_attempts = effective.config.max_retry_attempts;
                    report.job.next_retry_after = None;
                    report.job.quarantine_enabled = effective.config.quarantine_enabled;
                    report.job.quarantine_retention_seconds = effective.config.quarantine_retention_seconds;
                    report.job.heartbeat_at = None;
                    report.job.started_at = None;
                    report.job.finished_at = None;
                    refresh_table_maintenance_report_recommended_actions(&mut report);
                    push_table_maintenance_audit_event(
                        &mut report,
                        now,
                        TableMaintenanceAuditActor::Scheduler,
                        TableMaintenanceAuditAction::SchedulerQueued,
                        None,
                        before_status,
                        before_quarantined_object_count,
                    );
                    self.put_table_metadata_maintenance_report_for_entry(&report, &entry).await?;
                    report
                }
                TableMaintenanceSchedulerPreflight::Complete(report) => *report,
            }
        };

        let scheduler = self
            .get_table_maintenance_scheduler_report_at(table_bucket, &namespace_name, &table_name, now)
            .await?;
        Ok(TableMaintenanceSchedulerRunResult { report, scheduler })
    }

    pub(crate) async fn apply_table_maintenance_quarantine_operation(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        job_id: &str,
        request: TableMaintenanceQuarantineOperationRequest,
    ) -> TableCatalogStoreResult<TableMaintenanceQuarantineOperationResult> {
        let action = request.action.clone();
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let namespace_name = namespace.public_name();
        let table_name = table.as_str().to_string();

        let report = if matches!(action, TableMaintenanceQuarantineAction::Inspect) {
            self.get_table_metadata_maintenance_report(table_bucket, &namespace_name, &table_name, job_id)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::NotFound(format!(
                        "maintenance job {}/{}/{}/{}",
                        table_bucket, namespace_name, table_name, job_id
                    ))
                })?
        } else {
            let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
            let _publication_guard = self.acquire_table_maintenance_publication_permit(table_bucket).await?;
            let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket, namespace_name, table_name
                )));
            };
            let Some(mut report) = self
                .get_table_metadata_maintenance_report_for_entry_unlocked(
                    table_bucket,
                    &namespace,
                    &table,
                    &entry.table_id,
                    MAINTENANCE_JOB_ALIAS_CURRENT,
                )
                .await?
            else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "maintenance job {}/{}/{}/{}",
                    table_bucket, namespace_name, table_name, job_id
                )));
            };
            if report.job.job_id != job_id {
                return Err(TableCatalogStoreError::Conflict("maintenance job is not current".to_string()));
            }
            if !matches!(report.job.status, TableMetadataMaintenanceJobStatus::Failed) {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance quarantine operation requires a failed job".to_string(),
                ));
            }
            if !report.job.quarantine_enabled || report.job.quarantined_object_count == 0 {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance job has no active quarantine boundary".to_string(),
                ));
            }

            let before_status = Some(report.job.status.clone());
            let before_quarantined_object_count = Some(report.job.quarantined_object_count);
            report.job.quarantined_object_count = 0;
            match &action {
                TableMaintenanceQuarantineAction::Inspect => unreachable!("inspect branch handled before mutation"),
                TableMaintenanceQuarantineAction::Release => {
                    report.job.failure_reason =
                        Some(table_maintenance_quarantine_operator_reason("released", request.reason.as_deref()));
                }
                TableMaintenanceQuarantineAction::Retry => {
                    report.job.next_retry_after = None;
                    report.job.failure_reason = Some(table_maintenance_quarantine_operator_reason(
                        "released for retry",
                        request.reason.as_deref(),
                    ));
                }
                TableMaintenanceQuarantineAction::Abandon => {
                    report.job.next_retry_after = None;
                    report.job.failure_reason =
                        Some(table_maintenance_quarantine_operator_reason("abandoned", request.reason.as_deref()));
                }
            }
            refresh_table_maintenance_report_recommended_actions(&mut report);
            let audit_action = match &action {
                TableMaintenanceQuarantineAction::Inspect => unreachable!("inspect branch handled before mutation"),
                TableMaintenanceQuarantineAction::Release => TableMaintenanceAuditAction::QuarantineRelease,
                TableMaintenanceQuarantineAction::Retry => TableMaintenanceAuditAction::QuarantineRetry,
                TableMaintenanceQuarantineAction::Abandon => TableMaintenanceAuditAction::QuarantineAbandon,
            };
            push_table_maintenance_audit_event(
                &mut report,
                OffsetDateTime::now_utc(),
                TableMaintenanceAuditActor::Operator,
                audit_action,
                request.reason,
                before_status,
                before_quarantined_object_count,
            );
            self.put_table_metadata_maintenance_report_for_entry(&report, &entry).await?;
            report
        };

        let scheduler = self
            .get_table_maintenance_scheduler_report(table_bucket, &namespace_name, &table_name)
            .await?;
        Ok(TableMaintenanceQuarantineOperationResult {
            action,
            report,
            scheduler,
        })
    }

    pub(super) async fn list_table_metadata_maintenance_audit_objects_for_entry(
        &self,
        entry: &TableEntry,
    ) -> TableCatalogStoreResult<Vec<String>> {
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        let prefix = self
            .paths
            .table_maintenance_jobs_prefix(&entry.table_bucket, &namespace, &table, &entry.table_id);
        let mut objects = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let remaining = TABLE_MAINTENANCE_SCHEDULER_HISTORY_MAX_OBJECTS.saturating_sub(objects.len());
            let limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS.min(remaining + 1)).ok_or_else(|| {
                TableCatalogStoreError::Internal("maintenance scheduler page limit must be positive".to_string())
            })?;
            let page = self
                .backend
                .list_objects_page(self.catalog_bucket(), &prefix, cursor.as_deref(), limit)
                .await?;
            if page.objects.len() > limit.get() || (page.is_truncated && page.objects.is_empty()) {
                return Err(TableCatalogStoreError::Invalid(
                    "invalid maintenance scheduler history listing page".to_string(),
                ));
            }
            for object in page.objects {
                if !object.starts_with(&prefix) || cursor.as_ref().is_some_and(|cursor| object <= *cursor) {
                    return Err(TableCatalogStoreError::Invalid(
                        "maintenance scheduler history listing escaped its prefix or did not advance".to_string(),
                    ));
                }
                if objects.len() == TABLE_MAINTENANCE_SCHEDULER_HISTORY_MAX_OBJECTS {
                    return Err(TableCatalogStoreError::Unavailable(
                        "maintenance scheduler history exceeds the 4096-object read budget".to_string(),
                    ));
                }
                cursor = Some(object.clone());
                objects.push(object);
            }
            if !page.is_truncated {
                return Ok(objects);
            }
        }
    }

    pub(super) async fn read_table_metadata_maintenance_audit_reports_for_entry(
        &self,
        entry: &TableEntry,
        objects: &[String],
    ) -> TableCatalogStoreResult<Vec<TableMetadataMaintenanceReport>> {
        self.read_table_metadata_maintenance_audit_reports(
            &entry.table_bucket,
            &parse_namespace_for_store(&entry.namespace)?,
            &parse_table_for_store(&entry.table)?,
            &entry.table_id,
            objects,
            true,
        )
        .await
    }

    async fn read_table_metadata_maintenance_audit_reports(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
        objects: &[String],
        require_present: bool,
    ) -> TableCatalogStoreResult<Vec<TableMetadataMaintenanceReport>> {
        let mut reports = Vec::new();
        for object in objects {
            if !object.ends_with(".json") {
                continue;
            }
            if let Some((report, _)) = self
                .read_entry::<TableMetadataMaintenanceReport>(self.catalog_bucket(), object)
                .await?
            {
                validate_table_maintenance_report_owner(&report, table_bucket, namespace, table, table_id)?;
                if self
                    .paths
                    .table_maintenance_job_path(table_bucket, namespace, table, table_id, &report.job.job_id)
                    != *object
                {
                    return Err(TableCatalogStoreError::Invalid(
                        "table maintenance report identity does not match its object path".to_string(),
                    ));
                }
                reports.push(table_maintenance_report_with_recommended_actions(report));
            } else if require_present {
                return Err(TableCatalogStoreError::Unavailable(
                    "maintenance scheduler history changed during the read; retry the scheduler report".to_string(),
                ));
            }
        }
        reports.sort_by(|left, right| {
            table_maintenance_report_order_timestamp(right)
                .cmp(&table_maintenance_report_order_timestamp(left))
                .then_with(|| left.job.job_id.cmp(&right.job.job_id))
        });
        reports.truncate(TABLE_MAINTENANCE_SCHEDULER_AUDIT_LIMIT);
        Ok(reports)
    }

    pub(super) async fn validate_periodic_table_maintenance_queue_guard(
        &self,
        context: TableMaintenancePreflightContext<'_>,
        effective: &TableMaintenanceEffectiveConfig,
        expected: &PeriodicTableMaintenanceQueueGuard<'_>,
    ) -> TableCatalogStoreResult<()> {
        if context.entry.table_id != expected.table_id {
            return Err(TableCatalogStoreError::Conflict(
                "table identity changed before periodic maintenance claim".to_string(),
            ));
        }
        if !effective.config.background_enabled || effective.config.worker_paused {
            return Err(TableCatalogStoreError::Conflict("periodic maintenance is disabled or paused".to_string()));
        }
        let current = self
            .get_table_metadata_maintenance_report_for_entry_unlocked(
                context.table_bucket,
                context.namespace,
                context.table,
                &context.entry.table_id,
                MAINTENANCE_JOB_ALIAS_CURRENT,
            )
            .await?;
        if current.as_ref().map(table_maintenance_scheduler_job_summary).as_ref() != expected.current_job
            || current
                .as_ref()
                .is_some_and(|report| report.job.status != TableMetadataMaintenanceJobStatus::Successful)
        {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance job changed or requires explicit operator action before periodic scheduling".to_string(),
            ));
        }
        let latest = self
            .get_table_metadata_maintenance_report_for_entry_unlocked(
                context.table_bucket,
                context.namespace,
                context.table,
                &context.entry.table_id,
                MAINTENANCE_JOB_ALIAS_LATEST,
            )
            .await?;
        // Report publication writes latest before current. A partial write must
        // not let an automatic pass replace an unresolved job or quarantine.
        if latest != current
            || current
                .as_ref()
                .is_some_and(|report| report.job.quarantine_enabled && report.job.quarantined_object_count > 0)
        {
            return Err(TableCatalogStoreError::Conflict(
                "maintenance report publication or quarantine requires recovery before periodic scheduling".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) async fn table_metadata_maintenance_scheduler_preflight(
        &self,
        context: TableMaintenancePreflightContext<'_>,
        scheduler_id: &str,
        now: OffsetDateTime,
        effective: TableMaintenanceEffectiveConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerPreflight> {
        self.table_metadata_maintenance_scheduler_preflight_with_publication(context, scheduler_id, now, effective, None)
            .await
    }

    pub(super) async fn table_metadata_maintenance_scheduler_preflight_with_publication(
        &self,
        context: TableMaintenancePreflightContext<'_>,
        scheduler_id: &str,
        now: OffsetDateTime,
        effective: TableMaintenanceEffectiveConfig,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerPreflight> {
        if !effective.config.background_enabled {
            let report = self
                .put_table_metadata_maintenance_scheduler_control_report(
                    TableMaintenanceSchedulerControlReport {
                        table_bucket: context.table_bucket,
                        namespace: context.namespace,
                        table: context.table,
                        entry: context.entry,
                        scheduler_id: scheduler_id.to_string(),
                        effective: &effective,
                        status: TableMetadataMaintenanceJobStatus::Disabled,
                        reason: "background maintenance is disabled",
                        now,
                    },
                    publication,
                )
                .await?;
            return Ok(TableMaintenanceSchedulerPreflight::Complete(Box::new(report)));
        }
        if effective.config.worker_paused {
            let report = self
                .put_table_metadata_maintenance_scheduler_control_report(
                    TableMaintenanceSchedulerControlReport {
                        table_bucket: context.table_bucket,
                        namespace: context.namespace,
                        table: context.table,
                        entry: context.entry,
                        scheduler_id: scheduler_id.to_string(),
                        effective: &effective,
                        status: TableMetadataMaintenanceJobStatus::Paused,
                        reason: "background maintenance worker is paused",
                        now,
                    },
                    publication,
                )
                .await?;
            return Ok(TableMaintenanceSchedulerPreflight::Complete(Box::new(report)));
        }

        if let Some(current) = self
            .get_table_metadata_maintenance_report_for_entry_unlocked(
                context.table_bucket,
                context.namespace,
                context.table,
                &context.entry.table_id,
                MAINTENANCE_JOB_ALIAS_CURRENT,
            )
            .await?
        {
            if matches!(current.job.status, TableMetadataMaintenanceJobStatus::Running) {
                if table_maintenance_job_lease_is_active(&current.job, effective.config.worker_lease_timeout_seconds, now) {
                    return Ok(TableMaintenanceSchedulerPreflight::Complete(Box::new(current)));
                }
                self.expire_table_maintenance_job(
                    current,
                    context.entry,
                    now,
                    "maintenance worker lease expired",
                    TableMaintenanceAuditAction::WorkerLeaseExpired,
                    publication,
                )
                .await?;
            } else if matches!(current.job.status, TableMetadataMaintenanceJobStatus::Queued) {
                if table_maintenance_scheduler_lease_is_active(&current.job, effective.config.worker_lease_timeout_seconds, now) {
                    return Ok(TableMaintenanceSchedulerPreflight::Complete(Box::new(current)));
                }
                self.expire_table_maintenance_job(
                    current,
                    context.entry,
                    now,
                    "maintenance scheduler lease expired",
                    TableMaintenanceAuditAction::SchedulerLeaseExpired,
                    publication,
                )
                .await?;
            } else if table_maintenance_job_retry_is_pending(&current.job, now) {
                return Ok(TableMaintenanceSchedulerPreflight::Complete(Box::new(current)));
            }
        }

        Ok(TableMaintenanceSchedulerPreflight::Ready(effective))
    }

    pub(crate) async fn run_table_metadata_maintenance_worker_once_conditionally(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        worker_id: String,
        condition: &TableMaintenanceWorkerCondition,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        self.run_table_metadata_maintenance_worker_once_conditionally_with_clock(
            table_bucket,
            namespace,
            table,
            worker_id,
            condition,
            OffsetDateTime::now_utc,
        )
        .await
    }

    pub(in crate::table_catalog) async fn run_table_metadata_maintenance_worker_once_conditionally_with_clock(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        worker_id: String,
        condition: &TableMaintenanceWorkerCondition,
        clock: impl FnOnce() -> OffsetDateTime + Send,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        validate_table_maintenance_worker_condition(&worker_id, condition)?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        publication.attach_catalog_migration_read_guard(self.acquire_object_backed_catalog_write_permit(table_bucket).await?)?;
        publication.begin_table_bucket(table_bucket).await?;
        let _publication_completion = TableCommitPublicationCompletion::new(&publication);
        self.ensure_no_active_table_rename(table_bucket).await?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        publication.attach_catalog_write_guard(guard)?;
        publication
            .prepare(table_bucket, &namespace.public_name(), table.as_str())
            .await?;
        let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        self.ensure_exclusive_maintenance_roots(&entry, Some(&table_path)).await?;
        self.run_conditional_table_maintenance_worker_for_entry(&entry, worker_id, condition, clock, &publication)
            .await
    }

    pub(super) async fn run_conditional_table_maintenance_worker_for_entry(
        &self,
        entry: &TableEntry,
        worker_id: String,
        condition: &TableMaintenanceWorkerCondition,
        clock: impl FnOnce() -> OffsetDateTime + Send,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        validate_table_maintenance_worker_condition(&worker_id, condition)?;
        ensure_table_maintenance_publication_held(Some(publication), entry, "before conditional worker claim")?;
        if entry.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::Conflict(
                "conditional maintenance requires an active table".to_string(),
            ));
        }
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        let effective = self
            .get_effective_table_maintenance_config_for_entry_unlocked(&entry.table_bucket, &namespace, &table, entry)
            .await?;
        let mut reports = Vec::with_capacity(3);
        for job_id in [
            condition.expected_job_id.as_str(),
            MAINTENANCE_JOB_ALIAS_LATEST,
            MAINTENANCE_JOB_ALIAS_CURRENT,
        ] {
            let report = self.get_table_metadata_maintenance_report_for_entry_unlocked(
                &entry.table_bucket, &namespace, &table, &entry.table_id, job_id,
            ).await?.ok_or_else(|| TableCatalogStoreError::Conflict(
                "conditional maintenance requires fully published named, latest, and current reports; inspect the persisted job".to_string(),
            ))?;
            reports.push(report);
        }
        let mut report = reports.remove(0);
        // Lock acquisition and report reads can outlast the queued job's lease.
        // Sample the claim time only after those waits have completed.
        let now = clock();
        let replay = validate_conditional_table_maintenance_claim(&report, entry, &effective, &worker_id, condition, now)?;
        if reports.iter().any(|alias| alias != &report) {
            // A complete terminal receipt can finish publishing aliases after a
            // lost response. Never resume a partial Running claim or rerun deletes.
            if !replay
                || !matches!(
                    report.job.status,
                    TableMetadataMaintenanceJobStatus::Successful | TableMetadataMaintenanceJobStatus::Failed
                )
                || reports
                    .iter()
                    .any(|alias| alias != &report && !maintenance_terminal_report_continues_running(&report, alias))
            {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance report publication is incomplete; inspect the named job and recover before another claim"
                        .to_string(),
                ));
            }
            self.put_table_metadata_maintenance_report_for_entry_with_publication(&report, entry, Some(publication))
                .await?;
            return Ok(report);
        }
        if replay {
            return Ok(report);
        }

        // Backoff delays follow the validated current policy; its attempt limit
        // must never replace the budget persisted when this job was scheduled.
        let mut retry_effective = effective;
        retry_effective.config.max_retry_attempts = report.job.max_retry_attempts;
        validate_table_maintenance_config(&retry_effective.config)?;
        let before_status = Some(report.job.status.clone());
        let before_quarantined_object_count = Some(report.job.quarantined_object_count);
        let started_at = maintenance_timestamp(now);
        report.job.attempt = report
            .job
            .attempt
            .checked_add(1)
            .ok_or_else(|| TableCatalogStoreError::Invalid("maintenance attempt exceeds its supported range".to_string()))?;
        report.job.status = TableMetadataMaintenanceJobStatus::Running;
        report.job.failure_reason = None;
        report.job.worker_id = Some(worker_id);
        report.job.lease_id = Uuid::new_v4().to_string();
        report.job.next_retry_after = None;
        report.job.heartbeat_at = Some(started_at.clone());
        report.job.started_at = Some(started_at);
        report.job.finished_at = None;
        refresh_table_maintenance_report_recommended_actions(&mut report);
        push_table_maintenance_audit_event(
            &mut report,
            now,
            TableMaintenanceAuditActor::Worker,
            TableMaintenanceAuditAction::WorkerStarted,
            None,
            before_status,
            before_quarantined_object_count,
        );
        self.put_table_metadata_maintenance_report_for_entry_with_publication(&report, entry, Some(publication))
            .await?;

        let delete = matches!(report.job.operation, TableMetadataMaintenanceOperation::Delete);
        self.finish_table_metadata_maintenance_run_for_entry_with_optional_publication(
            entry,
            delete,
            &retry_effective,
            report,
            Some(publication),
            true,
        )
        .await
    }

    pub(crate) async fn run_table_metadata_maintenance_worker_once(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        worker_id: String,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        self.run_table_metadata_maintenance_worker_once_at(table_bucket, namespace, table, worker_id, OffsetDateTime::now_utc())
            .await
    }

    pub(in crate::table_catalog) async fn run_table_metadata_maintenance_worker_once_at(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        worker_id: String,
        now: OffsetDateTime,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let publication = self.metadata_maintenance_publication(table_bucket).await?;
        let _completion = TableCommitPublicationCompletion::new(&publication);
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let namespace_name = namespace.public_name();
        let table_name = table.as_str().to_string();

        let (effective, queued) = {
            let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket, namespace_name, table_name
                )));
            };
            let effective = self
                .get_effective_table_maintenance_config_for_entry_unlocked(table_bucket, &namespace, &table, &entry)
                .await?;
            match self
                .table_metadata_maintenance_worker_preflight(
                    TableMaintenancePreflightContext {
                        table_bucket,
                        namespace: &namespace,
                        table: &table,
                        entry: &entry,
                    },
                    &worker_id,
                    now,
                    effective,
                )
                .await?
            {
                TableMaintenanceWorkerPreflight::Ready { effective, queued } => (effective, queued),
                TableMaintenanceWorkerPreflight::Complete(report) => return Ok(*report),
            }
        };

        let mut report = if let Some(queued) = queued {
            *queued
        } else {
            self.plan_table_metadata_maintenance(
                table_bucket,
                &namespace_name,
                &table_name,
                effective.config.retain_recent_metadata_files,
            )
            .await?
        };

        let (report, effective) = {
            let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket, namespace_name, table_name
                )));
            };
            let effective = self
                .get_effective_table_maintenance_config_for_entry_unlocked(table_bucket, &namespace, &table, &entry)
                .await?;
            let (effective, queued) = match self
                .table_metadata_maintenance_worker_preflight(
                    TableMaintenancePreflightContext {
                        table_bucket,
                        namespace: &namespace,
                        table: &table,
                        entry: &entry,
                    },
                    &worker_id,
                    now,
                    effective,
                )
                .await?
            {
                TableMaintenanceWorkerPreflight::Ready { effective, queued } => (effective, queued),
                TableMaintenanceWorkerPreflight::Complete(report) => return Ok(*report),
            };
            if let Some(queued) = queued {
                if queued.job.job_id != report.job.job_id {
                    return Err(TableCatalogStoreError::Conflict(
                        "queued maintenance job changed before worker claim".to_string(),
                    ));
                }
                report = *queued;
            }

            if report.job.retain_recent_metadata_files != effective.config.retain_recent_metadata_files {
                return Err(TableCatalogStoreError::Conflict(
                    "maintenance config changed before worker claim".to_string(),
                ));
            }
            if entry.metadata_location != report.current_metadata_location {
                return Err(TableCatalogStoreError::Conflict(
                    "current metadata location changed before maintenance worker claim".to_string(),
                ));
            }

            let was_queued_claim = matches!(report.job.status, TableMetadataMaintenanceJobStatus::Queued);
            let before_status = Some(report.job.status.clone());
            let before_quarantined_object_count = Some(report.job.quarantined_object_count);
            let started_at = maintenance_timestamp(now);
            if !was_queued_claim {
                report.job.operation = if effective.config.delete_enabled {
                    TableMetadataMaintenanceOperation::Delete
                } else {
                    TableMetadataMaintenanceOperation::DryRun
                };
            }
            report.job.status = TableMetadataMaintenanceJobStatus::Running;
            report.job.failure_reason = None;
            report.job.config_source = effective.source;
            report.job.worker_id = Some(worker_id);
            report.job.lease_id = Uuid::new_v4().to_string();
            report.job.attempt = 1;
            report.job.max_retry_attempts = effective.config.max_retry_attempts;
            report.job.next_retry_after = None;
            report.job.quarantine_enabled = effective.config.quarantine_enabled;
            report.job.quarantine_retention_seconds = effective.config.quarantine_retention_seconds;
            report.job.heartbeat_at = Some(started_at.clone());
            report.job.started_at = Some(started_at);
            report.job.finished_at = None;
            refresh_table_maintenance_report_recommended_actions(&mut report);
            push_table_maintenance_audit_event(
                &mut report,
                now,
                TableMaintenanceAuditActor::Worker,
                TableMaintenanceAuditAction::WorkerStarted,
                None,
                before_status,
                before_quarantined_object_count,
            );
            self.put_table_metadata_maintenance_report_for_entry(&report, &entry).await?;

            (report, effective)
        };

        self.finish_table_metadata_maintenance_run(table_bucket, &namespace_name, &table_name, &publication, &effective, report)
            .await
    }

    pub(super) async fn table_metadata_maintenance_worker_preflight(
        &self,
        context: TableMaintenancePreflightContext<'_>,
        worker_id: &str,
        now: OffsetDateTime,
        effective: TableMaintenanceEffectiveConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceWorkerPreflight> {
        self.table_metadata_maintenance_worker_preflight_with_publication(context, worker_id, now, effective, None)
            .await
    }

    pub(super) async fn table_metadata_maintenance_worker_preflight_with_publication(
        &self,
        context: TableMaintenancePreflightContext<'_>,
        worker_id: &str,
        now: OffsetDateTime,
        effective: TableMaintenanceEffectiveConfig,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<TableMaintenanceWorkerPreflight> {
        if !effective.config.background_enabled {
            let report = self
                .put_table_metadata_maintenance_worker_control_report(
                    TableMaintenanceWorkerControlReport {
                        table_bucket: context.table_bucket,
                        namespace: context.namespace,
                        table: context.table,
                        entry: context.entry,
                        worker_id: worker_id.to_string(),
                        effective: &effective,
                        status: TableMetadataMaintenanceJobStatus::Disabled,
                        reason: "background maintenance is disabled",
                        now,
                    },
                    publication,
                )
                .await?;
            return Ok(TableMaintenanceWorkerPreflight::Complete(Box::new(report)));
        }
        if effective.config.worker_paused {
            let report = self
                .put_table_metadata_maintenance_worker_control_report(
                    TableMaintenanceWorkerControlReport {
                        table_bucket: context.table_bucket,
                        namespace: context.namespace,
                        table: context.table,
                        entry: context.entry,
                        worker_id: worker_id.to_string(),
                        effective: &effective,
                        status: TableMetadataMaintenanceJobStatus::Paused,
                        reason: "background maintenance worker is paused",
                        now,
                    },
                    publication,
                )
                .await?;
            return Ok(TableMaintenanceWorkerPreflight::Complete(Box::new(report)));
        }

        if let Some(current) = self
            .get_table_metadata_maintenance_report_for_entry_unlocked(
                context.table_bucket,
                context.namespace,
                context.table,
                &context.entry.table_id,
                MAINTENANCE_JOB_ALIAS_CURRENT,
            )
            .await?
        {
            if matches!(current.job.status, TableMetadataMaintenanceJobStatus::Running) {
                if table_maintenance_job_lease_is_active(&current.job, effective.config.worker_lease_timeout_seconds, now) {
                    return Ok(TableMaintenanceWorkerPreflight::Complete(Box::new(current)));
                }
                self.expire_table_maintenance_job(
                    current,
                    context.entry,
                    now,
                    "maintenance worker lease expired",
                    TableMaintenanceAuditAction::WorkerLeaseExpired,
                    publication,
                )
                .await?;
            } else if matches!(current.job.status, TableMetadataMaintenanceJobStatus::Queued) {
                if table_maintenance_scheduler_lease_is_active(&current.job, effective.config.worker_lease_timeout_seconds, now) {
                    return Ok(TableMaintenanceWorkerPreflight::Ready {
                        effective,
                        queued: Some(Box::new(current)),
                    });
                }
                self.expire_table_maintenance_job(
                    current,
                    context.entry,
                    now,
                    "maintenance scheduler lease expired",
                    TableMaintenanceAuditAction::SchedulerLeaseExpired,
                    publication,
                )
                .await?;
            } else if table_maintenance_job_retry_is_pending(&current.job, now) {
                return Ok(TableMaintenanceWorkerPreflight::Complete(Box::new(current)));
            }
        }

        Ok(TableMaintenanceWorkerPreflight::Ready { effective, queued: None })
    }

    pub(crate) async fn heartbeat_table_metadata_maintenance_job(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        job_id: &str,
        lease_id: &str,
        worker_id: &str,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        self.heartbeat_table_metadata_maintenance_job_at(
            TableMaintenanceHeartbeatRef {
                table_bucket,
                namespace,
                table,
                job_id,
                lease_id,
                worker_id,
            },
            OffsetDateTime::now_utc(),
        )
        .await
    }

    pub(in crate::table_catalog) async fn heartbeat_table_metadata_maintenance_job_at(
        &self,
        heartbeat: TableMaintenanceHeartbeatRef<'_>,
        now: OffsetDateTime,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let namespace = parse_namespace_for_store(heartbeat.namespace)?;
        let table = parse_table_for_store(heartbeat.table)?;
        let _migration_guard = self
            .acquire_object_backed_catalog_write_permit(heartbeat.table_bucket)
            .await?;
        let _publication_guard = self
            .acquire_table_maintenance_publication_permit(heartbeat.table_bucket)
            .await?;
        let table_path = self.paths.table_entry_path(heartbeat.table_bucket, &namespace, &table);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        let Some((entry, _)) = self
            .read_table_with_etag_unlocked(heartbeat.table_bucket, &namespace, &table)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                heartbeat.table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        self.heartbeat_table_metadata_maintenance_job_for_entry(&entry, heartbeat, now)
            .await
    }

    pub(super) async fn heartbeat_table_metadata_maintenance_job_for_entry(
        &self,
        entry: &TableEntry,
        heartbeat: TableMaintenanceHeartbeatRef<'_>,
        now: OffsetDateTime,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        self.heartbeat_table_metadata_maintenance_job_for_entry_with_publication(entry, heartbeat, now, None)
            .await
    }

    pub(super) async fn heartbeat_table_metadata_maintenance_job_for_entry_with_publication(
        &self,
        entry: &TableEntry,
        heartbeat: TableMaintenanceHeartbeatRef<'_>,
        now: OffsetDateTime,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        let Some(mut report) = self
            .get_table_metadata_maintenance_report_for_entry_unlocked(
                &entry.table_bucket,
                &namespace,
                &table,
                &entry.table_id,
                MAINTENANCE_JOB_ALIAS_CURRENT,
            )
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "maintenance job {}/{}/{}/{}",
                heartbeat.table_bucket,
                namespace.public_name(),
                table.as_str(),
                heartbeat.job_id
            )));
        };
        if report.job.job_id != heartbeat.job_id {
            return Err(TableCatalogStoreError::Conflict("maintenance job is not current".to_string()));
        }
        if !matches!(report.job.status, TableMetadataMaintenanceJobStatus::Running) {
            return Err(TableCatalogStoreError::Conflict("maintenance job is not running".to_string()));
        }
        if report.job.lease_id != heartbeat.lease_id {
            return Err(TableCatalogStoreError::Conflict("maintenance lease does not match".to_string()));
        }
        if report.job.worker_id.as_deref() != Some(heartbeat.worker_id) {
            return Err(TableCatalogStoreError::Conflict("maintenance worker does not match".to_string()));
        }

        report.job.heartbeat_at = Some(maintenance_timestamp(now));
        refresh_table_maintenance_report_recommended_actions(&mut report);
        let before_quarantined_object_count = Some(report.job.quarantined_object_count);
        push_table_maintenance_audit_event(
            &mut report,
            now,
            TableMaintenanceAuditActor::Worker,
            TableMaintenanceAuditAction::WorkerHeartbeat,
            None,
            Some(TableMetadataMaintenanceJobStatus::Running),
            before_quarantined_object_count,
        );
        self.put_table_metadata_maintenance_report_for_entry_with_publication(&report, entry, publication)
            .await?;
        Ok(report)
    }

    async fn expire_table_maintenance_job(
        &self,
        mut report: TableMetadataMaintenanceReport,
        entry: &TableEntry,
        now: OffsetDateTime,
        reason: &str,
        action: TableMaintenanceAuditAction,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let before_status = Some(report.job.status.clone());
        let before_quarantined_object_count = Some(report.job.quarantined_object_count);
        report.job.status = TableMetadataMaintenanceJobStatus::Failed;
        report.job.failure_reason = Some(reason.to_string());
        report.job.finished_at = Some(maintenance_timestamp(now));
        refresh_table_maintenance_report_recommended_actions(&mut report);
        push_table_maintenance_audit_event(
            &mut report,
            now,
            TableMaintenanceAuditActor::Scheduler,
            action,
            Some(reason.to_string()),
            before_status,
            before_quarantined_object_count,
        );
        self.put_table_metadata_maintenance_report_for_entry_with_publication(&report, entry, publication)
            .await?;
        Ok(report)
    }

    async fn put_table_metadata_maintenance_scheduler_control_report(
        &self,
        control: TableMaintenanceSchedulerControlReport<'_>,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let timestamp = maintenance_timestamp(control.now);
        let cleanup_watermark_unix_seconds =
            (control.now - Duration::seconds(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS)).unix_timestamp();
        let current_metadata_location = control.entry.metadata_location.clone();
        let report = TableMetadataMaintenanceReport {
            job: TableMetadataMaintenanceJob {
                job_id: Uuid::new_v4().to_string(),
                table_bucket: control.table_bucket.to_string(),
                namespace: control.namespace.public_name(),
                table: control.table.as_str().to_string(),
                table_id: control.entry.table_id.clone(),
                operation: TableMetadataMaintenanceOperation::DryRun,
                status: control.status,
                failure_reason: Some(control.reason.to_string()),
                recommended_actions: Vec::new(),
                config_source: control.effective.source,
                scheduler_id: Some(control.scheduler_id),
                scheduler_lease_id: String::new(),
                scheduled_at: Some(timestamp.clone()),
                worker_id: None,
                lease_id: String::new(),
                attempt: 0,
                max_retry_attempts: control.effective.config.max_retry_attempts,
                next_retry_after: None,
                quarantine_enabled: control.effective.config.quarantine_enabled,
                quarantine_retention_seconds: control.effective.config.quarantine_retention_seconds,
                heartbeat_at: None,
                started_at: None,
                finished_at: Some(timestamp),
                current_metadata_location: current_metadata_location.clone(),
                current_generation: control.entry.generation,
                retain_recent_metadata_files: control.effective.config.retain_recent_metadata_files,
                safety_window_seconds: TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS,
                cleanup_watermark_unix_seconds,
                planned_metadata_file_count: 0,
                retained_metadata_file_count: 0,
                cleanup_candidate_count: 0,
                deletable_metadata_file_count: 0,
                deleted_metadata_file_count: 0,
                planned_object_file_count: 0,
                cleanup_candidate_object_count: 0,
                deletable_object_count: 0,
                deleted_object_count: 0,
                quarantined_object_count: 0,
            },
            current_metadata_location,
            retained_metadata_locations: Vec::new(),
            cleanup_candidate_locations: Vec::new(),
            deletable_metadata_locations: Vec::new(),
            cleanup_object_candidate_locations: Vec::new(),
            deletable_object_locations: Vec::new(),
            object_reports: Vec::new(),
            object_cleanup_reports: Vec::new(),
            referenced_object_reports: Vec::new(),
            reachability_graph: TableMaintenanceReachabilityGraphReport::default(),
            snapshot_expiration: None,
            compaction: None,
            audit_events: Vec::new(),
        };
        let mut report = table_maintenance_report_with_recommended_actions(report);
        push_table_maintenance_audit_event(
            &mut report,
            control.now,
            TableMaintenanceAuditActor::Scheduler,
            TableMaintenanceAuditAction::SchedulerControl,
            Some(control.reason.to_string()),
            None,
            None,
        );
        self.put_table_metadata_maintenance_report_for_entry_with_publication(&report, control.entry, publication)
            .await?;
        Ok(report)
    }

    async fn put_table_metadata_maintenance_worker_control_report(
        &self,
        control: TableMaintenanceWorkerControlReport<'_>,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let timestamp = maintenance_timestamp(control.now);
        let cleanup_watermark_unix_seconds =
            (control.now - Duration::seconds(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS)).unix_timestamp();
        let current_metadata_location = control.entry.metadata_location.clone();
        let report = TableMetadataMaintenanceReport {
            job: TableMetadataMaintenanceJob {
                job_id: Uuid::new_v4().to_string(),
                table_bucket: control.table_bucket.to_string(),
                namespace: control.namespace.public_name(),
                table: control.table.as_str().to_string(),
                table_id: control.entry.table_id.clone(),
                operation: TableMetadataMaintenanceOperation::DryRun,
                status: control.status,
                failure_reason: Some(control.reason.to_string()),
                recommended_actions: Vec::new(),
                config_source: control.effective.source,
                scheduler_id: None,
                scheduler_lease_id: String::new(),
                scheduled_at: None,
                worker_id: Some(control.worker_id),
                lease_id: String::new(),
                attempt: 0,
                max_retry_attempts: control.effective.config.max_retry_attempts,
                next_retry_after: None,
                quarantine_enabled: control.effective.config.quarantine_enabled,
                quarantine_retention_seconds: control.effective.config.quarantine_retention_seconds,
                heartbeat_at: None,
                started_at: Some(timestamp.clone()),
                finished_at: Some(timestamp),
                current_metadata_location: current_metadata_location.clone(),
                current_generation: control.entry.generation,
                retain_recent_metadata_files: control.effective.config.retain_recent_metadata_files,
                safety_window_seconds: TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS,
                cleanup_watermark_unix_seconds,
                planned_metadata_file_count: 0,
                retained_metadata_file_count: 0,
                cleanup_candidate_count: 0,
                deletable_metadata_file_count: 0,
                deleted_metadata_file_count: 0,
                planned_object_file_count: 0,
                cleanup_candidate_object_count: 0,
                deletable_object_count: 0,
                deleted_object_count: 0,
                quarantined_object_count: 0,
            },
            current_metadata_location,
            retained_metadata_locations: Vec::new(),
            cleanup_candidate_locations: Vec::new(),
            deletable_metadata_locations: Vec::new(),
            cleanup_object_candidate_locations: Vec::new(),
            deletable_object_locations: Vec::new(),
            object_reports: Vec::new(),
            object_cleanup_reports: Vec::new(),
            referenced_object_reports: Vec::new(),
            reachability_graph: TableMaintenanceReachabilityGraphReport::default(),
            snapshot_expiration: None,
            compaction: None,
            audit_events: Vec::new(),
        };
        let mut report = table_maintenance_report_with_recommended_actions(report);
        push_table_maintenance_audit_event(
            &mut report,
            control.now,
            TableMaintenanceAuditActor::Scheduler,
            TableMaintenanceAuditAction::WorkerControl,
            Some(control.reason.to_string()),
            None,
            None,
        );
        self.put_table_metadata_maintenance_report_for_entry_with_publication(&report, control.entry, publication)
            .await?;
        Ok(report)
    }
}

#[async_trait::async_trait]
impl<B> TableCatalogStore for ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    async fn get_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<TableBucketEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let object = self.paths.table_bucket_entry_path(table_bucket);
            let Some((entry, _)) = self.read_entry::<TableBucketEntry>(self.catalog_bucket(), &object).await? else {
                return Ok(None);
            };
            validate_table_bucket_entry_object(&self.paths, &object, &entry)?;
            self.validate_store_bucket_entry(&entry)?;
            Ok(Some(entry))
        })
        .await
    }

    async fn put_table_bucket(&self, mut entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        self.validate_store_bucket_entry(&entry)?;
        let _registry_guard = self.acquire_table_bucket_registry_write_permit().await?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&entry.table_bucket).await?;
        let object = self.paths.table_bucket_entry_path(&entry.table_bucket);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?;
        if let Some((current, _)) = self.read_table_bucket_with_etag_unlocked(&entry.table_bucket).await? {
            if current.active_rename_id.is_some() {
                return Err(TableCatalogStoreError::Unavailable(format!(
                    "table bucket {} has an active table rename",
                    entry.table_bucket
                )));
            }
            entry.updated_at = Some(next_table_catalog_update_time(current.updated_at.as_deref()));
        }
        self.write_entry_unlocked(
            self.catalog_bucket(),
            &object,
            &entry,
            TableCatalogPutPrecondition::Any,
            _guard.write_commit_guards(),
        )
        .await
    }

    async fn ensure_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        self.validate_store_bucket_entry(&entry)?;
        let _registry_guard = self.acquire_table_bucket_registry_write_permit().await?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&entry.table_bucket).await?;
        let object = self.paths.table_bucket_entry_path(&entry.table_bucket);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?;
        if let Some((current, _)) = self.read_table_bucket_with_etag_unlocked(&entry.table_bucket).await? {
            return if current.state == TableCatalogEntryState::Active {
                Ok(())
            } else {
                Err(TableCatalogStoreError::NotFound(format!("table bucket {}", entry.table_bucket)))
            };
        }
        self.write_entry_unlocked(
            self.catalog_bucket(),
            &object,
            &entry,
            TableCatalogPutPrecondition::IfAbsent,
            _guard.write_commit_guards(),
        )
        .await
    }

    async fn disable_empty_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        let mut disablement = self.begin_table_bucket_disablement(entry, None).await?;
        // Namespace creation holds this same bucket lock. Retained table, view, and
        // maintenance records also prevent removing their data-plane protections.
        let page = self
            .backend
            .list_objects_page(
                self.catalog_bucket(),
                &self.paths.namespace_entries_prefix(&disablement.entry.table_bucket),
                None,
                NonZeroUsize::MIN,
            )
            .await?;
        if !page.objects.is_empty() || page.is_truncated {
            return Err(TableCatalogStoreError::Conflict(
                "table bucket contains namespaces or retained catalog resources".to_string(),
            ));
        }
        self.finish_table_bucket_disablement(&mut disablement).await
    }

    async fn create_namespace(&self, entry: NamespaceEntry) -> TableCatalogStoreResult<()> {
        let namespace = validate_namespace_entry_identity(&entry)?;
        validate_namespace_properties(&entry.properties)?;
        self.require_table_bucket(&entry.table_bucket).await?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&entry.table_bucket).await?;
        let bucket_path = self.paths.table_bucket_entry_path(&entry.table_bucket);
        let _bucket_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &bucket_path).await?;
        let current = self.read_table_bucket_with_etag_unlocked(&entry.table_bucket).await?;
        if current
            .as_ref()
            .is_none_or(|(entry, _)| entry.state != TableCatalogEntryState::Active)
        {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {}", entry.table_bucket)));
        }
        if current.is_some_and(|(current, _)| current.active_rename_id.is_some()) {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "table bucket {} has an active table rename",
                entry.table_bucket
            )));
        }
        let object = self.paths.namespace_entry_path(&entry.table_bucket, &namespace);
        let _namespace_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?;
        let precondition = match self
            .read_entry_unlocked::<NamespaceEntry>(self.catalog_bucket(), &object)
            .await?
        {
            Some((current, etag)) => {
                validate_namespace_entry_object(&self.paths, &object, &current)?;
                if self.paths.scope == CatalogScope::Vector || current.state == TableCatalogEntryState::Active {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog object already exists: namespace {}/{}",
                        entry.table_bucket, entry.namespace
                    )));
                }
                etag.map(TableCatalogPutPrecondition::IfMatch)
                    .ok_or_else(|| TableCatalogStoreError::Internal(format!("catalog namespace entry has no etag: {object}")))?
            }
            None => {
                if self.has_active_namespace_object(&entry.table_bucket, &namespace).await?
                    || self.has_active_namespace_descendant(&entry.table_bucket, &namespace).await?
                {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "catalog object already exists: namespace {}/{}",
                        entry.table_bucket, entry.namespace
                    )));
                }
                TableCatalogPutPrecondition::IfAbsent
            }
        };
        self.write_entry_unlocked(
            self.catalog_bucket(),
            &object,
            &entry,
            precondition,
            _namespace_guard.write_commit_guards(),
        )
        .await
    }

    async fn list_namespaces(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            self.list_active_namespaces_with_prefix(&self.paths.namespace_entries_prefix(table_bucket))
                .await
        })
        .await
    }

    async fn list_namespaces_page(
        &self,
        table_bucket: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            self.list_entry_page(
                &self.paths.namespace_entries_prefix(table_bucket),
                NAMESPACE_ENTRY_FILE,
                cursor,
                limit,
                |entry: &NamespaceEntry| entry.state == TableCatalogEntryState::Active,
                |object, entry: &NamespaceEntry| validate_namespace_entry_object(&self.paths, object, entry),
            )
            .await
        })
        .await
    }

    async fn get_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Option<NamespaceEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            let namespace_path = self.paths.namespace_entry_path(table_bucket, &namespace);
            let exact = self
                .read_entry::<NamespaceEntry>(self.catalog_bucket(), &namespace_path)
                .await?
                .map(|(entry, _)| entry);
            if let Some(entry) = exact.as_ref() {
                validate_namespace_entry_object(&self.paths, &namespace_path, entry)?;
            }
            if self.paths.scope == CatalogScope::Vector
                || exact
                    .as_ref()
                    .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
            {
                return Ok(exact);
            }
            if self.has_active_namespace_object(table_bucket, &namespace).await?
                || self.has_active_namespace_descendant(table_bucket, &namespace).await?
            {
                return Ok(Some(synthetic_namespace_entry(table_bucket, &namespace)));
            }
            Ok(None)
        })
        .await
    }

    async fn update_namespace_properties(
        &self,
        table_bucket: &str,
        namespace: &str,
        update: NamespacePropertiesUpdate,
    ) -> TableCatalogStoreResult<NamespacePropertiesUpdateResult> {
        let namespace = parse_namespace_for_store(namespace)?;
        self.require_table_bucket(table_bucket).await?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let bucket_path = self.paths.table_bucket_entry_path(table_bucket);
        let _bucket_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &bucket_path).await?;
        let namespace_path = self.paths.namespace_entry_path(table_bucket, &namespace);
        let _namespace_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &namespace_path)
            .await?;

        let current = self
            .read_entry_unlocked::<NamespaceEntry>(self.catalog_bucket(), &namespace_path)
            .await?;
        let (mut next, precondition) = match current {
            Some((entry, etag)) => {
                validate_namespace_entry_object(&self.paths, &namespace_path, &entry)?;
                validate_namespace_properties(&entry.properties)?;
                if entry.state != TableCatalogEntryState::Active {
                    return Err(TableCatalogStoreError::NotFound(format!(
                        "namespace {table_bucket}/{}",
                        namespace.public_name()
                    )));
                }
                let etag = etag.ok_or_else(|| {
                    TableCatalogStoreError::Internal(format!("catalog namespace entry has no etag: {namespace_path}"))
                })?;
                (entry, TableCatalogPutPrecondition::IfMatch(etag))
            }
            None => {
                if !self.has_active_namespace_object(table_bucket, &namespace).await?
                    && !self.has_active_namespace_descendant(table_bucket, &namespace).await?
                {
                    return Err(TableCatalogStoreError::NotFound(format!(
                        "namespace {table_bucket}/{}",
                        namespace.public_name()
                    )));
                }
                (synthetic_namespace_entry(table_bucket, &namespace), TableCatalogPutPrecondition::IfAbsent)
            }
        };

        let before = next.clone();
        let result = update.apply_to(&mut next);
        validate_namespace_entry_object(&self.paths, &namespace_path, &next)?;
        validate_namespace_properties(&next.properties)?;
        if before == next {
            return Ok(result);
        }
        self.write_entry_unlocked(
            self.catalog_bucket(),
            &namespace_path,
            &next,
            precondition,
            _namespace_guard.write_commit_guards(),
        )
        .await?;
        Ok(result)
    }

    async fn list_namespaces_under(&self, table_bucket: &str, parent: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let parent = parse_namespace_for_store(parent)?;
            let prefix = format!("{}{}/", self.paths.namespace_entries_prefix(table_bucket), parent.storage_id());
            self.list_active_namespaces_with_prefix(&prefix).await
        })
        .await
    }

    async fn list_namespace_children(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
    ) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS)
                .ok_or_else(|| TableCatalogStoreError::Internal("catalog namespace list limit must be positive".to_string()))?;
            let mut entries = Vec::new();
            let mut cursor = None;
            loop {
                let page = self
                    .list_namespace_children_page_inner(table_bucket, parent, cursor.as_deref(), limit)
                    .await?;
                entries.extend(page.entries);
                let Some(next_cursor) = page.next_cursor else {
                    return Ok(entries);
                };
                if cursor.as_deref() == Some(next_cursor.as_str()) {
                    return Err(TableCatalogStoreError::Internal(
                        "catalog namespace child pagination did not advance".to_string(),
                    ));
                }
                cursor = Some(next_cursor);
            }
        })
        .await
    }

    async fn list_namespace_children_page(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            self.list_namespace_children_page_inner(table_bucket, parent, cursor, limit)
                .await
        })
        .await
    }

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(namespace)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let bucket_path = self.paths.table_bucket_entry_path(table_bucket);
        let _bucket_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &bucket_path).await?;
        if self
            .read_table_bucket_with_etag_unlocked(table_bucket)
            .await?
            .is_some_and(|(current, _)| current.active_rename_id.is_some())
        {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "table bucket {table_bucket} has an active table rename"
            )));
        }
        let namespace_path = self.paths.namespace_entry_path(table_bucket, &namespace);
        // Match create_namespace and migration lock order while draining table/view creation.
        let _namespace_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &namespace_path)
            .await?;
        if self.has_active_namespace_descendant(table_bucket, &namespace).await? {
            return Err(TableCatalogStoreError::Conflict(format!(
                "namespace {table_bucket}/{} has child namespaces",
                namespace.public_name()
            )));
        }
        if self.has_namespace_resource_entry(table_bucket, &namespace).await? {
            return Err(TableCatalogStoreError::Conflict(format!(
                "namespace {table_bucket}/{} is not empty",
                namespace.public_name()
            )));
        }
        let current = self
            .read_entry_unlocked::<NamespaceEntry>(self.catalog_bucket(), &namespace_path)
            .await?;
        let Some((current, _)) = current else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {}/{}",
                table_bucket,
                namespace.public_name()
            )));
        };
        validate_namespace_entry_object(&self.paths, &namespace_path, &current)?;
        if current.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {}/{}",
                table_bucket,
                namespace.public_name()
            )));
        }
        self.backend
            .delete_object_unlocked(self.catalog_bucket(), &namespace_path)
            .await
    }

    async fn create_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        self.write_table_entry(entry, TableCatalogPutPrecondition::IfAbsent).await
    }

    async fn register_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        self.write_table_entry(entry, TableCatalogPutPrecondition::IfAbsent).await
    }

    async fn register_table_with_publication(
        &self,
        entry: TableEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        self.write_table_entry_with_publication(entry, TableCatalogPutPrecondition::IfAbsent, publication)
            .await
    }

    async fn list_tables(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let read_version = self.table_rename_read_version(table_bucket).await?;
            let namespace = parse_namespace_for_store(namespace)?;
            let mut entries = Vec::new();
            for object in self
                .backend
                .list_objects(self.catalog_bucket(), &self.paths.table_entries_prefix(table_bucket, &namespace))
                .await?
            {
                if !object.ends_with(TABLE_ENTRY_FILE) {
                    continue;
                }
                let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &object).await? else {
                    continue;
                };
                validate_table_entry_object(&self.paths, &object, &entry)?;
                if entry.state == TableCatalogEntryState::Active {
                    entries.push(entry);
                }
            }
            entries.sort_by(|left, right| left.table.cmp(&right.table));
            self.finish_table_rename_read(table_bucket, read_version.as_deref()).await?;
            Ok(entries)
        })
        .await
    }

    async fn list_all_tables(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let read_version = self.table_rename_read_version(table_bucket).await?;
            let entries = self.list_all_table_entries(table_bucket).await.map(|entries| {
                entries
                    .into_iter()
                    .filter(|entry| entry.state == TableCatalogEntryState::Active)
                    .collect()
            })?;
            self.finish_table_rename_read(table_bucket, read_version.as_deref()).await?;
            Ok(entries)
        })
        .await
    }

    async fn ensure_table_warehouse_location_available(&self, candidate: &TableEntry) -> TableCatalogStoreResult<()> {
        self.with_catalog_restore_read(&candidate.table_bucket, async {
            self.ensure_table_warehouse_prefix_available(candidate, None).await
        })
        .await
    }

    async fn list_tables_page(
        &self,
        table_bucket: &str,
        namespace: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<TableEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let read_version = self.table_rename_read_version(table_bucket).await?;
            let namespace = parse_namespace_for_store(namespace)?;
            let page = self
                .list_entry_page(
                    &self.paths.table_entries_prefix(table_bucket, &namespace),
                    TABLE_ENTRY_FILE,
                    cursor,
                    limit,
                    |entry: &TableEntry| entry.state == TableCatalogEntryState::Active,
                    |object, entry: &TableEntry| validate_table_entry_object(&self.paths, object, entry).map(|_| ()),
                )
                .await?;
            self.finish_table_rename_read(table_bucket, read_version.as_deref()).await?;
            Ok(page)
        })
        .await
    }

    async fn load_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<Option<TableEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let read_version = self.table_rename_read_version(table_bucket).await?;
            let entry = self
                .load_table_entry(table_bucket, namespace, table)
                .await
                .map(|entry| entry.filter(|table| table.state == TableCatalogEntryState::Active))?;
            self.finish_table_rename_read(table_bucket, read_version.as_deref()).await?;
            Ok(entry)
        })
        .await
    }

    async fn rename_table(
        &self,
        table_bucket: &str,
        source_namespace: &str,
        source_table: &str,
        destination_namespace: &str,
        destination_table: &str,
    ) -> TableCatalogStoreResult<()> {
        let source_namespace = parse_namespace_for_store(source_namespace)?;
        let source_table = parse_table_for_store(source_table)?;
        let destination_namespace = parse_namespace_for_store(destination_namespace)?;
        let destination_table = parse_table_for_store(destination_table)?;
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        publication.begin_table_bucket(table_bucket).await?;
        if !publication.holds_table_bucket(table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "table rename requires a table-bucket publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(&publication);
        let interrupted_request = if let Some((bucket, _)) = self
            .read_entry::<TableBucketEntry>(self.catalog_bucket(), &self.paths.table_bucket_entry_path(table_bucket))
            .await?
            && let Some(rename_id) = bucket.active_rename_id.as_deref()
        {
            self.read_entry::<TableRenameIntent>(
                self.catalog_bucket(),
                &self.paths.table_rename_intent_path(table_bucket, rename_id),
            )
            .await?
            .map(|(intent, _)| {
                intent.source.namespace == source_namespace.public_name()
                    && intent.source.table == source_table.as_str()
                    && intent.destination.namespace == destination_namespace.public_name()
                    && intent.destination.table == destination_table.as_str()
            })
            .unwrap_or(false)
        } else {
            false
        };
        let mutation_fence = publication.object_mutation_fence().unwrap_or_default();
        self.recover_active_table_rename(table_bucket, &publication).await?;
        if interrupted_request {
            return Ok(());
        }

        {
            let bucket_object = self.paths.table_bucket_entry_path(table_bucket);
            let _bucket_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &bucket_object).await?;
            let Some((mut bucket_entry, bucket_etag)) = self.read_table_bucket_with_etag_unlocked(table_bucket).await? else {
                return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
            };
            if bucket_entry.state != TableCatalogEntryState::Active {
                return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
            }
            if bucket_entry.active_rename_id.is_some() {
                return Err(TableCatalogStoreError::Unavailable(format!(
                    "table bucket {table_bucket} has an active table rename"
                )));
            }
            if self
                .backend
                .object_exists(self.catalog_bucket(), &maintenance_rename_intent_path(table_bucket))
                .await?
            {
                return Err(maintenance_rename_recovery_error(TableCatalogStoreError::Unavailable(
                    "maintenance rename intent has no owning bucket fence".to_string(),
                )));
            }

            let rename_id = Uuid::new_v4().to_string();
            let intent_object = self.paths.table_rename_intent_path(table_bucket, &rename_id);
            let _intent_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &intent_object).await?;
            let source_object = self.paths.table_entry_path(table_bucket, &source_namespace, &source_table);
            let destination_object = self
                .paths
                .table_entry_path(table_bucket, &destination_namespace, &destination_table);
            let _catalog_guards = self
                .acquire_catalog_write_locks(vec![
                    self.paths.namespace_entry_path(table_bucket, &source_namespace),
                    self.paths.namespace_entry_path(table_bucket, &destination_namespace),
                    source_object.clone(),
                    destination_object.clone(),
                    self.paths
                        .view_entry_path(table_bucket, &destination_namespace, &destination_table),
                ])
                .await?;
            self.require_active_namespace_unlocked(
                table_bucket,
                &source_namespace,
                &self.paths.namespace_entry_path(table_bucket, &source_namespace),
            )
            .await
            .map_err(|err| match err {
                TableCatalogStoreError::NotFound(_) => TableCatalogStoreError::TableNotFound(format!(
                    "{table_bucket}/{}/{}",
                    source_namespace.public_name(),
                    source_table.as_str()
                )),
                err => err,
            })?;
            self.require_active_namespace_unlocked(
                table_bucket,
                &destination_namespace,
                &self.paths.namespace_entry_path(table_bucket, &destination_namespace),
            )
            .await
            .map_err(|err| match err {
                TableCatalogStoreError::NotFound(_) => {
                    TableCatalogStoreError::NamespaceNotFound(format!("{table_bucket}/{}", destination_namespace.public_name()))
                }
                err => err,
            })?;
            let Some((source, source_etag)) = self
                .read_table_with_etag_unlocked(table_bucket, &source_namespace, &source_table)
                .await?
            else {
                return Err(TableCatalogStoreError::TableNotFound(format!(
                    "{table_bucket}/{}/{}",
                    source_namespace.public_name(),
                    source_table.as_str()
                )));
            };
            if source.state != TableCatalogEntryState::Active {
                return Err(TableCatalogStoreError::TableNotFound(format!(
                    "{table_bucket}/{}/{}",
                    source_namespace.public_name(),
                    source_table.as_str()
                )));
            }
            if !is_valid_table_metadata_location_for_entry(&source, &source.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(
                    "current metadata location must be inside the table metadata directory".to_string(),
                ));
            }
            let destination_etag = match self
                .read_table_with_etag_unlocked(table_bucket, &destination_namespace, &destination_table)
                .await?
            {
                None => None,
                Some((current, etag)) if current.state == TableCatalogEntryState::Deleted => Some(etag),
                Some(_) => {
                    return Err(TableCatalogStoreError::AlreadyExists(format!(
                        "destination table already exists: {table_bucket}/{}/{}",
                        destination_namespace.public_name(),
                        destination_table.as_str()
                    )));
                }
            };
            if self
                .read_entry_unlocked::<ViewEntry>(
                    self.catalog_bucket(),
                    &self
                        .paths
                        .view_entry_path(table_bucket, &destination_namespace, &destination_table),
                )
                .await?
                .is_some()
            {
                return Err(TableCatalogStoreError::AlreadyExists(format!(
                    "destination table already exists: {table_bucket}/{}/{}",
                    destination_namespace.public_name(),
                    destination_table.as_str()
                )));
            }

            let now = next_table_catalog_update_time(bucket_entry.updated_at.as_deref());
            let mut destination = source.clone();
            destination.namespace = destination_namespace.public_name();
            destination.table = destination_table.as_str().to_string();
            destination.updated_at = Some(now.clone());
            let source_index = table_warehouse_index_entry(&source)?;
            let index_object = self
                .paths
                .warehouse_index_entry_path(table_bucket, &source_index.warehouse_object_prefix);
            let _index_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &index_object).await?;
            let warehouse_index_etag = match self
                .read_entry_unlocked::<TableWarehouseIndexEntry>(self.catalog_bucket(), &index_object)
                .await?
            {
                Some((current, Some(etag))) if current == source_index => etag,
                Some((current, None)) if current == source_index => {
                    return Err(TableCatalogStoreError::Internal(format!(
                        "catalog warehouse index entry has no etag: {index_object}"
                    )));
                }
                Some(_) => {
                    return Err(TableCatalogStoreError::Conflict(
                        "table warehouse index does not match the rename source".to_string(),
                    ));
                }
                None => {
                    self.write_exact_entry_unlocked_with_fence(
                        self.catalog_bucket(),
                        &index_object,
                        &source_index,
                        TableCatalogPutPrecondition::IfAbsent,
                        _index_guard.write_commit_guards(),
                        Some(&mutation_fence),
                    )
                    .await?
                }
            };
            let intent = TableRenameIntent {
                version: TABLE_RENAME_INTENT_VERSION,
                rename_id: rename_id.clone(),
                table_bucket: table_bucket.to_string(),
                source,
                destination,
                source_etag,
                destination_etag,
                warehouse_index_etag,
                state: TableRenameIntentState::Prepared,
                created_at: now.clone(),
                updated_at: now.clone(),
            };
            validate_table_rename_intent_object(&self.paths, &intent_object, &intent)?;

            // An orphan intent has no catalog effect; a published bucket fence without its intent cannot be recovered safely.
            self.write_exact_entry_unlocked_with_fence(
                self.catalog_bucket(),
                &intent_object,
                &intent,
                TableCatalogPutPrecondition::IfAbsent,
                _intent_guard.write_commit_guards(),
                Some(&mutation_fence),
            )
            .await?;
            bucket_entry.active_rename_id = Some(rename_id);
            bucket_entry.updated_at = Some(now);
            self.write_exact_entry_unlocked_with_fence(
                self.catalog_bucket(),
                &bucket_object,
                &bucket_entry,
                TableCatalogPutPrecondition::IfMatch(bucket_etag),
                _bucket_guard.write_commit_guards(),
                Some(&mutation_fence),
            )
            .await?;
        }

        self.recover_active_table_rename(table_bucket, &publication).await
    }

    async fn resolve_table_data_plane_resource(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        if table_bucket.is_empty() || object.is_empty() {
            return Ok(None);
        }
        self.with_catalog_restore_read(table_bucket, async {
            let read_version = self.table_data_plane_read_version(table_bucket).await?;

            let resource = if self.warehouse_index_ready(table_bucket).await? {
                self.resolve_table_data_plane_resource_with_scan(table_bucket, object, true)
                    .await
            } else {
                match self.backfill_table_warehouse_index_for_data_plane(table_bucket).await {
                    Ok(()) => {
                        self.resolve_table_data_plane_resource_with_scan(table_bucket, object, true)
                            .await
                    }
                    Err(err @ TableCatalogStoreError::Internal(_)) => {
                        tracing::warn!(
                            table_bucket = %table_bucket,
                            error = %err,
                            "failed to backfill table warehouse index; falling back to catalog scan"
                        );
                        self.resolve_table_data_plane_resource_with_scan(table_bucket, object, true)
                            .await
                    }
                    Err(err) => Err(err),
                }
            }?;
            self.finish_table_rename_read(table_bucket, Some(&read_version)).await?;
            Ok(resource)
        })
        .await
    }

    async fn resolve_table_metadata_data_plane_resource(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        if table_bucket.is_empty() || table_identity_from_metadata_object_key(object).is_none() {
            return Ok(None);
        }
        self.with_catalog_restore_read(table_bucket, async {
            let read_version = self.table_data_plane_read_version(table_bucket).await?;
            let entries = self.list_all_table_entries(table_bucket).await?;
            let resource = table_metadata_data_plane_resource_from_entries(&entries, table_bucket, object)?;
            self.finish_table_rename_read(table_bucket, Some(&read_version)).await?;
            Ok(resource)
        })
        .await
    }

    #[cfg(test)]
    async fn commit_prepared(&self, prepared: PreparedCatalogCommit) -> TableCatalogStoreResult<TableCommitResult> {
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        self.commit_prepared_with_publication(prepared, &publication).await
    }

    #[cfg(test)]
    async fn commit_prepared_with_publication(
        &self,
        prepared: PreparedCatalogCommit,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        self.commit_catalog_with_preparation(CatalogCommitInput::Prepared(Box::new(prepared)), publication)
            .await
    }

    async fn drop_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()> {
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        publication.begin_table_bucket(table_bucket).await?;
        self.recover_active_table_rename(table_bucket, &publication).await?;
        let namespace_path = self.paths.namespace_entry_path(table_bucket, &namespace);
        let _namespace_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &namespace_path)
            .await?;
        let object = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?;
        publication
            .prepare(table_bucket, &namespace.public_name(), table.as_str())
            .await?;
        if !publication.holds_table_bucket(table_bucket)
            || !publication.holds_table(table_bucket, &namespace.public_name(), table.as_str())
        {
            return Err(TableCatalogStoreError::Internal(
                "table drop requires table-bucket and table publication fences".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(&publication);
        let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        let mutation_fence = publication.object_mutation_fence().unwrap_or_default();
        // A rename source tombstone can still share its table ID with a live destination.
        let history = if entry.state == TableCatalogEntryState::Active {
            self.prepare_dropped_table_commit_cleanup(&entry).await?
        } else {
            Vec::new()
        };
        mutation_fence.ensure_held()?;
        self.tombstone_table_warehouse_index_for_drop(&entry, false).await?;
        if !publication.holds_table_bucket(table_bucket)
            || !publication.holds_table(table_bucket, &namespace.public_name(), table.as_str())
        {
            self.restore_table_warehouse_index_after_failed_drop(&entry, &object, "table publication fence lost")
                .await;
            return Err(TableCatalogStoreError::Internal(
                "table drop publication fence was lost before catalog update".to_string(),
            ));
        }
        if let Err(err) = self
            .backend
            .delete_object_unlocked_fenced(self.catalog_bucket(), &object, _guard.write_commit_guards(), &mutation_fence)
            .await
        {
            match self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await {
                Ok(None) => {}
                Ok(Some((current, _))) if current == entry => {
                    self.restore_table_warehouse_index_after_failed_drop(&entry, &object, "table entry delete failed")
                        .await;
                    return Err(err);
                }
                Ok(Some(_)) => {
                    return Err(TableCatalogStoreError::Internal(format!(
                        "table {table_bucket}/{}/{} changed identity while its drop result was ambiguous",
                        namespace.public_name(),
                        table.as_str()
                    )));
                }
                Err(read_err) => {
                    tracing::warn!(
                        table_bucket,
                        namespace = %namespace.public_name(),
                        table = %table.as_str(),
                        error = %read_err,
                        "failed to verify table state after an ambiguous table entry delete"
                    );
                    return Err(err);
                }
            }
        }
        let config_path = self
            .paths
            .table_maintenance_config_path(table_bucket, &namespace, &table, &entry.table_id);
        if let Some(config) = self.backend.object_metadata(self.catalog_bucket(), &config_path).await? {
            let etag = config
                .etag
                .ok_or_else(|| TableCatalogStoreError::Conflict("table maintenance config has no ETag".to_string()))?;
            self.backend
                .delete_object_if_match_fenced(self.catalog_bucket(), &config_path, &etag, &mutation_fence)
                .await?;
        }
        for (key, etag) in history {
            self.backend
                .delete_object_if_match_fenced(self.catalog_bucket(), &key, &etag, &mutation_fence)
                .await?;
        }
        Ok(())
    }

    async fn create_view(&self, entry: ViewEntry) -> TableCatalogStoreResult<()> {
        self.write_view_entry(entry, TableCatalogPutPrecondition::IfAbsent).await
    }

    async fn create_view_with_publication(
        &self,
        entry: ViewEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        self.write_view_entry_with_publication(entry, TableCatalogPutPrecondition::IfAbsent, publication)
            .await
    }

    async fn list_views(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<ViewEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            let mut entries = Vec::new();
            for object in self
                .backend
                .list_objects(self.catalog_bucket(), &self.paths.view_entries_prefix(table_bucket, &namespace))
                .await?
            {
                if !object.ends_with(VIEW_ENTRY_FILE) {
                    continue;
                }
                let Some((entry, _)) = self.read_entry::<ViewEntry>(self.catalog_bucket(), &object).await? else {
                    continue;
                };
                validate_view_entry_object(&self.paths, &object, &entry)?;
                if entry.state == TableCatalogEntryState::Active {
                    entries.push(entry);
                }
            }
            entries.sort_by(|left, right| left.view.cmp(&right.view));
            Ok(entries)
        })
        .await
    }

    async fn list_views_page(
        &self,
        table_bucket: &str,
        namespace: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<ViewEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            self.list_entry_page(
                &self.paths.view_entries_prefix(table_bucket, &namespace),
                VIEW_ENTRY_FILE,
                cursor,
                limit,
                |entry: &ViewEntry| entry.state == TableCatalogEntryState::Active,
                |object, entry: &ViewEntry| validate_view_entry_object(&self.paths, object, entry).map(|_| ()),
            )
            .await
        })
        .await
    }

    async fn load_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<Option<ViewEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            let view = parse_table_for_store(view)?;
            let object = self.paths.view_entry_path(table_bucket, &namespace, &view);
            let Some((entry, _)) = self.read_entry::<ViewEntry>(self.catalog_bucket(), &object).await? else {
                return Ok(None);
            };
            validate_view_entry_object(&self.paths, &object, &entry)?;
            Ok((entry.state == TableCatalogEntryState::Active).then_some(entry))
        })
        .await
    }

    async fn drop_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<()> {
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        let namespace = parse_namespace_for_store(namespace)?;
        let view = parse_table_for_store(view)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        publication.begin_table_bucket(table_bucket).await?;
        let _publication_completion = TableCommitPublicationCompletion::new(&publication);
        self.recover_active_table_rename(table_bucket, &publication).await?;
        let namespace_path = self.paths.namespace_entry_path(table_bucket, &namespace);
        let _namespace_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &namespace_path)
            .await?;
        let object = self.paths.view_entry_path(table_bucket, &namespace, &view);
        let _view_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?;
        let Some((entry, _)) = self.read_entry_unlocked::<ViewEntry>(self.catalog_bucket(), &object).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "view {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                view.as_str()
            )));
        };
        validate_view_entry_object(&self.paths, &object, &entry)?;
        self.backend.delete_object_unlocked(self.catalog_bucket(), &object).await
    }

    async fn get_commit_by_id(
        &self,
        table_bucket: &str,
        table_id: &str,
        commit_id: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            self.read_commit_log_entry(table_bucket, table_id, commit_id).await
        })
        .await
    }

    async fn get_commit_by_idempotency_key(
        &self,
        table_bucket: &str,
        table_id: &str,
        idempotency_key: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        self.with_catalog_restore_read(table_bucket, async {
            self.read_commit_idempotency_entry(table_bucket, table_id, idempotency_key)
                .await
        })
        .await
    }
}

#[cfg(test)]
#[test]
fn catalog_lock_context_preserves_original_network_cause() {
    let error = super::catalog_lock_acquisition_error(
        "acquire catalog migration lock",
        rustfs_lock::LockError::network("peer unavailable", std::io::Error::from(std::io::ErrorKind::TimedOut)),
    );
    let error = maintenance_rename_recovery_error(error);
    assert!(error.is_unavailable());
    assert!(error.to_string().contains("maintenance rename recovery required"));
    let source = std::error::Error::source(&error).expect("retain the typed lock cause");
    assert!(source.is::<rustfs_lock::LockError>());
    let io = source
        .source()
        .and_then(|cause| cause.downcast_ref::<std::io::Error>())
        .expect("retain the original network cause without cloning it");
    assert_eq!(io.kind(), std::io::ErrorKind::TimedOut);
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    #[cfg(test)]
    pub(crate) async fn read_catalog_head(
        &self,
        bucket: &BucketHandle,
        namespace: &Namespace,
        name: &CatalogName,
    ) -> TableCatalogStoreResult<CatalogHead> {
        if self.paths.scope != CatalogScope::Vector {
            return Err(TableCatalogStoreError::Unsupported(
                "Table v1 has no persisted content-bound head".to_string(),
            ));
        }
        if self.vector_bucket_for(bucket.storage_bucket_id().as_str())? != bucket || namespace.public_name() != "indexes" {
            return Err(TableCatalogStoreError::Conflict(
                "catalog head request does not match Vector identity".to_string(),
            ));
        }
        self.with_catalog_restore_read(bucket.storage_bucket_id().as_str(), async {
            let (entry, _) = self
                .read_table_with_etag(bucket.storage_bucket_id().as_str(), namespace, name)
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound("Vector catalog head".to_string()))?;
            if entry.state != TableCatalogEntryState::Active {
                return Err(TableCatalogStoreError::NotFound("active Vector catalog head".to_string()));
            }
            let head = CatalogHead::from_vector_entry(&entry)?;
            if head.resource_identity().vector_bucket() != Some(bucket) {
                return Err(TableCatalogStoreError::Conflict(
                    "Vector head belongs to another bucket incarnation".to_string(),
                ));
            }
            Ok(head)
        })
        .await
    }

    #[cfg(test)]
    pub(crate) async fn read_committed_history(&self, head: &CatalogHead) -> TableCatalogStoreResult<CatalogCommittedHistory> {
        if self.paths.scope != CatalogScope::Vector {
            return Err(TableCatalogStoreError::Unsupported(
                "Table v1 has no persisted content-bound history".to_string(),
            ));
        }
        let bucket = head
            .resource_identity()
            .vector_bucket()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Vector history requires a Vector identity".to_string()))?;
        if self.vector_bucket_for(bucket.storage_bucket_id().as_str())? != bucket {
            return Err(TableCatalogStoreError::Conflict("Vector history belongs to another bucket".to_string()));
        }
        self.with_catalog_restore_read(bucket.storage_bucket_id().as_str(), async {
            let (entry, _) = self
                .read_table_with_etag(bucket.storage_bucket_id().as_str(), head.namespace(), head.name())
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound("Vector catalog history head".to_string()))?;
            let current_head = CatalogHead::from_vector_entry(&entry)?;
            if current_head.resource_identity() != head.resource_identity()
                || current_head.expected_head() != head.expected_head()
                || entry.state != TableCatalogEntryState::Active
            {
                return Err(TableCatalogStoreError::Conflict("Vector head changed before history read".to_string()));
            }
            let records = self
                .read_table_commit_logs(&entry)
                .await?
                .into_iter()
                .map(|(_, log)| log)
                .collect::<Vec<_>>();
            let versions = vector_committed_history_versions(&entry, &records)?;
            let (after, _) = self
                .read_table_with_etag(bucket.storage_bucket_id().as_str(), head.namespace(), head.name())
                .await?
                .ok_or_else(|| TableCatalogStoreError::Conflict("Vector head disappeared during history read".to_string()))?;
            if after.state != TableCatalogEntryState::Active {
                return Err(TableCatalogStoreError::Conflict(
                    "Vector head became inactive during history read".to_string(),
                ));
            }
            let after = CatalogHead::from_vector_entry(&after)?;
            if after.resource_identity() != head.resource_identity() || after.expected_head() != head.expected_head() {
                return Err(TableCatalogStoreError::Conflict("Vector head changed during history read".to_string()));
            }
            Ok(CatalogCommittedHistory::new(versions))
        })
        .await
    }

    pub(in crate::table_catalog) fn require_table_scope(&self) -> TableCatalogStoreResult<()> {
        if self.paths.scope != CatalogScope::Table {
            return Err(TableCatalogStoreError::Unsupported("operation requires Table catalog scope".to_string()));
        }
        Ok(())
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(in crate::table_catalog) async fn commit_catalog_with_preparation(
        &self,
        input: CatalogCommitInput<'_>,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        self.commit_catalog_with_operation_guard(input, publication, None).await
    }

    async fn commit_catalog_with_operation_guard(
        &self,
        input: CatalogCommitInput<'_>,
        publication: &(dyn TableCommitPublication + Sync),
        operation_guard: Option<&VectorCatalogOperationGuard>,
    ) -> TableCatalogStoreResult<TableCommitResult> {
        // Keep the large publication state machine out of the enclosing deadline
        // and request-context futures, including callers that restore and commit.
        let operation = Box::pin(async {
            let request = input.fields();
            let supplied = input.prepared();
            if self.paths.scope == CatalogScope::Vector {
                let expected = self.vector_bucket_for(&request.table_bucket)?;
                if supplied.and_then(|prepared| prepared.resource_identity().vector_bucket()) != Some(expected) {
                    return Err(TableCatalogStoreError::Conflict(
                        "prepared bucket identity does not match Vector catalog".to_string(),
                    ));
                }
            } else if let Some(prepared) = supplied
                && (prepared.resource_identity().format() != CatalogFormat::Iceberg
                    || self.backend.read_table_bucket_incarnation(&request.table_bucket).await?
                        != prepared.resource_identity().incarnation())
            {
                return Err(TableCatalogStoreError::Conflict("prepared Table incarnation changed".to_string()));
            }
            let commit_started = Instant::now();
            record_table_commit_attempt(&request.operation);
            let namespace = parse_namespace_for_store(&request.namespace)?;
            let table = self.paths.parse_name(&request.table)?;
            if let Some(guard) = operation_guard {
                self.validate_vector_operation_guard(guard)?;
                if supplied.is_none_or(|prepared| prepared.resource_identity() != guard.resource_identity())
                    || guard.namespace() != &namespace
                    || guard.name() != &table
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "Vector publication differs from the operation guard".into(),
                    ));
                }
            }
            let mut shared_lifecycle = if let Some(guard) = operation_guard {
                Some(guard.inner.lifecycle.lock().await)
            } else {
                None
            };
            let mut _migration_guard = if self.paths.scope == CatalogScope::Vector {
                if operation_guard.is_some() {
                    None
                } else {
                    Some(
                        self.backend
                            .acquire_bucket_lifecycle_read(self.vector_bucket_for(&request.table_bucket)?)
                            .await?,
                    )
                }
            } else {
                self.begin_table_bucket_with_migration_permit(&request.table_bucket, publication)
                    .await?
            };
            let _gc_guard = if self.paths.scope == CatalogScope::Vector && operation_guard.is_none() {
                let prepared =
                    supplied.ok_or_else(|| TableCatalogStoreError::Invalid("Vector commit requires prepared metadata".into()))?;
                Some(
                    self.backend
                        .acquire_read_lock(
                            self.catalog_bucket(),
                            &self
                                .paths
                                .vector_gc_lock_path(&request.table_bucket, prepared.resource_identity().table_id().as_str()),
                        )
                        .await?,
                )
            } else {
                None
            };
            if self.paths.scope == CatalogScope::Vector {
                publication.begin_table_bucket(&request.table_bucket).await?;
                self.require_vector_parent_and_namespace(self.vector_bucket_for(&request.table_bucket)?, &namespace)
                    .await?;
                if let Some(guard) = operation_guard {
                    guard.check_held()?;
                }
            }
            if !publication.holds_table_bucket(&request.table_bucket) {
                return Err(TableCatalogStoreError::Internal(
                    "table commit requires a table-bucket publication fence".to_string(),
                ));
            }
            retain_publication_preparation_fence(publication);
            if publication.holds_table_bucket(&request.table_bucket) {
                self.recover_active_table_rename(&request.table_bucket, publication).await?;
            } else {
                self.ensure_no_active_table_rename(&request.table_bucket).await?;
            }
            let table_path = self.paths.table_entry_path(&request.table_bucket, &namespace, &table);
            let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            // Preserve catalog -> publication -> object lock order across rolling upgrades.
            publication
                .prepare(&request.table_bucket, &request.namespace, &request.table)
                .await?;
            if !publication.holds_table(&request.table_bucket, &request.namespace, &request.table) {
                return Err(TableCatalogStoreError::Internal(
                    "table commit requires a table publication fence".to_string(),
                ));
            }
            retain_publication_preparation_fence(publication);
            let _publication_completion = TableCommitPublicationCompletion::new(publication);

            let mut mutation_fence = publication.object_mutation_fence().unwrap_or_default();
            mutation_fence.extend_signals([
                _migration_guard.as_ref().and_then(TableCatalogLockGuard::lock_lost_signal),
                _gc_guard.as_ref().and_then(TableCatalogLockGuard::lock_lost_signal),
            ]);
            if let Some(guard) = operation_guard {
                guard.check_held()?;
                mutation_fence.signals.extend(guard.inner.fence.signals.iter().cloned());
            }
            mutation_fence.ensure_held()?;
            let vector_mutation_fence = (self.paths.scope == CatalogScope::Vector).then_some(&mutation_fence);

            let Some((current, current_etag)) = self
                .read_table_with_etag_unlocked(&request.table_bucket, &namespace, &table)
                .await?
            else {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::NotFound(format!(
                        "table {}/{}/{}",
                        request.table_bucket, request.namespace, request.table
                    ))),
                );
            };
            if current.state != TableCatalogEntryState::Active {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::NotFound(format!(
                        "table {}/{}/{}",
                        request.table_bucket, request.namespace, request.table
                    ))),
                );
            }

            let commit_path = self
                .paths
                .commit_log_entry_path(&request.table_bucket, &current.table_id, &request.commit_id);
            let existing_commit = self
                .read_commit_log_entry(&request.table_bucket, &current.table_id, &request.commit_id)
                .await?;
            let idempotency_key = request.idempotency_key.as_deref();
            let idempotency_path = idempotency_key.map(|idempotency_key| {
                self.paths
                    .commit_idempotency_entry_path(&request.table_bucket, &current.table_id, idempotency_key)
            });
            let existing_idempotency_commit = match idempotency_key {
                Some(idempotency_key) => {
                    self.read_commit_idempotency_entry(&request.table_bucket, &current.table_id, idempotency_key)
                        .await?
                }
                None => None,
            };

            if let (Some(existing), Some(indexed)) = (&existing_commit, &existing_idempotency_commit)
                && !commit_logs_share_recovery_payload(existing, indexed)
            {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::Conflict(
                        "commit record and idempotency index contain different payloads".to_string(),
                    )),
                );
            }
            if let Some(existing) = existing_idempotency_commit.as_ref()
                && !input.matches_log(existing, &current.table_id)
            {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::Conflict("idempotency key already exists".to_string())),
                );
            }
            if existing_commit.is_none() && existing_idempotency_commit.is_some() {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::Conflict(
                        "idempotency key exists without a recoverable commit record".to_string(),
                    )),
                );
            }

            if let Some(existing) = existing_commit.as_ref() {
                if !input.matches_log(existing, &current.table_id) {
                    return table_commit_result(
                        &request.table_bucket,
                        &request.namespace,
                        &request.table,
                        &request.commit_id,
                        &request.operation,
                        commit_started,
                        Err(TableCatalogStoreError::Conflict(format!(
                            "commit id already exists: {}",
                            request.commit_id
                        ))),
                    );
                }
                if matches!(existing.status, CommitLogStatus::Failed) {
                    return table_commit_result(
                        &request.table_bucket,
                        &request.namespace,
                        &request.table,
                        &request.commit_id,
                        &request.operation,
                        commit_started,
                        Err(TableCatalogStoreError::Conflict("failed commit record cannot be replayed".to_string())),
                    );
                }
                if matches!(existing.status, CommitLogStatus::Committed) && table_matches_staged_base(&current, existing) {
                    return table_commit_result(
                        &request.table_bucket,
                        &request.namespace,
                        &request.table,
                        &request.commit_id,
                        &request.operation,
                        commit_started,
                        Err(TableCatalogStoreError::Conflict(
                            "committed record still matches the pre-commit table state".to_string(),
                        )),
                    );
                }
                let vector_replay = self.paths.scope == CatalogScope::Vector
                    && (matches!(existing.status, CommitLogStatus::Committed) || !table_matches_staged_base(&current, existing));
                let historically_committed = if vector_replay
                    || (matches!(existing.status, CommitLogStatus::Staged)
                        && !table_matches_staged_base(&current, existing)
                        && !table_matches_committed_log(&current, existing))
                {
                    let commit_logs = self
                        .read_table_commit_logs(&current)
                        .await?
                        .into_iter()
                        .map(|(_, commit_log)| commit_log)
                        .collect::<Vec<_>>();
                    if vector_replay {
                        let versions = vector_committed_history_versions(&current, &commit_logs)?;
                        let committed = versions.iter().any(|version| {
                            version
                                .commit_id()
                                .is_some_and(|commit_id| commit_id.as_str() == existing.commit_id)
                        });
                        if matches!(existing.status, CommitLogStatus::Committed) && !committed {
                            return Err(TableCatalogStoreError::Invalid(
                                "Vector committed record is detached from the current history".to_string(),
                            ));
                        }
                        committed
                    } else {
                        TableCommitHistoryIndex::new(&current, commit_logs.iter()).proves_committed(existing)
                    }
                } else {
                    false
                };
                if matches!(existing.status, CommitLogStatus::Committed)
                    || (matches!(existing.status, CommitLogStatus::Staged)
                        && (historically_committed
                            || (self.paths.scope == CatalogScope::Table && table_matches_committed_log(&current, existing))))
                {
                    let mut committed = existing.clone();
                    committed.status = CommitLogStatus::Committed;
                    let _ = self
                        .finalize_commit_log_with_fence(
                            &commit_path,
                            idempotency_path.as_deref(),
                            &committed,
                            vector_mutation_fence,
                        )
                        .await;
                    return table_commit_result(
                        &request.table_bucket,
                        &request.namespace,
                        &request.table,
                        &request.commit_id,
                        &request.operation,
                        commit_started,
                        Ok(TableCommitResult {
                            table: current,
                            commit_log: committed,
                        }),
                    );
                }
                if !matches!(existing.status, CommitLogStatus::Staged) || !table_matches_staged_base(&current, existing) {
                    return table_commit_result(
                        &request.table_bucket,
                        &request.namespace,
                        &request.table,
                        &request.commit_id,
                        &request.operation,
                        commit_started,
                        Err(TableCatalogStoreError::Conflict(
                            "existing commit record does not match current table state".to_string(),
                        )),
                    );
                }
            }

            if current.version_token != request.expected_version_token {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::Conflict(
                        "current table version token does not match expected token".to_string(),
                    )),
                );
            }
            if current.metadata_location != request.expected_metadata_location {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::Conflict(
                        "current table metadata location does not match expected location".to_string(),
                    )),
                );
            }
            let reader = if self.paths.scope == CatalogScope::Table {
                Some(publication.prepared_object_reader()?)
            } else {
                None
            };
            let prepared = input.prepare(&current, reader).await?;
            if self.paths.scope == CatalogScope::Table
                && (prepared.resource_identity().format() != CatalogFormat::Iceberg
                    || self.backend.read_table_bucket_incarnation(&request.table_bucket).await?
                        != prepared.resource_identity().incarnation())
            {
                return Err(TableCatalogStoreError::Conflict("prepared Table incarnation changed".to_string()));
            }
            if current.table_id != prepared.resource_identity().table_id().as_str()
                || current.generation != prepared.expected_head().generation().get()
            {
                return Err(TableCatalogStoreError::Conflict(
                    "prepared resource identity or generation changed".to_string(),
                ));
            }
            if self.paths.scope == CatalogScope::Vector {
                match (
                    current.version,
                    current.vector_index_definition.as_ref(),
                    prepared.vector_resource_mutation(),
                ) {
                    (2, None, None) => {}
                    (3, Some(definition), Some(mutation))
                        if definition == &mutation.expected_definition && current.state == mutation.expected_state => {}
                    _ => return Err(TableCatalogStoreError::Conflict("prepared Vector resource definition changed".into())),
                }
                let head = CatalogHead::from_vector_entry(&current)?;
                if head.expected_head() != *prepared.expected_head() {
                    return Err(TableCatalogStoreError::Conflict("prepared Vector head binding changed".to_string()));
                }
            }
            let candidate = prepared.candidate_metadata();
            let limit = usize::try_from(candidate.content_length())
                .ok()
                .and_then(|size| size.checked_add(1))
                .ok_or_else(|| TableCatalogStoreError::Invalid("candidate length exceeds address space".to_string()))?;
            let candidate_object = if self.paths.scope == CatalogScope::Vector {
                self.backend
                    .read_managed_candidate(
                        self.vector_bucket_for(&request.table_bucket)?,
                        candidate,
                        limit,
                        shared_lifecycle.as_deref_mut().or(_migration_guard.as_mut()).ok_or_else(|| {
                            TableCatalogStoreError::Internal("Vector commit has no lifecycle guard".to_string())
                        })?,
                    )
                    .await?
            } else {
                reader
                    .ok_or_else(|| TableCatalogStoreError::Internal("Table commit has no prepared object reader".into()))?
                    .read_object_limited(&request.table_bucket, candidate.object_key().as_str(), limit)
                    .await?
            }
            .ok_or_else(|| TableCatalogStoreError::NotFound("prepared candidate metadata is missing".to_string()))?;
            candidate.verify_read(&candidate_object)?;
            let next_metadata_state = prepared.entry_updates();
            let warehouse_relocation = next_metadata_state
                .warehouse_location()
                .is_some_and(|warehouse_location| warehouse_location != current.warehouse_location);
            if warehouse_relocation && !publication.holds_table_bucket(&request.table_bucket) {
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::Internal(
                        "table warehouse relocation requires a table-bucket publication fence".to_string(),
                    )),
                );
            }

            let compatibility_requirements = prepared.compatibility_json_array()?.into_values();
            let vector_commit_binding = prepared
                .resource_identity()
                .vector_bucket()
                .map(|_| VectorCommitBindingV1::from_prepared(&prepared))
                .transpose()?;
            let has_existing_commit = existing_commit.is_some();
            let mut staged_commit_log = existing_commit.unwrap_or_else(|| CommitLogEntry {
                vector_resource_mutation: prepared.vector_resource_mutation().cloned(),
                vector_commit_binding,
                version: if self.paths.scope == CatalogScope::Vector {
                    if prepared.vector_resource_mutation().is_some() { 3 } else { 2 }
                } else {
                    TABLE_CATALOG_ENTRY_VERSION
                },
                commit_id: request.commit_id.clone(),
                idempotency_key: request.idempotency_key.clone(),
                table_id: current.table_id.clone(),
                operation: request.operation.clone(),
                expected_version_token: request.expected_version_token.clone(),
                new_version_token: if prepared
                    .vector_resource_mutation()
                    .is_some_and(|mutation| mutation.kind == VectorResourceMutationKind::Delete)
                {
                    format!("delete-{}", prepared.commit_id().as_str())
                } else {
                    format!("token-{}", Uuid::new_v4())
                },
                previous_metadata_location: current.metadata_location.clone(),
                new_metadata_location: request.new_metadata_location.clone(),
                requirements: compatibility_requirements,
                status: CommitLogStatus::Staged,
                writer: request.writer.clone(),
                created_at: None,
                updated_at: None,
            });
            staged_commit_log.status = CommitLogStatus::Staged;

            let mut next = current.clone();
            next.metadata_location = staged_commit_log.new_metadata_location.clone();
            if let Some(warehouse_location) = next_metadata_state.warehouse_location() {
                next.warehouse_location = warehouse_location.to_string();
            }
            if let Some(format_version) = next_metadata_state.format_version() {
                next.format_version = format_version;
            }
            next.version_token = staged_commit_log.new_version_token.clone();
            next.generation = prepared.expected_head().generation().checked_next()?.get();
            if let Some(mutation) = prepared.vector_resource_mutation() {
                next.vector_index_definition = Some(mutation.next_definition.clone());
                next.state = mutation.next_state.clone();
            }
            if self.paths.scope == CatalogScope::Vector {
                next.vector_head_binding = Some(VectorHeadBindingV1::try_new(
                    prepared.resource_identity(),
                    candidate,
                    prepared.format_descriptor(),
                )?);
            }
            if self.paths.scope == CatalogScope::Table && next.warehouse_location != current.warehouse_location {
                self.ensure_table_warehouse_prefix_available(&next, Some(&table_path)).await?;
            }
            let reservation = if self.paths.scope == CatalogScope::Table {
                self.reserve_table_warehouse_index(&next, false, &table_path).await?
            } else {
                WarehouseIndexReservation::AlreadyReserved
            };

            let staged_write_result = async {
                if !has_existing_commit {
                    self.write_commit_record_with_fence(
                        &commit_path,
                        &staged_commit_log,
                        TableCatalogPutPrecondition::IfAbsent,
                        vector_mutation_fence,
                    )
                    .await?;
                }
                if let Some(idempotency_path) = idempotency_path.as_deref()
                    && existing_idempotency_commit.is_none()
                {
                    self.write_commit_record_with_fence(
                        idempotency_path,
                        &staged_commit_log,
                        TableCatalogPutPrecondition::IfAbsent,
                        vector_mutation_fence,
                    )
                    .await?;
                }
                Ok(())
            }
            .await;
            if let Err(err) = staged_write_result {
                self.rollback_table_warehouse_index_reservation(&next, reservation, "commit staging failed")
                    .await;
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(err),
                );
            }

            if !publication.holds_table(&request.table_bucket, &request.namespace, &request.table)
                || (warehouse_relocation && !publication.holds_table_bucket(&request.table_bucket))
            {
                self.rollback_table_warehouse_index_reservation(&next, reservation, "table publication fence lost")
                    .await;
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(TableCatalogStoreError::Internal(
                        "table commit publication fence was lost before pointer update".to_string(),
                    )),
                );
            }

            let cas_started = Instant::now();
            mutation_fence.ensure_held()?;
            if self.paths.scope == CatalogScope::Vector {
                self.backend
                    .revalidate_bucket_handle(self.vector_bucket_for(&request.table_bucket)?)
                    .await?;
            } else if self.backend.read_table_bucket_incarnation(&request.table_bucket).await?
                != prepared.resource_identity().incarnation()
            {
                return Err(TableCatalogStoreError::Conflict(
                    "Table incarnation changed before publication".to_string(),
                ));
            }

            let cas_result = self
                .write_entry_unlocked_with_fence(
                    self.catalog_bucket(),
                    &table_path,
                    &next,
                    TableCatalogPutPrecondition::IfMatch(current_etag),
                    _guard.write_commit_guards(),
                    Some(&mutation_fence),
                )
                .await;
            record_table_commit_cas_result(&request.operation, cas_started, &cas_result);
            if let Err(err) = cas_result {
                self.rollback_table_warehouse_index_reservation(&next, reservation, "table pointer CAS failed")
                    .await;
                return table_commit_result(
                    &request.table_bucket,
                    &request.namespace,
                    &request.table,
                    &request.commit_id,
                    &request.operation,
                    commit_started,
                    Err(err),
                );
            }
            if self.paths.scope == CatalogScope::Table {
                self.delete_table_warehouse_index_if_changed(&current, &next).await;
            }

            let mut commit_log = staged_commit_log;
            commit_log.status = CommitLogStatus::Committed;
            // After the table CAS succeeds, the staged record is the durable recovery source.
            // A finalization failure must not turn an externally committed pointer into a failed commit response.
            let _ = self
                .finalize_commit_log_with_fence(&commit_path, idempotency_path.as_deref(), &commit_log, vector_mutation_fence)
                .await;

            table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Ok(TableCommitResult { table: next, commit_log }),
            )
        });
        publication_preparation(publication, operation).await
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    #[cfg(test)]
    async fn retain_vector_resource_guard(
        &self,
        resource: CatalogResourceIdentity,
        namespace: Namespace,
        name: CatalogName,
        lifecycle: TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<VectorCatalogOperationGuard> {
        if self.paths.scope != CatalogScope::Vector
            || namespace.public_name() != "indexes"
            || resource.vector_bucket() != self.vector_bucket.as_ref()
        {
            return Err(TableCatalogStoreError::Conflict(
                "Vector operation identity differs from the store".into(),
            ));
        }
        let gc = self
            .backend
            .acquire_read_lock(
                self.catalog_bucket(),
                &self
                    .paths
                    .vector_gc_lock_path(resource.storage_bucket_id(), resource.table_id().as_str()),
            )
            .await?;
        let fence = TableCatalogObjectMutationFence::from_signals([lifecycle.lock_lost_signal(), gc.lock_lost_signal()]);
        let native_lifecycle = lifecycle.lifecycle_guard.clone();
        let guard = VectorCatalogOperationGuard {
            inner: Arc::new(VectorCatalogOperationState {
                owner: self.vector_owner.clone(),
                resource_identity: resource,
                namespace,
                name,
                lifecycle: tokio::sync::Mutex::new(lifecycle),
                native_lifecycle,
                gc: parking_lot::Mutex::new(gc),
                fence,
            }),
        };
        guard.check_held()?;
        Ok(guard)
    }

    async fn require_vector_parent_and_namespace(
        &self,
        bucket: &BucketHandle,
        namespace: &Namespace,
    ) -> TableCatalogStoreResult<(TableBucketEntry, String)> {
        let bucket_path = self.paths.table_bucket_entry_path(bucket.storage_bucket_id().as_str());
        let (parent, etag) = self
            .read_entry::<TableBucketEntry>(self.catalog_bucket(), &bucket_path)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("Vector bucket Catalog entry".into()))?;
        validate_table_bucket_entry_object(&self.paths, &bucket_path, &parent)?;
        self.validate_store_bucket_entry(&parent)?;
        if parent.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::NotFound("active Vector bucket Catalog entry".into()));
        }
        let namespace_path = self
            .paths
            .namespace_entry_path(bucket.storage_bucket_id().as_str(), namespace);
        let (entry, _) = self
            .read_entry::<NamespaceEntry>(self.catalog_bucket(), &namespace_path)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("Vector Index namespace".into()))?;
        validate_namespace_entry_object(&self.paths, &namespace_path, &entry)?;
        if entry.state != TableCatalogEntryState::Active || entry.namespace != "indexes" {
            return Err(TableCatalogStoreError::NotFound("active Vector Index namespace".into()));
        }
        Ok((
            parent,
            etag.ok_or_else(|| TableCatalogStoreError::Invalid("Vector parent entry has no ETag".into()))?,
        ))
    }

    fn validate_vector_operation_guard(&self, guard: &VectorCatalogOperationGuard) -> TableCatalogStoreResult<()> {
        if self.paths.scope != CatalogScope::Vector
            || !Arc::ptr_eq(&self.vector_owner, &guard.inner.owner)
            || guard.resource_identity().vector_bucket() != self.vector_bucket.as_ref()
        {
            return Err(TableCatalogStoreError::Conflict(
                "Vector operation guard belongs to another store or resource".into(),
            ));
        }
        guard.check_held()
    }

    #[cfg(test)]
    pub(crate) async fn begin_vector_operation(
        &self,
        bucket: &BucketHandle,
        namespace: &Namespace,
        name: &CatalogName,
    ) -> TableCatalogStoreResult<(VectorCatalogOperationGuard, VectorCatalogEntry)> {
        if self.paths.scope != CatalogScope::Vector
            || self.vector_bucket_for(bucket.storage_bucket_id().as_str())? != bucket
            || namespace.public_name() != "indexes"
        {
            return Err(TableCatalogStoreError::Conflict(
                "Vector operation identity differs from the store".into(),
            ));
        }
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        self.require_vector_parent_and_namespace(bucket, namespace).await?;
        let (entry, _) = self
            .read_table_with_etag(bucket.storage_bucket_id().as_str(), namespace, name)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("Vector Index".into()))?;
        let discovered = VectorCatalogEntry::from_entry(entry)?;
        if discovered.entry.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::NotFound("active Vector Index".into()));
        }
        let resource = discovered.head().resource_identity().clone();
        let guard = self
            .retain_vector_resource_guard(resource, namespace.clone(), name.clone(), lifecycle)
            .await?;
        let current = self.read_vector_entry_with_guard(&guard).await?;
        Ok((guard, current))
    }

    #[cfg(test)]
    pub(crate) async fn read_vector_entry_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<VectorCatalogEntry> {
        self.validate_vector_operation_guard(guard)?;
        let bucket = self.vector_bucket_for(guard.resource_identity().storage_bucket_id())?;
        self.backend.revalidate_bucket_handle(bucket).await?;
        self.require_vector_parent_and_namespace(bucket, guard.namespace()).await?;
        let (entry, _) = self
            .read_table_with_etag(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name())
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("Vector Index".into()))?;
        let current = VectorCatalogEntry::from_entry(entry)?;
        if current.head().resource_identity() != guard.resource_identity() {
            return Err(TableCatalogStoreError::Conflict("Vector Index lifetime changed".into()));
        }
        if current.entry.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::NotFound("active Vector Index".into()));
        }
        self.backend.revalidate_bucket_handle(bucket).await?;
        guard.check_held()?;
        Ok(current)
    }

    #[cfg(test)]
    pub(crate) async fn read_catalog_head_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
    ) -> TableCatalogStoreResult<CatalogHead> {
        Ok(self.read_vector_entry_with_guard(guard).await?.head)
    }

    #[cfg(test)]
    pub(crate) async fn read_committed_history_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        head: &CatalogHead,
    ) -> TableCatalogStoreResult<CatalogCommittedHistory> {
        let current = self.read_vector_entry_with_guard(guard).await?;
        if current.head().resource_identity() != head.resource_identity()
            || current.head().expected_head() != head.expected_head()
            || head.namespace() != guard.namespace()
            || head.name() != guard.name()
        {
            return Err(TableCatalogStoreError::Conflict("Vector head changed before history read".into()));
        }
        let logs = self
            .read_table_commit_logs(&current.entry)
            .await?
            .into_iter()
            .map(|(_, log)| log)
            .collect::<Vec<_>>();
        let versions = vector_committed_history_versions(&current.entry, &logs)?;
        let after = self.read_vector_entry_with_guard(guard).await?;
        if current.entry != after.entry {
            return Err(TableCatalogStoreError::Conflict("Vector entry changed during history read".into()));
        }
        Ok(CatalogCommittedHistory::new(versions))
    }

    #[cfg(test)]
    pub(crate) async fn commit_vector_prepared_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        prepared: PreparedCatalogCommit,
        expected_definition: &VectorIndexDefinitionV1,
    ) -> TableCatalogStoreResult<TableCommitResult> {
        self.validate_vector_operation_guard(guard)?;
        if prepared.resource_identity() != guard.resource_identity()
            || prepared.namespace() != guard.namespace()
            || prepared.name() != guard.name()
        {
            return Err(TableCatalogStoreError::Conflict(
                "prepared Vector commit differs from the operation guard".into(),
            ));
        }
        let prepared = prepared.with_vector_resource_mutation(VectorResourceMutationBindingV1 {
            version: 1,
            kind: VectorResourceMutationKind::Dataset,
            expected_definition: expected_definition.clone(),
            next_definition: expected_definition.clone(),
            expected_state: TableCatalogEntryState::Active,
            next_state: TableCatalogEntryState::Active,
        })?;
        let store = self.clone();
        let guard = guard.clone();
        run_vector_catalog_mutation(async move {
            let publication = TableCommitLockPublication::new(&store.backend, &store.paths);
            let result = store
                .commit_catalog_with_operation_guard(CatalogCommitInput::Prepared(Box::new(prepared)), &publication, Some(&guard))
                .await?;
            guard.check_held()?;
            Ok(result)
        })
        .await
    }
}

#[cfg(test)]
async fn run_vector_catalog_mutation<F, T>(operation: F) -> TableCatalogStoreResult<T>
where
    F: std::future::Future<Output = TableCatalogStoreResult<T>> + Send + 'static,
    T: Send + 'static,
{
    let credentials = rustfs_keystone::KEYSTONE_CREDENTIALS.try_with(Clone::clone).ok().flatten();
    let operation = rustfs_keystone::KEYSTONE_CREDENTIALS.scope(credentials, operation);
    let operation = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(
        rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context(),
        operation,
    );
    crate::storage_api::table::spawn_traced_join(operation)
        .await
        .map_err(|_| TableCatalogStoreError::Internal("Vector Catalog mutation worker failed".into()))?
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn write_commit_record_with_fence(
        &self,
        object: &str,
        record: &CommitLogEntry,
        precondition: TableCatalogPutPrecondition,
        fence: Option<&TableCatalogObjectMutationFence>,
    ) -> TableCatalogStoreResult<()> {
        match fence {
            Some(fence) => self.write_vector_record_fenced(object, record, precondition, fence).await,
            None => self.write_entry(self.catalog_bucket(), object, record, precondition).await,
        }
    }

    async fn write_vector_record_fenced<T: Serialize>(
        &self,
        object: &str,
        record: &T,
        precondition: TableCatalogPutPrecondition,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        if self.paths.scope != CatalogScope::Vector {
            return Err(TableCatalogStoreError::Unsupported(
                "Vector record write requires Vector Catalog scope".into(),
            ));
        }
        fence.ensure_held()?;
        let data = serde_json::to_vec(record)
            .map_err(|error| TableCatalogStoreError::Internal(format!("encode Vector Catalog record: {error}")))?;
        begin_publication_mutation()?;
        self.backend
            .put_object_fenced(self.catalog_bucket(), object, data, precondition, fence)
            .await?;
        fence.ensure_held()
    }

    async fn finalize_commit_log_with_fence(
        &self,
        commit_path: &str,
        idempotency_path: Option<&str>,
        committed: &CommitLogEntry,
        fence: Option<&TableCatalogObjectMutationFence>,
    ) -> TableCatalogStoreResult<()> {
        let Some(fence) = fence else {
            return self.finalize_commit_log(commit_path, idempotency_path, committed).await;
        };
        if self.paths.scope != CatalogScope::Vector || committed.status != CommitLogStatus::Committed {
            return Err(TableCatalogStoreError::Invalid("invalid fenced Vector commit finalization".into()));
        }
        for (path, require_existing) in std::iter::once((commit_path, true)).chain(idempotency_path.map(|path| (path, false))) {
            fence.ensure_held()?;
            let existing = self.read_entry::<CommitLogEntry>(self.catalog_bucket(), path).await?;
            let precondition = match existing {
                Some((current, etag)) => {
                    if !commit_logs_share_recovery_payload(&current, committed) || current.status == CommitLogStatus::Failed {
                        return Err(TableCatalogStoreError::Conflict("Vector commit receipt contains another payload".into()));
                    }
                    if current == *committed {
                        continue;
                    }
                    TableCatalogPutPrecondition::IfMatch(etag.ok_or_else(|| {
                        TableCatalogStoreError::Invalid("Vector commit receipt has no conditional-write ETag".into())
                    })?)
                }
                None if !require_existing => TableCatalogPutPrecondition::IfAbsent,
                None => {
                    return Err(TableCatalogStoreError::Invalid(
                        "Vector commit log disappeared before finalization".into(),
                    ));
                }
            };
            self.write_vector_record_fenced(path, committed, precondition, fence).await?;
        }
        fence.ensure_held()
    }
}
#[cfg(test)]
fn vector_reserved_name_matches(expected: Option<&VectorIndexCreateNameV1>, current: Option<&(TableEntry, String)>) -> bool {
    match (expected, current) {
        (None, None) => true,
        (Some(expected), Some((entry, etag))) => {
            expected.table_id == entry.table_id
                && expected.version_token == entry.version_token
                && expected.generation == entry.generation
                && expected.state == TableCatalogEntryState::Deleted
                && entry.state == TableCatalogEntryState::Deleted
                && expected.entry_etag == *etag
        }
        _ => false,
    }
}

#[cfg(test)]
fn validate_vector_create_intent_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    intent: &VectorIndexCreateIntentV1,
) -> TableCatalogStoreResult<()> {
    intent.validate()?;
    let namespace = Namespace::parse(&intent.namespace).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
    let name = CatalogName::parse(&intent.name).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
    if paths.scope != CatalogScope::Vector
        || paths.vector_create_intent_path(&intent.bucket.storage_bucket_id, &namespace, &name) != object
    {
        return Err(TableCatalogStoreError::Invalid(
            "Vector create intent differs from its canonical path".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn validate_vector_delete_intent_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    intent: &VectorIndexDeleteIntentV1,
) -> TableCatalogStoreResult<()> {
    intent.validate()?;
    if paths.scope != CatalogScope::Vector
        || paths.vector_delete_intent_path(&intent.bucket.storage_bucket_id, &intent.table_id) != object
    {
        return Err(TableCatalogStoreError::Invalid(
            "Vector delete intent differs from its resource path".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn initial_vector_entry(
    intent: &VectorIndexCreateIntentV1,
    candidate: &VectorIndexCreateCandidateV1,
) -> TableCatalogStoreResult<TableEntry> {
    let resource = intent.resource_identity()?;
    let metadata = CatalogMetadataRef::try_from(&candidate.metadata)?;
    let descriptor = CatalogFormatDescriptor::try_from(&candidate.descriptor)?;
    let entry = TableEntry {
        version: 3,
        table_bucket: intent.bucket.storage_bucket_id.clone(),
        namespace: intent.namespace.clone(),
        table: intent.name.clone(),
        table_id: intent.table_id.clone(),
        table_uuid: intent.table_id.clone(),
        format: "lance".into(),
        format_version: 1,
        warehouse_location: format!(
            "s3://{}/{}",
            intent.bucket.storage_bucket_id,
            resource.dataset_prefix().trim_end_matches('/')
        ),
        metadata_location: metadata.object_key().as_str().to_string(),
        version_token: format!("initial-{}", intent.create_id),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: Some(intent.created_at.clone()),
        updated_at: Some(intent.created_at.clone()),
        vector_head_binding: Some(VectorHeadBindingV1::try_new(&resource, &metadata, &descriptor)?),
        vector_index_definition: Some(intent.definition.clone()),
    };
    CatalogHead::from_vector_entry(&entry)?;
    Ok(entry)
}

#[cfg(test)]
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn list_vector_creation_intents_unprotected(
        &self,
        bucket: &BucketHandle,
    ) -> TableCatalogStoreResult<Vec<VectorIndexCreateIntentV1>> {
        let prefix = self.paths.table_bucket_root_prefix(bucket.storage_bucket_id().as_str());
        let mut intents = Vec::new();
        for path in self.backend.list_objects(self.catalog_bucket(), &prefix).await? {
            if !path.ends_with("/create-intent.json") {
                continue;
            }
            let (intent, _) = self
                .read_entry::<VectorIndexCreateIntentV1>(self.catalog_bucket(), &path)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Conflict("Vector create intent disappeared during enumeration".into()))?;
            validate_vector_create_intent_object(&self.paths, &path, &intent)?;
            if intent.bucket.to_handle()? != *bucket {
                return Err(TableCatalogStoreError::Conflict(
                    "Vector create intent belongs to another bucket incarnation".into(),
                ));
            }
            intents.push(intent);
        }
        Ok(intents)
    }

    pub(crate) async fn list_vector_index_creation_reservations(
        &self,
        bucket: &BucketHandle,
    ) -> TableCatalogStoreResult<Vec<VectorIndexCreateReservation>> {
        if self.vector_bucket_for(bucket.storage_bucket_id().as_str())? != bucket {
            return Err(TableCatalogStoreError::Conflict(
                "Vector creation enumeration differs from its store".into(),
            ));
        }
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        let intents = self.list_vector_creation_intents_unprotected(bucket).await?;
        if lifecycle.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable(
                "Vector creation enumeration lost lifecycle protection".into(),
            ));
        }
        self.backend.revalidate_bucket_handle(bucket).await?;
        intents.into_iter().map(VectorIndexCreateReservation::from_intent).collect()
    }

    pub(crate) async fn resume_vector_index_creation(
        &self,
        bucket: &BucketHandle,
        namespace: &Namespace,
        name: &CatalogName,
    ) -> TableCatalogStoreResult<(VectorIndexCreateReservation, VectorCatalogOperationGuard)> {
        if self.vector_bucket_for(bucket.storage_bucket_id().as_str())? != bucket || namespace.public_name() != "indexes" {
            return Err(TableCatalogStoreError::Conflict("Vector creation recovery differs from its store".into()));
        }
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        let (intent, _) = self
            .read_vector_create_intent(bucket, namespace, name)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("Vector creation reservation".into()))?;
        let reservation = VectorIndexCreateReservation::from_intent(intent)?;
        let guard = self
            .retain_vector_resource_guard(reservation.resource_identity.clone(), namespace.clone(), name.clone(), lifecycle)
            .await?;
        let request = VectorIndexCreateRequest {
            resource_identity: reservation.resource_identity,
            namespace: reservation.namespace,
            name: reservation.name,
            create_id: reservation.create_id,
            request_id: Uuid::parse_str(&reservation.intent.request_id)
                .map_err(|_| TableCatalogStoreError::Invalid("Vector create request ID is invalid".into()))?,
            definition: reservation.intent.definition,
            initialization_payload: reservation.initialization_payload,
            planned_manifest: reservation.planned_manifest,
        };
        let store = self.clone();
        run_vector_catalog_mutation(async move {
            let reservation = store.reserve_vector_index_guarded(&guard, request).await?;
            Ok((reservation, guard))
        })
        .await
    }

    async fn read_vector_create_intent(
        &self,
        bucket: &BucketHandle,
        namespace: &Namespace,
        name: &CatalogName,
    ) -> TableCatalogStoreResult<Option<(VectorIndexCreateIntentV1, String)>> {
        let path = self
            .paths
            .vector_create_intent_path(bucket.storage_bucket_id().as_str(), namespace, name);
        let Some((intent, etag)) = self
            .read_entry::<VectorIndexCreateIntentV1>(self.catalog_bucket(), &path)
            .await?
        else {
            return Ok(None);
        };
        validate_vector_create_intent_object(&self.paths, &path, &intent)?;
        if intent.bucket.to_handle()? != *bucket {
            return Err(TableCatalogStoreError::Conflict(
                "Vector creation belongs to another bucket lifetime".into(),
            ));
        }
        Ok(Some((
            intent,
            etag.ok_or_else(|| TableCatalogStoreError::Invalid("Vector create intent has no ETag".into()))?,
        )))
    }

    async fn verify_vector_replaced_entry(&self, entry: &TableEntry) -> TableCatalogStoreResult<()> {
        if entry.version != 3 || entry.state != TableCatalogEntryState::Deleted {
            return Err(TableCatalogStoreError::Conflict(
                "Vector name is occupied by an active or incompatible entry".into(),
            ));
        }
        let path = self.paths.vector_delete_intent_path(&entry.table_bucket, &entry.table_id);
        let (intent, _) = self
            .read_entry::<VectorIndexDeleteIntentV1>(self.catalog_bucket(), &path)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Conflict("Vector tombstone has no retained deletion proof".into()))?;
        validate_vector_delete_intent_object(&self.paths, &path, &intent)?;
        if intent.deleted_entry != *entry
            || !matches!(
                intent.phase,
                VectorIndexDeletePhase::LogicallyDeleted
                    | VectorIndexDeletePhase::RetirementPending
                    | VectorIndexDeletePhase::Complete
            )
        {
            return Err(TableCatalogStoreError::Conflict("Vector tombstone deletion proof is incomplete".into()));
        }
        self.verify_vector_deleted_history(&intent, Some(entry)).await.map(|_| ())
    }

    async fn verify_vector_initial_history(&self, current: &TableEntry, initial: &TableEntry) -> TableCatalogStoreResult<()> {
        if current.table_id != initial.table_id || current.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::Conflict("Vector creation no longer owns an active name".into()));
        }
        let logs = self
            .read_table_commit_logs(current)
            .await?
            .into_iter()
            .map(|(_, log)| log)
            .collect::<Vec<_>>();
        let versions = vector_committed_history_versions(current, &logs)?;
        let mut recovered = current.clone();
        if current.generation > 1 {
            let first_commit_id = versions
                .get(
                    versions
                        .len()
                        .checked_sub(2)
                        .ok_or_else(|| TableCatalogStoreError::Invalid("Vector creation history is incomplete".into()))?,
                )
                .and_then(CatalogCommittedVersion::commit_id)
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector creation history has no first commit".into()))?;
            let first = logs
                .iter()
                .find(|log| log.commit_id == first_commit_id.as_str())
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector initial predecessor record is missing".into()))?;
            let binding = first
                .vector_commit_binding
                .as_ref()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector initial predecessor has no binding".into()))?;
            let mutation = first
                .vector_resource_mutation
                .as_ref()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector initial predecessor has no resource state".into()))?;
            let metadata = CatalogMetadataRef::try_from(
                binding
                    .expected_metadata
                    .as_ref()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("Vector initial metadata is missing".into()))?,
            )?;
            let descriptor = CatalogFormatDescriptor::try_from(
                binding
                    .expected_descriptor
                    .as_ref()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("Vector initial descriptor is missing".into()))?,
            )?;
            let resource = CatalogHead::from_vector_entry(current)?.resource_identity().clone();
            recovered.metadata_location = metadata.object_key().as_str().to_string();
            recovered.vector_head_binding = Some(VectorHeadBindingV1::try_new(&resource, &metadata, &descriptor)?);
            recovered.version_token.clone_from(&first.expected_version_token);
            recovered.generation = binding.expected_generation;
            recovered.vector_index_definition = Some(mutation.expected_definition.clone());
            recovered.state = mutation.expected_state.clone();
            recovered.updated_at.clone_from(&initial.updated_at);
        }
        if recovered != *initial {
            return Err(TableCatalogStoreError::Invalid(
                "Vector history does not prove the exact initial publication".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn reserve_vector_index(
        &self,
        bucket: &BucketHandle,
        request: VectorIndexCreateRequest,
    ) -> TableCatalogStoreResult<(VectorIndexCreateReservation, VectorCatalogOperationGuard)> {
        if request.resource_identity.vector_bucket() != Some(bucket)
            || self.vector_bucket_for(bucket.storage_bucket_id().as_str())? != bucket
        {
            return Err(TableCatalogStoreError::Conflict("Vector creation identity differs from the store".into()));
        }
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        let guard = self
            .retain_vector_resource_guard(
                request.resource_identity.clone(),
                request.namespace.clone(),
                request.name.clone(),
                lifecycle,
            )
            .await?;
        let store = self.clone();
        run_vector_catalog_mutation(async move {
            let reservation = store.reserve_vector_index_guarded(&guard, request).await?;
            Ok((reservation, guard))
        })
        .await
    }

    async fn reserve_vector_index_guarded(
        &self,
        guard: &VectorCatalogOperationGuard,
        request: VectorIndexCreateRequest,
    ) -> TableCatalogStoreResult<VectorIndexCreateReservation> {
        self.validate_vector_operation_guard(guard)?;
        let bucket = self.vector_bucket_for(guard.resource_identity().storage_bucket_id())?;
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        publication_preparation(&publication, async {
            publication.begin_table_bucket(bucket.storage_bucket_id().as_str()).await?;
            let _completion = TableCommitPublicationCompletion::new(&publication);
            let table_path = self
                .paths
                .table_entry_path(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name());
            let table_lock = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            publication
                .prepare(
                    bucket.storage_bucket_id().as_str(),
                    &guard.namespace().public_name(),
                    guard.name().as_str(),
                )
                .await?;
            retain_publication_preparation_fence(&publication);
            let mut fence = publication.object_mutation_fence().unwrap_or_default();
            fence.signals.extend(guard.inner.fence.signals.iter().cloned());
            fence.extend_signals([table_lock.lock_lost_signal()]);
            fence.ensure_held()?;
            let (parent, parent_etag) = self.require_vector_parent_and_namespace(bucket, guard.namespace()).await?;
            let current = self
                .read_table_with_etag_unlocked(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name())
                .await?;
            let existing = self
                .read_vector_create_intent(bucket, guard.namespace(), guard.name())
                .await?;
            let expected_name = current
                .as_ref()
                .filter(|(entry, _)| entry.table_id != request.resource_identity.table_id().as_str())
                .map(|(entry, etag)| VectorIndexCreateNameV1 {
                    table_id: entry.table_id.clone(),
                    version_token: entry.version_token.clone(),
                    generation: entry.generation,
                    state: entry.state.clone(),
                    entry_etag: etag.clone(),
                });
            let now = next_table_catalog_update_time(None);
            let mut requested = VectorIndexCreateIntentV1 {
                version: 1,
                bucket: VectorBucketBindingV1::from_handle(bucket)?,
                namespace: request.namespace.public_name(),
                name: request.name.as_str().to_string(),
                table_id: request.resource_identity.table_id().as_str().to_string(),
                create_id: request.create_id.as_str().to_string(),
                request_id: request.request_id.to_string(),
                request_sha256: String::new(),
                expected_parent: VectorIndexCreateParentV1 {
                    bucket_entry_etag: parent_etag,
                    bucket: parent
                        .vector_bucket_binding
                        .ok_or_else(|| TableCatalogStoreError::Invalid("Vector parent has no binding".into()))?,
                    state: parent.state,
                },
                expected_name,
                definition: request.definition,
                initialization_payload_base64: base64_simd::STANDARD.encode_to_string(&request.initialization_payload),
                planned_manifest: (&request.planned_manifest).into(),
                candidate: None,
                phase: VectorIndexCreatePhase::Reserved,
                published_head: None,
                created_at: now.clone(),
                updated_at: now,
            };
            requested.request_sha256 = requested.canonical_request_digest()?.to_hex();
            if let Some((existing, _)) = &existing
                && existing.create_id == requested.create_id
            {
                if existing.request_sha256 != requested.request_sha256 {
                    return Err(TableCatalogStoreError::Conflict("Vector create ID already binds another request".into()));
                }
                if matches!(existing.phase, VectorIndexCreatePhase::Aborted | VectorIndexCreatePhase::ManualReview) {
                    return Err(TableCatalogStoreError::Conflict("Vector creation requires lifecycle recovery".into()));
                }
                let mut reservation = VectorIndexCreateReservation::from_intent(existing.clone())?;
                if existing.phase == VectorIndexCreatePhase::Published {
                    let initial = existing
                        .published_head
                        .as_ref()
                        .ok_or_else(|| TableCatalogStoreError::Invalid("completed Vector creation has no head".into()))?;
                    let (current, _) = current
                        .as_ref()
                        .ok_or_else(|| TableCatalogStoreError::Invalid("completed Vector creation lost its head".into()))?;
                    self.verify_vector_initial_history(current, initial).await?;
                    reservation.published_entry = Some(VectorCatalogEntry::from_entry(initial.clone())?);
                } else if current.as_ref().is_some_and(|(entry, _)| entry.table_id != existing.table_id)
                    && !vector_reserved_name_matches(existing.expected_name.as_ref(), current.as_ref())
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "Vector creation no longer owns its reserved name".into(),
                    ));
                }
                guard.check_held()?;
                return Ok(reservation);
            }
            if let Some((current, _)) = &current {
                self.verify_vector_replaced_entry(current).await?;
            }
            if let Some((existing, _)) = &existing {
                match existing.phase {
                    VectorIndexCreatePhase::Published
                        if current.as_ref().is_some_and(|(entry, _)| entry.table_id == existing.table_id) => {}
                    VectorIndexCreatePhase::Aborted if current.is_none() => {}
                    _ => return Err(TableCatalogStoreError::Conflict("Vector name has a pending creation reservation".into())),
                }
            }
            requested.validate()?;
            if self
                .read_entry::<VectorIndexDeleteIntentV1>(
                    self.catalog_bucket(),
                    &self
                        .paths
                        .vector_delete_intent_path(bucket.storage_bucket_id().as_str(), &requested.table_id),
                )
                .await?
                .is_some()
            {
                return Err(TableCatalogStoreError::Conflict("Vector table ID was already used".into()));
            }
            let entries = self
                .list_all_table_entries_with_limit(bucket.storage_bucket_id().as_str(), None, Some(&table_path), 1)
                .await?;
            if entries.iter().any(|entry| entry.table_id == requested.table_id) {
                return Err(TableCatalogStoreError::Conflict("Vector table ID belongs to another entry".into()));
            }
            for reservation in self.list_vector_creation_intents_unprotected(bucket).await? {
                if reservation.table_id == requested.table_id {
                    return Err(TableCatalogStoreError::Conflict("Vector table ID belongs to another reservation".into()));
                }
            }
            let path = self
                .paths
                .vector_create_intent_path(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name());
            let precondition = existing.map_or(TableCatalogPutPrecondition::IfAbsent, |(_, etag)| {
                TableCatalogPutPrecondition::IfMatch(etag)
            });
            self.write_vector_record_fenced(&path, &requested, precondition, &fence)
                .await?;
            VectorIndexCreateReservation::from_intent(requested)
        })
        .await
    }

    pub(crate) async fn publish_vector_index_initial_head(
        &self,
        guard: &VectorCatalogOperationGuard,
        reservation: &VectorIndexCreateReservation,
        prepared: PreparedCatalogCommit,
    ) -> TableCatalogStoreResult<VectorCatalogEntry> {
        self.validate_vector_operation_guard(guard)?;
        if reservation.resource_identity() != guard.resource_identity()
            || reservation.namespace() != guard.namespace()
            || reservation.name() != guard.name()
            || prepared.resource_identity() != guard.resource_identity()
            || prepared.namespace() != guard.namespace()
            || prepared.name() != guard.name()
            || prepared.expected_head() != reservation.expected_head()
            || prepared.commit_id() != reservation.create_id()
            || prepared.operation() != "vector-create"
            || prepared.idempotency_key().is_some()
            || prepared.writer().is_some()
            || prepared.vector_resource_mutation().is_some()
        {
            return Err(TableCatalogStoreError::Conflict(
                "Vector initial preparation differs from the reserved request".into(),
            ));
        }
        let planned = reservation.planned_manifest();
        let candidate = prepared.candidate_metadata();
        if candidate.object_key() != planned.object_key()
            || candidate.content_length() != planned.content_length()
            || candidate.content_sha256() != planned.content_sha256()
            || candidate.body_revision().is_none()
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector initial candidate differs from the reserved bytes".into(),
            ));
        }
        let store = self.clone();
        let guard = guard.clone();
        let reservation = reservation.clone();
        run_vector_catalog_mutation(async move {
            store
                .publish_vector_index_initial_guarded(&guard, &reservation, prepared)
                .await
        })
        .await
    }

    async fn publish_vector_index_initial_guarded(
        &self,
        guard: &VectorCatalogOperationGuard,
        reservation: &VectorIndexCreateReservation,
        prepared: PreparedCatalogCommit,
    ) -> TableCatalogStoreResult<VectorCatalogEntry> {
        self.validate_vector_operation_guard(guard)?;
        let bucket = self.vector_bucket_for(guard.resource_identity().storage_bucket_id())?;
        let (observed, _) = self
            .read_vector_create_intent(bucket, guard.namespace(), guard.name())
            .await?
            .ok_or_else(|| TableCatalogStoreError::Conflict("Vector creation reservation disappeared".into()))?;
        if observed.create_id != reservation.intent.create_id || observed.request_sha256 != reservation.intent.request_sha256 {
            return Err(TableCatalogStoreError::Conflict("Vector creation reservation changed".into()));
        }
        if observed.phase != VectorIndexCreatePhase::Published {
            let limit = usize::try_from(prepared.candidate_metadata().content_length())
                .ok()
                .and_then(|size| size.checked_add(1))
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector initial candidate exceeds address space".into()))?;
            let mut lifecycle = guard.inner.lifecycle.lock().await;
            let object = self
                .backend
                .read_managed_candidate(bucket, prepared.candidate_metadata(), limit, &mut lifecycle)
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound("Vector initial candidate".into()))?;
            prepared.candidate_metadata().verify_read(&object)?;
        }
        let candidate = VectorIndexCreateCandidateV1 {
            metadata: prepared.candidate_metadata().into(),
            descriptor: prepared.format_descriptor().into(),
        };
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        publication_preparation(&publication, async {
            publication.begin_table_bucket(bucket.storage_bucket_id().as_str()).await?;
            let _completion = TableCommitPublicationCompletion::new(&publication);
            let table_path = self
                .paths
                .table_entry_path(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name());
            let table_lock = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            publication
                .prepare(
                    bucket.storage_bucket_id().as_str(),
                    &guard.namespace().public_name(),
                    guard.name().as_str(),
                )
                .await?;
            retain_publication_preparation_fence(&publication);
            let mut fence = publication.object_mutation_fence().unwrap_or_default();
            fence.signals.extend(guard.inner.fence.signals.iter().cloned());
            fence.extend_signals([table_lock.lock_lost_signal()]);
            fence.ensure_held()?;
            self.backend.revalidate_bucket_handle(bucket).await?;
            self.require_vector_parent_and_namespace(bucket, guard.namespace()).await?;
            let (mut intent, mut etag) = self
                .read_vector_create_intent(bucket, guard.namespace(), guard.name())
                .await?
                .ok_or_else(|| TableCatalogStoreError::Conflict("Vector creation reservation disappeared".into()))?;
            if intent.create_id != reservation.intent.create_id
                || intent.request_sha256 != reservation.intent.request_sha256
                || matches!(intent.phase, VectorIndexCreatePhase::Aborted | VectorIndexCreatePhase::ManualReview)
            {
                return Err(TableCatalogStoreError::Conflict("Vector creation is no longer publishable".into()));
            }
            if intent.candidate.as_ref().is_some_and(|saved| saved != &candidate) {
                return Err(TableCatalogStoreError::Conflict("Vector creation candidate identity changed".into()));
            }
            let initial = initial_vector_entry(&intent, &candidate)?;
            let current = self
                .read_table_with_etag_unlocked(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name())
                .await?;
            let path = self
                .paths
                .vector_create_intent_path(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name());
            if let Some((current, _)) = &current
                && current.table_id == intent.table_id
            {
                if intent.candidate.is_none() {
                    return Err(TableCatalogStoreError::Invalid(
                        "Vector initial head has no preceding candidate record".into(),
                    ));
                }
                self.verify_vector_initial_history(current, &initial).await?;
            } else {
                if intent.phase == VectorIndexCreatePhase::Published
                    || !vector_reserved_name_matches(intent.expected_name.as_ref(), current.as_ref())
                {
                    return Err(TableCatalogStoreError::Conflict("Vector creation name precondition changed".into()));
                }
                if let Some((current, _)) = &current {
                    self.verify_vector_replaced_entry(current).await?;
                }
                if intent.phase == VectorIndexCreatePhase::Reserved {
                    intent.candidate = Some(candidate.clone());
                    intent.phase = VectorIndexCreatePhase::CandidateReady;
                    intent.updated_at = next_table_catalog_update_time(Some(&intent.updated_at));
                    intent.validate()?;
                    self.write_vector_record_fenced(&path, &intent, TableCatalogPutPrecondition::IfMatch(etag), &fence)
                        .await?;
                    let (saved, saved_etag) = self
                        .read_vector_create_intent(bucket, guard.namespace(), guard.name())
                        .await?
                        .ok_or_else(|| TableCatalogStoreError::Invalid("Vector candidate-ready record disappeared".into()))?;
                    if saved != intent {
                        return Err(TableCatalogStoreError::Conflict("Vector candidate-ready record changed".into()));
                    }
                    etag = saved_etag;
                }
                self.paths.validate_table_entry(&initial)?;
                let condition = current.map_or(TableCatalogPutPrecondition::IfAbsent, |(_, etag)| {
                    TableCatalogPutPrecondition::IfMatch(etag)
                });
                self.write_entry_unlocked_with_fence(
                    self.catalog_bucket(),
                    &table_path,
                    &initial,
                    condition,
                    table_lock.write_commit_guards(),
                    Some(&fence),
                )
                .await?;
                let (published, _) = self
                    .read_table_with_etag_unlocked(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name())
                    .await?
                    .ok_or_else(|| TableCatalogStoreError::Invalid("Vector initial head disappeared after publication".into()))?;
                if published != initial {
                    return Err(TableCatalogStoreError::Conflict("Vector initial head differs after publication".into()));
                }
            }
            if intent.phase != VectorIndexCreatePhase::Published {
                intent.phase = VectorIndexCreatePhase::Published;
                intent.published_head = Some(initial.clone());
                intent.updated_at = next_table_catalog_update_time(Some(&intent.updated_at));
                intent.validate()?;
                self.write_vector_record_fenced(&path, &intent, TableCatalogPutPrecondition::IfMatch(etag), &fence)
                    .await?;
            }
            fence.ensure_held()?;
            VectorCatalogEntry::from_entry(initial)
        })
        .await
    }
}

#[cfg(test)]
fn prepare_vector_resource_mutation(
    expected: &TableEntry,
    mutation: VectorResourceMutationBindingV1,
    commit_id: CatalogCommitId,
    operation: &str,
) -> TableCatalogStoreResult<PreparedCatalogCommit> {
    let head = CatalogHead::from_vector_entry(expected)?;
    PreparedCatalogCommit::try_from_validated_parts(
        CatalogCommitParts {
            resource_identity: head.resource_identity().clone(),
            namespace: Namespace::parse(&expected.namespace)
                .map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?,
            name: CatalogName::parse(&expected.table).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?,
            expected_head: head.expected_head(),
            commit_id,
            operation: operation.into(),
            idempotency_key: None,
            writer: None,
        },
        head.metadata().clone(),
        head.format_descriptor().clone(),
        CatalogEntryUpdates::try_new(Some(expected.warehouse_location.clone()), Some(expected.format_version))?,
        Vec::new(),
    )?
    .with_vector_resource_mutation(mutation)
}

#[cfg(test)]
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn read_vector_delete_intent(
        &self,
        bucket: &BucketHandle,
        table_id: &str,
    ) -> TableCatalogStoreResult<Option<(VectorIndexDeleteIntentV1, String)>> {
        let path = self
            .paths
            .vector_delete_intent_path(bucket.storage_bucket_id().as_str(), table_id);
        let Some((intent, etag)) = self
            .read_entry::<VectorIndexDeleteIntentV1>(self.catalog_bucket(), &path)
            .await?
        else {
            return Ok(None);
        };
        validate_vector_delete_intent_object(&self.paths, &path, &intent)?;
        if intent.bucket.to_handle()? != *bucket {
            return Err(TableCatalogStoreError::Conflict(
                "Vector deletion belongs to another bucket lifetime".into(),
            ));
        }
        Ok(Some((
            intent,
            etag.ok_or_else(|| TableCatalogStoreError::Invalid("Vector delete intent has no ETag".into()))?,
        )))
    }

    async fn verify_vector_deleted_history(
        &self,
        intent: &VectorIndexDeleteIntentV1,
        authoritative: Option<&TableEntry>,
    ) -> TableCatalogStoreResult<CommitLogEntry> {
        intent.validate()?;
        let logs = self
            .read_table_commit_logs(&intent.deleted_entry)
            .await?
            .into_iter()
            .map(|(_, log)| log)
            .collect::<Vec<_>>();
        let matching = logs
            .iter()
            .filter(|log| log.commit_id == intent.delete_id)
            .collect::<Vec<_>>();
        let [log] = matching.as_slice() else {
            return Err(TableCatalogStoreError::Invalid(
                "Vector deletion has no unique exact commit receipt".into(),
            ));
        };
        let mutation = VectorResourceMutationBindingV1 {
            version: 1,
            kind: VectorResourceMutationKind::Delete,
            expected_definition: intent
                .expected_entry
                .vector_index_definition
                .clone()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector deletion has no definition".into()))?,
            next_definition: intent
                .deleted_entry
                .vector_index_definition
                .clone()
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector deletion has no terminal definition".into()))?,
            expected_state: TableCatalogEntryState::Active,
            next_state: TableCatalogEntryState::Deleted,
        };
        let prepared = prepare_vector_resource_mutation(
            &intent.expected_entry,
            mutation,
            CatalogCommitId::try_new(intent.delete_id.clone())?,
            "vector-delete",
        )?;
        if !commit_log_matches_prepared(log, &prepared, &intent.table_id)
            || !table_matches_committed_log(&intent.deleted_entry, log)
            || !table_matches_staged_base(&intent.expected_entry, log)
            || log.status == CommitLogStatus::Failed
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector delete receipt differs from its exact resource transition".into(),
            ));
        }
        let observed_terminal = authoritative.is_some_and(|entry| *entry == intent.deleted_entry);
        let retained_terminal = matches!(
            intent.phase,
            VectorIndexDeletePhase::LogicallyDeleted
                | VectorIndexDeletePhase::RetirementPending
                | VectorIndexDeletePhase::Complete
        ) && log.status == CommitLogStatus::Committed;
        if !observed_terminal && !retained_terminal {
            return Err(TableCatalogStoreError::Conflict(
                "Vector deletion has no authoritative terminal evidence".into(),
            ));
        }
        // A PREPARED intent cannot prove its own publication. The current head
        // or a previously finalized retained deletion supplies that evidence.
        vector_committed_history_versions(&intent.deleted_entry, &logs)?;
        Ok((**log).clone())
    }

    pub(crate) async fn delete_vector_index_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        expected: &VectorCatalogEntry,
        delete_id: CatalogCommitId,
    ) -> TableCatalogStoreResult<()> {
        self.validate_vector_operation_guard(guard)?;
        if expected.head.resource_identity() != guard.resource_identity()
            || expected.entry.namespace != guard.namespace().public_name()
            || expected.entry.table != guard.name().as_str()
            || expected.entry.state != TableCatalogEntryState::Active
        {
            return Err(TableCatalogStoreError::Conflict(
                "Vector deletion differs from its operation guard".into(),
            ));
        }
        let mutation = VectorResourceMutationBindingV1 {
            version: 1,
            kind: VectorResourceMutationKind::Delete,
            expected_definition: expected.definition.clone(),
            next_definition: expected.definition.clone(),
            expected_state: TableCatalogEntryState::Active,
            next_state: TableCatalogEntryState::Deleted,
        };
        let prepared = prepare_vector_resource_mutation(&expected.entry, mutation, delete_id.clone(), "vector-delete")?;
        let mut deleted_entry = expected.entry.clone();
        deleted_entry.state = TableCatalogEntryState::Deleted;
        deleted_entry.generation = expected.head.generation().checked_next()?.get();
        deleted_entry.version_token = format!("delete-{}", delete_id.as_str());
        let now = next_table_catalog_update_time(None);
        let intent = VectorIndexDeleteIntentV1 {
            version: 1,
            bucket: VectorBucketBindingV1::from_handle(
                guard
                    .resource_identity()
                    .vector_bucket()
                    .ok_or_else(|| TableCatalogStoreError::Invalid("Vector deletion has no bucket".into()))?,
            )?,
            namespace: guard.namespace().public_name(),
            name: guard.name().as_str().into(),
            table_id: guard.resource_identity().table_id().as_str().into(),
            delete_id: delete_id.as_str().into(),
            phase: VectorIndexDeletePhase::Prepared,
            expected_entry: expected.entry.clone(),
            deleted_entry,
            request_sha256: prepared.request_digest().to_hex(),
            created_at: now.clone(),
            updated_at: now,
        };
        intent.validate()?;
        let store = self.clone();
        let guard = guard.clone();
        run_vector_catalog_mutation(async move {
            let intent = store.reserve_vector_deletion(&guard, intent).await?;
            store.continue_vector_deletion(&guard, intent, prepared).await
        })
        .await
    }

    async fn reserve_vector_deletion(
        &self,
        guard: &VectorCatalogOperationGuard,
        requested: VectorIndexDeleteIntentV1,
    ) -> TableCatalogStoreResult<VectorIndexDeleteIntentV1> {
        let bucket = self.vector_bucket_for(guard.resource_identity().storage_bucket_id())?;
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        publication_preparation(&publication, async {
            publication.begin_table_bucket(bucket.storage_bucket_id().as_str()).await?;
            let _completion = TableCommitPublicationCompletion::new(&publication);
            let table_path = self
                .paths
                .table_entry_path(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name());
            let table_lock = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            publication
                .prepare(
                    bucket.storage_bucket_id().as_str(),
                    &guard.namespace().public_name(),
                    guard.name().as_str(),
                )
                .await?;
            retain_publication_preparation_fence(&publication);
            let mut fence = publication.object_mutation_fence().unwrap_or_default();
            fence.signals.extend(guard.inner.fence.signals.iter().cloned());
            fence.extend_signals([table_lock.lock_lost_signal()]);
            fence.ensure_held()?;
            self.backend.revalidate_bucket_handle(bucket).await?;
            if let Some((existing, _)) = self.read_vector_delete_intent(bucket, &requested.table_id).await? {
                if existing.delete_id != requested.delete_id
                    || existing.request_sha256 != requested.request_sha256
                    || existing.expected_entry != requested.expected_entry
                    || existing.deleted_entry != requested.deleted_entry
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "Vector resource already has another deletion intent".into(),
                    ));
                }
                if existing.phase == VectorIndexDeletePhase::ManualReview {
                    return Err(TableCatalogStoreError::Conflict("Vector deletion requires manual review".into()));
                }
                return Ok(existing);
            }
            let (current, _) = self
                .read_table_with_etag_unlocked(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name())
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound("Vector deletion target".into()))?;
            if current != requested.expected_entry {
                return Err(TableCatalogStoreError::Conflict("Vector deletion expected entry changed".into()));
            }
            self.require_vector_parent_and_namespace(bucket, guard.namespace()).await?;
            let path = self
                .paths
                .vector_delete_intent_path(bucket.storage_bucket_id().as_str(), &requested.table_id);
            self.write_vector_record_fenced(&path, &requested, TableCatalogPutPrecondition::IfAbsent, &fence)
                .await?;
            Ok(requested)
        })
        .await
    }

    async fn continue_vector_deletion(
        &self,
        guard: &VectorCatalogOperationGuard,
        intent: VectorIndexDeleteIntentV1,
        prepared: PreparedCatalogCommit,
    ) -> TableCatalogStoreResult<()> {
        let bucket = self.vector_bucket_for(guard.resource_identity().storage_bucket_id())?;
        let current = self
            .read_table_with_etag(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name())
            .await?;
        if current.as_ref().is_some_and(|(entry, _)| *entry == intent.expected_entry)
            && intent.phase == VectorIndexDeletePhase::Prepared
        {
            let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
            self.commit_catalog_with_operation_guard(CatalogCommitInput::Prepared(Box::new(prepared)), &publication, Some(guard))
                .await?;
        }
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        publication_preparation(&publication, async {
            publication.begin_table_bucket(bucket.storage_bucket_id().as_str()).await?;
            let _completion = TableCommitPublicationCompletion::new(&publication);
            let table_path = self
                .paths
                .table_entry_path(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name());
            let table_lock = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
            publication
                .prepare(
                    bucket.storage_bucket_id().as_str(),
                    &guard.namespace().public_name(),
                    guard.name().as_str(),
                )
                .await?;
            retain_publication_preparation_fence(&publication);
            let mut fence = publication.object_mutation_fence().unwrap_or_default();
            fence.signals.extend(guard.inner.fence.signals.iter().cloned());
            fence.extend_signals([table_lock.lock_lost_signal()]);
            fence.ensure_held()?;
            self.backend.revalidate_bucket_handle(bucket).await?;
            let (mut saved, etag) = self
                .read_vector_delete_intent(bucket, &intent.table_id)
                .await?
                .ok_or_else(|| TableCatalogStoreError::Invalid("Vector deletion intent disappeared".into()))?;
            if saved.delete_id != intent.delete_id || saved.request_sha256 != intent.request_sha256 {
                return Err(TableCatalogStoreError::Conflict("Vector deletion identity changed".into()));
            }
            let current = self
                .read_table_with_etag_unlocked(bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name())
                .await?;
            let mut log = self
                .verify_vector_deleted_history(&saved, current.as_ref().map(|(entry, _)| entry))
                .await?;
            log.status = CommitLogStatus::Committed;
            let log_path =
                self.paths
                    .commit_log_entry_path(bucket.storage_bucket_id().as_str(), &saved.table_id, &saved.delete_id);
            self.finalize_commit_log_with_fence(&log_path, None, &log, Some(&fence))
                .await?;
            if saved.phase == VectorIndexDeletePhase::Prepared {
                saved.phase = VectorIndexDeletePhase::LogicallyDeleted;
                saved.updated_at = next_table_catalog_update_time(Some(&saved.updated_at));
                saved.validate()?;
                let path = self
                    .paths
                    .vector_delete_intent_path(bucket.storage_bucket_id().as_str(), &saved.table_id);
                self.write_vector_record_fenced(&path, &saved, TableCatalogPutPrecondition::IfMatch(etag), &fence)
                    .await?;
            }
            fence.ensure_held()
        })
        .await
    }

    pub(crate) async fn recover_vector_index_deletion(
        &self,
        bucket: &BucketHandle,
        table_id: &str,
    ) -> TableCatalogStoreResult<()> {
        if self.vector_bucket_for(bucket.storage_bucket_id().as_str())? != bucket {
            return Err(TableCatalogStoreError::Conflict("Vector delete recovery differs from its store".into()));
        }
        let lifecycle = self.backend.acquire_bucket_lifecycle_read(bucket).await?;
        let (intent, _) = self
            .read_vector_delete_intent(bucket, table_id)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("Vector deletion intent".into()))?;
        if intent.phase == VectorIndexDeletePhase::ManualReview {
            return Err(TableCatalogStoreError::Conflict("Vector deletion requires manual review".into()));
        }
        let head = CatalogHead::from_vector_entry(&intent.expected_entry)?;
        let namespace =
            Namespace::parse(&intent.namespace).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
        let name = CatalogName::parse(&intent.name).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
        let guard = self
            .retain_vector_resource_guard(head.resource_identity().clone(), namespace, name, lifecycle)
            .await?;
        let definition = intent
            .expected_entry
            .vector_index_definition
            .clone()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Vector deletion has no definition".into()))?;
        let mutation = VectorResourceMutationBindingV1 {
            version: 1,
            kind: VectorResourceMutationKind::Delete,
            expected_definition: definition.clone(),
            next_definition: definition,
            expected_state: TableCatalogEntryState::Active,
            next_state: TableCatalogEntryState::Deleted,
        };
        let prepared = prepare_vector_resource_mutation(
            &intent.expected_entry,
            mutation,
            CatalogCommitId::try_new(intent.delete_id.clone())?,
            "vector-delete",
        )?;
        let store = self.clone();
        run_vector_catalog_mutation(async move { store.continue_vector_deletion(&guard, intent, prepared).await }).await
    }
}

#[cfg(test)]
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn update_vector_tags_with_guard(
        &self,
        guard: &VectorCatalogOperationGuard,
        expected: &VectorCatalogEntry,
        update: VectorTagUpdate,
        commit_id: CatalogCommitId,
    ) -> TableCatalogStoreResult<VectorCatalogEntry> {
        self.validate_vector_operation_guard(guard)?;
        if expected.head.resource_identity() != guard.resource_identity()
            || expected.entry.namespace != guard.namespace().public_name()
            || expected.entry.table != guard.name().as_str()
            || expected.entry.state != TableCatalogEntryState::Active
        {
            return Err(TableCatalogStoreError::Conflict("Vector tags differ from the operation guard".into()));
        }
        let mut definition = expected.definition.clone();
        match update {
            VectorTagUpdate::Set(tags) => definition.tags.extend(tags),
            VectorTagUpdate::Remove(keys) => {
                for key in keys {
                    definition.tags.remove(&key);
                }
            }
        }
        definition.validate()?;
        let mutation = VectorResourceMutationBindingV1 {
            version: 1,
            kind: VectorResourceMutationKind::Tags,
            expected_definition: expected.definition.clone(),
            next_definition: definition,
            expected_state: TableCatalogEntryState::Active,
            next_state: TableCatalogEntryState::Active,
        };
        let prepared = prepare_vector_resource_mutation(&expected.entry, mutation, commit_id, "vector-tags")?;
        let store = self.clone();
        let guard = guard.clone();
        run_vector_catalog_mutation(async move {
            let publication = TableCommitLockPublication::new(&store.backend, &store.paths);
            let result = store
                .commit_catalog_with_operation_guard(CatalogCommitInput::Prepared(Box::new(prepared)), &publication, Some(&guard))
                .await?;
            guard.check_held()?;
            VectorCatalogEntry::from_entry(result.table)
        })
        .await
    }
}

#[cfg(test)]
impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    /// Remove only canonical empty Vector registration under ECStore's deletion writer.
    /// The owned application callback must supply the writer acquired by
    /// `delete_managed_bucket_with_preparation` and retain it through physical cleanup.
    pub(crate) async fn prepare_empty_vector_bucket_delete(
        &self,
        bucket: &BucketHandle,
        lifecycle: Arc<BucketIncarnationFenceGuard>,
    ) -> TableCatalogStoreResult<()> {
        // BucketHandle caps names at 63 bytes and region at 255 bytes. Canonical
        // empty records fit below 4 KiB even with six-byte JSON escaping.
        const MAX_EMPTY_REGISTRATION_BYTES: usize = 8 * 1024;
        if self.paths.scope != CatalogScope::Vector || self.vector_bucket_for(bucket.storage_bucket_id().as_str())? != bucket {
            return Err(TableCatalogStoreError::Conflict(
                "Vector deletion identity differs from its Catalog store".into(),
            ));
        }
        let lifecycle = self.backend.reuse_bucket_lifecycle_read(bucket, lifecycle).await?;
        let registry = self.acquire_table_bucket_registry_write_permit().await?;
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        let owner_signals = [lifecycle.lock_lost_signal(), registry.lock_lost_signal()];
        publication_preparation(&publication, async {
            let storage_name = bucket.storage_bucket_id().as_str();
            publication.begin_table_bucket(storage_name).await?;
            let _completion = TableCommitPublicationCompletion::new(&publication);
            retain_publication_preparation_fence(&publication);
            let mut fence = publication
                .object_mutation_fence()
                .ok_or_else(|| TableCatalogStoreError::Unavailable("Vector deletion publication fence is unavailable".into()))?;
            fence.extend_signals(owner_signals);
            fence.ensure_held()?;
            let prefix = self.paths.table_bucket_root_prefix(storage_name);
            let parent_path = self.paths.table_bucket_entry_path(storage_name);
            let namespace = Namespace::parse("indexes").map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
            let namespace_path = self.paths.namespace_entry_path(storage_name, &namespace);
            let expected_parent = TableBucketEntry {
                vector_bucket_binding: Some(VectorBucketBindingV1::from_handle(bucket)?),
                version: 2,
                table_bucket: storage_name.into(),
                catalog_type: "s3vectors".into(),
                warehouse_root: format!("s3://{storage_name}/"),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                active_rename_id: None,
                created_at: None,
                updated_at: None,
            };
            let expected_namespace = NamespaceEntry {
                version: 1,
                table_bucket: storage_name.into(),
                namespace: namespace.public_name(),
                namespace_id: namespace.storage_id(),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            };
            let mut parent_etag = None;
            let mut namespace_etag = None;
            let mut cursor: Option<String> = None;
            loop {
                fence.ensure_held()?;
                let page = self
                    .backend
                    .list_catalog_objects_page(self.catalog_bucket(), &prefix, cursor.as_deref(), NonZeroUsize::MIN)
                    .await?;
                if page.objects.len() > 1 || (page.is_truncated && page.objects.is_empty()) {
                    return Err(TableCatalogStoreError::Unavailable(
                        "Vector deletion received an invalid Catalog page".into(),
                    ));
                }
                for object in page.objects {
                    if !object.key.starts_with(&prefix) || cursor.as_ref().is_some_and(|previous| object.key <= *previous) {
                        return Err(TableCatalogStoreError::Invalid(
                            "Vector deletion Catalog scan did not advance within its prefix".into(),
                        ));
                    }
                    if object.delete_marker || object.version_id.is_some() {
                        return Err(TableCatalogStoreError::Invalid(
                            "Vector deletion encountered versioned or deleted Catalog metadata".into(),
                        ));
                    }
                    let etag = object
                        .etag
                        .filter(|etag| !etag.is_empty())
                        .ok_or_else(|| TableCatalogStoreError::Invalid("Vector deletion Catalog record has no ETag".into()))?;
                    if object.key == parent_path {
                        let (parent, current_etag) = self
                            .read_entry_with::<TableBucketEntry, _>(
                                self.catalog_bucket(),
                                &parent_path,
                                |backend, bucket, object| {
                                    Box::pin(async move {
                                        backend
                                            .read_object_unlocked_limited(bucket, object, MAX_EMPTY_REGISTRATION_BYTES)
                                            .await
                                    })
                                },
                            )
                            .await?
                            .ok_or_else(|| {
                                TableCatalogStoreError::Conflict("Vector parent disappeared during deletion preparation".into())
                            })?;
                        validate_table_bucket_entry_object(&self.paths, &parent_path, &parent)?;
                        self.validate_store_bucket_entry(&parent)?;
                        let current_etag = current_etag.ok_or_else(|| {
                            TableCatalogStoreError::Internal(format!("catalog table bucket entry has no etag: {parent_path}"))
                        })?;
                        if parent != expected_parent || current_etag != etag {
                            return Err(TableCatalogStoreError::Conflict(
                                "Vector parent differs from canonical empty registration".into(),
                            ));
                        }
                        parent_etag = Some(etag);
                    } else if object.key == namespace_path {
                        let (entry, current_etag) = self
                            .read_entry_with::<NamespaceEntry, _>(
                                self.catalog_bucket(),
                                &namespace_path,
                                |backend, bucket, object| {
                                    Box::pin(async move {
                                        backend
                                            .read_object_unlocked_limited(bucket, object, MAX_EMPTY_REGISTRATION_BYTES)
                                            .await
                                    })
                                },
                            )
                            .await?
                            .ok_or_else(|| {
                                TableCatalogStoreError::Conflict(
                                    "Vector namespace disappeared during deletion preparation".into(),
                                )
                            })?;
                        validate_namespace_entry_object(&self.paths, &namespace_path, &entry)?;
                        if entry != expected_namespace || current_etag.as_deref() != Some(etag.as_str()) {
                            return Err(TableCatalogStoreError::Conflict(
                                "Vector namespace differs from canonical empty registration".into(),
                            ));
                        }
                        namespace_etag = Some(etag);
                    } else {
                        return Err(TableCatalogStoreError::Conflict(
                            "Vector bucket retains Catalog resources or history".into(),
                        ));
                    }
                    cursor = Some(object.key);
                }
                if !page.is_truncated {
                    break;
                }
            }
            if namespace_etag.is_some() && parent_etag.is_none() {
                return Err(TableCatalogStoreError::Invalid("Vector namespace has no parent registration".into()));
            }
            self.backend.revalidate_bucket_handle(bucket).await?;
            fence.ensure_held()?;
            // Native conditional deletion acquires each exact object writer once.
            // Keep the namespace-to-parent order recoverable for the same identity.
            if let Some(etag) = namespace_etag {
                begin_publication_mutation()?;
                self.backend
                    .delete_object_if_match_fenced(self.catalog_bucket(), &namespace_path, &etag, &fence)
                    .await?;
            }
            self.backend.revalidate_bucket_handle(bucket).await?;
            fence.ensure_held()?;
            if let Some(etag) = parent_etag {
                begin_publication_mutation()?;
                self.backend
                    .delete_object_if_match_fenced(self.catalog_bucket(), &parent_path, &etag, &fence)
                    .await?;
            }
            self.backend.revalidate_bucket_handle(bucket).await?;
            fence.ensure_held()
        })
        .await
    }

    pub(crate) async fn ensure_vector_catalog_parent(
        &self,
        bucket: &BucketHandle,
        lifecycle: Arc<BucketIncarnationFenceGuard>,
    ) -> TableCatalogStoreResult<()> {
        let guard = self.backend.reuse_bucket_lifecycle_read(bucket, lifecycle).await?;
        let store = self.clone();
        let bucket = bucket.clone();
        run_vector_catalog_mutation(async move { store.ensure_vector_catalog_parent_guarded(&bucket, guard).await }).await
    }

    pub(in crate::table_catalog) async fn ensure_vector_catalog_parent_guarded(
        &self,
        bucket: &BucketHandle,
        lifecycle: TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<()> {
        if self.paths.scope != CatalogScope::Vector || self.vector_bucket_for(bucket.storage_bucket_id().as_str())? != bucket {
            return Err(TableCatalogStoreError::Conflict(
                "Vector parent registration differs from its store".into(),
            ));
        }
        let _registry = self.acquire_table_bucket_registry_write_permit().await?;
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        // Keep both owners in this scope through publication completion.
        let owner_signals = [lifecycle.lock_lost_signal(), _registry.lock_lost_signal()];
        publication_preparation(&publication, async {
            publication.begin_table_bucket(bucket.storage_bucket_id().as_str()).await?;
            let _completion = TableCommitPublicationCompletion::new(&publication);
            retain_publication_preparation_fence(&publication);
            let parent_path = self.paths.table_bucket_entry_path(bucket.storage_bucket_id().as_str());
            let parent_lock = self.backend.acquire_write_lock(self.catalog_bucket(), &parent_path).await?;
            let mut fence = publication.object_mutation_fence().unwrap_or_default();
            fence.extend_signals(owner_signals);
            fence.extend_signals([parent_lock.lock_lost_signal()]);
            fence.ensure_held()?;
            self.backend.revalidate_bucket_handle(bucket).await?;
            let parent = TableBucketEntry {
                vector_bucket_binding: Some(VectorBucketBindingV1::from_handle(bucket)?),
                version: 2,
                table_bucket: bucket.storage_bucket_id().as_str().into(),
                catalog_type: "s3vectors".into(),
                warehouse_root: format!("s3://{}/", bucket.storage_bucket_id().as_str()),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                active_rename_id: None,
                created_at: None,
                updated_at: None,
            };
            match self
                .read_table_bucket_with_etag_unlocked(bucket.storage_bucket_id().as_str())
                .await?
            {
                Some((current, _)) => {
                    self.validate_store_bucket_entry(&current)?;
                    if current.state != TableCatalogEntryState::Active
                        || current.catalog_type != parent.catalog_type
                        || current.warehouse_root != parent.warehouse_root
                        || current.active_rename_id.is_some()
                        || !current.properties.is_empty()
                    {
                        return Err(TableCatalogStoreError::Conflict(
                            "Vector parent registration contains incompatible state".into(),
                        ));
                    }
                }
                None => {
                    self.write_entry_unlocked_with_fence(
                        self.catalog_bucket(),
                        &parent_path,
                        &parent,
                        TableCatalogPutPrecondition::IfAbsent,
                        parent_lock.write_commit_guards(),
                        Some(&fence),
                    )
                    .await?
                }
            }
            let namespace = Namespace::parse("indexes").map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
            let namespace_path = self
                .paths
                .namespace_entry_path(bucket.storage_bucket_id().as_str(), &namespace);
            let namespace_lock = self
                .backend
                .acquire_write_lock(self.catalog_bucket(), &namespace_path)
                .await?;
            fence.extend_signals([namespace_lock.lock_lost_signal()]);
            fence.ensure_held()?;
            let entry = NamespaceEntry {
                version: 1,
                table_bucket: bucket.storage_bucket_id().as_str().into(),
                namespace: namespace.public_name(),
                namespace_id: namespace.storage_id(),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            };
            match self
                .read_entry_unlocked::<NamespaceEntry>(self.catalog_bucket(), &namespace_path)
                .await?
            {
                Some((current, _)) => {
                    validate_namespace_entry_object(&self.paths, &namespace_path, &current)?;
                    if current.state != TableCatalogEntryState::Active || !current.properties.is_empty() {
                        return Err(TableCatalogStoreError::Conflict(
                            "Vector indexes namespace contains incompatible state".into(),
                        ));
                    }
                }
                None => {
                    self.write_entry_unlocked_with_fence(
                        self.catalog_bucket(),
                        &namespace_path,
                        &entry,
                        TableCatalogPutPrecondition::IfAbsent,
                        namespace_lock.write_commit_guards(),
                        Some(&fence),
                    )
                    .await?
                }
            }
            self.backend.revalidate_bucket_handle(bucket).await?;
            fence.ensure_held()
        })
        .await
    }
}
