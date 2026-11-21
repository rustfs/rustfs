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
use crate::storage_api::table::{WriteCommitGuard, WriteCompletion};

pub(in crate::table_catalog) mod backup_types;
pub(in crate::table_catalog) mod cleanup;
mod directory_index;
mod manifest;
pub(in crate::table_catalog) mod object;
pub(in crate::table_catalog) mod object_backup;
pub(in crate::table_catalog) use object_backup::{
    OBJECT_CATALOG_BACKUP_MAX_SIZE, OBJECT_CATALOG_BACKUP_REFERENCE_MAX_OBJECTS, ObjectCatalogBackup, ObjectCatalogBackupObject,
    ObjectCatalogBackupReference, ObjectCatalogBackupReferenceKind, append_backup_object, backup_sha256, safe_object_key,
};
mod publication_deadline;
mod reader;
mod registration_cache;
pub(crate) use publication_deadline::TablePublicationPreparation;
pub(in crate::table_catalog) use publication_deadline::publication_preparation;
#[cfg(test)]
pub(crate) use publication_deadline::publication_preparation_timeout;
use publication_deadline::{begin_publication_mutation, publication_recovery, retain_publication_preparation_fence};
pub(crate) use reader::CatalogObjectReader;
#[cfg(test)]
mod registration_cache_tests;
pub(crate) use registration_cache::TableCatalogRegistrationCache;

pub(crate) use backup_types::{TableCatalogBackupObjectPreparationReport, TableCatalogBackupReport, TableCatalogRestoreReport};
#[cfg(test)]
pub(super) use backup_types::{TableCatalogBackupStatus, TableCatalogRestoreStatus};
pub(in crate::table_catalog) use manifest::table_catalog_backing_manifest;
pub(crate) use object::ObjectTableCatalogStore;
#[cfg(test)]
pub(super) use object::bounded_table_entry_objects_for_data_plane_scan;

struct PeriodicTableMaintenanceQueueGuard<'a> {
    table_id: &'a str,
    current_job: Option<&'a TableMaintenanceSchedulerJobSummary>,
}

pub(in crate::table_catalog) fn catalog_lock_acquisition_error(
    operation: &str,
    err: rustfs_lock::LockError,
) -> TableCatalogStoreError {
    let unavailable = matches!(
        &err,
        rustfs_lock::LockError::Timeout { .. }
            | rustfs_lock::LockError::Network { .. }
            | rustfs_lock::LockError::AlreadyLocked { .. }
            | rustfs_lock::LockError::InsufficientNodes { .. }
            | rustfs_lock::LockError::QuorumNotReached { .. }
            | rustfs_lock::LockError::QueueFull { .. }
    );
    let message = format!("failed to {operation}: {err}");
    if unavailable {
        TableCatalogStoreError::LockUnavailable {
            message,
            source: Box::new(err),
        }
    } else {
        TableCatalogStoreError::Internal(message)
    }
}

fn validate_table_bucket_entry(entry: &TableBucketEntry) -> TableCatalogStoreResult<()> {
    validate_catalog_entry_version("table bucket", entry.version)?;
    if entry.vector_bucket_binding.is_some() {
        return Err(TableCatalogStoreError::Invalid("Table scope rejects Vector bucket binding".to_string()));
    }
    if entry.table_bucket.is_empty() {
        return Err(TableCatalogStoreError::Invalid("table bucket name cannot be empty".to_string()));
    }
    if entry.catalog_type != TABLE_BUCKET_CATALOG_TYPE {
        return Err(TableCatalogStoreError::Invalid("unsupported table bucket catalog type".to_string()));
    }
    if entry.active_rename_id.as_ref().is_some_and(String::is_empty) {
        return Err(TableCatalogStoreError::Invalid("active table rename id cannot be empty".to_string()));
    }
    Ok(())
}

fn validate_table_entry_version_and_id(entry: &TableEntry) -> TableCatalogStoreResult<()> {
    validate_catalog_entry_version("table", entry.version)?;
    if entry.vector_head_binding.is_some()
        || entry.vector_index_definition.is_some()
        || !entry.format.eq_ignore_ascii_case("iceberg")
    {
        return Err(TableCatalogStoreError::Invalid(
            "Table scope requires Iceberg without Vector binding".to_string(),
        ));
    }
    if entry.table_id.is_empty() {
        return Err(TableCatalogStoreError::Invalid("table id cannot be empty".to_string()));
    }
    Ok(())
}

fn validate_view_entry_version_and_id(entry: &ViewEntry) -> TableCatalogStoreResult<()> {
    validate_catalog_entry_version("view", entry.version)?;
    if entry.view_id.is_empty() {
        return Err(TableCatalogStoreError::Invalid("view id cannot be empty".to_string()));
    }
    Ok(())
}

fn validate_namespace_entry_identity(entry: &NamespaceEntry) -> TableCatalogStoreResult<Namespace> {
    validate_catalog_entry_version("namespace", entry.version)?;
    let namespace = parse_namespace_for_store(&entry.namespace)?;
    if entry.namespace_id != namespace.storage_id() {
        return Err(TableCatalogStoreError::Invalid(
            "catalog namespace entry storage identity does not match its namespace".to_string(),
        ));
    }
    Ok(namespace)
}

fn direct_namespace_children(
    table_bucket: &str,
    parent: Option<&Namespace>,
    entries: Vec<NamespaceEntry>,
) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
    let parent_depth = parent.map_or(0, |parent| parent.segments().len());
    let mut children = BTreeMap::new();
    for entry in entries {
        if entry.table_bucket != table_bucket {
            return Err(TableCatalogStoreError::Invalid(
                "catalog namespace entry belongs to a different table bucket".to_string(),
            ));
        }
        let namespace = validate_namespace_entry_identity(&entry)?;
        if entry.state != TableCatalogEntryState::Active
            || parent.is_some_and(|parent| !namespace.segments().starts_with(parent.segments()))
            || namespace.segments().len() <= parent_depth
        {
            continue;
        }
        let child = Namespace::from_segments(
            namespace.segments()[..=parent_depth]
                .iter()
                .map(|segment| segment.as_str().to_string())
                .collect(),
        )
        .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid catalog namespace child: {err}")))?;
        let child_name = child.public_name();
        if namespace == child {
            children.insert(child_name, entry);
        } else {
            children
                .entry(child_name)
                .or_insert_with(|| synthetic_namespace_entry(table_bucket, &child));
        }
    }
    Ok(children.into_values().collect())
}

#[async_trait::async_trait]
pub(crate) trait TableCatalogStore: Send + Sync {
    async fn get_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<TableBucketEntry>>;

    async fn put_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()>;

    async fn ensure_table_bucket(&self, _entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        Err(TableCatalogStoreError::Unsupported(
            "table bucket initialization is not supported by this catalog store".to_string(),
        ))
    }

    /// Persists an inactive entry only when no namespace or retained resource exists.
    /// The caller holds the bucket publication fence until its metadata marker is cleared.
    async fn disable_empty_table_bucket(&self, _entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        Err(TableCatalogStoreError::Unsupported(
            "table bucket disablement is not supported by this catalog store".to_string(),
        ))
    }

    async fn create_namespace(&self, entry: NamespaceEntry) -> TableCatalogStoreResult<()>;

    async fn list_namespaces(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>>;

    async fn list_namespaces_under(&self, table_bucket: &str, parent: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        let parent = parse_namespace_for_store(parent)?.public_name();
        Ok(self
            .list_namespaces(table_bucket)
            .await?
            .into_iter()
            .filter(|entry| entry.namespace == parent || namespace_is_descendant(&entry.namespace, &parent))
            .collect())
    }

    async fn list_namespace_children(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
    ) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
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
        let entries = match parent.as_ref() {
            Some(parent) => self.list_namespaces_under(table_bucket, &parent.public_name()).await?,
            None => self.list_namespaces(table_bucket).await?,
        };
        direct_namespace_children(table_bucket, parent.as_ref(), entries)
    }

    async fn list_namespace_children_page(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        Ok(catalog_list_page_from_entries(
            self.list_namespace_children(table_bucket, parent).await?,
            cursor,
            limit,
            |entry| &entry.namespace,
        ))
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    async fn list_namespaces_page(
        &self,
        table_bucket: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        Ok(catalog_list_page_from_entries(
            self.list_namespaces(table_bucket).await?,
            cursor,
            limit,
            |entry| &entry.namespace,
        ))
    }

    async fn get_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Option<NamespaceEntry>>;

    async fn update_namespace_properties(
        &self,
        _table_bucket: &str,
        _namespace: &str,
        _update: NamespacePropertiesUpdate,
    ) -> TableCatalogStoreResult<NamespacePropertiesUpdateResult> {
        Err(TableCatalogStoreError::Unsupported(
            "namespace property updates are not supported by this catalog store".to_string(),
        ))
    }

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<()>;

    #[allow(
        dead_code,
        reason = "declared trait method: implementors provide it but no caller dispatches through the trait yet (backlog#1823)"
    )]
    async fn create_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()>;

    #[allow(
        dead_code,
        reason = "declared trait method: implementors provide it but no caller dispatches through the trait yet (backlog#1823)"
    )]
    async fn register_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()>;

    async fn register_table_with_publication(
        &self,
        entry: TableEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()>;

    async fn list_tables(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<TableEntry>>;

    async fn list_all_tables(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableEntry>>;

    /// Checks whether an active table already owns any part of the candidate warehouse location.
    ///
    /// This is a preflight for clients that write data before catalog registration. Registration
    /// remains the authoritative atomic check for concurrent creators.
    async fn ensure_table_warehouse_location_available(&self, candidate: &TableEntry) -> TableCatalogStoreResult<()> {
        let candidate_prefix = table_warehouse_object_prefix(candidate)?;
        for existing in self.list_all_tables(&candidate.table_bucket).await? {
            if existing.state != TableCatalogEntryState::Active || existing.table_id == candidate.table_id {
                continue;
            }
            let existing_prefix = table_warehouse_object_prefix(&existing)?;
            if warehouse_object_prefixes_overlap(&existing_prefix, &candidate_prefix) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table warehouse location overlaps an active table: {candidate_prefix}"
                )));
            }
        }
        Ok(())
    }

    async fn list_tables_page(
        &self,
        table_bucket: &str,
        namespace: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<TableEntry>> {
        Ok(catalog_list_page_from_entries(
            self.list_tables(table_bucket, namespace).await?,
            cursor,
            limit,
            |entry| &entry.table,
        ))
    }

    async fn load_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<Option<TableEntry>>;

    async fn rename_table(
        &self,
        table_bucket: &str,
        source_namespace: &str,
        source_table: &str,
        destination_namespace: &str,
        destination_table: &str,
    ) -> TableCatalogStoreResult<()> {
        let _ = (table_bucket, source_namespace, source_table, destination_namespace, destination_table);
        Err(TableCatalogStoreError::Unsupported(
            "table rename is not supported by this catalog store".to_string(),
        ))
    }

    async fn resolve_table_data_plane_resource(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        scan_table_data_plane_resource_for_object(self, table_bucket, object).await
    }

    async fn resolve_table_metadata_data_plane_resource(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        if table_bucket.is_empty() || table_identity_from_metadata_object_key(object).is_none() {
            return Ok(None);
        }
        let Some(table_bucket_entry) = self.get_table_bucket(table_bucket).await? else {
            return Ok(None);
        };
        if table_bucket_entry.state != TableCatalogEntryState::Active {
            return Ok(None);
        }
        let entries = self.list_all_tables(table_bucket).await?;
        table_metadata_data_plane_resource_from_entries(&entries, table_bucket, object)
    }

    async fn commit_prepared(&self, prepared: PreparedCatalogCommit) -> TableCatalogStoreResult<TableCommitResult>;

    async fn commit_prepared_with_publication(
        &self,
        prepared: PreparedCatalogCommit,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult>;

    async fn drop_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()>;

    async fn create_view(&self, entry: ViewEntry) -> TableCatalogStoreResult<()>;

    async fn create_view_with_publication(
        &self,
        entry: ViewEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        publication.begin_table_bucket(&entry.table_bucket).await?;
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "view creation requires a table-bucket publication fence".to_string(),
            ));
        }
        publication
            .prepare(&entry.table_bucket, &entry.namespace, &entry.view)
            .await?;
        if !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.view) {
            return Err(TableCatalogStoreError::Internal(
                "view creation requires a view publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        self.create_view(entry).await
    }

    async fn list_views(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<ViewEntry>>;

    async fn list_views_page(
        &self,
        table_bucket: &str,
        namespace: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<ViewEntry>> {
        Ok(catalog_list_page_from_entries(
            self.list_views(table_bucket, namespace).await?,
            cursor,
            limit,
            |entry| &entry.view,
        ))
    }

    async fn load_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<Option<ViewEntry>>;

    async fn drop_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<()>;

    async fn get_commit_by_id(
        &self,
        table_bucket: &str,
        table_id: &str,
        commit_id: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>>;

    async fn get_commit_by_idempotency_key(
        &self,
        table_bucket: &str,
        table_id: &str,
        idempotency_key: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>>;
}

#[async_trait::async_trait]
pub(crate) trait TableCommitPublication: Send + Sync {
    fn prepared_object_reader(&self) -> TableCatalogStoreResult<&dyn CatalogObjectReader> {
        Err(TableCatalogStoreError::Unsupported(
            "prepared catalog object reader is unavailable".into(),
        ))
    }

    fn preparation_budget(&self) -> Arc<TablePublicationPreparation> {
        Arc::new(TablePublicationPreparation::default())
    }

    async fn begin_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<()>;

    async fn prepare(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()>;

    fn holds_table_bucket(&self, table_bucket: &str) -> bool;

    fn holds_table(&self, table_bucket: &str, namespace: &str, table: &str) -> bool;

    fn catalog_migration_read_permit_status(&self) -> Option<bool> {
        None
    }

    /// Storage-level lease signals, available only from publications that retain their guards.
    fn object_mutation_fence(&self) -> Option<TableCatalogObjectMutationFence> {
        None
    }

    fn complete(&self);
}

pub(crate) struct TableCommitPublicationCompletion<'a> {
    publication: &'a (dyn TableCommitPublication + Sync),
}

impl<'a> TableCommitPublicationCompletion<'a> {
    pub(crate) fn new(publication: &'a (dyn TableCommitPublication + Sync)) -> Self {
        Self { publication }
    }
}

impl Drop for TableCommitPublicationCompletion<'_> {
    fn drop(&mut self) {
        self.publication.complete();
    }
}

pub(in crate::table_catalog) struct TableCommitLockPublication<'a, B> {
    paths: TableCatalogObjectPaths,
    backend: &'a B,
    preparation: Arc<TablePublicationPreparation>,
    state: parking_lot::Mutex<TableCommitLockPublicationState>,
}

#[derive(Default)]
struct TableCommitLockPublicationState {
    prepared: bool,
    table_bucket: Option<String>,
    table: Option<(String, String, String)>,
    migration_read_guard: Option<Arc<parking_lot::Mutex<TableCatalogLockGuard>>>,
    guards: Vec<Arc<parking_lot::Mutex<TableCatalogLockGuard>>>,
}

impl<'a, B> TableCommitLockPublication<'a, B> {
    pub(in crate::table_catalog) fn new(backend: &'a B, paths: &TableCatalogObjectPaths) -> Self {
        Self {
            paths: paths.clone(),
            backend,
            preparation: Arc::new(TablePublicationPreparation::default()),
            state: parking_lot::Mutex::new(TableCommitLockPublicationState::default()),
        }
    }

    fn attach_catalog_migration_read_guard(&self, guard: TableCatalogLockGuard) -> TableCatalogStoreResult<()> {
        if guard.is_lock_lost() {
            return Err(TableCatalogStoreError::Conflict(
                "table-bucket catalog migration read permit was lost".to_string(),
            ));
        }
        let mut state = self.state.lock();
        if state.migration_read_guard.is_some() || state.table_bucket.is_some() || state.table.is_some() {
            return Err(TableCatalogStoreError::Internal(
                "catalog migration read permit must be attached before publication".to_string(),
            ));
        }
        state.migration_read_guard = Some(Arc::new(parking_lot::Mutex::new(guard)));
        Ok(())
    }

    fn attach_maintenance_bucket_read_guard(
        &self,
        table_bucket: &str,
        guard: TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<()> {
        if guard.is_lock_lost() {
            return Err(TableCatalogStoreError::Conflict("table maintenance bucket permit was lost".to_string()));
        }
        let mut state = self.state.lock();
        if state.table_bucket.is_some() || state.table.is_some() {
            return Err(TableCatalogStoreError::Internal(
                "table maintenance bucket permit must be attached before table publication".to_string(),
            ));
        }
        state.table_bucket = Some(table_bucket.to_string());
        state.guards.push(Arc::new(parking_lot::Mutex::new(guard)));
        Ok(())
    }

    pub(in crate::table_catalog) fn attach_catalog_write_guard(
        &self,
        guard: TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<()> {
        if guard.is_lock_lost() {
            return Err(TableCatalogStoreError::Conflict("table catalog write lock was lost".to_string()));
        }
        let mut state = self.state.lock();
        if state.table_bucket.is_none() || state.table.is_some() {
            return Err(TableCatalogStoreError::Internal(
                "catalog write guard must be attached between bucket and table publication locks".to_string(),
            ));
        }
        state.guards.push(Arc::new(parking_lot::Mutex::new(guard)));
        Ok(())
    }
}

#[async_trait::async_trait]
impl<'a, B> TableCommitPublication for TableCommitLockPublication<'a, B>
where
    B: TableCatalogObjectBackend,
{
    fn prepared_object_reader(&self) -> TableCatalogStoreResult<&dyn CatalogObjectReader> {
        self.reader_lease(None)?;
        Ok(self)
    }

    fn preparation_budget(&self) -> Arc<TablePublicationPreparation> {
        self.preparation.clone()
    }

    async fn begin_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        // Includes an earlier acquisition even when a caller prepares before entering the store.
        let _ = self.preparation.deadline();
        {
            let mut state = self.state.lock();
            if state
                .migration_read_guard
                .as_ref()
                .is_some_and(|guard| guard.lock().is_lock_lost())
            {
                return Err(TableCatalogStoreError::Conflict(
                    "table-bucket catalog migration read permit was lost".to_string(),
                ));
            }
            if state.table_bucket.as_deref() == Some(table_bucket) {
                return Ok(());
            }
            if state.table_bucket.is_some() || state.table.is_some() {
                return Err(TableCatalogStoreError::Internal(
                    "table-bucket publication lock is already held for another table bucket".to_string(),
                ));
            }
            state.table_bucket = Some(table_bucket.to_string());
        }
        let (lock_bucket, publication_lock) = match self.paths.scope {
            CatalogScope::Table => (table_bucket, default_table_bucket_publication_lock_path()),
            CatalogScope::Vector => (
                RUSTFS_META_BUCKET,
                format!("{}publication.lock", self.paths.table_bucket_root_prefix(table_bucket)),
            ),
        };
        let guard = match self.backend.acquire_write_lock(lock_bucket, &publication_lock).await {
            Ok(guard) => guard,
            Err(err) => {
                self.state.lock().table_bucket = None;
                return Err(err);
            }
        };
        self.state.lock().guards.push(Arc::new(parking_lot::Mutex::new(guard)));
        Ok(())
    }

    async fn prepare(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = self.paths.parse_name(table)?;
        let table_key = (table_bucket.to_string(), namespace.public_name(), table.as_str().to_string());
        {
            let mut state = self.state.lock();
            if state.prepared && state.table.as_ref() == Some(&table_key) {
                return Ok(());
            }
            if state.table.is_some() {
                return Err(TableCatalogStoreError::Internal(
                    "table publication lock is already held for another table".to_string(),
                ));
            }
            state.table = Some(table_key);
        }
        let (lock_bucket, publication_lock) = match self.paths.scope {
            CatalogScope::Table => (
                table_bucket,
                default_table_publication_lock_path(&namespace, &parse_table_for_store(table.as_str())?),
            ),
            CatalogScope::Vector => (
                RUSTFS_META_BUCKET,
                format!("{}.publication.lock", self.paths.table_entry_path(table_bucket, &namespace, &table)),
            ),
        };
        let guard = match self.backend.acquire_write_lock(lock_bucket, &publication_lock).await {
            Ok(guard) => guard,
            Err(err) => {
                self.state.lock().table = None;
                return Err(err);
            }
        };
        let mut state = self.state.lock();
        state.guards.push(Arc::new(parking_lot::Mutex::new(guard)));
        state.prepared = true;
        Ok(())
    }

    fn holds_table_bucket(&self, table_bucket: &str) -> bool {
        let state = self.state.lock();
        state.table_bucket.as_deref() == Some(table_bucket)
            && state
                .migration_read_guard
                .as_ref()
                .is_none_or(|guard| !guard.lock().is_lock_lost())
            && state.guards.iter().all(|guard| !guard.lock().is_lock_lost())
    }

    fn holds_table(&self, table_bucket: &str, namespace: &str, table: &str) -> bool {
        let state = self.state.lock();
        state.prepared
            && state
                .table
                .as_ref()
                .is_some_and(|held| held.0 == table_bucket && held.1 == namespace && held.2 == table)
            && state
                .migration_read_guard
                .as_ref()
                .is_none_or(|guard| !guard.lock().is_lock_lost())
            && state.guards.iter().all(|guard| !guard.lock().is_lock_lost())
    }

    fn catalog_migration_read_permit_status(&self) -> Option<bool> {
        self.state
            .lock()
            .migration_read_guard
            .as_ref()
            .map(|guard| !guard.lock().is_lock_lost())
    }

    fn object_mutation_fence(&self) -> Option<TableCatalogObjectMutationFence> {
        let state = self.state.lock();
        Some(TableCatalogObjectMutationFence::from_signals(
            state
                .migration_read_guard
                .iter()
                .chain(state.guards.iter())
                .map(|guard| guard.lock().lock_lost_signal()),
        ))
    }

    fn complete(&self) {
        *self.state.lock() = TableCommitLockPublicationState::default();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObject {
    pub data: Vec<u8>,
    /// Server-generated identity of the body returned by this read.
    pub body_revision: Option<Uuid>,
    pub etag: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObjectMetadata {
    pub body_revision: Option<Uuid>,
    pub etag: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
    pub size: u64,
}

pub(crate) struct TableCatalogLockGuard {
    _guard: Box<dyn Send>,
    lifecycle_guard: Option<Arc<BucketIncarnationFenceGuard>>,
    lock_lost: Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
    write_commit_guard: Option<WriteCommitGuard>,
}

impl TableCatalogLockGuard {
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) fn stable(guard: impl Send + 'static) -> Self {
        Self {
            _guard: Box::new(guard),
            lifecycle_guard: None,
            lock_lost: None,
            write_commit_guard: None,
        }
    }

    fn namespace(guard: rustfs_lock::NamespaceLockGuard) -> Self {
        let lock_lost = guard.lock_lost_signal();
        Self {
            _guard: Box::new(guard),
            lifecycle_guard: None,
            lock_lost,
            write_commit_guard: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_namespace(guard: rustfs_lock::NamespaceLockGuard) -> Self {
        Self::namespace(guard)
    }

    fn write_namespace(guard: WriteCommitGuard) -> Self {
        let lock_lost = guard.lock_lost_signal();
        Self {
            _guard: Box::new(()),
            lifecycle_guard: None,
            lock_lost,
            write_commit_guard: Some(guard),
        }
    }

    pub(in crate::table_catalog) fn write_commit_guards(&self) -> Vec<WriteCommitGuard> {
        self.write_commit_guard.iter().cloned().collect()
    }

    pub(crate) fn bucket_lifecycle_guard(&self) -> TableCatalogStoreResult<&BucketIncarnationFenceGuard> {
        self.lifecycle_guard.as_deref().ok_or_else(|| {
            TableCatalogStoreError::Unsupported("managed candidate read requires a bucket lifecycle guard".to_string())
        })
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        self.lock_lost.as_ref().is_some_and(|signal| signal.is_lost())
            || self.write_commit_guard.as_ref().is_some_and(WriteCommitGuard::is_lock_lost)
    }

    pub(crate) fn lock_lost_signal(&self) -> Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>> {
        self.lock_lost.clone()
    }
}

#[derive(Clone, Default)]
pub(crate) struct TableCatalogObjectMutationFence {
    signals: Vec<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
}

impl TableCatalogObjectMutationFence {
    pub(crate) fn from_signals(
        signals: impl IntoIterator<Item = Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>>,
    ) -> Self {
        Self {
            signals: signals.into_iter().flatten().collect(),
        }
    }

    fn extend_signals(&mut self, signals: impl IntoIterator<Item = Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>>) {
        self.signals.extend(signals.into_iter().flatten());
    }

    pub(crate) fn ensure_held(&self) -> TableCatalogStoreResult<()> {
        if self.signals.iter().any(|signal| signal.is_lost()) {
            return Err(TableCatalogStoreError::Unavailable(
                "catalog backup or restore lost its fencing lock".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn add_to_object_options(&self, options: &mut ObjectOptions) {
        for signal in &self.signals {
            options.add_namespace_lock_lost_signal(Arc::clone(signal));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObjectListPage {
    pub objects: Vec<String>,
    pub is_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogCleanupObject {
    pub key: String,
    pub version_id: Option<String>,
    pub etag: Option<String>,
    pub delete_marker: bool,
}

pub(crate) struct TableCatalogCleanupPage {
    pub objects: Vec<TableCatalogCleanupObject>,
    pub is_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableCatalogPutPrecondition {
    Any,
    IfAbsent,
    IfMatch(String),
}

pub(in crate::table_catalog) fn catalog_list_next_continuation(
    seen: &mut BTreeSet<String>,
    is_truncated: bool,
    next: Option<String>,
) -> TableCatalogStoreResult<Option<String>> {
    if !is_truncated {
        return Ok(None);
    }
    let next = next.filter(|next| !next.is_empty()).ok_or_else(|| {
        TableCatalogStoreError::Internal("truncated catalog object listing has no continuation token".to_string())
    })?;
    if !seen.insert(next.clone()) {
        return Err(TableCatalogStoreError::Internal(
            "catalog object listing continuation token did not advance".to_string(),
        ));
    }
    Ok(Some(next))
}

#[async_trait::async_trait]
pub(crate) trait TableCatalogObjectBackend: Clone + Send + Sync + 'static {
    fn prepared_catalog_object_reader(&self) -> TableCatalogStoreResult<&dyn CatalogObjectReader> {
        Err(TableCatalogStoreError::Unsupported(
            "prepared catalog object reader is unavailable".into(),
        ))
    }

    async fn read_table_bucket_incarnation(&self, _bucket: &str) -> TableCatalogStoreResult<Uuid> {
        Err(TableCatalogStoreError::Unsupported(
            "table bucket identity validation is unavailable".to_string(),
        ))
    }
    async fn revalidate_bucket_handle(&self, _expected: &BucketHandle) -> TableCatalogStoreResult<()> {
        Err(TableCatalogStoreError::Unsupported(
            "bucket identity validation is unavailable".to_string(),
        ))
    }

    async fn acquire_bucket_lifecycle_read(&self, _expected: &BucketHandle) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        Err(TableCatalogStoreError::Unsupported(
            "bucket lifecycle validation is unavailable".to_string(),
        ))
    }
    async fn reuse_bucket_lifecycle_read(
        &self,
        _expected: &BucketHandle,
        _guard: Arc<BucketIncarnationFenceGuard>,
    ) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        Err(TableCatalogStoreError::Unsupported(
            "shared bucket lifecycle validation is unavailable".into(),
        ))
    }
    async fn read_managed_candidate(
        &self,
        _bucket: &BucketHandle,
        _metadata: &CatalogMetadataRef,
        _max_size: usize,
        _lifecycle: &mut TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        Err(TableCatalogStoreError::Unsupported(
            "managed candidate reading is unavailable".to_string(),
        ))
    }

    fn registration_cache(&self) -> Option<TableCatalogRegistrationCache> {
        None
    }

    fn acquires_catalog_migration_read_permit(&self) -> bool {
        false
    }

    async fn acquire_catalog_migration_read_guards(
        &self,
        _table_bucket: &str,
    ) -> TableCatalogStoreResult<Vec<TableCatalogLockGuard>> {
        Ok(Vec::new())
    }

    async fn read_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>>;

    async fn read_object_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let result = self.read_object(bucket, object).await?;
        if result.as_ref().is_some_and(|object| object.data.len() > max_size) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog object {bucket}/{object} exceeds the maximum size of {max_size} bytes"
            )));
        }
        Ok(result)
    }

    async fn read_object_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.read_object(bucket, object).await
    }

    async fn read_object_unlocked_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let result = self.read_object_unlocked(bucket, object).await?;
        if result.as_ref().is_some_and(|object| object.data.len() > max_size) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog object {bucket}/{object} exceeds the maximum size of {max_size} bytes"
            )));
        }
        Ok(result)
    }

    async fn object_metadata(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObjectMetadata>> {
        Ok(self
            .read_object(bucket, object)
            .await?
            .map(|object| TableCatalogObjectMetadata {
                body_revision: object.body_revision,
                etag: object.etag,
                mod_time: object.mod_time,
                size: u64::try_from(object.data.len()).unwrap_or(u64::MAX),
            }))
    }

    async fn object_metadata_unlocked(
        &self,
        bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableCatalogObjectMetadata>> {
        Ok(self
            .read_object_unlocked(bucket, object)
            .await?
            .map(|object| TableCatalogObjectMetadata {
                body_revision: object.body_revision,
                etag: object.etag,
                mod_time: object.mod_time,
                size: u64::try_from(object.data.len()).unwrap_or(u64::MAX),
            }))
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool>;

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    async fn object_exists_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool> {
        self.object_exists(bucket, object).await
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()>;

    /// Write an object that precedes the catalog transaction, such as the
    /// initial metadata file of a create. It is authorized and fenced like
    /// `put_object`, but a publication-aware backend must not treat it as the
    /// first durable catalog mutation, so the preparation deadline keeps
    /// bounding the work that follows. The caller removes the object when the
    /// transaction fails before any catalog write may have begun; nothing else
    /// references it, since its root belongs to no table.
    async fn put_object_before_publication(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        self.put_object(bucket, object, data, precondition).await
    }

    async fn put_object_unlocked(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        _write_guards: Vec<WriteCommitGuard>,
    ) -> TableCatalogStoreResult<()> {
        self.put_object(bucket, object, data, precondition).await
    }

    async fn put_object_fenced(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        self.put_object(bucket, object, data, precondition).await?;
        fence.ensure_held()
    }

    async fn put_object_unlocked_fenced(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        _write_guards: Vec<WriteCommitGuard>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        self.put_object_fenced(bucket, object, data, precondition, fence).await
    }

    async fn delete_object_unlocked_fenced(
        &self,
        bucket: &str,
        object: &str,
        _write_guards: Vec<WriteCommitGuard>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        self.delete_object_unlocked(bucket, object).await?;
        fence.ensure_held()
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()>;

    async fn delete_object_if_match(&self, bucket: &str, object: &str, expected_etag: &str) -> TableCatalogStoreResult<()>;

    async fn delete_object_if_match_fenced(
        &self,
        bucket: &str,
        object: &str,
        expected_etag: &str,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        self.delete_object_if_match(bucket, object, expected_etag).await?;
        fence.ensure_held()
    }

    async fn delete_object_if_match_fenced_for_incarnation(
        &self,
        _bucket: &str,
        _object: &str,
        _expected_etag: &str,
        _expected_incarnation_id: Uuid,
        _fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        Err(TableCatalogStoreError::Unsupported(
            "incarnation-checked cleanup is not supported by this backend".to_string(),
        ))
    }

    async fn delete_object_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
        self.delete_object(bucket, object).await
    }

    async fn list_objects(&self, bucket: &str, prefix: &str) -> TableCatalogStoreResult<Vec<String>>;

    async fn list_objects_for_cleanup(
        &self,
        _bucket: &str,
        _prefix: &str,
    ) -> TableCatalogStoreResult<Vec<TableCatalogCleanupObject>> {
        Err(TableCatalogStoreError::Unsupported(
            "authoritative cleanup listing is not supported by this backend".to_string(),
        ))
    }

    async fn ensure_no_multipart_uploads(&self, _bucket: &str) -> TableCatalogStoreResult<()> {
        Err(TableCatalogStoreError::Unsupported(
            "multipart cleanup validation is not supported by this backend".to_string(),
        ))
    }

    /// GET-equivalent table ownership entries, selected before metadata reads.
    /// Report history must not consume pages. No ordinary S3 list fallback is safe.
    async fn list_table_entry_objects_page(
        &self,
        _bucket: &str,
        _prefix: &str,
        _start_after: Option<&str>,
        _limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogObjectListPage> {
        Err(TableCatalogStoreError::Unsupported(
            "authoritative table-entry pagination is not supported by this backend".to_string(),
        ))
    }

    /// GET-equivalent, bounded enumeration for publishing authoritative indexes.
    /// Backends without this contract must not fall back to an ordinary S3 list.
    async fn list_catalog_objects_page(
        &self,
        _bucket: &str,
        _prefix: &str,
        _start_after: Option<&str>,
        _limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogCleanupPage> {
        Err(TableCatalogStoreError::Unsupported(
            "authoritative catalog pagination is not supported by this backend".to_string(),
        ))
    }

    async fn list_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogObjectListPage> {
        let mut objects = self.list_objects(bucket, prefix).await?;
        objects.sort();
        let start = start_after.map_or(0, |cursor| objects.partition_point(|object| object.as_str() <= cursor));
        let mut objects = objects
            .into_iter()
            .skip(start)
            .take(limit.get().saturating_add(1))
            .collect::<Vec<_>>();
        let is_truncated = objects.len() > limit.get();
        objects.truncate(limit.get());
        Ok(TableCatalogObjectListPage { objects, is_truncated })
    }

    async fn acquire_read_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        self.acquire_write_lock(bucket, object).await
    }

    async fn acquire_write_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<TableCatalogLockGuard>;

    /// Authorize adopting a caller-selected warehouse prefix. Request adapters
    /// enforce the caller's policy; trusted storage backends have no IAM identity.
    async fn authorize_table_warehouse_location(&self, _table_bucket: &str, _location: &str) -> TableCatalogStoreResult<()> {
        Ok(())
    }

    fn table_publication_preparation_budget(&self) -> Arc<TablePublicationPreparation> {
        Arc::new(TablePublicationPreparation::default())
    }

    async fn begin_table_bucket_commit_publication(&self, _table_bucket: &str) -> TableCatalogStoreResult<()> {
        Ok(())
    }

    fn table_bucket_commit_publication_is_held(&self, _table_bucket: &str) -> bool {
        false
    }

    async fn prepare_table_commit_publication(
        &self,
        _table_bucket: &str,
        _namespace: &str,
        _table: &str,
    ) -> TableCatalogStoreResult<()> {
        Ok(())
    }

    fn table_commit_publication_is_held(&self, _table_bucket: &str, _namespace: &str, _table: &str) -> bool {
        false
    }

    fn catalog_migration_read_permit_status(&self) -> Option<bool> {
        None
    }

    fn table_commit_object_mutation_fence(&self) -> Option<TableCatalogObjectMutationFence> {
        None
    }

    fn complete_table_commit_publication(&self) {}
}

#[async_trait::async_trait]
impl<B> TableCommitPublication for B
where
    B: TableCatalogObjectBackend,
{
    fn prepared_object_reader(&self) -> TableCatalogStoreResult<&dyn CatalogObjectReader> {
        self.prepared_catalog_object_reader()
    }

    fn preparation_budget(&self) -> Arc<TablePublicationPreparation> {
        self.table_publication_preparation_budget()
    }

    async fn begin_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        self.begin_table_bucket_commit_publication(table_bucket).await
    }

    async fn prepare(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()> {
        self.prepare_table_commit_publication(table_bucket, namespace, table).await
    }

    fn holds_table_bucket(&self, table_bucket: &str) -> bool {
        self.table_bucket_commit_publication_is_held(table_bucket)
    }

    fn holds_table(&self, table_bucket: &str, namespace: &str, table: &str) -> bool {
        self.table_commit_publication_is_held(table_bucket, namespace, table)
    }

    fn catalog_migration_read_permit_status(&self) -> Option<bool> {
        TableCatalogObjectBackend::catalog_migration_read_permit_status(self)
    }

    fn object_mutation_fence(&self) -> Option<TableCatalogObjectMutationFence> {
        self.table_commit_object_mutation_fence()
    }

    fn complete(&self) {
        self.complete_table_commit_publication();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::table_catalog) enum CatalogScope {
    Table,
    Vector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObjectPaths {
    scope: CatalogScope,
}

impl Default for TableCatalogObjectPaths {
    fn default() -> Self {
        Self {
            scope: CatalogScope::Table,
        }
    }
}

impl TableCatalogObjectPaths {
    pub(in crate::table_catalog) fn for_vector() -> Self {
        Self {
            scope: CatalogScope::Vector,
        }
    }

    fn catalog_root(&self) -> &'static str {
        match self.scope {
            CatalogScope::Table => INTERNAL_CATALOG_ROOT,
            CatalogScope::Vector => "s3vectors/catalog",
        }
    }

    pub(in crate::table_catalog) fn parse_name(&self, name: &str) -> TableCatalogStoreResult<CatalogName> {
        match self.scope {
            CatalogScope::Table => IdentifierSegment::parse(name).map(|name| CatalogName::from(&name)),
            CatalogScope::Vector => CatalogName::parse(name),
        }
        .map_err(|err| TableCatalogStoreError::Invalid(err.to_string()))
    }
    fn validate_bucket_entry(&self, entry: &TableBucketEntry) -> TableCatalogStoreResult<()> {
        if self.scope == CatalogScope::Table {
            return validate_table_bucket_entry(entry);
        }
        let binding = entry
            .vector_bucket_binding
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Vector bucket binding is required".to_string()))?;
        let handle = binding.to_handle()?;
        if entry.version != 2
            || entry.catalog_type != "s3vectors"
            || entry.table_bucket != handle.storage_bucket_id().as_str()
            || entry.active_rename_id.is_some()
        {
            return Err(TableCatalogStoreError::Invalid(
                "Vector bucket entry contradicts scope or identity".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_table_entry(&self, entry: &TableEntry) -> TableCatalogStoreResult<()> {
        if self.scope == CatalogScope::Table {
            return validate_table_entry_version_and_id(entry);
        }
        CatalogHead::from_vector_entry(entry).map(|_| ())
    }

    fn validate_commit_entry(&self, entry: &CommitLogEntry, bucket: &str, table_id: &str) -> TableCatalogStoreResult<()> {
        if self.scope == CatalogScope::Table {
            validate_catalog_entry_version("commit", entry.version)?;
            if entry.vector_commit_binding.is_some() || entry.vector_resource_mutation.is_some() {
                return Err(TableCatalogStoreError::Invalid("Table scope rejects Vector commit binding".to_string()));
            }
            return Ok(());
        }
        let binding = entry
            .vector_commit_binding
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Vector commit binding is required".to_string()))?;
        binding.validate_log(entry, bucket, table_id)
    }

    pub(crate) fn vector_gc_lock_path(&self, bucket: &str, table_id: &str) -> String {
        format!(
            "{}resources/{}/gc.lock",
            self.table_bucket_root_prefix(bucket),
            table_catalog_path_hash(table_id)
        )
    }

    pub fn table_bucket_entries_prefix(&self) -> String {
        format!("{}/{}/", self.catalog_root(), TABLE_BUCKET_ROOT)
    }

    pub fn table_bucket_entry_path(&self, table_bucket: &str) -> String {
        format!("{}{}", self.table_bucket_root_prefix(table_bucket), TABLE_BUCKET_ENTRY_FILE)
    }

    pub fn table_bucket_maintenance_config_path(&self, table_bucket: &str) -> String {
        format!(
            "{}{MAINTENANCE_ROOT}/{MAINTENANCE_CONFIG_FILE}",
            self.table_bucket_root_prefix(table_bucket)
        )
    }

    pub fn namespace_entries_prefix(&self, table_bucket: &str) -> String {
        format!("{}{}/", self.table_bucket_root_prefix(table_bucket), NAMESPACE_ROOT)
    }

    pub fn namespace_entry_path(&self, table_bucket: &str, namespace: &Namespace) -> String {
        format!(
            "{}{}/{}",
            self.namespace_entries_prefix(table_bucket),
            namespace.storage_id(),
            NAMESPACE_ENTRY_FILE
        )
    }

    pub fn table_entries_prefix(&self, table_bucket: &str, namespace: &Namespace) -> String {
        format!(
            "{}{}/{}/",
            self.namespace_entries_prefix(table_bucket),
            namespace.storage_id(),
            TABLE_ROOT
        )
    }

    pub fn table_entry_path(&self, table_bucket: &str, namespace: &Namespace, table: &impl AsRef<str>) -> String {
        format!(
            "{}{}/{}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_ref(),
            TABLE_ENTRY_FILE
        )
    }

    pub fn external_catalog_bridge_path(&self, table_bucket: &str, namespace: &Namespace, table: &IdentifierSegment) -> String {
        format!(
            "{}{}/{EXTERNAL_CATALOG_ROOT}/{EXTERNAL_CATALOG_BRIDGE_FILE}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str()
        )
    }

    pub fn view_entries_prefix(&self, table_bucket: &str, namespace: &Namespace) -> String {
        format!("{}{}/{}/", self.namespace_entries_prefix(table_bucket), namespace.storage_id(), VIEW_ROOT)
    }

    pub fn view_entry_path(&self, table_bucket: &str, namespace: &Namespace, view: &IdentifierSegment) -> String {
        format!(
            "{}{}/{}",
            self.view_entries_prefix(table_bucket, namespace),
            view.as_str(),
            VIEW_ENTRY_FILE
        )
    }

    pub fn table_maintenance_config_path(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
    ) -> String {
        format!(
            "{}{}/{MAINTENANCE_ROOT}/{}/{MAINTENANCE_CONFIG_FILE}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            table_catalog_path_hash(table_id)
        )
    }

    /// Returns the table maintenance directory and record name for a canonical catalog path.
    pub(in crate::table_catalog) fn historical_table_maintenance_path<'a>(
        &self,
        table_bucket: &str,
        object: &'a str,
    ) -> Option<(&'a str, &'a str)> {
        let namespace_prefix = self.namespace_entries_prefix(table_bucket);
        let relative = object.strip_prefix(&namespace_prefix)?;
        let (head, tail) = relative.rsplit_once(&format!("/{MAINTENANCE_ROOT}/"))?;
        let (namespace_path, table) = head.rsplit_once(&format!("/{TABLE_ROOT}/"))?;
        if table.contains('/')
            || IdentifierSegment::parse(table).is_err()
            || Namespace::from_segments(namespace_path.split('/').map(str::to_string).collect()).is_err()
        {
            return None;
        }
        let (identity_hash, suffix) = tail.split_once('/')?;
        if identity_hash.len() != 64
            || !identity_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return None;
        }
        let is_known = matches!(
            suffix,
            MAINTENANCE_CONFIG_FILE | MAINTENANCE_CURRENT_JOB_FILE | MAINTENANCE_LATEST_JOB_FILE
        ) || suffix
            .strip_prefix(&format!("{MAINTENANCE_JOB_ROOT}/"))
            .and_then(|name| name.strip_suffix(".json"))
            .is_some_and(|hash| {
                hash.len() == 64
                    && hash
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            });
        is_known.then(|| (&object[..object.len() - suffix.len()], suffix))
    }

    pub fn table_maintenance_job_path(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
        job_id: &str,
    ) -> String {
        format!(
            "{}{}/{MAINTENANCE_ROOT}/{}/{MAINTENANCE_JOB_ROOT}/{}.json",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            table_catalog_path_hash(table_id),
            table_catalog_path_hash(job_id)
        )
    }

    pub fn table_maintenance_jobs_prefix(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
    ) -> String {
        format!(
            "{}{}/{MAINTENANCE_ROOT}/{}/{MAINTENANCE_JOB_ROOT}/",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            table_catalog_path_hash(table_id)
        )
    }

    pub fn table_maintenance_latest_job_path(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
    ) -> String {
        format!(
            "{}{}/{MAINTENANCE_ROOT}/{}/{MAINTENANCE_LATEST_JOB_FILE}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            table_catalog_path_hash(table_id)
        )
    }

    pub fn table_maintenance_current_job_path(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
    ) -> String {
        format!(
            "{}{}/{MAINTENANCE_ROOT}/{}/{MAINTENANCE_CURRENT_JOB_FILE}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
            table_catalog_path_hash(table_id)
        )
    }

    pub fn commit_log_entry_path(&self, table_bucket: &str, table_id: &str, commit_id: &str) -> String {
        format!(
            "{}{}/{}/{}.json",
            self.table_bucket_root_prefix(table_bucket),
            COMMIT_LOG_ROOT,
            table_catalog_path_hash(table_id),
            table_catalog_path_hash(commit_id)
        )
    }

    pub fn commit_log_entries_prefix(&self, table_bucket: &str, table_id: &str) -> String {
        format!(
            "{}{}/{}/",
            self.table_bucket_root_prefix(table_bucket),
            COMMIT_LOG_ROOT,
            table_catalog_path_hash(table_id)
        )
    }

    pub fn commit_idempotency_entry_path(&self, table_bucket: &str, table_id: &str, idempotency_key: &str) -> String {
        format!(
            "{}{}/{}/{}.json",
            self.table_bucket_root_prefix(table_bucket),
            COMMIT_IDEMPOTENCY_ROOT,
            table_catalog_path_hash(table_id),
            table_catalog_path_hash(idempotency_key)
        )
    }

    pub fn commit_idempotency_entries_prefix(&self, table_bucket: &str, table_id: &str) -> String {
        format!(
            "{}{}/{}/",
            self.table_bucket_root_prefix(table_bucket),
            COMMIT_IDEMPOTENCY_ROOT,
            table_catalog_path_hash(table_id)
        )
    }

    pub fn warehouse_index_state_path(&self, table_bucket: &str) -> String {
        format!(
            "{}{}/{}",
            self.table_bucket_root_prefix(table_bucket),
            WAREHOUSE_INDEX_ROOT,
            WAREHOUSE_INDEX_STATE_FILE
        )
    }

    pub fn warehouse_index_entries_prefix(&self, table_bucket: &str) -> String {
        format!("{}{}/", self.table_bucket_root_prefix(table_bucket), WAREHOUSE_INDEX_ROOT)
    }

    pub fn warehouse_index_entry_path(&self, table_bucket: &str, warehouse_object_prefix: &str) -> String {
        format!(
            "{}{}/{}.json",
            self.table_bucket_root_prefix(table_bucket),
            WAREHOUSE_INDEX_ROOT,
            table_catalog_path_hash(warehouse_object_prefix)
        )
    }

    pub fn table_rename_intent_path(&self, table_bucket: &str, rename_id: &str) -> String {
        format!(
            "{}{}/{}.json",
            self.table_bucket_root_prefix(table_bucket),
            TABLE_RENAME_ROOT,
            table_catalog_path_hash(rename_id)
        )
    }

    pub fn catalog_backup_path(&self, table_bucket: &str, backup_id: &str) -> String {
        format!(
            "{}/{}/{}/{}.json",
            self.catalog_root(),
            TABLE_CATALOG_BACKUP_ROOT,
            table_catalog_path_hash(table_bucket),
            table_catalog_path_hash(backup_id)
        )
    }

    pub fn catalog_backup_fence_lock_path(&self, table_bucket: &str) -> String {
        // Coordinate with directory activation, rollback, and writers from before the backing retirement.
        self.backing_migration_fence_lock_path(table_bucket)
    }

    pub fn catalog_restore_state_path(&self, table_bucket: &str) -> String {
        format!(
            "{}/{}/{}/restore-state.json",
            self.catalog_root(),
            TABLE_CATALOG_BACKUP_ROOT,
            table_catalog_path_hash(table_bucket)
        )
    }

    pub fn catalog_restore_intent_path(&self, table_bucket: &str, restore_id: &str) -> String {
        format!(
            "{}/{}/{}/restores/{}.json",
            self.catalog_root(),
            TABLE_CATALOG_BACKUP_ROOT,
            table_catalog_path_hash(table_bucket),
            table_catalog_path_hash(restore_id)
        )
    }

    pub fn legacy_backing_migration_fence_path(&self, table_bucket: &str) -> String {
        format!(
            "{}{}/{}",
            self.table_bucket_root_prefix(table_bucket),
            "backing-migration",
            "durable-strong-fence.json"
        )
    }

    /// Object used by the optional table-entry directory (and formerly by backing
    /// migration). Kept under the historical backing-migration prefix so directory
    /// state stays compatible with main.
    pub fn backing_migration_fence_path(&self, table_bucket: &str) -> String {
        self.legacy_backing_migration_fence_path(table_bucket)
    }

    pub fn backing_migration_fence_lock_path(&self, table_bucket: &str) -> String {
        format!(
            "{}{}/{}",
            self.table_bucket_root_prefix(table_bucket),
            "backing-migration",
            "durable-strong-fence.lock"
        )
    }

    pub fn legacy_backing_migration_global_fence_path(&self) -> String {
        format!("{}/{}/{}", self.catalog_root(), "backing-migration", "durable-strong-global-fence.json")
    }

    pub fn legacy_catalog_restore_intent_path(&self, table_bucket: &str) -> String {
        format!(
            "{}/{}/{}/intent.json",
            self.catalog_root(),
            "restore-intents",
            table_catalog_path_hash(table_bucket)
        )
    }

    pub fn legacy_catalog_snapshot_path(&self) -> String {
        format!("{}/{}/{}", self.catalog_root(), "strong-backing", "snapshot.json")
    }

    pub fn catalog_backup_global_fence_lock_path(&self) -> String {
        // Older object-backed registry writers use this lock during a rolling upgrade.
        format!("{}/{}/{}", self.catalog_root(), "backing-migration", "durable-strong-global-fence.lock")
    }

    pub(in crate::table_catalog) fn table_bucket_root_prefix(&self, table_bucket: &str) -> String {
        format!("{}{}/", self.table_bucket_entries_prefix(), table_catalog_path_hash(table_bucket))
    }
}

#[derive(Clone)]
pub(crate) enum ConfiguredTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    ObjectBacked(ObjectTableCatalogStore<B>),
}

impl<B> ConfiguredTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub(crate) fn from_env(backend: B) -> TableCatalogStoreResult<Self> {
        Ok(match TableCatalogBackingMode::from_env()? {
            TableCatalogBackingMode::ObjectBacked => Self::ObjectBacked(ObjectTableCatalogStore::new(backend)),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(backend: B, mode: TableCatalogBackingMode) -> Self {
        match mode {
            TableCatalogBackingMode::ObjectBacked => Self::ObjectBacked(ObjectTableCatalogStore::new(backend)),
        }
    }

    pub(crate) async fn resolve_table_data_plane_resource_for_authorization(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .resolve_table_data_plane_resource_for_authorization(table_bucket, object)
                    .await
            }
        }
    }

    pub(crate) async fn get_table_catalog_diagnostics_retention(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<usize> {
        match self {
            Self::ObjectBacked(store) => Ok(store
                .get_table_maintenance_config(table_bucket, namespace, table)
                .await?
                .retain_recent_metadata_files),
        }
    }
}

#[async_trait::async_trait]
impl<B> TableCatalogStore for ConfiguredTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    async fn get_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<TableBucketEntry>> {
        match self {
            Self::ObjectBacked(store) => store.get_table_bucket(table_bucket).await,
        }
    }

    async fn put_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.put_table_bucket(entry).await,
        }
    }

    async fn ensure_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.ensure_table_bucket(entry).await,
        }
    }

    async fn disable_empty_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.disable_empty_table_bucket(entry).await,
        }
    }

    async fn create_namespace(&self, entry: NamespaceEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.create_namespace(entry).await,
        }
    }

    async fn list_namespaces(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_namespaces(table_bucket).await,
        }
    }

    async fn list_namespaces_under(&self, table_bucket: &str, parent: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_namespaces_under(table_bucket, parent).await,
        }
    }

    async fn list_namespace_children(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
    ) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_namespace_children(table_bucket, parent).await,
        }
    }

    async fn list_namespace_children_page(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_namespace_children_page(table_bucket, parent, cursor, limit).await,
        }
    }

    async fn list_namespaces_page(
        &self,
        table_bucket: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_namespaces_page(table_bucket, cursor, limit).await,
        }
    }

    async fn get_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Option<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.get_namespace(table_bucket, namespace).await,
        }
    }

    async fn update_namespace_properties(
        &self,
        table_bucket: &str,
        namespace: &str,
        update: NamespacePropertiesUpdate,
    ) -> TableCatalogStoreResult<NamespacePropertiesUpdateResult> {
        match self {
            Self::ObjectBacked(store) => store.update_namespace_properties(table_bucket, namespace, update).await,
        }
    }

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.drop_namespace(table_bucket, namespace).await,
        }
    }

    async fn create_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.create_table(entry).await,
        }
    }

    async fn register_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.register_table(entry).await,
        }
    }

    async fn register_table_with_publication(
        &self,
        entry: TableEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.register_table_with_publication(entry, publication).await,
        }
    }

    async fn list_tables(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_tables(table_bucket, namespace).await,
        }
    }

    async fn list_all_tables(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_all_tables(table_bucket).await,
        }
    }

    async fn ensure_table_warehouse_location_available(&self, candidate: &TableEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.ensure_table_warehouse_location_available(candidate).await,
        }
    }

    async fn list_tables_page(
        &self,
        table_bucket: &str,
        namespace: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<TableEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_tables_page(table_bucket, namespace, cursor, limit).await,
        }
    }

    async fn load_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<Option<TableEntry>> {
        match self {
            Self::ObjectBacked(store) => store.load_table(table_bucket, namespace, table).await,
        }
    }

    async fn rename_table(
        &self,
        table_bucket: &str,
        source_namespace: &str,
        source_table: &str,
        destination_namespace: &str,
        destination_table: &str,
    ) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .rename_table(table_bucket, source_namespace, source_table, destination_namespace, destination_table)
                    .await
            }
        }
    }

    async fn resolve_table_data_plane_resource(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        match self {
            Self::ObjectBacked(store) => store.resolve_table_data_plane_resource(table_bucket, object).await,
        }
    }

    async fn resolve_table_metadata_data_plane_resource(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        match self {
            Self::ObjectBacked(store) => store.resolve_table_metadata_data_plane_resource(table_bucket, object).await,
        }
    }

    async fn commit_prepared(&self, prepared: PreparedCatalogCommit) -> TableCatalogStoreResult<TableCommitResult> {
        match self {
            Self::ObjectBacked(store) => store.commit_prepared(prepared).await,
        }
    }

    async fn commit_prepared_with_publication(
        &self,
        prepared: PreparedCatalogCommit,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        match self {
            Self::ObjectBacked(store) => store.commit_prepared_with_publication(prepared, publication).await,
        }
    }

    async fn drop_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.drop_table(table_bucket, namespace, table).await,
        }
    }

    async fn create_view(&self, entry: ViewEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.create_view(entry).await,
        }
    }

    async fn create_view_with_publication(
        &self,
        entry: ViewEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.create_view_with_publication(entry, publication).await,
        }
    }

    async fn list_views(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<ViewEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_views(table_bucket, namespace).await,
        }
    }

    async fn list_views_page(
        &self,
        table_bucket: &str,
        namespace: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<ViewEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_views_page(table_bucket, namespace, cursor, limit).await,
        }
    }

    async fn load_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<Option<ViewEntry>> {
        match self {
            Self::ObjectBacked(store) => store.load_view(table_bucket, namespace, view).await,
        }
    }

    async fn drop_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.drop_view(table_bucket, namespace, view).await,
        }
    }

    async fn get_commit_by_id(
        &self,
        table_bucket: &str,
        table_id: &str,
        commit_id: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        match self {
            Self::ObjectBacked(store) => store.get_commit_by_id(table_bucket, table_id, commit_id).await,
        }
    }

    async fn get_commit_by_idempotency_key(
        &self,
        table_bucket: &str,
        table_id: &str,
        idempotency_key: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .get_commit_by_idempotency_key(table_bucket, table_id, idempotency_key)
                    .await
            }
        }
    }
}

impl<B> ConfiguredTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub(crate) async fn get_table_maintenance_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        match self {
            Self::ObjectBacked(store) => store.get_table_maintenance_config(table_bucket, namespace, table).await,
        }
    }

    pub(crate) async fn put_table_maintenance_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableMaintenanceConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .put_table_maintenance_config(table_bucket, namespace, table, config)
                    .await
            }
        }
    }

    pub(crate) async fn get_table_metadata_maintenance_report(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        job_id: &str,
    ) -> TableCatalogStoreResult<Option<TableMetadataMaintenanceReport>> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .get_table_metadata_maintenance_report(table_bucket, namespace, table, job_id)
                    .await
            }
        }
    }

    pub(crate) async fn get_table_maintenance_scheduler_report(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerReport> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .get_table_maintenance_scheduler_report(table_bucket, namespace, table)
                    .await
            }
        }
    }

    pub(crate) async fn run_table_maintenance_scheduler_once(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        scheduler_id: String,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerRunResult> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .run_table_maintenance_scheduler_once(table_bucket, namespace, table, scheduler_id)
                    .await
            }
        }
    }

    pub(crate) async fn queue_periodic_table_maintenance(
        &self,
        entry: &TableEntry,
        scheduler_id: String,
        expected_job: Option<&TableMaintenanceSchedulerJobSummary>,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerRunResult> {
        let guard = PeriodicTableMaintenanceQueueGuard {
            table_id: &entry.table_id,
            current_job: expected_job,
        };
        match self {
            Self::ObjectBacked(store) => {
                store
                    .run_table_maintenance_scheduler_once_with_guard_at(
                        &entry.table_bucket,
                        &entry.namespace,
                        &entry.table,
                        scheduler_id,
                        OffsetDateTime::now_utc(),
                        Some(&guard),
                    )
                    .await
            }
        }
    }

    pub(crate) async fn apply_table_maintenance_quarantine_operation(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        job_id: &str,
        request: TableMaintenanceQuarantineOperationRequest,
    ) -> TableCatalogStoreResult<TableMaintenanceQuarantineOperationResult> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .apply_table_maintenance_quarantine_operation(table_bucket, namespace, table, job_id, request)
                    .await
            }
        }
    }

    pub(crate) async fn run_table_metadata_maintenance_worker_once(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        worker_id: String,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .run_table_metadata_maintenance_worker_once(table_bucket, namespace, table, worker_id)
                    .await
            }
        }
    }

    pub(crate) async fn run_table_metadata_maintenance_worker_once_conditionally(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        worker_id: String,
        condition: &TableMaintenanceWorkerCondition,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .run_table_metadata_maintenance_worker_once_conditionally(
                        table_bucket,
                        namespace,
                        table,
                        worker_id,
                        condition,
                    )
                    .await
            }
        }
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
        match self {
            Self::ObjectBacked(store) => {
                store
                    .heartbeat_table_metadata_maintenance_job(table_bucket, namespace, table, job_id, lease_id, worker_id)
                    .await
            }
        }
    }

    pub(crate) async fn export_table_catalog_entry(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCatalogExport> {
        match self {
            Self::ObjectBacked(store) => store.export_table_catalog_entry(table_bucket, namespace, table).await,
        }
    }

    pub(crate) async fn diagnose_table_catalog(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableCatalogDiagnosticsReport> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .diagnose_table_catalog(table_bucket, namespace, table, retain_recent_metadata_files)
                    .await
            }
        }
    }

    pub(crate) async fn recover_table_commits(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
        match self {
            Self::ObjectBacked(store) => store.recover_table_commits(table_bucket, namespace, table).await,
        }
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) async fn get_external_catalog_bridge(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<Option<ExternalCatalogBridgeEntry>> {
        match self {
            Self::ObjectBacked(store) => store.get_external_catalog_bridge(table_bucket, namespace, table).await,
        }
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) async fn put_external_catalog_bridge(
        &self,
        entry: ExternalCatalogBridgeEntry,
    ) -> TableCatalogStoreResult<ExternalCatalogBridgeEntry> {
        match self {
            Self::ObjectBacked(store) => store.put_external_catalog_bridge(entry).await,
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait CatalogManagedCandidateReader: Send + Sync {
    async fn read_managed_candidate(
        &self,
        bucket: &BucketHandle,
        metadata: &CatalogMetadataRef,
        max_size: usize,
        lifecycle: &BucketIncarnationFenceGuard,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>>;
}

pub(crate) struct EcStoreTableCatalogObjectBackend<S> {
    store: Arc<S>,
    registration_cache: TableCatalogRegistrationCache,
    managed_candidate_reader: Option<Arc<dyn CatalogManagedCandidateReader>>,
}

impl<S> Clone for EcStoreTableCatalogObjectBackend<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            registration_cache: self.registration_cache.clone(),
            managed_candidate_reader: self.managed_candidate_reader.clone(),
        }
    }
}

impl<S> EcStoreTableCatalogObjectBackend<S>
where
    S: TableCatalogStorage,
{
    pub fn new(store: Arc<S>) -> Self {
        Self::new_with_registration_cache(store, TableCatalogRegistrationCache::default())
    }

    pub fn new_with_registration_cache(store: Arc<S>, registration_cache: TableCatalogRegistrationCache) -> Self {
        Self {
            store,
            registration_cache,
            managed_candidate_reader: None,
        }
    }

    pub(crate) fn new_with_managed_candidate_reader(store: Arc<S>, reader: Arc<dyn CatalogManagedCandidateReader>) -> Self {
        Self {
            store,
            registration_cache: TableCatalogRegistrationCache::default(),
            managed_candidate_reader: Some(reader),
        }
    }
}

pub(crate) type EcStoreTableCatalogStore<S> = ConfiguredTableCatalogStore<EcStoreTableCatalogObjectBackend<S>>;

#[async_trait::async_trait]
impl<S> TableCatalogObjectBackend for EcStoreTableCatalogObjectBackend<S>
where
    S: TableCatalogStorage,
{
    async fn read_table_bucket_incarnation(&self, bucket: &str) -> TableCatalogStoreResult<Uuid> {
        let metadata = self
            .store
            .get_managed_bucket_metadata(bucket)
            .await
            .map_err(|err| storage_error_to_catalog("read table bucket identity", err))?;
        if metadata
            .bucket_kind()
            .map_err(|err| storage_error_to_catalog("validate table bucket kind", err))?
            != BucketKind::Table
        {
            return Err(TableCatalogStoreError::Invalid("catalog requires a Table bucket".to_string()));
        }
        if metadata.bucket_incarnation_id.is_nil() {
            return Err(TableCatalogStoreError::Invalid("table bucket has no persistent incarnation".to_string()));
        }
        Ok(metadata.bucket_incarnation_id)
    }

    async fn revalidate_bucket_handle(&self, expected: &BucketHandle) -> TableCatalogStoreResult<()> {
        let metadata = self
            .store
            .get_managed_bucket_metadata(expected.storage_bucket_id().as_str())
            .await
            .map_err(|err| storage_error_to_catalog("read managed bucket identity", err))?;
        metadata
            .validate_bucket_handle(expected)
            .map_err(|err| storage_error_to_catalog("validate managed bucket identity", err))
    }

    async fn acquire_bucket_lifecycle_read(&self, expected: &BucketHandle) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        let guard = self
            .store
            .acquire_managed_bucket_fence(expected.storage_bucket_id().as_str(), expected.incarnation().as_uuid())
            .await
            .map_err(|err| storage_error_to_catalog("acquire managed bucket lifecycle guard", err))?;
        self.revalidate_bucket_handle(expected).await?;
        if guard.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable("managed bucket lifecycle guard was lost".to_string()));
        }
        let lock_lost = guard.lock_lost_signal();
        Ok(TableCatalogLockGuard {
            _guard: Box::new(()),
            lifecycle_guard: Some(Arc::new(guard)),
            lock_lost,
            write_commit_guard: None,
        })
    }
    async fn reuse_bucket_lifecycle_read(
        &self,
        expected: &BucketHandle,
        guard: Arc<BucketIncarnationFenceGuard>,
    ) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        self.reuse_vector_bucket_lifecycle(expected, guard).await
    }
    async fn read_managed_candidate(
        &self,
        bucket: &BucketHandle,
        metadata: &CatalogMetadataRef,
        max_size: usize,
        lifecycle: &mut TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let reader = self
            .managed_candidate_reader
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Unsupported("managed candidate reader is not configured".to_string()))?;
        if lifecycle.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable(
                "managed candidate lifecycle guard was lost".to_string(),
            ));
        }
        let result = reader
            .read_managed_candidate(bucket, metadata, max_size, lifecycle.bucket_lifecycle_guard()?)
            .await?;
        if lifecycle.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable(
                "managed candidate lifecycle guard was lost".to_string(),
            ));
        }
        Ok(result)
    }
    fn registration_cache(&self) -> Option<TableCatalogRegistrationCache> {
        Some(self.registration_cache.clone())
    }
    async fn acquire_catalog_migration_read_guards(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Vec<TableCatalogLockGuard>> {
        // Handlers discover and publish objects through one publication object. Keep the
        // migration read permits ahead of its publication lock, matching store mutations.
        let paths = TableCatalogObjectPaths::default();
        let global = self
            .acquire_read_lock(RUSTFS_META_BUCKET, &paths.catalog_backup_global_fence_lock_path())
            .await?;
        let bucket = self
            .acquire_read_lock(RUSTFS_META_BUCKET, &paths.catalog_backup_fence_lock_path(table_bucket))
            .await?;
        Ok(vec![global, bucket])
    }

    async fn read_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.read_object_with_options(bucket, object, ObjectOptions::default(), None)
            .await
    }

    async fn read_object_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.read_object_with_options(bucket, object, ObjectOptions::default(), Some(max_size))
            .await
    }

    async fn read_object_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.read_object_with_options(
            bucket,
            object,
            ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
            None,
        )
        .await
    }

    async fn read_object_unlocked_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.read_object_with_options(
            bucket,
            object,
            ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
            Some(max_size),
        )
        .await
    }

    async fn object_metadata(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObjectMetadata>> {
        match self.store.get_object_info(bucket, object, &ObjectOptions::default()).await {
            Ok(info) => Ok(Some(TableCatalogObjectMetadata {
                body_revision: info.data_dir.filter(|id| !id.is_nil()),
                etag: info.etag,
                mod_time: info.mod_time,
                size: u64::try_from(info.size.max(0)).unwrap_or(u64::MAX),
            })),
            Err(err) if is_missing_storage_error(&err) => Ok(None),
            Err(err) => Err(storage_error_to_catalog("stat catalog object", err)),
        }
    }

    async fn object_metadata_unlocked(
        &self,
        bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableCatalogObjectMetadata>> {
        match self
            .store
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(info) => Ok(Some(TableCatalogObjectMetadata {
                body_revision: info.data_dir.filter(|id| !id.is_nil()),
                etag: info.etag,
                mod_time: info.mod_time,
                size: u64::try_from(info.size.max(0)).unwrap_or(u64::MAX),
            })),
            Err(err) if is_missing_storage_error(&err) => Ok(None),
            Err(err) => Err(storage_error_to_catalog("stat catalog object", err)),
        }
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool> {
        match self.store.get_object_info(bucket, object, &ObjectOptions::default()).await {
            Ok(_) => Ok(true),
            Err(err) if is_missing_storage_error(&err) => Ok(false),
            Err(err) => Err(storage_error_to_catalog("check catalog object", err)),
        }
    }

    async fn object_exists_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool> {
        match self
            .store
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(err) if is_missing_storage_error(&err) => Ok(false),
            Err(err) => Err(storage_error_to_catalog("check catalog object", err)),
        }
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        self.put_object_with_options(bucket, object, data, precondition, None, None)
            .await
    }

    async fn put_object_unlocked(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        write_guards: Vec<WriteCommitGuard>,
    ) -> TableCatalogStoreResult<()> {
        self.put_object_with_options(bucket, object, data, precondition, Some(write_guards), None)
            .await
    }

    async fn put_object_unlocked_fenced(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        write_guards: Vec<WriteCommitGuard>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        self.put_object_with_options(bucket, object, data, precondition, Some(write_guards), Some(fence))
            .await
    }

    async fn delete_object_unlocked_fenced(
        &self,
        bucket: &str,
        object: &str,
        write_guards: Vec<WriteCommitGuard>,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        if write_guards.is_empty() {
            return Err(TableCatalogStoreError::Internal("catalog delete has no namespace owner".to_string()));
        }
        let mut options = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        fence.add_to_object_options(&mut options);
        for guard in &write_guards {
            options.add_write_commit_guard(guard);
        }
        match self.store.delete_object(bucket, object, options).await {
            Ok(_) => fence.ensure_held(),
            Err(err) if is_missing_storage_error(&err) => fence.ensure_held(),
            Err(err) => Err(storage_error_to_catalog("delete fenced catalog object", err)),
        }
    }

    async fn put_object_fenced(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        self.put_object_with_options(bucket, object, data, precondition, None, Some(fence))
            .await
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
        match self.store.delete_object(bucket, object, ObjectOptions::default()).await {
            Ok(_) => Ok(()),
            Err(err) if is_missing_storage_error(&err) => Ok(()),
            Err(err) => Err(storage_error_to_catalog("delete catalog object", err)),
        }
    }

    async fn delete_object_if_match(&self, bucket: &str, object: &str, expected_etag: &str) -> TableCatalogStoreResult<()> {
        self.delete_object_if_match_with_fence(bucket, object, expected_etag, None, None)
            .await
    }

    async fn delete_object_if_match_fenced(
        &self,
        bucket: &str,
        object: &str,
        expected_etag: &str,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        fence.ensure_held()?;
        self.delete_object_if_match_with_fence(bucket, object, expected_etag, Some(fence), None)
            .await
    }

    async fn delete_object_if_match_fenced_for_incarnation(
        &self,
        bucket: &str,
        object: &str,
        expected_etag: &str,
        expected_incarnation_id: Uuid,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        if expected_incarnation_id.is_nil() {
            return Err(TableCatalogStoreError::Invalid(
                "cleanup requires a non-nil bucket incarnation".to_string(),
            ));
        }
        fence.ensure_held()?;
        self.delete_object_if_match_with_fence(bucket, object, expected_etag, Some(fence), Some(expected_incarnation_id))
            .await
    }

    async fn delete_object_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
        match self
            .store
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) if is_missing_storage_error(&err) => Ok(()),
            Err(err) => Err(storage_error_to_catalog("delete catalog object", err)),
        }
    }

    async fn list_objects(&self, bucket: &str, prefix: &str) -> TableCatalogStoreResult<Vec<String>> {
        let mut continuation = None;
        let mut seen_continuations = BTreeSet::new();
        let mut objects = BTreeSet::new();
        let max_keys = i32::try_from(TABLE_CATALOG_LIST_MAX_KEYS)
            .map_err(|_| TableCatalogStoreError::Internal("catalog list limit exceeds storage API range".to_string()))?;

        loop {
            let result = self
                .store
                .clone()
                .list_objects_v2(bucket, prefix, continuation, None, max_keys, false, None, false)
                .await
                .map_err(|err| storage_error_to_catalog("list catalog objects", err))?;

            for object in result.objects {
                objects.insert(object.name);
            }

            match catalog_list_next_continuation(&mut seen_continuations, result.is_truncated, result.next_continuation_token)? {
                Some(next) => continuation = Some(next),
                None => break,
            };
        }

        Ok(objects.into_iter().collect())
    }

    async fn list_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogObjectListPage> {
        let max_keys = i32::try_from(limit.get())
            .map_err(|_| TableCatalogStoreError::Invalid("catalog page size exceeds storage API range".to_string()))?;
        let result = self
            .store
            .clone()
            .list_objects_v2(bucket, prefix, None, None, max_keys, false, start_after.map(str::to_string), false)
            .await
            .map_err(|err| storage_error_to_catalog("list catalog object page", err))?;
        let is_truncated = result.is_truncated;
        let objects = result.objects.into_iter().map(|object| object.name).collect::<BTreeSet<_>>();
        Ok(TableCatalogObjectListPage {
            objects: objects.into_iter().collect(),
            is_truncated,
        })
    }

    async fn list_table_entry_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogObjectListPage> {
        let max_keys = i32::try_from(limit.get())
            .map_err(|_| TableCatalogStoreError::Invalid("catalog page size exceeds storage API range".to_string()))?;
        let page = self
            .store
            .clone()
            .list_identifier_tree_leaves_authoritative(
                bucket,
                prefix,
                TABLE_ENTRY_FILE,
                start_after.map(str::to_string),
                max_keys,
            )
            .await
            .map_err(|err| storage_error_to_catalog("list authoritative table entries", err))?;
        Ok(TableCatalogObjectListPage {
            objects: page.objects.into_iter().map(|object| object.name).collect(),
            is_truncated: page.is_truncated,
        })
    }

    async fn list_catalog_objects_page(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogCleanupPage> {
        let max_keys = i32::try_from(limit.get())
            .map_err(|_| TableCatalogStoreError::Invalid("catalog page size exceeds storage API range".to_string()))?;
        let page = self
            .store
            .clone()
            .list_unversioned_objects_authoritative(bucket, prefix, start_after.map(str::to_string), max_keys)
            .await
            .map_err(|err| storage_error_to_catalog("list authoritative catalog page", err))?;
        Ok(TableCatalogCleanupPage {
            objects: page
                .objects
                .into_iter()
                .map(|object| TableCatalogCleanupObject {
                    key: object.name,
                    version_id: object.version_id.filter(|id| !id.is_nil()).map(|id| id.to_string()),
                    etag: object.etag,
                    delete_marker: object.delete_marker,
                })
                .collect(),
            is_truncated: page.is_truncated,
        })
    }

    async fn list_objects_for_cleanup(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> TableCatalogStoreResult<Vec<TableCatalogCleanupObject>> {
        let cancel = tokio_util::sync::CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<CatalogObjectInfoOrErr>(100);
        let walk = self.store.clone().walk_authoritative(
            cancel,
            bucket,
            prefix,
            sender,
            CatalogWalkOptions {
                include_free_versions: true,
                ..Default::default()
            },
        );
        let collect = async move {
            let mut objects = Vec::new();
            while let Some(result) = receiver.recv().await {
                if let Some(err) = result.err {
                    return Err(storage_error_to_catalog("inspect cleanup object versions", err));
                }
                let object = result.item.ok_or_else(|| {
                    TableCatalogStoreError::Internal("cleanup object walk returned an empty result".to_string())
                })?;
                objects.push(TableCatalogCleanupObject {
                    key: object.name,
                    version_id: object.version_id.filter(|id| !id.is_nil()).map(|id| id.to_string()),
                    etag: object.etag,
                    delete_marker: object.delete_marker,
                });
            }
            Ok(objects)
        };
        let (walk_result, objects) = tokio::join!(walk, collect);
        let objects = objects?;
        walk_result.map_err(|err| storage_error_to_catalog("list cleanup object versions", err))?;
        Ok(objects)
    }

    async fn ensure_no_multipart_uploads(&self, bucket: &str) -> TableCatalogStoreResult<()> {
        self.store.ensure_no_multipart_uploads(bucket).await.map_err(|err| match err {
            StorageError::BucketNotEmpty(_) => {
                TableCatalogStoreError::Conflict(format!("table bucket {bucket} contains unfinished multipart uploads"))
            }
            other => storage_error_to_catalog("inspect unfinished multipart uploads", other),
        })
    }

    async fn acquire_write_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        let lock = self
            .store
            .new_ns_lock(bucket, object)
            .await
            .map_err(|err| storage_error_to_catalog("create catalog table lock", err))?;
        let guard = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout())
            .await
            .map_err(|err| catalog_lock_acquisition_error("acquire catalog table lock", err))?;
        Ok(TableCatalogLockGuard::write_namespace(guard))
    }

    async fn acquire_read_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        let lock = self
            .store
            .new_ns_lock(bucket, object)
            .await
            .map_err(|err| storage_error_to_catalog("create catalog migration lock", err))?;
        let guard = lock
            .get_read_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| catalog_lock_acquisition_error("acquire catalog migration lock", err))?;
        Ok(TableCatalogLockGuard::namespace(guard))
    }
}

impl<S> EcStoreTableCatalogObjectBackend<S>
where
    S: TableCatalogStorage,
{
    async fn delete_object_if_match_with_fence(
        &self,
        bucket: &str,
        object: &str,
        expected_etag: &str,
        fence: Option<&TableCatalogObjectMutationFence>,
        expected_incarnation_id: Option<Uuid>,
    ) -> TableCatalogStoreResult<()> {
        let mut options = ObjectOptions {
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(expected_etag.to_string()),
                ..Default::default()
            }),
            expected_bucket_incarnation_id: expected_incarnation_id,
            ..Default::default()
        };
        if let Some(fence) = fence {
            fence.add_to_object_options(&mut options);
        }
        match self.store.delete_object(bucket, object, options).await {
            Ok(_) => {
                if let Some(fence) = fence {
                    fence.ensure_held()?;
                }
                Ok(())
            }
            Err(err) if is_missing_storage_error(&err) => Err(TableCatalogStoreError::Conflict(format!(
                "delete catalog object: {bucket}/{object} disappeared before conditional cleanup"
            ))),
            Err(err) => Err(storage_error_to_catalog("conditionally delete catalog object", err)),
        }
    }

    async fn read_object_with_options(
        &self,
        bucket: &str,
        object: &str,
        opts: ObjectOptions,
        max_size: Option<usize>,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let mut reader = match self
            .store
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
        {
            Ok(reader) => reader,
            Err(err) if is_missing_storage_error(&err) => return Ok(None),
            Err(err) => return Err(storage_error_to_catalog("read catalog object", err)),
        };
        if let Some(max_size) = max_size {
            let object_size = usize::try_from(reader.object_info.size)
                .map_err(|_| TableCatalogStoreError::Invalid(format!("catalog object {bucket}/{object} has an invalid size")))?;
            if object_size > max_size {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog object {bucket}/{object} exceeds the maximum size of {max_size} bytes"
                )));
            }
        }
        let body_revision = reader.object_info.data_dir.filter(|id| !id.is_nil());
        let etag = reader.object_info.etag.clone();
        let mod_time = reader.object_info.mod_time;
        let mut data = Vec::new();
        if let Some(max_size) = max_size {
            let read_limit = u64::try_from(max_size.saturating_add(1)).unwrap_or(u64::MAX);
            reader.stream.take(read_limit).read_to_end(&mut data).await.map_err(|err| {
                TableCatalogStoreError::Internal(format!("failed to read catalog object {bucket}/{object}: {err}"))
            })?;
            if data.len() > max_size {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog object {bucket}/{object} exceeds the maximum size of {max_size} bytes"
                )));
            }
        } else {
            reader.stream.read_to_end(&mut data).await.map_err(|err| {
                TableCatalogStoreError::Internal(format!("failed to read catalog object {bucket}/{object}: {err}"))
            })?;
        }
        Ok(Some(TableCatalogObject {
            data,
            body_revision,
            etag,
            mod_time,
        }))
    }

    async fn put_object_with_options(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        write_guards: Option<Vec<WriteCommitGuard>>,
        fence: Option<&TableCatalogObjectMutationFence>,
    ) -> TableCatalogStoreResult<()> {
        let mut reader = PutObjReader::from_vec(data);
        let mut opts = ObjectOptions {
            http_preconditions: http_preconditions_for_catalog_put(precondition),
            ..Default::default()
        };
        if let Some(fence) = fence {
            fence.add_to_object_options(&mut opts);
            opts.write_completion = WriteCompletion::TailDrained;
        }
        if let Some(write_guards) = write_guards {
            if write_guards.is_empty() {
                return Err(TableCatalogStoreError::Internal("catalog write has no namespace owner".to_string()));
            }
            for guard in &write_guards {
                opts.add_write_commit_guard(guard);
            }
            opts.write_completion = WriteCompletion::TailDrained;
        }
        let result = self
            .store
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .map(|_| ())
            .map_err(|err| storage_error_to_catalog("write catalog object", err));
        if result.is_ok()
            && let Some(fence) = fence
        {
            fence.ensure_held()?;
        }
        result
    }
}

#[derive(Clone)]
pub(crate) struct VectorCatalogOperationGuard {
    inner: Arc<VectorCatalogOperationState>,
}

struct VectorCatalogOperationState {
    owner: Arc<()>,
    resource_identity: CatalogResourceIdentity,
    namespace: Namespace,
    name: CatalogName,
    lifecycle: tokio::sync::Mutex<TableCatalogLockGuard>,
    native_lifecycle: Option<Arc<BucketIncarnationFenceGuard>>,
    gc: parking_lot::Mutex<TableCatalogLockGuard>,
    fence: TableCatalogObjectMutationFence,
}

impl std::fmt::Debug for VectorCatalogOperationGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VectorCatalogOperationGuard")
            .field("resource_identity", self.resource_identity())
            .field("namespace", self.namespace())
            .field("name", self.name())
            .finish_non_exhaustive()
    }
}

impl VectorCatalogOperationGuard {
    pub(crate) fn resource_identity(&self) -> &CatalogResourceIdentity {
        &self.inner.resource_identity
    }
    pub(crate) fn namespace(&self) -> &Namespace {
        &self.inner.namespace
    }
    pub(crate) fn name(&self) -> &CatalogName {
        &self.inner.name
    }
    pub(crate) fn check_held(&self) -> TableCatalogStoreResult<()> {
        self.inner.fence.ensure_held()?;
        if self.inner.gc.lock().is_lock_lost() || self.inner.native_lifecycle.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            return Err(TableCatalogStoreError::Unavailable("Vector operation protection was lost".into()));
        }
        Ok(())
    }
    pub(crate) fn lifecycle_guard(&self) -> TableCatalogStoreResult<Arc<BucketIncarnationFenceGuard>> {
        self.check_held()?;
        self.inner
            .native_lifecycle
            .clone()
            .ok_or_else(|| TableCatalogStoreError::Unsupported("Vector operation has no Managed lifecycle guard".into()))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VectorCatalogEntry {
    head: CatalogHead,
    definition: VectorIndexDefinitionV1,
    entry: TableEntry,
}

impl VectorCatalogEntry {
    fn from_entry(entry: TableEntry) -> TableCatalogStoreResult<Self> {
        if entry.version != 3 {
            return Err(TableCatalogStoreError::Unsupported("formal Vector operations require Catalog v3".into()));
        }
        let head = CatalogHead::from_vector_entry(&entry)?;
        let definition = entry
            .vector_index_definition
            .clone()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Vector v3 entry has no Index definition".into()))?;
        Ok(Self { head, definition, entry })
    }
    pub(crate) fn head(&self) -> &CatalogHead {
        &self.head
    }
    pub(crate) fn definition(&self) -> &VectorIndexDefinitionV1 {
        &self.definition
    }
    pub(crate) fn created_at(&self) -> TableCatalogStoreResult<OffsetDateTime> {
        let value = self
            .entry
            .created_at
            .as_deref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("Vector entry has no creation time".into()))?;
        let timestamp = OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .map_err(|_| TableCatalogStoreError::Invalid("Vector entry creation time is invalid".into()))?;
        if timestamp.offset() != time::UtcOffset::UTC {
            return Err(TableCatalogStoreError::Invalid("Vector entry creation time is not UTC".into()));
        }
        Ok(timestamp)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VectorIndexCreateReservation {
    intent: VectorIndexCreateIntentV1,
    expected_head: CatalogExpectedHead,
    resource_identity: CatalogResourceIdentity,
    namespace: Namespace,
    name: CatalogName,
    create_id: CatalogCommitId,
    planned_manifest: CatalogMetadataRef,
    initialization_payload: Vec<u8>,
    published_entry: Option<VectorCatalogEntry>,
}

impl VectorIndexCreateReservation {
    fn from_intent(intent: VectorIndexCreateIntentV1) -> TableCatalogStoreResult<Self> {
        intent.validate()?;
        let resource_identity = intent.resource_identity()?;
        let namespace =
            Namespace::parse(&intent.namespace).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
        let name = CatalogName::parse(&intent.name).map_err(|error| TableCatalogStoreError::Invalid(error.to_string()))?;
        let create_id = CatalogCommitId::try_new(intent.create_id.clone())?;
        let expected_head = CatalogExpectedHead::for_vector_creation(&intent.create_id)?;
        let planned_manifest = CatalogMetadataRef::try_from(&intent.planned_manifest)?;
        let initialization_payload = base64_simd::STANDARD
            .decode_to_vec(intent.initialization_payload_base64.as_bytes())
            .map_err(|_| TableCatalogStoreError::Invalid("invalid reserved initialization payload".into()))?;
        Ok(Self {
            intent,
            expected_head,
            resource_identity,
            namespace,
            name,
            create_id,
            planned_manifest,
            initialization_payload,
            published_entry: None,
        })
    }
    pub(crate) fn resource_identity(&self) -> &CatalogResourceIdentity {
        &self.resource_identity
    }
    pub(crate) fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    pub(crate) fn name(&self) -> &CatalogName {
        &self.name
    }
    pub(crate) fn create_id(&self) -> &CatalogCommitId {
        &self.create_id
    }
    pub(crate) fn expected_head(&self) -> &CatalogExpectedHead {
        &self.expected_head
    }
    pub(crate) fn planned_manifest(&self) -> &CatalogMetadataRef {
        &self.planned_manifest
    }
    pub(crate) fn definition(&self) -> &VectorIndexDefinitionV1 {
        &self.intent.definition
    }
    pub(crate) fn initialization_payload(&self) -> &[u8] {
        &self.initialization_payload
    }
    #[cfg(test)]
    pub(crate) fn phase(&self) -> VectorIndexCreatePhase {
        self.intent.phase
    }
    pub(crate) fn published_entry(&self) -> Option<&VectorCatalogEntry> {
        self.published_entry.as_ref()
    }
}

impl TableCatalogObjectPaths {
    fn vector_create_intent_path(&self, bucket: &str, namespace: &Namespace, name: &CatalogName) -> String {
        let head = self.table_entry_path(bucket, namespace, name);
        format!("{}create-intent.json", head.trim_end_matches(TABLE_ENTRY_FILE))
    }
    pub(in crate::table_catalog) fn vector_delete_intent_path(&self, bucket: &str, table_id: &str) -> String {
        format!(
            "{}resources/{}/delete-intent.json",
            self.table_bucket_root_prefix(bucket),
            table_catalog_path_hash(table_id)
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) enum VectorTagUpdate {
    Set(BTreeMap<String, String>),
    Remove(BTreeSet<String>),
}

impl<S: TableCatalogStorage> EcStoreTableCatalogObjectBackend<S> {
    async fn reuse_vector_bucket_lifecycle(
        &self,
        expected: &BucketHandle,
        guard: Arc<BucketIncarnationFenceGuard>,
    ) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        self.store
            .validate_managed_bucket_fence(&guard, expected.storage_bucket_id().as_str())
            .map_err(|error| storage_error_to_catalog("validate shared managed bucket lifecycle", error))?;
        self.revalidate_bucket_handle(expected).await?;
        if guard.is_lock_lost() {
            return Err(TableCatalogStoreError::Unavailable(
                "shared managed bucket lifecycle guard was lost".into(),
            ));
        }
        let lock_lost = guard.lock_lost_signal();
        Ok(TableCatalogLockGuard {
            _guard: Box::new(()),
            lifecycle_guard: Some(guard),
            lock_lost,
            write_commit_guard: None,
        })
    }
}
