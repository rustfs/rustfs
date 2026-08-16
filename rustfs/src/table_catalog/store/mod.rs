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

mod migration;
mod object;
mod strong;

use migration::table_catalog_backing_manifest;
pub(crate) use object::ObjectTableCatalogStore;
#[cfg(test)]
pub(super) use strong::{
    STRONG_TABLE_CATALOG_RELOAD_MAX_ATTEMPTS, STRONG_TABLE_CATALOG_SNAPSHOT_MAX_SIZE, StrongCommitSnapshotRecord,
    StrongTableCatalogBucketSnapshot, StrongTableCatalogSnapshot, strong_snapshot_write_version,
    table_catalog_bucket_snapshot_fingerprint,
};
pub(crate) use strong::{StrongTableCatalogRuntime, StrongTableCatalogStore};

fn validate_table_bucket_entry(entry: &TableBucketEntry) -> TableCatalogStoreResult<()> {
    validate_catalog_entry_version("table bucket", entry.version)?;
    if entry.table_bucket.is_empty() {
        return Err(TableCatalogStoreError::Invalid("table bucket name cannot be empty".to_string()));
    }
    if entry.catalog_type != TABLE_BUCKET_CATALOG_TYPE {
        return Err(TableCatalogStoreError::Invalid("unsupported table bucket catalog type".to_string()));
    }
    Ok(())
}

fn validate_table_entry_version_and_id(entry: &TableEntry) -> TableCatalogStoreResult<()> {
    validate_catalog_entry_version("table", entry.version)?;
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

    /// Atomically advances a validated table metadata pointer.
    ///
    /// Callers publishing client-supplied Iceberg metadata must validate its logical shape and the physical graph of
    /// newly introduced or changed snapshots before invoking this persistence boundary.
    #[allow(
        dead_code,
        reason = "declared trait method: implementors provide it but no caller dispatches through the trait yet (backlog#1823)"
    )]
    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult>;

    async fn commit_table_with_publication(
        &self,
        request: TableCommitRequest,
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

    async fn replace_view(&self, request: ViewCommitRequest) -> TableCatalogStoreResult<ViewCommitResult>;

    async fn replace_view_with_publication(
        &self,
        request: ViewCommitRequest,
        table_bucket_fence_required: bool,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<ViewCommitResult> {
        if table_bucket_fence_required {
            publication.begin_table_bucket(&request.table_bucket).await?;
            if !publication.holds_table_bucket(&request.table_bucket) {
                return Err(TableCatalogStoreError::Internal(
                    "view replacement requires a table-bucket publication fence".to_string(),
                ));
            }
        }
        publication
            .prepare(&request.table_bucket, &request.namespace, &request.view)
            .await?;
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.view) {
            return Err(TableCatalogStoreError::Internal(
                "view replacement requires a view publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        self.replace_view(request).await
    }

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
    async fn begin_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<()>;

    async fn prepare(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()>;

    fn holds_table_bucket(&self, table_bucket: &str) -> bool;

    fn holds_table(&self, table_bucket: &str, namespace: &str, table: &str) -> bool;

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

struct TableCommitLockPublication<'a, B> {
    backend: &'a B,
    state: parking_lot::Mutex<TableCommitLockPublicationState>,
}

#[derive(Default)]
struct TableCommitLockPublicationState {
    table_bucket: Option<String>,
    table: Option<(String, String, String)>,
    guards: Vec<TableCatalogLockGuard>,
}

impl<'a, B> TableCommitLockPublication<'a, B> {
    fn new(backend: &'a B) -> Self {
        Self {
            backend,
            state: parking_lot::Mutex::new(TableCommitLockPublicationState::default()),
        }
    }
}

#[async_trait::async_trait]
impl<'a, B> TableCommitPublication for TableCommitLockPublication<'a, B>
where
    B: TableCatalogObjectBackend,
{
    async fn begin_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        {
            let mut state = self.state.lock();
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
        let publication_lock = default_table_bucket_publication_lock_path();
        let guard = match self.backend.acquire_write_lock(table_bucket, &publication_lock).await {
            Ok(guard) => guard,
            Err(err) => {
                self.state.lock().table_bucket = None;
                return Err(err);
            }
        };
        self.state.lock().guards.push(guard);
        Ok(())
    }

    async fn prepare(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let table_key = (table_bucket.to_string(), namespace.public_name(), table.as_str().to_string());
        {
            let mut state = self.state.lock();
            if state.table.as_ref() == Some(&table_key) {
                return Ok(());
            }
            if state.table.is_some() {
                return Err(TableCatalogStoreError::Internal(
                    "table publication lock is already held for another table".to_string(),
                ));
            }
            state.table = Some(table_key);
        }
        let publication_lock = default_table_publication_lock_path(&namespace, &table);
        let guard = match self.backend.acquire_write_lock(table_bucket, &publication_lock).await {
            Ok(guard) => guard,
            Err(err) => {
                self.state.lock().table = None;
                return Err(err);
            }
        };
        self.state.lock().guards.push(guard);
        Ok(())
    }

    fn holds_table_bucket(&self, table_bucket: &str) -> bool {
        let state = self.state.lock();
        state.table_bucket.as_deref() == Some(table_bucket) && state.guards.iter().all(|guard| !guard.is_lock_lost())
    }

    fn holds_table(&self, table_bucket: &str, namespace: &str, table: &str) -> bool {
        let state = self.state.lock();
        state
            .table
            .as_ref()
            .is_some_and(|held| held.0 == table_bucket && held.1 == namespace && held.2 == table)
            && state.guards.iter().all(|guard| !guard.is_lock_lost())
    }

    fn complete(&self) {
        *self.state.lock() = TableCommitLockPublicationState::default();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObject {
    pub data: Vec<u8>,
    pub etag: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObjectMetadata {
    pub etag: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
}

pub(crate) struct TableCatalogLockGuard {
    _guard: Box<dyn Send>,
    lock_lost: Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
}

impl TableCatalogLockGuard {
    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) fn stable(guard: impl Send + 'static) -> Self {
        Self {
            _guard: Box::new(guard),
            lock_lost: None,
        }
    }

    fn namespace(guard: rustfs_lock::NamespaceLockGuard) -> Self {
        let lock_lost = guard.lock_lost_signal();
        Self {
            _guard: Box::new(guard),
            lock_lost,
        }
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        self.lock_lost.as_ref().is_some_and(|signal| signal.is_lost())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObjectListPage {
    pub objects: Vec<String>,
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
    fn strong_catalog_runtime(&self) -> Option<StrongTableCatalogRuntime> {
        None
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
                etag: object.etag,
                mod_time: object.mod_time,
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
                etag: object.etag,
                mod_time: object.mod_time,
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

    async fn put_object_unlocked(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        self.put_object(bucket, object, data, precondition).await
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()>;

    async fn delete_object_unlocked(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
        self.delete_object(bucket, object).await
    }

    async fn list_objects(&self, bucket: &str, prefix: &str) -> TableCatalogStoreResult<Vec<String>>;

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

    fn complete_table_commit_publication(&self) {}
}

#[async_trait::async_trait]
impl<B> TableCommitPublication for B
where
    B: TableCatalogObjectBackend,
{
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

    fn complete(&self) {
        self.complete_table_commit_publication();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableCatalogObjectPaths {
    catalog_root: &'static str,
}

impl Default for TableCatalogObjectPaths {
    fn default() -> Self {
        Self {
            catalog_root: INTERNAL_CATALOG_ROOT,
        }
    }
}

impl TableCatalogObjectPaths {
    pub fn table_bucket_entries_prefix(&self) -> String {
        format!("{}/{}/", self.catalog_root, TABLE_BUCKET_ROOT)
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

    pub fn table_entry_path(&self, table_bucket: &str, namespace: &Namespace, table: &IdentifierSegment) -> String {
        format!(
            "{}{}/{}",
            self.table_entries_prefix(table_bucket, namespace),
            table.as_str(),
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

    pub fn backing_migration_fence_path(&self, table_bucket: &str) -> String {
        format!(
            "{}{}/{}",
            self.table_bucket_root_prefix(table_bucket),
            TABLE_CATALOG_MIGRATION_ROOT,
            TABLE_CATALOG_MIGRATION_FENCE_FILE
        )
    }

    pub fn backing_migration_fence_lock_path(&self, table_bucket: &str) -> String {
        format!(
            "{}{}/{}",
            self.table_bucket_root_prefix(table_bucket),
            TABLE_CATALOG_MIGRATION_ROOT,
            TABLE_CATALOG_MIGRATION_FENCE_LOCK
        )
    }

    pub fn backing_migration_global_fence_path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.catalog_root, TABLE_CATALOG_MIGRATION_ROOT, TABLE_CATALOG_MIGRATION_GLOBAL_FENCE_FILE
        )
    }

    pub fn backing_migration_global_fence_lock_path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.catalog_root, TABLE_CATALOG_MIGRATION_ROOT, TABLE_CATALOG_MIGRATION_GLOBAL_FENCE_LOCK
        )
    }

    fn table_bucket_root_prefix(&self, table_bucket: &str) -> String {
        format!("{}/{}/{}/", self.catalog_root, TABLE_BUCKET_ROOT, table_catalog_path_hash(table_bucket))
    }
}

#[derive(Clone)]
pub(crate) enum ConfiguredTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    ObjectBacked(ObjectTableCatalogStore<B>),
    DurableStrong(StrongTableCatalogStore<B>),
}

impl<B> ConfiguredTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub(crate) fn from_env(backend: B) -> TableCatalogStoreResult<Self> {
        Ok(match TableCatalogBackingMode::from_env()? {
            TableCatalogBackingMode::ObjectBacked => Self::ObjectBacked(ObjectTableCatalogStore::new(backend)),
            TableCatalogBackingMode::DurableStrong => {
                Self::DurableStrong(StrongTableCatalogStore::new_requiring_snapshot(backend))
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(backend: B, mode: TableCatalogBackingMode) -> Self {
        match mode {
            TableCatalogBackingMode::ObjectBacked => Self::ObjectBacked(ObjectTableCatalogStore::new(backend)),
            TableCatalogBackingMode::DurableStrong => Self::DurableStrong(StrongTableCatalogStore::new(backend)),
        }
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) fn backing_mode(&self) -> TableCatalogBackingMode {
        match self {
            Self::ObjectBacked(_) => TableCatalogBackingMode::ObjectBacked,
            Self::DurableStrong(_) => TableCatalogBackingMode::DurableStrong,
        }
    }

    fn unsupported_for_durable_strong(operation: &str) -> TableCatalogStoreError {
        TableCatalogStoreError::Invalid(format!(
            "{operation} is not supported with {TABLE_CATALOG_BACKING_DURABLE_STRONG} table catalog backing"
        ))
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
            Self::DurableStrong(store) => store.get_table_bucket(table_bucket).await,
        }
    }

    async fn put_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.put_table_bucket(entry).await,
            Self::DurableStrong(store) => store.put_table_bucket(entry).await,
        }
    }

    async fn create_namespace(&self, entry: NamespaceEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.create_namespace(entry).await,
            Self::DurableStrong(store) => store.create_namespace(entry).await,
        }
    }

    async fn list_namespaces(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_namespaces(table_bucket).await,
            Self::DurableStrong(store) => store.list_namespaces(table_bucket).await,
        }
    }

    async fn list_namespaces_under(&self, table_bucket: &str, parent: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_namespaces_under(table_bucket, parent).await,
            Self::DurableStrong(store) => store.list_namespaces_under(table_bucket, parent).await,
        }
    }

    async fn list_namespace_children(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
    ) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_namespace_children(table_bucket, parent).await,
            Self::DurableStrong(store) => store.list_namespace_children(table_bucket, parent).await,
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
            Self::DurableStrong(store) => store.list_namespace_children_page(table_bucket, parent, cursor, limit).await,
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
            Self::DurableStrong(store) => store.list_namespaces_page(table_bucket, cursor, limit).await,
        }
    }

    async fn get_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Option<NamespaceEntry>> {
        match self {
            Self::ObjectBacked(store) => store.get_namespace(table_bucket, namespace).await,
            Self::DurableStrong(store) => store.get_namespace(table_bucket, namespace).await,
        }
    }

    async fn update_namespace_properties(
        &self,
        table_bucket: &str,
        namespace: &str,
        update: NamespacePropertiesUpdate,
    ) -> TableCatalogStoreResult<NamespacePropertiesUpdateResult> {
        match self {
            Self::ObjectBacked(_) => Err(TableCatalogStoreError::Unsupported(
                "namespace property updates require durable-strong catalog backing".to_string(),
            )),
            Self::DurableStrong(store) => store.update_namespace_properties(table_bucket, namespace, update).await,
        }
    }

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.drop_namespace(table_bucket, namespace).await,
            Self::DurableStrong(store) => store.drop_namespace(table_bucket, namespace).await,
        }
    }

    async fn create_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.create_table(entry).await,
            Self::DurableStrong(store) => store.create_table(entry).await,
        }
    }

    async fn register_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.register_table(entry).await,
            Self::DurableStrong(store) => store.register_table(entry).await,
        }
    }

    async fn register_table_with_publication(
        &self,
        entry: TableEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.register_table_with_publication(entry, publication).await,
            Self::DurableStrong(store) => store.register_table_with_publication(entry, publication).await,
        }
    }

    async fn list_tables(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_tables(table_bucket, namespace).await,
            Self::DurableStrong(store) => store.list_tables(table_bucket, namespace).await,
        }
    }

    async fn list_all_tables(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_all_tables(table_bucket).await,
            Self::DurableStrong(store) => store.list_all_tables(table_bucket).await,
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
            Self::DurableStrong(store) => store.list_tables_page(table_bucket, namespace, cursor, limit).await,
        }
    }

    async fn load_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<Option<TableEntry>> {
        match self {
            Self::ObjectBacked(store) => store.load_table(table_bucket, namespace, table).await,
            Self::DurableStrong(store) => store.load_table(table_bucket, namespace, table).await,
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
            Self::ObjectBacked(_) => Err(TableCatalogStoreError::Unsupported(
                "table rename requires durable-strong catalog backing".to_string(),
            )),
            Self::DurableStrong(store) => {
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
            Self::DurableStrong(store) => store.resolve_table_data_plane_resource(table_bucket, object).await,
        }
    }

    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult> {
        match self {
            Self::ObjectBacked(store) => store.commit_table(request).await,
            Self::DurableStrong(store) => store.commit_table(request).await,
        }
    }

    async fn commit_table_with_publication(
        &self,
        request: TableCommitRequest,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        match self {
            Self::ObjectBacked(store) => store.commit_table_with_publication(request, publication).await,
            Self::DurableStrong(store) => store.commit_table_with_publication(request, publication).await,
        }
    }

    async fn drop_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.drop_table(table_bucket, namespace, table).await,
            Self::DurableStrong(store) => store.drop_table(table_bucket, namespace, table).await,
        }
    }

    async fn create_view(&self, entry: ViewEntry) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.create_view(entry).await,
            Self::DurableStrong(store) => store.create_view(entry).await,
        }
    }

    async fn create_view_with_publication(
        &self,
        entry: ViewEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.create_view_with_publication(entry, publication).await,
            Self::DurableStrong(store) => store.create_view_with_publication(entry, publication).await,
        }
    }

    async fn list_views(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<ViewEntry>> {
        match self {
            Self::ObjectBacked(store) => store.list_views(table_bucket, namespace).await,
            Self::DurableStrong(store) => store.list_views(table_bucket, namespace).await,
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
            Self::DurableStrong(store) => store.list_views_page(table_bucket, namespace, cursor, limit).await,
        }
    }

    async fn load_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<Option<ViewEntry>> {
        match self {
            Self::ObjectBacked(store) => store.load_view(table_bucket, namespace, view).await,
            Self::DurableStrong(store) => store.load_view(table_bucket, namespace, view).await,
        }
    }

    async fn replace_view(&self, request: ViewCommitRequest) -> TableCatalogStoreResult<ViewCommitResult> {
        match self {
            Self::ObjectBacked(store) => store.replace_view(request).await,
            Self::DurableStrong(store) => store.replace_view(request).await,
        }
    }

    async fn replace_view_with_publication(
        &self,
        request: ViewCommitRequest,
        table_bucket_fence_required: bool,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<ViewCommitResult> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .replace_view_with_publication(request, table_bucket_fence_required, publication)
                    .await
            }
            Self::DurableStrong(store) => {
                store
                    .replace_view_with_publication(request, table_bucket_fence_required, publication)
                    .await
            }
        }
    }

    async fn drop_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<()> {
        match self {
            Self::ObjectBacked(store) => store.drop_view(table_bucket, namespace, view).await,
            Self::DurableStrong(store) => store.drop_view(table_bucket, namespace, view).await,
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
            Self::DurableStrong(store) => store.get_commit_by_id(table_bucket, table_id, commit_id).await,
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
            Self::DurableStrong(store) => {
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("table maintenance config")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("table maintenance config")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("table maintenance report")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("table maintenance scheduler")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("table maintenance scheduler")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("table maintenance quarantine")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("table maintenance worker")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("table maintenance heartbeat")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("catalog export")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("catalog diagnostics")),
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
            Self::DurableStrong(store) => store.plan_table_commit_recovery(table_bucket, namespace, table).await,
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("external catalog bridge")),
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
            Self::DurableStrong(_) => Err(Self::unsupported_for_durable_strong("external catalog bridge")),
        }
    }
}

pub(crate) struct EcStoreTableCatalogObjectBackend<S> {
    store: Arc<S>,
    strong_runtime: StrongTableCatalogRuntime,
}

impl<S> Clone for EcStoreTableCatalogObjectBackend<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            strong_runtime: self.strong_runtime.clone(),
        }
    }
}

impl<S> EcStoreTableCatalogObjectBackend<S>
where
    S: TableCatalogStorage,
{
    pub fn new_with_strong_runtime(store: Arc<S>, strong_runtime: StrongTableCatalogRuntime) -> Self {
        Self { store, strong_runtime }
    }
}

pub(crate) type EcStoreTableCatalogStore<S> = ConfiguredTableCatalogStore<EcStoreTableCatalogObjectBackend<S>>;

#[async_trait::async_trait]
impl<S> TableCatalogObjectBackend for EcStoreTableCatalogObjectBackend<S>
where
    S: TableCatalogStorage,
{
    fn strong_catalog_runtime(&self) -> Option<StrongTableCatalogRuntime> {
        Some(self.strong_runtime.clone())
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
                etag: info.etag,
                mod_time: info.mod_time,
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
                etag: info.etag,
                mod_time: info.mod_time,
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
        self.put_object_with_options(bucket, object, data, precondition, false).await
    }

    async fn put_object_unlocked(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        self.put_object_with_options(bucket, object, data, precondition, true).await
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
        match self.store.delete_object(bucket, object, ObjectOptions::default()).await {
            Ok(_) => Ok(()),
            Err(err) if is_missing_storage_error(&err) => Ok(()),
            Err(err) => Err(storage_error_to_catalog("delete catalog object", err)),
        }
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

    async fn acquire_write_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        let lock = self
            .store
            .new_ns_lock(bucket, object)
            .await
            .map_err(|err| storage_error_to_catalog("create catalog table lock", err))?;
        let guard = lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to acquire catalog table lock: {err}")))?;
        Ok(TableCatalogLockGuard::namespace(guard))
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
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to acquire catalog migration lock: {err}")))?;
        Ok(TableCatalogLockGuard::namespace(guard))
    }
}

impl<S> EcStoreTableCatalogObjectBackend<S>
where
    S: TableCatalogStorage,
{
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
        Ok(Some(TableCatalogObject { data, etag, mod_time }))
    }

    async fn put_object_with_options(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
        no_lock: bool,
    ) -> TableCatalogStoreResult<()> {
        let mut reader = PutObjReader::from_vec(data);
        let opts = ObjectOptions {
            http_preconditions: http_preconditions_for_catalog_put(precondition),
            no_lock,
            ..Default::default()
        };
        self.store
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .map(|_| ())
            .map_err(|err| storage_error_to_catalog("write catalog object", err))
    }
}
