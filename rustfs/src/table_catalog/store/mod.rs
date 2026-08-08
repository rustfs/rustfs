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
pub(super) use strong::StrongTableCatalogSnapshot;
pub(crate) use strong::StrongTableCatalogStore;

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

    async fn create_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()>;

    async fn register_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()>;

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
    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult>;

    async fn drop_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()>;

    async fn create_view(&self, entry: ViewEntry) -> TableCatalogStoreResult<()>;

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

#[async_trait::async_trait]
pub(crate) trait TableCatalogObjectBackend: Clone + Send + Sync + 'static {
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

    async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool>;

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

    async fn acquire_read_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Box<dyn Send>> {
        self.acquire_write_lock(bucket, object).await
    }

    async fn acquire_write_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Box<dyn Send>>;
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
        Ok(Self::new(backend, TableCatalogBackingMode::from_env()?))
    }

    pub(crate) fn new(backend: B, mode: TableCatalogBackingMode) -> Self {
        match mode {
            TableCatalogBackingMode::ObjectBacked => Self::ObjectBacked(ObjectTableCatalogStore::new(backend)),
            TableCatalogBackingMode::DurableStrong => Self::DurableStrong(StrongTableCatalogStore::new(backend)),
        }
    }

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
}

impl<S> Clone for EcStoreTableCatalogObjectBackend<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl<S> EcStoreTableCatalogObjectBackend<S>
where
    S: TableCatalogStorage,
{
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

pub(crate) type EcStoreTableCatalogStore<S> = ConfiguredTableCatalogStore<EcStoreTableCatalogObjectBackend<S>>;

#[async_trait::async_trait]
impl<S> TableCatalogObjectBackend for EcStoreTableCatalogObjectBackend<S>
where
    S: TableCatalogStorage,
{
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

            if !result.is_truncated {
                break;
            }

            let Some(next) = result.next_continuation_token else {
                break;
            };
            continuation = Some(next);
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

    async fn acquire_write_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Box<dyn Send>> {
        let lock = self
            .store
            .new_ns_lock(bucket, object)
            .await
            .map_err(|err| storage_error_to_catalog("create catalog table lock", err))?;
        let guard = lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to acquire catalog table lock: {err}")))?;
        Ok(Box::new(guard))
    }

    async fn acquire_read_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Box<dyn Send>> {
        let lock = self
            .store
            .new_ns_lock(bucket, object)
            .await
            .map_err(|err| storage_error_to_catalog("create catalog migration lock", err))?;
        let guard = lock
            .get_read_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to acquire catalog migration lock: {err}")))?;
        Ok(Box::new(guard))
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
        let info = match self.store.get_object_info(bucket, object, &opts).await {
            Ok(info) => info,
            Err(err) if is_missing_storage_error(&err) => return Ok(None),
            Err(err) => return Err(storage_error_to_catalog("read catalog object info", err)),
        };
        if let Some(max_size) = max_size {
            let object_size = usize::try_from(info.size)
                .map_err(|_| TableCatalogStoreError::Invalid(format!("catalog object {bucket}/{object} has an invalid size")))?;
            if object_size > max_size {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "catalog object {bucket}/{object} exceeds the maximum size of {max_size} bytes"
                )));
            }
        }
        let mut reader = match self
            .store
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
        {
            Ok(reader) => reader,
            Err(err) if is_missing_storage_error(&err) => return Ok(None),
            Err(err) => return Err(storage_error_to_catalog("read catalog object", err)),
        };
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
            etag: info.etag,
            mod_time: info.mod_time,
        }))
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
