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

pub(super) fn validate_table_bucket_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    entry: &TableBucketEntry,
) -> TableCatalogStoreResult<()> {
    validate_table_bucket_entry(entry)?;
    if paths.table_bucket_entry_path(&entry.table_bucket) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog table bucket entry identity does not match its object path".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_namespace_entry_object(
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

pub(super) fn validate_table_entry_object(
    paths: &TableCatalogObjectPaths,
    object: &str,
    entry: &TableEntry,
) -> TableCatalogStoreResult<Namespace> {
    validate_table_entry_version_and_id(entry)?;
    let namespace = parse_namespace_for_store(&entry.namespace)?;
    let table = parse_table_for_store(&entry.table)?;
    if paths.table_entry_path(&entry.table_bucket, &namespace, &table) != object {
        return Err(TableCatalogStoreError::Invalid(
            "catalog table entry identity does not match its object path".to_string(),
        ));
    }
    Ok(namespace)
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
    validate_catalog_entry_version("commit log", entry.version)?;
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
    validate_catalog_entry_version("commit idempotency index", entry.version)?;
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

fn validate_table_maintenance_report_owner(
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

fn validate_table_warehouse_index_entry_object(
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

fn table_warehouse_index_state_ready(state: &TableWarehouseIndexStateEntry, table_bucket: &str) -> TableCatalogStoreResult<bool> {
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

#[derive(Clone)]
pub(crate) struct ObjectTableCatalogStore<B> {
    pub(in crate::table_catalog) backend: B,
    pub(in crate::table_catalog) paths: TableCatalogObjectPaths,
}

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            paths: TableCatalogObjectPaths::default(),
        }
    }

    pub(in crate::table_catalog) fn catalog_bucket(&self) -> &'static str {
        RUSTFS_META_BUCKET
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
                if page.objects.iter().any(|object| object.ends_with(entry_file)) {
                    return Ok(true);
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

    pub(super) async fn read_entry_unlocked<T>(
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
        self.backend.put_object(bucket, object, data, precondition).await
    }

    async fn write_entry_unlocked<T>(
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
        self.backend.put_object_unlocked(bucket, object, data, precondition).await
    }

    async fn write_warehouse_index_state_unlocked(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
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
        )
        .await
    }

    pub(in crate::table_catalog) async fn warehouse_index_ready(&self, table_bucket: &str) -> TableCatalogStoreResult<bool> {
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

    async fn replace_stale_table_warehouse_index(
        &self,
        object: &str,
        stale: &TableWarehouseIndexEntry,
        replacement: &TableWarehouseIndexEntry,
        reason: &'static str,
    ) -> TableCatalogStoreResult<bool> {
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), object).await?;
        let Some((current, _)) = self
            .read_entry_unlocked::<TableWarehouseIndexEntry>(self.catalog_bucket(), object)
            .await?
        else {
            return Ok(false);
        };
        validate_table_warehouse_index_entry_object(&self.paths, object, &current)?;
        if current != *stale {
            return Ok(false);
        }
        self.delete_warehouse_index_object_unlocked(object, stale, reason).await?;
        self.write_entry_unlocked(self.catalog_bucket(), object, replacement, TableCatalogPutPrecondition::IfAbsent)
            .await?;
        Ok(true)
    }

    async fn ensure_table_warehouse_prefix_available(&self, entry: &TableEntry) -> TableCatalogStoreResult<()> {
        let candidate = table_warehouse_index_entry(entry)?;
        validate_table_entry_version_and_id(entry)?;
        for existing in self.list_all_table_entries(&candidate.table_bucket).await? {
            if existing.table_id == candidate.table_id {
                if existing.namespace != candidate.namespace || existing.table != candidate.table {
                    return Err(TableCatalogStoreError::Conflict(
                        "table id is already registered in this table bucket".to_string(),
                    ));
                }
                continue;
            }
            if existing.state != TableCatalogEntryState::Active {
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
        for object in self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.warehouse_index_entries_prefix(&candidate.table_bucket))
            .await?
        {
            if object == state_object {
                continue;
            }
            let Some((existing, _)) = self
                .read_entry::<TableWarehouseIndexEntry>(self.catalog_bucket(), &object)
                .await?
            else {
                continue;
            };
            validate_table_warehouse_index_entry_object(&self.paths, &object, &existing)?;
            if existing.table_id == candidate.table_id || existing.state != TableCatalogEntryState::Active {
                continue;
            }
            if warehouse_object_prefixes_overlap(&existing.warehouse_object_prefix, &candidate.warehouse_object_prefix)
                && self.warehouse_index_entry_has_active_owner(&existing).await?
            {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table warehouse location overlaps an active table: {}",
                    candidate.warehouse_object_prefix
                )));
            }
        }
        Ok(())
    }

    async fn reserve_table_warehouse_index(&self, entry: &TableEntry) -> TableCatalogStoreResult<WarehouseIndexReservation> {
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
        self.ensure_table_warehouse_prefix_available(entry).await?;
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
                    if self
                        .replace_stale_table_warehouse_index(&object, &existing, &index, "stale reservation conflict")
                        .await?
                    {
                        return Ok(WarehouseIndexReservation::Created);
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

    async fn fail_closed_for_broken_warehouse_index(
        &self,
        object: &str,
        index: &TableWarehouseIndexEntry,
        reason: &'static str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        Err(TableCatalogStoreError::Internal(format!(
            "active warehouse index {object} for {}/{}/{} ({}) is inconsistent: {reason}",
            index.table_bucket, index.namespace, index.table, index.table_id
        )))
    }

    async fn resolve_table_data_plane_resource_from_index_entry(
        &self,
        index_object: &str,
        index: TableWarehouseIndexEntry,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        validate_table_warehouse_index_entry_object(&self.paths, index_object, &index)?;
        if index.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::Internal(format!(
                "warehouse index {index_object} for {}/{}/{} is inactive while the index is authoritative",
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
            self.delete_stale_table_warehouse_index(index_object, &index, "referenced table is inactive")
                .await?;
            return Ok(None);
        }
        let current_prefix = table_warehouse_object_prefix(&table).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("warehouse index table entry has invalid location {index_object}: {err}"))
        })?;
        if current_prefix != index.warehouse_object_prefix {
            self.delete_stale_table_warehouse_index(index_object, &index, "referenced table moved warehouse prefix")
                .await?;
            return Ok(None);
        }
        if table.table_id != index.table_id {
            return self
                .fail_closed_for_broken_warehouse_index(index_object, &index, "referenced table identity changed")
                .await;
        }
        Ok(Some(table_data_plane_resource_from_entry(table, current_prefix)))
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

    async fn delete_created_table_warehouse_index(
        &self,
        entry: &TableEntry,
        reservation: WarehouseIndexReservation,
        reason: &'static str,
    ) {
        if reservation != WarehouseIndexReservation::Created {
            return;
        }
        let warehouse_object_prefix = table_warehouse_object_prefix(entry).ok();
        if let Err(err) = self.delete_table_warehouse_index(entry).await {
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

    async fn delete_owned_table_warehouse_index_for_drop(&self, entry: &TableEntry) -> TableCatalogStoreResult<()> {
        let index = table_warehouse_index_entry(entry)?;
        let object = self
            .paths
            .warehouse_index_entry_path(&index.table_bucket, &index.warehouse_object_prefix);
        validate_table_warehouse_index_entry_object(&self.paths, &object, &index)?;
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?;
        let Some((current, _)) = self
            .read_entry_unlocked::<TableWarehouseIndexEntry>(self.catalog_bucket(), &object)
            .await?
        else {
            return Ok(());
        };
        validate_table_warehouse_index_entry_object(&self.paths, &object, &current)?;
        if current != index {
            return Err(TableCatalogStoreError::Conflict(format!(
                "table warehouse index owner changed before drop: {}",
                index.warehouse_object_prefix
            )));
        }
        self.delete_warehouse_index_object_unlocked(&object, &index, "table warehouse index owner dropped")
            .await
    }

    async fn restore_table_warehouse_index_after_failed_drop(&self, entry: &TableEntry, reason: &'static str) {
        if let Err(err) = self.reserve_table_warehouse_index(entry).await {
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
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        let mut matched: Option<TableDataPlaneResource> = None;
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
            if let Some(resource) = self
                .resolve_table_data_plane_resource_from_index_entry(&index_object, index)
                .await?
            {
                if let Some(current) = matched.as_ref() {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "object {object} matches overlapping active table warehouse indexes {} and {}",
                        current.warehouse_object_prefix, resource.warehouse_object_prefix
                    )));
                }
                matched = Some(resource);
            }
        }
        Ok(matched)
    }

    pub(in crate::table_catalog) async fn backfill_active_table_warehouse_index(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
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
        self.reserve_table_warehouse_index(&current).await.map(|_| ())
    }

    pub(in crate::table_catalog) async fn backfill_table_warehouse_index(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<()> {
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let state_object = self.paths.warehouse_index_state_path(table_bucket);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &state_object).await?;
        if self.read_warehouse_index_state_unlocked(table_bucket).await? {
            return Ok(());
        }
        let tables = self.list_all_table_entries(table_bucket).await?;
        let mut table_ids = BTreeSet::new();
        if let Some(table) = tables.iter().find(|table| !table_ids.insert(table.table_id.as_str())) {
            return Err(TableCatalogStoreError::Conflict(format!(
                "table id {} is registered by multiple tables in table bucket {table_bucket}",
                table.table_id
            )));
        }
        let mut active_prefixes = Vec::new();
        for table in tables.iter().filter(|table| table.state == TableCatalogEntryState::Active) {
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
            if table.state != TableCatalogEntryState::Active {
                continue;
            }
            self.backfill_active_table_warehouse_index(&table.table_bucket, &table.namespace, &table.table)
                .await?;
        }
        self.write_warehouse_index_state_unlocked(table_bucket).await
    }

    async fn require_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        if self
            .get_table_bucket(table_bucket)
            .await?
            .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
        {
            return Ok(());
        }
        Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")))
    }

    async fn read_table_with_etag(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
    ) -> TableCatalogStoreResult<Option<(TableEntry, String)>> {
        let table_path = self.paths.table_entry_path(table_bucket, namespace, table);
        let Some((entry, etag)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &table_path).await? else {
            return Ok(None);
        };
        validate_table_entry_object(&self.paths, &table_path, &entry)?;
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
        let table = parse_table_for_store(table)?;
        self.read_table_with_etag(table_bucket, &namespace, &table)
            .await
            .map(|entry| entry.map(|(table, _)| table))
    }

    async fn list_all_table_entries(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        let mut entries = Vec::new();
        for object in self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.namespace_entries_prefix(table_bucket))
            .await?
        {
            if !object.ends_with(TABLE_ENTRY_FILE) {
                continue;
            }
            let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &object).await? else {
                continue;
            };
            validate_table_entry_object(&self.paths, &object, &entry)?;
            entries.push(entry);
        }
        entries.sort_by(|left, right| (&left.namespace, &left.table).cmp(&(&right.namespace, &right.table)));
        Ok(entries)
    }

    async fn read_table_with_etag_unlocked(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
    ) -> TableCatalogStoreResult<Option<(TableEntry, String)>> {
        let table_path = self.paths.table_entry_path(table_bucket, namespace, table);
        let Some((entry, etag)) = self
            .read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &table_path)
            .await?
        else {
            return Ok(None);
        };
        validate_table_entry_object(&self.paths, &table_path, &entry)?;
        let Some(etag) = etag else {
            return Err(TableCatalogStoreError::Internal(format!("catalog table entry has no etag: {table_path}")));
        };
        Ok(Some((entry, etag)))
    }

    async fn read_view_with_etag_unlocked(
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
        let publication = TableCommitLockPublication::new(&self.backend);
        self.write_table_entry_with_publication(entry, precondition, &publication)
            .await
    }

    async fn write_table_entry_with_publication(
        &self,
        entry: TableEntry,
        precondition: TableCatalogPutPrecondition,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        validate_table_entry_version_and_id(&entry)?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        validate_table_warehouse_location(&entry.table_bucket, &entry.warehouse_location)?;
        publication.begin_table_bucket(&entry.table_bucket).await?;
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "table registration requires a table-bucket publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        self.require_table_bucket(&entry.table_bucket).await?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&entry.table_bucket).await?;
        let namespace_path = self.paths.namespace_entry_path(&entry.table_bucket, &namespace);
        let _namespace_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &namespace_path)
            .await?;
        self.require_active_namespace_unlocked(&entry.table_bucket, &namespace, &namespace_path)
            .await?;
        let table_path = self.paths.table_entry_path(&entry.table_bucket, &namespace, &table);
        let _table_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        let view_path = self.paths.view_entry_path(&entry.table_bucket, &namespace, &table);
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
        // Preserve catalog -> publication -> object lock order across rolling upgrades.
        publication
            .prepare(&entry.table_bucket, &entry.namespace, &entry.table)
            .await?;
        if !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.table) {
            return Err(TableCatalogStoreError::Internal(
                "table registration requires a table publication fence".to_string(),
            ));
        }
        self.ensure_table_warehouse_prefix_available(&entry).await?;
        let reservation = self.reserve_table_warehouse_index(&entry).await?;
        if !publication.holds_table_bucket(&entry.table_bucket)
            || !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.table)
        {
            self.delete_created_table_warehouse_index(&entry, reservation, "table publication fence lost")
                .await;
            return Err(TableCatalogStoreError::Internal(
                "table registration publication fence was lost before catalog update".to_string(),
            ));
        }
        let result = self
            .write_entry_unlocked(self.catalog_bucket(), &table_path, &entry, precondition)
            .await;
        if result.is_err() {
            self.delete_created_table_warehouse_index(&entry, reservation, "table entry write failed")
                .await;
        }
        result
    }

    async fn write_view_entry(&self, entry: ViewEntry, precondition: TableCatalogPutPrecondition) -> TableCatalogStoreResult<()> {
        let publication = TableCommitLockPublication::new(&self.backend);
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
        publication.begin_table_bucket(&entry.table_bucket).await?;
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "view creation requires a table-bucket publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        self.require_table_bucket(&entry.table_bucket).await?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let view = parse_table_for_store(&entry.view)?;
        validate_view_warehouse_location(&entry.table_bucket, &entry.warehouse_location)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&entry.table_bucket).await?;
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
        self.write_entry_unlocked(self.catalog_bucket(), &view_path, &entry, precondition)
            .await
    }

    pub(crate) async fn get_external_catalog_bridge(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<Option<ExternalCatalogBridgeEntry>> {
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

    pub(super) async fn table_commit_recovery_report_for_entry(
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
            commits.push(table_commit_recovery_entry(
                entry,
                commit_log,
                idempotency_commit.as_ref(),
                history.proves_committed(commit_log),
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

    pub(crate) async fn plan_table_commit_recovery(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
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
        self.table_commit_recovery_report_for_entry(&entry, 0).await
    }

    pub(crate) async fn recover_table_commits(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
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
            let recovery_entry = table_commit_recovery_entry(
                &entry,
                commit_log,
                idempotency_commit.as_ref(),
                history.proves_committed(commit_log),
            );
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

        let config_path = self
            .paths
            .table_maintenance_config_path(table_bucket, &namespace, &table, &entry.table_id);
        let config = self
            .read_entry::<TableMaintenanceConfig>(self.catalog_bucket(), &config_path)
            .await?
            .map(|(config, _)| config)
            .unwrap_or_default();
        validate_table_maintenance_config(&config)?;
        Ok(config)
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) async fn put_table_bucket_maintenance_config(
        &self,
        table_bucket: &str,
        config: TableMaintenanceConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceConfig> {
        validate_table_maintenance_config(&config)?;
        self.require_table_bucket(table_bucket).await?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let config_path = self.paths.table_bucket_maintenance_config_path(table_bucket);
        self.write_entry(self.catalog_bucket(), &config_path, &config, TableCatalogPutPrecondition::Any)
            .await?;
        Ok(config)
    }

    pub(crate) async fn get_effective_table_maintenance_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableMaintenanceEffectiveConfig> {
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
    }

    async fn get_effective_table_maintenance_config_for_entry_unlocked(
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

        let config_path = self
            .paths
            .table_maintenance_config_path(table_bucket, &namespace, &table, &entry.table_id);
        self.write_entry(self.catalog_bucket(), &config_path, &config, TableCatalogPutPrecondition::Any)
            .await?;
        Ok(config)
    }

    pub(crate) async fn put_table_metadata_maintenance_report(
        &self,
        report: &TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<()> {
        let _migration_guard = self
            .acquire_object_backed_catalog_write_permit(&report.job.table_bucket)
            .await?;
        self.put_table_metadata_maintenance_report_unfenced(report).await
    }

    async fn put_table_metadata_maintenance_report_unfenced(
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

    async fn put_table_metadata_maintenance_report_for_entry(
        &self,
        report: &TableMetadataMaintenanceReport,
        entry: &TableEntry,
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
        self.write_entry(self.catalog_bucket(), &job_path, &report, TableCatalogPutPrecondition::Any)
            .await?;
        self.write_entry(self.catalog_bucket(), &latest_job_path, &report, TableCatalogPutPrecondition::Any)
            .await?;
        self.write_entry(self.catalog_bucket(), &current_job_path, &report, TableCatalogPutPrecondition::Any)
            .await
    }

    pub(crate) async fn get_table_metadata_maintenance_report(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        job_id: &str,
    ) -> TableCatalogStoreResult<Option<TableMetadataMaintenanceReport>> {
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

        self.get_table_metadata_maintenance_report_for_entry_unlocked(table_bucket, &namespace, &table, &entry.table_id, job_id)
            .await
    }

    async fn get_table_metadata_maintenance_report_for_entry_unlocked(
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
        let effective = self
            .get_effective_table_maintenance_config(table_bucket, &namespace.public_name(), table.as_str())
            .await?;
        let current = self
            .get_table_metadata_maintenance_report(
                table_bucket,
                &namespace.public_name(),
                table.as_str(),
                MAINTENANCE_JOB_ALIAS_CURRENT,
            )
            .await?;
        let reports = self
            .list_table_metadata_maintenance_audit_reports(table_bucket, &namespace, &table, &entry.table_id)
            .await?;
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
            table_bucket: table_bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            table_id: entry.table_id,
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
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
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

    async fn list_table_metadata_maintenance_audit_reports(
        &self,
        table_bucket: &str,
        namespace: &Namespace,
        table: &IdentifierSegment,
        table_id: &str,
    ) -> TableCatalogStoreResult<Vec<TableMetadataMaintenanceReport>> {
        let jobs_prefix = self
            .paths
            .table_maintenance_jobs_prefix(table_bucket, namespace, table, table_id);
        let mut reports = Vec::new();
        for object in self.backend.list_objects(self.catalog_bucket(), &jobs_prefix).await? {
            if !object.ends_with(".json") {
                continue;
            }
            if let Some((report, _)) = self
                .read_entry::<TableMetadataMaintenanceReport>(self.catalog_bucket(), &object)
                .await?
            {
                validate_table_maintenance_report_owner(&report, table_bucket, namespace, table, table_id)?;
                if self
                    .paths
                    .table_maintenance_job_path(table_bucket, namespace, table, table_id, &report.job.job_id)
                    != object
                {
                    return Err(TableCatalogStoreError::Invalid(
                        "table maintenance report identity does not match its object path".to_string(),
                    ));
                }
                reports.push(table_maintenance_report_with_recommended_actions(report));
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

    async fn table_metadata_maintenance_scheduler_preflight(
        &self,
        context: TableMaintenancePreflightContext<'_>,
        scheduler_id: &str,
        now: OffsetDateTime,
        effective: TableMaintenanceEffectiveConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceSchedulerPreflight> {
        if !effective.config.background_enabled {
            let report = self
                .put_table_metadata_maintenance_scheduler_control_report(TableMaintenanceSchedulerControlReport {
                    table_bucket: context.table_bucket,
                    namespace: context.namespace,
                    table: context.table,
                    entry: context.entry,
                    scheduler_id: scheduler_id.to_string(),
                    effective: &effective,
                    status: TableMetadataMaintenanceJobStatus::Disabled,
                    reason: "background maintenance is disabled",
                    now,
                })
                .await?;
            return Ok(TableMaintenanceSchedulerPreflight::Complete(Box::new(report)));
        }
        if effective.config.worker_paused {
            let report = self
                .put_table_metadata_maintenance_scheduler_control_report(TableMaintenanceSchedulerControlReport {
                    table_bucket: context.table_bucket,
                    namespace: context.namespace,
                    table: context.table,
                    entry: context.entry,
                    scheduler_id: scheduler_id.to_string(),
                    effective: &effective,
                    status: TableMetadataMaintenanceJobStatus::Paused,
                    reason: "background maintenance worker is paused",
                    now,
                })
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
                )
                .await?;
            } else if table_maintenance_job_retry_is_pending(&current.job, now) {
                return Ok(TableMaintenanceSchedulerPreflight::Complete(Box::new(current)));
            }
        }

        Ok(TableMaintenanceSchedulerPreflight::Ready(effective))
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
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
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

        let (report, effective, delete) = {
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

            let delete = matches!(report.job.operation, TableMetadataMaintenanceOperation::Delete);
            (report, effective, delete)
        };

        self.finish_table_metadata_maintenance_run(table_bucket, &namespace_name, &table_name, delete, &effective, report)
            .await
    }

    async fn table_metadata_maintenance_worker_preflight(
        &self,
        context: TableMaintenancePreflightContext<'_>,
        worker_id: &str,
        now: OffsetDateTime,
        effective: TableMaintenanceEffectiveConfig,
    ) -> TableCatalogStoreResult<TableMaintenanceWorkerPreflight> {
        if !effective.config.background_enabled {
            let report = self
                .put_table_metadata_maintenance_worker_control_report(TableMaintenanceWorkerControlReport {
                    table_bucket: context.table_bucket,
                    namespace: context.namespace,
                    table: context.table,
                    entry: context.entry,
                    worker_id: worker_id.to_string(),
                    effective: &effective,
                    status: TableMetadataMaintenanceJobStatus::Disabled,
                    reason: "background maintenance is disabled",
                    now,
                })
                .await?;
            return Ok(TableMaintenanceWorkerPreflight::Complete(Box::new(report)));
        }
        if effective.config.worker_paused {
            let report = self
                .put_table_metadata_maintenance_worker_control_report(TableMaintenanceWorkerControlReport {
                    table_bucket: context.table_bucket,
                    namespace: context.namespace,
                    table: context.table,
                    entry: context.entry,
                    worker_id: worker_id.to_string(),
                    effective: &effective,
                    status: TableMetadataMaintenanceJobStatus::Paused,
                    reason: "background maintenance worker is paused",
                    now,
                })
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
        let Some(mut report) = self
            .get_table_metadata_maintenance_report_for_entry_unlocked(
                heartbeat.table_bucket,
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
        self.put_table_metadata_maintenance_report_for_entry(&report, &entry).await?;
        Ok(report)
    }

    async fn expire_table_maintenance_job(
        &self,
        mut report: TableMetadataMaintenanceReport,
        entry: &TableEntry,
        now: OffsetDateTime,
        reason: &str,
        action: TableMaintenanceAuditAction,
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
        self.put_table_metadata_maintenance_report_for_entry(&report, entry).await?;
        Ok(report)
    }

    async fn put_table_metadata_maintenance_scheduler_control_report(
        &self,
        control: TableMaintenanceSchedulerControlReport<'_>,
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
        self.put_table_metadata_maintenance_report_for_entry(&report, control.entry)
            .await?;
        Ok(report)
    }

    async fn put_table_metadata_maintenance_worker_control_report(
        &self,
        control: TableMaintenanceWorkerControlReport<'_>,
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
        self.put_table_metadata_maintenance_report_for_entry(&report, control.entry)
            .await?;
        Ok(report)
    }

    pub(crate) async fn plan_table_snapshot_expiration(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableSnapshotExpirationConfig,
    ) -> TableCatalogStoreResult<TableSnapshotExpirationReport> {
        self.plan_table_snapshot_expiration_with_backend(&self.backend, table_bucket, namespace, table, config)
            .await
    }

    pub(crate) async fn plan_table_snapshot_expiration_with_backend<P>(
        &self,
        metadata_backend: &P,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableSnapshotExpirationConfig,
    ) -> TableCatalogStoreResult<TableSnapshotExpirationReport>
    where
        P: TableCatalogObjectBackend,
    {
        validate_table_snapshot_expiration_config(&config)?;
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
        if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let Some(current_metadata) = read_table_metadata_value(metadata_backend, table_bucket, &entry.metadata_location).await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };

        Ok(table_snapshot_expiration_report(
            table_bucket,
            &namespace,
            &table,
            &entry,
            &current_metadata,
            config,
            OffsetDateTime::now_utc(),
        ))
    }

    pub(crate) async fn plan_table_compaction(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableCompactionPlanningConfig,
    ) -> TableCatalogStoreResult<TableCompactionPlanningReport> {
        validate_table_compaction_planning_config(&config)?;
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
        if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let Some(current_metadata) = read_table_metadata_value(&self.backend, table_bucket, &entry.metadata_location).await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };

        table_compaction_planning_report(&self.backend, table_bucket, &namespace, &table, &entry, &current_metadata, config).await
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) async fn commit_table_compaction(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableCompactionPlanningConfig,
    ) -> TableCatalogStoreResult<TableCompactionPlanningReport> {
        let publication = TableCommitLockPublication::new(&self.backend);
        self.commit_table_compaction_with_publication(&self.backend, &publication, table_bucket, namespace, table, config)
            .await
    }

    pub(crate) async fn commit_table_compaction_with_publication<P>(
        &self,
        object_backend: &P,
        publication: &(dyn TableCommitPublication + Sync),
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableCompactionPlanningConfig,
    ) -> TableCatalogStoreResult<TableCompactionPlanningReport>
    where
        P: TableCatalogObjectBackend,
    {
        validate_table_compaction_planning_config(&config)?;
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
        if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let Some(current_metadata) = read_table_metadata_value(object_backend, table_bucket, &entry.metadata_location).await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };
        let mut report =
            table_compaction_planning_report(object_backend, table_bucket, &namespace, &table, &entry, &current_metadata, config)
                .await?;
        if report.status != TableCompactionPlanningStatus::RewriteCandidates {
            return Err(TableCatalogStoreError::Invalid("compaction has no safe rewrite candidates".to_string()));
        }
        let current_data_files =
            compaction_current_data_files(object_backend, table_bucket, &namespace, &table, &entry, &current_metadata).await?;
        let current_data_files_by_key = current_data_files
            .iter()
            .map(|file| (file.object_key.as_str(), file))
            .collect::<BTreeMap<_, _>>();
        let rewritten_inputs = report
            .rewrite_groups
            .iter()
            .flat_map(|group| group.input_file_locations.iter().cloned())
            .collect::<BTreeSet<_>>();
        let mut manifest_data_files = current_data_files
            .iter()
            .filter(|file| !rewritten_inputs.contains(&file.object_key))
            .cloned()
            .collect::<Vec<_>>();

        let now = OffsetDateTime::now_utc();
        let snapshot_id = compaction_snapshot_id(&current_metadata, &entry, now);
        let sequence_number = next_compaction_sequence_number(&current_metadata);
        let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
        let warehouse_object_prefix = table_warehouse_object_prefix(&entry)?;
        let compaction_id = Uuid::new_v4().to_string();
        let mut compacted_files = Vec::with_capacity(report.rewrite_groups.len());
        for rewrite_group in &mut report.rewrite_groups {
            let output_prefix = rewrite_group
                .input_file_locations
                .first()
                .and_then(|input| compaction_data_file_rewrite_prefix(&namespace, &table, Some(&warehouse_object_prefix), input))
                .ok_or_else(|| TableCatalogStoreError::Invalid("compaction rewrite group has no input files".to_string()))?;
            let output_file = format!("{output_prefix}/compaction-{compaction_id}-{}.parquet", rewrite_group.group_id);
            let output_file_path = table_object_s3_location(table_bucket, &output_file);
            let (partition_spec_id, partition) = compaction_rewrite_group_partition(&current_data_files_by_key, rewrite_group)?;
            let sort_order_id = compaction_rewrite_group_sort_order(&current_data_files_by_key, rewrite_group)?;
            let mut input_files = Vec::with_capacity(rewrite_group.input_file_locations.len());
            for input_file in &rewrite_group.input_file_locations {
                let Some(input_object) = object_backend.read_object(table_bucket, input_file).await? else {
                    return Err(TableCatalogStoreError::NotFound(format!("compaction input data file {input_file}")));
                };
                input_files.push((input_file.clone(), input_object.data));
            }
            let compacted_file = compact_parquet_data_files(&input_files)?;
            let output_bytes = u64::try_from(compacted_file.data.len()).unwrap_or(u64::MAX);
            object_backend
                .put_object(table_bucket, &output_file, compacted_file.data, TableCatalogPutPrecondition::IfAbsent)
                .await?;
            rewrite_group.output_file_location = Some(output_file_path.clone());
            rewrite_group.output_bytes = Some(output_bytes);
            compacted_files.push(CompactedDataFile {
                object_key: output_file,
                file_path: output_file_path,
                file_size_bytes: output_bytes,
                record_count: compacted_file.record_count,
                partition_spec_id,
                partition,
                sort_order_id,
                status: 1,
                snapshot_id,
                sequence_number,
                file_sequence_number: sequence_number,
            });
        }
        manifest_data_files.extend(compacted_files.iter().cloned());

        let new_manifest = format!("{metadata_dir}/manifest-compaction-{compaction_id}.avro");
        let new_manifest_list = format!("{metadata_dir}/snap-{snapshot_id}-compaction-{compaction_id}.avro");
        let new_metadata =
            default_table_metadata_file_path(&namespace, &table, &format!("compaction-{compaction_id}.metadata.json"));
        let manifest_data = compacted_manifest_avro_bytes(&manifest_data_files)?;
        let manifest_length = u64::try_from(manifest_data.len()).unwrap_or(u64::MAX);
        object_backend
            .put_object(table_bucket, &new_manifest, manifest_data, TableCatalogPutPrecondition::IfAbsent)
            .await?;
        let added_files_count = compacted_files.len();
        let added_rows_count = compacted_files
            .iter()
            .fold(0_u64, |rows, file| rows.saturating_add(file.record_count));
        let existing_files_count = manifest_data_files.len().saturating_sub(added_files_count);
        let existing_rows_count = manifest_data_files
            .iter()
            .filter(|file| file.status == 0)
            .fold(0_u64, |rows, file| rows.saturating_add(file.record_count));
        let manifest_list_data = compacted_manifest_list_avro_bytes(CompactionManifestListSummary {
            manifest_path: &new_manifest,
            manifest_length,
            partition_spec_id: compaction_manifest_partition_spec_id(&manifest_data_files)?,
            snapshot_id,
            sequence_number,
            added_files_count,
            existing_files_count,
            added_rows_count,
            existing_rows_count,
        })?;
        object_backend
            .put_object(
                table_bucket,
                &new_manifest_list,
                manifest_list_data,
                TableCatalogPutPrecondition::IfAbsent,
            )
            .await?;
        let new_metadata_data = compaction_metadata_json(
            &current_metadata,
            &entry,
            snapshot_id,
            sequence_number,
            &new_manifest_list,
            &entry.metadata_location,
            now,
        )?;
        object_backend
            .put_object(table_bucket, &new_metadata, new_metadata_data, TableCatalogPutPrecondition::IfAbsent)
            .await?;

        let commit_result = self
            .commit_table_with_publication(
                TableCommitRequest {
                    table_bucket: table_bucket.to_string(),
                    namespace: namespace.public_name(),
                    table: table.as_str().to_string(),
                    commit_id: format!("compaction-{compaction_id}"),
                    idempotency_key: Some(format!("compaction-{compaction_id}")),
                    operation: "compaction".to_string(),
                    expected_version_token: entry.version_token,
                    expected_metadata_location: entry.metadata_location,
                    new_metadata_location: new_metadata.clone(),
                    requirements: Vec::new(),
                    writer: Some("rustfs-maintenance".to_string()),
                },
                publication,
            )
            .await?;

        report.status = TableCompactionPlanningStatus::Committed;
        report.committed_metadata_location = Some(commit_result.table.metadata_location);
        for snapshot in &mut report.snapshot_reports {
            if snapshot.status == TableCompactionPlanningStatus::RewriteCandidates {
                snapshot.status = TableCompactionPlanningStatus::Committed;
                if !snapshot.reasons.contains(&TableCompactionPlanningReason::CompactionCommitted) {
                    snapshot.reasons.push(TableCompactionPlanningReason::CompactionCommitted);
                }
            }
        }
        Ok(report)
    }

    pub(crate) async fn export_table_catalog_entry(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCatalogExport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;

        let table_bucket_path = self.paths.table_bucket_entry_path(table_bucket);
        let Some((table_bucket_entry, _)) = self
            .read_entry::<TableBucketEntry>(self.catalog_bucket(), &table_bucket_path)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
        };
        validate_table_bucket_entry_object(&self.paths, &table_bucket_path, &table_bucket_entry)?;
        let namespace_path = self.paths.namespace_entry_path(table_bucket, &namespace);
        let Some((namespace_entry, _)) = self
            .read_entry::<NamespaceEntry>(self.catalog_bucket(), &namespace_path)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {}/{}",
                table_bucket,
                namespace.public_name()
            )));
        };
        validate_namespace_entry_object(&self.paths, &namespace_path, &namespace_entry)?;
        let Some((table_entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        let commit_recovery = self.table_commit_recovery_report_for_entry(&table_entry, 0).await?;
        let backing_manifest = table_catalog_backing_manifest(&self.paths, &namespace, &table, &table_entry, &commit_recovery);

        Ok(TableCatalogExport {
            table_bucket: table_bucket_entry,
            namespace: namespace_entry,
            table: table_entry,
            backing_manifest,
        })
    }

    pub(crate) async fn diagnose_table_catalog(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableCatalogDiagnosticsReport> {
        let parsed_namespace = parse_namespace_for_store(namespace)?;
        let parsed_table = parse_table_for_store(table)?;
        let catalog = self.export_table_catalog_entry(table_bucket, namespace, table).await?;
        let current_metadata_location = catalog.table.metadata_location.clone();

        let mut retained = BTreeSet::new();
        let mut current_metadata_for_refs = None;
        let current_metadata_status =
            if is_valid_table_metadata_location(&parsed_namespace, &parsed_table, &current_metadata_location) {
                retained.insert(current_metadata_location.clone());
                match read_table_metadata_value(&self.backend, table_bucket, &current_metadata_location).await {
                    Ok(Some(current_metadata)) => {
                        retained.extend(metadata_log_locations(
                            &current_metadata,
                            table_bucket,
                            &parsed_namespace,
                            &parsed_table,
                        ));
                        current_metadata_for_refs = Some(current_metadata);
                        TableMetadataPointerStatus::Valid
                    }
                    Ok(None) => TableMetadataPointerStatus::MissingObject,
                    Err(TableCatalogStoreError::Invalid(_)) => TableMetadataPointerStatus::InvalidJson,
                    Err(err) => return Err(err),
                }
            } else {
                TableMetadataPointerStatus::InvalidLocation
            };

        let mut metadata_locations = Vec::new();
        let metadata_prefix = format!("{}/", default_table_metadata_dir_path(&parsed_namespace, &parsed_table));
        for object in self.backend.list_objects(table_bucket, &metadata_prefix).await? {
            if let Some(metadata_location) = metadata_location_from_metadata_file_path(&parsed_namespace, &parsed_table, &object)
            {
                metadata_locations.push(metadata_location);
            }
        }
        metadata_locations.sort();
        metadata_locations.dedup();

        for metadata_location in metadata_locations.iter().rev().take(retain_recent_metadata_files) {
            retained.insert(metadata_location.clone());
        }
        if let Some(current_metadata) = current_metadata_for_refs.as_ref() {
            retained.extend(
                metadata_locations_for_protected_snapshot_refs(
                    &self.backend,
                    table_bucket,
                    &parsed_namespace,
                    &parsed_table,
                    current_metadata,
                    &metadata_locations,
                )
                .await?,
            );
        }

        let orphan_metadata_candidate_locations = metadata_locations
            .into_iter()
            .filter(|metadata_location| !retained.contains(metadata_location))
            .collect();

        let commit_recovery = self.plan_table_commit_recovery(table_bucket, namespace, table).await?;
        let (recovery_status, recommended_actions) = table_catalog_recovery_summary(&current_metadata_status, &commit_recovery);
        let backing_manifest =
            table_catalog_backing_manifest(&self.paths, &parsed_namespace, &parsed_table, &catalog.table, &commit_recovery);

        Ok(TableCatalogDiagnosticsReport {
            catalog,
            current_metadata_status,
            recovery_status,
            recommended_actions,
            commit_recovery,
            backing_manifest,
            orphan_metadata_candidate_locations,
        })
    }

    pub(crate) async fn plan_table_metadata_maintenance(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
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
        if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let Some(current_metadata) = read_table_metadata_value(&self.backend, table_bucket, &entry.metadata_location).await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };

        let mut retained = BTreeSet::new();
        let mut maintenance_reasons = BTreeMap::<String, BTreeSet<TableMetadataMaintenanceReason>>::new();
        for metadata_location in metadata_log_locations(&current_metadata, table_bucket, &namespace, &table) {
            retained.insert(metadata_location.clone());
            insert_metadata_maintenance_reason(
                &mut maintenance_reasons,
                metadata_location,
                TableMetadataMaintenanceReason::MetadataLog,
            );
        }
        retained.insert(entry.metadata_location.clone());
        insert_metadata_maintenance_reason(
            &mut maintenance_reasons,
            entry.metadata_location.clone(),
            TableMetadataMaintenanceReason::CurrentMetadata,
        );

        let mut metadata_locations = Vec::new();
        let metadata_prefix = format!("{}/", default_table_metadata_dir_path(&namespace, &table));
        for object in self.backend.list_objects(table_bucket, &metadata_prefix).await? {
            if let Some(metadata_location) = metadata_location_from_metadata_file_path(&namespace, &table, &object) {
                metadata_locations.push(metadata_location);
            }
        }
        metadata_locations.sort();
        metadata_locations.dedup();
        let planned_metadata_file_count = metadata_locations.len();

        for metadata_location in metadata_locations.iter().rev().take(retain_recent_metadata_files) {
            retained.insert(metadata_location.clone());
            if metadata_location != &entry.metadata_location {
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::RecentMetadata,
                );
            }
        }
        for metadata_location in metadata_locations_for_protected_snapshot_refs(
            &self.backend,
            table_bucket,
            &namespace,
            &table,
            &current_metadata,
            &metadata_locations,
        )
        .await?
        {
            retained.insert(metadata_location.clone());
            insert_metadata_maintenance_reason(
                &mut maintenance_reasons,
                metadata_location,
                TableMetadataMaintenanceReason::ProtectedSnapshotRef,
            );
        }

        let cleanup_candidate_locations = metadata_locations
            .iter()
            .filter(|metadata_location| !retained.contains(metadata_location.as_str()))
            .cloned()
            .collect::<Vec<_>>();

        let now = OffsetDateTime::now_utc();
        let mut deletable_metadata_locations = Vec::new();
        for metadata_location in &cleanup_candidate_locations {
            insert_metadata_maintenance_reason(
                &mut maintenance_reasons,
                metadata_location.clone(),
                TableMetadataMaintenanceReason::NoCurrentReachability,
            );
            let Some(candidate_object) = self.backend.object_metadata(table_bucket, metadata_location).await? else {
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::SafetyWindowPending,
                );
                continue;
            };
            if metadata_candidate_is_past_safety_window(candidate_object.mod_time, now) {
                deletable_metadata_locations.push(metadata_location.clone());
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::SafetyWindowSatisfied,
                );
            } else {
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::SafetyWindowPending,
                );
            }
        }
        let warehouse_object_prefix = table_warehouse_object_prefix(&entry).ok();
        let current_metadata_location = entry.metadata_location;
        let retained_metadata_locations = retained.into_iter().collect::<Vec<_>>();
        let object_reports = metadata_maintenance_object_reports(maintenance_reasons);
        let referenced_object_reports = metadata_maintenance_referenced_object_reports(
            &self.backend,
            table_bucket,
            &namespace,
            &table,
            warehouse_object_prefix.as_deref(),
            &current_metadata,
            &retained_metadata_locations,
        )
        .await?;
        let reachability_graph =
            metadata_maintenance_reachability_graph_report(planned_metadata_file_count, &referenced_object_reports);
        let (planned_object_file_count, cleanup_object_candidate_locations, deletable_object_locations, object_cleanup_reports) =
            metadata_maintenance_object_cleanup_reports(
                &self.backend,
                table_bucket,
                &namespace,
                &table,
                warehouse_object_prefix.as_deref(),
                &referenced_object_reports,
                now,
            )
            .await?;

        let mut report = table_maintenance_report_with_recommended_actions(TableMetadataMaintenanceReport {
            job: TableMetadataMaintenanceJob {
                job_id: Uuid::new_v4().to_string(),
                table_bucket: table_bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                table_id: entry.table_id,
                operation: TableMetadataMaintenanceOperation::DryRun,
                status: TableMetadataMaintenanceJobStatus::Successful,
                failure_reason: None,
                recommended_actions: Vec::new(),
                config_source: TableMaintenanceConfigSource::Default,
                scheduler_id: None,
                scheduler_lease_id: String::new(),
                scheduled_at: None,
                worker_id: None,
                lease_id: String::new(),
                attempt: 0,
                max_retry_attempts: 0,
                next_retry_after: None,
                quarantine_enabled: false,
                quarantine_retention_seconds: 0,
                heartbeat_at: None,
                started_at: None,
                finished_at: None,
                current_metadata_location: current_metadata_location.clone(),
                current_generation: entry.generation,
                retain_recent_metadata_files,
                safety_window_seconds: TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS,
                cleanup_watermark_unix_seconds: (now - Duration::seconds(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS))
                    .unix_timestamp(),
                planned_metadata_file_count,
                retained_metadata_file_count: retained_metadata_locations.len(),
                cleanup_candidate_count: cleanup_candidate_locations.len(),
                deletable_metadata_file_count: deletable_metadata_locations.len(),
                deleted_metadata_file_count: 0,
                planned_object_file_count,
                cleanup_candidate_object_count: cleanup_object_candidate_locations.len(),
                deletable_object_count: deletable_object_locations.len(),
                deleted_object_count: 0,
                quarantined_object_count: 0,
            },
            current_metadata_location,
            retained_metadata_locations,
            cleanup_candidate_locations,
            deletable_metadata_locations,
            cleanup_object_candidate_locations,
            deletable_object_locations,
            object_reports,
            object_cleanup_reports,
            referenced_object_reports,
            reachability_graph,
            snapshot_expiration: None,
            compaction: None,
            audit_events: Vec::new(),
        });
        push_table_maintenance_audit_event(
            &mut report,
            now,
            TableMaintenanceAuditActor::Scheduler,
            TableMaintenanceAuditAction::Planned,
            None,
            None,
            None,
        );
        Ok(report)
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) async fn delete_table_metadata_maintenance_candidates(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let report = self
            .plan_table_metadata_maintenance(table_bucket, namespace, table, retain_recent_metadata_files)
            .await?;
        self.delete_table_metadata_maintenance_report(table_bucket, namespace, table, report)
            .await
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) async fn run_table_metadata_maintenance(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        worker_id: Option<String>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let effective = self
            .get_effective_table_maintenance_config(table_bucket, namespace, table)
            .await?;
        self.run_table_metadata_maintenance_with_config(table_bucket, namespace, table, delete, worker_id, effective)
            .await
    }

    pub(crate) async fn run_table_metadata_maintenance_with_retention(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        worker_id: Option<String>,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let mut effective = self
            .get_effective_table_maintenance_config(table_bucket, namespace, table)
            .await?;
        effective.config.retain_recent_metadata_files = retain_recent_metadata_files;
        self.run_table_metadata_maintenance_with_config(table_bucket, namespace, table, delete, worker_id, effective)
            .await
    }

    async fn run_table_metadata_maintenance_with_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        worker_id: Option<String>,
        effective: TableMaintenanceEffectiveConfig,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let mut report = self
            .plan_table_metadata_maintenance(table_bucket, namespace, table, effective.config.retain_recent_metadata_files)
            .await?;

        let started_at_time = OffsetDateTime::now_utc();
        let started_at = maintenance_timestamp(started_at_time);
        report.job.operation = if delete {
            TableMetadataMaintenanceOperation::Delete
        } else {
            TableMetadataMaintenanceOperation::DryRun
        };
        report.job.status = TableMetadataMaintenanceJobStatus::Running;
        report.job.failure_reason = None;
        report.job.config_source = effective.source;
        report.job.worker_id = worker_id;
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
            started_at_time,
            TableMaintenanceAuditActor::Worker,
            TableMaintenanceAuditAction::WorkerStarted,
            None,
            Some(TableMetadataMaintenanceJobStatus::Successful),
            Some(0),
        );
        self.put_table_metadata_maintenance_report_unfenced(&report).await?;

        self.finish_table_metadata_maintenance_run(table_bucket, namespace, table, delete, &effective, report)
            .await
    }

    async fn finish_table_metadata_maintenance_run(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        effective: &TableMaintenanceEffectiveConfig,
        mut report: TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        if delete && !effective.config.delete_enabled {
            let finished_at = OffsetDateTime::now_utc();
            let before_status = Some(report.job.status.clone());
            let before_quarantined_object_count = Some(report.job.quarantined_object_count);
            report.job.status = TableMetadataMaintenanceJobStatus::Failed;
            report.job.failure_reason = Some(TABLE_MAINTENANCE_DELETE_DISABLED_REASON.to_string());
            apply_maintenance_retry_after(&mut report.job, &effective.config, finished_at);
            report.job.finished_at = Some(maintenance_timestamp(finished_at));
            refresh_table_maintenance_report_recommended_actions(&mut report);
            push_table_maintenance_audit_event(
                &mut report,
                finished_at,
                TableMaintenanceAuditActor::Worker,
                TableMaintenanceAuditAction::WorkerFailed,
                Some(TABLE_MAINTENANCE_DELETE_DISABLED_REASON.to_string()),
                before_status,
                before_quarantined_object_count,
            );
            self.put_table_metadata_maintenance_report_unfenced(&report).await?;
            return Ok(report);
        }

        if delete {
            let running_report = report.clone();
            let mut deleted = match self
                .delete_table_metadata_maintenance_report_unfenced(table_bucket, namespace, table, report)
                .await
            {
                Ok(report) => report,
                Err(err) => {
                    let finished_at = OffsetDateTime::now_utc();
                    let mut failed = running_report;
                    let before_status = Some(failed.job.status.clone());
                    let before_quarantined_object_count = Some(failed.job.quarantined_object_count);
                    let reason = err.to_string();
                    failed.job.status = TableMetadataMaintenanceJobStatus::Failed;
                    failed.job.failure_reason = Some(reason.clone());
                    apply_maintenance_retry_after(&mut failed.job, &effective.config, finished_at);
                    failed.job.finished_at = Some(maintenance_timestamp(finished_at));
                    refresh_table_maintenance_report_recommended_actions(&mut failed);
                    push_table_maintenance_audit_event(
                        &mut failed,
                        finished_at,
                        TableMaintenanceAuditActor::Worker,
                        TableMaintenanceAuditAction::WorkerFailed,
                        Some(reason),
                        before_status,
                        before_quarantined_object_count,
                    );
                    self.put_table_metadata_maintenance_report_unfenced(&failed).await?;
                    return Err(err);
                }
            };
            let finished_at = OffsetDateTime::now_utc();
            let before_status = Some(TableMetadataMaintenanceJobStatus::Running);
            let before_quarantined_object_count = Some(0);
            deleted.job.finished_at = Some(maintenance_timestamp(finished_at));
            refresh_table_maintenance_report_recommended_actions(&mut deleted);
            push_table_maintenance_audit_event(
                &mut deleted,
                finished_at,
                TableMaintenanceAuditActor::Worker,
                TableMaintenanceAuditAction::WorkerSucceeded,
                None,
                before_status,
                before_quarantined_object_count,
            );
            self.put_table_metadata_maintenance_report_unfenced(&deleted).await?;
            return Ok(deleted);
        }

        let finished_at = OffsetDateTime::now_utc();
        let before_status = Some(report.job.status.clone());
        let before_quarantined_object_count = Some(report.job.quarantined_object_count);
        report.job.status = TableMetadataMaintenanceJobStatus::Successful;
        report.job.finished_at = Some(maintenance_timestamp(finished_at));
        refresh_table_maintenance_report_recommended_actions(&mut report);
        push_table_maintenance_audit_event(
            &mut report,
            finished_at,
            TableMaintenanceAuditActor::Worker,
            TableMaintenanceAuditAction::WorkerSucceeded,
            None,
            before_status,
            before_quarantined_object_count,
        );
        self.put_table_metadata_maintenance_report_unfenced(&report).await?;
        Ok(report)
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(in crate::table_catalog) async fn delete_table_metadata_maintenance_report(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        report: TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        self.delete_table_metadata_maintenance_report_unfenced(table_bucket, namespace, table, report)
            .await
    }

    async fn delete_table_metadata_maintenance_report_unfenced(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        report: TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        if !is_valid_table_metadata_location(&namespace, &table, &report.current_metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "maintenance report current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        let publication_lock = default_table_publication_lock_path(&namespace, &table);
        let _publication_guard = self.backend.acquire_write_lock(table_bucket, &publication_lock).await?;
        let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        if entry.metadata_location != report.current_metadata_location {
            return Err(TableCatalogStoreError::Conflict(
                "current metadata location changed before maintenance delete".to_string(),
            ));
        }
        let warehouse_object_prefix = table_warehouse_object_prefix(&entry).ok();

        let Some(current_metadata) = read_table_metadata_value(&self.backend, table_bucket, &entry.metadata_location).await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };

        let mut protected = metadata_log_locations(&current_metadata, table_bucket, &namespace, &table);
        protected.insert(entry.metadata_location.clone());
        protected.extend(report.retained_metadata_locations.iter().cloned());
        protected.extend(
            metadata_locations_for_protected_snapshot_refs(
                &self.backend,
                table_bucket,
                &namespace,
                &table,
                &current_metadata,
                &report.cleanup_candidate_locations,
            )
            .await?,
        );

        let cleanup_candidate_count = report.cleanup_candidate_locations.len();
        let planned_deletable_locations = report.deletable_metadata_locations.iter().cloned().collect::<BTreeSet<_>>();
        let mut cleanup_candidate_locations = BTreeSet::new();
        let now = OffsetDateTime::now_utc();
        for metadata_location in &report.cleanup_candidate_locations {
            if !is_valid_table_metadata_location(&namespace, &table, metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "cleanup candidate {metadata_location} must be inside the table metadata directory"
                )));
            }
            if protected.contains(metadata_location.as_str()) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "cleanup candidate {metadata_location} is retained by current metadata"
                )));
            }
            let Some(candidate_object) = self.backend.object_metadata(table_bucket, metadata_location).await? else {
                continue;
            };
            if !planned_deletable_locations.contains(metadata_location.as_str()) {
                continue;
            }
            if !metadata_candidate_is_past_safety_window(candidate_object.mod_time, now) {
                continue;
            }
            cleanup_candidate_locations.insert(metadata_location.clone());
        }

        let cleanup_candidate_locations = cleanup_candidate_locations.into_iter().collect::<Vec<_>>();
        let deleted_locations = cleanup_candidate_locations.iter().cloned().collect::<BTreeSet<_>>();
        for metadata_location in &cleanup_candidate_locations {
            self.backend.delete_object(table_bucket, metadata_location).await?;
        }

        let referenced_object_reports = metadata_maintenance_referenced_object_reports(
            &self.backend,
            table_bucket,
            &namespace,
            &table,
            warehouse_object_prefix.as_deref(),
            &current_metadata,
            &report.retained_metadata_locations,
        )
        .await?;
        let referenced_object_locations = if referenced_object_reports
            .iter()
            .any(|report| report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired)
        {
            BTreeSet::new()
        } else {
            referenced_object_reports
                .iter()
                .filter_map(|report| table_catalog_object_key_from_location(table_bucket, &report.object_location))
                .collect::<BTreeSet<_>>()
        };
        let planned_deletable_object_locations = report.deletable_object_locations.iter().cloned().collect::<BTreeSet<_>>();
        let mut cleanup_object_candidate_locations = BTreeSet::new();
        if !referenced_object_reports
            .iter()
            .any(|report| report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired)
        {
            for object_location in &report.cleanup_object_candidate_locations {
                if table_maintenance_object_kind(&namespace, &table, warehouse_object_prefix.as_deref(), object_location)
                    .is_none()
                {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "cleanup object candidate {object_location} must be inside table metadata, data, or delete directories"
                    )));
                }
                if referenced_object_locations.contains(object_location.as_str()) {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "cleanup object candidate {object_location} is retained by current metadata"
                    )));
                }
                let Some(candidate_object) = self.backend.read_object(table_bucket, object_location).await? else {
                    continue;
                };
                if !planned_deletable_object_locations.contains(object_location.as_str()) {
                    continue;
                }
                if !metadata_candidate_is_past_safety_window(candidate_object.mod_time, now) {
                    continue;
                }
                cleanup_object_candidate_locations.insert(object_location.clone());
            }
        }

        let cleanup_object_candidate_locations = cleanup_object_candidate_locations.into_iter().collect::<Vec<_>>();
        let deleted_object_locations = cleanup_object_candidate_locations.iter().cloned().collect::<BTreeSet<_>>();
        for object_location in &cleanup_object_candidate_locations {
            self.backend.delete_object(table_bucket, object_location).await?;
        }

        let retained_metadata_locations = protected.into_iter().collect::<Vec<_>>();
        let mut job = report.job;
        job.operation = TableMetadataMaintenanceOperation::Delete;
        job.status = TableMetadataMaintenanceJobStatus::Successful;
        job.failure_reason = None;
        job.retained_metadata_file_count = retained_metadata_locations.len();
        job.cleanup_candidate_count = cleanup_candidate_count;
        job.deletable_metadata_file_count = planned_deletable_locations.len();
        job.deleted_metadata_file_count = cleanup_candidate_locations.len();
        job.cleanup_candidate_object_count = report.cleanup_object_candidate_locations.len();
        job.deletable_object_count = planned_deletable_object_locations.len();
        job.deleted_object_count = cleanup_object_candidate_locations.len();
        let mut object_reports = report.object_reports;
        mark_deleted_metadata_object_reports(&mut object_reports, &deleted_locations);
        let mut object_cleanup_reports = report.object_cleanup_reports;
        mark_deleted_object_cleanup_reports(&mut object_cleanup_reports, &deleted_object_locations);

        Ok(table_maintenance_report_with_recommended_actions(TableMetadataMaintenanceReport {
            job,
            current_metadata_location: entry.metadata_location,
            retained_metadata_locations,
            cleanup_candidate_locations: cleanup_candidate_locations.clone(),
            deletable_metadata_locations: cleanup_candidate_locations,
            cleanup_object_candidate_locations: cleanup_object_candidate_locations.clone(),
            deletable_object_locations: cleanup_object_candidate_locations,
            object_reports,
            object_cleanup_reports,
            referenced_object_reports,
            reachability_graph: report.reachability_graph,
            snapshot_expiration: report.snapshot_expiration,
            compaction: report.compaction,
            audit_events: report.audit_events,
        }))
    }
}

#[async_trait::async_trait]
impl<B> TableCatalogStore for ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    async fn get_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<TableBucketEntry>> {
        let object = self.paths.table_bucket_entry_path(table_bucket);
        let Some((entry, _)) = self.read_entry::<TableBucketEntry>(self.catalog_bucket(), &object).await? else {
            return Ok(None);
        };
        validate_table_bucket_entry_object(&self.paths, &object, &entry)?;
        Ok(Some(entry))
    }

    async fn put_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        validate_table_bucket_entry(&entry)?;
        let _registry_guard = self.acquire_table_bucket_registry_write_permit().await?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&entry.table_bucket).await?;
        let object = self.paths.table_bucket_entry_path(&entry.table_bucket);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?;
        self.write_entry_unlocked(self.catalog_bucket(), &object, &entry, TableCatalogPutPrecondition::Any)
            .await
    }

    async fn create_namespace(&self, entry: NamespaceEntry) -> TableCatalogStoreResult<()> {
        let namespace = validate_namespace_entry_identity(&entry)?;
        validate_namespace_properties(&entry.properties)?;
        self.require_table_bucket(&entry.table_bucket).await?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&entry.table_bucket).await?;
        let bucket_path = self.paths.table_bucket_entry_path(&entry.table_bucket);
        let _bucket_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &bucket_path).await?;
        let object = self.paths.namespace_entry_path(&entry.table_bucket, &namespace);
        let _namespace_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &object).await?;
        let precondition = match self
            .read_entry_unlocked::<NamespaceEntry>(self.catalog_bucket(), &object)
            .await?
        {
            Some((current, etag)) => {
                validate_namespace_entry_object(&self.paths, &object, &current)?;
                if current.state == TableCatalogEntryState::Active {
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
        self.write_entry_unlocked(self.catalog_bucket(), &object, &entry, precondition)
            .await
    }

    async fn list_namespaces(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        self.list_active_namespaces_with_prefix(&self.paths.namespace_entries_prefix(table_bucket))
            .await
    }

    async fn list_namespaces_page(
        &self,
        table_bucket: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        self.list_entry_page(
            &self.paths.namespace_entries_prefix(table_bucket),
            NAMESPACE_ENTRY_FILE,
            cursor,
            limit,
            |entry: &NamespaceEntry| entry.state == TableCatalogEntryState::Active,
            |object, entry: &NamespaceEntry| validate_namespace_entry_object(&self.paths, object, entry),
        )
        .await
    }

    async fn get_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Option<NamespaceEntry>> {
        let namespace = parse_namespace_for_store(namespace)?;
        let namespace_path = self.paths.namespace_entry_path(table_bucket, &namespace);
        let exact = self
            .read_entry::<NamespaceEntry>(self.catalog_bucket(), &namespace_path)
            .await?
            .map(|(entry, _)| entry);
        if let Some(entry) = exact.as_ref() {
            validate_namespace_entry_object(&self.paths, &namespace_path, entry)?;
        }
        if exact
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
    }

    async fn list_namespaces_under(&self, table_bucket: &str, parent: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        let parent = parse_namespace_for_store(parent)?;
        let prefix = format!("{}{}/", self.paths.namespace_entries_prefix(table_bucket), parent.storage_id());
        self.list_active_namespaces_with_prefix(&prefix).await
    }

    async fn list_namespace_children(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
    ) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
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
    }

    async fn list_namespace_children_page(
        &self,
        table_bucket: &str,
        parent: Option<&str>,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        self.list_namespace_children_page_inner(table_bucket, parent, cursor, limit)
            .await
    }

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(namespace)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let bucket_path = self.paths.table_bucket_entry_path(table_bucket);
        let _bucket_guard = self.backend.acquire_write_lock(self.catalog_bucket(), &bucket_path).await?;
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
        Ok(entries)
    }

    async fn list_all_tables(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        self.list_all_table_entries(table_bucket).await.map(|entries| {
            entries
                .into_iter()
                .filter(|entry| entry.state == TableCatalogEntryState::Active)
                .collect()
        })
    }

    async fn list_tables_page(
        &self,
        table_bucket: &str,
        namespace: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<TableEntry>> {
        let namespace = parse_namespace_for_store(namespace)?;
        self.list_entry_page(
            &self.paths.table_entries_prefix(table_bucket, &namespace),
            TABLE_ENTRY_FILE,
            cursor,
            limit,
            |entry: &TableEntry| entry.state == TableCatalogEntryState::Active,
            |object, entry: &TableEntry| validate_table_entry_object(&self.paths, object, entry).map(|_| ()),
        )
        .await
    }

    async fn load_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<Option<TableEntry>> {
        self.load_table_entry(table_bucket, namespace, table)
            .await
            .map(|entry| entry.filter(|table| table.state == TableCatalogEntryState::Active))
    }

    async fn resolve_table_data_plane_resource(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        if table_bucket.is_empty() || object.is_empty() {
            return Ok(None);
        }
        let Some(table_bucket_entry) = self.get_table_bucket(table_bucket).await? else {
            return Err(TableCatalogStoreError::Internal(format!(
                "object-backed catalog has no entry for table-enabled bucket {table_bucket}"
            )));
        };
        if table_bucket_entry.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::Internal(format!(
                "table-enabled bucket {table_bucket} has an inactive object-backed catalog entry"
            )));
        }

        if self.warehouse_index_ready(table_bucket).await? {
            return match self
                .resolve_table_data_plane_resource_from_index(table_bucket, object)
                .await?
            {
                Some(resource) => Ok(Some(resource)),
                None => scan_table_data_plane_resource_for_object(self, table_bucket, object).await,
            };
        }

        match self.backfill_table_warehouse_index(table_bucket).await {
            Ok(()) => match self
                .resolve_table_data_plane_resource_from_index(table_bucket, object)
                .await?
            {
                Some(resource) => Ok(Some(resource)),
                None => scan_table_data_plane_resource_for_object(self, table_bucket, object).await,
            },
            Err(err @ TableCatalogStoreError::Internal(_)) => {
                tracing::warn!(
                    table_bucket = %table_bucket,
                    error = %err,
                    "failed to backfill table warehouse index; falling back to catalog scan"
                );
                scan_table_data_plane_resource_for_object(self, table_bucket, object).await
            }
            Err(err) => Err(err),
        }
    }

    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult> {
        let publication = TableCommitLockPublication::new(&self.backend);
        publication.begin_table_bucket(&request.table_bucket).await?;
        self.commit_table_with_publication(request, &publication).await
    }

    async fn commit_table_with_publication(
        &self,
        request: TableCommitRequest,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        let commit_started = Instant::now();
        record_table_commit_attempt(&request.operation);
        let namespace = parse_namespace_for_store(&request.namespace)?;
        let table = parse_table_for_store(&request.table)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&request.table_bucket).await?;
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
        let _publication_completion = TableCommitPublicationCompletion::new(publication);

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
            && !commit_log_matches_request(existing, &request, &current.table_id)
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
            if !commit_log_matches_request(existing, &request, &current.table_id) {
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
            let historically_committed = if matches!(existing.status, CommitLogStatus::Staged)
                && !table_matches_staged_base(&current, existing)
                && !table_matches_committed_log(&current, existing)
            {
                let commit_logs = self
                    .read_table_commit_logs(&current)
                    .await?
                    .into_iter()
                    .map(|(_, commit_log)| commit_log)
                    .collect::<Vec<_>>();
                TableCommitHistoryIndex::new(&current, commit_logs.iter()).proves_committed(existing)
            } else {
                false
            };
            if matches!(existing.status, CommitLogStatus::Committed)
                || (matches!(existing.status, CommitLogStatus::Staged)
                    && (table_matches_committed_log(&current, existing) || historically_committed))
            {
                let mut committed = existing.clone();
                committed.status = CommitLogStatus::Committed;
                let _ = self
                    .finalize_commit_log(&commit_path, idempotency_path.as_deref(), &committed)
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
        if !is_valid_table_metadata_location(&namespace, &table, &request.new_metadata_location) {
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                Err(TableCatalogStoreError::Invalid(
                    "new metadata location must be inside the table metadata directory".to_string(),
                )),
            );
        }
        let Some(new_metadata_object) = self
            .backend
            .read_object_limited(&request.table_bucket, &request.new_metadata_location, TABLE_METADATA_JSON_MAX_SIZE)
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
                    "new metadata object {}",
                    request.new_metadata_location
                ))),
            );
        };
        validate_commit_metadata_digest(&request, &new_metadata_object)?;
        let table_bucket = request.table_bucket.clone();
        let metadata_location = request.new_metadata_location.clone();
        let next_metadata_state = tokio::task::spawn_blocking(move || {
            table_metadata_commit_state(&table_bucket, &metadata_location, &new_metadata_object)
        })
        .await
        .map_err(|err| TableCatalogStoreError::Internal(format!("table metadata parser task failed: {err}")))??;
        let warehouse_relocation = next_metadata_state
            .warehouse_location
            .as_ref()
            .is_some_and(|warehouse_location| warehouse_location != &current.warehouse_location);
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

        let has_existing_commit = existing_commit.is_some();
        let mut staged_commit_log = existing_commit.unwrap_or_else(|| CommitLogEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            commit_id: request.commit_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            table_id: current.table_id.clone(),
            operation: request.operation.clone(),
            expected_version_token: request.expected_version_token.clone(),
            new_version_token: format!("token-{}", Uuid::new_v4()),
            previous_metadata_location: current.metadata_location.clone(),
            new_metadata_location: request.new_metadata_location.clone(),
            requirements: request.requirements.clone(),
            status: CommitLogStatus::Staged,
            writer: request.writer.clone(),
            created_at: None,
            updated_at: None,
        });
        staged_commit_log.status = CommitLogStatus::Staged;

        let mut next = current.clone();
        next.metadata_location = staged_commit_log.new_metadata_location.clone();
        if let Some(warehouse_location) = next_metadata_state.warehouse_location {
            next.warehouse_location = warehouse_location;
        }
        if let Some(format_version) = next_metadata_state.format_version {
            next.format_version = format_version;
        }
        next.version_token = staged_commit_log.new_version_token.clone();
        next.generation = current.generation.saturating_add(1);
        if next.warehouse_location != current.warehouse_location {
            self.ensure_table_warehouse_prefix_available(&next).await?;
        }
        let reservation = self.reserve_table_warehouse_index(&next).await?;

        let staged_write_result = async {
            if !has_existing_commit {
                self.write_entry(
                    self.catalog_bucket(),
                    &commit_path,
                    &staged_commit_log,
                    TableCatalogPutPrecondition::IfAbsent,
                )
                .await?;
            }
            if let Some(idempotency_path) = idempotency_path.as_deref()
                && existing_idempotency_commit.is_none()
            {
                self.write_entry(
                    self.catalog_bucket(),
                    idempotency_path,
                    &staged_commit_log,
                    TableCatalogPutPrecondition::IfAbsent,
                )
                .await?;
            }
            Ok(())
        }
        .await;
        if let Err(err) = staged_write_result {
            self.delete_created_table_warehouse_index(&next, reservation, "commit staging failed")
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
            self.delete_created_table_warehouse_index(&next, reservation, "table publication fence lost")
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
        let cas_result = self
            .write_entry_unlocked(
                self.catalog_bucket(),
                &table_path,
                &next,
                TableCatalogPutPrecondition::IfMatch(current_etag),
            )
            .await;
        record_table_commit_cas_result(&request.operation, cas_started, &cas_result);
        if let Err(err) = cas_result {
            self.delete_created_table_warehouse_index(&next, reservation, "table pointer CAS failed")
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
        self.delete_table_warehouse_index_if_changed(&current, &next).await;

        let mut commit_log = staged_commit_log;
        commit_log.status = CommitLogStatus::Committed;
        // After the table CAS succeeds, the staged record is the durable recovery source.
        // A finalization failure must not turn an externally committed pointer into a failed commit response.
        let _ = self
            .finalize_commit_log(&commit_path, idempotency_path.as_deref(), &commit_log)
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
    }

    async fn drop_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()> {
        let publication = TableCommitLockPublication::new(&self.backend);
        publication.begin_table_bucket(table_bucket).await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
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
        self.delete_owned_table_warehouse_index_for_drop(&entry).await?;
        if !publication.holds_table_bucket(table_bucket)
            || !publication.holds_table(table_bucket, &namespace.public_name(), table.as_str())
        {
            self.restore_table_warehouse_index_after_failed_drop(&entry, "table publication fence lost")
                .await;
            return Err(TableCatalogStoreError::Internal(
                "table drop publication fence was lost before catalog update".to_string(),
            ));
        }
        if let Err(err) = self.backend.delete_object_unlocked(self.catalog_bucket(), &object).await {
            match self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await {
                Ok(None) => return Ok(()),
                Ok(Some((current, _))) if current == entry => {
                    self.restore_table_warehouse_index_after_failed_drop(&entry, "table entry delete failed")
                        .await;
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
                }
            }
            return Err(err);
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
    }

    async fn list_views_page(
        &self,
        table_bucket: &str,
        namespace: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<ViewEntry>> {
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
    }

    async fn load_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<Option<ViewEntry>> {
        let namespace = parse_namespace_for_store(namespace)?;
        let view = parse_table_for_store(view)?;
        let object = self.paths.view_entry_path(table_bucket, &namespace, &view);
        let Some((entry, _)) = self.read_entry::<ViewEntry>(self.catalog_bucket(), &object).await? else {
            return Ok(None);
        };
        validate_view_entry_object(&self.paths, &object, &entry)?;
        Ok((entry.state == TableCatalogEntryState::Active).then_some(entry))
    }

    async fn replace_view(&self, request: ViewCommitRequest) -> TableCatalogStoreResult<ViewCommitResult> {
        let publication = TableCommitLockPublication::new(&self.backend);
        self.replace_view_with_publication(request, true, &publication).await
    }

    async fn replace_view_with_publication(
        &self,
        request: ViewCommitRequest,
        table_bucket_fence_required: bool,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<ViewCommitResult> {
        let namespace = parse_namespace_for_store(&request.namespace)?;
        let view = parse_table_for_store(&request.view)?;
        if table_bucket_fence_required {
            publication.begin_table_bucket(&request.table_bucket).await?;
            if !publication.holds_table_bucket(&request.table_bucket) {
                return Err(TableCatalogStoreError::Internal(
                    "view replacement requires a table-bucket publication fence".to_string(),
                ));
            }
        }
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(&request.table_bucket).await?;
        let namespace_path = self.paths.namespace_entry_path(&request.table_bucket, &namespace);
        let _namespace_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &namespace_path)
            .await?;
        let view_path = self.paths.view_entry_path(&request.table_bucket, &namespace, &view);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &view_path).await?;
        // Preserve catalog -> publication -> object lock order across rolling upgrades.
        publication
            .prepare(&request.table_bucket, &request.namespace, &request.view)
            .await?;
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.view) {
            return Err(TableCatalogStoreError::Internal(
                "view replacement requires a table publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        let Some((current, current_etag)) = self
            .read_view_with_etag_unlocked(&request.table_bucket, &namespace, &view)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "view {}/{}/{}",
                request.table_bucket, request.namespace, request.view
            )));
        };
        if current.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::NotFound(format!(
                "view {}/{}/{}",
                request.table_bucket, request.namespace, request.view
            )));
        }
        if current.version_token != request.expected_version_token {
            return Err(TableCatalogStoreError::Conflict(
                "current view version token does not match expected token".to_string(),
            ));
        }
        if current.metadata_location != request.expected_metadata_location {
            return Err(TableCatalogStoreError::Conflict(
                "current view metadata location does not match expected location".to_string(),
            ));
        }
        if !is_valid_view_metadata_location(&namespace, &view, &request.new_metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "new metadata location must be inside the view metadata directory".to_string(),
            ));
        }
        let Some(new_metadata_object) = self
            .backend
            .read_object_limited(&request.table_bucket, &request.new_metadata_location, TABLE_METADATA_JSON_MAX_SIZE)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "new view metadata object {}",
                request.new_metadata_location
            )));
        };
        let table_bucket = request.table_bucket.clone();
        let metadata_location = request.new_metadata_location.clone();
        let next_warehouse_location = tokio::task::spawn_blocking(move || {
            view_metadata_warehouse_location(&table_bucket, &metadata_location, &new_metadata_object)
        })
        .await
        .map_err(|err| TableCatalogStoreError::Internal(format!("view metadata parser task failed: {err}")))??;
        let warehouse_relocation = next_warehouse_location
            .as_deref()
            .is_some_and(|location| location != current.warehouse_location);
        if warehouse_relocation && !publication.holds_table_bucket(&request.table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "view warehouse relocation requires a table-bucket publication fence".to_string(),
            ));
        }

        let mut next = current;
        next.metadata_location = request.new_metadata_location;
        if let Some(warehouse_location) = next_warehouse_location {
            next.warehouse_location = warehouse_location;
        }
        next.version_token = format!("token-{}", Uuid::new_v4());
        next.generation = next.generation.saturating_add(1);
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.view)
            || ((table_bucket_fence_required || warehouse_relocation) && !publication.holds_table_bucket(&request.table_bucket))
        {
            return Err(TableCatalogStoreError::Internal(
                "view replacement publication fence was lost before catalog update".to_string(),
            ));
        }
        let write_result = self
            .write_entry_unlocked(
                self.catalog_bucket(),
                &view_path,
                &next,
                TableCatalogPutPrecondition::IfMatch(current_etag),
            )
            .await;
        if let Err(err) = write_result {
            match self
                .read_view_with_etag_unlocked(&request.table_bucket, &namespace, &view)
                .await
            {
                Ok(Some((persisted, _))) if persisted == next => {}
                Ok(_) => return Err(err),
                Err(read_err) => {
                    tracing::warn!(
                        table_bucket = %request.table_bucket,
                        namespace = %request.namespace,
                        view = %request.view,
                        error = %read_err,
                        "failed to verify view state after an ambiguous catalog update"
                    );
                    return Err(err);
                }
            }
        }
        Ok(ViewCommitResult { view: next })
    }

    async fn drop_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<()> {
        let namespace = parse_namespace_for_store(namespace)?;
        let view = parse_table_for_store(view)?;
        let _migration_guard = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
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
        self.read_commit_log_entry(table_bucket, table_id, commit_id).await
    }

    async fn get_commit_by_idempotency_key(
        &self,
        table_bucket: &str,
        table_id: &str,
        idempotency_key: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        self.read_commit_idempotency_entry(table_bucket, table_id, idempotency_key)
            .await
    }
}
