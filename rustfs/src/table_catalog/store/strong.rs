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

pub(in crate::table_catalog) type StrongNamespaceKey = (String, String);
type StrongResourceKey = (String, String, String);
type StrongCommitKey = (String, String, String);
type StrongNamespaceChildKey = (String, String, String);
type StrongWarehouseIndex = BTreeMap<String, BTreeMap<String, StrongResourceKey>>;

#[derive(Clone, Default)]
pub(in crate::table_catalog) struct StrongTableCatalogState {
    pub(super) hydrated: bool,
    pub(super) snapshot_etag: Option<String>,
    pub(super) table_buckets: BTreeMap<String, TableBucketEntry>,
    pub(in crate::table_catalog) namespaces: BTreeMap<StrongNamespaceKey, NamespaceEntry>,
    namespace_children: BTreeMap<StrongNamespaceChildKey, String>,
    namespace_objects: BTreeSet<StrongNamespaceKey>,
    pub(super) tables: BTreeMap<StrongResourceKey, TableEntry>,
    pub(super) views: BTreeMap<StrongResourceKey, ViewEntry>,
    pub(super) commits: BTreeMap<StrongCommitKey, CommitLogEntry>,
    pub(super) idempotency: BTreeMap<StrongCommitKey, CommitLogEntry>,
    pub(super) warehouse_index: StrongWarehouseIndex,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(in crate::table_catalog) struct StrongCommitSnapshotRecord {
    pub(super) table_bucket: String,
    pub(super) table_id: String,
    pub(super) lookup_key: String,
    pub(super) commit: CommitLogEntry,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(in crate::table_catalog) struct StrongTableCatalogSnapshot {
    pub(in crate::table_catalog) version: u16,
    pub(in crate::table_catalog) table_buckets: Vec<TableBucketEntry>,
    pub(in crate::table_catalog) namespaces: Vec<NamespaceEntry>,
    pub(in crate::table_catalog) tables: Vec<TableEntry>,
    pub(in crate::table_catalog) views: Vec<ViewEntry>,
    pub(in crate::table_catalog) commits: Vec<StrongCommitSnapshotRecord>,
    pub(in crate::table_catalog) idempotency: Vec<StrongCommitSnapshotRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(super) struct StrongTableCatalogBucketSnapshot {
    pub(super) table_bucket: TableBucketEntry,
    pub(super) namespaces: Vec<NamespaceEntry>,
    pub(super) tables: Vec<TableEntry>,
    pub(super) views: Vec<ViewEntry>,
    pub(super) commits: Vec<StrongCommitSnapshotRecord>,
    pub(super) idempotency: Vec<StrongCommitSnapshotRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum TableCatalogBackingMigrationFenceStatus {
    Preparing,
    Materialized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TableCatalogBackingMigrationGlobalFence {
    pub(super) version: u16,
    pub(super) migration_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TableCatalogBackingMigrationFence {
    pub(super) version: u16,
    pub(super) table_bucket: String,
    pub(super) migration_id: String,
    pub(super) status: TableCatalogBackingMigrationFenceStatus,
    pub(super) target_bucket_existed: bool,
    pub(super) source_fingerprint: Option<String>,
    pub(super) target_snapshot_etag: Option<String>,
}

pub(super) fn table_catalog_bucket_snapshot_fingerprint(
    snapshot: &StrongTableCatalogBucketSnapshot,
) -> TableCatalogStoreResult<String> {
    let data = serde_json::to_vec(snapshot)
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode catalog migration snapshot: {err}")))?;
    Ok(hex_simd::encode_to_string(Sha256::digest(data), hex_simd::AsciiCase::Lower))
}

#[derive(Clone)]
pub(crate) struct StrongTableCatalogStore<B> {
    object_backend: B,
    // Single mutex protecting all catalog state (table_buckets, namespaces, tables, views, commits, idempotency).
    // This is intentional: many operations require atomic read-modify-write across multiple fields.
    // Splitting into per-field locks would introduce deadlock risk and complexity.
    // If lock contention becomes a bottleneck (acquisition time > 10ms), consider:
    // 1. Using RwLock for read-heavy paths (but most paths need write access)
    // 2. Splitting into logical groups (e.g., metadata vs commits)
    // 3. Using optimistic concurrency with version checks
    pub(in crate::table_catalog) state: Arc<tokio::sync::Mutex<StrongTableCatalogState>>,
    // Serializes local snapshot mutations; object ETags fence independent store instances.
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl<B> StrongTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub fn new(object_backend: B) -> Self {
        Self {
            object_backend,
            state: Arc::new(tokio::sync::Mutex::new(StrongTableCatalogState::default())),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(in crate::table_catalog) fn namespace_key(table_bucket: &str, namespace: &Namespace) -> StrongNamespaceKey {
        (table_bucket.to_string(), namespace.public_name())
    }

    fn has_active_namespace_descendant_locked(state: &StrongTableCatalogState, table_bucket: &str, parent: &str) -> bool {
        let range_start = (table_bucket.to_string(), parent.to_string(), String::new());
        state
            .namespace_children
            .range(range_start..)
            .next()
            .is_some_and(|((bucket, candidate_parent, _), _)| bucket == table_bucket && candidate_parent == parent)
    }

    fn namespace_exists_locked(state: &StrongTableCatalogState, table_bucket: &str, namespace: &Namespace) -> bool {
        let key = Self::namespace_key(table_bucket, namespace);
        state
            .namespaces
            .get(&key)
            .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
            || state.namespace_objects.contains(&key)
            || Self::has_active_namespace_descendant_locked(state, table_bucket, &namespace.public_name())
    }

    fn require_active_namespace_locked(
        state: &StrongTableCatalogState,
        table_bucket: &str,
        namespace: &Namespace,
    ) -> TableCatalogStoreResult<()> {
        if Self::namespace_exists_locked(state, table_bucket, namespace) {
            return Ok(());
        }
        Err(TableCatalogStoreError::NotFound(format!(
            "namespace {table_bucket}/{}",
            namespace.public_name()
        )))
    }

    fn list_namespace_children_page_locked(
        state: &StrongTableCatalogState,
        table_bucket: &str,
        parent: Option<&str>,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        let parent = parent.map(parse_namespace_for_store).transpose()?;
        let parent_name = parent.as_ref().map_or_else(String::new, Namespace::public_name);
        if let Some(parent) = parent.as_ref()
            && !Self::namespace_exists_locked(state, table_bucket, parent)
        {
            return Err(TableCatalogStoreError::NotFound(format!("namespace {table_bucket}/{parent_name}")));
        }

        let cursor = catalog_list_cursor(cursor, STRONG_CATALOG_LIST_CURSOR_PREFIX)?;
        let start = match cursor {
            Some(cursor) => {
                let child = parse_namespace_for_store(cursor)?;
                let parent_depth = parent.as_ref().map_or(0, |parent| parent.segments().len());
                if child.segments().len() != parent_depth.saturating_add(1)
                    || parent
                        .as_ref()
                        .is_some_and(|parent| !child.segments().starts_with(parent.segments()))
                {
                    return Err(TableCatalogStoreError::Invalid(
                        "page cursor does not match this namespace child list operation".to_string(),
                    ));
                }
                let sort_key = format!("{}/", child.segments()[parent_depth].as_str());
                Bound::Excluded((table_bucket.to_string(), parent_name.clone(), sort_key))
            }
            None => Bound::Included((table_bucket.to_string(), parent_name.clone(), String::new())),
        };
        let entries = state
            .namespace_children
            .range((start, Bound::Unbounded))
            .take_while(|((bucket, candidate_parent, _), _)| bucket == table_bucket && candidate_parent == &parent_name)
            .take(limit.get().saturating_add(1))
            .map(|(_, child_name)| {
                let child = parse_namespace_for_store(child_name)?;
                Ok(state
                    .namespaces
                    .get(&Self::namespace_key(table_bucket, &child))
                    .filter(|entry| entry.state == TableCatalogEntryState::Active)
                    .cloned()
                    .unwrap_or_else(|| synthetic_namespace_entry(table_bucket, &child)))
            })
            .collect::<TableCatalogStoreResult<Vec<_>>>()?;
        Ok(finish_catalog_list_page(entries, limit, STRONG_CATALOG_LIST_CURSOR_PREFIX, |entry| {
            &entry.namespace
        }))
    }

    fn table_key(table_bucket: &str, namespace: &Namespace, table: &IdentifierSegment) -> StrongResourceKey {
        (table_bucket.to_string(), namespace.public_name(), table.as_str().to_string())
    }

    fn commit_key(table_bucket: &str, table_id: &str, commit_id: &str) -> StrongCommitKey {
        (table_bucket.to_string(), table_id.to_string(), commit_id.to_string())
    }

    fn idempotency_key(table_bucket: &str, table_id: &str, idempotency_key: &str) -> StrongCommitKey {
        (table_bucket.to_string(), table_id.to_string(), idempotency_key.to_string())
    }

    pub(in crate::table_catalog) fn snapshot_object_path() -> String {
        format!("{INTERNAL_CATALOG_ROOT}/{STRONG_TABLE_CATALOG_BACKING_ROOT}/{STRONG_TABLE_CATALOG_SNAPSHOT_FILE}")
    }

    fn snapshot_from_state_locked(state: &StrongTableCatalogState) -> StrongTableCatalogSnapshot {
        StrongTableCatalogSnapshot {
            version: STRONG_TABLE_CATALOG_SNAPSHOT_VERSION,
            table_buckets: state.table_buckets.values().cloned().collect(),
            namespaces: state.namespaces.values().cloned().collect(),
            tables: state.tables.values().cloned().collect(),
            views: state.views.values().cloned().collect(),
            commits: state
                .commits
                .iter()
                .map(|((table_bucket, table_id, lookup_key), commit)| StrongCommitSnapshotRecord {
                    table_bucket: table_bucket.clone(),
                    table_id: table_id.clone(),
                    lookup_key: lookup_key.clone(),
                    commit: commit.clone(),
                })
                .collect(),
            idempotency: state
                .idempotency
                .iter()
                .map(|((table_bucket, table_id, lookup_key), commit)| StrongCommitSnapshotRecord {
                    table_bucket: table_bucket.clone(),
                    table_id: table_id.clone(),
                    lookup_key: lookup_key.clone(),
                    commit: commit.clone(),
                })
                .collect(),
        }
    }

    fn bucket_snapshot_from_state_locked(
        state: &StrongTableCatalogState,
        table_bucket: &str,
    ) -> Option<StrongTableCatalogBucketSnapshot> {
        let table_bucket_entry = state.table_buckets.get(table_bucket)?.clone();
        Some(StrongTableCatalogBucketSnapshot {
            table_bucket: table_bucket_entry,
            namespaces: state
                .namespaces
                .iter()
                .filter(|((entry_bucket, _), _)| entry_bucket == table_bucket)
                .map(|(_, entry)| entry.clone())
                .collect(),
            tables: state
                .tables
                .iter()
                .filter(|((entry_bucket, _, _), _)| entry_bucket == table_bucket)
                .map(|(_, entry)| entry.clone())
                .collect(),
            views: state
                .views
                .iter()
                .filter(|((entry_bucket, _, _), _)| entry_bucket == table_bucket)
                .map(|(_, entry)| entry.clone())
                .collect(),
            commits: state
                .commits
                .iter()
                .filter(|((entry_bucket, _, _), _)| entry_bucket == table_bucket)
                .map(|((entry_bucket, table_id, lookup_key), commit)| StrongCommitSnapshotRecord {
                    table_bucket: entry_bucket.clone(),
                    table_id: table_id.clone(),
                    lookup_key: lookup_key.clone(),
                    commit: commit.clone(),
                })
                .collect(),
            idempotency: state
                .idempotency
                .iter()
                .filter(|((entry_bucket, _, _), _)| entry_bucket == table_bucket)
                .map(|((entry_bucket, table_id, lookup_key), commit)| StrongCommitSnapshotRecord {
                    table_bucket: entry_bucket.clone(),
                    table_id: table_id.clone(),
                    lookup_key: lookup_key.clone(),
                    commit: commit.clone(),
                })
                .collect(),
        })
    }

    fn remove_bucket_from_state_locked(state: &mut StrongTableCatalogState, table_bucket: &str) {
        state.table_buckets.remove(table_bucket);
        state.namespaces.retain(|(entry_bucket, _), _| entry_bucket != table_bucket);
        state
            .namespace_children
            .retain(|(entry_bucket, _, _), _| entry_bucket != table_bucket);
        state
            .namespace_objects
            .retain(|(entry_bucket, _)| entry_bucket != table_bucket);
        state.tables.retain(|(entry_bucket, _, _), _| entry_bucket != table_bucket);
        state.views.retain(|(entry_bucket, _, _), _| entry_bucket != table_bucket);
        state.commits.retain(|(entry_bucket, _, _), _| entry_bucket != table_bucket);
        state
            .idempotency
            .retain(|(entry_bucket, _, _), _| entry_bucket != table_bucket);
        state.warehouse_index.remove(table_bucket);
    }

    pub(super) fn insert_bucket_snapshot_locked(
        state: &mut StrongTableCatalogState,
        snapshot: StrongTableCatalogBucketSnapshot,
    ) -> TableCatalogStoreResult<()> {
        let table_bucket = snapshot.table_bucket.table_bucket.clone();
        Self::remove_bucket_from_state_locked(state, &table_bucket);
        state.table_buckets.insert(table_bucket.clone(), snapshot.table_bucket);
        for entry in snapshot.namespaces {
            let namespace = validate_namespace_entry_identity(&entry)?;
            state.namespaces.insert(Self::namespace_key(&table_bucket, &namespace), entry);
        }
        for entry in snapshot.tables {
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let table = parse_table_for_store(&entry.table)?;
            state.tables.insert(Self::table_key(&table_bucket, &namespace, &table), entry);
        }
        for entry in snapshot.views {
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let view = parse_table_for_store(&entry.view)?;
            state.views.insert(Self::table_key(&table_bucket, &namespace, &view), entry);
        }
        for record in snapshot.commits {
            state.commits.insert(
                Self::commit_key(&record.table_bucket, &record.table_id, &record.lookup_key),
                record.commit,
            );
        }
        for record in snapshot.idempotency {
            state.idempotency.insert(
                Self::idempotency_key(&record.table_bucket, &record.table_id, &record.lookup_key),
                record.commit,
            );
        }
        Self::rebuild_namespace_indexes_locked(state)?;
        Self::rebuild_warehouse_index_locked(state)
    }

    fn index_namespace_children(
        children: &mut BTreeMap<StrongNamespaceChildKey, String>,
        table_bucket: &str,
        namespace: &Namespace,
    ) {
        for depth in 0..namespace.segments().len() {
            let parent = namespace.segments()[..depth]
                .iter()
                .map(IdentifierSegment::as_str)
                .collect::<Vec<_>>()
                .join(".");
            let child = namespace.segments()[..=depth]
                .iter()
                .map(IdentifierSegment::as_str)
                .collect::<Vec<_>>()
                .join(".");
            let sort_key = format!("{}/", namespace.segments()[depth].as_str());
            children.insert((table_bucket.to_string(), parent, sort_key), child);
        }
    }

    fn rebuild_namespace_indexes_locked(state: &mut StrongTableCatalogState) -> TableCatalogStoreResult<()> {
        let mut children = BTreeMap::new();
        for entry in state
            .namespaces
            .values()
            .filter(|entry| entry.state == TableCatalogEntryState::Active)
        {
            let namespace = validate_namespace_entry_identity(entry)?;
            Self::index_namespace_children(&mut children, &entry.table_bucket, &namespace);
        }

        let mut objects = BTreeSet::new();
        for (table_bucket, namespace_name) in state
            .tables
            .values()
            .filter(|entry| entry.state == TableCatalogEntryState::Active)
            .map(|entry| (&entry.table_bucket, &entry.namespace))
            .chain(
                state
                    .views
                    .values()
                    .filter(|entry| entry.state == TableCatalogEntryState::Active)
                    .map(|entry| (&entry.table_bucket, &entry.namespace)),
            )
        {
            let namespace = parse_namespace_for_store(namespace_name)?;
            objects.insert(Self::namespace_key(table_bucket, &namespace));
            Self::index_namespace_children(&mut children, table_bucket, &namespace);
        }
        state.namespace_children = children;
        state.namespace_objects = objects;
        Ok(())
    }

    fn rebuild_warehouse_index_locked(state: &mut StrongTableCatalogState) -> TableCatalogStoreResult<()> {
        let mut warehouse_index: StrongWarehouseIndex = BTreeMap::new();
        for ((table_bucket, namespace, table), entry) in &state.tables {
            if entry.state != TableCatalogEntryState::Active {
                continue;
            }
            if !state
                .table_buckets
                .get(table_bucket)
                .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
            {
                continue;
            }
            let namespace_identity = parse_namespace_for_store(namespace)?;
            if !Self::namespace_exists_locked(state, table_bucket, &namespace_identity) {
                continue;
            }
            let Ok(warehouse_object_prefix) = table_warehouse_object_prefix(entry) else {
                continue;
            };
            let table_key = (table_bucket.clone(), namespace.clone(), table.clone());
            if let Some(existing_key) = warehouse_index
                .entry(table_bucket.clone())
                .or_default()
                .insert(warehouse_object_prefix.clone(), table_key.clone())
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "duplicate active table warehouse location in strong catalog snapshot: {warehouse_object_prefix} is owned by {}/{}/{} and {}/{}/{}",
                    existing_key.0, existing_key.1, existing_key.2, table_key.0, table_key.1, table_key.2
                )));
            }
        }
        state.warehouse_index = warehouse_index;
        Ok(())
    }

    fn snapshot_from_mutated_state_locked(
        state: &mut StrongTableCatalogState,
    ) -> TableCatalogStoreResult<StrongTableCatalogSnapshot> {
        Self::rebuild_namespace_indexes_locked(state)?;
        Self::rebuild_warehouse_index_locked(state)?;
        Ok(Self::snapshot_from_state_locked(state))
    }

    fn state_from_snapshot(
        snapshot: StrongTableCatalogSnapshot,
        snapshot_etag: Option<String>,
    ) -> TableCatalogStoreResult<StrongTableCatalogState> {
        if snapshot.version != STRONG_TABLE_CATALOG_SNAPSHOT_VERSION {
            return Err(TableCatalogStoreError::Invalid(format!(
                "unsupported strong catalog snapshot version: {}",
                snapshot.version
            )));
        }

        let mut state = StrongTableCatalogState {
            hydrated: true,
            snapshot_etag,
            ..StrongTableCatalogState::default()
        };
        for entry in snapshot.table_buckets {
            state.table_buckets.insert(entry.table_bucket.clone(), entry);
        }
        for entry in snapshot.namespaces {
            let namespace = validate_namespace_entry_identity(&entry)?;
            state
                .namespaces
                .insert(Self::namespace_key(&entry.table_bucket, &namespace), entry);
        }
        for entry in snapshot.tables {
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let table = parse_table_for_store(&entry.table)?;
            state
                .tables
                .insert(Self::table_key(&entry.table_bucket, &namespace, &table), entry);
        }
        for entry in snapshot.views {
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let view = parse_table_for_store(&entry.view)?;
            state
                .views
                .insert(Self::table_key(&entry.table_bucket, &namespace, &view), entry);
        }
        for record in snapshot.commits {
            state.commits.insert(
                Self::commit_key(&record.table_bucket, &record.table_id, &record.lookup_key),
                record.commit,
            );
        }
        for record in snapshot.idempotency {
            state.idempotency.insert(
                Self::idempotency_key(&record.table_bucket, &record.table_id, &record.lookup_key),
                record.commit,
            );
        }
        Self::rebuild_namespace_indexes_locked(&mut state)?;
        Self::rebuild_warehouse_index_locked(&mut state)?;
        Ok(state)
    }

    fn snapshot_write_precondition_locked(state: &StrongTableCatalogState) -> TableCatalogPutPrecondition {
        state
            .snapshot_etag
            .as_ref()
            .map_or(TableCatalogPutPrecondition::IfAbsent, |etag| {
                TableCatalogPutPrecondition::IfMatch(etag.clone())
            })
    }

    fn snapshot_draft_context_locked(state: &StrongTableCatalogState) -> (TableCatalogPutPrecondition, StrongTableCatalogState) {
        (Self::snapshot_write_precondition_locked(state), state.clone())
    }

    async fn hydrate_state(&self) -> TableCatalogStoreResult<()> {
        let Some(current_snapshot_etag) = ({
            let state = self.state.lock().await;
            if state.hydrated {
                Some(state.snapshot_etag.clone())
            } else {
                None
            }
        }) else {
            return self.reload_state_from_durable().await;
        };

        let snapshot_metadata = self
            .object_backend
            .object_metadata(RUSTFS_META_BUCKET, &Self::snapshot_object_path())
            .await?;
        match (snapshot_metadata, current_snapshot_etag.as_deref()) {
            (None, None) => Ok(()),
            (Some(metadata), Some(current_etag)) if metadata.etag.as_deref() == Some(current_etag) => Ok(()),
            _ => self.reload_state_from_durable().await,
        }
    }

    async fn reload_state_from_durable(&self) -> TableCatalogStoreResult<()> {
        let snapshot_object = self
            .object_backend
            .read_object(RUSTFS_META_BUCKET, &Self::snapshot_object_path())
            .await?;
        let mut state = self.state.lock().await;
        if let Some(snapshot_object) = snapshot_object {
            let snapshot = serde_json::from_slice::<StrongTableCatalogSnapshot>(&snapshot_object.data)
                .map_err(|err| TableCatalogStoreError::Internal(format!("failed to decode strong catalog snapshot: {err}")))?;
            *state = Self::state_from_snapshot(snapshot, snapshot_object.etag)?;
        } else {
            *state = StrongTableCatalogState {
                hydrated: true,
                ..StrongTableCatalogState::default()
            };
        }
        Ok(())
    }

    async fn persist_snapshot(
        &self,
        snapshot: StrongTableCatalogSnapshot,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        let data = serde_json::to_vec(&snapshot)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode strong catalog snapshot: {err}")))?;
        self.object_backend
            .put_object(RUSTFS_META_BUCKET, &Self::snapshot_object_path(), data, precondition)
            .await
    }

    async fn finalize_snapshot_write(
        &self,
        snapshot: StrongTableCatalogSnapshot,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        match self.persist_snapshot(snapshot, precondition).await {
            Ok(()) => self.reload_state_from_durable().await,
            Err(err) => {
                let _ = self.reload_state_from_durable().await;
                Err(err)
            }
        }
    }

    pub(super) async fn materialize_bucket_snapshot(
        &self,
        source: StrongTableCatalogBucketSnapshot,
    ) -> TableCatalogStoreResult<(String, bool)> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let table_bucket = source.table_bucket.table_bucket.clone();
        let (snapshot, precondition) = {
            let state = self.state.lock().await;
            if let Some(current) = Self::bucket_snapshot_from_state_locked(&state, &table_bucket) {
                if current != source {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "durable strong catalog already contains different state for table bucket {table_bucket}"
                    )));
                }
                let snapshot_etag = state
                    .snapshot_etag
                    .clone()
                    .ok_or_else(|| TableCatalogStoreError::Internal("durable strong catalog snapshot has no etag".to_string()))?;
                return Ok((snapshot_etag, false));
            }
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            Self::insert_bucket_snapshot_locked(&mut draft_state, source)?;
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition)
        };
        self.finalize_snapshot_write(snapshot, precondition).await?;
        let state = self.state.lock().await;
        let snapshot_etag = state
            .snapshot_etag
            .clone()
            .ok_or_else(|| TableCatalogStoreError::Internal("durable strong catalog snapshot has no etag".to_string()))?;
        Ok((snapshot_etag, true))
    }

    pub(super) async fn remove_bucket_snapshot_if_unchanged(
        &self,
        table_bucket: &str,
        expected_fingerprint: &str,
    ) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let (snapshot, precondition) = {
            let state = self.state.lock().await;
            let Some(current) = Self::bucket_snapshot_from_state_locked(&state, table_bucket) else {
                return Ok(());
            };
            if table_catalog_bucket_snapshot_fingerprint(&current)? != expected_fingerprint {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "durable strong catalog state changed after materializing table bucket {table_bucket}"
                )));
            }
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            Self::remove_bucket_from_state_locked(&mut draft_state, table_bucket);
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition)
        };
        self.finalize_snapshot_write(snapshot, precondition).await
    }

    pub(super) async fn bucket_snapshot_fingerprint(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<String>> {
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        Self::bucket_snapshot_from_state_locked(&state, table_bucket)
            .as_ref()
            .map(table_catalog_bucket_snapshot_fingerprint)
            .transpose()
    }

    fn require_table_bucket_in_state(state: &StrongTableCatalogState, table_bucket: &str) -> TableCatalogStoreResult<()> {
        if !state.table_buckets.contains_key(table_bucket) {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
        }
        Ok(())
    }

    fn ensure_table_warehouse_prefix_available_locked(
        state: &StrongTableCatalogState,
        candidate: &TableEntry,
        candidate_key: &StrongResourceKey,
    ) -> TableCatalogStoreResult<()> {
        if candidate.state != TableCatalogEntryState::Active {
            return Ok(());
        }
        let candidate_prefix = table_warehouse_object_prefix(candidate)?;
        for (existing_key, existing) in &state.tables {
            if existing_key == candidate_key
                || existing.table_bucket != candidate.table_bucket
                || existing.state != TableCatalogEntryState::Active
            {
                continue;
            }
            let Ok(existing_prefix) = table_warehouse_object_prefix(existing) else {
                continue;
            };
            if existing_prefix == candidate_prefix {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table warehouse location is already registered: {candidate_prefix}"
                )));
            }
        }
        Ok(())
    }

    fn table_commit_recovery_report_for_entry_locked(
        state: &StrongTableCatalogState,
        entry: &TableEntry,
    ) -> TableCommitRecoveryReport {
        let mut commits = state
            .commits
            .iter()
            .filter(|((table_bucket, table_id, _), _)| table_bucket == &entry.table_bucket && table_id == &entry.table_id)
            .map(|((_, _, _), commit_log)| {
                let idempotency_commit = commit_log.idempotency_key.as_deref().and_then(|idempotency_key| {
                    state
                        .idempotency
                        .get(&Self::idempotency_key(&entry.table_bucket, &entry.table_id, idempotency_key))
                });
                table_commit_recovery_entry(entry, commit_log, idempotency_commit)
            })
            .collect::<Vec<_>>();
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
        let finalized_count = commits
            .iter()
            .filter(|commit| matches!(commit.recovery_state, TableCommitRecoveryState::Committed))
            .count();

        TableCommitRecoveryReport {
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
        }
    }

    fn validate_new_table_commit_locked(
        state: &StrongTableCatalogState,
        key: &StrongResourceKey,
        request: &TableCommitRequest,
        namespace: &Namespace,
        table: &IdentifierSegment,
    ) -> TableCatalogStoreResult<TableEntry> {
        let Some(current) = state.tables.get(key).cloned() else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                request.table_bucket, request.namespace, request.table
            )));
        };
        let commit_key = Self::commit_key(&request.table_bucket, &current.table_id, &request.commit_id);
        let existing_commit = state.commits.get(&commit_key);
        let idempotency_key = request
            .idempotency_key
            .as_deref()
            .map(|idempotency_key| Self::idempotency_key(&request.table_bucket, &current.table_id, idempotency_key));
        let existing_idempotency_commit = idempotency_key.as_ref().and_then(|key| state.idempotency.get(key));

        if let Some(existing) = existing_commit {
            if !commit_log_matches_request(existing, request, &current.table_id) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "commit id already exists: {}",
                    request.commit_id
                )));
            }
            if matches!(existing.status, CommitLogStatus::Committed) || table_matches_committed_log(&current, existing) {
                return Ok(current);
            }
            return Err(TableCatalogStoreError::Conflict(
                "existing commit record does not match current table state".to_string(),
            ));
        }
        if let Some(existing) = existing_idempotency_commit
            && !commit_log_matches_request(existing, request, &current.table_id)
        {
            return Err(TableCatalogStoreError::Conflict("idempotency key already exists".to_string()));
        }
        if existing_idempotency_commit.is_some() {
            return Err(TableCatalogStoreError::Conflict(
                "idempotency key exists without a recoverable commit record".to_string(),
            ));
        }
        if current.version_token != request.expected_version_token {
            return Err(TableCatalogStoreError::Conflict(
                "current table version token does not match expected token".to_string(),
            ));
        }
        if current.metadata_location != request.expected_metadata_location {
            return Err(TableCatalogStoreError::Conflict(
                "current table metadata location does not match expected location".to_string(),
            ));
        }
        if !is_valid_table_metadata_location(namespace, table, &request.new_metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "new metadata location must be inside the table metadata directory".to_string(),
            ));
        }
        Ok(current)
    }

    fn committed_existing_result_locked(
        state: &mut StrongTableCatalogState,
        request: &TableCommitRequest,
        current: TableEntry,
    ) -> Option<TableCommitResult> {
        let commit_key = Self::commit_key(&request.table_bucket, &current.table_id, &request.commit_id);
        let existing = state.commits.get(&commit_key)?;
        if !commit_log_matches_request(existing, request, &current.table_id) {
            return None;
        }
        if !matches!(existing.status, CommitLogStatus::Committed) && !table_matches_committed_log(&current, existing) {
            return None;
        }

        let mut committed = existing.clone();
        committed.status = CommitLogStatus::Committed;
        state.commits.insert(commit_key, committed.clone());
        if let Some(idempotency_key) = committed.idempotency_key.as_deref() {
            state.idempotency.insert(
                Self::idempotency_key(&request.table_bucket, &current.table_id, idempotency_key),
                committed.clone(),
            );
        }
        Some(TableCommitResult {
            table: current,
            commit_log: committed,
        })
    }

    fn apply_commit_locked(
        state: &mut StrongTableCatalogState,
        request: &TableCommitRequest,
        namespace: &Namespace,
        table: &IdentifierSegment,
        next_warehouse_location: Option<String>,
    ) -> TableCatalogStoreResult<TableCommitResult> {
        let key = Self::table_key(&request.table_bucket, namespace, table);
        let current = Self::validate_new_table_commit_locked(state, &key, request, namespace, table)?;
        if let Some(result) = Self::committed_existing_result_locked(state, request, current.clone()) {
            return Ok(result);
        }

        let commit_log = CommitLogEntry {
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
            status: CommitLogStatus::Committed,
            writer: request.writer.clone(),
            created_at: None,
            updated_at: None,
        };

        let mut next = current;
        next.metadata_location = commit_log.new_metadata_location.clone();
        if let Some(warehouse_location) = next_warehouse_location {
            next.warehouse_location = warehouse_location;
        }
        Self::ensure_table_warehouse_prefix_available_locked(state, &next, &key)?;
        next.version_token = commit_log.new_version_token.clone();
        next.generation = next.generation.saturating_add(1);

        let commit_key = Self::commit_key(&request.table_bucket, &next.table_id, &request.commit_id);
        state.commits.insert(commit_key, commit_log.clone());
        if let Some(idempotency_key) = request.idempotency_key.as_deref() {
            state.idempotency.insert(
                Self::idempotency_key(&request.table_bucket, &next.table_id, idempotency_key),
                commit_log.clone(),
            );
        }
        state.tables.insert(key, next.clone());

        Ok(TableCommitResult { table: next, commit_log })
    }

    pub(crate) async fn plan_table_commit_recovery(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCommitRecoveryReport> {
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let key = Self::table_key(table_bucket, &namespace, &table);
        let state = self.state.lock().await;
        let Some(entry) = state.tables.get(&key) else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        Ok(Self::table_commit_recovery_report_for_entry_locked(&state, entry))
    }
}

#[async_trait::async_trait]
impl<B> TableCatalogStore for StrongTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    async fn get_table_bucket(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<TableBucketEntry>> {
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        Ok(state.table_buckets.get(table_bucket).cloned())
    }

    async fn put_table_bucket(&self, entry: TableBucketEntry) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        validate_catalog_entry_version("table bucket", entry.version)?;
        if entry.table_bucket.is_empty() {
            return Err(TableCatalogStoreError::Invalid("table bucket name cannot be empty".to_string()));
        }
        if entry.catalog_type != TABLE_BUCKET_CATALOG_TYPE {
            return Err(TableCatalogStoreError::Invalid("unsupported table bucket catalog type".to_string()));
        }

        let (snapshot, precondition) = {
            let state = self.state.lock().await;
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.table_buckets.insert(entry.table_bucket.clone(), entry);
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition)
        };
        self.finalize_snapshot_write(snapshot, precondition).await
    }

    async fn create_namespace(&self, entry: NamespaceEntry) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = validate_namespace_entry_identity(&entry)?;
        validate_namespace_properties(&entry.properties)?;
        let key = Self::namespace_key(&entry.table_bucket, &namespace);
        let (snapshot, precondition) = {
            let state = self.state.lock().await;
            Self::require_table_bucket_in_state(&state, &entry.table_bucket)?;
            let existing = state.namespaces.get(&key);
            if existing.is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
                || (existing.is_none() && Self::namespace_exists_locked(&state, &entry.table_bucket, &namespace))
            {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog object already exists: namespace {}/{}",
                    entry.table_bucket, entry.namespace
                )));
            }
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.namespaces.insert(key, entry);
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition)
        };
        self.finalize_snapshot_write(snapshot, precondition).await
    }

    async fn list_namespaces(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        let mut entries = state
            .namespaces
            .iter()
            .filter(|((bucket, _), entry)| bucket == table_bucket && entry.state == TableCatalogEntryState::Active)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.namespace.cmp(&right.namespace));
        Ok(entries)
    }

    async fn list_namespaces_under(&self, table_bucket: &str, parent: &str) -> TableCatalogStoreResult<Vec<NamespaceEntry>> {
        self.hydrate_state().await?;
        let parent = parse_namespace_for_store(parent)?.public_name();
        let state = self.state.lock().await;
        let exact = state
            .namespaces
            .get(&(table_bucket.to_string(), parent.clone()))
            .filter(|entry| entry.state == TableCatalogEntryState::Active)
            .cloned();
        let descendant_start = (table_bucket.to_string(), format!("{parent}."));
        let descendants = state
            .namespaces
            .range(descendant_start..)
            .take_while(|((bucket, namespace), _)| bucket == table_bucket && namespace_is_descendant(namespace, &parent))
            .filter(|(_, entry)| entry.state == TableCatalogEntryState::Active)
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        Ok(exact.into_iter().chain(descendants).collect())
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
                .list_namespace_children_page(table_bucket, parent, cursor.as_deref(), limit)
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
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        Self::list_namespace_children_page_locked(&state, table_bucket, parent, cursor, limit)
    }

    async fn list_namespaces_page(
        &self,
        table_bucket: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<NamespaceEntry>> {
        self.hydrate_state().await?;
        let cursor = catalog_list_cursor(cursor, STRONG_CATALOG_LIST_CURSOR_PREFIX)?;
        let cursor = cursor
            .map(parse_namespace_for_store)
            .transpose()?
            .map(|namespace| namespace.public_name());
        let start = match cursor {
            Some(cursor) => Bound::Excluded((table_bucket.to_string(), cursor)),
            None => Bound::Included((table_bucket.to_string(), String::new())),
        };
        let state = self.state.lock().await;
        let entries = state
            .namespaces
            .range((start, Bound::Unbounded))
            .take_while(|((bucket, _), _)| bucket == table_bucket)
            .filter(|(_, entry)| entry.state == TableCatalogEntryState::Active)
            .take(limit.get().saturating_add(1))
            .map(|(_, entry)| entry.clone())
            .collect();
        Ok(finish_catalog_list_page(entries, limit, STRONG_CATALOG_LIST_CURSOR_PREFIX, |entry| {
            &entry.namespace
        }))
    }

    async fn get_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Option<NamespaceEntry>> {
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let state = self.state.lock().await;
        let exact = state.namespaces.get(&Self::namespace_key(table_bucket, &namespace)).cloned();
        if exact
            .as_ref()
            .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
        {
            return Ok(exact);
        }
        if Self::namespace_exists_locked(&state, table_bucket, &namespace) {
            return Ok(Some(synthetic_namespace_entry(table_bucket, &namespace)));
        }
        Ok(None)
    }

    async fn update_namespace_properties(
        &self,
        table_bucket: &str,
        namespace: &str,
        update: NamespacePropertiesUpdate,
    ) -> TableCatalogStoreResult<NamespacePropertiesUpdateResult> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let key = Self::namespace_key(table_bucket, &namespace);
        let (snapshot, precondition, result) = {
            let state = self.state.lock().await;
            let current = state
                .namespaces
                .get(&key)
                .filter(|entry| entry.state == TableCatalogEntryState::Active);
            let mut next = match current {
                Some(current) => current.clone(),
                None if Self::namespace_exists_locked(&state, table_bucket, &namespace) => {
                    synthetic_namespace_entry(table_bucket, &namespace)
                }
                None => {
                    return Err(TableCatalogStoreError::NotFound(format!(
                        "namespace {table_bucket}/{}",
                        namespace.public_name()
                    )));
                }
            };
            let result = update.apply_to(&mut next);
            validate_namespace_properties(&next.properties)?;
            let unchanged =
                current.map_or_else(|| next == synthetic_namespace_entry(table_bucket, &namespace), |current| &next == current);
            if unchanged {
                return Ok(result);
            }
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.namespaces.insert(key, next);
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition, result)
        };
        self.finalize_snapshot_write(snapshot, precondition).await?;
        Ok(result)
    }

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let key = Self::namespace_key(table_bucket, &namespace);
        let (snapshot, precondition) = {
            let state = self.state.lock().await;
            let parent = namespace.public_name();
            if Self::has_active_namespace_descendant_locked(&state, table_bucket, &parent) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "namespace {table_bucket}/{parent} has child namespaces"
                )));
            }
            if state.namespace_objects.contains(&key) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "namespace {table_bucket}/{parent} is not empty"
                )));
            }
            if !state
                .namespaces
                .get(&key)
                .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
            {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "namespace {}/{}",
                    table_bucket,
                    namespace.public_name()
                )));
            }
            if state
                .tables
                .keys()
                .any(|(bucket, namespace_name, _)| bucket == table_bucket && namespace_name == &namespace.public_name())
                || state
                    .views
                    .keys()
                    .any(|(bucket, namespace_name, _)| bucket == table_bucket && namespace_name == &namespace.public_name())
            {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "namespace {}/{} is not empty",
                    table_bucket,
                    namespace.public_name()
                )));
            }
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.namespaces.remove(&key);
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition)
        };
        self.finalize_snapshot_write(snapshot, precondition).await
    }

    async fn create_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        self.register_table(entry).await
    }

    async fn register_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        validate_catalog_entry_version("table", entry.version)?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        table_warehouse_object_prefix(&entry)?;
        let key = Self::table_key(&entry.table_bucket, &namespace, &table);
        let (snapshot, precondition) = {
            let state = self.state.lock().await;
            Self::require_table_bucket_in_state(&state, &entry.table_bucket)?;
            Self::require_active_namespace_locked(&state, &entry.table_bucket, &namespace)?;
            if state.tables.contains_key(&key) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog object already exists: table {}/{}/{}",
                    entry.table_bucket, entry.namespace, entry.table
                )));
            }
            Self::ensure_table_warehouse_prefix_available_locked(&state, &entry, &key)?;
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.tables.insert(key, entry);
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition)
        };
        self.finalize_snapshot_write(snapshot, precondition).await
    }

    async fn list_tables(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let state = self.state.lock().await;
        let mut entries = state
            .tables
            .iter()
            .filter(|((bucket, namespace_name, _), _)| bucket == table_bucket && namespace_name == &namespace.public_name())
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.table.cmp(&right.table));
        Ok(entries)
    }

    async fn list_all_tables(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        Ok(state
            .tables
            .range((table_bucket.to_string(), String::new(), String::new())..)
            .take_while(|((bucket, _, _), _)| bucket == table_bucket)
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    async fn list_tables_page(
        &self,
        table_bucket: &str,
        namespace: &str,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> TableCatalogStoreResult<TableCatalogListPage<TableEntry>> {
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?.public_name();
        let cursor = catalog_list_cursor(cursor, STRONG_CATALOG_LIST_CURSOR_PREFIX)?;
        let cursor = cursor
            .map(parse_table_for_store)
            .transpose()?
            .map(|table| table.as_str().to_string());
        let start = match cursor {
            Some(cursor) => Bound::Excluded((table_bucket.to_string(), namespace.clone(), cursor)),
            None => Bound::Included((table_bucket.to_string(), namespace.clone(), String::new())),
        };
        let state = self.state.lock().await;
        let entries = state
            .tables
            .range((start, Bound::Unbounded))
            .take_while(|((bucket, entry_namespace, _), _)| bucket == table_bucket && entry_namespace == &namespace)
            .take(limit.get().saturating_add(1))
            .map(|(_, entry)| entry.clone())
            .collect();
        Ok(finish_catalog_list_page(entries, limit, STRONG_CATALOG_LIST_CURSOR_PREFIX, |entry| {
            &entry.table
        }))
    }

    async fn load_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<Option<TableEntry>> {
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let state = self.state.lock().await;
        Ok(state.tables.get(&Self::table_key(table_bucket, &namespace, &table)).cloned())
    }

    async fn resolve_table_data_plane_resource(
        &self,
        table_bucket: &str,
        object: &str,
    ) -> TableCatalogStoreResult<Option<TableDataPlaneResource>> {
        if table_bucket.is_empty() || object.is_empty() {
            return Ok(None);
        }

        self.hydrate_state().await?;
        let state = self.state.lock().await;
        let Some(bucket_entry) = state.table_buckets.get(table_bucket) else {
            return Ok(None);
        };
        if bucket_entry.state != TableCatalogEntryState::Active {
            return Ok(None);
        }

        let Some(bucket_index) = state.warehouse_index.get(table_bucket) else {
            return Ok(None);
        };

        for warehouse_object_prefix in warehouse_index_candidate_prefixes(object) {
            if let Some(table_key) = bucket_index.get(warehouse_object_prefix) {
                let Some(table) = state.tables.get(table_key) else {
                    continue;
                };
                return Ok(Some(table_data_plane_resource_from_entry(
                    table.clone(),
                    warehouse_object_prefix.to_string(),
                )));
            }
        }
        Ok(None)
    }

    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let commit_started = Instant::now();
        record_table_commit_attempt(&request.operation);
        let namespace = parse_namespace_for_store(&request.namespace)?;
        let table = parse_table_for_store(&request.table)?;
        let key = Self::table_key(&request.table_bucket, &namespace, &table);

        let committed_existing_result = {
            let state = self.state.lock().await;
            let current = Self::validate_new_table_commit_locked(&state, &key, &request, &namespace, &table);
            match current {
                Ok(current) => {
                    let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
                    Self::committed_existing_result_locked(&mut draft_state, &request, current).map(|result| {
                        Self::snapshot_from_mutated_state_locked(&mut draft_state)
                            .map(|snapshot| (result, snapshot, precondition))
                    })
                }
                Err(error) => {
                    return table_commit_result(
                        &request.table_bucket,
                        &request.namespace,
                        &request.table,
                        &request.commit_id,
                        &request.operation,
                        commit_started,
                        Err(error),
                    );
                }
            }
        };
        if let Some(prepared_result) = committed_existing_result {
            let result = match prepared_result {
                Ok((result, snapshot, precondition)) => {
                    self.finalize_snapshot_write(snapshot, precondition).await.map(|_| result)
                }
                Err(err) => Err(err),
            };
            return table_commit_result(
                &request.table_bucket,
                &request.namespace,
                &request.table,
                &request.commit_id,
                &request.operation,
                commit_started,
                result,
            );
        }

        let Some(new_metadata_object) = self
            .object_backend
            .read_object(&request.table_bucket, &request.new_metadata_location)
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
        let next_warehouse_location =
            table_metadata_warehouse_location(&request.table_bucket, &request.new_metadata_location, &new_metadata_object)?;

        let cas_started = Instant::now();
        let prepared_result = {
            let state = self.state.lock().await;
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            match Self::apply_commit_locked(&mut draft_state, &request, &namespace, &table, next_warehouse_location) {
                Ok(result) => {
                    Self::snapshot_from_mutated_state_locked(&mut draft_state).map(|snapshot| (result, snapshot, precondition))
                }
                Err(err) => Err(err),
            }
        };
        let result = match prepared_result {
            Ok((result, snapshot, precondition)) => self.finalize_snapshot_write(snapshot, precondition).await.map(|_| result),
            Err(err) => Err(err),
        };
        let cas_result = result.as_ref().map(|_| ()).map_err(Clone::clone);
        record_table_commit_cas_result(&request.operation, cas_started, &cas_result);
        table_commit_result(
            &request.table_bucket,
            &request.namespace,
            &request.table,
            &request.commit_id,
            &request.operation,
            commit_started,
            result,
        )
    }

    async fn drop_table(&self, table_bucket: &str, namespace: &str, table: &str) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let key = Self::table_key(table_bucket, &namespace, &table);
        let (snapshot, precondition) = {
            let state = self.state.lock().await;
            if !state.tables.contains_key(&key) {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    table.as_str()
                )));
            }
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.tables.remove(&key);
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition)
        };
        self.finalize_snapshot_write(snapshot, precondition).await
    }

    async fn create_view(&self, entry: ViewEntry) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        validate_catalog_entry_version("view", entry.version)?;
        validate_view_warehouse_location(&entry.table_bucket, &entry.warehouse_location)?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let view = parse_table_for_store(&entry.view)?;
        let key = Self::table_key(&entry.table_bucket, &namespace, &view);
        let (snapshot, precondition) = {
            let state = self.state.lock().await;
            Self::require_table_bucket_in_state(&state, &entry.table_bucket)?;
            Self::require_active_namespace_locked(&state, &entry.table_bucket, &namespace)?;
            if state.views.contains_key(&key) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog object already exists: view {}/{}/{}",
                    entry.table_bucket, entry.namespace, entry.view
                )));
            }
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.views.insert(key, entry);
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition)
        };
        self.finalize_snapshot_write(snapshot, precondition).await
    }

    async fn list_views(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<ViewEntry>> {
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let state = self.state.lock().await;
        let mut entries = state
            .views
            .iter()
            .filter(|((bucket, namespace_name, _), _)| bucket == table_bucket && namespace_name == &namespace.public_name())
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
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
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?.public_name();
        let cursor = catalog_list_cursor(cursor, STRONG_CATALOG_LIST_CURSOR_PREFIX)?;
        let cursor = cursor
            .map(parse_table_for_store)
            .transpose()?
            .map(|view| view.as_str().to_string());
        let start = match cursor {
            Some(cursor) => Bound::Excluded((table_bucket.to_string(), namespace.clone(), cursor)),
            None => Bound::Included((table_bucket.to_string(), namespace.clone(), String::new())),
        };
        let state = self.state.lock().await;
        let entries = state
            .views
            .range((start, Bound::Unbounded))
            .take_while(|((bucket, entry_namespace, _), _)| bucket == table_bucket && entry_namespace == &namespace)
            .take(limit.get().saturating_add(1))
            .map(|(_, entry)| entry.clone())
            .collect();
        Ok(finish_catalog_list_page(entries, limit, STRONG_CATALOG_LIST_CURSOR_PREFIX, |entry| {
            &entry.view
        }))
    }

    async fn load_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<Option<ViewEntry>> {
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let view = parse_table_for_store(view)?;
        let state = self.state.lock().await;
        Ok(state.views.get(&Self::table_key(table_bucket, &namespace, &view)).cloned())
    }

    async fn replace_view(&self, request: ViewCommitRequest) -> TableCatalogStoreResult<ViewCommitResult> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(&request.namespace)?;
        let view = parse_table_for_store(&request.view)?;
        if !is_valid_view_metadata_location(&namespace, &view, &request.new_metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "new metadata location must be inside the view metadata directory".to_string(),
            ));
        }
        let Some(new_metadata_object) = self
            .object_backend
            .read_object(&request.table_bucket, &request.new_metadata_location)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "new view metadata object {}",
                request.new_metadata_location
            )));
        };
        let next_warehouse_location =
            view_metadata_warehouse_location(&request.table_bucket, &request.new_metadata_location, &new_metadata_object)?;

        let key = Self::table_key(&request.table_bucket, &namespace, &view);
        let (snapshot, precondition, next) = {
            let state = self.state.lock().await;
            let Some(current) = state.views.get(&key).cloned() else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "view {}/{}/{}",
                    request.table_bucket, request.namespace, request.view
                )));
            };
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

            let mut next = current;
            next.metadata_location = request.new_metadata_location;
            if let Some(warehouse_location) = next_warehouse_location {
                next.warehouse_location = warehouse_location;
            }
            next.version_token = format!("token-{}", Uuid::new_v4());
            next.generation = next.generation.saturating_add(1);
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.views.insert(key, next.clone());
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition, next)
        };
        self.finalize_snapshot_write(snapshot, precondition).await?;
        Ok(ViewCommitResult { view: next })
    }

    async fn drop_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let view = parse_table_for_store(view)?;
        let key = Self::table_key(table_bucket, &namespace, &view);
        let (snapshot, precondition) = {
            let state = self.state.lock().await;
            if !state.views.contains_key(&key) {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "view {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    view.as_str()
                )));
            }
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.views.remove(&key);
            (Self::snapshot_from_mutated_state_locked(&mut draft_state)?, precondition)
        };
        self.finalize_snapshot_write(snapshot, precondition).await
    }

    async fn get_commit_by_id(
        &self,
        table_bucket: &str,
        table_id: &str,
        commit_id: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        Ok(state
            .commits
            .get(&Self::commit_key(table_bucket, table_id, commit_id))
            .cloned())
    }

    async fn get_commit_by_idempotency_key(
        &self,
        table_bucket: &str,
        table_id: &str,
        idempotency_key: &str,
    ) -> TableCatalogStoreResult<Option<CommitLogEntry>> {
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        Ok(state
            .idempotency
            .get(&Self::idempotency_key(table_bucket, table_id, idempotency_key))
            .cloned())
    }
}
