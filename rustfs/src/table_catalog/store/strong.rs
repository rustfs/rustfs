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
#[derive(Clone, PartialEq, Eq)]
struct StrongSnapshotObservation {
    hydrated: bool,
    snapshot_required: bool,
    etag: Option<String>,
    version: Option<u16>,
}
pub(in crate::table_catalog) const STRONG_TABLE_CATALOG_RELOAD_MAX_ATTEMPTS: usize = 3;
pub(in crate::table_catalog) const STRONG_TABLE_CATALOG_SNAPSHOT_MAX_SIZE: usize = 64 * 1024 * 1024;

pub(in crate::table_catalog) fn strong_snapshot_write_version(version_two_requested: bool, fleet_confirmed: bool) -> u16 {
    if version_two_requested && fleet_confirmed {
        STRONG_TABLE_CATALOG_SNAPSHOT_VERSION
    } else {
        STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION
    }
}

#[derive(Clone, Default)]
pub(crate) struct StrongTableCatalogRuntime {
    state: Arc<tokio::sync::Mutex<StrongTableCatalogState>>,
    write_lock: Arc<tokio::sync::Mutex<()>>,
    reload_lock: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    reload_lock_attempts: Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Clone, Default)]
pub(in crate::table_catalog) struct StrongTableCatalogState {
    pub(super) hydrated: bool,
    snapshot_required: bool,
    pub(in crate::table_catalog) snapshot_etag: Option<String>,
    snapshot_version: Option<u16>,
    pub(in crate::table_catalog) table_buckets: BTreeMap<String, TableBucketEntry>,
    pub(in crate::table_catalog) namespaces: BTreeMap<StrongNamespaceKey, NamespaceEntry>,
    namespace_children: BTreeMap<StrongNamespaceChildKey, String>,
    namespace_objects: BTreeSet<StrongNamespaceKey>,
    pub(super) tables: BTreeMap<StrongResourceKey, TableEntry>,
    pub(super) views: BTreeMap<StrongResourceKey, ViewEntry>,
    pub(super) commits: BTreeMap<StrongCommitKey, CommitLogEntry>,
    pub(super) idempotency: BTreeMap<StrongCommitKey, CommitLogEntry>,
    pub(super) warehouse_index: StrongWarehouseIndex,
    identifier_collisions: BTreeSet<StrongResourceKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::table_catalog) struct StrongCommitSnapshotRecord {
    pub(super) table_bucket: String,
    pub(super) table_id: String,
    pub(super) lookup_key: String,
    pub(in crate::table_catalog) commit: CommitLogEntry,
}

#[cfg(test)]
impl StrongCommitSnapshotRecord {
    pub(in crate::table_catalog) fn new_for_test(
        table_bucket: String,
        table_id: String,
        lookup_key: String,
        commit: CommitLogEntry,
    ) -> Self {
        Self {
            table_bucket,
            table_id,
            lookup_key,
            commit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
pub(in crate::table_catalog) struct StrongTableCatalogBucketSnapshot {
    pub(super) table_bucket: TableBucketEntry,
    pub(super) namespaces: Vec<NamespaceEntry>,
    pub(super) tables: Vec<TableEntry>,
    pub(super) views: Vec<ViewEntry>,
    pub(super) commits: Vec<StrongCommitSnapshotRecord>,
    pub(super) idempotency: Vec<StrongCommitSnapshotRecord>,
}

#[derive(Clone)]
enum StrongSnapshotWritePostcondition {
    TableBucketPresent(TableBucketEntry),
    TableBucketAbsent(String),
    NamespacePresent(NamespaceEntry),
    NamespaceAbsent(StrongNamespaceKey),
    TablePresent(TableEntry),
    TableAbsent {
        key: StrongResourceKey,
        table_id: String,
    },
    TableRenamed {
        source_key: StrongResourceKey,
        destination_key: StrongResourceKey,
        table_id: String,
    },
    ViewPresent(ViewEntry),
    ViewAbsent {
        key: StrongResourceKey,
        view_id: String,
    },
    CommitPresent {
        table_bucket: String,
        table_id: String,
        commit: CommitLogEntry,
    },
    BucketSnapshotPresent(StrongTableCatalogBucketSnapshot),
}

impl StrongSnapshotWritePostcondition {
    fn is_satisfied_by<B>(&self, state: &StrongTableCatalogState) -> bool
    where
        B: TableCatalogObjectBackend,
    {
        match self {
            Self::TableBucketPresent(expected) => state.table_buckets.get(&expected.table_bucket) == Some(expected),
            Self::TableBucketAbsent(table_bucket) => !state.table_buckets.contains_key(table_bucket),
            Self::NamespacePresent(expected) => {
                let Ok(namespace) = parse_namespace_for_store(&expected.namespace) else {
                    return false;
                };
                state
                    .namespaces
                    .get(&StrongTableCatalogStore::<B>::namespace_key(&expected.table_bucket, &namespace))
                    == Some(expected)
            }
            Self::NamespaceAbsent(key) => !state.namespaces.contains_key(key),
            Self::TablePresent(expected) => {
                let (Ok(namespace), Ok(table)) =
                    (parse_namespace_for_store(&expected.namespace), parse_table_for_store(&expected.table))
                else {
                    return false;
                };
                state
                    .tables
                    .get(&StrongTableCatalogStore::<B>::table_key(&expected.table_bucket, &namespace, &table))
                    == Some(expected)
            }
            Self::TableAbsent { key, table_id } => state.tables.get(key).is_none_or(|current| current.table_id != *table_id),
            Self::TableRenamed {
                source_key,
                destination_key,
                table_id,
            } => {
                state
                    .tables
                    .get(source_key)
                    .is_none_or(|current| current.table_id != *table_id)
                    && state
                        .tables
                        .get(destination_key)
                        .is_some_and(|current| current.table_id == *table_id)
            }
            Self::ViewPresent(expected) => {
                let (Ok(namespace), Ok(view)) =
                    (parse_namespace_for_store(&expected.namespace), parse_table_for_store(&expected.view))
                else {
                    return false;
                };
                state
                    .views
                    .get(&StrongTableCatalogStore::<B>::table_key(&expected.table_bucket, &namespace, &view))
                    == Some(expected)
            }
            Self::ViewAbsent { key, view_id } => state.views.get(key).is_none_or(|current| current.view_id != *view_id),
            Self::CommitPresent {
                table_bucket,
                table_id,
                commit,
            } => {
                let commit_key = StrongTableCatalogStore::<B>::commit_key(table_bucket, table_id, &commit.commit_id);
                if state.commits.get(&commit_key) != Some(commit) {
                    return false;
                }
                if !commit.idempotency_key.as_deref().is_none_or(|idempotency_key| {
                    state
                        .idempotency
                        .get(&StrongTableCatalogStore::<B>::idempotency_key(table_bucket, table_id, idempotency_key))
                        == Some(commit)
                }) {
                    return false;
                }
                let Some(table) = state.tables.values().find(|table| {
                    table.table_bucket == *table_bucket
                        && table.table_id == *table_id
                        && table.state == TableCatalogEntryState::Active
                }) else {
                    return false;
                };
                TableCommitHistoryIndex::new(
                    table,
                    StrongTableCatalogStore::<B>::table_commits_locked(state, table_bucket, table_id),
                )
                .proves_committed(commit)
            }
            Self::BucketSnapshotPresent(expected) => {
                StrongTableCatalogStore::<B>::bucket_snapshot_from_state_locked(state, &expected.table_bucket.table_bucket)
                    .as_ref()
                    == Some(expected)
            }
        }
    }
}

#[cfg(test)]
impl StrongTableCatalogBucketSnapshot {
    pub(in crate::table_catalog) fn new_for_test(table_bucket: TableBucketEntry) -> Self {
        Self {
            table_bucket,
            namespaces: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
            commits: Vec::new(),
            idempotency: Vec::new(),
        }
    }

    pub(in crate::table_catalog) fn push_commit_for_test(&mut self, record: StrongCommitSnapshotRecord) {
        self.commits.push(record);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum TableCatalogBackingMigrationFenceStatus {
    Preparing,
    Materialized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TableCatalogBackingMigrationTargetSnapshotState {
    Unknown,
    Absent,
    Present,
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

pub(in crate::table_catalog) fn table_catalog_bucket_snapshot_fingerprint(
    snapshot: &StrongTableCatalogBucketSnapshot,
) -> TableCatalogStoreResult<String> {
    let data = serde_json::to_vec(snapshot)
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode catalog migration snapshot: {err}")))?;
    Ok(hex_simd::encode_to_string(Sha256::digest(data), hex_simd::AsciiCase::Lower))
}

#[derive(Clone)]
pub(crate) struct StrongTableCatalogStore<B> {
    object_backend: B,
    snapshot_write_version: u16,
    snapshot_required_on_start: bool,
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
    // Coalesces reloads for clones of one store so only one task reads and decodes a changed snapshot.
    reload_lock: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    reload_lock_attempts: Arc<std::sync::atomic::AtomicUsize>,
}

impl<B> StrongTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub fn new(object_backend: B) -> Self {
        Self::new_with_snapshot_requirement(object_backend, false)
    }

    pub(in crate::table_catalog) fn new_requiring_snapshot(object_backend: B) -> Self {
        Self::new_with_snapshot_requirement(object_backend, true)
    }

    #[cfg(test)]
    pub(in crate::table_catalog) async fn is_hydrated_for_test(&self) -> bool {
        self.state.lock().await.hydrated
    }

    #[cfg(test)]
    pub(in crate::table_catalog) fn reload_lock_attempts_for_test(&self) -> usize {
        self.reload_lock_attempts.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn new_with_snapshot_requirement(object_backend: B, snapshot_required_on_start: bool) -> Self {
        let snapshot_write_version = strong_snapshot_write_version(
            rustfs_utils::get_env_bool(ENV_TABLE_CATALOG_STRONG_SNAPSHOT_V2, false),
            rustfs_utils::get_env_bool(ENV_TABLE_CATALOG_STRONG_SNAPSHOT_V2_FLEET_CONFIRMED, false),
        );
        // RUSTFS_COMPAT_TODO(table-catalog-strong-snapshot-v1): Keep version 1 writes during mixed-version rollout. Remove after all supported releases read version 2 and every retained snapshot is upgraded.
        // Remove after the minimum supported release reads version 2 and operators no longer need collision cleanup.
        let runtime = object_backend.strong_catalog_runtime().unwrap_or_default();
        Self::new_with_runtime_and_snapshot_write_version(
            object_backend,
            runtime,
            snapshot_write_version,
            snapshot_required_on_start,
        )
    }

    fn new_with_runtime_and_snapshot_write_version(
        object_backend: B,
        runtime: StrongTableCatalogRuntime,
        snapshot_write_version: u16,
        snapshot_required_on_start: bool,
    ) -> Self {
        Self {
            object_backend,
            snapshot_write_version,
            snapshot_required_on_start,
            state: runtime.state,
            write_lock: runtime.write_lock,
            reload_lock: runtime.reload_lock,
            #[cfg(test)]
            reload_lock_attempts: runtime.reload_lock_attempts,
        }
    }

    #[cfg(test)]
    pub(in crate::table_catalog) fn new_with_snapshot_write_version(object_backend: B, snapshot_write_version: u16) -> Self {
        let runtime = object_backend.strong_catalog_runtime().unwrap_or_default();
        Self::new_with_runtime_and_snapshot_write_version(object_backend, runtime, snapshot_write_version, false)
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
        if let Some(entry) = state.namespaces.get(&key) {
            return entry.state == TableCatalogEntryState::Active;
        }
        state.namespace_objects.contains(&key)
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

    fn ensure_identifier_is_unambiguous_locked(
        state: &StrongTableCatalogState,
        key: &StrongResourceKey,
    ) -> TableCatalogStoreResult<()> {
        if state.identifier_collisions.contains(key) {
            return Err(TableCatalogStoreError::Internal(format!(
                "legacy table/view identifier collision requires operator cleanup: {}/{}/{}",
                key.0, key.1, key.2
            )));
        }
        Ok(())
    }

    fn ensure_namespace_identifiers_are_unambiguous_locked(
        state: &StrongTableCatalogState,
        table_bucket: &str,
        namespace: &str,
    ) -> TableCatalogStoreResult<()> {
        if state
            .identifier_collisions
            .iter()
            .any(|(bucket, entry_namespace, _)| bucket == table_bucket && entry_namespace == namespace)
        {
            return Err(TableCatalogStoreError::Internal(format!(
                "legacy table/view identifier collision requires operator cleanup in {table_bucket}/{namespace}"
            )));
        }
        Ok(())
    }

    fn ensure_table_bucket_identifiers_are_unambiguous_locked(
        state: &StrongTableCatalogState,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<()> {
        if state
            .identifier_collisions
            .iter()
            .any(|(bucket, _, _)| bucket == table_bucket)
        {
            return Err(TableCatalogStoreError::Internal(format!(
                "legacy table/view identifier collision requires operator cleanup in {table_bucket}"
            )));
        }
        Ok(())
    }

    fn commit_key(table_bucket: &str, table_id: &str, commit_id: &str) -> StrongCommitKey {
        (table_bucket.to_string(), table_id.to_string(), commit_id.to_string())
    }

    fn idempotency_key(table_bucket: &str, table_id: &str, idempotency_key: &str) -> StrongCommitKey {
        (table_bucket.to_string(), table_id.to_string(), idempotency_key.to_string())
    }

    fn table_commits_locked<'a>(
        state: &'a StrongTableCatalogState,
        table_bucket: &str,
        table_id: &str,
    ) -> impl Iterator<Item = &'a CommitLogEntry> + 'a {
        let owner = (table_bucket.to_string(), table_id.to_string());
        let start = (owner.0.clone(), owner.1.clone(), String::new());
        state
            .commits
            .range(start..)
            .take_while(move |((entry_bucket, entry_table_id, _), _)| entry_bucket == &owner.0 && entry_table_id == &owner.1)
            .map(|(_, commit)| commit)
    }

    fn commit_write_postcondition(table_bucket: &str, commit: &CommitLogEntry) -> StrongSnapshotWritePostcondition {
        StrongSnapshotWritePostcondition::CommitPresent {
            table_bucket: table_bucket.to_string(),
            table_id: commit.table_id.clone(),
            commit: commit.clone(),
        }
    }

    pub(in crate::table_catalog) fn snapshot_object_path() -> String {
        format!("{INTERNAL_CATALOG_ROOT}/{STRONG_TABLE_CATALOG_BACKING_ROOT}/{STRONG_TABLE_CATALOG_SNAPSHOT_FILE}")
    }

    // Ordinary mutations hold the global migration read lock before the local write lock; migration takes the
    // write side before invoking its dedicated snapshot mutation methods.
    async fn acquire_snapshot_write_permit(&self) -> TableCatalogStoreResult<TableCatalogLockGuard> {
        let lock_path = TableCatalogObjectPaths::default().backing_migration_global_fence_lock_path();
        self.object_backend.acquire_read_lock(RUSTFS_META_BUCKET, &lock_path).await
    }

    fn effective_snapshot_write_version(state: &StrongTableCatalogState, configured_write_version: u16) -> u16 {
        state
            .snapshot_version
            .unwrap_or(STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION)
            .max(configured_write_version)
    }

    fn snapshot_from_state_locked(state: &StrongTableCatalogState, snapshot_version: u16) -> StrongTableCatalogSnapshot {
        StrongTableCatalogSnapshot {
            version: snapshot_version,
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
        state
            .identifier_collisions
            .retain(|(entry_bucket, _, _)| entry_bucket != table_bucket);
    }

    pub(super) fn insert_bucket_snapshot_locked(
        state: &mut StrongTableCatalogState,
        snapshot: StrongTableCatalogBucketSnapshot,
    ) -> TableCatalogStoreResult<()> {
        let expected_counts = (
            1,
            snapshot.namespaces.len(),
            snapshot.tables.len(),
            snapshot.views.len(),
            snapshot.commits.len(),
            snapshot.idempotency.len(),
        );
        let table_bucket = snapshot.table_bucket.table_bucket.clone();
        let unexpected_owner = snapshot
            .namespaces
            .iter()
            .map(|entry| entry.table_bucket.as_str())
            .chain(snapshot.tables.iter().map(|entry| entry.table_bucket.as_str()))
            .chain(snapshot.views.iter().map(|entry| entry.table_bucket.as_str()))
            .chain(snapshot.commits.iter().map(|record| record.table_bucket.as_str()))
            .chain(snapshot.idempotency.iter().map(|record| record.table_bucket.as_str()))
            .find(|owner| *owner != table_bucket);
        if let Some(owner) = unexpected_owner {
            return Err(TableCatalogStoreError::Invalid(format!(
                "durable strong bucket snapshot for {table_bucket} contains state owned by {owner}"
            )));
        }
        let validated = Self::state_from_snapshot(
            StrongTableCatalogSnapshot {
                version: STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION,
                table_buckets: vec![snapshot.table_bucket],
                namespaces: snapshot.namespaces,
                tables: snapshot.tables,
                views: snapshot.views,
                commits: snapshot.commits,
                idempotency: snapshot.idempotency,
            },
            None,
        )?;
        let validated_counts = (
            validated.table_buckets.len(),
            validated.namespaces.len(),
            validated.tables.len(),
            validated.views.len(),
            validated.commits.len(),
            validated.idempotency.len(),
        );
        if validated_counts != expected_counts {
            return Err(TableCatalogStoreError::Invalid(
                "durable strong bucket snapshot contains state without a valid owner".to_string(),
            ));
        }
        Self::remove_bucket_from_state_locked(state, &table_bucket);
        state.table_buckets.extend(validated.table_buckets);
        state.namespaces.extend(validated.namespaces);
        state.tables.extend(validated.tables);
        state.views.extend(validated.views);
        state.commits.extend(validated.commits);
        state.idempotency.extend(validated.idempotency);
        Self::rebuild_namespace_indexes_locked(state)?;
        Self::rebuild_warehouse_index_locked(state)?;
        Ok(())
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
        for ((table_bucket, namespace_name), entry) in &state.namespaces {
            let namespace = validate_namespace_entry_identity(entry)?;
            validate_namespace_properties(&entry.properties)?;
            if entry.table_bucket != *table_bucket || entry.namespace != *namespace_name {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog namespace entry identity does not match its snapshot key: {table_bucket}/{namespace_name}"
                )));
            }
            let Some(bucket_entry) = state.table_buckets.get(table_bucket) else {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog namespace {table_bucket}/{namespace_name} has no table bucket"
                )));
            };
            if entry.state != TableCatalogEntryState::Active {
                continue;
            }
            if bucket_entry.state != TableCatalogEntryState::Active {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "active namespace {table_bucket}/{namespace_name} has no active table bucket"
                )));
            }
            Self::index_namespace_children(&mut children, &entry.table_bucket, &namespace);
        }

        let mut objects = BTreeSet::new();
        for (table_bucket, namespace_name, active) in state
            .tables
            .values()
            .map(|entry| (&entry.table_bucket, &entry.namespace, entry.state == TableCatalogEntryState::Active))
            .chain(
                state
                    .views
                    .values()
                    .map(|entry| (&entry.table_bucket, &entry.namespace, entry.state == TableCatalogEntryState::Active)),
            )
        {
            let namespace = parse_namespace_for_store(namespace_name)?;
            if !active {
                continue;
            }
            objects.insert(Self::namespace_key(table_bucket, &namespace));
            Self::index_namespace_children(&mut children, table_bucket, &namespace);
        }
        state.namespace_children = children;
        state.namespace_objects = objects;
        for ((table_bucket, namespace), entry) in &state.namespaces {
            if entry.state == TableCatalogEntryState::Active {
                continue;
            }
            let key = (table_bucket.clone(), namespace.clone());
            if state.namespace_objects.contains(&key)
                || Self::has_active_namespace_descendant_locked(state, table_bucket, namespace)
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "inactive namespace {table_bucket}/{namespace} has active resources or descendants"
                )));
            }
        }
        Ok(())
    }

    fn rebuild_warehouse_index_locked(state: &mut StrongTableCatalogState) -> TableCatalogStoreResult<()> {
        let mut warehouse_index: StrongWarehouseIndex = BTreeMap::new();
        for ((table_bucket, namespace, table), entry) in &state.tables {
            if entry.table_bucket != *table_bucket || entry.namespace != *namespace || entry.table != *table {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog table entry identity does not match its snapshot key: {table_bucket}/{namespace}/{table}"
                )));
            }
            if !state.table_buckets.contains_key(table_bucket) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog table {table_bucket}/{namespace}/{table} has no table bucket"
                )));
            }
            if entry.state != TableCatalogEntryState::Active {
                continue;
            }
            let namespace_identity = parse_namespace_for_store(namespace)?;
            validate_table_warehouse_location(table_bucket, &entry.warehouse_location)?;
            if !is_valid_table_metadata_location_for_entry(entry, &entry.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog table {table_bucket}/{namespace}/{table} has an invalid metadata location"
                )));
            }
            if !Self::namespace_exists_locked(state, table_bucket, &namespace_identity) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "table {table_bucket}/{namespace}/{table} has no active namespace"
                )));
            }
            if !state
                .table_buckets
                .get(table_bucket)
                .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "active table {table_bucket}/{namespace}/{table} has no active table bucket"
                )));
            }
            let warehouse_object_prefix = table_warehouse_object_prefix(entry)?;
            let table_key = (table_bucket.clone(), namespace.clone(), table.clone());
            let bucket_index = warehouse_index.entry(table_bucket.clone()).or_default();
            let predecessor = bucket_index.range(..=warehouse_object_prefix.clone()).next_back();
            let successor = bucket_index.range(warehouse_object_prefix.clone()..).next();
            if let Some((existing_prefix, existing_key)) = predecessor
                .into_iter()
                .chain(successor)
                .find(|(existing_prefix, _)| warehouse_object_prefixes_overlap(existing_prefix, &warehouse_object_prefix))
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "overlapping active table warehouse location in strong catalog snapshot: {warehouse_object_prefix} overlaps {existing_prefix} owned by {}/{}/{} and {}/{}/{}",
                    existing_key.0, existing_key.1, existing_key.2, table_key.0, table_key.1, table_key.2
                )));
            }
            bucket_index.insert(warehouse_object_prefix, table_key);
        }

        let mut identifier_collisions = BTreeSet::new();
        for ((table_bucket, namespace, view), entry) in &state.views {
            if entry.table_bucket != *table_bucket || entry.namespace != *namespace || entry.view != *view {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog view entry identity does not match its snapshot key: {table_bucket}/{namespace}/{view}"
                )));
            }
            if !state.table_buckets.contains_key(table_bucket) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog view {table_bucket}/{namespace}/{view} has no table bucket"
                )));
            }
            if entry.state != TableCatalogEntryState::Active {
                continue;
            }
            let namespace_identity = parse_namespace_for_store(namespace)?;
            let view_identity = parse_table_for_store(view)?;
            validate_view_warehouse_location(table_bucket, &entry.warehouse_location)?;
            if !is_valid_view_metadata_location(&namespace_identity, &view_identity, &entry.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog view {table_bucket}/{namespace}/{view} has an invalid metadata location"
                )));
            }
            if !Self::namespace_exists_locked(state, table_bucket, &namespace_identity) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "view {table_bucket}/{namespace}/{view} has no active namespace"
                )));
            }
            if !state
                .table_buckets
                .get(table_bucket)
                .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "active view {table_bucket}/{namespace}/{view} has no active table bucket"
                )));
            }
            let view_key = (table_bucket.clone(), namespace.clone(), view.clone());
            if state
                .tables
                .get(&view_key)
                .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
            {
                identifier_collisions.insert(view_key);
            }
        }
        state.warehouse_index = warehouse_index;
        state.identifier_collisions = identifier_collisions;
        Ok(())
    }

    fn validate_inactive_resource_locations_locked(state: &StrongTableCatalogState) -> TableCatalogStoreResult<()> {
        for ((table_bucket, namespace, table), entry) in &state.tables {
            if entry.state == TableCatalogEntryState::Active {
                continue;
            }
            let namespace_identity = parse_namespace_for_store(namespace)?;
            validate_table_warehouse_location(table_bucket, &entry.warehouse_location)?;
            if !is_valid_table_metadata_location_for_entry(entry, &entry.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog table {table_bucket}/{namespace}/{table} has an invalid metadata location"
                )));
            }
            if !Self::namespace_exists_locked(state, table_bucket, &namespace_identity) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "table {table_bucket}/{namespace}/{table} has no active namespace"
                )));
            }
        }
        for ((table_bucket, namespace, view), entry) in &state.views {
            if entry.state == TableCatalogEntryState::Active {
                continue;
            }
            let namespace_identity = parse_namespace_for_store(namespace)?;
            let view_identity = parse_table_for_store(view)?;
            validate_view_warehouse_location(table_bucket, &entry.warehouse_location)?;
            if !is_valid_view_metadata_location(&namespace_identity, &view_identity, &entry.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog view {table_bucket}/{namespace}/{view} has an invalid metadata location"
                )));
            }
            if !Self::namespace_exists_locked(state, table_bucket, &namespace_identity) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "view {table_bucket}/{namespace}/{view} has no active namespace"
                )));
            }
        }
        Ok(())
    }

    fn snapshot_from_mutated_state_locked(
        state: &mut StrongTableCatalogState,
        configured_write_version: u16,
    ) -> TableCatalogStoreResult<StrongTableCatalogSnapshot> {
        let previous_identifier_collisions = state.identifier_collisions.clone();
        Self::rebuild_namespace_indexes_locked(state)?;
        Self::rebuild_warehouse_index_locked(state)?;
        if !state.identifier_collisions.is_empty()
            && state
                .snapshot_version
                .is_some_and(|version| version >= STRONG_TABLE_CATALOG_SNAPSHOT_VERSION)
        {
            return Err(TableCatalogStoreError::Invalid(
                "version 1 collision quarantine cannot be persisted after snapshot version 2 was observed".to_string(),
            ));
        }
        let collision_cleanup_progress = !previous_identifier_collisions.is_empty()
            && state.identifier_collisions.is_subset(&previous_identifier_collisions)
            && state.identifier_collisions.len() < previous_identifier_collisions.len();
        // RUSTFS_COMPAT_TODO(table-catalog-strong-snapshot-v1): Version 1 collision quarantine permits cleanup-only writes. Remove after all supported releases read version 2 and every retained snapshot is collision-free.
        // Remove with version 1 snapshot write support after every supported deployment writes collision-free version 2 snapshots.
        if !state.identifier_collisions.is_empty() && !collision_cleanup_progress {
            return Err(TableCatalogStoreError::Conflict(
                "legacy table/view identifier collision permits only collision cleanup".to_string(),
            ));
        }
        let snapshot_version = if state.identifier_collisions.is_empty() {
            Self::effective_snapshot_write_version(state, configured_write_version)
        } else {
            STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION
        };
        if snapshot_version >= STRONG_TABLE_CATALOG_SNAPSHOT_VERSION {
            Self::validate_inactive_resource_locations_locked(state)?;
        }
        let snapshot = Self::snapshot_from_state_locked(state, snapshot_version);
        Self::state_from_snapshot(snapshot.clone(), None)?;
        state.snapshot_version = Some(state.snapshot_version.unwrap_or(snapshot.version).max(snapshot.version));
        Ok(snapshot)
    }

    fn state_from_snapshot(
        snapshot: StrongTableCatalogSnapshot,
        snapshot_etag: Option<String>,
    ) -> TableCatalogStoreResult<StrongTableCatalogState> {
        if !(STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION..=STRONG_TABLE_CATALOG_SNAPSHOT_VERSION).contains(&snapshot.version) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "unsupported strong catalog snapshot version: {}",
                snapshot.version
            )));
        }

        let snapshot_version = snapshot.version;
        let mut state = StrongTableCatalogState {
            hydrated: true,
            snapshot_required: true,
            snapshot_etag,
            snapshot_version: Some(snapshot_version),
            ..StrongTableCatalogState::default()
        };
        for entry in snapshot.table_buckets {
            validate_table_bucket_entry(&entry)?;
            let table_bucket = entry.table_bucket.clone();
            if state.table_buckets.insert(table_bucket.clone(), entry).is_some() {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog snapshot contains duplicate table bucket {table_bucket}"
                )));
            }
        }
        for entry in snapshot.namespaces {
            let namespace = validate_namespace_entry_identity(&entry)?;
            validate_namespace_properties(&entry.properties)?;
            let Some(table_bucket_entry) = state.table_buckets.get(&entry.table_bucket) else {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog namespace {}/{} has no table bucket",
                    entry.table_bucket, entry.namespace
                )));
            };
            if entry.state == TableCatalogEntryState::Active && table_bucket_entry.state != TableCatalogEntryState::Active {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "active namespace {}/{} has no active table bucket",
                    entry.table_bucket, entry.namespace
                )));
            }
            let key = Self::namespace_key(&entry.table_bucket, &namespace);
            if state.namespaces.insert(key, entry).is_some() {
                return Err(TableCatalogStoreError::Invalid(
                    "strong catalog snapshot contains duplicate namespace identifiers".to_string(),
                ));
            }
        }
        let mut table_ids = BTreeSet::new();
        for entry in snapshot.tables {
            validate_table_entry_version_and_id(&entry)?;
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let table = parse_table_for_store(&entry.table)?;
            if !state.table_buckets.contains_key(&entry.table_bucket) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog table {}/{}/{} has no table bucket",
                    entry.table_bucket, entry.namespace, entry.table
                )));
            }
            if !table_ids.insert((entry.table_bucket.clone(), entry.table_id.clone())) {
                return Err(TableCatalogStoreError::Invalid(
                    "strong catalog snapshot contains duplicate table ids".to_string(),
                ));
            }
            let key = Self::table_key(&entry.table_bucket, &namespace, &table);
            if state.tables.insert(key, entry).is_some() {
                return Err(TableCatalogStoreError::Invalid(
                    "strong catalog snapshot contains duplicate table identifiers".to_string(),
                ));
            }
        }
        for entry in snapshot.views {
            validate_view_entry_version_and_id(&entry)?;
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let view = parse_table_for_store(&entry.view)?;
            if !state.table_buckets.contains_key(&entry.table_bucket) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog view {}/{}/{} has no table bucket",
                    entry.table_bucket, entry.namespace, entry.view
                )));
            }
            let key = Self::table_key(&entry.table_bucket, &namespace, &view);
            if state.views.insert(key, entry).is_some() {
                return Err(TableCatalogStoreError::Invalid(
                    "strong catalog snapshot contains duplicate view identifiers".to_string(),
                ));
            }
        }
        for record in snapshot.commits {
            validate_catalog_entry_version("commit log", record.commit.version)?;
            if record.commit.table_id != record.table_id || record.commit.commit_id != record.lookup_key {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog commit {} does not match its snapshot owner",
                    record.lookup_key
                )));
            }
            if !table_ids.contains(&(record.table_bucket.clone(), record.table_id.clone())) {
                if snapshot_version >= STRONG_TABLE_CATALOG_SNAPSHOT_VERSION {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "strong catalog commit {} has no owning table",
                        record.lookup_key
                    )));
                }
                continue;
            }
            let key = Self::commit_key(&record.table_bucket, &record.table_id, &record.lookup_key);
            if state.commits.insert(key, record.commit).is_some() {
                return Err(TableCatalogStoreError::Invalid(
                    "strong catalog snapshot contains duplicate commit lookup keys".to_string(),
                ));
            }
        }
        for record in snapshot.idempotency {
            validate_catalog_entry_version("commit idempotency", record.commit.version)?;
            if record.commit.table_id != record.table_id
                || record.commit.idempotency_key.as_deref() != Some(record.lookup_key.as_str())
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog idempotency index {} does not match its snapshot owner",
                    record.lookup_key
                )));
            }
            if !table_ids.contains(&(record.table_bucket.clone(), record.table_id.clone())) {
                if snapshot_version >= STRONG_TABLE_CATALOG_SNAPSHOT_VERSION {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "strong catalog idempotency index {} has no owning table",
                        record.lookup_key
                    )));
                }
                continue;
            }
            let commit_key = Self::commit_key(&record.table_bucket, &record.table_id, &record.commit.commit_id);
            if state.commits.get(&commit_key) != Some(&record.commit) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog idempotency index {} has no matching commit",
                    record.lookup_key
                )));
            }
            let key = Self::idempotency_key(&record.table_bucket, &record.table_id, &record.lookup_key);
            if state.idempotency.insert(key, record.commit).is_some() {
                return Err(TableCatalogStoreError::Invalid(
                    "strong catalog snapshot contains duplicate idempotency lookup keys".to_string(),
                ));
            }
        }
        for ((table_bucket, table_id, _), commit) in &state.commits {
            let Some(idempotency_key) = commit.idempotency_key.as_deref() else {
                continue;
            };
            let index_key = Self::idempotency_key(table_bucket, table_id, idempotency_key);
            if state.idempotency.get(&index_key) != Some(commit) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog commit {} has no matching idempotency index",
                    commit.commit_id
                )));
            }
        }
        for table in state.tables.values() {
            let table_commits = Self::table_commits_locked(&state, &table.table_bucket, &table.table_id).collect::<Vec<_>>();
            let history = TableCommitHistoryIndex::new(table, table_commits.iter().copied());
            if let Some(commit) = table_commits
                .into_iter()
                .find(|commit| matches!(commit.status, CommitLogStatus::Failed) || !history.proves_committed(commit))
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "strong catalog commit {} is not recoverable in the current table history",
                    commit.commit_id
                )));
            }
        }
        Self::rebuild_namespace_indexes_locked(&mut state)?;
        Self::rebuild_warehouse_index_locked(&mut state)?;
        if snapshot_version >= STRONG_TABLE_CATALOG_SNAPSHOT_VERSION {
            Self::validate_inactive_resource_locations_locked(&state)?;
        }
        if snapshot_version >= STRONG_TABLE_CATALOG_SNAPSHOT_VERSION && !state.identifier_collisions.is_empty() {
            return Err(TableCatalogStoreError::Invalid(
                "strong catalog snapshot contains a table/view identifier collision".to_string(),
            ));
        }
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

    fn snapshot_observation_locked(state: &StrongTableCatalogState) -> StrongSnapshotObservation {
        StrongSnapshotObservation {
            hydrated: state.hydrated,
            snapshot_required: state.snapshot_required,
            etag: state.snapshot_etag.clone(),
            version: state.snapshot_version,
        }
    }

    fn snapshot_etag(etag: Option<String>) -> TableCatalogStoreResult<String> {
        etag.filter(|etag| !etag.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Internal("durable strong catalog snapshot has no etag".to_string()))
    }

    fn state_from_snapshot_object(snapshot_object: TableCatalogObject) -> TableCatalogStoreResult<StrongTableCatalogState> {
        let snapshot_etag = Self::snapshot_etag(snapshot_object.etag)?;
        let snapshot = serde_json::from_slice::<StrongTableCatalogSnapshot>(&snapshot_object.data)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to decode strong catalog snapshot: {err}")))?;
        Self::state_from_snapshot(snapshot, Some(snapshot_etag))
    }

    async fn mark_snapshot_reload_failed(&self, err: &TableCatalogStoreError, phase: &'static str) {
        self.state.lock().await.hydrated = false;
        tracing::warn!(
            error = %err,
            phase,
            "durable strong catalog state reload failed after a snapshot write outcome"
        );
    }

    async fn hydrate_state(&self) -> TableCatalogStoreResult<()> {
        let Some((current_snapshot_etag, current_snapshot_required)) = ({
            let state = self.state.lock().await;
            if state.hydrated {
                Some((state.snapshot_etag.clone(), state.snapshot_required))
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
            (None, None) if !self.snapshot_required_on_start && !current_snapshot_required => Ok(()),
            (Some(metadata), Some(current_etag)) if metadata.etag.as_deref() == Some(current_etag) => Ok(()),
            _ => self.reload_state_from_durable().await,
        }
    }

    pub(in crate::table_catalog) async fn reload_state_from_durable(&self) -> TableCatalogStoreResult<()> {
        let initial_observation = {
            let state = self.state.lock().await;
            Self::snapshot_observation_locked(&state)
        };
        #[cfg(test)]
        self.reload_lock_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _reload_guard = self.reload_lock.lock().await;
        {
            let state = self.state.lock().await;
            if state.hydrated
                && (!self.snapshot_required_on_start || state.snapshot_required)
                && Self::snapshot_observation_locked(&state) != initial_observation
            {
                return Ok(());
            }
        }

        for _ in 0..STRONG_TABLE_CATALOG_RELOAD_MAX_ATTEMPTS {
            let observed_state = {
                let state = self.state.lock().await;
                Self::snapshot_observation_locked(&state)
            };
            let snapshot_object = self
                .object_backend
                .read_object_limited(RUSTFS_META_BUCKET, &Self::snapshot_object_path(), STRONG_TABLE_CATALOG_SNAPSHOT_MAX_SIZE)
                .await?;
            let snapshot_metadata = self
                .object_backend
                .object_metadata(RUSTFS_META_BUCKET, &Self::snapshot_object_path())
                .await?;
            let loaded_state = match (snapshot_object, snapshot_metadata) {
                (Some(snapshot_object), Some(snapshot_metadata)) => {
                    let object_etag = Self::snapshot_etag(snapshot_object.etag.clone())?;
                    let metadata_etag = Self::snapshot_etag(snapshot_metadata.etag)?;
                    if object_etag != metadata_etag {
                        continue;
                    }
                    Some(Self::state_from_snapshot_object(snapshot_object)?)
                }
                (Some(_), None) => {
                    return Err(TableCatalogStoreError::Internal(
                        "durable strong catalog snapshot disappeared while it was being loaded".to_string(),
                    ));
                }
                (None, Some(_)) => continue,
                (None, None) => None,
            };
            let mut state = self.state.lock().await;
            if Self::snapshot_observation_locked(&state) != observed_state {
                continue;
            }
            let loaded_state = match loaded_state {
                Some(loaded_state) => loaded_state,
                None if observed_state.snapshot_required || self.snapshot_required_on_start => {
                    return Err(TableCatalogStoreError::Internal(
                        "durable strong catalog snapshot disappeared after it was observed".to_string(),
                    ));
                }
                None => StrongTableCatalogState {
                    hydrated: true,
                    ..StrongTableCatalogState::default()
                },
            };
            if let (Some(observed), Some(loaded)) = (observed_state.version, loaded_state.snapshot_version)
                && loaded < observed
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "durable strong catalog snapshot version {loaded} cannot replace process high-water version {observed}"
                )));
            }
            *state = loaded_state;
            return Ok(());
        }
        Err(TableCatalogStoreError::Internal(
            "durable strong catalog state changed repeatedly while reloading".to_string(),
        ))
    }

    async fn finalize_snapshot_write(
        &self,
        snapshot: StrongTableCatalogSnapshot,
        precondition: TableCatalogPutPrecondition,
        postcondition: StrongSnapshotWritePostcondition,
    ) -> TableCatalogStoreResult<()> {
        let data = serde_json::to_vec(&snapshot)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to encode strong catalog snapshot: {err}")))?;
        if data.len() > STRONG_TABLE_CATALOG_SNAPSHOT_MAX_SIZE {
            return Err(TableCatalogStoreError::Invalid(format!(
                "durable strong catalog snapshot exceeds the maximum encoded size of {STRONG_TABLE_CATALOG_SNAPSHOT_MAX_SIZE} bytes"
            )));
        }
        match self
            .object_backend
            .put_object(RUSTFS_META_BUCKET, &Self::snapshot_object_path(), data, precondition)
            .await
        {
            Ok(()) => {
                self.state.lock().await.snapshot_required = true;
                if let Err(err) = self.reload_state_from_durable().await {
                    self.mark_snapshot_reload_failed(&err, "confirmed-write").await;
                }
                Ok(())
            }
            Err(err @ TableCatalogStoreError::Conflict(_)) => {
                self.state.lock().await.snapshot_required = true;
                if let Err(reload_err) = self.reload_state_from_durable().await {
                    self.mark_snapshot_reload_failed(&reload_err, "write-conflict").await;
                }
                Err(err)
            }
            Err(err) => {
                self.state.lock().await.snapshot_required = true;
                match self.reload_state_from_durable().await {
                    Ok(()) => {
                        let state = self.state.lock().await;
                        if postcondition.is_satisfied_by::<B>(&state) {
                            Ok(())
                        } else {
                            Err(err)
                        }
                    }
                    Err(reload_err) => {
                        self.mark_snapshot_reload_failed(&reload_err, "ambiguous-write").await;
                        Err(err)
                    }
                }
            }
        }
    }

    pub(in crate::table_catalog) async fn materialize_bucket_snapshot(
        &self,
        source: StrongTableCatalogBucketSnapshot,
    ) -> TableCatalogStoreResult<(String, bool)> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let table_bucket = source.table_bucket.table_bucket.clone();
        let (snapshot, precondition, postcondition) = {
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
            let postcondition = StrongSnapshotWritePostcondition::BucketSnapshotPresent(source.clone());
            Self::insert_bucket_snapshot_locked(&mut draft_state, source)?;
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                postcondition,
            )
        };
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await?;
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        let snapshot_etag = state
            .snapshot_etag
            .clone()
            .ok_or_else(|| TableCatalogStoreError::Internal("durable strong catalog snapshot has no etag".to_string()))?;
        Ok((snapshot_etag, true))
    }

    pub(in crate::table_catalog) async fn remove_bucket_snapshot_if_unchanged(
        &self,
        table_bucket: &str,
        expected_fingerprint: &str,
    ) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let (snapshot, precondition, postcondition) = {
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
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                StrongSnapshotWritePostcondition::TableBucketAbsent(table_bucket.to_string()),
            )
        };
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await
    }

    pub(super) async fn restore_absent_migration_snapshot_baseline(&self) -> TableCatalogStoreResult<()> {
        let _write_guard = self.write_lock.lock().await;
        let _reload_guard = self.reload_lock.lock().await;
        let snapshot_object = self
            .object_backend
            .read_object_limited(RUSTFS_META_BUCKET, &Self::snapshot_object_path(), STRONG_TABLE_CATALOG_SNAPSHOT_MAX_SIZE)
            .await?;
        let snapshot_metadata = self
            .object_backend
            .object_metadata(RUSTFS_META_BUCKET, &Self::snapshot_object_path())
            .await?;
        match (snapshot_object, snapshot_metadata) {
            (None, None) => {}
            (Some(_), Some(_)) => return Ok(()),
            _ => {
                return Err(TableCatalogStoreError::Internal(
                    "durable strong catalog snapshot changed while restoring an absent migration baseline".to_string(),
                ));
            }
        }

        let mut state = self.state.lock().await;
        if state.snapshot_etag.is_some()
            || state.snapshot_version.is_some()
            || !state.table_buckets.is_empty()
            || !state.namespaces.is_empty()
            || !state.namespace_children.is_empty()
            || !state.namespace_objects.is_empty()
            || !state.tables.is_empty()
            || !state.views.is_empty()
            || !state.commits.is_empty()
            || !state.idempotency.is_empty()
            || !state.warehouse_index.is_empty()
            || !state.identifier_collisions.is_empty()
        {
            return Err(TableCatalogStoreError::Invalid(
                "cannot restore an absent migration baseline after durable strong catalog state was observed".to_string(),
            ));
        }
        *state = StrongTableCatalogState {
            hydrated: true,
            ..StrongTableCatalogState::default()
        };
        Ok(())
    }

    pub(super) async fn bucket_snapshot_observation(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<(Option<String>, Option<String>)> {
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        let fingerprint = Self::bucket_snapshot_from_state_locked(&state, table_bucket)
            .as_ref()
            .map(table_catalog_bucket_snapshot_fingerprint)
            .transpose()?;
        Ok((fingerprint, state.snapshot_etag.clone()))
    }

    pub(super) async fn bucket_snapshot_fingerprint(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<String>> {
        self.bucket_snapshot_observation(table_bucket)
            .await
            .map(|(fingerprint, _)| fingerprint)
    }

    pub(super) async fn table_bucket_names(&self) -> TableCatalogStoreResult<BTreeSet<String>> {
        self.hydrate_state().await?;
        Ok(self.state.lock().await.table_buckets.keys().cloned().collect())
    }

    fn require_table_bucket_in_state(state: &StrongTableCatalogState, table_bucket: &str) -> TableCatalogStoreResult<()> {
        if !state
            .table_buckets
            .get(table_bucket)
            .is_some_and(|entry| entry.state == TableCatalogEntryState::Active)
        {
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
        let Some(bucket_index) = state.warehouse_index.get(&candidate.table_bucket) else {
            return Ok(());
        };
        let predecessor = bucket_index
            .range(..=candidate_prefix.clone())
            .rev()
            .find(|(_, existing_key)| *existing_key != candidate_key)
            .map(|(prefix, existing_key)| (prefix.as_str(), existing_key));
        let successor = bucket_index
            .range(candidate_prefix.clone()..)
            .find(|(_, existing_key)| *existing_key != candidate_key)
            .map(|(prefix, existing_key)| (prefix.as_str(), existing_key));
        if predecessor
            .into_iter()
            .chain(successor)
            .any(|(existing_prefix, _)| warehouse_object_prefixes_overlap(existing_prefix, &candidate_prefix))
        {
            return Err(TableCatalogStoreError::Conflict(format!(
                "table warehouse location overlaps an active table: {candidate_prefix}"
            )));
        }
        Ok(())
    }

    fn table_commit_recovery_report_for_entry_locked(
        state: &StrongTableCatalogState,
        entry: &TableEntry,
    ) -> TableCommitRecoveryReport {
        let history =
            TableCommitHistoryIndex::new(entry, Self::table_commits_locked(state, &entry.table_bucket, &entry.table_id));
        let mut commits = Self::table_commits_locked(state, &entry.table_bucket, &entry.table_id)
            .map(|commit_log| {
                let idempotency_commit = commit_log.idempotency_key.as_deref().and_then(|idempotency_key| {
                    state
                        .idempotency
                        .get(&Self::idempotency_key(&entry.table_bucket, &entry.table_id, idempotency_key))
                });
                table_commit_recovery_entry(entry, commit_log, idempotency_commit, history.proves_committed(commit_log))
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
    ) -> TableCatalogStoreResult<TableEntry> {
        Self::ensure_identifier_is_unambiguous_locked(state, key)?;
        let Some(current) = state.tables.get(key).cloned() else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                request.table_bucket, request.namespace, request.table
            )));
        };
        if current.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                request.table_bucket, request.namespace, request.table
            )));
        }
        let commit_key = Self::commit_key(&request.table_bucket, &current.table_id, &request.commit_id);
        let existing_commit = state.commits.get(&commit_key);
        let idempotency_key = request
            .idempotency_key
            .as_deref()
            .map(|idempotency_key| Self::idempotency_key(&request.table_bucket, &current.table_id, idempotency_key));
        let existing_idempotency_commit = idempotency_key.as_ref().and_then(|key| state.idempotency.get(key));

        if let (Some(existing), Some(indexed)) = (existing_commit, existing_idempotency_commit)
            && !commit_logs_share_recovery_payload(existing, indexed)
        {
            return Err(TableCatalogStoreError::Conflict(
                "commit record and idempotency index contain different payloads".to_string(),
            ));
        }
        if let Some(existing) = existing_idempotency_commit
            && !commit_log_matches_request(existing, request, &current.table_id)
        {
            return Err(TableCatalogStoreError::Conflict("idempotency key already exists".to_string()));
        }
        if existing_commit.is_none() && existing_idempotency_commit.is_some() {
            return Err(TableCatalogStoreError::Conflict(
                "idempotency key exists without a recoverable commit record".to_string(),
            ));
        }

        if let Some(existing) = existing_commit {
            if !commit_log_matches_request(existing, request, &current.table_id) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "commit id already exists: {}",
                    request.commit_id
                )));
            }
            if matches!(existing.status, CommitLogStatus::Failed) {
                return Err(TableCatalogStoreError::Conflict("failed commit record cannot be replayed".to_string()));
            }
            if matches!(existing.status, CommitLogStatus::Committed) && table_matches_staged_base(&current, existing) {
                return Err(TableCatalogStoreError::Conflict(
                    "committed record still matches the pre-commit table state".to_string(),
                ));
            }
            let historically_committed = if matches!(existing.status, CommitLogStatus::Staged)
                && !table_matches_staged_base(&current, existing)
                && !table_matches_committed_log(&current, existing)
            {
                TableCommitHistoryIndex::new(
                    &current,
                    Self::table_commits_locked(state, &request.table_bucket, &current.table_id),
                )
                .proves_committed(existing)
            } else {
                false
            };
            if matches!(existing.status, CommitLogStatus::Committed)
                || (matches!(existing.status, CommitLogStatus::Staged)
                    && (table_matches_committed_log(&current, existing) || historically_committed))
            {
                return Ok(current);
            }
            return Err(TableCatalogStoreError::Conflict(
                "existing commit record does not match current table state".to_string(),
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
        if !is_valid_table_metadata_location_for_entry(&current, &request.new_metadata_location) {
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
    ) -> Option<(TableCommitResult, bool)> {
        let commit_key = Self::commit_key(&request.table_bucket, &current.table_id, &request.commit_id);
        let existing = state.commits.get(&commit_key)?.clone();
        if !commit_log_matches_request(&existing, request, &current.table_id) {
            return None;
        }
        let historically_committed = if matches!(existing.status, CommitLogStatus::Staged)
            && !table_matches_staged_base(&current, &existing)
            && !table_matches_committed_log(&current, &existing)
        {
            TableCommitHistoryIndex::new(&current, Self::table_commits_locked(state, &request.table_bucket, &current.table_id))
                .proves_committed(&existing)
        } else {
            false
        };
        if matches!(existing.status, CommitLogStatus::Failed)
            || (matches!(existing.status, CommitLogStatus::Committed) && table_matches_staged_base(&current, &existing))
            || (!matches!(existing.status, CommitLogStatus::Committed)
                && !table_matches_committed_log(&current, &existing)
                && !historically_committed)
        {
            return None;
        }

        let mut committed = existing;
        committed.status = CommitLogStatus::Committed;
        let commit_changed = state.commits.get(&commit_key) != Some(&committed);
        if commit_changed {
            state.commits.insert(commit_key, committed.clone());
        }
        let mut idempotency_changed = false;
        if let Some(idempotency_key) = committed.idempotency_key.as_deref() {
            let idempotency_key = Self::idempotency_key(&request.table_bucket, &current.table_id, idempotency_key);
            idempotency_changed = state.idempotency.get(&idempotency_key) != Some(&committed);
            if idempotency_changed {
                state.idempotency.insert(idempotency_key, committed.clone());
            }
        }
        Some((
            TableCommitResult {
                table: current,
                commit_log: committed,
            },
            commit_changed || idempotency_changed,
        ))
    }

    fn apply_commit_locked(
        state: &mut StrongTableCatalogState,
        request: &TableCommitRequest,
        namespace: &Namespace,
        table: &IdentifierSegment,
        next_metadata_state: TableMetadataCommitState,
    ) -> TableCatalogStoreResult<TableCommitResult> {
        let key = Self::table_key(&request.table_bucket, namespace, table);
        let current = Self::validate_new_table_commit_locked(state, &key, request)?;
        if let Some((result, _)) = Self::committed_existing_result_locked(state, request, current.clone()) {
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
        if let Some(warehouse_location) = next_metadata_state.warehouse_location {
            next.warehouse_location = warehouse_location;
        }
        if let Some(format_version) = next_metadata_state.format_version {
            next.format_version = format_version;
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
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        validate_table_bucket_entry(&entry)?;

        let (snapshot, precondition, postcondition) = {
            let state = self.state.lock().await;
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.table_buckets.insert(entry.table_bucket.clone(), entry.clone());
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                StrongSnapshotWritePostcondition::TableBucketPresent(entry),
            )
        };
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await
    }

    async fn create_namespace(&self, entry: NamespaceEntry) -> TableCatalogStoreResult<()> {
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = validate_namespace_entry_identity(&entry)?;
        validate_namespace_properties(&entry.properties)?;
        let key = Self::namespace_key(&entry.table_bucket, &namespace);
        let (snapshot, precondition, postcondition) = {
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
            draft_state.namespaces.insert(key, entry.clone());
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                StrongSnapshotWritePostcondition::NamespacePresent(entry),
            )
        };
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await
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
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let key = Self::namespace_key(table_bucket, &namespace);
        let (snapshot, precondition, result, postcondition) = {
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
            draft_state.namespaces.insert(key, next.clone());
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                result,
                StrongSnapshotWritePostcondition::NamespacePresent(next),
            )
        };
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await?;
        Ok(result)
    }

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<()> {
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let key = Self::namespace_key(table_bucket, &namespace);
        let (snapshot, precondition, postcondition) = {
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
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                StrongSnapshotWritePostcondition::NamespaceAbsent(key),
            )
        };
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await
    }

    async fn create_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        self.register_table(entry).await
    }

    async fn register_table(&self, entry: TableEntry) -> TableCatalogStoreResult<()> {
        let publication = TableCommitLockPublication::new(&self.object_backend);
        self.register_table_with_publication(entry, &publication).await
    }

    async fn register_table_with_publication(
        &self,
        entry: TableEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        validate_table_entry_version_and_id(&entry)?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        table_warehouse_object_prefix(&entry)?;
        if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "table metadata location must be inside the table metadata directory".to_string(),
            ));
        }
        publication.begin_table_bucket(&entry.table_bucket).await?;
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "table registration requires a table-bucket publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        publication
            .prepare(&entry.table_bucket, &entry.namespace, &entry.table)
            .await?;
        if !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.table) {
            return Err(TableCatalogStoreError::Internal(
                "table registration requires a table publication fence".to_string(),
            ));
        }
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let key = Self::table_key(&entry.table_bucket, &namespace, &table);
        let publication_identity = (entry.table_bucket.clone(), entry.namespace.clone(), entry.table.clone());
        let (snapshot, precondition, postcondition) = {
            let state = self.state.lock().await;
            Self::require_table_bucket_in_state(&state, &entry.table_bucket)?;
            Self::require_active_namespace_locked(&state, &entry.table_bucket, &namespace)?;
            if state.tables.contains_key(&key) || state.views.contains_key(&key) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog object already exists: table {}/{}/{}",
                    entry.table_bucket, entry.namespace, entry.table
                )));
            }
            if state
                .tables
                .values()
                .any(|existing| existing.table_bucket == entry.table_bucket && existing.table_id == entry.table_id)
            {
                return Err(TableCatalogStoreError::Conflict(
                    "table id is already registered in this table bucket".to_string(),
                ));
            }
            Self::ensure_table_warehouse_prefix_available_locked(&state, &entry, &key)?;
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.tables.insert(key, entry.clone());
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                StrongSnapshotWritePostcondition::TablePresent(entry),
            )
        };
        if !publication.holds_table_bucket(&publication_identity.0)
            || !publication.holds_table(&publication_identity.0, &publication_identity.1, &publication_identity.2)
        {
            return Err(TableCatalogStoreError::Internal(
                "table registration publication fence was lost before snapshot update".to_string(),
            ));
        }
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await
    }

    async fn list_tables(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let state = self.state.lock().await;
        let namespace_name = namespace.public_name();
        Self::ensure_namespace_identifiers_are_unambiguous_locked(&state, table_bucket, &namespace_name)?;
        let mut entries = state
            .tables
            .iter()
            .filter(|((bucket, entry_namespace, _), entry)| {
                bucket == table_bucket && entry_namespace == &namespace_name && entry.state == TableCatalogEntryState::Active
            })
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.table.cmp(&right.table));
        Ok(entries)
    }

    async fn list_all_tables(&self, table_bucket: &str) -> TableCatalogStoreResult<Vec<TableEntry>> {
        self.hydrate_state().await?;
        let state = self.state.lock().await;
        Self::ensure_table_bucket_identifiers_are_unambiguous_locked(&state, table_bucket)?;
        Ok(state
            .tables
            .range((table_bucket.to_string(), String::new(), String::new())..)
            .take_while(|((bucket, _, _), _)| bucket == table_bucket)
            .filter(|(_, entry)| entry.state == TableCatalogEntryState::Active)
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
        Self::ensure_namespace_identifiers_are_unambiguous_locked(&state, table_bucket, &namespace)?;
        let entries = state
            .tables
            .range((start, Bound::Unbounded))
            .take_while(|((bucket, entry_namespace, _), _)| bucket == table_bucket && entry_namespace == &namespace)
            .filter(|(_, entry)| entry.state == TableCatalogEntryState::Active)
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
        let key = Self::table_key(table_bucket, &namespace, &table);
        Self::ensure_identifier_is_unambiguous_locked(&state, &key)?;
        Ok(state
            .tables
            .get(&key)
            .filter(|entry| entry.state == TableCatalogEntryState::Active)
            .cloned())
    }

    async fn rename_table(
        &self,
        table_bucket: &str,
        source_namespace: &str,
        source_table: &str,
        destination_namespace: &str,
        destination_table: &str,
    ) -> TableCatalogStoreResult<()> {
        let publication = TableCommitLockPublication::new(&self.object_backend);
        publication.begin_table_bucket(table_bucket).await?;
        if !publication.holds_table_bucket(table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "table rename requires a table-bucket publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(&publication);
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;

        let source_namespace = parse_namespace_for_store(source_namespace)?;
        let source_table = parse_table_for_store(source_table)?;
        let destination_namespace = parse_namespace_for_store(destination_namespace)?;
        let destination_table = parse_table_for_store(destination_table)?;
        let source_key = Self::table_key(table_bucket, &source_namespace, &source_table);
        let destination_key = Self::table_key(table_bucket, &destination_namespace, &destination_table);

        let (snapshot, precondition, postcondition) = {
            let state = self.state.lock().await;
            Self::require_table_bucket_in_state(&state, table_bucket)?;
            Self::ensure_identifier_is_unambiguous_locked(&state, &source_key)?;
            Self::ensure_identifier_is_unambiguous_locked(&state, &destination_key)?;
            let source = state
                .tables
                .get(&source_key)
                .filter(|entry| entry.state == TableCatalogEntryState::Active)
                .cloned()
                .ok_or_else(|| {
                    TableCatalogStoreError::TableNotFound(format!(
                        "{table_bucket}/{}/{}",
                        source_namespace.public_name(),
                        source_table.as_str()
                    ))
                })?;
            if !Self::namespace_exists_locked(&state, table_bucket, &source_namespace) {
                return Err(TableCatalogStoreError::TableNotFound(format!(
                    "{table_bucket}/{}/{}",
                    source_namespace.public_name(),
                    source_table.as_str()
                )));
            }
            if !Self::namespace_exists_locked(&state, table_bucket, &destination_namespace) {
                return Err(TableCatalogStoreError::NamespaceNotFound(format!(
                    "{table_bucket}/{}",
                    destination_namespace.public_name()
                )));
            }
            if state.tables.contains_key(&destination_key) || state.views.contains_key(&destination_key) {
                return Err(TableCatalogStoreError::AlreadyExists(format!(
                    "destination table already exists: {table_bucket}/{}/{}",
                    destination_namespace.public_name(),
                    destination_table.as_str()
                )));
            }
            if !is_valid_table_metadata_location_for_entry(&source, &source.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(
                    "current metadata location must be inside the table metadata directory".to_string(),
                ));
            }

            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.tables.remove(&source_key);
            let mut destination = source;
            destination.namespace = destination_namespace.public_name();
            destination.table = destination_table.as_str().to_string();
            draft_state.tables.insert(destination_key.clone(), destination.clone());
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                StrongSnapshotWritePostcondition::TableRenamed {
                    source_key,
                    destination_key,
                    table_id: destination.table_id,
                },
            )
        };
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await
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
            return Err(TableCatalogStoreError::Internal(format!(
                "durable strong catalog has no entry for table-enabled bucket {table_bucket}"
            )));
        };
        if bucket_entry.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::Internal(format!(
                "table-enabled bucket {table_bucket} has an inactive durable strong catalog entry"
            )));
        }
        if self.snapshot_write_version >= STRONG_TABLE_CATALOG_SNAPSHOT_VERSION
            && state
                .snapshot_version
                .is_none_or(|version| version < STRONG_TABLE_CATALOG_SNAPSHOT_VERSION)
        {
            return Err(TableCatalogStoreError::Internal(
                "durable strong catalog data-plane access requires a version 2 snapshot after fleet confirmation".to_string(),
            ));
        }

        let Some(bucket_index) = state.warehouse_index.get(table_bucket) else {
            return Ok(None);
        };

        for warehouse_object_prefix in warehouse_index_candidate_prefixes(object) {
            if let Some(table_key) = bucket_index.get(warehouse_object_prefix) {
                Self::ensure_identifier_is_unambiguous_locked(&state, table_key)?;
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
        let publication = TableCommitLockPublication::new(&self.object_backend);
        publication.begin_table_bucket(&request.table_bucket).await?;
        self.commit_table_with_publication(request, &publication).await
    }

    async fn commit_table_with_publication(
        &self,
        request: TableCommitRequest,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        let commit_started = Instant::now();
        record_table_commit_attempt(&request.operation);
        let namespace = parse_namespace_for_store(&request.namespace)?;
        let table = parse_table_for_store(&request.table)?;
        publication
            .prepare(&request.table_bucket, &request.namespace, &request.table)
            .await?;
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.table) {
            return Err(TableCatalogStoreError::Internal(
                "table commit requires a table publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        let write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let key = Self::table_key(&request.table_bucket, &namespace, &table);

        let committed_existing_result = {
            let state = self.state.lock().await;
            let current = Self::validate_new_table_commit_locked(&state, &key, &request);
            match current {
                Ok(current) => {
                    let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
                    Self::committed_existing_result_locked(&mut draft_state, &request, current).map(|(result, state_changed)| {
                        if state_changed {
                            Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)
                                .map(|snapshot| (result, Some((snapshot, precondition))))
                        } else {
                            Ok((result, None))
                        }
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
                Ok((result, Some((snapshot, precondition)))) => {
                    let postcondition = Self::commit_write_postcondition(&request.table_bucket, &result.commit_log);
                    if !publication.holds_table(&request.table_bucket, &request.namespace, &request.table) {
                        Err(TableCatalogStoreError::Internal(
                            "table commit publication fence was lost before snapshot update".to_string(),
                        ))
                    } else {
                        self.finalize_snapshot_write(snapshot, precondition, postcondition)
                            .await
                            .map(|_| result)
                    }
                }
                Ok((result, None)) => Ok(result),
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
        drop(write_guard);

        let Some(new_metadata_object) = self
            .object_backend
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
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let current_warehouse_location = {
            let state = self.state.lock().await;
            state
                .tables
                .get(&key)
                .map(|entry| entry.warehouse_location.clone())
                .ok_or_else(|| {
                    TableCatalogStoreError::NotFound(format!(
                        "table {}/{}/{}",
                        request.table_bucket, request.namespace, request.table
                    ))
                })?
        };
        let warehouse_relocation = next_metadata_state
            .warehouse_location
            .as_ref()
            .is_some_and(|warehouse_location| warehouse_location != &current_warehouse_location);
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

        let cas_started = Instant::now();
        let prepared_result = {
            let state = self.state.lock().await;
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            match Self::apply_commit_locked(&mut draft_state, &request, &namespace, &table, next_metadata_state) {
                Ok(result) => Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)
                    .map(|snapshot| (result, snapshot, precondition)),
                Err(err) => Err(err),
            }
        };
        let result = match prepared_result {
            Ok((result, snapshot, precondition)) => {
                let postcondition = Self::commit_write_postcondition(&request.table_bucket, &result.commit_log);
                let snapshot_result = if publication.holds_table(&request.table_bucket, &request.namespace, &request.table)
                    && (!warehouse_relocation || publication.holds_table_bucket(&request.table_bucket))
                {
                    self.finalize_snapshot_write(snapshot, precondition, postcondition).await
                } else {
                    Err(TableCatalogStoreError::Internal(
                        "table commit publication fence was lost before snapshot update".to_string(),
                    ))
                };
                match snapshot_result {
                    Ok(()) => Ok(result),
                    Err(err) => {
                        let replay = {
                            let state = self.state.lock().await;
                            let mut replay_state = state.clone();
                            state
                                .tables
                                .get(&key)
                                .cloned()
                                .and_then(|current| Self::committed_existing_result_locked(&mut replay_state, &request, current))
                                .and_then(|(result, state_changed)| (!state_changed).then_some(result))
                        };
                        replay.ok_or(err)
                    }
                }
            }
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
        let publication = TableCommitLockPublication::new(&self.object_backend);
        publication.begin_table_bucket(table_bucket).await?;
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        publication.prepare(table_bucket, namespace, table).await?;
        if !publication.holds_table_bucket(table_bucket) || !publication.holds_table(table_bucket, namespace, table) {
            return Err(TableCatalogStoreError::Internal(
                "table drop requires table-bucket and table publication fences".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(&publication);
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        let key = Self::table_key(table_bucket, &namespace, &table);
        let (snapshot, precondition, postcondition) = {
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
            let removed = draft_state.tables.remove(&key).ok_or_else(|| {
                TableCatalogStoreError::Internal("table disappeared while preparing the strong catalog snapshot".to_string())
            })?;
            draft_state
                .commits
                .retain(|(entry_bucket, table_id, _), _| entry_bucket != table_bucket || table_id != &removed.table_id);
            draft_state
                .idempotency
                .retain(|(entry_bucket, table_id, _), _| entry_bucket != table_bucket || table_id != &removed.table_id);
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                StrongSnapshotWritePostcondition::TableAbsent {
                    key,
                    table_id: removed.table_id,
                },
            )
        };
        if !publication.holds_table_bucket(table_bucket)
            || !publication.holds_table(table_bucket, &namespace.public_name(), table.as_str())
        {
            return Err(TableCatalogStoreError::Internal(
                "table drop publication fence was lost before snapshot update".to_string(),
            ));
        }
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await
    }

    async fn create_view(&self, entry: ViewEntry) -> TableCatalogStoreResult<()> {
        let publication = TableCommitLockPublication::new(&self.object_backend);
        self.create_view_with_publication(entry, &publication).await
    }

    async fn create_view_with_publication(
        &self,
        entry: ViewEntry,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        validate_view_entry_version_and_id(&entry)?;
        validate_view_warehouse_location(&entry.table_bucket, &entry.warehouse_location)?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let view = parse_table_for_store(&entry.view)?;
        if !is_valid_view_metadata_location(&namespace, &view, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "view metadata location must be inside the view metadata directory".to_string(),
            ));
        }
        publication.begin_table_bucket(&entry.table_bucket).await?;
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "view creation requires a table-bucket publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        publication
            .prepare(&entry.table_bucket, &entry.namespace, &entry.view)
            .await?;
        if !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.view) {
            return Err(TableCatalogStoreError::Internal(
                "view creation requires a table publication fence".to_string(),
            ));
        }
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let key = Self::table_key(&entry.table_bucket, &namespace, &view);
        let publication_identity = (entry.table_bucket.clone(), entry.namespace.clone(), entry.view.clone());
        let (snapshot, precondition, postcondition) = {
            let state = self.state.lock().await;
            Self::require_table_bucket_in_state(&state, &entry.table_bucket)?;
            Self::require_active_namespace_locked(&state, &entry.table_bucket, &namespace)?;
            if state.views.contains_key(&key) || state.tables.contains_key(&key) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog object already exists: view {}/{}/{}",
                    entry.table_bucket, entry.namespace, entry.view
                )));
            }
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.views.insert(key, entry.clone());
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                StrongSnapshotWritePostcondition::ViewPresent(entry),
            )
        };
        if !publication.holds_table_bucket(&publication_identity.0)
            || !publication.holds_table(&publication_identity.0, &publication_identity.1, &publication_identity.2)
        {
            return Err(TableCatalogStoreError::Internal(
                "view creation publication fence was lost before snapshot update".to_string(),
            ));
        }
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await
    }

    async fn list_views(&self, table_bucket: &str, namespace: &str) -> TableCatalogStoreResult<Vec<ViewEntry>> {
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let state = self.state.lock().await;
        let namespace_name = namespace.public_name();
        Self::ensure_namespace_identifiers_are_unambiguous_locked(&state, table_bucket, &namespace_name)?;
        let mut entries = state
            .views
            .iter()
            .filter(|((bucket, entry_namespace, _), entry)| {
                bucket == table_bucket && entry_namespace == &namespace_name && entry.state == TableCatalogEntryState::Active
            })
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
        Self::ensure_namespace_identifiers_are_unambiguous_locked(&state, table_bucket, &namespace)?;
        let entries = state
            .views
            .range((start, Bound::Unbounded))
            .take_while(|((bucket, entry_namespace, _), _)| bucket == table_bucket && entry_namespace == &namespace)
            .filter(|(_, entry)| entry.state == TableCatalogEntryState::Active)
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
        let key = Self::table_key(table_bucket, &namespace, &view);
        Self::ensure_identifier_is_unambiguous_locked(&state, &key)?;
        Ok(state
            .views
            .get(&key)
            .filter(|entry| entry.state == TableCatalogEntryState::Active)
            .cloned())
    }

    async fn replace_view(&self, request: ViewCommitRequest) -> TableCatalogStoreResult<ViewCommitResult> {
        let publication = TableCommitLockPublication::new(&self.object_backend);
        self.replace_view_with_publication(request, true, &publication).await
    }

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
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        let namespace = parse_namespace_for_store(&request.namespace)?;
        let view = parse_table_for_store(&request.view)?;
        publication
            .prepare(&request.table_bucket, &request.namespace, &request.view)
            .await?;
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.view) {
            return Err(TableCatalogStoreError::Internal(
                "view replacement requires a table publication fence".to_string(),
            ));
        }
        let write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let key = Self::table_key(&request.table_bucket, &namespace, &view);
        let expected_view_id = {
            let state = self.state.lock().await;
            Self::ensure_identifier_is_unambiguous_locked(&state, &key)?;
            let Some(current) = state
                .views
                .get(&key)
                .filter(|entry| entry.state == TableCatalogEntryState::Active)
            else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "view {}/{}/{}",
                    request.table_bucket, request.namespace, request.view
                )));
            };
            current.view_id.clone()
        };
        drop(write_guard);
        if !is_valid_view_metadata_location(&namespace, &view, &request.new_metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "new metadata location must be inside the view metadata directory".to_string(),
            ));
        }
        let Some(new_metadata_object) = self
            .object_backend
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

        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let (snapshot, precondition, next, postcondition, warehouse_relocation) = {
            let state = self.state.lock().await;
            Self::ensure_identifier_is_unambiguous_locked(&state, &key)?;
            let Some(current) = state.views.get(&key).cloned() else {
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
            if current.view_id != expected_view_id {
                return Err(TableCatalogStoreError::Conflict(
                    "current view identity changed while metadata was being validated".to_string(),
                ));
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
            let (precondition, mut draft_state) = Self::snapshot_draft_context_locked(&state);
            draft_state.views.insert(key, next.clone());
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                next.clone(),
                StrongSnapshotWritePostcondition::ViewPresent(next),
                warehouse_relocation,
            )
        };
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.view)
            || ((table_bucket_fence_required || warehouse_relocation) && !publication.holds_table_bucket(&request.table_bucket))
        {
            return Err(TableCatalogStoreError::Internal(
                "view replacement publication fence was lost before snapshot update".to_string(),
            ));
        }
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await?;
        Ok(ViewCommitResult { view: next })
    }

    async fn drop_view(&self, table_bucket: &str, namespace: &str, view: &str) -> TableCatalogStoreResult<()> {
        let _migration_guard = self.acquire_snapshot_write_permit().await?;
        let _write_guard = self.write_lock.lock().await;
        self.hydrate_state().await?;
        let namespace = parse_namespace_for_store(namespace)?;
        let view = parse_table_for_store(view)?;
        let key = Self::table_key(table_bucket, &namespace, &view);
        let (snapshot, precondition, postcondition) = {
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
            let removed = draft_state.views.remove(&key).ok_or_else(|| {
                TableCatalogStoreError::Internal("view disappeared while preparing the strong catalog snapshot".to_string())
            })?;
            (
                Self::snapshot_from_mutated_state_locked(&mut draft_state, self.snapshot_write_version)?,
                precondition,
                StrongSnapshotWritePostcondition::ViewAbsent {
                    key,
                    view_id: removed.view_id,
                },
            )
        };
        self.finalize_snapshot_write(snapshot, precondition, postcondition).await
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
