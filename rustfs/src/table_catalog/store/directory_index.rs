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

use super::object::validate_table_entry_object;
use super::*;

const DIRECTORY_KIND: &str = "table-entry-directory";
const DIRECTORY_VERSION: u16 = 1;
const DIRECTORY_PAGE_SIZE: usize = 128;
pub(super) const DIRECTORY_STATE_MAX_SIZE: usize = 16 * 1024;
const ENV_DIRECTORY_ENABLE: &str = "RUSTFS_TABLE_CATALOG_DIRECTORY_INDEX";
const ENV_DIRECTORY_FLEET: &str = "RUSTFS_TABLE_CATALOG_DIRECTORY_INDEX_FLEET_CONFIRMED";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum TableDirectoryPhase {
    Building,
    Ready,
}

// This occupies the existing migration fence path. Old binaries reject the
// unknown fields and cannot publish an entry without its directory candidate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TableDirectoryState {
    kind: String,
    version: u16,
    table_bucket: String,
    activation_id: Uuid,
    phase: TableDirectoryPhase,
    cursor: Option<String>,
    prune_cursor: Option<String>,
    #[serde(skip)]
    etag: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TableDirectoryStatus {
    pub(crate) phase: Option<TableDirectoryPhase>,
    pub(crate) scanned_objects: usize,
    pub(crate) removed_candidates: usize,
}

// A directory read does not acquire publication or migration locks. Callers
// retain their existing synchronization; the activation epoch detects rollback
// and reactivation without recursively taking an already-held fair lock.
pub(super) struct TableDirectoryScan {
    pub(super) objects: Vec<String>,
    pub(super) activation_id: Uuid,
}

impl TableCatalogObjectPaths {
    pub(super) fn table_directory_prefix(&self, table_bucket: &str) -> String {
        format!("{}table-entry-directory/v1/", self.table_bucket_root_prefix(table_bucket))
    }

    fn table_directory_candidate(&self, entry: &TableEntry) -> TableCatalogStoreResult<String> {
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        Ok(format!(
            "{}{}/tables/{}/{}",
            self.table_directory_prefix(&entry.table_bucket),
            namespace.storage_id(),
            table.as_str(),
            TABLE_ENTRY_FILE
        ))
    }

    fn table_directory_progress_lock(&self, table_bucket: &str) -> String {
        format!("{}table-entry-directory/progress.lock", self.table_bucket_root_prefix(table_bucket))
    }
}

async fn run_table_directory_worker<F, T>(worker: F) -> TableCatalogStoreResult<T>
where
    F: std::future::Future<Output = TableCatalogStoreResult<T>> + Send + 'static,
    T: Send + 'static,
{
    let credentials = rustfs_keystone::KEYSTONE_CREDENTIALS.try_with(Clone::clone).ok().flatten();
    let worker = rustfs_keystone::KEYSTONE_CREDENTIALS.scope(credentials, worker);
    let worker = rustfs_io_metrics::s3_http_metrics::with_s3_http_operation_context(
        rustfs_io_metrics::s3_http_metrics::current_s3_http_operation_context(),
        worker,
    );
    crate::storage_api::table::spawn_traced_join(worker)
        .await
        .map_err(|_| TableCatalogStoreError::Internal("table directory worker failed".to_string()))?
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(super) async fn read_table_directory_state(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Option<TableDirectoryState>> {
        let path = self.paths.backing_migration_fence_path(table_bucket);
        let Some(object) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, DIRECTORY_STATE_MAX_SIZE)
            .await?
        else {
            return Ok(None);
        };
        self.table_directory_state_from_object(table_bucket, &object)
    }

    /// Decode the object stored on the migration-fence path as directory
    /// state; `None` when it is some other kind, such as a migration fence.
    pub(super) fn table_directory_state_from_object(
        &self,
        table_bucket: &str,
        object: &TableCatalogObject,
    ) -> TableCatalogStoreResult<Option<TableDirectoryState>> {
        let value: serde_json::Value = serde_json::from_slice(&object.data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid catalog migration fence: {err}")))?;
        if value.get("kind").and_then(serde_json::Value::as_str) != Some(DIRECTORY_KIND) {
            return Ok(None);
        }
        let mut state: TableDirectoryState = serde_json::from_value(value)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid table directory state: {err}")))?;
        if state.version != DIRECTORY_VERSION
            || state.activation_id.is_nil()
            || state.table_bucket != table_bucket
            || state
                .cursor
                .as_ref()
                .is_some_and(|cursor| !cursor.starts_with(&self.paths.namespace_entries_prefix(table_bucket)))
            || state
                .prune_cursor
                .as_ref()
                .is_some_and(|cursor| !cursor.starts_with(&self.paths.table_directory_prefix(table_bucket)))
            || (state.phase == TableDirectoryPhase::Ready && state.cursor.is_some())
        {
            return Err(TableCatalogStoreError::Invalid(
                "table directory state identity or version is invalid".to_string(),
            ));
        }
        state.etag =
            Some(object.etag.clone().ok_or_else(|| {
                TableCatalogStoreError::Unavailable("directory state has no conditional-write ETag".to_string())
            })?);
        Ok(Some(state))
    }

    pub(crate) async fn table_directory_status(&self, table_bucket: &str) -> TableCatalogStoreResult<TableDirectoryStatus> {
        self.require_table_bucket(table_bucket).await?;
        Ok(TableDirectoryStatus {
            phase: self.read_table_directory_state(table_bucket).await?.map(|state| state.phase),
            scanned_objects: 0,
            removed_candidates: 0,
        })
    }

    // The locked entry writer calls this before the authoritative put. The
    // original entry remains authoritative in every phase, including rollback.
    pub(in crate::table_catalog) async fn prepare_table_directory_candidate(
        &self,
        bucket: &str,
        object: &str,
        data: &[u8],
    ) -> TableCatalogStoreResult<()> {
        if bucket != self.catalog_bucket() || !object.ends_with(&format!("/{TABLE_ENTRY_FILE}")) {
            return Ok(());
        }
        let entry: TableEntry = serde_json::from_slice(data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid table entry for directory publication: {err}")))?;
        validate_table_entry_object(&self.paths, object, &entry)?;
        // Without an activated directory there is nothing to keep consistent, and
        // activation backfills every existing entry; writing candidates regardless
        // would grow an unreclaimed set on every cluster.
        if self.read_table_directory_state(&entry.table_bucket).await?.is_none() {
            return Ok(());
        }
        let candidate = self.paths.table_directory_candidate(&entry)?;
        let expected = object.as_bytes();
        if let Some(current) = self
            .backend
            .read_object_limited(bucket, &candidate, DIRECTORY_STATE_MAX_SIZE)
            .await?
        {
            if current.data.as_slice() != expected {
                return Err(TableCatalogStoreError::Invalid(
                    "table directory candidate has an invalid target".to_string(),
                ));
            }
            return Ok(());
        }
        match self
            .backend
            .put_object(bucket, &candidate, expected.to_vec(), TableCatalogPutPrecondition::IfAbsent)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                // Concurrent backfill and an entry writer may add the same
                // immutable candidate. Only the exact persisted target proves it.
                if self
                    .backend
                    .read_object_limited(bucket, &candidate, DIRECTORY_STATE_MAX_SIZE)
                    .await?
                    .is_some_and(|current| current.data.as_slice() == expected)
                {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub(crate) async fn advance_table_directory(&self, table_bucket: &str) -> TableCatalogStoreResult<TableDirectoryStatus> {
        self.advance_table_directory_with_confirmation(
            table_bucket,
            rustfs_utils::get_env_bool(ENV_DIRECTORY_ENABLE, false),
            rustfs_utils::get_env_bool(ENV_DIRECTORY_FLEET, false),
        )
        .await
    }

    async fn advance_table_directory_with_confirmation(
        &self,
        table_bucket: &str,
        enabled: bool,
        fleet_confirmed: bool,
    ) -> TableCatalogStoreResult<TableDirectoryStatus> {
        let owner = self.clone();
        let table_bucket = table_bucket.to_string();
        // Once a page starts, keep its migration/progress guards through the
        // durable tail even when the HTTP caller disconnects. In particular an
        // initial IfAbsent must not arrive after rollback has observed absence.
        run_table_directory_worker(async move {
            owner
                .advance_table_directory_owned(&table_bucket, enabled, fleet_confirmed)
                .await
        })
        .await
    }

    async fn advance_table_directory_owned(
        &self,
        table_bucket: &str,
        enabled: bool,
        fleet_confirmed: bool,
    ) -> TableCatalogStoreResult<TableDirectoryStatus> {
        self.require_table_bucket(table_bucket).await?;
        let fence_path = self.paths.backing_migration_fence_path(table_bucket);
        if self.read_table_directory_state(table_bucket).await?.is_none() {
            if !enabled || !fleet_confirmed {
                return Err(TableCatalogStoreError::Conflict(
                    "directory activation requires both directory-index and fleet-confirmed settings after the compatibility rollout".to_string(),
                ));
            }
            // Drain in-flight legacy writers before publishing the durable
            // capability fence. Readers still use the original catalog.
            let guard = self
                .backend
                .acquire_write_lock(self.catalog_bucket(), &self.paths.backing_migration_fence_lock_path(table_bucket))
                .await?;
            if self
                .backend
                .object_metadata(self.catalog_bucket(), &fence_path)
                .await?
                .is_none()
            {
                let state = TableDirectoryState {
                    kind: DIRECTORY_KIND.to_string(),
                    version: DIRECTORY_VERSION,
                    table_bucket: table_bucket.to_string(),
                    activation_id: Uuid::new_v4(),
                    phase: TableDirectoryPhase::Building,
                    cursor: None,
                    prune_cursor: None,
                    etag: None,
                };
                self.write_table_directory_state(
                    &state,
                    TableCatalogPutPrecondition::IfAbsent,
                    TableCatalogObjectMutationFence::from_signals([guard.lock_lost_signal()]),
                )
                .await?;
                return Ok(TableDirectoryStatus {
                    phase: Some(state.phase),
                    scanned_objects: 0,
                    removed_candidates: 0,
                });
            }
            if self.read_table_directory_state(table_bucket).await?.is_none() {
                return Err(TableCatalogStoreError::Conflict(
                    "table bucket has an active backing migration".to_string(),
                ));
            }
        }

        let permit = self.acquire_object_backed_catalog_write_permit(table_bucket).await?;
        let progress = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &self.paths.table_directory_progress_lock(table_bucket))
            .await?;
        let mut state = self
            .read_table_directory_state(table_bucket)
            .await?
            .ok_or_else(|| TableCatalogStoreError::Conflict("directory activation was cancelled".to_string()))?;
        if state.phase == TableDirectoryPhase::Ready {
            return self.prune_table_directory_page(state, permit, progress).await;
        }
        let prefix = self.paths.namespace_entries_prefix(table_bucket);
        let page = self
            .backend
            .list_catalog_objects_page(
                self.catalog_bucket(),
                &prefix,
                state.cursor.as_deref(),
                NonZeroUsize::new(DIRECTORY_PAGE_SIZE)
                    .ok_or_else(|| TableCatalogStoreError::Internal("directory page size must be positive".to_string()))?,
            )
            .await?;
        let page = catalog_page_names(page)?;
        validate_directory_page(&page, &prefix, state.cursor.as_deref(), DIRECTORY_PAGE_SIZE)?;
        for object in &page.objects {
            if object.ends_with(&format!("/{TABLE_ENTRY_FILE}"))
                && let Some((entry, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), object).await?
            {
                validate_table_entry_object(&self.paths, object, &entry)?;
                let data = serde_json::to_vec(&entry).map_err(|err| TableCatalogStoreError::Internal(err.to_string()))?;
                self.prepare_table_directory_candidate(self.catalog_bucket(), object, &data)
                    .await?;
            }
        }
        state.cursor = page.objects.last().cloned();
        if !page.is_truncated {
            state.phase = TableDirectoryPhase::Ready;
            state.cursor = None;
        }
        // A cancelled page can leave candidates, but never advances the cursor
        // before they are durable. Writers behind the cursor publish candidates
        // first, so reaching EOF does not require a stop-the-world rescan.
        self.write_table_directory_state(
            &state,
            directory_state_precondition(&state)?,
            TableCatalogObjectMutationFence::from_signals([permit.lock_lost_signal(), progress.lock_lost_signal()]),
        )
        .await?;
        Ok(TableDirectoryStatus {
            phase: Some(state.phase),
            scanned_objects: page.objects.len(),
            removed_candidates: 0,
        })
    }

    pub(crate) async fn disable_table_directory(&self, table_bucket: &str) -> TableCatalogStoreResult<TableDirectoryStatus> {
        let owner = self.clone();
        let table_bucket = table_bucket.to_string();
        run_table_directory_worker(async move { owner.disable_table_directory_owned(&table_bucket).await }).await
    }

    async fn disable_table_directory_owned(&self, table_bucket: &str) -> TableCatalogStoreResult<TableDirectoryStatus> {
        self.require_table_bucket(table_bucket).await?;
        let guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &self.paths.backing_migration_fence_lock_path(table_bucket))
            .await?;
        if self.read_table_directory_state(table_bucket).await?.is_some() {
            // Entry publication has always retained the legacy objects. Exclude
            // backfill and all writers until the old authority is restored.
            let path = self.paths.backing_migration_fence_path(table_bucket);
            let etag = self
                .backend
                .object_metadata(self.catalog_bucket(), &path)
                .await?
                .and_then(|metadata| metadata.etag)
                .ok_or_else(|| TableCatalogStoreError::Unavailable("directory state has no deletion ETag".to_string()))?;
            let fence = TableCatalogObjectMutationFence::from_signals([guard.lock_lost_signal()]);
            self.backend
                .delete_object_if_match_fenced(self.catalog_bucket(), &path, &etag, &fence)
                .await?;
        } else if self
            .backend
            .object_metadata(self.catalog_bucket(), &self.paths.backing_migration_fence_path(table_bucket))
            .await?
            .is_some()
        {
            return Err(TableCatalogStoreError::Conflict(
                "cannot cancel a backing migration through directory rollback".to_string(),
            ));
        }
        Ok(TableDirectoryStatus {
            phase: None,
            scanned_objects: 0,
            removed_candidates: 0,
        })
    }

    async fn write_table_directory_state(
        &self,
        state: &TableDirectoryState,
        precondition: TableCatalogPutPrecondition,
        fence: TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let data = serde_json::to_vec(state).map_err(|err| TableCatalogStoreError::Internal(err.to_string()))?;
        self.backend
            .put_object_fenced(
                self.catalog_bucket(),
                &self.paths.backing_migration_fence_path(&state.table_bucket),
                data,
                precondition,
                &fence,
            )
            .await
    }

    async fn prune_table_directory_page(
        &self,
        mut state: TableDirectoryState,
        permit: TableCatalogLockGuard,
        progress: TableCatalogLockGuard,
    ) -> TableCatalogStoreResult<TableDirectoryStatus> {
        let prefix = self.paths.table_directory_prefix(&state.table_bucket);
        let page = self
            .backend
            .list_catalog_objects_page(
                self.catalog_bucket(),
                &prefix,
                state.prune_cursor.as_deref(),
                NonZeroUsize::new(DIRECTORY_PAGE_SIZE)
                    .ok_or_else(|| TableCatalogStoreError::Internal("directory page size must be positive".to_string()))?,
            )
            .await?;
        let page = catalog_page_names(page)?;
        validate_directory_page(&page, &prefix, state.prune_cursor.as_deref(), DIRECTORY_PAGE_SIZE)?;
        let mut removed = 0;
        for candidate in &page.objects {
            let (target, candidate_etag) = self.directory_candidate_target(&state.table_bucket, candidate).await?;
            // A creator holds the authoritative entry's write lock before
            // publishing its candidate. Retain this read lock through removal,
            // so pruning never erases a concurrent creator's only candidate.
            let entry_guard = self.backend.acquire_read_lock(self.catalog_bucket(), &target).await?;
            let current = self.read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &target).await?;
            if let Some((entry, _)) = current.as_ref() {
                validate_table_entry_object(&self.paths, &target, entry)?;
            }
            if current
                .as_ref()
                .is_none_or(|(entry, _)| entry.state == TableCatalogEntryState::Deleted)
            {
                let fence = TableCatalogObjectMutationFence::from_signals([
                    permit.lock_lost_signal(),
                    progress.lock_lost_signal(),
                    entry_guard.lock_lost_signal(),
                ]);
                let etag = candidate_etag
                    .ok_or_else(|| TableCatalogStoreError::Unavailable("directory candidate has no deletion ETag".to_string()))?;
                self.backend
                    .delete_object_if_match_fenced(self.catalog_bucket(), candidate, &etag, &fence)
                    .await?;
                removed += 1;
            }
        }
        state.prune_cursor = if page.is_truncated {
            page.objects.last().cloned()
        } else {
            None
        };
        self.write_table_directory_state(
            &state,
            directory_state_precondition(&state)?,
            TableCatalogObjectMutationFence::from_signals([permit.lock_lost_signal(), progress.lock_lost_signal()]),
        )
        .await?;
        Ok(TableDirectoryStatus {
            phase: Some(state.phase),
            scanned_objects: page.objects.len(),
            removed_candidates: removed,
        })
    }

    pub(super) async fn directory_candidate_target(
        &self,
        table_bucket: &str,
        candidate: &str,
    ) -> TableCatalogStoreResult<(String, Option<String>)> {
        let prefix = self.paths.table_directory_prefix(table_bucket);
        let suffix = candidate
            .strip_prefix(&prefix)
            .ok_or_else(|| TableCatalogStoreError::Invalid("table directory candidate is outside its prefix".to_string()))?;
        // Namespace segments may themselves be named `tables`; parse the final
        // table boundary and validate every identifier before trusting this key.
        let invalid = || TableCatalogStoreError::Invalid("unexpected object in table-entry directory".to_string());
        let (namespace, name) = suffix
            .strip_suffix(&format!("/{TABLE_ENTRY_FILE}"))
            .and_then(|path| path.rsplit_once(&format!("/{TABLE_ROOT}/")))
            .ok_or_else(invalid)?;
        let namespace = Namespace::from_segments(namespace.split('/').map(str::to_owned).collect()).map_err(|_| invalid())?;
        let table = IdentifierSegment::parse(name.to_owned()).map_err(|_| invalid())?;
        let target = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let current = self
            .backend
            .read_object_limited(self.catalog_bucket(), candidate, DIRECTORY_STATE_MAX_SIZE)
            .await?
            .ok_or_else(|| {
                TableCatalogStoreError::Unavailable("table directory candidate disappeared during scan".to_string())
            })?;
        if current.data.as_slice() != target.as_bytes() {
            return Err(TableCatalogStoreError::Invalid(
                "table directory target does not match its key".to_string(),
            ));
        }
        Ok((target, current.etag))
    }

    pub(super) async fn table_directory_objects(
        &self,
        table_bucket: &str,
        max_objects: Option<usize>,
    ) -> TableCatalogStoreResult<Option<TableDirectoryScan>> {
        let Some(state) = self.read_table_directory_state(table_bucket).await? else {
            return Ok(None);
        };
        if state.phase != TableDirectoryPhase::Ready {
            return Ok(None);
        }
        let prefix = self.paths.table_directory_prefix(table_bucket);
        let mut cursor = None;
        let mut objects = Vec::new();
        loop {
            let remaining = max_objects.map_or(TABLE_CATALOG_LIST_MAX_KEYS, |limit| limit.saturating_sub(objects.len()));
            // A full authoritative page stays truncated until another read
            // proves EOF. At the inclusive cap, probe one key without admitting
            // it into the inventory; an empty complete page is still valid.
            let limit = NonZeroUsize::new(remaining.min(TABLE_CATALOG_LIST_MAX_KEYS)).unwrap_or(NonZeroUsize::MIN);
            let page = self
                .backend
                .list_catalog_objects_page(self.catalog_bucket(), &prefix, cursor.as_deref(), limit)
                .await?;
            let page = catalog_page_names(page)?;
            validate_directory_page(&page, &prefix, cursor.as_deref(), limit.get())?;
            if remaining == 0 && !page.objects.is_empty() {
                return Err(TableCatalogStoreError::Unavailable(
                    "table-entry directory exceeds the data-plane scan safety limit".to_string(),
                ));
            }
            for candidate in &page.objects {
                let (target, _) = self.directory_candidate_target(table_bucket, candidate).await?;
                objects.push(target);
            }
            if !page.is_truncated {
                self.ensure_table_directory_epoch(table_bucket, state.activation_id).await?;
                return Ok(Some(TableDirectoryScan {
                    objects,
                    activation_id: state.activation_id,
                }));
            }
            cursor = page.objects.last().cloned();
        }
    }

    pub(super) async fn ensure_table_directory_epoch(
        &self,
        table_bucket: &str,
        activation_id: Uuid,
    ) -> TableCatalogStoreResult<()> {
        if self
            .read_table_directory_state(table_bucket)
            .await?
            .is_none_or(|state| state.phase != TableDirectoryPhase::Ready || state.activation_id != activation_id)
        {
            return Err(TableCatalogStoreError::Unavailable(
                "table directory activation changed during catalog inventory; retry the request".to_string(),
            ));
        }
        Ok(())
    }
}

fn catalog_page_names(page: TableCatalogCleanupPage) -> TableCatalogStoreResult<TableCatalogObjectListPage> {
    if page
        .objects
        .iter()
        .any(|object| object.delete_marker || object.version_id.as_deref().is_some_and(|id| !id.is_empty() && id != "null"))
    {
        return Err(TableCatalogStoreError::Invalid(
            "table directory requires an unversioned authoritative catalog page".to_string(),
        ));
    }
    Ok(TableCatalogObjectListPage {
        objects: page.objects.into_iter().map(|object| object.key).collect(),
        is_truncated: page.is_truncated,
    })
}

fn directory_state_precondition(state: &TableDirectoryState) -> TableCatalogStoreResult<TableCatalogPutPrecondition> {
    state
        .etag
        .clone()
        .map(TableCatalogPutPrecondition::IfMatch)
        .ok_or_else(|| TableCatalogStoreError::Unavailable("directory state has no update ETag".to_string()))
}

fn validate_directory_page(
    page: &TableCatalogObjectListPage,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
) -> TableCatalogStoreResult<()> {
    let mut previous = after;
    if page.objects.len() > limit || (page.is_truncated && page.objects.is_empty()) {
        return Err(TableCatalogStoreError::Unavailable(
            "table directory listing returned an invalid page".to_string(),
        ));
    }
    for object in &page.objects {
        if !object.starts_with(prefix) || previous.is_some_and(|previous| object.as_str() <= previous) {
            return Err(TableCatalogStoreError::Invalid(
                "table directory listing did not advance within its prefix".to_string(),
            ));
        }
        previous = Some(object);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::test_support::TestCatalogObjectBackend;
    use crate::table_catalog::tests::{test_bucket_entry, test_namespace_entry, test_table_entry};

    fn entry(bucket: &str, namespace: &Namespace, name: &str) -> TableEntry {
        let mut entry = test_table_entry(
            bucket,
            namespace,
            &IdentifierSegment::parse(name).unwrap(),
            format!("tables/{name}/metadata/v1.json"),
        );
        entry.table_id = name.to_string();
        entry.table_uuid = name.to_string();
        entry.warehouse_location = format!("s3://{bucket}/tables/{name}");
        entry
    }

    async fn fixture() -> (TestCatalogObjectBackend, ObjectTableCatalogStore<TestCatalogObjectBackend>) {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        store.put_table_bucket(test_bucket_entry("warehouse")).await.unwrap();
        (backend, store)
    }

    async fn finish(store: &ObjectTableCatalogStore<TestCatalogObjectBackend>) {
        for _ in 0..100 {
            let status = store
                .advance_table_directory_with_confirmation("warehouse", true, true)
                .await
                .unwrap();
            assert!(status.scanned_objects <= DIRECTORY_PAGE_SIZE);
            if status.phase == Some(TableDirectoryPhase::Ready) {
                return;
            }
        }
        panic!("bounded backfill did not finish");
    }

    #[tokio::test]
    async fn directory_index_keeps_legacy_authority_until_bounded_backfill_reaches_eof() {
        let (backend, store) = fixture().await;
        let namespace = Namespace::parse("sales").unwrap();
        let legacy = entry("warehouse", &namespace, "orders");
        let path = store
            .paths
            .table_entry_path("warehouse", &namespace, &IdentifierSegment::parse("orders").unwrap());
        // Simulate an old writer that never creates a directory candidate.
        backend
            .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&legacy).unwrap())
            .await;
        assert_eq!(
            store
                .resolve_table_data_plane_resource("warehouse", "tables/orders/data/a")
                .await
                .unwrap()
                .unwrap()
                .table,
            "orders"
        );
        for (enabled, confirmed) in [(false, false), (true, false), (false, true)] {
            assert!(
                store
                    .advance_table_directory_with_confirmation("warehouse", enabled, confirmed)
                    .await
                    .is_err()
            );
            assert!(store.read_table_directory_state("warehouse").await.unwrap().is_none());
        }
        let started = store
            .advance_table_directory_with_confirmation("warehouse", true, true)
            .await
            .unwrap();
        assert_eq!(started.phase, Some(TableDirectoryPhase::Building));
        assert!(
            store
                .table_directory_objects("warehouse", Some(4096))
                .await
                .unwrap()
                .is_none()
        );
        let fence = backend
            .read_object(RUSTFS_META_BUCKET, &store.paths.backing_migration_fence_path("warehouse"))
            .await
            .unwrap()
            .unwrap();
        assert!(
            serde_json::from_slice::<super::super::strong::TableCatalogBackingMigrationFence>(&fence.data).is_err(),
            "legacy writer parser must reject the directory capability fence"
        );
        finish(&store).await;
        assert!(
            backend
                .object_exists(RUSTFS_META_BUCKET, &store.paths.table_directory_candidate(&legacy).unwrap())
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .resolve_table_data_plane_resource("warehouse", "tables/orders/data/a")
                .await
                .unwrap()
                .unwrap()
                .table,
            "orders"
        );
        // Restart with local activation settings off still honors persisted mode.
        let restarted = ObjectTableCatalogStore::new(backend.clone());
        assert_eq!(
            restarted
                .advance_table_directory_with_confirmation("warehouse", false, false)
                .await
                .unwrap()
                .phase,
            Some(TableDirectoryPhase::Ready)
        );
        restarted.disable_table_directory("warehouse").await.unwrap();
        restarted.disable_table_directory("warehouse").await.unwrap();
        assert!(backend.object_exists(RUSTFS_META_BUCKET, &path).await.unwrap());
        assert!(
            restarted
                .table_directory_objects("warehouse", Some(4096))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            restarted
                .resolve_table_data_plane_resource("warehouse", "tables/orders/data/a")
                .await
                .unwrap()
                .unwrap()
                .table,
            "orders"
        );
    }

    #[tokio::test]
    async fn directory_index_excludes_reports_but_preserves_colliding_nested_namespaces() {
        let (backend, store) = fixture().await;
        let parent = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        for number in 0..4100 {
            let report = store
                .paths
                .table_maintenance_job_path("warehouse", &parent, &table, "orders", &format!("job-{number}"));
            backend.seed_object(RUSTFS_META_BUCKET, &report, b"{}".to_vec()).await;
        }
        let nested = Namespace::parse(&format!(
            "sales.tables.orders.maintenance.{}.jobs.{}",
            table_catalog_path_hash("orders"),
            table_catalog_path_hash("job-0")
        ))
        .unwrap();
        let protected = entry("warehouse", &nested, "nested");
        let object = store
            .paths
            .table_entry_path("warehouse", &nested, &IdentifierSegment::parse("nested").unwrap());
        backend
            .seed_object(RUSTFS_META_BUCKET, &object, serde_json::to_vec(&protected).unwrap())
            .await;
        finish(&store).await;
        backend.reset_call_counts().await;
        let resource = store
            .resolve_table_data_plane_resource("warehouse", "tables/nested/data/file")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resource.namespace, nested.public_name());
        assert!(
            store
                .resolve_table_data_plane_resource("warehouse", "ordinary/file")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            backend.list_call_count().await <= 3,
            "report count must not add foreground directory pages"
        );
        assert!(
            backend.read_call_count().await < 100,
            "report bodies must not be read by index-miss resolution"
        );
        let candidates = backend
            .list_objects(RUSTFS_META_BUCKET, &store.paths.table_directory_prefix("warehouse"))
            .await
            .unwrap();
        assert_eq!(candidates, vec![store.paths.table_directory_candidate(&protected).unwrap()]);
        // Ready registration must not return to the mixed namespace inventory,
        // even though its unlimited ownership check still validates every table.
        backend
            .state
            .lock()
            .await
            .failed_inventory_prefixes
            .insert(store.paths.namespace_entries_prefix("warehouse"));
        let fresh = Namespace::parse("fresh").unwrap();
        store
            .create_namespace(test_namespace_entry("warehouse", &fresh))
            .await
            .unwrap();
        store.create_table(entry("warehouse", &fresh, "independent")).await.unwrap();
        let mut overlapping = entry("warehouse", &fresh, "collision");
        overlapping.warehouse_location = "s3://warehouse/tables/nested/child".to_string();
        assert!(
            matches!(store.create_table(overlapping).await, Err(TableCatalogStoreError::Conflict(_))),
            "directory inventory must retain real ownership conflicts"
        );
    }

    #[tokio::test]
    async fn directory_index_cancelled_page_retries_and_dual_write_precedes_entry_publication() {
        let (backend, store) = fixture().await;
        let namespace = Namespace::parse("sales").unwrap();
        store
            .create_namespace(test_namespace_entry("warehouse", &namespace))
            .await
            .unwrap();
        store
            .advance_table_directory_with_confirmation("warehouse", true, true)
            .await
            .unwrap();
        let state_path = store.paths.backing_migration_fence_path("warehouse");
        let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &state_path).await;
        let owner = store.clone();
        let task = tokio::spawn(async move { owner.advance_table_directory_with_confirmation("warehouse", true, true).await });
        tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
            .await
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            store.read_table_directory_state("warehouse").await.unwrap().unwrap().phase,
            TableDirectoryPhase::Building
        );
        pause.release();
        let table = entry("warehouse", &namespace, "new");
        let candidate = store.paths.table_directory_candidate(&table).unwrap();
        let path = store
            .paths
            .table_entry_path("warehouse", &namespace, &IdentifierSegment::parse("new").unwrap());
        backend.fail_next_put(RUSTFS_META_BUCKET, &candidate).await;
        assert!(store.create_table(table.clone()).await.is_err());
        assert!(
            !backend.object_exists(RUSTFS_META_BUCKET, &path).await.unwrap(),
            "candidate write failure must prevent authoritative publication"
        );
        store.create_table(table.clone()).await.unwrap();
        finish(&store).await;
        assert_eq!(
            store
                .resolve_table_data_plane_resource("warehouse", "tables/new/data/a")
                .await
                .unwrap()
                .unwrap()
                .table,
            "new"
        );
    }
    #[tokio::test]
    async fn directory_index_pruning_waits_for_create_and_retains_rename_and_recreation() {
        let (backend, store) = fixture().await;
        let namespace = Namespace::parse("sales").unwrap();
        store
            .create_namespace(test_namespace_entry("warehouse", &namespace))
            .await
            .unwrap();
        finish(&store).await;
        let table = entry("warehouse", &namespace, "orders");
        let path = store
            .paths
            .table_entry_path("warehouse", &namespace, &IdentifierSegment::parse("orders").unwrap());
        let candidate = store.paths.table_directory_candidate(&table).unwrap();
        let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &path).await;
        let owner = store.clone();
        let create = tokio::spawn(async move { owner.create_table(table).await });
        tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
            .await
            .unwrap();
        assert!(backend.object_exists(RUSTFS_META_BUCKET, &candidate).await.unwrap());
        assert!(!backend.object_exists(RUSTFS_META_BUCKET, &path).await.unwrap());
        let reads = backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &path).await;
        let owner = store.clone();
        let prune = tokio::spawn(async move {
            owner
                .advance_table_directory_with_confirmation("warehouse", false, false)
                .await
        });
        tokio::time::timeout(StdDuration::from_secs(5), async {
            while backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &path).await == reads {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!prune.is_finished(), "GC must wait on the creator's authoritative entry lock");
        assert!(backend.object_exists(RUSTFS_META_BUCKET, &candidate).await.unwrap());
        pause.release();
        create.await.unwrap().unwrap();
        assert_eq!(prune.await.unwrap().unwrap().removed_candidates, 0);
        store
            .rename_table("warehouse", "sales", "orders", "sales", "renamed")
            .await
            .unwrap();
        let candidates = backend
            .list_objects(RUSTFS_META_BUCKET, &store.paths.table_directory_prefix("warehouse"))
            .await
            .unwrap();
        assert_eq!(candidates.len(), 2, "rename must publish destination before retiring source");
        assert_eq!(
            store
                .resolve_table_data_plane_resource("warehouse", "tables/orders/data/a")
                .await
                .unwrap()
                .unwrap()
                .table,
            "renamed"
        );
        assert_eq!(
            store
                .advance_table_directory_with_confirmation("warehouse", false, false)
                .await
                .unwrap()
                .removed_candidates,
            1
        );
        store.drop_table("warehouse", "sales", "renamed").await.unwrap();
        store
            .advance_table_directory_with_confirmation("warehouse", false, false)
            .await
            .unwrap();
        let replacement = entry("warehouse", &namespace, "renamed");
        store.create_table(replacement.clone()).await.unwrap();
        assert!(
            backend
                .object_exists(RUSTFS_META_BUCKET, &store.paths.table_directory_candidate(&replacement).unwrap())
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .resolve_table_data_plane_resource("warehouse", "tables/renamed/data/a")
                .await
                .unwrap()
                .unwrap()
                .table,
            "renamed"
        );
    }
    #[tokio::test]
    async fn directory_index_cancelled_initial_write_retains_guard_until_rollback_can_finish() {
        let (backend, store) = fixture().await;
        let fence_path = store.paths.backing_migration_fence_path("warehouse");
        let lock_path = store.paths.backing_migration_fence_lock_path("warehouse");
        let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &fence_path).await;
        let owner = store.clone();
        let request = tokio::spawn(async move { owner.advance_table_directory_with_confirmation("warehouse", true, true).await });
        tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
            .await
            .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        let locks = backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await;
        let owner = store.clone();
        let rollback = tokio::spawn(async move { owner.disable_table_directory("warehouse").await });
        tokio::time::timeout(StdDuration::from_secs(5), async {
            while backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await == locks {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !rollback.is_finished(),
            "rollback must not observe Absent while an owned initial put can still arrive"
        );
        pause.release();
        rollback.await.unwrap().unwrap();
        assert!(!backend.object_exists(RUSTFS_META_BUCKET, &fence_path).await.unwrap());
        assert!(store.read_table_directory_state("warehouse").await.unwrap().is_none());
    }
    #[tokio::test]
    async fn directory_index_lookup_reuses_existing_migration_and_publication_protection() {
        let (backend, store) = fixture().await;
        finish(&store).await;
        let publication_path = default_table_bucket_publication_lock_path();
        let publication_write = backend.acquire_write_lock("warehouse", &publication_path).await.unwrap();
        assert!(
            tokio::time::timeout(StdDuration::from_secs(5), store.resolve_table_data_plane_resource("warehouse", "outside"))
                .await
                .unwrap()
                .unwrap()
                .is_none(),
            "an enclosing catalog publication must not self-deadlock"
        );
        drop(publication_write);
        let publication_read = backend.acquire_read_lock("warehouse", &publication_path).await.unwrap();
        let writes = backend.write_lock_acquisition_count("warehouse", &publication_path).await;
        let writer_backend = backend.clone();
        let writer_path = publication_path.clone();
        let writer = tokio::spawn(async move {
            drop(writer_backend.acquire_write_lock("warehouse", &writer_path).await.unwrap());
        });
        tokio::time::timeout(StdDuration::from_secs(5), async {
            while backend.write_lock_acquisition_count("warehouse", &publication_path).await == writes {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(StdDuration::from_secs(5), store.resolve_table_data_plane_resource("warehouse", "outside"))
                .await
                .unwrap()
                .unwrap()
                .is_none(),
            "a queued writer must not turn an already-protected lookup into a recursive read wait"
        );
        drop(publication_read);
        writer.await.unwrap();

        let state_path = store.paths.warehouse_index_state_path("warehouse");
        backend.delete_object(RUSTFS_META_BUCKET, &state_path).await.unwrap();
        let pause = crate::table_catalog::test_support::TestCatalogObjectPause::default();
        {
            let mut state = backend.state.lock().await;
            let key = (RUSTFS_META_BUCKET.to_string(), state_path);
            // First lookup checks readiness; second read belongs to backfill,
            // after it has taken its migration permit and warehouse-state lock.
            let attempt = state.read_attempts.get(&key).copied().unwrap_or_default() + 2;
            state
                .pause_before_read_attempts
                .entry(key)
                .or_default()
                .insert(attempt, pause.clone());
        }
        let owner = store.clone();
        let mut lookup = tokio::spawn(async move { owner.resolve_table_data_plane_resource("warehouse", "outside").await });
        tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
            .await
            .unwrap();
        let lock_path = store.paths.backing_migration_fence_lock_path("warehouse");
        let writes = backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await;
        let writer_backend = backend.clone();
        let writer_path = lock_path.clone();
        let writer = tokio::spawn(async move {
            drop(
                writer_backend
                    .acquire_write_lock(RUSTFS_META_BUCKET, &writer_path)
                    .await
                    .unwrap(),
            );
        });
        tokio::time::timeout(StdDuration::from_secs(5), async {
            while backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await == writes {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        pause.release();
        let result = tokio::time::timeout(StdDuration::from_secs(5), &mut lookup).await;
        if result.is_err() {
            lookup.abort();
        }
        assert!(
            result.unwrap().unwrap().unwrap().is_none(),
            "backfill must reuse its existing migration permit"
        );
        writer.await.unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn directory_index_owned_advance_and_rollback_preserve_request_context() {
        use crate::table_catalog::test_support::TestCatalogWorkerContext;
        use rustfs_io_metrics::s3_http_metrics::{
            S3HttpRequestGuard, current_s3_http_operation_context, observe_s3_http_operation, with_s3_http_operation_context,
        };
        use tracing::Instrument;

        let _subscriber = tracing::subscriber::set_default(tracing_subscriber::registry());
        let (backend, store) = fixture().await;
        backend.state.lock().await.observed_worker_contexts = Some(Vec::new());
        let mut request = S3HttpRequestGuard::new("POST");
        let operation = request.in_scope(|| {
            observe_s3_http_operation(rustfs_s3_ops::S3Operation::PutObject);
            current_s3_http_operation_context().unwrap()
        });
        let credentials = rustfs_credentials::Credentials {
            access_key: "directory-request-user".to_string(),
            ..Default::default()
        };
        rustfs_keystone::KEYSTONE_CREDENTIALS
            .scope(
                Some(credentials),
                with_s3_http_operation_context(Some(operation), async {
                    for step in 0..3 {
                        backend.state.lock().await.observed_worker_contexts.as_mut().unwrap().clear();
                        let status = if step == 2 {
                            store.disable_table_directory("warehouse").await.unwrap()
                        } else {
                            store
                                .advance_table_directory_with_confirmation("warehouse", true, true)
                                .await
                                .unwrap()
                        };
                        assert_eq!(
                            status.phase,
                            match step {
                                0 => Some(TableDirectoryPhase::Building),
                                1 => Some(TableDirectoryPhase::Ready),
                                _ => None,
                            }
                        );
                        let state = backend.state.lock().await;
                        let observations = state.observed_worker_contexts.as_ref().unwrap();
                        assert!(!observations.is_empty(), "each owned operation must reach actual backend reads");
                        let expected = TestCatalogWorkerContext {
                            access_key: Some("directory-request-user".to_string()),
                            operation: Some("s3:PutObject"),
                            span_name: Some("table_directory_request"),
                        };
                        assert!(observations.iter().all(|observation| observation == &expected), "{observations:?}");
                    }
                })
                .instrument(tracing::info_span!("table_directory_request")),
            )
            .await;
        backend.state.lock().await.observed_worker_contexts.as_mut().unwrap().clear();
        store
            .advance_table_directory_with_confirmation("warehouse", true, true)
            .await
            .unwrap();
        let state = backend.state.lock().await;
        let observations = state.observed_worker_contexts.as_ref().unwrap();
        assert!(!observations.is_empty());
        let expected = TestCatalogWorkerContext {
            access_key: None,
            operation: None,
            span_name: None,
        };
        assert!(observations.iter().all(|observation| observation == &expected), "{observations:?}");
    }

    #[tokio::test]
    async fn directory_index_accepts_inclusive_cap_only_after_authoritative_eof_probe() {
        let (backend, store) = fixture().await;
        let namespace = Namespace::parse("sales").unwrap();
        store
            .create_namespace(test_namespace_entry("warehouse", &namespace))
            .await
            .unwrap();
        for name in ["one", "two", "three", "four"] {
            store.create_table(entry("warehouse", &namespace, name)).await.unwrap();
        }
        finish(&store).await;
        let prefix = store.paths.table_directory_prefix("warehouse");
        let full = backend
            .list_catalog_objects_page(RUSTFS_META_BUCKET, &prefix, None, NonZeroUsize::new(4).unwrap())
            .await
            .unwrap();
        assert_eq!(full.objects.len(), 4);
        assert!(full.is_truncated, "the fixture must match production's full-page EOF semantics");
        let last = full.objects.last().unwrap().key.clone();
        let eof = backend
            .list_catalog_objects_page(RUSTFS_META_BUCKET, &prefix, Some(&last), NonZeroUsize::MIN)
            .await
            .unwrap();
        assert!(eof.objects.is_empty());
        assert!(!eof.is_truncated);
        let scan = store.table_directory_objects("warehouse", Some(4)).await.unwrap().unwrap();
        assert_eq!(scan.objects.len(), 4, "the configured candidate cap is inclusive");
        store.create_table(entry("warehouse", &namespace, "five")).await.unwrap();
        assert!(
            matches!(
                store.table_directory_objects("warehouse", Some(4)).await,
                Err(TableCatalogStoreError::Unavailable(_))
            ),
            "the EOF probe must reject an additional candidate, not expand the inventory cap"
        );
    }

    #[tokio::test]
    async fn directory_index_rejects_rollback_reactivation_during_inventory() {
        let (backend, store) = fixture().await;
        let namespace = Namespace::parse("sales").unwrap();
        store
            .create_namespace(test_namespace_entry("warehouse", &namespace))
            .await
            .unwrap();
        let table = entry("warehouse", &namespace, "orders");
        store.create_table(table.clone()).await.unwrap();
        finish(&store).await;
        // A cold warehouse-index backfill owns the migration read lock. The
        // race under test is an ordinary lookup observing the directory epoch.
        store.backfill_table_warehouse_index("warehouse").await.unwrap();
        assert!(store.warehouse_index_ready("warehouse").await.unwrap());
        let old_epoch = store
            .read_table_directory_state("warehouse")
            .await
            .unwrap()
            .unwrap()
            .activation_id;
        let candidate = store.paths.table_directory_candidate(&table).unwrap();
        let pause = backend.pause_next_read(RUSTFS_META_BUCKET, &candidate).await;
        let owner = store.clone();
        let lookup = tokio::spawn(async move { owner.resolve_table_data_plane_resource("warehouse", "ordinary/file").await });
        tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
            .await
            .unwrap();
        tokio::time::timeout(StdDuration::from_secs(5), store.disable_table_directory("warehouse"))
            .await
            .expect("an ordinary lookup must not prevent rollback")
            .unwrap();
        tokio::time::timeout(StdDuration::from_secs(5), finish(&store))
            .await
            .expect("reactivation must reach Ready while the old lookup is paused");
        let new_epoch = store
            .read_table_directory_state("warehouse")
            .await
            .unwrap()
            .unwrap()
            .activation_id;
        assert_ne!(old_epoch, new_epoch);
        pause.release();
        assert!(
            matches!(
                tokio::time::timeout(StdDuration::from_secs(5), lookup)
                    .await
                    .unwrap()
                    .unwrap(),
                Err(TableCatalogStoreError::Unavailable(_))
            ),
            "a stale Ready observation cannot survive rollback and reactivation"
        );
        assert!(
            store
                .resolve_table_data_plane_resource("warehouse", "tables/orders/file")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn directory_index_real_backend_requires_authoritative_eof_before_ready() {
        use crate::storage_api::table::test_support::{BucketOperations as _, MakeBucketOptions};
        let (_temp, paths, storage) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        storage.make_bucket("warehouse", &MakeBucketOptions::default()).await.unwrap();
        let backend = EcStoreTableCatalogObjectBackend::new_with_strong_runtime(storage, StrongTableCatalogRuntime::default());
        let catalog = ObjectTableCatalogStore::new(backend.clone());
        catalog.put_table_bucket(test_bucket_entry("warehouse")).await.unwrap();
        let prefix = catalog.paths.namespace_entries_prefix("warehouse");
        for index in 0..130 {
            backend
                .put_object(
                    RUSTFS_META_BUCKET,
                    &format!("{prefix}sales/tables/orders/maintenance/jobs/{index:04}.json"),
                    b"{}".to_vec(),
                    TableCatalogPutPrecondition::IfAbsent,
                )
                .await
                .unwrap();
        }
        catalog
            .advance_table_directory_with_confirmation("warehouse", true, true)
            .await
            .unwrap();
        let first = catalog
            .advance_table_directory_with_confirmation("warehouse", false, false)
            .await
            .unwrap();
        assert_eq!(first.scanned_objects, DIRECTORY_PAGE_SIZE);
        assert_eq!(
            first.phase,
            Some(TableDirectoryPhase::Building),
            "a successful bounded cut does not establish EOF"
        );
        let last = catalog
            .advance_table_directory_with_confirmation("warehouse", false, false)
            .await
            .unwrap();
        assert_eq!(last.scanned_objects, 2);
        assert_eq!(last.phase, Some(TableDirectoryPhase::Ready));
        catalog.disable_table_directory("warehouse").await.unwrap();
        let bad_key = format!("{prefix}000bad.json");
        backend
            .put_object(RUSTFS_META_BUCKET, &bad_key, b"{}".to_vec(), TableCatalogPutPrecondition::IfAbsent)
            .await
            .unwrap();
        // Poison metadata on both pools so another healthy producer cannot
        // turn an unresolved early key into a successful partial inventory.
        for disk in paths.iter().flatten() {
            let object = disk.join(RUSTFS_META_BUCKET).join(&bad_key);
            tokio::fs::create_dir_all(&object).await.unwrap();
            tokio::fs::write(object.join("xl.meta"), b"invalid xl.meta").await.unwrap();
        }
        catalog
            .advance_table_directory_with_confirmation("warehouse", true, true)
            .await
            .unwrap();
        assert!(
            catalog
                .advance_table_directory_with_confirmation("warehouse", false, false)
                .await
                .is_err()
        );
        assert_eq!(
            catalog.read_table_directory_state("warehouse").await.unwrap().unwrap().phase,
            TableDirectoryPhase::Building
        );
    }
}
