// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::object::{
    TABLE_REGISTRATION_READ_CONCURRENCY, table_warehouse_index_state_ready, validate_table_entry_object,
    validate_table_warehouse_index_entry_object,
};
use super::*;
use futures::{StreamExt, TryStreamExt, stream};
use std::sync::OnceLock;

const REGISTRATION_CACHE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone)]
struct CachedOwner {
    etag: String,
    owner: Arc<TableWarehouseIndexEntry>,
}

/// Runtime-local object projections. Every lookup requires a fresh committed inventory.
#[derive(Clone)]
pub(crate) struct TableCatalogRegistrationCache {
    entries: Arc<OnceLock<moka::sync::Cache<String, CachedOwner>>>,
    capacity_bytes: u64,
}

impl Default for TableCatalogRegistrationCache {
    fn default() -> Self {
        Self::with_capacity(REGISTRATION_CACHE_BYTES)
    }
}

impl TableCatalogRegistrationCache {
    fn with_capacity(bytes: u64) -> Self {
        Self {
            entries: Arc::new(OnceLock::new()),
            capacity_bytes: bytes,
        }
    }

    fn entries(&self) -> &moka::sync::Cache<String, CachedOwner> {
        self.entries.get_or_init(|| {
            moka::sync::Cache::builder()
                .max_capacity(self.capacity_bytes)
                .weigher(|key: &String, value: &CachedOwner| {
                    let owner = &value.owner;
                    let bytes = [
                        key.len(),
                        value.etag.len(),
                        owner.table_bucket.len(),
                        owner.namespace.len(),
                        owner.table.len(),
                        owner.table_id.len(),
                        owner.warehouse_object_prefix.len(),
                    ]
                    .into_iter()
                    .fold(256_usize, usize::saturating_add);
                    u32::try_from(bytes).unwrap_or(u32::MAX)
                })
                .build()
        })
    }

    fn get(&self, object: &TableCatalogCleanupObject) -> Option<Arc<TableWarehouseIndexEntry>> {
        let etag = object.etag.as_deref().filter(|etag| !etag.is_empty())?;
        self.entries
            .get()?
            .get(&object.key)
            .filter(|cached| cached.etag == etag)
            .map(|cached| cached.owner)
    }

    fn insert(&self, object: String, etag: Option<String>, owner: Arc<TableWarehouseIndexEntry>) {
        if let Some(etag) = etag.filter(|etag| !etag.is_empty()) {
            self.entries().insert(object, CachedOwner { etag, owner });
        }
    }

    #[cfg(test)]
    pub(in crate::table_catalog) fn clear(&self) {
        if let Some(entries) = self.entries.get() {
            entries.invalidate_all();
            entries.run_pending_tasks();
        }
    }
}

fn validate_inventory(objects: &mut [TableCatalogCleanupObject], prefix: &str) -> TableCatalogStoreResult<()> {
    objects.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    if objects.iter().any(|object| {
        !object.key.starts_with(prefix)
            || object.delete_marker
            || object
                .version_id
                .as_deref()
                .is_some_and(|version| !version.is_empty() && version != "null")
    }) || objects.windows(2).any(|pair| pair[0].key == pair[1].key)
    {
        return Err(TableCatalogStoreError::Invalid(
            "registration requires an unversioned, complete catalog inventory".to_string(),
        ));
    }
    Ok(())
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn registration_owner(
        &self,
        object: TableCatalogCleanupObject,
        table_record: bool,
        locked_table_entry: &str,
        cache: Option<&TableCatalogRegistrationCache>,
    ) -> TableCatalogStoreResult<Arc<TableWarehouseIndexEntry>> {
        if let Some(owner) = cache.and_then(|cache| cache.get(&object)) {
            return Ok(owner);
        }
        let (owner, etag) = if table_record {
            let value = if object.key == locked_table_entry {
                self.read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &object.key)
                    .await?
            } else {
                self.read_entry::<TableEntry>(self.catalog_bucket(), &object.key).await?
            };
            let (entry, etag) = value.ok_or_else(|| {
                TableCatalogStoreError::Unavailable("table record changed during registration inventory".to_string())
            })?;
            validate_table_entry_object(&self.paths, &object.key, &entry)?;
            (table_warehouse_index_entry(&entry)?, etag)
        } else {
            let (entry, etag) = self
                .read_entry::<TableWarehouseIndexEntry>(self.catalog_bucket(), &object.key)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::Unavailable("warehouse index changed during registration inventory".to_string())
                })?;
            validate_table_warehouse_index_entry_object(&self.paths, &object.key, &entry)?;
            (entry, etag)
        };
        let owner = Arc::new(owner);
        if let Some(cache) = cache {
            // A concurrent index repair may change its ETag after enumeration.
            // Associate the projection only with the object that GET validated.
            cache.insert(object.key, etag, Arc::clone(&owner));
        }
        Ok(owner)
    }

    pub(super) async fn check_registration_ownership(
        &self,
        entry: &TableEntry,
        locked_table_entry: &str,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(TableCatalogStoreError::Unavailable("registration publication fence was lost".to_string()));
        }
        validate_table_entry_version_and_id(entry)?;
        let candidate = table_warehouse_index_entry(entry)?;
        let namespace_prefix = self.paths.namespace_entries_prefix(&entry.table_bucket);
        let index_prefix = self.paths.warehouse_index_entries_prefix(&entry.table_bucket);
        // The backend must preserve system-bucket committed-read semantics as
        // well as completeness; ordinary S3 LIST is insufficient for cache hits.
        let mut tables = self
            .backend
            .list_objects_for_cleanup(self.catalog_bucket(), &namespace_prefix)
            .await?;
        let mut indexes = self
            .backend
            .list_objects_for_cleanup(self.catalog_bucket(), &index_prefix)
            .await?;
        validate_inventory(&mut tables, &namespace_prefix)?;
        validate_inventory(&mut indexes, &index_prefix)?;
        let cache = self.backend.registration_cache();
        let mut owners = BTreeMap::new();
        let mut table_ids = BTreeSet::new();
        let mut table_reads = stream::iter(tables.into_iter().filter(|object| object.key.ends_with(TABLE_ENTRY_FILE)))
            .map(|object| self.registration_owner(object, true, locked_table_entry, cache.as_ref()))
            .buffered(TABLE_REGISTRATION_READ_CONCURRENCY);
        while let Some(owner) = table_reads.try_next().await? {
            if owner.state != TableCatalogEntryState::Active {
                continue;
            }
            if !table_ids.insert(owner.table_id.clone())
                || (owner.table_id == candidate.table_id
                    && (owner.namespace != candidate.namespace || owner.table != candidate.table))
            {
                return Err(TableCatalogStoreError::Conflict(
                    "table id is already registered in this table bucket".to_string(),
                ));
            }
            if owner.table_id != candidate.table_id
                && warehouse_object_prefixes_overlap(&owner.warehouse_object_prefix, &candidate.warehouse_object_prefix)
            {
                return Err(TableCatalogStoreError::Conflict(
                    "table warehouse location overlaps an active table".to_string(),
                ));
            }
            owners.insert((owner.namespace.clone(), owner.table.clone()), owner);
        }
        let mut prefixes = owners
            .values()
            .map(|owner| owner.warehouse_object_prefix.as_str())
            .collect::<Vec<_>>();
        prefixes.sort_unstable();
        if prefixes
            .windows(2)
            .any(|pair| warehouse_object_prefixes_overlap(pair[0], pair[1]))
        {
            return Err(TableCatalogStoreError::Conflict("active table warehouse locations overlap".to_string()));
        }
        let state_path = self.paths.warehouse_index_state_path(&entry.table_bucket);
        let has_state = indexes.iter().any(|object| object.key == state_path);
        let mut index_reads = stream::iter(indexes.into_iter().filter(|object| object.key != state_path))
            .map(|object| self.registration_owner(object, false, locked_table_entry, cache.as_ref()))
            .buffered(TABLE_REGISTRATION_READ_CONCURRENCY);
        while let Some(index) = index_reads.try_next().await? {
            if index.table_id == candidate.table_id || index.state != TableCatalogEntryState::Active {
                continue;
            }
            if warehouse_object_prefixes_overlap(&index.warehouse_object_prefix, &candidate.warehouse_object_prefix) {
                parse_namespace_for_store(&index.namespace)?;
                parse_table_for_store(&index.table)?;
                if owners
                    .get(&(index.namespace.clone(), index.table.clone()))
                    .is_some_and(|owner| {
                        owner.table_id == index.table_id && owner.warehouse_object_prefix == index.warehouse_object_prefix
                    })
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "table warehouse location overlaps an active table".to_string(),
                    ));
                }
            }
        }
        if has_state {
            let (state, _) = self
                .read_entry::<TableWarehouseIndexStateEntry>(self.catalog_bucket(), &state_path)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::Unavailable("warehouse index state changed during registration inventory".to_string())
                })?;
            table_warehouse_index_state_ready(&state, &entry.table_bucket)?;
        }
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(TableCatalogStoreError::Unavailable("registration publication fence was lost".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::registration_cache_tests::{cache_fixture, registration_entry};
    use super::*;

    #[tokio::test]
    async fn registration_cache_capacity_and_oversized_owners_preserve_checks() {
        let (backend, _, namespace) = cache_fixture().await;
        let cache = TableCatalogRegistrationCache::with_capacity(1024);
        let backend = crate::table_catalog::test_support::TestCatalogObjectBackend {
            registration_cache: cache.clone(),
            ..backend
        };
        let store = ObjectTableCatalogStore::new(backend.clone());
        let mut oversized = registration_entry(&namespace, "oversized");
        oversized.warehouse_location = format!("s3://analytics/{}", "directory".repeat(512));
        store.create_table(oversized.clone()).await.unwrap();
        for number in 0..12 {
            store
                .create_table(registration_entry(&namespace, &format!("table_{number}")))
                .await
                .unwrap();
        }
        cache.entries().run_pending_tasks();
        assert!(cache.entries().weighted_size() <= 1024);
        backend.reset_call_counts().await;
        let mut candidate = registration_entry(&namespace, "conflicting");
        candidate.warehouse_location = format!("{}/child", oversized.warehouse_location);
        assert!(matches!(store.create_table(candidate).await, Err(TableCatalogStoreError::Conflict(_))));
        assert!(
            backend.read_call_count().await > 0,
            "eviction must cause reads instead of accepting an absent owner"
        );
        cache.entries().run_pending_tasks();
        assert!(cache.entries().weighted_size() <= 1024);
    }
}
