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

use super::object::{
    table_warehouse_index_state_ready, validate_table_entry_object, validate_table_warehouse_index_entry_object,
};
use super::registration_tree::{RegistrationTree, RegistrationTreeRoot, registration_tree_entry_supported};
use super::*;

const REGISTRATION_INDEX_VERSION: u16 = 1;
const REGISTRATION_ROOT_MAX_SIZE: usize = 64 * 1024;
const LOG_COMPONENT_TABLE_CATALOG: &str = "table_catalog";
const LOG_SUBSYSTEM_REGISTRATION_INDEX: &str = "registration_index";
const EVENT_REGISTRATION_INDEX_MAINTENANCE: &str = "table_catalog_registration_index_maintenance";

pub(super) fn log_registration_index_maintenance_failure(bucket: &str, reason: &'static str) {
    tracing::warn!(
        event = EVENT_REGISTRATION_INDEX_MAINTENANCE,
        component = LOG_COMPONENT_TABLE_CATALOG,
        subsystem = LOG_SUBSYSTEM_REGISTRATION_INDEX,
        state = "degraded",
        bucket,
        reason,
        "Registration index maintenance requires retry"
    );
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrationIndexRoot {
    version: u16,
    table_bucket: String,
    epoch: Uuid,
    token: Uuid,
    ready: bool,
    tree: RegistrationTreeRoot,
}

pub(super) struct RegistrationIndexUpdate {
    root: RegistrationIndexRoot,
    dirty_etag: String,
    fence: TableCatalogObjectMutationFence,
    obsolete_nodes: Vec<(String, String)>,
}

fn registration_index_error(message: impl Into<String>) -> TableCatalogStoreError {
    TableCatalogStoreError::Conflict(message.into())
}

fn decode_root(table_bucket: &str, data: &[u8]) -> TableCatalogStoreResult<RegistrationIndexRoot> {
    let root: RegistrationIndexRoot = serde_json::from_slice(data)
        .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid registration index root: {err}")))?;
    if root.version != REGISTRATION_INDEX_VERSION
        || root.table_bucket != table_bucket
        || root.epoch.is_nil()
        || root.token.is_nil()
    {
        return Err(TableCatalogStoreError::Invalid("registration index root identity is invalid".to_string()));
    }
    root.tree.validate()?;
    Ok(root)
}

pub(super) fn validate_registration_index_root(table_bucket: &str, data: &[u8]) -> TableCatalogStoreResult<()> {
    decode_root(table_bucket, data).map(|_| ())
}

fn id_key(owner: &TableWarehouseIndexEntry) -> String {
    format!("id:{}", owner.table_id)
}

fn prefix_key(owner: &TableWarehouseIndexEntry) -> String {
    format!("prefix:{}", owner.warehouse_object_prefix)
}

fn supported(owner: &TableWarehouseIndexEntry) -> bool {
    registration_tree_entry_supported(&id_key(owner), owner) && registration_tree_entry_supported(&prefix_key(owner), owner)
}

fn validate_owner(table_bucket: &str, key: &str, owner: &TableWarehouseIndexEntry) -> TableCatalogStoreResult<()> {
    if owner.table_bucket != table_bucket
        || owner.state != TableCatalogEntryState::Active
        || owner.table_id.is_empty()
        || (key != id_key(owner) && key != prefix_key(owner))
    {
        return Err(TableCatalogStoreError::Invalid(
            "registration index owner does not match its key".to_string(),
        ));
    }
    let paths = TableCatalogObjectPaths::default();
    validate_table_warehouse_index_entry_object(
        &paths,
        &paths.warehouse_index_entry_path(table_bucket, &owner.warehouse_object_prefix),
        owner,
    )?;
    parse_namespace_for_store(&owner.namespace)?;
    parse_table_for_store(&owner.table)?;
    Ok(())
}

async fn check_available<B: TableCatalogObjectBackend>(
    tree: &RegistrationTree<'_, B>,
    root: &RegistrationTreeRoot,
    candidate: &TableWarehouseIndexEntry,
) -> TableCatalogStoreResult<()> {
    if let Some(existing) = tree.get(root, &id_key(candidate)).await? {
        validate_owner(&candidate.table_bucket, &id_key(candidate), &existing)?;
        if existing.namespace != candidate.namespace || existing.table != candidate.table {
            return Err(registration_index_error("table id is already registered in this table bucket"));
        }
    }
    let (before, after) = tree
        .neighbors(root, &prefix_key(candidate), Some(&candidate.table_id))
        .await?;
    for (key, owner) in before.into_iter().chain(after) {
        validate_owner(&candidate.table_bucket, &key, &owner)?;
        if key.starts_with("prefix:")
            && warehouse_object_prefixes_overlap(&owner.warehouse_object_prefix, &candidate.warehouse_object_prefix)
        {
            return Err(registration_index_error(format!(
                "table warehouse location overlaps an active table: {}",
                candidate.warehouse_object_prefix
            )));
        }
    }
    Ok(())
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    fn registration_index_prefix(&self, table_bucket: &str) -> String {
        format!("{}registration-index/", self.paths.table_bucket_root_prefix(table_bucket))
    }

    fn registration_index_root_path(&self, table_bucket: &str) -> String {
        format!("{}root.json", self.registration_index_prefix(table_bucket))
    }

    async fn read_registration_index_root(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Option<(RegistrationIndexRoot, String)>> {
        let path = self.registration_index_root_path(table_bucket);
        let Some(object) = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, REGISTRATION_ROOT_MAX_SIZE)
            .await?
        else {
            return Ok(None);
        };
        let root = decode_root(table_bucket, &object.data)?;
        let etag = object
            .etag
            .ok_or_else(|| registration_index_error("registration index root has no etag"))?;
        Ok(Some((root, etag)))
    }

    async fn write_registration_index_root(
        &self,
        root: &RegistrationIndexRoot,
        precondition: TableCatalogPutPrecondition,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<String> {
        fence.ensure_held()?;
        let path = self.registration_index_root_path(&root.table_bucket);
        let data = serde_json::to_vec(root)
            .map_err(|err| TableCatalogStoreError::Internal(format!("serialize registration index root: {err}")))?;
        self.backend
            .put_object_fenced(self.catalog_bucket(), &path, data, precondition, fence)
            .await?;
        let Some((current, etag)) = self.read_registration_index_root(&root.table_bucket).await? else {
            return Err(registration_index_error("registration index root disappeared after publication"));
        };
        if current.token != root.token || current.ready != root.ready || current.epoch != root.epoch {
            return Err(registration_index_error("registration index root changed during publication"));
        }
        fence.ensure_held()?;
        Ok(etag)
    }

    pub(super) async fn invalidate_registration_index(
        &self,
        table_bucket: &str,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let Some((mut root, etag)) = self.read_registration_index_root(table_bucket).await? else {
            return Ok(());
        };
        if !root.ready {
            return Ok(());
        }
        root.ready = false;
        root.token = Uuid::new_v4();
        self.write_registration_index_root(&root, TableCatalogPutPrecondition::IfMatch(etag), fence)
            .await
            .map(|_| ())
    }

    // Registration identity excludes ordinary metadata-pointer and schema changes.
    // All primary object-backed catalog writes pass through the shared write helpers.
    pub(super) async fn invalidate_registration_index_for_write(
        &self,
        bucket: &str,
        object: &str,
        data: &[u8],
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        if bucket != self.catalog_bucket()
            || !(object.ends_with(TABLE_ENTRY_FILE)
                || object.ends_with(TABLE_BUCKET_ENTRY_FILE)
                || object.split('/').any(|segment| segment == WAREHOUSE_INDEX_ROOT))
        {
            return Ok(());
        }
        let value: serde_json::Value = serde_json::from_slice(data)
            .map_err(|err| TableCatalogStoreError::Invalid(format!("invalid catalog write: {err}")))?;
        let Some(table_bucket) = value.get("table_bucket").and_then(serde_json::Value::as_str) else {
            return Ok(());
        };
        let fields: &[&str] =
            if object.starts_with(&self.paths.namespace_entries_prefix(table_bucket)) && object.ends_with(TABLE_ENTRY_FILE) {
                &[
                    "version",
                    "table_bucket",
                    "namespace",
                    "table",
                    "table_id",
                    "warehouse_location",
                    "state",
                ]
            } else if object.starts_with(&self.paths.warehouse_index_entries_prefix(table_bucket))
                && object != self.paths.warehouse_index_state_path(table_bucket)
            {
                &[
                    "version",
                    "table_bucket",
                    "namespace",
                    "table",
                    "table_id",
                    "warehouse_object_prefix",
                    "state",
                ]
            } else if object == self.paths.table_bucket_entry_path(table_bucket) {
                &["version", "table_bucket", "state", "updated_at", "active_rename_id"]
            } else {
                return Ok(());
            };
        let previous = self.backend.read_object_unlocked(bucket, object).await?;
        if let Some(previous) = previous
            && let Ok(previous) = serde_json::from_slice::<serde_json::Value>(&previous.data)
            && fields.iter().all(|field| previous.get(*field) == value.get(*field))
        {
            return Ok(());
        }
        self.invalidate_registration_index(table_bucket, fence).await
    }

    async fn rebuild_registration_index(
        &self,
        candidate: &TableWarehouseIndexEntry,
        epoch: Uuid,
        locked_table_entry: &str,
        fence: &TableCatalogObjectMutationFence,
        obsolete_nodes: &mut Vec<(String, String)>,
    ) -> TableCatalogStoreResult<Option<RegistrationIndexRoot>> {
        let table_bucket = candidate.table_bucket.as_str();
        let objects = self
            .backend
            .list_objects_for_cleanup(self.catalog_bucket(), &self.paths.table_bucket_root_prefix(table_bucket))
            .await?;
        if objects.iter().any(|object| {
            object.delete_marker
                || object
                    .version_id
                    .as_deref()
                    .is_some_and(|version| !version.is_empty() && version != "null")
        }) {
            return Err(registration_index_error(
                "registration index rebuild requires unversioned catalog records",
            ));
        }
        let prefix = self.registration_index_prefix(table_bucket);
        let namespace_prefix = self.paths.namespace_entries_prefix(table_bucket);
        let mut owners = Vec::new();
        let mut table_ids = BTreeSet::new();
        for object in objects
            .iter()
            .filter(|object| object.key.starts_with(&namespace_prefix) && object.key.ends_with(TABLE_ENTRY_FILE))
        {
            let entry = if object.key == locked_table_entry {
                self.read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &object.key)
                    .await?
            } else {
                self.read_entry::<TableEntry>(self.catalog_bucket(), &object.key).await?
            };
            let Some((entry, _)) = entry else {
                return Err(registration_index_error("table record changed during registration index rebuild"));
            };
            validate_table_entry_object(&self.paths, &object.key, &entry)?;
            if entry.state != TableCatalogEntryState::Active {
                continue;
            }
            let owner = table_warehouse_index_entry(&entry)?;
            if !supported(&owner) {
                return Ok(None);
            }
            if !table_ids.insert(owner.table_id.clone())
                || (owner.table_id == candidate.table_id
                    && (owner.namespace != candidate.namespace || owner.table != candidate.table))
            {
                return Err(registration_index_error("table id is already registered in this table bucket"));
            }
            if owner.table_id != candidate.table_id
                && warehouse_object_prefixes_overlap(&owner.warehouse_object_prefix, &candidate.warehouse_object_prefix)
            {
                return Err(registration_index_error("table warehouse location overlaps an active table"));
            }
            owners.push(owner);
        }
        owners.sort_unstable_by(|left, right| left.warehouse_object_prefix.cmp(&right.warehouse_object_prefix));
        if owners
            .windows(2)
            .any(|pair| warehouse_object_prefixes_overlap(&pair[0].warehouse_object_prefix, &pair[1].warehouse_object_prefix))
        {
            return Err(registration_index_error("active table warehouse locations overlap"));
        }
        let index_prefix = self.paths.warehouse_index_entries_prefix(table_bucket);
        let state_path = self.paths.warehouse_index_state_path(table_bucket);
        for object in objects.iter().filter(|object| object.key.starts_with(&index_prefix)) {
            if object.key == state_path {
                let Some((state, _)) = self
                    .read_entry::<TableWarehouseIndexStateEntry>(self.catalog_bucket(), &object.key)
                    .await?
                else {
                    return Err(registration_index_error("warehouse index state changed during registration rebuild"));
                };
                table_warehouse_index_state_ready(&state, table_bucket)?;
            } else {
                let Some((index, _)) = self
                    .read_entry::<TableWarehouseIndexEntry>(self.catalog_bucket(), &object.key)
                    .await?
                else {
                    return Err(registration_index_error("warehouse index changed during registration rebuild"));
                };
                validate_table_warehouse_index_entry_object(&self.paths, &object.key, &index)?;
            }
        }
        fence.ensure_held()?;
        // Validate the complete source and candidate before allocating nodes so
        // repeated rejected registrations cannot accumulate abandoned trees.
        let tree = RegistrationTree::new(&self.backend, self.catalog_bucket(), &prefix, fence);
        let mut root = RegistrationIndexRoot {
            version: REGISTRATION_INDEX_VERSION,
            table_bucket: table_bucket.to_string(),
            epoch,
            token: Uuid::new_v4(),
            ready: false,
            tree: RegistrationTreeRoot::default(),
        };
        for owner in owners {
            tree.insert(&mut root.tree, id_key(&owner), owner.clone()).await?;
            tree.insert(&mut root.tree, prefix_key(&owner), owner).await?;
        }
        // Rebuilding allocates fresh node IDs. Only objects observed before the
        // rebuild can be reclaimed after the replacement root is published.
        let node_prefix = format!("{prefix}nodes/");
        obsolete_nodes.extend(objects.into_iter().filter_map(|object| {
            let name = object.key.strip_prefix(&node_prefix)?;
            let id = Uuid::parse_str(name.strip_suffix(".json")?).ok()?;
            if id.is_nil() || name != format!("{id}.json") {
                return None;
            }
            object.etag.map(|etag| (object.key, etag))
        }));
        Ok(Some(root))
    }

    pub(super) async fn prepare_registration_index_update(
        &self,
        entry: &TableEntry,
        locked_table_entry: &str,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<Option<RegistrationIndexUpdate>> {
        let Some(epoch) = self.backend.registration_index_epoch()? else {
            return Ok(None);
        };
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(registration_index_error("registration index requires the bucket publication fence"));
        }
        let fence = publication
            .object_mutation_fence()
            .ok_or_else(|| registration_index_error("registration index requires storage fencing"))?;
        let owner = table_warehouse_index_entry(entry)?;
        if !supported(&owner) {
            self.invalidate_registration_index(&entry.table_bucket, &fence).await?;
            return Ok(None);
        }
        let observed = self.read_registration_index_root(&entry.table_bucket).await?;
        let mut obsolete_nodes = Vec::new();
        let mut root = match &observed {
            Some((root, _)) if root.ready && root.epoch == epoch => root.clone(),
            _ => {
                let Some(root) = self
                    .rebuild_registration_index(&owner, epoch, locked_table_entry, &fence, &mut obsolete_nodes)
                    .await?
                else {
                    self.invalidate_registration_index(&entry.table_bucket, &fence).await?;
                    return Ok(None);
                };
                root
            }
        };
        let prefix = self.registration_index_prefix(&entry.table_bucket);
        let tree = RegistrationTree::new(&self.backend, self.catalog_bucket(), &prefix, &fence);
        check_available(&tree, &root.tree, &owner).await?;
        root.ready = false;
        root.token = Uuid::new_v4();
        let precondition = observed.map_or(TableCatalogPutPrecondition::IfAbsent, |(_, etag)| {
            TableCatalogPutPrecondition::IfMatch(etag)
        });
        let dirty_etag = self.write_registration_index_root(&root, precondition, &fence).await?;
        Ok(Some(RegistrationIndexUpdate {
            root,
            dirty_etag,
            fence,
            obsolete_nodes,
        }))
    }

    pub(super) async fn finish_registration_index_update(
        &self,
        mut update: RegistrationIndexUpdate,
        entry: &TableEntry,
    ) -> TableCatalogStoreResult<()> {
        update.fence.ensure_held()?;
        let prefix = self.registration_index_prefix(&entry.table_bucket);
        let tree = RegistrationTree::new(&self.backend, self.catalog_bucket(), &prefix, &update.fence);
        let owner = table_warehouse_index_entry(entry)?;
        tree.insert(&mut update.root.tree, id_key(&owner), owner.clone()).await?;
        tree.insert(&mut update.root.tree, prefix_key(&owner), owner).await?;
        update.root.ready = true;
        update.root.token = Uuid::new_v4();
        self.write_registration_index_root(&update.root, TableCatalogPutPrecondition::IfMatch(update.dirty_etag), &update.fence)
            .await?;
        // The table and replacement root are committed. Cleanup cannot change
        // their result; a later rebuild inventories and retries remaining nodes.
        for (key, etag) in update.obsolete_nodes {
            if self
                .backend
                .delete_object_if_match_fenced(self.catalog_bucket(), &key, &etag, &update.fence)
                .await
                .is_err()
            {
                log_registration_index_maintenance_failure(&entry.table_bucket, "obsolete_node_cleanup_failed");
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::test_support::TestCatalogObjectBackend;
    use crate::table_catalog::tests::{test_bucket_entry, test_namespace_entry, test_table_entry};

    const BUCKET: &str = "analytics";

    async fn fixture() -> (TestCatalogObjectBackend, ObjectTableCatalogStore<TestCatalogObjectBackend>, Namespace) {
        let backend = TestCatalogObjectBackend {
            registration_index_epoch: Some(Uuid::new_v4()),
            reject_reads_while_write_locked: true,
            ..Default::default()
        };
        let store = ObjectTableCatalogStore::new(backend.clone());
        let namespace = Namespace::parse("sales").expect("namespace is valid");
        store
            .put_table_bucket(test_bucket_entry(BUCKET))
            .await
            .expect("create bucket");
        store
            .create_namespace(test_namespace_entry(BUCKET, &namespace))
            .await
            .expect("create namespace");
        (backend, store, namespace)
    }

    fn entry(namespace: &Namespace, name: &str) -> TableEntry {
        let table = IdentifierSegment::parse(name).expect("table name is valid");
        let mut entry = test_table_entry(
            BUCKET,
            namespace,
            &table,
            default_table_metadata_file_path(namespace, &table, "00001.metadata.json"),
        );
        entry.table_id = format!("id-{name}");
        entry.warehouse_location = format!("s3://{BUCKET}/tables/id-{name}");
        entry
    }

    async fn nodes(store: &ObjectTableCatalogStore<TestCatalogObjectBackend>) -> BTreeSet<String> {
        store
            .backend
            .list_objects(store.catalog_bucket(), &format!("{}nodes/", store.registration_index_prefix(BUCKET)))
            .await
            .expect("list registration nodes")
            .into_iter()
            .collect()
    }

    #[tokio::test]
    async fn registration_index_reclaims_nodes_after_repeated_rebuilds() {
        let (_, store, namespace) = fixture().await;
        let mut previous_name = None::<String>;
        let mut previous_nodes = BTreeSet::new();
        for number in 0..5 {
            if let Some(name) = previous_name {
                store.drop_table(BUCKET, "sales", &name).await.expect("drop previous owner");
            }
            let name = format!("table_{number}");
            store
                .create_table(entry(&namespace, &name))
                .await
                .expect("create replacement owner");
            let current_nodes = nodes(&store).await;
            assert_eq!(current_nodes.len(), 1, "one owner needs one leaf after rebuilding");
            assert!(current_nodes.is_disjoint(&previous_nodes), "old node IDs must be reclaimed");
            assert!(
                store
                    .read_registration_index_root(BUCKET)
                    .await
                    .expect("read root")
                    .expect("root exists")
                    .0
                    .ready
            );
            previous_name = Some(name);
            previous_nodes = current_nodes;
        }
    }

    #[tokio::test]
    async fn registration_index_retries_node_cleanup_without_failing_committed_creation() {
        let (backend, store, namespace) = fixture().await;
        store
            .create_table(entry(&namespace, "first"))
            .await
            .expect("create initial owner");
        let original_nodes = nodes(&store).await;
        let original_node = original_nodes.first().expect("initial tree has a node");
        backend.fail_delete_attempt(store.catalog_bucket(), original_node, 1).await;
        store.drop_table(BUCKET, "sales", "first").await.expect("drop initial owner");
        store
            .create_table(entry(&namespace, "second"))
            .await
            .expect("node cleanup failure must preserve successful creation");
        assert!(nodes(&store).await.contains(original_node), "failed deletion remains available for retry");
        assert!(
            store
                .load_table(BUCKET, "sales", "second")
                .await
                .expect("load committed table")
                .is_some()
        );
        store
            .drop_table(BUCKET, "sales", "second")
            .await
            .expect("invalidate for another rebuild");
        store
            .create_table(entry(&namespace, "third"))
            .await
            .expect("rebuild and retry node cleanup");
        let current_nodes = nodes(&store).await;
        assert_eq!(current_nodes.len(), 1);
        assert!(current_nodes.is_disjoint(&original_nodes));
    }

    #[tokio::test]
    async fn registration_index_conflicting_rebuilds_do_not_allocate_nodes() {
        let (backend, store, namespace) = fixture().await;
        let original = entry(&namespace, "original");
        store.create_table(original.clone()).await.expect("create original owner");
        let original_nodes = nodes(&store).await;
        let store = ObjectTableCatalogStore::new(TestCatalogObjectBackend {
            registration_index_epoch: Some(Uuid::new_v4()),
            ..backend
        });
        for duplicate_id in [true, false] {
            let mut candidate = entry(&namespace, "conflict");
            if duplicate_id {
                candidate.table_id = original.table_id.clone();
            } else {
                candidate.warehouse_location = original.warehouse_location.clone();
            }
            for _ in 0..3 {
                assert!(matches!(
                    store.create_table(candidate.clone()).await,
                    Err(TableCatalogStoreError::Conflict(_))
                ));
                assert_eq!(nodes(&store).await, original_nodes, "rejected candidates must not write new nodes");
            }
        }
    }
}
