// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Owned Table fixtures read through the same store as the Vector worker.

use super::*;
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::storage_api::table::contract::object::ObjectIO as _;
use crate::storage_api::table::managed::ManagedBucketStorage as _;
use crate::storage_api::table::test_support::BucketOperations as _;
use crate::storage_api::table::{StorageObjectOptions, StoragePutObjReader};
use crate::table_catalog::test_support::table_metadata_json;

const NAMESPACE: &str = "d09";
const TABLE: &str = "sentinel";
const MAX_SENTINEL_OBJECT_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Identity {
    fixture_id: Uuid,
    bucket_incarnation: Uuid,
    account_namespace_id: String,
    region: String,
    public_name: String,
    storage_bucket_id: String,
}

impl Identity {
    fn from_handle(fixture_id: Uuid, bucket: &BucketHandle) -> WorkerResult<Self> {
        if bucket.kind() != BucketKind::Table {
            return Err(invalid("sentinel requires the actual Table bucket kind"));
        }
        let identity = Self {
            fixture_id,
            bucket_incarnation: bucket.incarnation().as_uuid(),
            account_namespace_id: bucket.account_namespace_id().as_str().into(),
            region: bucket.region().into(),
            public_name: bucket.public_name().as_str().into(),
            storage_bucket_id: bucket.storage_bucket_id().as_str().into(),
        };
        identity.validate()?;
        Ok(identity)
    }

    fn validate(&self) -> WorkerResult<()> {
        let expected = format!("d09-table-{}", self.fixture_id.simple());
        if self.fixture_id.is_nil()
            || self.bucket_incarnation.is_nil()
            || !self.account_namespace_id.is_empty()
            || self.public_name != expected
            || self.storage_bucket_id != expected
            || self.region.is_empty()
            || self.region.len() > 255
        {
            return Err(invalid("sentinel identity is outside its owned Table fixture"));
        }
        Ok(())
    }

    fn table_id(&self) -> String {
        self.fixture_id.to_string()
    }

    fn data_prefix(&self) -> String {
        format!("tables/{}", self.fixture_id.simple())
    }
}

impl Worker {
    fn reserve_table_sentinel(&self) -> WorkerResult<String> {
        let mut state = self.state.lock().expect("worker state");
        if state.table_sentinels.len() >= MAX_SESSIONS {
            return Err(invalid("Table sentinel capacity reached"));
        }
        let token = Uuid::new_v4().to_string();
        // Failed setup keeps its reservation and owned objects available for diagnosis.
        state.table_sentinels.insert(token.clone(), None);
        Ok(token)
    }

    async fn sentinel_bucket(&self, name: &str) -> WorkerResult<BucketHandle> {
        let app = self
            .server_ctx
            .installed_app_context()
            .ok_or_else(|| invalid("application is not installed"))?;
        if !Arc::ptr_eq(&app.object_store(), &self.store) {
            return Err(invalid("Table sentinel store differs from installed application"));
        }
        let region = self
            .store
            .region()
            .map(|region| region.to_string())
            .unwrap_or_else(|| rustfs_config::RUSTFS_REGION.into());
        DefaultBucketUsecase::with_context(Some(app))
            .resolve_bucket_handle(
                BucketKind::Table,
                &AccountNamespaceId::empty(),
                &region,
                &BucketPublicName::try_new(name.into()).map_err(invalid)?,
            )
            .await
            .map_err(invalid)
    }

    pub(super) async fn create_table_sentinel(&self) -> WorkerResult<Value> {
        let token = self.reserve_table_sentinel()?;
        let fixture_id = Uuid::new_v4();
        let name = format!("d09-table-{}", fixture_id.simple());
        self.store
            .make_bucket(&name, &MakeBucketOptions::default())
            .await
            .map_err(invalid)?;
        self.store
            .update_bucket_metadata_config(&name, TABLE_BUCKET_MARKER_CONFIG, table_bucket_marker_json()?)
            .await
            .map_err(invalid)?;
        let bucket = self.sentinel_bucket(&name).await?;
        let identity = Identity::from_handle(fixture_id, &bucket)?;
        let backend = EcStoreTableCatalogObjectBackend::new(self.store.clone());
        let catalog = ConfiguredTableCatalogStore::new_for_test(backend, TableCatalogBackingMode::ObjectBacked);
        let warehouse = format!("s3://{name}/{}", identity.data_prefix());
        let metadata_key = format!("{}/metadata.json", identity.data_prefix());
        let metadata = serde_json::to_vec(&table_metadata_json(&identity.table_id(), &warehouse))?;
        for (key, body) in [
            (metadata_key.clone(), metadata),
            (
                format!("{}/sentinel.bin", identity.data_prefix()),
                format!("Table sentinel {fixture_id}").into_bytes(),
            ),
        ] {
            self.store
                .put_object(&name, &key, &mut StoragePutObjReader::from_vec(body), &StorageObjectOptions::default())
                .await
                .map_err(invalid)?;
        }
        catalog
            .put_table_bucket(TableBucketEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: name.clone(),
                vector_bucket_binding: None,
                catalog_type: TABLE_BUCKET_CATALOG_TYPE.into(),
                warehouse_root: format!("s3://{name}/"),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                active_rename_id: None,
                created_at: None,
                updated_at: None,
            })
            .await?;
        catalog
            .create_namespace(NamespaceEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: name.clone(),
                namespace: NAMESPACE.into(),
                namespace_id: NAMESPACE.into(),
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await?;
        catalog
            .create_table(TableEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: name.clone(),
                namespace: NAMESPACE.into(),
                table: TABLE.into(),
                table_id: identity.table_id(),
                table_uuid: fixture_id.to_string(),
                vector_head_binding: None,
                vector_index_definition: None,
                vector_maintenance_binding: None,
                format: "ICEBERG".into(),
                format_version: 2,
                warehouse_location: warehouse,
                metadata_location: format!("s3://{name}/{metadata_key}"),
                version_token: "v1".into(),
                generation: 1,
                state: TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await?;
        self.finish_table_sentinel(token, identity).await
    }

    pub(super) async fn open_table_sentinel(&self, identity: Identity) -> WorkerResult<Value> {
        identity.validate()?;
        let token = self.reserve_table_sentinel()?;
        self.finish_table_sentinel(token, identity).await
    }

    async fn finish_table_sentinel(&self, token: String, identity: Identity) -> WorkerResult<Value> {
        let observation = self.observe_table_sentinel(&identity).await?;
        let mut state = self.state.lock().expect("worker state");
        let reserved = state
            .table_sentinels
            .get_mut(&token)
            .ok_or_else(|| invalid("missing Table sentinel reservation"))?;
        *reserved = Some(identity);
        Ok(json!({"sentinel_token":token,"observation":observation}))
    }

    pub(super) async fn read_table_sentinel(&self, token: &str) -> WorkerResult<Value> {
        let identity = self
            .state
            .lock()
            .expect("worker state")
            .table_sentinels
            .get(token)
            .and_then(Clone::clone)
            .ok_or_else(|| invalid("unknown or incomplete Table sentinel"))?;
        self.observe_table_sentinel(&identity).await
    }

    async fn observe_table_sentinel(&self, expected: &Identity) -> WorkerResult<Value> {
        expected.validate()?;
        let bucket = self.sentinel_bucket(&expected.public_name).await?;
        let observed_identity = Identity::from_handle(expected.fixture_id, &bucket)?;
        if &observed_identity != expected {
            return Err(TableCatalogStoreError::Conflict("Table sentinel bucket identity changed".into()).into());
        }
        let app = self
            .server_ctx
            .installed_app_context()
            .ok_or_else(|| invalid("application is not installed"))?;
        let buckets = DefaultBucketUsecase::with_context(Some(app));
        let lifecycle = buckets.acquire_bucket_handle_guard(&bucket).await.map_err(invalid)?;
        let backend = EcStoreTableCatalogObjectBackend::new(self.store.clone());
        let catalog = ConfiguredTableCatalogStore::new_for_test(backend.clone(), TableCatalogBackingMode::ObjectBacked);
        let name = bucket.storage_bucket_id().as_str();
        let parent = catalog
            .get_table_bucket(name)
            .await?
            .ok_or_else(|| invalid("Table sentinel parent is missing"))?;
        let namespace = catalog
            .get_namespace(name, NAMESPACE)
            .await?
            .ok_or_else(|| invalid("Table sentinel namespace is missing"))?;
        let entry = catalog
            .load_table(name, NAMESPACE, TABLE)
            .await?
            .ok_or_else(|| invalid("Table sentinel entry is missing"))?;
        if parent.vector_bucket_binding.is_some()
            || parent.catalog_type != TABLE_BUCKET_CATALOG_TYPE
            || parent.state != TableCatalogEntryState::Active
            || namespace.state != TableCatalogEntryState::Active
            || entry.state != TableCatalogEntryState::Active
            || entry.table_id != expected.table_id()
            || entry.table_uuid != expected.fixture_id.to_string()
            || entry.format != "ICEBERG"
            || entry.vector_head_binding.is_some()
            || entry.vector_index_definition.is_some()
            || entry.vector_maintenance_binding.is_some()
            || entry.warehouse_location != format!("s3://{name}/{}", expected.data_prefix())
            || entry.metadata_location != format!("s3://{name}/{}/metadata.json", expected.data_prefix())
        {
            return Err(invalid("Table sentinel Catalog binding differs from its owned resource"));
        }
        let paths = TableCatalogObjectPaths::default();
        let ns = Namespace::parse(NAMESPACE).map_err(invalid)?;
        let mut objects = Vec::new();
        for (key, decoded) in [
            (paths.table_bucket_entry_path(name), serde_json::to_value(&parent)?),
            (paths.namespace_entry_path(name, &ns), serde_json::to_value(&namespace)?),
            (paths.table_entry_path(name, &ns, &TABLE), serde_json::to_value(&entry)?),
        ] {
            let object = backend
                .read_object_limited(RUSTFS_META_BUCKET, &key, MAX_SENTINEL_OBJECT_BYTES)
                .await?
                .ok_or_else(|| invalid("Table sentinel Catalog bytes are missing"))?;
            if serde_json::from_slice::<Value>(&object.data)? != decoded {
                return Err(
                    TableCatalogStoreError::Conflict("Table sentinel Catalog bytes changed during observation".into()).into(),
                );
            }
            objects.push((RUSTFS_META_BUCKET.to_owned(), key, object));
        }
        for key in [
            format!("{}/metadata.json", expected.data_prefix()),
            format!("{}/sentinel.bin", expected.data_prefix()),
        ] {
            let object = backend
                .read_object_limited(name, &key, MAX_SENTINEL_OBJECT_BYTES)
                .await?
                .ok_or_else(|| invalid("Table sentinel data evidence is missing"))?;
            objects.push((name.to_owned(), key, object));
        }
        for (object_bucket, key, object) in &objects {
            let current = backend
                .read_object_limited(object_bucket, key, MAX_SENTINEL_OBJECT_BYTES)
                .await?
                .ok_or_else(|| invalid("Table sentinel evidence disappeared during observation"))?;
            let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key.clone())?, object)?;
            verify_reference(&reference, &current)?;
        }
        let metadata = self.store.get_managed_bucket_metadata(name).await.map_err(invalid)?;
        metadata.validate_bucket_handle(&bucket).map_err(invalid)?;
        if lifecycle.is_lock_lost() {
            return Err(invalid("Table sentinel observation lost lifecycle protection"));
        }
        Ok(json!({"instance_id":self.store.id,"kind":"Table","identity":observed_identity,
            "parent":parent,"namespace":namespace,"entry":entry,
            "objects":objects.iter().map(|(bucket,key,object)| object_value(bucket,key,object)).collect::<Vec<_>>()}))
    }
}
