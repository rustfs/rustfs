use super::identifier::{
    TableIdentifier, TablePathResolver, default_namespace_marker_path, default_table_current_pointer_path,
    default_table_lifecycle_path, default_table_marker_path, default_table_root_prefix, is_valid_table_metadata_file_name,
    namespace_name_from_marker_path, table_name_from_marker_path, validate_object_mutation,
};
use super::test_support::{
    BlockingObjectPublication, NoopTableCatalogStore, TestCatalogObjectBackend, UnserializedTestPublication,
};
use super::*;
use datafusion::{
    arrow::{
        array::{Array, Int32Array, Int64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
};
use std::assert_matches;
use std::sync::Arc;

const TABLE_CATALOG_TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

#[test]
fn reserved_table_object_key_matches_exact_prefix_and_children_only() {
    assert!(is_reserved_table_object_key(".rustfs-table"));
    assert!(is_reserved_table_object_key(".rustfs-table/"));
    assert!(is_reserved_table_object_key(".rustfs-table/metadata/current.json"));

    assert!(!is_reserved_table_object_key(""));
    assert!(!is_reserved_table_object_key(".rustfs-table-other"));
    assert!(!is_reserved_table_object_key("prefix/.rustfs-table/object"));
    assert!(!is_reserved_table_object_key("user/.rustfs-table"));
}

#[test]
fn object_mutation_guard_only_blocks_reserved_prefix_for_table_buckets() {
    assert!(validate_object_mutation(false, ".rustfs-table/current.json").is_ok());
    assert_eq!(
        validate_object_mutation(true, ".rustfs-table/current.json").unwrap_err(),
        TableObjectMutationError::ReservedCatalogObject
    );
    assert!(validate_object_mutation(true, ".rustfs-table-other/current.json").is_ok());
}

#[tokio::test]
async fn bucket_object_mutation_guard_fails_closed_for_reserved_prefix_when_bucket_metadata_is_unavailable() {
    assert_eq!(
        validate_bucket_object_mutation("missing-bucket", ".rustfs-table/current.json")
            .await
            .unwrap_err(),
        TableObjectMutationError::ReservedCatalogObject
    );
    assert!(
        validate_bucket_object_mutation("missing-bucket", "ordinary/current.json")
            .await
            .is_ok()
    );
}

#[test]
fn table_bucket_marker_json_uses_stable_catalog_defaults() {
    let marker = serde_json::to_value(TableBucketMarker::default()).unwrap();

    assert_eq!(marker["version"], TABLE_BUCKET_CONFIG_VERSION);
    assert_eq!(marker["catalog_type"], TABLE_BUCKET_CATALOG_TYPE);
    assert_eq!(marker["reserved_prefix"], TABLE_RESERVED_PREFIX);
    assert!(!table_bucket_marker_json().unwrap().is_empty());
}

#[test]
fn catalog_entry_structures_serialize_stable_fields() {
    use std::collections::BTreeMap;

    let bucket = TableBucketEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "analytics".to_string(),
        catalog_type: TABLE_BUCKET_CATALOG_TYPE.to_string(),
        warehouse_root: "s3://analytics/".to_string(),
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::from([("owner".to_string(), "platform".to_string())]),
        created_at: Some("2026-05-23T00:00:00Z".to_string()),
        updated_at: Some("2026-05-23T00:00:00Z".to_string()),
    };
    let namespace = NamespaceEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "analytics".to_string(),
        namespace: "sales".to_string(),
        namespace_id: "sales".to_string(),
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::from([("purpose".to_string(), "orders".to_string())]),
        created_at: Some("2026-05-23T00:00:00Z".to_string()),
        updated_at: Some("2026-05-23T00:00:00Z".to_string()),
    };
    let table = TableEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "analytics".to_string(),
        namespace: "sales".to_string(),
        table: "orders".to_string(),
        table_id: "table-id".to_string(),
        table_uuid: "table-uuid".to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: "s3://analytics/tables/table-id".to_string(),
        metadata_location: "s3://analytics/tables/table-id/metadata/v1.metadata.json".to_string(),
        version_token: "token-v1".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::from([("write.format.default".to_string(), "parquet".to_string())]),
        created_at: Some("2026-05-23T00:00:00Z".to_string()),
        updated_at: Some("2026-05-23T00:00:00Z".to_string()),
    };
    let commit = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "commit-id".to_string(),
        idempotency_key: Some("client-request-id".to_string()),
        table_id: "table-id".to_string(),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        new_version_token: "token-v2".to_string(),
        previous_metadata_location: "s3://analytics/tables/table-id/metadata/v1.metadata.json".to_string(),
        new_metadata_location: "s3://analytics/tables/table-id/metadata/v2.metadata.json".to_string(),
        requirements: vec![serde_json::json!({"type": "assert-table-uuid", "uuid": "table-uuid"})],
        status: CommitLogStatus::Committed,
        writer: Some("pyiceberg/test".to_string()),
        created_at: Some("2026-05-23T00:01:00Z".to_string()),
        updated_at: Some("2026-05-23T00:01:00Z".to_string()),
    };

    let bucket_json = serde_json::to_value(&bucket).unwrap();
    let namespace_json = serde_json::to_value(&namespace).unwrap();
    let table_json = serde_json::to_value(&table).unwrap();
    let commit_json = serde_json::to_value(&commit).unwrap();

    assert_eq!(bucket_json["state"], "ACTIVE");
    assert_eq!(bucket_json["properties"]["owner"], "platform");
    assert_eq!(namespace_json["namespace_id"], "sales");
    assert_eq!(table_json["version_token"], "token-v1");
    assert_eq!(table_json["generation"], 1);
    assert_eq!(table_json["state"], "ACTIVE");
    assert_eq!(commit_json["status"], "COMMITTED");
    assert_eq!(commit_json["requirements"][0]["type"], "assert-table-uuid");
}

#[test]
fn catalog_entry_deserialization_rejects_unknown_fields() {
    use std::collections::BTreeMap;

    let table = TableEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "analytics".to_string(),
        namespace: "sales".to_string(),
        table: "orders".to_string(),
        table_id: "table-id".to_string(),
        table_uuid: "table-uuid".to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: "s3://analytics/tables/table-id".to_string(),
        metadata_location: "s3://analytics/tables/table-id/metadata/v1.metadata.json".to_string(),
        version_token: "token-v1".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    let mut value = serde_json::to_value(table).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("unexpected".to_string(), serde_json::json!(true));

    assert!(serde_json::from_value::<TableEntry>(value).is_err());
}

#[test]
#[serial_test::serial]
fn table_catalog_backing_mode_defaults_to_object() {
    temp_env::with_var_unset(ENV_TABLE_CATALOG_BACKING, || {
        assert_eq!(TableCatalogBackingMode::from_env().unwrap(), TableCatalogBackingMode::ObjectBacked);
    });
}

#[test]
#[serial_test::serial]
fn table_catalog_backing_mode_accepts_durable_strong_value() {
    temp_env::with_var(ENV_TABLE_CATALOG_BACKING, Some(TABLE_CATALOG_BACKING_DURABLE_STRONG), || {
        assert_eq!(TableCatalogBackingMode::from_env().unwrap(), TableCatalogBackingMode::DurableStrong);
    });
}

#[test]
#[serial_test::serial]
fn table_catalog_backing_mode_rejects_unknown_value() {
    temp_env::with_var(ENV_TABLE_CATALOG_BACKING, Some("memory"), || {
        assert!(matches!(
            TableCatalogBackingMode::from_env().unwrap_err(),
            TableCatalogStoreError::Invalid(_)
        ));
    });
}

#[test]
fn catalog_object_listing_rejects_missing_or_stalled_continuation_tokens() {
    let mut seen = BTreeSet::new();
    assert_eq!(
        catalog_list_next_continuation(&mut seen, false, None).expect("complete listings need no token"),
        None
    );
    assert_eq!(
        catalog_list_next_continuation(&mut seen, true, Some("next".to_string())).expect("truncated listing should advance"),
        Some("next".to_string())
    );
    assert_matches!(
        catalog_list_next_continuation(&mut seen, true, None),
        Err(TableCatalogStoreError::Internal(message)) if message.contains("no continuation token")
    );
    assert_matches!(
        catalog_list_next_continuation(&mut seen, true, Some(String::new())),
        Err(TableCatalogStoreError::Internal(message)) if message.contains("no continuation token")
    );
    assert_matches!(
        catalog_list_next_continuation(&mut seen, true, Some("next".to_string())),
        Err(TableCatalogStoreError::Internal(message)) if message.contains("did not advance")
    );
}

#[tokio::test]
async fn table_catalog_store_trait_covers_entry_read_write_shapes() {
    let store: &dyn TableCatalogStore = &NoopTableCatalogStore;

    assert!(store.get_table_bucket("analytics").await.unwrap().is_none());
    assert!(store.list_namespaces("analytics").await.unwrap().is_empty());
    assert!(
        store
            .get_commit_by_id("analytics", "table-id", "commit-id")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_commit_by_idempotency_key("analytics", "table-id", "client-request-id")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn table_catalog_store_trait_has_atomic_commit_shape() {
    let store: &dyn TableCatalogStore = &NoopTableCatalogStore;
    let request = TableCommitRequest {
        table_bucket: "analytics".to_string(),
        namespace: "sales".to_string(),
        table: "orders".to_string(),
        commit_id: "commit-id".to_string(),
        idempotency_key: Some("client-request-id".to_string()),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        expected_metadata_location: "s3://analytics/tables/table-id/metadata/v1.metadata.json".to_string(),
        new_metadata_location: "s3://analytics/tables/table-id/metadata/v2.metadata.json".to_string(),
        requirements: vec![serde_json::json!({"type": "assert-table-uuid", "uuid": "table-uuid"})],
        writer: Some("pyiceberg/test".to_string()),
    };

    let result = store.commit_table(request).await.unwrap();

    assert_eq!(result.table.version_token, "token-v2");
    assert_eq!(result.table.generation, 2);
    assert_eq!(result.commit_log.status, CommitLogStatus::Committed);
}

async fn assert_direct_commit_uses_publication_lock<S>(store: &S, backend: &TestCatalogObjectBackend)
where
    S: TableCatalogStore + ?Sized,
{
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let publication_lock = default_table_publication_lock_path(&namespace, &table);

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    let acquisitions_before = backend.write_lock_acquisition_count(bucket, &publication_lock).await;

    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: Some("rustfs-maintenance".to_string()),
        })
        .await
        .expect("direct commit should succeed");

    assert_eq!(
        backend.write_lock_acquisition_count(bucket, &publication_lock).await,
        acquisitions_before + 1
    );
    let guard = tokio::time::timeout(
        TABLE_CATALOG_TEST_TIMEOUT,
        TableCatalogObjectBackend::acquire_write_lock(backend, bucket, &publication_lock),
    )
    .await
    .expect("publication lock should be released after commit")
    .expect("publication lock should be reacquired");
    drop(guard);
}

#[tokio::test]
async fn catalog_backings_fence_direct_commits_with_publication_lock() {
    let object_backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(object_backend.clone());
    assert_direct_commit_uses_publication_lock(&object_store, &object_backend).await;

    let strong_backend = TestCatalogObjectBackend::default();
    let strong_store = StrongTableCatalogStore::new(strong_backend.clone());
    assert_direct_commit_uses_publication_lock(&strong_store, &strong_backend).await;
}

#[derive(Default)]
struct LosingTestPublication {
    table_checks: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl TableCommitPublication for LosingTestPublication {
    async fn begin_table_bucket(&self, _table_bucket: &str) -> TableCatalogStoreResult<()> {
        Ok(())
    }

    async fn prepare(&self, _table_bucket: &str, _namespace: &str, _table: &str) -> TableCatalogStoreResult<()> {
        Ok(())
    }

    fn holds_table_bucket(&self, _table_bucket: &str) -> bool {
        true
    }

    fn holds_table(&self, _table_bucket: &str, _namespace: &str, _table: &str) -> bool {
        self.table_checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0
    }

    fn complete(&self) {}
}

async fn assert_view_replacement_rechecks_publication_fence<S>(store: &S, backend: &TestCatalogObjectBackend)
where
    S: TableCatalogStore,
{
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let current_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let next_metadata = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_view(test_view_entry(bucket, &namespace, &view, current_metadata.clone()))
        .await
        .expect("view should be created");
    backend
        .seed_object(
            bucket,
            &next_metadata,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 1,
                "view-uuid": "view-uuid",
                "location": format!("s3://{bucket}/views/view-id")
            }))
            .expect("view metadata should encode"),
        )
        .await;

    let error = store
        .replace_view_with_publication(
            ViewCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                view: view.as_str().to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata.clone(),
                new_metadata_location: next_metadata,
            },
            true,
            &LosingTestPublication::default(),
        )
        .await
        .expect_err("a lost publication fence must stop the view replacement");
    assert_matches!(
        error,
        TableCatalogStoreError::Internal(message) if message.contains("publication fence was lost")
    );

    let loaded = store
        .load_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect("view lookup should succeed")
        .expect("view should remain present");
    assert_eq!(loaded.metadata_location, current_metadata);
    assert_eq!(loaded.version_token, "token-v1");
    assert_eq!(loaded.generation, 1);
}

#[tokio::test]
async fn catalog_backings_stop_view_replacement_after_publication_fence_loss() {
    let object_backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(object_backend.clone());
    assert_view_replacement_rechecks_publication_fence(&object_store, &object_backend).await;

    let strong_backend = TestCatalogObjectBackend::default();
    let strong_store = StrongTableCatalogStore::new(strong_backend.clone());
    assert_view_replacement_rechecks_publication_fence(&strong_store, &strong_backend).await;
}

#[tokio::test]
async fn strong_table_registration_and_drop_acquire_publication_before_migration_read_lock() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let metadata_location = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();

    let publication_lock = default_table_bucket_publication_lock_path();
    let migration_lock = TableCatalogObjectPaths::default().backing_migration_global_fence_lock_path();
    let publication_guard = backend
        .acquire_write_lock(bucket, &publication_lock)
        .await
        .expect("publication lock should be acquired");
    let publication_attempts = backend.write_lock_acquisition_count(bucket, &publication_lock).await;
    let migration_reads = backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock).await;
    let register_store = store.clone();
    let register = tokio::spawn(async move {
        register_store
            .register_table(test_table_entry(bucket, &namespace, &table, metadata_location))
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.write_lock_acquisition_count(bucket, &publication_lock).await == publication_attempts {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("registration should wait on the table-bucket publication lock");
    assert_eq!(
        backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock).await,
        migration_reads,
        "registration must not retain a migration read lock while waiting for publication"
    );
    let migration_guard = tokio::time::timeout(
        TABLE_CATALOG_TEST_TIMEOUT,
        backend.acquire_write_lock(RUSTFS_META_BUCKET, &migration_lock),
    )
    .await
    .expect("migration writer must not be blocked by registration waiting on publication")
    .expect("migration write lock should be acquired");
    drop(publication_guard);
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock).await == migration_reads {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("registration should request the migration read lock after publication");
    assert!(!register.is_finished());
    drop(migration_guard);
    register
        .await
        .expect("registration task should join")
        .expect("registration should succeed");

    let publication_guard = backend
        .acquire_read_lock(bucket, &publication_lock)
        .await
        .expect("publication reader should be acquired");
    let publication_attempts = backend.write_lock_acquisition_count(bucket, &publication_lock).await;
    let migration_reads = backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock).await;
    let drop_store = store.clone();
    let drop_task = tokio::spawn(async move { drop_store.drop_table(bucket, "sales", "orders").await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.write_lock_acquisition_count(bucket, &publication_lock).await == publication_attempts {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop should wait on the table-bucket publication lock");
    assert_eq!(
        backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock).await,
        migration_reads,
        "drop must not retain a migration read lock while waiting for publication"
    );
    let migration_guard = tokio::time::timeout(
        TABLE_CATALOG_TEST_TIMEOUT,
        backend.acquire_write_lock(RUSTFS_META_BUCKET, &migration_lock),
    )
    .await
    .expect("migration writer must not be blocked by drop waiting on publication")
    .expect("migration write lock should be acquired");
    drop(publication_guard);
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock).await == migration_reads {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop should request the migration read lock after publication");
    assert!(!drop_task.is_finished());
    drop(migration_guard);
    drop_task.await.expect("drop task should join").expect("drop should succeed");
}

async fn assert_direct_drop_uses_publication_locks<S>(store: S, backend: &TestCatalogObjectBackend)
where
    S: TableCatalogStore + Clone + Send + Sync + 'static,
{
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata))
        .await
        .expect("table should be created");

    let bucket_lock = default_table_bucket_publication_lock_path();
    let table_lock = default_table_publication_lock_path(&namespace, &table);
    let bucket_guard = backend
        .acquire_read_lock(bucket, &bucket_lock)
        .await
        .expect("bucket publication reader should be acquired");
    let table_guard = backend
        .acquire_read_lock(bucket, &table_lock)
        .await
        .expect("table publication reader should be acquired");
    let bucket_attempts = backend.write_lock_acquisition_count(bucket, &bucket_lock).await;
    let table_attempts = backend.write_lock_acquisition_count(bucket, &table_lock).await;

    let drop_store = store.clone();
    let drop_task = tokio::spawn(async move { drop_store.drop_table(bucket, "sales", "orders").await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.write_lock_acquisition_count(bucket, &bucket_lock).await == bucket_attempts {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop should attempt the bucket publication fence");
    assert!(!drop_task.is_finished(), "drop must wait for an in-flight bucket writer");

    drop(bucket_guard);
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.write_lock_acquisition_count(bucket, &table_lock).await == table_attempts {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drop should attempt the table publication fence");
    assert!(!drop_task.is_finished(), "drop must wait for an in-flight table writer");

    drop(table_guard);
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, drop_task)
        .await
        .expect("drop should continue after publication readers finish")
        .expect("drop task should join")
        .expect("drop should succeed");
    assert!(
        store
            .load_table(bucket, "sales", "orders")
            .await
            .expect("table lookup should succeed")
            .is_none(),
        "table must become invisible before publication fences are released"
    );
}

#[tokio::test]
async fn catalog_backings_fence_direct_table_drop() {
    let object_backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(object_backend.clone());
    assert_direct_drop_uses_publication_locks(object_store, &object_backend).await;

    let strong_backend = TestCatalogObjectBackend::default();
    let strong_store = StrongTableCatalogStore::new(strong_backend.clone());
    assert_direct_drop_uses_publication_locks(strong_store, &strong_backend).await;
}

#[tokio::test]
async fn strong_catalog_blocked_publication_object_does_not_stall_unrelated_writes() {
    let backend = TestCatalogObjectBackend::default();
    let store = Arc::new(StrongTableCatalogStore::new(backend.clone()));
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

    let blocked_object = "tables/table-id/data/late.parquet";
    let blocker = backend
        .acquire_write_lock(bucket, blocked_object)
        .await
        .expect("blocked object lock should be acquired");
    let publication = Arc::new(BlockingObjectPublication::new(backend.clone(), blocked_object));
    let commit_store = Arc::clone(&store);
    let commit_publication = Arc::clone(&publication);
    let commit = tokio::spawn(async move {
        commit_store
            .commit_table_with_publication(
                TableCommitRequest {
                    table_bucket: bucket.to_string(),
                    namespace: namespace.public_name(),
                    table: table.as_str().to_string(),
                    commit_id: "blocked-publication".to_string(),
                    idempotency_key: None,
                    operation: "append".to_string(),
                    expected_version_token: "token-v1".to_string(),
                    expected_metadata_location: current_metadata,
                    new_metadata_location: new_metadata,
                    requirements: Vec::new(),
                    writer: Some("concurrency-test".to_string()),
                },
                commit_publication.as_ref(),
            )
            .await
    });
    publication.wait_started().await;

    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, store.put_table_bucket(test_bucket_entry("independent")))
        .await
        .expect("a blocked table publication must not hold the strong catalog write lock")
        .expect("the unrelated table bucket write should succeed");

    drop(blocker);
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, commit)
        .await
        .expect("the blocked commit should finish after its object lock is released")
        .expect("the commit task should join")
        .expect("the commit should succeed");
    assert!(
        store
            .get_table_bucket("independent")
            .await
            .expect("unrelated table bucket lookup should succeed")
            .is_some()
    );
    assert_eq!(
        store
            .load_table(bucket, "sales", "orders")
            .await
            .expect("committed table lookup should succeed")
            .expect("committed table should exist")
            .metadata_location,
        default_table_metadata_file_path(
            &Namespace::parse("sales").expect("namespace should parse"),
            &IdentifierSegment::parse("orders").expect("table should parse"),
            "00002.metadata.json",
        )
    );
}

#[tokio::test]
async fn strong_catalog_metadata_read_does_not_stall_unrelated_writes() {
    let backend = TestCatalogObjectBackend::default();
    let store = Arc::new(StrongTableCatalogStore::new(backend.clone()));
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    let metadata_read = backend.pause_next_read(bucket, &new_metadata).await;

    let commit_store = Arc::clone(&store);
    let commit = tokio::spawn(async move {
        commit_store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "slow-metadata-read".to_string(),
                idempotency_key: None,
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata,
                new_metadata_location: new_metadata,
                requirements: Vec::new(),
                writer: Some("concurrency-test".to_string()),
            })
            .await
    });
    metadata_read.wait_started().await;

    let independent = Namespace::parse("independent").expect("namespace should parse");
    tokio::time::timeout(
        TABLE_CATALOG_TEST_TIMEOUT,
        store.create_namespace(test_namespace_entry(bucket, &independent)),
    )
    .await
    .expect("a slow metadata read must not hold the strong catalog write lock")
    .expect("the unrelated namespace write should succeed");

    metadata_read.release();
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, commit)
        .await
        .expect("the commit should finish after the metadata read is released")
        .expect("the commit task should join")
        .expect("the commit should succeed");
    assert!(
        store
            .get_namespace(bucket, &independent.public_name())
            .await
            .expect("unrelated namespace lookup should succeed")
            .is_some()
    );
}

#[tokio::test]
async fn strong_catalog_view_metadata_read_does_not_stall_unrelated_writes() {
    let backend = TestCatalogObjectBackend::default();
    let store = Arc::new(StrongTableCatalogStore::new(backend.clone()));
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let current_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let new_metadata = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_view(test_view_entry(bucket, &namespace, &view, current_metadata.clone()))
        .await
        .expect("view should be created");
    backend
        .seed_object(
            bucket,
            &new_metadata,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 1,
                "view-uuid": "view-uuid",
                "location": "s3://analytics/views/view-id"
            }))
            .expect("view metadata should encode"),
        )
        .await;
    let metadata_read = backend.pause_next_read(bucket, &new_metadata).await;

    let replace_store = Arc::clone(&store);
    let replace = tokio::spawn(async move {
        replace_store
            .replace_view(ViewCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                view: view.as_str().to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata,
                new_metadata_location: new_metadata,
            })
            .await
    });
    metadata_read.wait_started().await;

    let independent = Namespace::parse("independent").expect("namespace should parse");
    tokio::time::timeout(
        TABLE_CATALOG_TEST_TIMEOUT,
        store.create_namespace(test_namespace_entry(bucket, &independent)),
    )
    .await
    .expect("a slow view metadata read must not hold the strong catalog write lock")
    .expect("the unrelated namespace write should succeed");

    metadata_read.release();
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, replace)
        .await
        .expect("the view replacement should finish after the metadata read is released")
        .expect("the view replacement task should join")
        .expect("the view replacement should succeed");
    assert!(
        store
            .get_namespace(bucket, &independent.public_name())
            .await
            .expect("unrelated namespace lookup should succeed")
            .is_some()
    );
}

#[tokio::test]
async fn strong_catalog_view_replace_rejects_identity_recreation_during_metadata_read() {
    let backend = TestCatalogObjectBackend::default();
    let store = Arc::new(StrongTableCatalogStore::new(backend.clone()));
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let current_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let new_metadata = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_view(test_view_entry(bucket, &namespace, &view, current_metadata.clone()))
        .await
        .expect("view should be created");
    backend
        .seed_object(
            bucket,
            &new_metadata,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 1,
                "view-uuid": "view-uuid",
                "location": "s3://analytics/views/view-id"
            }))
            .expect("view metadata should encode"),
        )
        .await;
    let metadata_read = backend.pause_next_read(bucket, &new_metadata).await;

    let replace_store = Arc::clone(&store);
    let replace_namespace = namespace.clone();
    let replace_view = view.clone();
    let replace_current_metadata = current_metadata.clone();
    let replace = tokio::spawn(async move {
        replace_store
            .replace_view_with_publication(
                ViewCommitRequest {
                    table_bucket: bucket.to_string(),
                    namespace: replace_namespace.public_name(),
                    view: replace_view.as_str().to_string(),
                    expected_version_token: "token-v1".to_string(),
                    expected_metadata_location: replace_current_metadata,
                    new_metadata_location: new_metadata,
                },
                false,
                &UnserializedTestPublication,
            )
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, metadata_read.wait_started())
        .await
        .expect("the replacement should reach the paused metadata read");

    store
        .drop_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect("original view should be dropped");
    let mut recreated = test_view_entry(bucket, &namespace, &view, current_metadata);
    recreated.view_id = "replacement-view-id".to_string();
    recreated.view_uuid = "replacement-view-uuid".to_string();
    recreated.warehouse_location = format!("s3://{bucket}/views/replacement-view-id");
    store
        .create_view(recreated.clone())
        .await
        .expect("replacement view should be created");

    metadata_read.release();
    assert_matches!(
        tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, replace)
            .await
            .expect("the view replacement should finish after the metadata read is released")
            .expect("the view replacement task should join"),
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("identity changed")
    );
    assert_eq!(
        store
            .load_view(bucket, &namespace.public_name(), view.as_str())
            .await
            .expect("replacement view lookup should succeed"),
        Some(recreated)
    );
}

#[test]
fn catalog_object_entry_paths_use_internal_root_and_hashed_untrusted_ids() {
    let paths = TableCatalogObjectPaths::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("analytics.daily_events").unwrap();
    let table = IdentifierSegment::parse("events").unwrap();
    let bucket_root = format!("s3tables/catalog/table-buckets/{}/", table_catalog_path_hash(bucket));

    assert_eq!(paths.table_bucket_entry_path(bucket), format!("{bucket_root}table-bucket.json"));
    assert_eq!(
        paths.namespace_entry_path(bucket, &namespace),
        format!("{bucket_root}namespaces/analytics/daily_events/namespace-entry.json")
    );
    assert_eq!(
        paths.table_entry_path(bucket, &namespace, &table),
        format!("{bucket_root}namespaces/analytics/daily_events/tables/events/table-entry.json")
    );
    assert_eq!(
        paths.view_entry_path(bucket, &namespace, &table),
        format!("{bucket_root}namespaces/analytics/daily_events/views/events/view-entry.json")
    );

    let commit_path = paths.commit_log_entry_path("table/../bucket", "table/../id", "commit/%2f\nid");
    let idempotency_path = paths.commit_idempotency_entry_path("table/../bucket", "table/../id", "client/%2f\nrequest");
    let warehouse_index_path = paths.warehouse_index_entry_path("table/../bucket", "tables/table/../id/data\nprefix/");
    let warehouse_index_state_path = paths.warehouse_index_state_path("table/../bucket");
    let maintenance_config_path = paths.table_maintenance_config_path("table/../bucket", &namespace, &table, "table/../id");
    let maintenance_job_path =
        paths.table_maintenance_job_path("table/../bucket", &namespace, &table, "table/../id", "job/%2f\nid");

    for path in [
        commit_path,
        idempotency_path,
        warehouse_index_path,
        warehouse_index_state_path,
        maintenance_config_path,
        maintenance_job_path,
    ] {
        assert!(path.starts_with("s3tables/catalog/table-buckets/"));
        assert!(path.ends_with(".json"));
        assert!(!path.contains(".."));
        assert!(!path.contains('%'));
        assert!(!path.contains('\n'));
        assert!(!path.contains("table/../bucket"));
        assert!(!path.contains("table/../id"));
        assert!(!path.contains("client/%2f"));
    }
}

fn maintenance_object_report<'a>(
    report: &'a TableMetadataMaintenanceReport,
    metadata_location: &str,
) -> &'a TableMetadataMaintenanceObjectReport {
    report
        .object_reports
        .iter()
        .find(|object| object.metadata_location == metadata_location)
        .expect("metadata maintenance object report should exist")
}

fn snapshot_expiration_report(
    report: &TableSnapshotExpirationReport,
    snapshot_id: i64,
) -> &TableSnapshotExpirationSnapshotReport {
    report
        .snapshot_reports
        .iter()
        .find(|snapshot| snapshot.snapshot_id == Some(snapshot_id))
        .expect("snapshot expiration report should include the snapshot")
}

fn compaction_snapshot_report(report: &TableCompactionPlanningReport, snapshot_id: i64) -> &TableCompactionSnapshotReport {
    report
        .snapshot_reports
        .iter()
        .find(|snapshot| snapshot.snapshot_id == Some(snapshot_id))
        .expect("compaction planning report should include the snapshot")
}

fn object_cleanup_report<'a>(
    report: &'a TableMetadataMaintenanceReport,
    object_location: &str,
) -> &'a TableMetadataMaintenanceObjectCleanupReport {
    report
        .object_cleanup_reports
        .iter()
        .find(|object| object.object_location == object_location)
        .expect("metadata maintenance object cleanup report should exist")
}

fn manifest_list_avro_bytes(manifests: &[(&str, usize)]) -> Vec<u8> {
    manifest_list_avro_bytes_with_spec(manifests, 0)
}

fn manifest_list_avro_bytes_with_spec(manifests: &[(&str, usize)], partition_spec_id: i32) -> Vec<u8> {
    manifest_list_avro_bytes_with_spec_and_content(manifests, partition_spec_id, 0)
}

fn manifest_list_avro_bytes_with_spec_and_content(manifests: &[(&str, usize)], partition_spec_id: i32, content: i32) -> Vec<u8> {
    let manifests = manifests
        .iter()
        .map(|(path, length)| (*path, *length, partition_spec_id, content, 7_i64, 20_i64))
        .collect::<Vec<_>>();
    crate::table_catalog::test_support::manifest_list_avro_entries_with_content(&manifests)
}

fn manifest_list_avro_bytes_with_spec_and_null_counts(
    manifests: &[(&str, usize)],
    partition_spec_id: i32,
    null_counts: bool,
) -> Vec<u8> {
    if !null_counts {
        return manifest_list_avro_bytes_with_spec(manifests, partition_spec_id);
    }
    let manifests = manifests
        .iter()
        .map(|(path, length)| (*path, *length, partition_spec_id, 7_i64, 20_i64))
        .collect::<Vec<_>>();
    crate::table_catalog::test_support::manifest_list_avro_entries_with_nullable_counts(&manifests)
}

fn v1_manifest_list_avro_bytes(manifest_path: &str, manifest_length: usize) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
        {
          "type": "record",
          "name": "manifest_file",
          "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
            {"name": "partition_spec_id", "type": "int"},
            {"name": "added_snapshot_id", "type": "long"}
          ]
        }
        "#,
    )
    .expect("v1 manifest list schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("v1 manifest list writer should initialize");
    writer
        .append_value(apache_avro::types::Value::Record(vec![
            ("manifest_path".to_string(), apache_avro::types::Value::String(manifest_path.to_string())),
            (
                "manifest_length".to_string(),
                apache_avro::types::Value::Long(i64::try_from(manifest_length).expect("test manifest length should fit")),
            ),
            ("partition_spec_id".to_string(), apache_avro::types::Value::Int(0)),
            ("added_snapshot_id".to_string(), apache_avro::types::Value::Long(10)),
        ]))
        .expect("v1 manifest list record should append");
    writer.into_inner().expect("v1 manifest list should flush")
}

fn v1_manifest_avro_bytes(data_file_path: &str) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
        {
          "type": "record",
          "name": "manifest_entry",
          "fields": [
            {"name": "status", "type": "int"},
            {"name": "snapshot_id", "type": "long"},
            {
              "name": "data_file",
              "type": {
                "type": "record",
                "name": "data_file",
                "fields": [
                  {"name": "file_path", "type": "string"},
                  {"name": "file_format", "type": "string"},
                  {"name": "partition", "type": {"type": "record", "name": "partition", "fields": []}},
                  {"name": "record_count", "type": "long"},
                  {"name": "file_size_in_bytes", "type": "long"}
                ]
              }
            }
          ]
        }
        "#,
    )
    .expect("v1 manifest schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("v1 manifest writer should initialize");
    writer
        .add_user_metadata("partition-spec-id".to_string(), "0")
        .expect("v1 manifest partition spec metadata should write");
    writer
        .append_value(apache_avro::types::Value::Record(vec![
            ("status".to_string(), apache_avro::types::Value::Int(1)),
            ("snapshot_id".to_string(), apache_avro::types::Value::Long(10)),
            (
                "data_file".to_string(),
                apache_avro::types::Value::Record(vec![
                    ("file_path".to_string(), apache_avro::types::Value::String(data_file_path.to_string())),
                    ("file_format".to_string(), apache_avro::types::Value::String("PARQUET".to_string())),
                    ("partition".to_string(), apache_avro::types::Value::Record(Vec::new())),
                    ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                    ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                ]),
            ),
        ]))
        .expect("v1 manifest record should append");
    writer.into_inner().expect("v1 manifest should flush")
}

fn table_metadata_json_for_validation() -> serde_json::Value {
    serde_json::json!({
        "format-version": 2,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "last-sequence-number": 0,
        "last-updated-ms": 1,
        "last-column-id": 1,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        }],
        "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    })
}

#[test]
fn iceberg_metadata_validation_accepts_complete_v1_and_v2_shapes() {
    validate_supported_table_metadata(&table_metadata_json_for_validation())
        .expect("complete Iceberg v2 metadata should validate");

    let v1 = serde_json::json!({
        "format-version": 1,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "last-updated-ms": 1,
        "last-column-id": 1,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "partition-spec": [{"source-id": 1, "name": "id", "transform": "identity"}],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": []
    });
    validate_supported_table_metadata(&v1).expect("complete Iceberg v1 metadata should validate");

    let mut v1_retired_partition_source = v1.clone();
    v1_retired_partition_source["schemas"] = serde_json::json!([
        {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        {"type": "struct", "schema-id": 1, "fields": []}
    ]);
    v1_retired_partition_source["current-schema-id"] = serde_json::Value::from(1);
    v1_retired_partition_source["schema"] = serde_json::json!({"type": "struct", "schema-id": 1, "fields": []});
    validate_supported_table_metadata(&v1_retired_partition_source)
        .expect_err("the v1 partition spec must bind to the current schema rather than a historical schema");

    let mut negative_v1_schema_id = v1;
    negative_v1_schema_id["schema"]["schema-id"] = serde_json::Value::from(-1);
    validate_supported_table_metadata(&negative_v1_schema_id).expect_err("Iceberg v1 schema IDs must not be negative");
}

#[test]
fn iceberg_metadata_validation_rejects_incomplete_v2_and_dangling_references() {
    let metadata = table_metadata_json_for_validation();
    for field in [
        "last-updated-ms",
        "last-column-id",
        "last-sequence-number",
        "schemas",
        "current-schema-id",
        "partition-specs",
        "default-spec-id",
        "last-partition-id",
        "sort-orders",
        "default-sort-order-id",
    ] {
        let mut incomplete = metadata.clone();
        incomplete
            .as_object_mut()
            .expect("metadata should be an object")
            .remove(field);
        validate_supported_table_metadata(&incomplete).expect_err("missing required Iceberg v2 metadata fields must be rejected");
    }

    let mut dangling = metadata;
    dangling["snapshots"] = serde_json::json!([{"snapshot-id": 10, "schema-id": 7}]);
    dangling["current-snapshot-id"] = serde_json::Value::from(10);
    dangling["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 11}});
    let error = validate_supported_table_metadata(&dangling).expect_err("dangling snapshot references must be rejected");
    assert!(matches!(error, TableCatalogStoreError::Invalid(_)));
}

#[test]
fn iceberg_metadata_validation_rejects_invalid_primitive_types_and_field_ids() {
    let metadata = table_metadata_json_for_validation();
    for invalid_type in [
        "banana",
        "decimal(0,0)",
        "decimal(10,11)",
        "fixed[0]",
        "fixed[2147483648]",
        "decimal(10)",
    ] {
        let mut invalid = metadata.clone();
        invalid["schemas"][0]["fields"][0]["type"] = serde_json::Value::from(invalid_type);
        validate_supported_table_metadata(&invalid).expect_err("invalid Iceberg primitive types must be rejected");
    }
    for valid_type in [
        "decimal(38,38)",
        "decimal(9, 2)",
        "decimal( 9 , 2 )",
        "fixed[16]",
        "fixed[ 16 ]",
        "timestamptz",
        "uuid",
    ] {
        let mut valid = metadata.clone();
        valid["schemas"][0]["fields"][0]["type"] = serde_json::Value::from(valid_type);
        validate_supported_table_metadata(&valid).expect("standard Iceberg primitive types should validate");
    }
    for invalid_id in [0, -1] {
        let mut invalid = metadata.clone();
        invalid["schemas"][0]["fields"][0]["id"] = serde_json::Value::from(invalid_id);
        validate_supported_table_metadata(&invalid).expect_err("Iceberg field IDs must be positive");
    }
    let mut reserved_id = metadata;
    reserved_id["schemas"][0]["fields"][0]["id"] = serde_json::Value::from(2_147_483_448_i64);
    validate_supported_table_metadata(&reserved_id).expect_err("reserved Iceberg metadata field IDs must be rejected");

    let mut negative_schema_id = table_metadata_json_for_validation();
    negative_schema_id["schemas"][0]["schema-id"] = serde_json::Value::from(-1);
    negative_schema_id["current-schema-id"] = serde_json::Value::from(-1);
    validate_supported_table_metadata(&negative_schema_id).expect_err("Iceberg schema IDs must not be negative");

    let mut negative_last_partition_id = table_metadata_json_for_validation();
    negative_last_partition_id["last-partition-id"] = serde_json::Value::from(-1);
    validate_supported_table_metadata(&negative_last_partition_id).expect_err("Iceberg last-partition-id must not be negative");
}

#[test]
fn iceberg_metadata_validation_enforces_schema_evolution() {
    let mut promoted = table_metadata_json_for_validation();
    promoted["schemas"][0]["fields"][0]["type"] = serde_json::Value::from("int");
    promoted["schemas"]
        .as_array_mut()
        .expect("schemas should be an array")
        .push(serde_json::json!({
            "type": "struct",
            "schema-id": 1,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        }));
    promoted["current-schema-id"] = serde_json::Value::from(1);
    validate_supported_table_metadata(&promoted).expect("int fields may promote to long");

    let mut decimal_promotion = table_metadata_json_for_validation();
    decimal_promotion["schemas"][0]["fields"][0]["type"] = serde_json::Value::from("decimal(9, 2)");
    decimal_promotion["schemas"]
        .as_array_mut()
        .expect("schemas should be an array")
        .push(serde_json::json!({
            "type": "struct",
            "schema-id": 1,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "decimal( 10 , 2 )"}]
        }));
    decimal_promotion["current-schema-id"] = serde_json::Value::from(1);
    validate_supported_table_metadata(&decimal_promotion)
        .expect("decimal precision promotion must accept optional parameter whitespace");

    let mut incompatible = promoted;
    incompatible["schemas"][1]["fields"][0]["type"] = serde_json::Value::from("string");
    let error = validate_supported_table_metadata(&incompatible).expect_err("field IDs must retain compatible types");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("schema field 1 has an incompatible type evolution".to_string())
    );

    let mut moved_into_collection = table_metadata_json_for_validation();
    moved_into_collection["last-column-id"] = serde_json::Value::from(2);
    moved_into_collection["schemas"]
        .as_array_mut()
        .expect("schemas should be an array")
        .push(serde_json::json!({
            "type": "struct",
            "schema-id": 1,
            "fields": [{
                "id": 2,
                "name": "items",
                "required": true,
                "type": {
                    "type": "list",
                    "element-id": 1,
                    "element-required": true,
                    "element": "long"
                }
            }]
        }));
    moved_into_collection["current-schema-id"] = serde_json::Value::from(1);
    validate_supported_table_metadata(&moved_into_collection).expect_err("a schema field ID must not move into a list or map");

    let mut reused = table_metadata_json_for_validation();
    reused["schemas"] = serde_json::json!([
        {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        {"type": "struct", "schema-id": 1, "fields": []},
        {
            "type": "struct",
            "schema-id": 2,
            "fields": [{"id": 1, "name": "replacement", "required": true, "type": "long"}]
        }
    ]);
    reused["current-schema-id"] = serde_json::Value::from(2);
    let error = validate_supported_table_metadata(&reused).expect_err("removed Iceberg field IDs must never be reused");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("schema field 1 cannot be reused after removal".to_string())
    );

    let mut stale_last_column_id = table_metadata_json_for_validation();
    stale_last_column_id["last-column-id"] = serde_json::Value::from(0);
    let error = validate_supported_table_metadata(&stale_last_column_id)
        .expect_err("last-column-id must cover nested and top-level assigned field IDs");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid(
            "last-column-id must be non-negative and cover every assigned schema field id".to_string()
        )
    );
}

#[test]
fn iceberg_metadata_transition_preserves_assignment_watermarks_and_schema_history() {
    let current = table_metadata_json_for_validation();

    let mut lower_column_watermark = current.clone();
    lower_column_watermark["last-column-id"] = serde_json::Value::from(0);
    let error = validate_table_metadata_transition(&current, &lower_column_watermark)
        .expect_err("last-column-id must not decrease across commits");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("last-column-id must not decrease across table metadata commits".to_string())
    );

    let mut lower_partition_watermark = current.clone();
    lower_partition_watermark["last-partition-id"] = serde_json::Value::from(998);
    let error = validate_table_metadata_transition(&current, &lower_partition_watermark)
        .expect_err("last-partition-id must not decrease across commits");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("last-partition-id must not decrease across table metadata commits".to_string())
    );

    let mut current_sequence = current.clone();
    current_sequence["last-sequence-number"] = serde_json::Value::from(2);
    let mut lower_sequence_watermark = current_sequence.clone();
    lower_sequence_watermark["last-sequence-number"] = serde_json::Value::from(1);
    let error = validate_table_metadata_transition(&current_sequence, &lower_sequence_watermark)
        .expect_err("last-sequence-number must not decrease across commits");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("last-sequence-number must not decrease across table metadata commits".to_string())
    );

    let mut current_partitioned = current.clone();
    current_partitioned["partition-specs"] = serde_json::json!([{
        "spec-id": 0,
        "fields": [{
            "source-id": 1,
            "field-id": 1000,
            "name": "id",
            "transform": "identity"
        }]
    }]);
    current_partitioned["last-partition-id"] = serde_json::Value::from(1000);
    let mut modified_partition = current_partitioned.clone();
    modified_partition["partition-specs"][0]["fields"][0]["name"] = serde_json::Value::from("renamed_id");
    let error = validate_table_metadata_transition(&current_partitioned, &modified_partition)
        .expect_err("published partition specs must be immutable");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("existing partition spec 0 must not be modified".to_string())
    );

    let mut current_sorted = current.clone();
    current_sorted["sort-orders"] = serde_json::json!([
        {"order-id": 0, "fields": []},
        {
            "order-id": 1,
            "fields": [{
                "source-id": 1,
                "transform": "identity",
                "direction": "asc",
                "null-order": "nulls-first"
            }]
        }
    ]);
    current_sorted["default-sort-order-id"] = serde_json::Value::from(1);
    let mut modified_sort = current_sorted.clone();
    modified_sort["sort-orders"][1]["fields"][0]["direction"] = serde_json::Value::from("desc");
    let error =
        validate_table_metadata_transition(&current_sorted, &modified_sort).expect_err("published sort orders must be immutable");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("existing sort order 1 must not be modified".to_string())
    );

    let mut current_with_snapshot = current.clone();
    current_with_snapshot["last-sequence-number"] = serde_json::Value::from(1);
    current_with_snapshot["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
        "summary": {"operation": "append"}
    }]);
    let mut modified_snapshot = current_with_snapshot.clone();
    modified_snapshot["snapshots"][0]["timestamp-ms"] = serde_json::Value::from(2);
    let error = validate_table_metadata_transition(&current_with_snapshot, &modified_snapshot)
        .expect_err("published snapshots must be immutable");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("existing snapshot 10 must not be modified".to_string())
    );

    let mut modified_history = current.clone();
    modified_history["schemas"][0]["fields"][0]["type"] = serde_json::Value::from("string");
    let error = validate_table_metadata_transition(&current, &modified_history)
        .expect_err("published schema definitions must be immutable");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("existing schema 0 must not be modified".to_string())
    );

    let mut current_with_retired_id = current.clone();
    current_with_retired_id["schemas"] = serde_json::json!([
        {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        {"type": "struct", "schema-id": 1, "fields": []}
    ]);
    current_with_retired_id["current-schema-id"] = serde_json::Value::from(1);
    let mut reused_id = current_with_retired_id.clone();
    reused_id["schemas"] = serde_json::json!([
        {"type": "struct", "schema-id": 1, "fields": []},
        {
            "type": "struct",
            "schema-id": 2,
            "fields": [{"id": 1, "name": "replacement", "required": true, "type": "long"}]
        }
    ]);
    reused_id["current-schema-id"] = serde_json::Value::from(2);
    let error = validate_table_metadata_transition(&current_with_retired_id, &reused_id)
        .expect_err("new schemas must not reuse a previously assigned field ID");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("schema field 1 cannot reuse a previously assigned field id".to_string())
    );

    let mut valid = current.clone();
    valid["last-column-id"] = serde_json::Value::from(2);
    valid["schemas"]
        .as_array_mut()
        .expect("schemas should be an array")
        .push(serde_json::json!({
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {"id": 1, "name": "renamed_id", "required": true, "type": "long"},
                {"id": 2, "name": "value", "required": false, "type": "string"}
            ]
        }));
    valid["current-schema-id"] = serde_json::Value::from(1);
    validate_table_metadata_transition(&current, &valid)
        .expect("renaming an existing field and allocating a new field ID must remain valid");
}

#[test]
fn iceberg_metadata_validation_enforces_identifier_field_contracts() {
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-column-id"] = serde_json::Value::from(10);
    metadata["schemas"][0]["fields"] = serde_json::json!([
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "optional_id", "required": false, "type": "string"},
        {"id": 3, "name": "float_id", "required": true, "type": "float"},
        {
            "id": 4,
            "name": "required_parent",
            "required": true,
            "type": {
                "type": "struct",
                "fields": [{"id": 5, "name": "nested_id", "required": true, "type": "string"}]
            }
        },
        {
            "id": 6,
            "name": "optional_parent",
            "required": false,
            "type": {
                "type": "struct",
                "fields": [{"id": 7, "name": "nested_id", "required": true, "type": "long"}]
            }
        },
        {
            "id": 8,
            "name": "ids",
            "required": true,
            "type": {"type": "list", "element-id": 9, "element-required": true, "element": "long"}
        }
    ]);
    metadata["schemas"][0]["identifier-field-ids"] = serde_json::json!([1, 5]);
    validate_supported_table_metadata(&metadata).expect("required primitive fields in required structs may identify rows");

    for invalid_id in [2, 3, 4, 7, 9] {
        let mut invalid = metadata.clone();
        invalid["schemas"][0]["identifier-field-ids"] = serde_json::json!([invalid_id]);
        validate_supported_table_metadata(&invalid)
            .expect_err("optional, floating, complex, collection, and optional-parent fields must not identify rows");
    }
}

#[test]
fn iceberg_metadata_validation_binds_partition_fields_to_schema_and_field_identity() {
    let mut missing_source = table_metadata_json_for_validation();
    missing_source["partition-specs"] = serde_json::json!([{
        "spec-id": 0,
        "fields": [{"source-id": 99, "field-id": 1000, "name": "missing", "transform": "identity"}]
    }]);
    missing_source["last-partition-id"] = serde_json::Value::from(1000);
    validate_supported_table_metadata(&missing_source).expect_err("partition source IDs must reference schema fields");

    let mut reassigned = table_metadata_json_for_validation();
    reassigned["last-column-id"] = serde_json::Value::from(2);
    reassigned["schemas"][0]["fields"] = serde_json::json!([
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "category", "required": false, "type": "string"}
    ]);
    reassigned["partition-specs"] = serde_json::json!([
        {
            "spec-id": 0,
            "fields": [{"source-id": 1, "field-id": 1000, "name": "id", "transform": "identity"}]
        },
        {
            "spec-id": 1,
            "fields": [{"source-id": 2, "field-id": 1000, "name": "category", "transform": "identity"}]
        }
    ]);
    reassigned["last-partition-id"] = serde_json::Value::from(1000);
    validate_supported_table_metadata(&reassigned)
        .expect_err("a partition field ID must not be reassigned to a different source or transform");

    reassigned["partition-specs"][1]["fields"][0] =
        serde_json::json!({"source-id": 1, "field-id": 1000, "name": "renamed_id", "transform": "identity"});
    validate_supported_table_metadata(&reassigned)
        .expect("a historical partition field may retain its ID when only its name changes");

    let mut v1_missing_source = serde_json::json!({
        "format-version": 1,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "last-updated-ms": 1,
        "last-column-id": 1,
        "schema": {
            "type": "struct",
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "partition-spec": [{"source-id": 2, "name": "missing", "transform": "identity"}]
    });
    validate_supported_table_metadata(&v1_missing_source)
        .expect_err("Iceberg v1 partition source IDs must reference schema fields");
    v1_missing_source["partition-spec"][0]["source-id"] = serde_json::Value::from(1);
    v1_missing_source["partition-spec"][0]["field-id"] = serde_json::Value::from(1001);
    validate_supported_table_metadata(&v1_missing_source)
        .expect_err("explicit Iceberg v1 partition field IDs must retain sequential compatibility IDs");

    let mut nested_source = table_metadata_json_for_validation();
    nested_source["last-column-id"] = serde_json::Value::from(4);
    nested_source["schemas"][0]["fields"] = serde_json::json!([
        {
            "id": 1,
            "name": "payload",
            "required": true,
            "type": {
                "type": "struct",
                "fields": [{"id": 2, "name": "event_date", "required": true, "type": "date"}]
            }
        },
        {
            "id": 3,
            "name": "dates",
            "required": true,
            "type": {"type": "list", "element-id": 4, "element-required": true, "element": "date"}
        }
    ]);
    nested_source["partition-specs"] = serde_json::json!([{
        "spec-id": 0,
        "fields": [{"source-id": 2, "field-id": 1000, "name": "event_day", "transform": "day"}]
    }]);
    nested_source["last-partition-id"] = serde_json::Value::from(1000);
    validate_supported_table_metadata(&nested_source).expect("a primitive nested in a struct may be a partition source");

    nested_source["partition-specs"][0]["fields"][0]["source-id"] = serde_json::Value::from(4);
    validate_supported_table_metadata(&nested_source).expect_err("a primitive nested in a list must not be a partition source");
}

#[test]
fn iceberg_metadata_validation_binds_defaults_to_current_schema() {
    let mut partitioned = table_metadata_json_for_validation();
    partitioned["schemas"]
        .as_array_mut()
        .expect("schemas should be an array")
        .push(serde_json::json!({"type": "struct", "schema-id": 1, "fields": []}));
    partitioned["current-schema-id"] = serde_json::Value::from(1);
    partitioned["partition-specs"] = serde_json::json!([
        {"spec-id": 0, "fields": []},
        {
            "spec-id": 1,
            "fields": [{"source-id": 1, "field-id": 1000, "name": "id", "transform": "identity"}]
        }
    ]);
    partitioned["default-spec-id"] = serde_json::Value::from(1);
    partitioned["last-partition-id"] = serde_json::Value::from(1000);
    validate_supported_table_metadata(&partitioned).expect_err("the default partition spec must bind to the current schema");
    partitioned["partition-specs"][1]["fields"][0]["transform"] = serde_json::Value::from("void");
    validate_supported_table_metadata(&partitioned)
        .expect("a void partition field may retain a source removed from the current schema");
    partitioned["partition-specs"][1]["fields"][0]["transform"] = serde_json::Value::from("identity");
    partitioned["default-spec-id"] = serde_json::Value::from(0);
    validate_supported_table_metadata(&partitioned)
        .expect("a non-default historical partition spec may retain a source removed from the current schema");

    let mut sorted = table_metadata_json_for_validation();
    sorted["schemas"]
        .as_array_mut()
        .expect("schemas should be an array")
        .push(serde_json::json!({"type": "struct", "schema-id": 1, "fields": []}));
    sorted["current-schema-id"] = serde_json::Value::from(1);
    sorted["sort-orders"] = serde_json::json!([
        {"order-id": 0, "fields": []},
        {
            "order-id": 1,
            "fields": [{
                "source-id": 1,
                "transform": "identity",
                "direction": "asc",
                "null-order": "nulls-first"
            }]
        }
    ]);
    sorted["default-sort-order-id"] = serde_json::Value::from(1);
    validate_supported_table_metadata(&sorted).expect_err("the default sort order must bind to the current schema");
    sorted["default-sort-order-id"] = serde_json::Value::from(0);
    validate_supported_table_metadata(&sorted)
        .expect("a non-default historical sort order may retain a source removed from the current schema");
}

#[test]
fn iceberg_metadata_validation_binds_transforms_to_source_types() {
    let mut valid = table_metadata_json_for_validation();
    valid["schemas"][0]["fields"][0]["type"] = serde_json::Value::from("date");
    valid["partition-specs"] = serde_json::json!([{
        "spec-id": 0,
        "fields": [{"source-id": 1, "field-id": 1000, "name": "day", "transform": "day"}]
    }]);
    valid["last-partition-id"] = serde_json::Value::from(1000);
    validate_supported_table_metadata(&valid).expect("day transforms may bind to date fields");

    let mut invalid_source = valid;
    invalid_source["schemas"][0]["fields"][0]["type"] = serde_json::Value::from("string");
    validate_supported_table_metadata(&invalid_source).expect_err("day transforms must reject string fields");

    let mut invalid_width = table_metadata_json_for_validation();
    invalid_width["partition-specs"] = serde_json::json!([{
        "spec-id": 0,
        "fields": [{"source-id": 1, "field-id": 1000, "name": "bucket", "transform": "bucket[0]"}]
    }]);
    invalid_width["last-partition-id"] = serde_json::Value::from(1000);
    validate_supported_table_metadata(&invalid_width).expect_err("bucket widths must be positive");
}

#[test]
fn iceberg_metadata_validation_enforces_sort_order_fields() {
    let mut metadata = table_metadata_json_for_validation();
    metadata["sort-orders"] = serde_json::json!([{
        "order-id": 1,
        "fields": [{
            "source-id": 1,
            "transform": "identity",
            "direction": "asc",
            "null-order": "nulls-first"
        }]
    }]);
    metadata["default-sort-order-id"] = serde_json::Value::from(1);
    validate_supported_table_metadata(&metadata).expect("complete sort order fields should validate");

    for (field, invalid_value) in [
        ("source-id", serde_json::Value::from(99)),
        ("transform", serde_json::Value::from("")),
        ("direction", serde_json::Value::from("ascending")),
        ("null-order", serde_json::Value::from("first")),
    ] {
        let mut invalid = metadata.clone();
        invalid["sort-orders"][0]["fields"][0][field] = invalid_value;
        validate_supported_table_metadata(&invalid).expect_err("invalid Iceberg sort fields must be rejected");
    }

    let mut reserved_unsorted = metadata;
    reserved_unsorted["sort-orders"][0]["order-id"] = serde_json::Value::from(0);
    reserved_unsorted["default-sort-order-id"] = serde_json::Value::from(0);
    validate_supported_table_metadata(&reserved_unsorted).expect_err("sort order 0 must remain unsorted");
}

#[test]
fn iceberg_metadata_validation_enforces_snapshot_and_ref_contracts() {
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(1);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
        "summary": {"operation": "append"},
        "schema-id": 0
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    validate_supported_table_metadata(&metadata).expect("complete snapshot and main ref should validate");

    metadata["snapshots"][0]["summary"]["operation"] = serde_json::Value::from("rewrite-manifests");
    validate_supported_table_metadata(&metadata).expect("Iceberg snapshot operations are extensible strings");

    let mut empty_operation = metadata.clone();
    empty_operation["snapshots"][0]["summary"]["operation"] = serde_json::Value::from("");
    validate_supported_table_metadata(&empty_operation).expect_err("snapshot operation must not be empty");

    let mut non_string_summary = metadata.clone();
    non_string_summary["snapshots"][0]["summary"]["added-records"] = serde_json::Value::from(1);
    validate_supported_table_metadata(&non_string_summary).expect_err("snapshot summary values must be strings");

    let mut missing_timestamp = metadata.clone();
    missing_timestamp["snapshots"][0]
        .as_object_mut()
        .expect("snapshot should be an object")
        .remove("timestamp-ms");
    validate_supported_table_metadata(&missing_timestamp).expect_err("snapshot timestamp must be required");

    let mut malformed_snapshot_log = metadata.clone();
    malformed_snapshot_log["snapshot-log"] = serde_json::json!([{"timestamp-ms": 1, "snapshot-id": "10"}]);
    validate_supported_table_metadata(&malformed_snapshot_log).expect_err("snapshot log entries must use integer snapshot IDs");

    let mut malformed_metadata_log = metadata.clone();
    malformed_metadata_log["metadata-log"] = serde_json::json!([{"timestamp-ms": 1, "metadata-file": ""}]);
    validate_supported_table_metadata(&malformed_metadata_log).expect_err("metadata log entries must identify a metadata file");

    let mut mismatched_main = metadata.clone();
    mismatched_main["snapshots"]
        .as_array_mut()
        .expect("snapshots should be an array")
        .push(serde_json::json!({
            "snapshot-id": 11,
            "sequence-number": 1,
            "timestamp-ms": 2,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
            "summary": {"operation": "append"}
        }));
    mismatched_main["refs"]["main"]["snapshot-id"] = serde_json::Value::from(11);
    validate_supported_table_metadata(&mismatched_main).expect_err("main ref must point to current-snapshot-id");

    let mut invalid_tag = metadata;
    invalid_tag["refs"]["release"] = serde_json::json!({
        "type": "tag",
        "snapshot-id": 10,
        "min-snapshots-to-keep": 2
    });
    validate_supported_table_metadata(&invalid_tag).expect_err("tags must reject branch-only retention fields");
}

#[test]
fn iceberg_metadata_validation_rejects_duplicate_snapshot_statistics() {
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(1);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
        "summary": {"operation": "append"}
    }]);
    let statistics = serde_json::json!({
        "snapshot-id": 10,
        "statistics-path": "s3://warehouse/tables/table-id/metadata/stats.puffin",
        "file-size-in-bytes": 1,
        "file-footer-size-in-bytes": 0,
        "blob-metadata": []
    });
    metadata["statistics"] = serde_json::json!([statistics.clone(), statistics]);

    let error = validate_supported_table_metadata(&metadata).expect_err("a snapshot must not have duplicate statistics entries");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("statistics contains duplicate entries for snapshot 10".to_string())
    );
}

#[tokio::test]
async fn snapshot_validation_bounds_changed_statistics_object_fanout() {
    let backend = TestCatalogObjectBackend::default();
    let mut metadata = table_metadata_json_for_validation();
    let object_count = TABLE_COMMIT_MAX_STATISTICS_OBJECTS + 1;
    metadata["last-sequence-number"] = serde_json::Value::from(object_count);
    metadata["snapshots"] = serde_json::Value::Array(
        (1..=object_count)
            .map(|snapshot_id| {
                serde_json::json!({
                    "snapshot-id": snapshot_id,
                    "sequence-number": snapshot_id,
                    "timestamp-ms": snapshot_id,
                    "manifest-list": format!("s3://warehouse/tables/table-id/metadata/snap-{snapshot_id}.avro"),
                    "summary": {"operation": "append"}
                })
            })
            .collect(),
    );
    metadata["partition-statistics"] = serde_json::Value::Array(
        (1..=object_count)
            .map(|snapshot_id| {
                serde_json::json!({
                    "snapshot-id": snapshot_id,
                    "statistics-path": format!(
                        "s3://warehouse/tables/table-id/metadata/partition-stats-{snapshot_id}.parquet"
                    ),
                    "file-size-in-bytes": 1
                })
            })
            .collect(),
    );
    let entry = TableEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "warehouse".to_string(),
        namespace: "analytics".to_string(),
        table: "events".to_string(),
        table_id: "table-id".to_string(),
        table_uuid: "table-uuid".to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: "s3://warehouse/tables/table-id".to_string(),
        metadata_location: "metadata/00001.metadata.json".to_string(),
        version_token: "token-v1".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect_err("statistics object fanout must be bounded before storage lookups");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("statistics object count exceeds the commit limit".to_string())
    );
}

#[tokio::test]
async fn snapshot_validation_bounds_changed_statistics_object_bytes() {
    let backend = TestCatalogObjectBackend::default();
    let mut metadata = table_metadata_json_for_validation();
    let object_count = TABLE_COMMIT_MAX_STATISTICS_BYTES / TABLE_STATISTICS_FILE_MAX_SIZE + 1;
    metadata["last-sequence-number"] = serde_json::Value::from(object_count);
    metadata["snapshots"] = serde_json::Value::Array(
        (1..=object_count)
            .map(|snapshot_id| {
                serde_json::json!({
                    "snapshot-id": snapshot_id,
                    "sequence-number": snapshot_id,
                    "timestamp-ms": snapshot_id,
                    "manifest-list": format!("s3://warehouse/tables/table-id/metadata/snap-{snapshot_id}.avro"),
                    "summary": {"operation": "append"}
                })
            })
            .collect(),
    );
    metadata["partition-statistics"] = serde_json::Value::Array(
        (1..=object_count)
            .map(|snapshot_id| {
                serde_json::json!({
                    "snapshot-id": snapshot_id,
                    "statistics-path": format!(
                        "s3://warehouse/tables/table-id/metadata/partition-stats-{snapshot_id}.parquet"
                    ),
                    "file-size-in-bytes": TABLE_STATISTICS_FILE_MAX_SIZE
                })
            })
            .collect(),
    );
    let entry = TableEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "warehouse".to_string(),
        namespace: "analytics".to_string(),
        table: "events".to_string(),
        table_id: "table-id".to_string(),
        table_uuid: "table-uuid".to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: "s3://warehouse/tables/table-id".to_string(),
        metadata_location: "metadata/00001.metadata.json".to_string(),
        version_token: "token-v1".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect_err("statistics bytes must be bounded before storage lookups");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("statistics bytes exceed the commit validation limit".to_string())
    );
}

#[tokio::test]
async fn snapshot_validation_rechecks_retained_statistics_locations() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let mut current = table_metadata_json_for_validation();
    current["last-sequence-number"] = serde_json::Value::from(1);
    current["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/missing-history.avro",
        "summary": {"operation": "append"}
    }]);
    current["current-snapshot-id"] = serde_json::Value::from(10);
    current["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    current["statistics"] = serde_json::json!([{
        "snapshot-id": 10,
        "statistics-path": "s3://warehouse/tables/another-table/metadata/stats.puffin",
        "file-size-in-bytes": 1,
        "file-footer-size-in-bytes": 0,
        "blob-metadata": []
    }]);
    let target = current.clone();
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, Some(&current), &target)
        .await
        .expect_err("retained statistics must remain inside the table warehouse");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("statistics object is outside the table warehouse".to_string())
    );
}

#[tokio::test]
async fn snapshot_validation_rejects_non_puffin_table_statistics() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let mut current = table_metadata_json_for_validation();
    current["last-sequence-number"] = serde_json::Value::from(1);
    current["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
        "summary": {"operation": "append"}
    }]);
    current["current-snapshot-id"] = serde_json::Value::from(10);
    current["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let mut target = current.clone();
    let statistics = b"notpuffin".to_vec();
    target["statistics"] = serde_json::json!([{
        "snapshot-id": 10,
        "statistics-path": "s3://warehouse/tables/table-id/metadata/stats.puffin",
        "file-size-in-bytes": statistics.len(),
        "file-footer-size-in-bytes": 0,
        "blob-metadata": []
    }]);
    backend
        .seed_object("warehouse", "tables/table-id/metadata/stats.puffin", statistics)
        .await;
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, Some(&current), &target)
        .await
        .expect_err("table statistics must be a Puffin file");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("table statistics object is not a Puffin file".to_string())
    );
}

#[tokio::test]
async fn snapshot_validation_rejects_non_parquet_partition_statistics() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let mut current = table_metadata_json_for_validation();
    current["last-sequence-number"] = serde_json::Value::from(1);
    current["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
        "summary": {"operation": "append"}
    }]);
    current["current-snapshot-id"] = serde_json::Value::from(10);
    current["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let mut target = current.clone();
    let statistics = b"notparquet".to_vec();
    target["partition-statistics"] = serde_json::json!([{
        "snapshot-id": 10,
        "statistics-path": "s3://warehouse/tables/table-id/metadata/partition-stats.parquet",
        "file-size-in-bytes": statistics.len()
    }]);
    backend
        .seed_object("warehouse", "tables/table-id/metadata/partition-stats.parquet", statistics)
        .await;
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, Some(&current), &target)
        .await
        .expect_err("partition statistics must be a Parquet file");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("partition statistics object is not a Parquet file".to_string())
    );
}

#[tokio::test]
async fn snapshot_validation_rejects_statistics_size_mismatch() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let mut current = table_metadata_json_for_validation();
    current["last-sequence-number"] = serde_json::Value::from(1);
    current["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
        "summary": {"operation": "append"}
    }]);
    current["current-snapshot-id"] = serde_json::Value::from(10);
    current["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let mut target = current.clone();
    let statistics = b"PFA1PFA1".to_vec();
    target["statistics"] = serde_json::json!([{
        "snapshot-id": 10,
        "statistics-path": "s3://warehouse/tables/table-id/metadata/stats.puffin",
        "file-size-in-bytes": statistics.len() + 1,
        "file-footer-size-in-bytes": 0,
        "blob-metadata": []
    }]);
    backend
        .seed_object("warehouse", "tables/table-id/metadata/stats.puffin", statistics)
        .await;
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, Some(&current), &target)
        .await
        .expect_err("statistics lengths must match the published object");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("statistics file-size-in-bytes does not match the object".to_string())
    );
}

#[test]
fn iceberg_metadata_validation_bounds_snapshot_sequence_numbers() {
    let mut v2 = table_metadata_json_for_validation();
    v2["last-sequence-number"] = serde_json::Value::from(1);
    v2["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 2,
        "timestamp-ms": 1,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
        "summary": {"operation": "append"}
    }]);
    let error = validate_supported_table_metadata(&v2).expect_err("a v2 snapshot sequence must not exceed the table sequence");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid(
            "Iceberg v2 snapshot sequence-number must be between zero and last-sequence-number".to_string()
        )
    );
    v2["last-sequence-number"] = serde_json::Value::from(0);
    v2["snapshots"][0]
        .as_object_mut()
        .expect("snapshot should be an object")
        .remove("sequence-number");
    let error = validate_supported_table_metadata(&v2).expect_err("Iceberg v2 snapshots must include sequence-number");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("Iceberg v2 snapshot sequence-number is required".to_string())
    );

    let mut v1 = serde_json::json!({
        "format-version": 1,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "last-updated-ms": 1,
        "last-column-id": 1,
        "schema": {"type": "struct", "fields": []},
        "partition-spec": [],
        "snapshots": [{
            "snapshot-id": 10,
            "sequence-number": 1,
            "timestamp-ms": 1,
            "manifests": []
        }]
    });
    let error = validate_supported_table_metadata(&v1).expect_err("v1 metadata must not carry a non-zero snapshot sequence");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("Iceberg v1 snapshot sequence-number must be zero when present".to_string())
    );
    v1["snapshots"][0]["sequence-number"] = serde_json::Value::from(0);
    validate_supported_table_metadata(&v1).expect("a zero v1 compatibility sequence should validate");
    v1["snapshots"][0]
        .as_object_mut()
        .expect("snapshot should be an object")
        .remove("sequence-number");
    validate_supported_table_metadata(&v1).expect("Iceberg v1 snapshots may omit sequence-number");
}

#[test]
fn iceberg_metadata_decoder_accepts_both_gzip_file_name_conventions() {
    use std::io::Write;

    let metadata = table_metadata_json_for_validation();
    let data = serde_json::to_vec(&metadata).expect("metadata should serialize");
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&data).expect("metadata should compress");
    let compressed = encoder.finish().expect("gzip stream should finish");

    for metadata_location in ["v1.gz.metadata.json", "v1.metadata.json.gz"] {
        let decoded = decode_table_metadata_json(metadata_location, &compressed)
            .expect("Iceberg gzip metadata naming conventions should decode");
        assert_eq!(decoded, metadata);
    }
}

#[test]
fn metadata_log_locations_normalize_same_bucket_s3_uris() {
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let metadata_location = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let metadata = serde_json::json!({
        "metadata-log": [
            {"timestamp-ms": 1, "metadata-file": format!("s3://warehouse/{metadata_location}")},
            {"timestamp-ms": 2, "metadata-file": format!("s3://other/{metadata_location}")}
        ]
    });

    assert_eq!(
        metadata_log_locations(&metadata, "warehouse", &namespace, &table),
        BTreeSet::from([metadata_location])
    );
}

#[test]
fn iceberg_metadata_version_synchronization_builds_complete_v2_shape() {
    let mut metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "last-updated-ms": 1,
        "last-column-id": 1,
        "schema": {
            "type": "struct",
            "schema-id": 7,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "partition-spec": [],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": []
    });

    synchronize_table_metadata_version_fields(&mut metadata).expect("v1 fields should synchronize to v2");
    validate_supported_table_metadata(&metadata).expect("synchronized metadata should satisfy the v2 contract");
    assert_eq!(metadata["current-schema-id"], 7);
    assert_eq!(metadata["default-spec-id"], 0);
    assert_eq!(metadata["default-sort-order-id"], 0);
    assert_eq!(metadata["last-sequence-number"], 0);
    assert!(metadata.get("schema").is_none());
    assert!(metadata.get("partition-spec").is_none());
}

#[test]
fn iceberg_manifest_validation_accepts_standard_codecs_and_rejects_unknown_content() {
    let schema = apache_avro::Schema::parse_str(
        r#"
        {
          "type": "record",
          "name": "manifest_file",
          "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "partition_spec_id", "type": "int"}
          ]
        }
        "#,
    )
    .expect("manifest list schema should parse");
    for (label, codec) in [
        ("null", apache_avro::Codec::Null),
        ("deflate", apache_avro::Codec::Deflate(Default::default())),
        ("snappy", apache_avro::Codec::Snappy),
        ("zstandard", apache_avro::Codec::Zstandard(Default::default())),
    ] {
        let mut writer = apache_avro::Writer::with_codec(&schema, Vec::new(), codec)
            .expect("compressed manifest list writer should initialize");
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                (
                    "manifest_path".to_string(),
                    apache_avro::types::Value::String("s3://warehouse/tables/table-id/metadata/manifest.avro".to_string()),
                ),
                ("partition_spec_id".to_string(), apache_avro::types::Value::Int(0)),
            ]))
            .expect("compressed manifest list record should append");
        let compressed = writer.into_inner().expect("compressed manifest list should flush");
        let references = manifest_list_references_from_manifest_list_avro(&compressed)
            .unwrap_or_else(|error| panic!("{label}-compressed manifest lists should be supported: {error}"));
        assert_eq!(references.len(), 1);
        assert_eq!(references[0].manifest_path, "s3://warehouse/tables/table-id/metadata/manifest.avro");
    }

    let unknown_content = manifest_avro_bytes_with_status(&[("s3://warehouse/tables/table-id/data/part.parquet", 3, 1)]);
    let error = data_file_references_from_manifest_avro(&unknown_content)
        .expect_err("unknown Iceberg manifest content values must be rejected");
    assert!(matches!(error, TableCatalogStoreError::Invalid(_)));
}

#[test]
fn apache_avro_021_iceberg_manifest_fixtures_remain_readable() {
    // Fixed 0.21 output prevents this compatibility check from becoming a current-version round trip.
    let manifest_list = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/table_catalog/apache-avro-0.21-manifest-list.avro"
    ));
    let manifest_list = decode_manifest_list_avro(manifest_list).expect("apache-avro 0.21 manifest list should remain readable");
    assert_eq!(manifest_list.references.len(), 1);
    let manifest_list_reference = &manifest_list.references[0];
    assert_eq!(manifest_list_reference.format_version, 2);
    assert_eq!(
        manifest_list_reference.manifest_path,
        "s3://warehouse/tables/table-id/metadata/manifest-021.avro"
    );
    assert_eq!(manifest_list_reference.manifest_length, Some(4096));
    assert_eq!(manifest_list_reference.partition_spec_id, Some(3));
    assert_eq!(manifest_list_reference.sequence_number, Some(9));
    assert_eq!(manifest_list_reference.min_sequence_number, Some(8));
    assert_eq!(manifest_list_reference.added_snapshot_id, Some(101));

    let manifest = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/table_catalog/apache-avro-0.21-manifest.avro"
    ));
    let manifest = decode_manifest_avro(manifest).expect("apache-avro 0.21 manifest should remain readable");
    assert_eq!(manifest.references.len(), 1);
    let data_file = &manifest.references[0];
    assert_eq!(data_file.format_version, 2);
    assert_eq!(data_file.location, "s3://warehouse/tables/table-id/data/part-021.parquet");
    assert_eq!(data_file.snapshot_id, Some(101));
    assert_eq!(data_file.sequence_number, Some(9));
    assert_eq!(data_file.file_sequence_number, Some(9));
    assert_eq!(data_file.record_count, Some(10));
    assert_eq!(data_file.file_size_bytes, Some(1024));
    assert_eq!(data_file.sort_order_id, Some(7));
    assert_eq!(data_file.partition, vec![("dt".to_string(), apache_avro::types::Value::Date(20_312))]);
}

#[tokio::test]
async fn iceberg_snapshot_graph_rejects_unknown_partition_specs() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = TableEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "warehouse".to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: "table-id".to_string(),
        table_uuid: "table-uuid".to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: "s3://warehouse/tables/table-id".to_string(),
        metadata_location: "tables/table-id/metadata/v1.metadata.json".to_string(),
        version_token: "token-v1".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    let manifest_list_location = "s3://warehouse/tables/table-id/metadata/snap-10.avro";
    let manifest_location = "s3://warehouse/tables/table-id/metadata/manifest-10.avro";
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/snap-10.avro",
            manifest_list_avro_bytes_with_spec(&[(manifest_location, 1)], 7),
        )
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list_location,
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect_err("manifest partition specs absent from table metadata must be rejected");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("snapshot manifest references missing partition spec 7".to_string())
    );
}

#[tokio::test]
async fn iceberg_snapshot_graph_accepts_null_manifest_list_counts() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list_location = "s3://warehouse/tables/table-id/metadata/snap-10.avro";
    let manifest_location = "s3://warehouse/tables/table-id/metadata/manifest-10.avro";
    let manifest = manifest_avro_bytes(&[]);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/snap-10.avro",
            manifest_list_avro_bytes_with_spec_and_null_counts(&[(manifest_location, manifest.len())], 0, true),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/manifest-10.avro", manifest)
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list_location,
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect("nullable v2 manifest-list counts must remain compatible");
}

#[tokio::test]
async fn iceberg_snapshot_graph_revalidates_unchanged_snapshots_after_spec_removal() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list_location = "s3://warehouse/tables/table-id/metadata/snap-10.avro";
    let manifest_location = "s3://warehouse/tables/table-id/metadata/manifest-10.avro";
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/snap-10.avro",
            manifest_list_avro_bytes_with_spec(&[(manifest_location, 1)], 7),
        )
        .await;
    let mut current = table_metadata_json_for_validation();
    current["partition-specs"]
        .as_array_mut()
        .expect("partition specs should be an array")
        .push(serde_json::json!({"spec-id": 7, "fields": []}));
    current["last-sequence-number"] = serde_json::Value::from(7);
    current["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list_location,
        "summary": {"operation": "append"}
    }]);
    current["current-snapshot-id"] = serde_json::Value::from(10);
    current["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let mut target = current.clone();
    target["partition-specs"]
        .as_array_mut()
        .expect("partition specs should be an array")
        .retain(|spec| spec.get("spec-id").and_then(serde_json::Value::as_i64) != Some(7));
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, Some(&current), &target)
        .await
        .expect_err("removing a spec referenced by an unchanged snapshot must fail");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("snapshot manifest references missing partition spec 7".to_string())
    );
}

#[tokio::test]
async fn iceberg_snapshot_graph_skips_unchanged_history_after_spec_addition() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let mut current = table_metadata_json_for_validation();
    current["last-sequence-number"] = serde_json::Value::from(1);
    current["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/missing-history.avro",
        "summary": {"operation": "append"}
    }]);
    current["current-snapshot-id"] = serde_json::Value::from(10);
    current["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let mut target = current.clone();
    target["partition-specs"]
        .as_array_mut()
        .expect("partition specs should be an array")
        .push(serde_json::json!({"spec-id": 7, "fields": []}));
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, Some(&current), &target)
        .await
        .expect("adding a partition spec must not reread unchanged snapshot history");
}

#[tokio::test]
async fn iceberg_snapshot_graph_allows_missing_deleted_files() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list_location = "s3://warehouse/tables/table-id/metadata/snap-10.avro";
    let manifest_location = "s3://warehouse/tables/table-id/metadata/manifest-10.avro";
    let deleted_data_location = "s3://warehouse/tables/table-id/data/deleted.parquet";
    let manifest_bytes = manifest_avro_bytes_with_status(&[(deleted_data_location, 0, 2)]);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/snap-10.avro",
            manifest_list_avro_bytes(&[(manifest_location, manifest_bytes.len())]),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/manifest-10.avro", manifest_bytes)
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list_location,
        "summary": {"operation": "delete"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect("deleted manifest entries may reference files that have already been removed");
}

#[tokio::test]
async fn iceberg_snapshot_graph_accepts_empty_manifest_lists() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list_location = "s3://warehouse/tables/table-id/metadata/snap-empty.avro";
    backend
        .seed_object("warehouse", "tables/table-id/metadata/snap-empty.avro", manifest_list_avro_bytes(&[]))
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list_location,
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect("an empty Iceberg snapshot may have an empty manifest list");
}

#[tokio::test]
async fn iceberg_v2_snapshot_graph_accepts_reused_v1_manifests() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list_location = "s3://warehouse/tables/table-id/metadata/snap-v1.avro";
    let manifest_location = "s3://warehouse/tables/table-id/metadata/manifest-v1.avro";
    let data_location = "s3://warehouse/tables/table-id/data/v1.parquet";
    let manifest_bytes = v1_manifest_avro_bytes(data_location);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/snap-v1.avro",
            v1_manifest_list_avro_bytes(manifest_location, manifest_bytes.len()),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/manifest-v1.avro", manifest_bytes)
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/data/v1.parquet", vec![1])
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list_location,
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect("v2 tables may retain v1 manifest lists and manifests after upgrade");
}

#[tokio::test]
async fn iceberg_snapshot_change_validation_skips_unchanged_history() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let historical_manifest_list = "s3://warehouse/tables/table-id/metadata/snap-10.avro";
    let new_manifest_list = "s3://warehouse/tables/table-id/metadata/snap-11.avro";
    backend
        .seed_object("warehouse", "tables/table-id/metadata/snap-11.avro", manifest_list_avro_bytes(&[]))
        .await;

    let mut current_metadata = table_metadata_json_for_validation();
    current_metadata["last-sequence-number"] = serde_json::Value::from(7);
    current_metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": historical_manifest_list,
        "summary": {"operation": "append"}
    }]);
    current_metadata["current-snapshot-id"] = serde_json::Value::from(10);
    current_metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let mut next_metadata = current_metadata.clone();
    next_metadata
        .get_mut("snapshots")
        .and_then(serde_json::Value::as_array_mut)
        .expect("snapshots should be an array")
        .push(serde_json::json!({
            "snapshot-id": 11,
            "sequence-number": 7,
            "timestamp-ms": 2,
            "manifest-list": new_manifest_list,
            "summary": {"operation": "append"}
        }));
    next_metadata["current-snapshot-id"] = serde_json::Value::from(11);
    next_metadata["refs"]["main"]["snapshot-id"] = serde_json::Value::from(11);
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, Some(&current_metadata), &next_metadata)
        .await
        .expect("an unchanged historical snapshot must not be reread during commit validation");
}

#[tokio::test]
async fn iceberg_snapshot_registration_validates_all_retained_snapshots() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    backend
        .seed_object("warehouse", "tables/table-id/metadata/snap-11.avro", manifest_list_avro_bytes(&[]))
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([
        {
            "snapshot-id": 10,
            "sequence-number": 7,
            "timestamp-ms": 1,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/missing-history.avro",
            "summary": {"operation": "append"}
        },
        {
            "snapshot-id": 11,
            "sequence-number": 7,
            "timestamp-ms": 2,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
            "summary": {"operation": "append"}
        }
    ]);
    metadata["current-snapshot-id"] = serde_json::Value::from(11);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 11}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect_err("registration must validate every retained snapshot");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("snapshot manifest-list object is missing".to_string())
    );
}

#[tokio::test]
async fn iceberg_snapshot_graph_counts_shared_manifests_once() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list = "s3://warehouse/tables/table-id/metadata/shared-list.avro";
    let manifest = "s3://warehouse/tables/table-id/metadata/shared-manifest.avro";
    let data_file = "s3://warehouse/tables/table-id/data/shared.parquet";
    let manifest_bytes = manifest_avro_bytes(&[(data_file, 0)]);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/shared-list.avro",
            manifest_list_avro_bytes(&[(manifest, manifest_bytes.len())]),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/shared-manifest.avro", manifest_bytes)
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/data/shared.parquet", vec![1])
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::Value::Array(
        (0..=TABLE_COMMIT_MAX_MANIFESTS)
            .map(|index| {
                let snapshot_id = i64::try_from(index + 1).expect("snapshot id should fit in i64");
                serde_json::json!({
                    "snapshot-id": snapshot_id,
                    "sequence-number": 7,
                    "timestamp-ms": snapshot_id,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
                })
            })
            .collect(),
    );
    let current_snapshot_id = i64::try_from(TABLE_COMMIT_MAX_MANIFESTS + 1).expect("snapshot id should fit in i64");
    metadata["current-snapshot-id"] = serde_json::Value::from(current_snapshot_id);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": current_snapshot_id}});
    let current_metadata = table_metadata_json_for_validation();
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, Some(&current_metadata), &metadata)
        .await
        .expect("shared manifest objects must consume the commit budget only once");
}

#[tokio::test]
async fn iceberg_snapshot_graph_revalidates_cached_manifest_declarations() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest = "s3://warehouse/tables/table-id/metadata/shared-manifest.avro";
    let data_file = "s3://warehouse/tables/table-id/data/shared.parquet";
    let manifest_bytes = manifest_avro_bytes_with_status_and_partition_spec(&[(data_file, 0, 1)], 0);
    let manifest_length = manifest_bytes.len();
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/list-10.avro",
            manifest_list_avro_bytes(&[(manifest, manifest_length)]),
        )
        .await;
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/list-11.avro",
            manifest_list_avro_bytes(&[(manifest, manifest_length + 1)]),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/shared-manifest.avro", manifest_bytes)
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/data/shared.parquet", vec![1])
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([
        {
            "snapshot-id": 10,
            "sequence-number": 7,
            "timestamp-ms": 1,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/list-10.avro",
            "summary": {"operation": "append"}
        },
        {
            "snapshot-id": 11,
            "sequence-number": 7,
            "timestamp-ms": 2,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/list-11.avro",
            "summary": {"operation": "append"}
        }
    ]);
    metadata["current-snapshot-id"] = serde_json::Value::from(11);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 11}});
    let current_metadata = table_metadata_json_for_validation();
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, Some(&current_metadata), &metadata)
        .await
        .expect_err("every manifest-list declaration must match the cached manifest object");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("manifest-list manifest_length does not match the manifest object".to_string())
    );

    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/list-11.avro",
            manifest_list_avro_bytes_with_spec(&[(manifest, manifest_length)], 1),
        )
        .await;
    metadata["partition-specs"]
        .as_array_mut()
        .expect("partition specs should be an array")
        .push(serde_json::json!({"spec-id": 1, "fields": []}));

    let error = validate_table_snapshot_changes(&context, Some(&current_metadata), &metadata)
        .await
        .expect_err("every cached manifest must match each manifest-list partition spec declaration");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("manifest partition-spec-id does not match its manifest-list entry".to_string())
    );
}

#[tokio::test]
async fn iceberg_snapshot_graph_bounds_shared_manifest_traversals() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list = "s3://warehouse/tables/table-id/metadata/shared-list.avro";
    let manifest = "s3://warehouse/tables/table-id/metadata/shared-manifest.avro";
    let data_file = "s3://warehouse/tables/table-id/data/shared.parquet";
    let manifest_bytes = manifest_avro_bytes(&[(data_file, 0)]);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/shared-list.avro",
            manifest_list_avro_bytes(&[(manifest, manifest_bytes.len())]),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/shared-manifest.avro", manifest_bytes)
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/data/shared.parquet", vec![1])
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::Value::Array(
        (0..=TABLE_COMMIT_MAX_MANIFEST_TRAVERSALS)
            .map(|index| {
                let snapshot_id = i64::try_from(index + 1).expect("snapshot id should fit in i64");
                serde_json::json!({
                    "snapshot-id": snapshot_id,
                    "sequence-number": 7,
                    "timestamp-ms": snapshot_id,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
                })
            })
            .collect(),
    );
    let current_snapshot_id = i64::try_from(TABLE_COMMIT_MAX_MANIFEST_TRAVERSALS + 1).expect("snapshot id should fit in i64");
    metadata["current-snapshot-id"] = serde_json::Value::from(current_snapshot_id);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": current_snapshot_id}});
    let current_metadata = table_metadata_json_for_validation();
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, Some(&current_metadata), &metadata)
        .await
        .expect_err("logical manifest traversals must remain bounded across shared snapshots");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("snapshot manifest traversal count exceeds the commit limit".to_string())
    );
}

#[tokio::test]
async fn iceberg_snapshot_graph_accepts_more_than_ten_thousand_live_files() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list = "s3://warehouse/tables/table-id/metadata/boundary-list.avro";
    let manifest = "s3://warehouse/tables/table-id/metadata/boundary-manifest.avro";
    let data_file_count = 10_001;
    let data_files = (0..data_file_count)
        .map(|index| format!("s3://warehouse/tables/table-id/data/part-{index:05}.parquet"))
        .collect::<Vec<_>>();
    for data_file in &data_files {
        backend
            .seed_object(
                "warehouse",
                data_file
                    .strip_prefix("s3://warehouse/")
                    .expect("test data location should be in warehouse"),
                vec![1],
            )
            .await;
    }
    let references = data_files.iter().map(|data_file| (data_file.as_str(), 0)).collect::<Vec<_>>();
    let manifest_bytes = manifest_avro_bytes(&references);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/boundary-list.avro",
            manifest_list_avro_bytes(&[(manifest, manifest_bytes.len())]),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/boundary-manifest.avro", manifest_bytes)
        .await;

    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 20,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list,
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(20);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 20}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect("a valid graph with more than ten thousand live files should validate");
}

#[tokio::test]
async fn iceberg_v2_snapshot_graph_rejects_embedded_v2_manifests() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest = "s3://warehouse/tables/table-id/metadata/embedded.avro";
    let data_file = "s3://warehouse/tables/table-id/data/embedded.parquet";
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/embedded.avro",
            manifest_avro_bytes(&[(data_file, 0)]),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/data/embedded.parquet", vec![1])
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifests": [manifest],
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect_err("new v2 snapshots must use a manifest list");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("new Iceberg v2 snapshots require manifest-list".to_string())
    );
}

#[tokio::test]
async fn iceberg_snapshot_graph_rejects_delete_files_in_data_manifest() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list = "s3://warehouse/tables/table-id/metadata/list-10.avro";
    let manifest = "s3://warehouse/tables/table-id/metadata/snap-delete-manifest.avro";
    let delete_file = "s3://warehouse/tables/table-id/data/position-deletes.parquet";
    let manifest_bytes = manifest_avro_bytes(&[(delete_file, 1)]);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/list-10.avro",
            manifest_list_avro_bytes(&[(manifest, manifest_bytes.len())]),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/snap-delete-manifest.avro", manifest_bytes)
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/data/position-deletes.parquet", vec![1])
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list,
        "summary": {"operation": "delete"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect_err("a data manifest must not contain delete files");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("manifest-list content does not match manifest file content".to_string())
    );
}

#[tokio::test]
async fn iceberg_snapshot_graph_accepts_delete_files_in_delete_manifest() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list = "s3://warehouse/tables/table-id/metadata/list-10.avro";
    let manifest = "s3://warehouse/tables/table-id/metadata/snap-delete-manifest.avro";
    let delete_file = "s3://warehouse/tables/table-id/data/position-deletes.parquet";
    let manifest_bytes = manifest_avro_bytes(&[(delete_file, 1)]);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/list-10.avro",
            manifest_list_avro_bytes_with_spec_and_content(&[(manifest, manifest_bytes.len())], 0, 1),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/snap-delete-manifest.avro", manifest_bytes)
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/data/position-deletes.parquet", vec![1])
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list,
        "summary": {"operation": "delete"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect("a delete manifest may contain delete files regardless of their object directory");
}

#[tokio::test]
async fn iceberg_snapshot_graph_rejects_manifest_length_mismatch() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list = "s3://warehouse/tables/table-id/metadata/list-20.avro";
    let manifest = "s3://warehouse/tables/table-id/metadata/manifest-20.avro";
    let data_file = "s3://warehouse/tables/table-id/data/part-20.parquet";
    let manifest_bytes = manifest_avro_bytes(&[(data_file, 0)]);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/list-20.avro",
            manifest_list_avro_bytes(&[(manifest, manifest_bytes.len() + 1)]),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/manifest-20.avro", manifest_bytes)
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/data/part-20.parquet", vec![1])
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 20,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list,
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(20);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 20}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect_err("manifest-list lengths must match the published manifest object");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("manifest-list manifest_length does not match the manifest object".to_string())
    );
}

#[tokio::test]
async fn iceberg_snapshot_graph_rejects_manifest_partition_spec_mismatch() {
    let backend = TestCatalogObjectBackend::default();
    let namespace = Namespace::parse("analytics").expect("namespace should parse");
    let table = IdentifierSegment::parse("events").expect("table should parse");
    let entry = test_table_entry("warehouse", &namespace, &table, "tables/table-id/metadata/v1.metadata.json".to_string());
    let manifest_list = "s3://warehouse/tables/table-id/metadata/list-20.avro";
    let manifest = "s3://warehouse/tables/table-id/metadata/manifest-20.avro";
    let data_file = "s3://warehouse/tables/table-id/data/part-20.parquet";
    let manifest_bytes = manifest_avro_bytes_with_status_and_partition_spec(&[(data_file, 0, 1)], 7);
    backend
        .seed_object(
            "warehouse",
            "tables/table-id/metadata/list-20.avro",
            manifest_list_avro_bytes(&[(manifest, manifest_bytes.len())]),
        )
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/metadata/manifest-20.avro", manifest_bytes)
        .await;
    backend
        .seed_object("warehouse", "tables/table-id/data/part-20.parquet", vec![1])
        .await;
    let mut metadata = table_metadata_json_for_validation();
    metadata["last-sequence-number"] = serde_json::Value::from(7);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 20,
        "sequence-number": 7,
        "timestamp-ms": 1,
        "manifest-list": manifest_list,
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(20);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 20}});
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    let error = validate_table_snapshot_changes(&context, None, &metadata)
        .await
        .expect_err("manifest headers must agree with their manifest-list entry");
    assert_eq!(
        error,
        TableCatalogStoreError::Invalid("manifest partition-spec-id does not match its manifest-list entry".to_string())
    );
}

#[tokio::test]
async fn catalog_object_limited_reads_reject_oversized_results() {
    let backend = TestCatalogObjectBackend::default();
    backend.seed_object("warehouse", "metadata.json", vec![0; 5]).await;

    let error = backend
        .read_object_limited("warehouse", "metadata.json", 4)
        .await
        .expect_err("bounded catalog reads must reject oversized objects");
    assert!(matches!(error, TableCatalogStoreError::Invalid(_)));
}

fn manifest_avro_bytes(files: &[(&str, i32)]) -> Vec<u8> {
    manifest_avro_bytes_with_status(
        &files
            .iter()
            .map(|(file_path, content)| (*file_path, *content, 1))
            .collect::<Vec<_>>(),
    )
}

fn manifest_avro_bytes_with_status(files: &[(&str, i32, i32)]) -> Vec<u8> {
    let files = files
        .iter()
        .map(|(path, content, status)| (*path, *content, *status, 20_i64, 7_i64))
        .collect::<Vec<_>>();
    crate::table_catalog::test_support::manifest_avro_bytes(&files)
}

fn manifest_avro_bytes_with_status_and_partition_spec(files: &[(&str, i32, i32)], partition_spec_id: i32) -> Vec<u8> {
    let files = files
        .iter()
        .map(|(path, content, status)| (*path, *content, *status, 20_i64, 7_i64))
        .collect::<Vec<_>>();
    crate::table_catalog::test_support::manifest_avro_bytes_with_partition_spec(&files, Some(partition_spec_id))
}

fn manifest_avro_bytes_with_dt_partition(files: &[(&str, i32, &str)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
            {
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": "long"},
                {"name": "sequence_number", "type": "long"},
                {"name": "file_sequence_number", "type": "long"},
                {
                  "name": "data_file",
                  "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                      {"name": "content", "type": "int"},
                      {"name": "file_path", "type": "string"},
                      {"name": "partition", "type": {"type": "record", "name": "partition", "fields": [
                        {"name": "dt", "type": "string"}
                      ]}},
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"}
                    ]
                  }
                }
              ]
            }
            "#,
    )
    .expect("partitioned manifest avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("partitioned manifest writer should initialize");
    writer
        .add_user_metadata("partition-spec-id".to_string(), "0")
        .expect("manifest partition spec metadata should write");
    for (file_path, content, partition_value) in files {
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(1)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(20)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(7)),
                ("file_sequence_number".to_string(), apache_avro::types::Value::Long(7)),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(*content)),
                        ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                        (
                            "partition".to_string(),
                            apache_avro::types::Value::Record(vec![(
                                "dt".to_string(),
                                apache_avro::types::Value::String((*partition_value).to_string()),
                            )]),
                        ),
                        ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                        ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                    ]),
                ),
            ]))
            .expect("partitioned manifest record should append");
    }
    writer.into_inner().expect("partitioned manifest avro bytes should flush")
}

fn manifest_avro_bytes_with_sort_order(files: &[(&str, i32, i32)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
            {
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": "long"},
                {"name": "sequence_number", "type": "long"},
                {"name": "file_sequence_number", "type": "long"},
                {
                  "name": "data_file",
                  "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                      {"name": "content", "type": "int"},
                      {"name": "file_path", "type": "string"},
                      {"name": "partition", "type": {"type": "record", "name": "partition", "fields": []}},
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"},
                      {"name": "sort_order_id", "type": ["null", "int"], "default": null}
                    ]
                  }
                }
              ]
            }
            "#,
    )
    .expect("sort-order manifest avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("sorted manifest writer should initialize");
    writer
        .add_user_metadata("partition-spec-id".to_string(), "0")
        .expect("manifest partition spec metadata should write");
    for (file_path, content, sort_order_id) in files {
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(1)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(20)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(7)),
                ("file_sequence_number".to_string(), apache_avro::types::Value::Long(7)),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(*content)),
                        ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                        ("partition".to_string(), apache_avro::types::Value::Record(Vec::new())),
                        ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                        ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                        (
                            "sort_order_id".to_string(),
                            apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Int(*sort_order_id))),
                        ),
                    ]),
                ),
            ]))
            .expect("sort-order manifest record should append");
    }
    writer.into_inner().expect("sort-order manifest avro bytes should flush")
}

fn parquet_i32_bytes(values: &[i32]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema) as SchemaRef, vec![Arc::new(Int32Array::from(values.to_vec()))])
        .expect("parquet test batch should build");
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).expect("parquet writer should build");
        writer.write(&batch).expect("parquet batch should write");
        writer.close().expect("parquet writer should close");
    }
    bytes
}

fn parquet_i64_bytes(values: &[i64]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema) as SchemaRef, vec![Arc::new(Int64Array::from(values.to_vec()))])
        .expect("parquet test batch should build");
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).expect("parquet writer should build");
        writer.write(&batch).expect("parquet batch should write");
        writer.close().expect("parquet writer should close");
    }
    bytes
}

fn parquet_i32_values(data: Vec<u8>) -> Vec<i32> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))
        .expect("parquet reader should build")
        .build()
        .expect("parquet batches should build");
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.expect("parquet batch should read");
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("first parquet column should be int32");
        values.extend((0..column.len()).map(|index| column.value(index)));
    }
    values
}

fn test_bucket_entry(bucket: &str) -> TableBucketEntry {
    TableBucketEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        catalog_type: TABLE_BUCKET_CATALOG_TYPE.to_string(),
        warehouse_root: format!("s3://{bucket}/"),
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    }
}

fn test_namespace_entry(bucket: &str, namespace: &Namespace) -> NamespaceEntry {
    NamespaceEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        namespace_id: namespace.storage_id(),
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    }
}

fn test_table_entry(bucket: &str, namespace: &Namespace, table: &IdentifierSegment, metadata_location: String) -> TableEntry {
    TableEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: "table-id".to_string(),
        table_uuid: "table-uuid".to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: format!("s3://{bucket}/tables/table-id"),
        metadata_location,
        version_token: "token-v1".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    }
}

fn test_view_entry(bucket: &str, namespace: &Namespace, view: &IdentifierSegment, metadata_location: String) -> ViewEntry {
    ViewEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        view: view.as_str().to_string(),
        view_id: "view-id".to_string(),
        view_uuid: "view-uuid".to_string(),
        format: "ICEBERG_VIEW".to_string(),
        format_version: 1,
        warehouse_location: format!("s3://{bucket}/views/view-id"),
        metadata_location,
        version_token: "token-v1".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    }
}

fn test_strong_snapshot(
    bucket: &str,
    namespace: &Namespace,
    tables: Vec<TableEntry>,
    views: Vec<ViewEntry>,
) -> StrongTableCatalogSnapshot {
    StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: vec![test_namespace_entry(bucket, namespace)],
        tables,
        views,
        commits: Vec::new(),
        idempotency: Vec::new(),
    }
}

async fn seed_strong_snapshot(backend: &TestCatalogObjectBackend, snapshot: &StrongTableCatalogSnapshot) {
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            serde_json::to_vec(snapshot).expect("strong snapshot should encode"),
        )
        .await;
}

async fn read_strong_snapshot(backend: &TestCatalogObjectBackend) -> StrongTableCatalogSnapshot {
    let object = backend
        .read_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
        )
        .await
        .expect("strong snapshot should load")
        .expect("strong snapshot should exist");
    serde_json::from_slice(&object.data).expect("strong snapshot should decode")
}

async fn strong_snapshot_hydration_error(snapshot: StrongTableCatalogSnapshot) -> TableCatalogStoreError {
    let backend = TestCatalogObjectBackend::default();
    let bucket = snapshot
        .table_buckets
        .first()
        .map(|entry| entry.table_bucket.clone())
        .unwrap_or_else(|| "missing".to_string());
    seed_strong_snapshot(&backend, &snapshot).await;
    StrongTableCatalogStore::new(backend)
        .get_table_bucket(&bucket)
        .await
        .expect_err("strong snapshot hydration should fail")
}

async fn seed_catalog_list_entries<S>(store: &S, bucket: &str, namespace: &Namespace)
where
    S: TableCatalogStore + ?Sized,
{
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    for namespace in [
        Namespace::parse("analytics").expect("namespace should parse"),
        namespace.clone(),
    ] {
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .expect("namespace should be created");
    }
    for name in ["alpha", "beta"] {
        let table_identifier = IdentifierSegment::parse(name).expect("table name should parse");
        let metadata_location = default_table_metadata_file_path(namespace, &table_identifier, "00001.metadata.json");
        let mut table = test_table_entry(bucket, namespace, &table_identifier, metadata_location);
        table.table_id = format!("table-{name}");
        table.table_uuid = format!("table-uuid-{name}");
        table.warehouse_location = format!("s3://{bucket}/tables/table-{name}");
        store.create_table(table).await.expect("table should be created");

        let view_identifier = IdentifierSegment::parse(format!("view_{name}")).expect("view name should parse");
        let view_metadata_location = default_view_metadata_file_path(namespace, &view_identifier, "00001.metadata.json");
        let mut view = test_view_entry(bucket, namespace, &view_identifier, view_metadata_location);
        view.view_id = format!("view-{name}");
        view.view_uuid = format!("view-uuid-{name}");
        view.warehouse_location = format!("s3://{bucket}/views/view-{name}");
        store.create_view(view).await.expect("view should be created");
    }
}

async fn seed_table_for_metadata_maintenance(
    store: &ObjectTableCatalogStore<TestCatalogObjectBackend>,
    bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    current_metadata: String,
) {
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store.create_namespace(test_namespace_entry(bucket, namespace)).await.unwrap();
    store
        .create_table(test_table_entry(bucket, namespace, table, current_metadata))
        .await
        .unwrap();
    store.backfill_table_warehouse_index(bucket).await.unwrap();
    store.backend.reset_call_counts().await;
}

async fn seed_quarantined_table_maintenance(
    store: &ObjectTableCatalogStore<TestCatalogObjectBackend>,
    backend: &TestCatalogObjectBackend,
    bucket: &str,
    now: OffsetDateTime,
    next_retry_after: Option<OffsetDateTime>,
) -> (Namespace, IdentifierSegment, TableMetadataMaintenanceReport) {
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                max_retry_attempts: 2,
                retry_initial_backoff_seconds: 60,
                retry_max_backoff_seconds: 300,
                quarantine_enabled: true,
                quarantine_retention_seconds: 86_400,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");
    let mut failed = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    failed.job.status = TableMetadataMaintenanceJobStatus::Failed;
    failed.job.failure_reason = Some("quarantine retained failed cleanup candidates".to_string());
    failed.job.max_retry_attempts = 2;
    failed.job.next_retry_after = next_retry_after.map(maintenance_timestamp);
    failed.job.quarantine_enabled = true;
    failed.job.quarantine_retention_seconds = 86_400;
    failed.job.quarantined_object_count = 2;
    failed.job.finished_at = Some(maintenance_timestamp(now - Duration::seconds(10)));
    store
        .put_table_metadata_maintenance_report(&failed)
        .await
        .expect("failed maintenance report should be seeded");
    (namespace, table, failed)
}

#[tokio::test]
async fn object_table_catalog_store_writes_catalog_entries_to_internal_meta_bucket() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    assert!(!store.warehouse_index_ready(bucket).await.unwrap());

    let state = backend.state.lock().await;
    let object_buckets = state
        .objects
        .keys()
        .map(|(bucket, _)| bucket.as_str())
        .collect::<BTreeSet<_>>();

    assert_eq!(object_buckets, BTreeSet::from([RUSTFS_META_BUCKET]));
}

#[tokio::test]
async fn configured_table_catalog_store_uses_durable_strong_snapshot() {
    let backend = TestCatalogObjectBackend::default();
    let store = ConfiguredTableCatalogStore::new_for_test(backend.clone(), TableCatalogBackingMode::DurableStrong);
    let bucket = "analytics";

    assert_eq!(store.backing_mode(), TableCatalogBackingMode::DurableStrong);
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();

    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    assert!(backend.object_exists(RUSTFS_META_BUCKET, &snapshot_path).await.unwrap());

    let reloaded = ConfiguredTableCatalogStore::new_for_test(backend.clone(), TableCatalogBackingMode::DurableStrong);
    assert!(reloaded.get_table_bucket(bucket).await.unwrap().is_some());
}

#[tokio::test]
async fn object_table_catalog_store_persists_view_entries_and_blocks_non_empty_namespace_drop() {
    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let view = IdentifierSegment::parse("recent_orders").unwrap();
    let current_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let next_metadata = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_view(test_view_entry(bucket, &namespace, &view, current_metadata.clone()))
        .await
        .unwrap();

    assert_eq!(store.list_views(bucket, &namespace.public_name()).await.unwrap()[0].view, "recent_orders");
    assert!(
        store
            .load_view(bucket, &namespace.public_name(), view.as_str())
            .await
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        store.drop_namespace(bucket, &namespace.public_name()).await,
        Err(TableCatalogStoreError::Conflict(_))
    ));

    backend
        .seed_object(
            bucket,
            &next_metadata,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 1,
                "view-uuid": "view-uuid",
                "location": format!("s3://{bucket}/views/view-id")
            }))
            .unwrap(),
        )
        .await;
    let result = store
        .replace_view(ViewCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            view: view.as_str().to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: next_metadata.clone(),
        })
        .await
        .unwrap();

    assert_eq!(result.view.metadata_location, next_metadata);
    assert_eq!(result.view.generation, 2);
    assert_ne!(result.view.version_token, "token-v1");

    store
        .drop_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .unwrap();
    assert!(store.list_views(bucket, &namespace.public_name()).await.unwrap().is_empty());
    store.drop_namespace(bucket, &namespace.public_name()).await.unwrap();
}

#[tokio::test]
async fn object_catalog_view_replacement_recovers_after_committed_write_response_loss() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let current_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let next_metadata = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_view(test_view_entry(bucket, &namespace, &view, current_metadata.clone()))
        .await
        .unwrap();
    backend
        .seed_object(
            bucket,
            &next_metadata,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 1,
                "view-uuid": "view-uuid",
                "location": format!("s3://{bucket}/views/view-id")
            }))
            .expect("view metadata should encode"),
        )
        .await;
    let view_path = store.paths.view_entry_path(bucket, &namespace, &view);
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &view_path).await;

    let replaced = store
        .replace_view(ViewCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            view: view.as_str().to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: next_metadata.clone(),
        })
        .await
        .expect("an exact persisted replacement must prove the ambiguous write succeeded");

    assert_eq!(replaced.view.metadata_location, next_metadata);
    assert_eq!(replaced.view.generation, 2);
    let loaded = store
        .load_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect("view lookup should succeed")
        .expect("view should remain present");
    assert_eq!(loaded, replaced.view);
}

#[tokio::test]
async fn maintenance_dry_run_keeps_current_metadata() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let v1 = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let v2 = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &v1, b"{}".to_vec()).await;
    backend.seed_object(bucket, &v2, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .unwrap();

    assert_eq!(report.current_metadata_location, current);
    assert!(report.retained_metadata_locations.contains(&report.current_metadata_location));
    assert!(!report.cleanup_candidate_locations.contains(&report.current_metadata_location));
    assert_eq!(report.cleanup_candidate_locations, vec![v1, v2]);
}

#[tokio::test]
async fn table_data_plane_resource_resolves_registered_warehouse_prefix() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend.reset_call_counts().await;
    assert_eq!(backend.list_call_count().await, 0);

    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("data-plane resource lookup should succeed")
        .expect("object should resolve to the registered table");

    assert_eq!(resource.table_bucket, bucket);
    assert_eq!(resource.namespace, "sales");
    assert_eq!(resource.table, "orders");
    assert_eq!(resource.table_id, "table-id");
    assert_eq!(resource.warehouse_object_prefix, "tables/table-id/");
    assert_eq!(resource.catalog_resource_object(), "namespaces/sales/tables/orders");
    assert_eq!(backend.list_call_count().await, 0);
}

#[tokio::test]
async fn table_data_plane_resource_does_not_match_sibling_prefix() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend.reset_call_counts().await;

    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id-other/data/part-00001.parquet")
        .await
        .expect("data-plane resource lookup should succeed");

    assert!(resource.is_none());
    assert_eq!(backend.list_call_count().await, 1);
}

#[tokio::test]
async fn catalog_backings_reject_overlapping_registered_warehouse_prefixes() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let parent_table = IdentifierSegment::parse("orders").unwrap();
    let child_table = IdentifierSegment::parse("orders_child").unwrap();
    let current = default_table_metadata_file_path(&namespace, &parent_table, "00001.metadata.json");
    let child_metadata = default_table_metadata_file_path(&namespace, &child_table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &parent_table, current).await;
    let mut child_entry = test_table_entry(bucket, &namespace, &child_table, child_metadata);
    child_entry.table_id = "table-id-child".to_string();
    child_entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child");
    assert_matches!(store.create_table(child_entry.clone()).await, Err(TableCatalogStoreError::Conflict(_)));

    let strong_backend = TestCatalogObjectBackend::default();
    let strong_store = StrongTableCatalogStore::new(strong_backend);
    strong_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    strong_store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    strong_store
        .create_table(test_table_entry(
            bucket,
            &namespace,
            &parent_table,
            default_table_metadata_file_path(&namespace, &parent_table, "00001.metadata.json"),
        ))
        .await
        .unwrap();
    assert_matches!(strong_store.create_table(child_entry).await, Err(TableCatalogStoreError::Conflict(_)));
}

async fn assert_resource_id_registration_contract<S>(store: &S)
where
    S: TableCatalogStore,
{
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let orders = IdentifierSegment::parse("orders").expect("table should parse");
    let returns = IdentifierSegment::parse("returns").expect("table should parse");
    let empty = IdentifierSegment::parse("empty_id").expect("table should parse");
    let empty_view = IdentifierSegment::parse("empty_view_id").expect("view should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(
            bucket,
            &namespace,
            &orders,
            default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json"),
        ))
        .await
        .expect("first table should be created");

    let mut duplicate = test_table_entry(
        bucket,
        &namespace,
        &returns,
        default_table_metadata_file_path(&namespace, &returns, "00001.metadata.json"),
    );
    duplicate.warehouse_location = format!("s3://{bucket}/tables/returns");
    assert_matches!(store.create_table(duplicate).await, Err(TableCatalogStoreError::Conflict(_)));

    let mut empty_id = test_table_entry(
        bucket,
        &namespace,
        &empty,
        default_table_metadata_file_path(&namespace, &empty, "00001.metadata.json"),
    );
    empty_id.table_id.clear();
    empty_id.warehouse_location = format!("s3://{bucket}/tables/empty-id");
    assert_matches!(store.create_table(empty_id).await, Err(TableCatalogStoreError::Invalid(_)));

    let mut empty_view_id = test_view_entry(
        bucket,
        &namespace,
        &empty_view,
        default_view_metadata_file_path(&namespace, &empty_view, "00001.view-metadata.json"),
    );
    empty_view_id.view_id.clear();
    assert_matches!(store.create_view(empty_view_id).await, Err(TableCatalogStoreError::Invalid(_)));
}

#[tokio::test]
async fn catalog_backings_reject_empty_resource_ids_and_duplicate_table_ids() {
    assert_resource_id_registration_contract(&ObjectTableCatalogStore::new(TestCatalogObjectBackend::default())).await;
    assert_resource_id_registration_contract(&StrongTableCatalogStore::new(TestCatalogObjectBackend::default())).await;
}

#[tokio::test]
async fn object_catalog_rejects_overlapping_registration_when_a_ready_index_entry_is_missing() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let parent_table = IdentifierSegment::parse("orders").expect("parent table should parse");
    let child_table = IdentifierSegment::parse("orders_child").expect("child table should parse");
    let current = default_table_metadata_file_path(&namespace, &parent_table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &parent_table, current.clone()).await;
    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.warehouse_index_entry_path(bucket, "tables/table-id/"))
        .await
        .expect("warehouse index entry should be removed");
    assert!(
        store
            .warehouse_index_ready(bucket)
            .await
            .expect("warehouse index state should remain ready")
    );

    let mut child_entry = test_table_entry(bucket, &namespace, &child_table, current);
    child_entry.table_id = "table-id-child".to_string();
    child_entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child");

    assert_matches!(store.create_table(child_entry).await, Err(TableCatalogStoreError::Conflict(_)));
}

#[tokio::test]
async fn object_catalog_rechecks_overlap_when_candidate_has_a_stale_reservation() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let parent_table = IdentifierSegment::parse("orders").expect("parent table should parse");
    let child_table = IdentifierSegment::parse("orders_child").expect("child table should parse");
    let current = default_table_metadata_file_path(&namespace, &parent_table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &parent_table, current).await;
    let mut child_entry = test_table_entry(
        bucket,
        &namespace,
        &child_table,
        default_table_metadata_file_path(&namespace, &child_table, "00001.metadata.json"),
    );
    child_entry.table_id = "table-id-child".to_string();
    child_entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child");
    let child_index = table_warehouse_index_entry(&child_entry).expect("child index should be valid");
    let child_index_path = store
        .paths
        .warehouse_index_entry_path(bucket, &child_index.warehouse_object_prefix);
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &child_index_path,
            serde_json::to_vec(&child_index).expect("child index should serialize"),
        )
        .await;

    assert_matches!(store.create_table(child_entry).await, Err(TableCatalogStoreError::Conflict(_)));
}

#[tokio::test]
async fn object_catalog_revalidates_legacy_warehouse_index_before_resolution() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let parent_table = IdentifierSegment::parse("orders").expect("parent table should parse");
    let child_table = IdentifierSegment::parse("orders_child").expect("child table should parse");
    let current = default_table_metadata_file_path(&namespace, &parent_table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &parent_table, current).await;
    let mut child_entry = test_table_entry(
        bucket,
        &namespace,
        &child_table,
        default_table_metadata_file_path(&namespace, &child_table, "00001.metadata.json"),
    );
    child_entry.table_id = "table-id-child".to_string();
    child_entry.table_uuid = "table-uuid-child".to_string();
    child_entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child");
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &child_table),
            &child_entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("legacy overlapping table entry should be seeded");
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.warehouse_index_state_path(bucket),
            &TableWarehouseIndexStateEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.to_string(),
                state: TableCatalogEntryState::Active,
            },
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("legacy warehouse index state should be seeded");

    let error = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/child/data/part-00001.parquet")
        .await
        .expect_err("legacy overlapping prefixes must fail closed during index rebuild");

    assert!(matches!(
        error,
        TableCatalogStoreError::Conflict(message) if message.contains("warehouse locations overlap")
    ));
}

#[tokio::test]
async fn object_catalog_ready_index_rejects_overlapping_active_owners() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let parent_table = IdentifierSegment::parse("orders").expect("parent table should parse");
    let child_table = IdentifierSegment::parse("orders_child").expect("child table should parse");
    let current = default_table_metadata_file_path(&namespace, &parent_table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &parent_table, current).await;
    let mut child_entry = test_table_entry(
        bucket,
        &namespace,
        &child_table,
        default_table_metadata_file_path(&namespace, &child_table, "00001.metadata.json"),
    );
    child_entry.table_id = "table-id-child".to_string();
    child_entry.table_uuid = "table-uuid-child".to_string();
    child_entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child");
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &child_table),
            &child_entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("legacy overlapping table entry should be seeded");
    let child_index = table_warehouse_index_entry(&child_entry).expect("child warehouse index should be valid");
    store
        .write_entry(
            store.catalog_bucket(),
            &store
                .paths
                .warehouse_index_entry_path(bucket, &child_index.warehouse_object_prefix),
            &child_index,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("legacy overlapping warehouse index should be seeded");
    assert!(
        store
            .warehouse_index_ready(bucket)
            .await
            .expect("warehouse index should remain ready")
    );

    let error = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/child/data/part-00001.parquet")
        .await
        .expect_err("overlapping authoritative indexes must fail closed");

    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("overlapping active table warehouse indexes")
    );
}

#[tokio::test]
async fn object_catalog_index_backfill_rejects_legacy_duplicate_table_ids() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let orders = IdentifierSegment::parse("orders").expect("orders table should parse");
    let returns = IdentifierSegment::parse("returns").expect("returns table should parse");
    let current = default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &orders, current).await;
    let mut duplicate = test_table_entry(
        bucket,
        &namespace,
        &returns,
        default_table_metadata_file_path(&namespace, &returns, "00001.metadata.json"),
    );
    duplicate.warehouse_location = format!("s3://{bucket}/tables/returns");
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &returns),
            &duplicate,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("legacy duplicate table id should be seeded");
    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.warehouse_index_state_path(bucket))
        .await
        .expect("warehouse index readiness marker should be removed");

    let error = store
        .backfill_table_warehouse_index(bucket)
        .await
        .expect_err("duplicate table ids must block warehouse index readiness");

    assert_matches!(
        error,
        TableCatalogStoreError::Conflict(message) if message.contains("registered by multiple tables")
    );
    assert!(
        !store
            .warehouse_index_ready(bucket)
            .await
            .expect("warehouse index should remain unready")
    );
}

#[tokio::test]
async fn object_catalog_data_plane_rejects_invalid_active_warehouse_location() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let mut entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    entry.warehouse_location = "s3://other-bucket/tables/table-id".to_string();

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &table),
            &entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("invalid persisted table should be seeded");

    let error = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect_err("invalid active warehouse locations must fail closed");

    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("must be inside the table bucket"));
    assert!(!store.warehouse_index_ready(bucket).await.unwrap());
}

#[tokio::test]
async fn table_data_plane_resource_skips_stale_deeper_index_and_matches_parent() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    let stale_prefix = "tables/table-id/child/";
    let stale_index = TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: "table-id".to_string(),
        warehouse_object_prefix: stale_prefix.to_string(),
        state: TableCatalogEntryState::Active,
    };
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.warehouse_index_entry_path(bucket, stale_prefix),
            &stale_index,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .unwrap();
    backend.reset_call_counts().await;

    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/child/data/part-00001.parquet")
        .await
        .expect("stale deeper index should not fail lookup")
        .expect("parent table should still protect the object");

    assert_eq!(resource.table, "orders");
    assert_eq!(resource.warehouse_object_prefix, "tables/table-id/");
    assert_eq!(backend.list_call_count().await, 0);
}

#[tokio::test]
async fn object_catalog_rebuilds_warehouse_index_for_table_backed_namespace() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let object = "tables/table-id/data/part-00001.parquet";

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.namespace_entry_path(bucket, &namespace))
        .await
        .expect("namespace marker should be removed");

    let scanned = scan_table_data_plane_resource_for_object(&store, bucket, object)
        .await
        .expect("resource scan should succeed")
        .expect("table entry should keep its namespace discoverable");
    assert_eq!(scanned.table, "orders");

    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.warehouse_index_entry_path(bucket, "tables/table-id/"))
        .await
        .expect("warehouse index entry should be removed");
    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.warehouse_index_state_path(bucket))
        .await
        .expect("warehouse index state should be removed");

    let rebuilt = table_data_plane_resource_for_object(&store, bucket, object)
        .await
        .expect("resource lookup should rebuild the index")
        .expect("rebuilt index should resolve the table");
    assert_eq!(rebuilt.table, "orders");
    assert!(
        store
            .warehouse_index_ready(bucket)
            .await
            .expect("warehouse index state should load")
    );
}

#[tokio::test]
async fn table_data_plane_resource_scans_when_a_ready_index_entry_is_missing() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let object = "tables/table-id/data/part-00001.parquet";

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.warehouse_index_entry_path(bucket, "tables/table-id/"))
        .await
        .expect("warehouse index entry should be removed");
    backend.reset_call_counts().await;

    let resource = table_data_plane_resource_for_object(&store, bucket, object)
        .await
        .expect("missing ready index entries must use the authoritative table scan")
        .expect("the table scan must retain table-aware protection");

    assert_eq!(resource.table, "orders");
    assert!(backend.list_call_count().await > 0);
}

#[tokio::test]
async fn table_data_plane_resource_scans_when_an_index_disappears_during_backfill() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let object = "tables/table-id/data/part-00001.parquet";
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");
    let state_path = store.paths.warehouse_index_state_path(bucket);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend
        .delete_object(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("warehouse index entry should be removed before backfill");
    backend
        .delete_object(RUSTFS_META_BUCKET, &state_path)
        .await
        .expect("warehouse index readiness should be cleared before backfill");
    let paused_state_write = backend.pause_next_put(RUSTFS_META_BUCKET, &state_path).await;
    let resolve_store = store.clone();
    let resolution = tokio::spawn(async move { table_data_plane_resource_for_object(&resolve_store, bucket, object).await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, paused_state_write.wait_started())
        .await
        .expect("warehouse index state publication should start");
    backend
        .delete_object(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("warehouse index entry should be removed after backfill publication");
    paused_state_write.release();

    let resource = resolution
        .await
        .expect("data-plane resolution task should join")
        .expect("authoritative catalog scan should preserve table-aware resolution")
        .expect("table-aware resource must not be lost after an index race");
    assert_eq!(resource.table, "orders");
}

#[tokio::test]
async fn table_data_plane_resource_fails_closed_for_an_inactive_ready_index() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let prefix = "tables/table-id/";

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    let inactive = TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: "table-id".to_string(),
        warehouse_object_prefix: prefix.to_string(),
        state: TableCatalogEntryState::Deleted,
    };
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.warehouse_index_entry_path(bucket, prefix),
            &inactive,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("inactive warehouse index should be seeded");

    assert_matches!(
        table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet").await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("inactive while the index is authoritative")
    );
}

#[tokio::test]
async fn object_table_catalog_store_rejects_duplicate_warehouse_prefix() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let first_table = IdentifierSegment::parse("orders").unwrap();
    let second_table = IdentifierSegment::parse("returns").unwrap();
    let current = default_table_metadata_file_path(&namespace, &first_table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &first_table, current.clone()).await;
    let mut second_entry = test_table_entry(bucket, &namespace, &second_table, current);
    second_entry.table_id = "second-table-id".to_string();
    second_entry.warehouse_location = format!("s3://{bucket}/tables/table-id");

    let error = store.create_table(second_entry).await.unwrap_err();

    assert!(matches!(
        error,
        TableCatalogStoreError::Conflict(message) if message.contains("warehouse location overlaps an active table")
    ));
}

#[tokio::test]
async fn table_data_plane_resource_fails_closed_for_missing_indexed_table() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let prefix = "tables/missing-table/";
    let index = TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: "orders".to_string(),
        table_id: "missing-table-id".to_string(),
        warehouse_object_prefix: prefix.to_string(),
        state: TableCatalogEntryState::Active,
    };

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store.backfill_table_warehouse_index(bucket).await.unwrap();
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.warehouse_index_entry_path(bucket, prefix),
            &index,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .unwrap();

    let error = table_data_plane_resource_for_object(&store, bucket, "tables/missing-table/data/part-00001.parquet")
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        TableCatalogStoreError::Internal(message) if message.contains("referenced table entry is missing")
    ));
}

#[tokio::test]
async fn table_data_plane_resource_fails_closed_for_mismatched_indexed_table_identity() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

    let mut mismatched = store
        .load_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    mismatched.namespace = "finance".to_string();
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &table),
            &mismatched,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("mismatched table identity should be seeded");
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    let error = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect_err("mismatched persisted table identity must fail closed");
    let recovery_error = store
        .plan_table_commit_recovery(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect_err("catalog recovery must reject a mismatched persisted table identity");

    for error in [error, recovery_error] {
        assert_matches!(
            error,
            TableCatalogStoreError::Invalid(message)
                if message.contains("catalog table entry identity does not match its object path")
        );
    }
    assert!(
        backend
            .object_exists(RUSTFS_META_BUCKET, &index_path)
            .await
            .expect("warehouse index lookup should succeed")
    );
}

#[tokio::test]
async fn table_data_plane_resource_rejects_unknown_warehouse_index_version() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

    let prefix = "tables/table-id/";
    let unsupported = TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION + 1,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: "table-id".to_string(),
        warehouse_object_prefix: prefix.to_string(),
        state: TableCatalogEntryState::Active,
    };
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.warehouse_index_entry_path(bucket, prefix),
            &unsupported,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("unsupported warehouse index should be seeded");
    store
        .backend
        .delete_object(store.catalog_bucket(), &store.paths.warehouse_index_state_path(bucket))
        .await
        .expect("warehouse index state should be removed");

    let error = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect_err("unknown warehouse index versions must fail closed");

    assert!(matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("unsupported warehouse index entry version")
    ));
    assert!(
        !store
            .warehouse_index_ready(bucket)
            .await
            .expect("warehouse index state lookup should succeed")
    );
}

#[tokio::test]
async fn object_catalog_does_not_overwrite_unknown_warehouse_index_state_version() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let state_path = store.paths.warehouse_index_state_path(bucket);
    let future_state = TableWarehouseIndexStateEntry {
        version: TABLE_WAREHOUSE_INDEX_STATE_VERSION + 1,
        table_bucket: bucket.to_string(),
        state: TableCatalogEntryState::Active,
    };

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .write_entry(store.catalog_bucket(), &state_path, &future_state, TableCatalogPutPrecondition::Any)
        .await
        .expect("future warehouse index state should be seeded");

    let error = store
        .backfill_table_warehouse_index(bucket)
        .await
        .expect_err("unknown warehouse index state versions must fail closed");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("unsupported warehouse index state version")
    );
    let persisted = store
        .read_entry::<TableWarehouseIndexStateEntry>(store.catalog_bucket(), &state_path)
        .await
        .expect("warehouse index state should load")
        .expect("warehouse index state should remain")
        .0;
    assert_eq!(persisted, future_state);
}

#[tokio::test]
async fn object_catalog_rejects_mismatched_table_bucket_identity() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let requested_bucket = "analytics";
    let mut mismatched = test_bucket_entry("finance");
    mismatched.state = TableCatalogEntryState::Active;
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_bucket_entry_path(requested_bucket),
            &mismatched,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("mismatched table bucket identity should be seeded");

    let error = store
        .get_table_bucket(requested_bucket)
        .await
        .expect_err("mismatched persisted table bucket identity must fail closed");

    assert!(matches!(
        error,
        TableCatalogStoreError::Invalid(message)
            if message.contains("catalog table bucket entry identity does not match its object path")
    ));
}

#[tokio::test]
async fn object_catalog_view_reads_reject_mismatched_persisted_identity() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("orders_view").expect("view should parse");
    let metadata_location = default_view_metadata_file_path(&namespace, &view, "00001.view-metadata.json");
    let mut mismatched = test_view_entry(bucket, &namespace, &view, metadata_location);
    mismatched.namespace = "finance".to_string();
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.view_entry_path(bucket, &namespace, &view),
            &mismatched,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("mismatched view identity should be seeded");

    let load_error = store
        .load_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect_err("mismatched persisted view identity must fail closed on load");
    let list_error = store
        .list_views(bucket, &namespace.public_name())
        .await
        .expect_err("mismatched persisted view identity must fail closed on list");
    let drop_error = store
        .drop_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect_err("mismatched persisted view identity must fail closed before deletion");

    for error in [load_error, list_error, drop_error] {
        assert!(matches!(
            error,
            TableCatalogStoreError::Invalid(message)
                if message.contains("catalog view entry identity does not match its object path")
        ));
    }
}

#[tokio::test]
async fn object_catalog_view_reads_reject_empty_persisted_id() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("orders_view").expect("view should parse");
    let metadata_location = default_view_metadata_file_path(&namespace, &view, "00001.view-metadata.json");
    let mut invalid = test_view_entry(bucket, &namespace, &view, metadata_location);
    invalid.view_id.clear();
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.view_entry_path(bucket, &namespace, &view),
            &invalid,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("invalid view entry should be seeded");

    let error = store
        .load_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect_err("an empty persisted view id must fail closed");

    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("view id cannot be empty"));
}

#[tokio::test]
async fn object_catalog_data_plane_removes_index_for_inactive_table() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    let mut inactive = store
        .load_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    inactive.state = TableCatalogEntryState::Deleted;
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &table),
            &inactive,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("inactive table state should be seeded");
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("inactive table lookup should safely clean its stale index");

    assert!(resource.is_none());
    assert!(
        !backend
            .object_exists(RUSTFS_META_BUCKET, &index_path)
            .await
            .expect("warehouse index lookup should succeed")
    );
}

#[tokio::test]
async fn object_table_catalog_store_replaces_stale_warehouse_index_on_create() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let prefix = "tables/shared-table/";
    let index_path = store.paths.warehouse_index_entry_path(bucket, prefix);
    let stale_index = TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: "missing_orders".to_string(),
        table_id: "missing-table-id".to_string(),
        warehouse_object_prefix: prefix.to_string(),
        state: TableCatalogEntryState::Active,
    };

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .write_entry(store.catalog_bucket(), &index_path, &stale_index, TableCatalogPutPrecondition::Any)
        .await
        .unwrap();

    let mut entry = test_table_entry(bucket, &namespace, &table, current);
    entry.warehouse_location = format!("s3://{bucket}/tables/shared-table");
    store
        .create_table(entry)
        .await
        .expect("stale warehouse index should be repaired before reserving the prefix");

    let (index, _) = store
        .read_entry::<TableWarehouseIndexEntry>(store.catalog_bucket(), &index_path)
        .await
        .unwrap()
        .expect("repaired index should exist");
    assert_eq!(index.table, "orders");
    assert_eq!(index.table_id, "table-id");
}

#[tokio::test]
async fn object_table_catalog_store_does_not_replace_unknown_warehouse_index_version() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let prefix = "tables/shared-table/";
    let index_path = store.paths.warehouse_index_entry_path(bucket, prefix);
    let unsupported = TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION + 1,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: "missing_orders".to_string(),
        table_id: "missing-table-id".to_string(),
        warehouse_object_prefix: prefix.to_string(),
        state: TableCatalogEntryState::Active,
    };

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .write_entry(store.catalog_bucket(), &index_path, &unsupported, TableCatalogPutPrecondition::Any)
        .await
        .expect("unsupported warehouse index should be seeded");

    let mut entry = test_table_entry(bucket, &namespace, &table, current);
    entry.warehouse_location = format!("s3://{bucket}/tables/shared-table");
    let error = store
        .create_table(entry)
        .await
        .expect_err("unknown warehouse index versions must not be replaced as stale");

    assert!(matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("unsupported warehouse index entry version")
    ));
}

#[tokio::test]
async fn object_table_catalog_store_does_not_ignore_unknown_unrelated_warehouse_index_version() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let prefix = "tables/future-table/";
    let index_path = store.paths.warehouse_index_entry_path(bucket, prefix);
    let unsupported = TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION + 1,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: "future_orders".to_string(),
        table_id: "future-table-id".to_string(),
        warehouse_object_prefix: prefix.to_string(),
        state: TableCatalogEntryState::Active,
    };

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .write_entry(store.catalog_bucket(), &index_path, &unsupported, TableCatalogPutPrecondition::Any)
        .await
        .expect("future warehouse index should be seeded");

    let error = store
        .create_table(test_table_entry(bucket, &namespace, &table, current))
        .await
        .expect_err("unknown index versions must not be skipped during warehouse uniqueness checks");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("unsupported warehouse index entry version")
    );
}

#[tokio::test]
async fn object_table_catalog_store_rejects_misplaced_warehouse_index_before_create() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let misplaced = TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: "missing_orders".to_string(),
        table_id: "missing-table-id".to_string(),
        warehouse_object_prefix: "tables/payload-prefix/".to_string(),
        state: TableCatalogEntryState::Active,
    };
    let misplaced_path = store.paths.warehouse_index_entry_path(bucket, "tables/object-prefix/");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .write_entry(store.catalog_bucket(), &misplaced_path, &misplaced, TableCatalogPutPrecondition::Any)
        .await
        .expect("misplaced warehouse index should be seeded");

    let error = store
        .create_table(test_table_entry(bucket, &namespace, &table, current))
        .await
        .expect_err("misplaced warehouse indexes must fail closed during uniqueness checks");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("warehouse index identity does not match its object path")
    );
}

#[tokio::test]
async fn table_data_plane_resource_falls_back_to_scan_without_index_state() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let bucket_entry = test_bucket_entry(bucket);
    let namespace_entry = test_namespace_entry(bucket, &namespace);
    let table_entry = test_table_entry(bucket, &namespace, &table, current);

    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_bucket_entry_path(bucket),
            &bucket_entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("table bucket entry should be seeded");
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.namespace_entry_path(bucket, &namespace),
            &namespace_entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .unwrap();
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &table),
            &table_entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .unwrap();

    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("legacy catalog lookup should fall back to scanning")
        .expect("legacy table entry should resolve");

    assert_eq!(resource.table, "orders");
    assert!(backend.list_call_count().await > 0);
    assert!(store.warehouse_index_ready(bucket).await.unwrap());

    backend.reset_call_counts().await;
    let indexed_resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00002.parquet")
        .await
        .expect("backfilled index lookup should succeed")
        .expect("backfilled table entry should resolve");

    assert_eq!(indexed_resource.table, "orders");
    assert_eq!(backend.list_call_count().await, 0);
    assert!(backend.read_call_count().await <= 6);
}

#[tokio::test]
async fn object_table_catalog_store_backfill_skips_table_deleted_after_listing() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let bucket_entry = test_bucket_entry(bucket);
    let namespace_entry = test_namespace_entry(bucket, &namespace);
    let table_entry = test_table_entry(bucket, &namespace, &table, current);
    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_bucket_entry_path(bucket),
            &bucket_entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .unwrap();
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.namespace_entry_path(bucket, &namespace),
            &namespace_entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("namespace entry should be seeded");
    store
        .write_entry(store.catalog_bucket(), &table_path, &table_entry, TableCatalogPutPrecondition::Any)
        .await
        .expect("table entry should be seeded without an index");

    let listed = store
        .list_tables(bucket, &namespace.public_name())
        .await
        .expect("table listing should succeed");
    assert_eq!(listed.len(), 1);
    backend
        .delete_object(RUSTFS_META_BUCKET, &table_path)
        .await
        .expect("listed table entry should be deleted before backfill");

    for table in listed {
        store
            .backfill_active_table_warehouse_index(&table.table_bucket, &table.namespace, &table.table)
            .await
            .expect("backfill should skip a table deleted after listing");
    }

    assert!(
        store
            .read_entry::<TableWarehouseIndexEntry>(store.catalog_bucket(), &index_path)
            .await
            .expect("warehouse index lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn object_catalog_pagination_bounds_reads_and_covers_rest_resources() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let namespace_name = namespace.public_name();
    let one = NonZeroUsize::new(1).expect("page size should be non-zero");

    seed_catalog_list_entries(&store, bucket, &namespace).await;

    let namespace_page = store
        .list_namespaces_page(bucket, None, one)
        .await
        .expect("first namespace page should load");
    assert_eq!(namespace_page.entries[0].namespace, "analytics");
    assert!(
        namespace_page
            .next_cursor
            .as_deref()
            .is_some_and(|cursor| cursor.starts_with(OBJECT_CATALOG_LIST_CURSOR_PREFIX))
    );
    let namespace_page = store
        .list_namespaces_page(bucket, namespace_page.next_cursor.as_deref(), one)
        .await
        .expect("second namespace page should load");
    assert_eq!(namespace_page.entries[0].namespace, "sales");
    assert!(namespace_page.next_cursor.is_none());

    backend.reset_call_counts().await;
    let table_page = store
        .list_tables_page(bucket, &namespace_name, None, one)
        .await
        .expect("first table page should load");
    assert_eq!(table_page.entries[0].table, "alpha");
    assert_eq!(backend.read_call_count().await, 1);
    let table_page = store
        .list_tables_page(bucket, &namespace_name, table_page.next_cursor.as_deref(), one)
        .await
        .expect("second table page should load");
    assert_eq!(table_page.entries[0].table, "beta");
    assert!(table_page.next_cursor.is_none());

    let view_page = store
        .list_views_page(bucket, &namespace_name, None, one)
        .await
        .expect("first view page should load");
    assert_eq!(view_page.entries[0].view, "view_alpha");
    let view_page = store
        .list_views_page(bucket, &namespace_name, view_page.next_cursor.as_deref(), one)
        .await
        .expect("second view page should load");
    assert_eq!(view_page.entries[0].view, "view_beta");
    assert!(view_page.next_cursor.is_none());

    let exact_page = store
        .list_tables_page(bucket, &namespace_name, None, NonZeroUsize::new(2).expect("page size should be non-zero"))
        .await
        .expect("exact table page should load");
    assert_eq!(exact_page.entries.len(), 2);
    assert!(exact_page.next_cursor.is_none());
    assert!(matches!(
        store
            .list_tables_page(bucket, &namespace_name, Some("strong:alpha"), one)
            .await,
        Err(TableCatalogStoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn object_catalog_pagination_bounds_sparse_namespace_scans() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let prefix = store.paths.namespace_entries_prefix(bucket);

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    for index in 0..TABLE_CATALOG_LIST_MAX_KEYS {
        backend
            .seed_object(RUSTFS_META_BUCKET, &format!("{prefix}0000-spacer/{index:04}.json"), Vec::new())
            .await;
    }
    backend.reset_call_counts().await;

    let one = NonZeroUsize::new(1).expect("page size should be non-zero");
    let first = store
        .list_namespaces_page(bucket, None, one)
        .await
        .expect("sparse first page should load");
    assert!(first.entries.is_empty());
    assert_eq!(backend.list_call_count().await, 1);
    let cursor = first.next_cursor.expect("truncated sparse page should have a cursor");
    assert!(cursor.starts_with(OBJECT_CATALOG_LIST_CURSOR_PREFIX));
    assert!(!cursor.ends_with(NAMESPACE_ENTRY_FILE));

    let second = store
        .list_namespaces_page(bucket, Some(&cursor), one)
        .await
        .expect("sparse continuation page should load");
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].namespace, namespace.public_name());
    assert!(second.next_cursor.is_none());
    assert_eq!(backend.list_call_count().await, 2);
}

#[tokio::test]
async fn object_catalog_drop_namespace_scans_all_pages_for_inactive_resources() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let prefix = store.paths.table_entries_prefix(bucket, &namespace);

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    for index in 0..TABLE_CATALOG_LIST_MAX_KEYS {
        backend
            .seed_object(RUSTFS_META_BUCKET, &format!("{prefix}0000-spacer/{index:04}.json"), Vec::new())
            .await;
    }
    let mut inactive = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    inactive.state = TableCatalogEntryState::Deleted;
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &table),
            &inactive,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("inactive table entry should be seeded after the sparse first page");
    backend.reset_call_counts().await;

    assert_matches!(
        store.drop_namespace(bucket, &namespace.public_name()).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("not empty")
    );
    assert_eq!(backend.list_call_count().await, 4);
}

#[tokio::test]
async fn object_table_catalog_store_rolls_back_warehouse_index_when_table_entry_write_fails() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let failed_table = IdentifierSegment::parse("failed_orders").unwrap();
    let next_table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &failed_table, "00001.metadata.json");
    let failed_table_path = store.paths.table_entry_path(bucket, &namespace, &failed_table);

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &failed_table_path, 1).await;

    let mut failed_entry = test_table_entry(bucket, &namespace, &failed_table, current.clone());
    failed_entry.table_id = "failed-table-id".to_string();
    failed_entry.warehouse_location = format!("s3://{bucket}/tables/shared-table");
    let error = store.create_table(failed_entry).await.unwrap_err();
    assert!(matches!(error, TableCatalogStoreError::Internal(_)));

    let mut next_entry = test_table_entry(bucket, &namespace, &next_table, current);
    next_entry.table_id = "next-table-id".to_string();
    next_entry.warehouse_location = format!("s3://{bucket}/tables/shared-table");
    store
        .create_table(next_entry)
        .await
        .expect("rolled back warehouse index should not block the next table");
}

#[tokio::test]
async fn object_table_catalog_store_keeps_table_when_drop_index_delete_fails() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend.fail_delete_attempt(RUSTFS_META_BUCKET, &index_path, 1).await;
    backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;

    let error = store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .unwrap_err();

    assert!(matches!(error, TableCatalogStoreError::Internal(_)));
    let retained = store
        .load_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("retained table lookup should succeed")
        .expect("table entry should remain present");
    assert_eq!(retained.table_id, "table-id");
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("retained table data-plane lookup should succeed")
        .expect("retained table should keep data-plane protection");
    assert_eq!(resource.table, "orders");
}

#[tokio::test]
async fn object_table_catalog_store_rejects_drop_when_warehouse_index_owner_changed() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    let conflicting_index = TableWarehouseIndexEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: "finance".to_string(),
        table: "returns".to_string(),
        table_id: "other-table-id".to_string(),
        warehouse_object_prefix: "tables/table-id/".to_string(),
        state: TableCatalogEntryState::Active,
    };
    store
        .write_entry(store.catalog_bucket(), &index_path, &conflicting_index, TableCatalogPutPrecondition::Any)
        .await
        .expect("conflicting warehouse index should be seeded");

    assert_matches!(
        store.drop_table(bucket, &namespace.public_name(), table.as_str()).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("owner changed")
    );
    let retained = store
        .load_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("retained table lookup should succeed")
        .expect("table entry should remain present");
    assert_eq!(retained.table_id, "table-id");
    let (retained_index, _) = store
        .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("conflicting warehouse index lookup should succeed")
        .expect("conflicting warehouse index should remain present");
    assert_eq!(retained_index, conflicting_index);
}

#[tokio::test]
async fn object_table_catalog_store_drops_table_when_warehouse_index_is_missing() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    let entry = store
        .load_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("table lookup should succeed")
        .expect("table entry should exist");
    store
        .delete_table_warehouse_index(&entry)
        .await
        .expect("warehouse index should be removed");
    assert!(
        store
            .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
            .await
            .expect("warehouse index lookup should succeed")
            .is_none()
    );

    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("a missing warehouse index should not block table drop");
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .expect("dropped table lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn object_table_catalog_store_restores_index_when_table_entry_delete_fails() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend.fail_delete_attempt(RUSTFS_META_BUCKET, &table_path, 1).await;

    assert_matches!(
        store.drop_table(bucket, &namespace.public_name(), table.as_str()).await,
        Err(TableCatalogStoreError::Internal(_))
    );
    let retained = store
        .load_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("retained table lookup should succeed")
        .expect("table entry should remain present");
    assert_eq!(retained.table_id, "table-id");
    let (restored_index, _) = store
        .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("restored warehouse index lookup should succeed")
        .expect("warehouse index should be restored");
    assert_eq!(restored_index.table_id, "table-id");
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("restored index lookup should succeed")
        .expect("restored index should keep data-plane protection");
    assert_eq!(resource.table, "orders");
}

#[tokio::test]
async fn object_table_catalog_store_falls_back_to_scan_when_drop_index_restore_fails() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend.fail_delete_attempt(RUSTFS_META_BUCKET, &table_path, 1).await;
    backend.fail_next_put(RUSTFS_META_BUCKET, &index_path).await;

    assert_matches!(
        store.drop_table(bucket, &namespace.public_name(), table.as_str()).await,
        Err(TableCatalogStoreError::Internal(_))
    );
    let retained = store
        .load_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("retained table lookup should succeed")
        .expect("table entry should remain present");
    assert_eq!(retained.table_id, "table-id");
    assert!(
        store
            .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
            .await
            .expect("warehouse index lookup should succeed")
            .is_none(),
        "the injected index restore failure must leave the scan fallback under test"
    );
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("catalog scan fallback should succeed")
        .expect("catalog scan fallback should keep data-plane protection");
    assert_eq!(resource.table, "orders");
}

#[tokio::test]
async fn object_table_catalog_store_accepts_ambiguous_delete_when_table_is_absent() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend.fail_after_next_delete(RUSTFS_META_BUCKET, &table_path).await;

    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("a confirmed absent table should satisfy an ambiguous delete");
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .expect("dropped table lookup should succeed")
            .is_none()
    );
    assert!(
        table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
            .await
            .expect("dropped table data-plane lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn table_data_plane_resource_bounds_deep_object_index_reads() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend.reset_call_counts().await;

    let deep_suffix = (0..100).map(|index| format!("level-{index}/")).collect::<String>();
    let object = format!("tables/table-id/{deep_suffix}part-00001.parquet");
    let resource = table_data_plane_resource_for_object(&store, bucket, &object)
        .await
        .expect("deep object lookup should succeed")
        .expect("deep object should resolve to the table");

    assert_eq!(resource.table, "orders");
    assert_eq!(backend.list_call_count().await, 0);
    assert!(backend.read_call_count().await <= WAREHOUSE_INDEX_MAX_PREFIX_DEPTH + 3);
}

#[tokio::test]
async fn object_table_catalog_store_rejects_invalid_table_warehouse_location() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");

    let mut entry = test_table_entry(bucket, &namespace, &table, current);
    entry.warehouse_location = format!("s3://{bucket}/tables/../table-id");

    let error = store.create_table(entry).await.unwrap_err();
    assert!(matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("invalid path segment")
    ));
}

#[tokio::test]
async fn object_table_catalog_store_rejects_deep_table_warehouse_location() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let deep_prefix = (0..=WAREHOUSE_INDEX_MAX_PREFIX_DEPTH)
        .map(|index| format!("level-{index}"))
        .collect::<Vec<_>>()
        .join("/");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("table should be created");

    let mut entry = test_table_entry(bucket, &namespace, &table, current);
    entry.warehouse_location = format!("s3://{bucket}/{deep_prefix}");

    let error = store.create_table(entry).await.unwrap_err();
    assert!(matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("maximum prefix depth")
    ));
}

#[tokio::test]
async fn object_table_catalog_store_rejects_invalid_view_warehouse_location() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let current = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");

    let mut entry = test_view_entry(bucket, &namespace, &view, current);
    entry.warehouse_location = format!("s3://{bucket}/views/../view-id");

    let error = store.create_view(entry).await.unwrap_err();
    assert!(matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("invalid path segment")
    ));
}

#[tokio::test]
async fn object_table_catalog_store_allows_deep_view_warehouse_location() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let view = IdentifierSegment::parse("recent_orders").unwrap();
    let current = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let next = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");
    let deep_prefix = (0..=WAREHOUSE_INDEX_MAX_PREFIX_DEPTH)
        .map(|index| format!("level-{index}"))
        .collect::<Vec<_>>()
        .join("/");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();

    let mut entry = test_view_entry(bucket, &namespace, &view, current.clone());
    entry.warehouse_location = format!("s3://{bucket}/{deep_prefix}");
    store
        .create_view(entry)
        .await
        .expect("view warehouse location should not inherit table index depth limits");

    let relocated_prefix = format!("{deep_prefix}/relocated");
    backend
        .seed_object(
            bucket,
            &next,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 1,
                "view-uuid": "view-uuid",
                "location": format!("s3://{bucket}/{relocated_prefix}")
            }))
            .expect("view metadata should serialize"),
        )
        .await;

    let result = store
        .replace_view(ViewCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            view: view.as_str().to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current,
            new_metadata_location: next,
        })
        .await
        .expect("view metadata location should not inherit table index depth limits");

    assert_eq!(result.view.warehouse_location, format!("s3://{bucket}/{relocated_prefix}"));
}

#[tokio::test]
async fn table_data_plane_resource_fails_closed_when_any_active_warehouse_location_is_invalid() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let invalid_table = IdentifierSegment::parse("bad_orders").unwrap();
    let valid_table = IdentifierSegment::parse("orders").unwrap();
    let invalid_metadata = default_table_metadata_file_path(&namespace, &invalid_table, "00001.metadata.json");
    let valid_metadata = default_table_metadata_file_path(&namespace, &valid_table, "00001.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();

    let mut invalid_entry = test_table_entry(bucket, &namespace, &invalid_table, invalid_metadata);
    invalid_entry.table_id = "bad-table-id".to_string();
    invalid_entry.warehouse_location = format!("s3://{bucket}/");
    let invalid_path = store.paths.table_entry_path(bucket, &namespace, &invalid_table);
    store
        .write_entry(
            store.catalog_bucket(),
            &invalid_path,
            &invalid_entry,
            TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .unwrap();
    let valid_entry = test_table_entry(bucket, &namespace, &valid_table, valid_metadata);
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &valid_table),
            &valid_entry,
            TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .unwrap();

    for object in ["ordinary/object.parquet", "tables/table-id/data/part-00001.parquet"] {
        let error = table_data_plane_resource_for_object(&store, bucket, object)
            .await
            .expect_err("an invalid active warehouse location must fail closed for the whole table bucket");
        assert_matches!(error, TableCatalogStoreError::Invalid(_));
    }
}

#[tokio::test]
async fn maintenance_dry_run_reports_job_context_and_deletable_candidates() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let fresh = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    backend
        .seed_object_with_mod_time(bucket, &fresh, b"{}".to_vec(), Some(OffsetDateTime::now_utc()))
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .unwrap();

    assert_eq!(report.job.table_bucket, bucket);
    assert_eq!(report.job.namespace, "sales");
    assert_eq!(report.job.table, "orders");
    assert_eq!(report.job.table_id, "table-id");
    assert_eq!(report.job.operation, TableMetadataMaintenanceOperation::DryRun);
    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
    assert_eq!(report.job.deleted_metadata_file_count, 0);
    assert_eq!(report.job.current_generation, 1);
    assert_eq!(report.job.safety_window_seconds, TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS);
    assert!(!report.job.job_id.is_empty());
    assert!(report.job.cleanup_watermark_unix_seconds <= OffsetDateTime::now_utc().unix_timestamp());
    assert_eq!(report.cleanup_candidate_locations, vec![old.clone(), fresh]);
    assert_eq!(report.deletable_metadata_locations, vec![old]);
}

#[tokio::test]
async fn maintenance_dry_run_explains_metadata_reachability() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let logged = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let fresh = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let old = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
    let recent = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00005.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &logged, b"{}".to_vec()).await;
    backend.seed_object(bucket, &recent, b"{}".to_vec()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object_with_mod_time(bucket, &fresh, b"{}".to_vec(), Some(OffsetDateTime::now_utc()))
        .await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [
                    {
                        "timestamp-ms": 1,
                        "metadata-file": logged
                    }
                ]
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 2)
        .await
        .unwrap();

    assert_eq!(report.job.planned_metadata_file_count, 5);
    assert_eq!(report.job.retained_metadata_file_count, 3);
    assert_eq!(report.job.cleanup_candidate_count, 2);
    assert_eq!(report.job.deletable_metadata_file_count, 1);
    assert_eq!(
        report.job.recommended_actions,
        vec![TableMaintenanceRecommendedAction::ReviewAndRunDelete]
    );

    let current_report = maintenance_object_report(&report, &current);
    assert_eq!(current_report.state, TableMetadataMaintenanceObjectState::Retained);
    assert_eq!(current_report.reasons, vec![TableMetadataMaintenanceReason::CurrentMetadata]);

    let logged_report = maintenance_object_report(&report, &logged);
    assert_eq!(logged_report.state, TableMetadataMaintenanceObjectState::Retained);
    assert_eq!(logged_report.reasons, vec![TableMetadataMaintenanceReason::MetadataLog]);

    let recent_report = maintenance_object_report(&report, &recent);
    assert_eq!(recent_report.state, TableMetadataMaintenanceObjectState::Retained);
    assert_eq!(recent_report.reasons, vec![TableMetadataMaintenanceReason::RecentMetadata]);

    let old_report = maintenance_object_report(&report, &old);
    assert_eq!(old_report.state, TableMetadataMaintenanceObjectState::Deletable);
    assert_eq!(
        old_report.reasons,
        vec![
            TableMetadataMaintenanceReason::NoCurrentReachability,
            TableMetadataMaintenanceReason::SafetyWindowSatisfied,
        ]
    );

    let fresh_report = maintenance_object_report(&report, &fresh);
    assert_eq!(fresh_report.state, TableMetadataMaintenanceObjectState::PendingSafetyWindow);
    assert_eq!(
        fresh_report.reasons,
        vec![
            TableMetadataMaintenanceReason::NoCurrentReachability,
            TableMetadataMaintenanceReason::SafetyWindowPending,
        ]
    );
}

#[tokio::test]
async fn maintenance_report_read_back_derives_actions_for_legacy_records() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    let mut report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    report.job.status = TableMetadataMaintenanceJobStatus::Running;
    report.job.worker_id = Some("worker-a".to_string());
    report.job.lease_id = "lease-a".to_string();
    report.job.heartbeat_at = Some(maintenance_timestamp(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)));

    let job_path = store
        .paths
        .table_maintenance_job_path(bucket, &namespace, &table, "table-id", &report.job.job_id);
    let mut legacy_report = serde_json::to_value(&report).expect("legacy report should serialize");
    legacy_report
        .get_mut("job")
        .and_then(serde_json::Value::as_object_mut)
        .expect("legacy report job should be an object")
        .remove("recommended-actions");
    store
        .write_entry(store.catalog_bucket(), &job_path, &legacy_report, TableCatalogPutPrecondition::Any)
        .await
        .expect("legacy maintenance report should be seeded");

    let loaded = store
        .get_table_metadata_maintenance_report(bucket, "sales", "orders", &report.job.job_id)
        .await
        .expect("legacy maintenance report lookup should succeed")
        .expect("legacy maintenance report should be returned");

    assert_eq!(
        loaded.job.recommended_actions,
        vec![TableMaintenanceRecommendedAction::WaitForActiveWorker]
    );
}

#[tokio::test]
async fn maintenance_state_is_scoped_to_current_table_identity() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();

    let mut first_table = test_table_entry(bucket, &namespace, &table, current.clone());
    first_table.table_id = "table-id-1".to_string();
    store.create_table(first_table).await.unwrap();
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 7,
                delete_enabled: true,
                background_enabled: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .unwrap();
    store.put_table_metadata_maintenance_report(&report).await.unwrap();
    assert!(
        store
            .get_table_metadata_maintenance_report(bucket, "sales", "orders", &report.job.job_id)
            .await
            .unwrap()
            .is_some()
    );

    store.drop_table(bucket, "sales", "orders").await.unwrap();

    let mut second_table = test_table_entry(bucket, &namespace, &table, current);
    second_table.table_id = "table-id-2".to_string();
    store.create_table(second_table).await.unwrap();

    assert_eq!(
        store.get_table_maintenance_config(bucket, "sales", "orders").await.unwrap(),
        TableMaintenanceConfig::default()
    );
    assert!(
        store
            .get_table_metadata_maintenance_report(bucket, "sales", "orders", &report.job.job_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn maintenance_config_rejects_unsupported_config_version() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

    let err = store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION.saturating_add(1),
                retain_recent_metadata_files: 1,
                delete_enabled: false,
                background_enabled: false,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

    assert_matches!(err, TableCatalogStoreError::Invalid(_));
}

#[tokio::test]
async fn maintenance_config_inherits_bucket_default_and_tracks_override_source() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    store
        .put_table_bucket_maintenance_config(
            bucket,
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 3,
                delete_enabled: true,
                background_enabled: false,
                ..Default::default()
            },
        )
        .await
        .expect("bucket default maintenance config should persist");

    let inherited = store
        .get_effective_table_maintenance_config(bucket, "sales", "orders")
        .await
        .expect("effective maintenance config should load");

    assert_eq!(inherited.source, TableMaintenanceConfigSource::TableBucketDefault);
    assert_eq!(inherited.config.retain_recent_metadata_files, 3);
    assert!(inherited.config.delete_enabled);
    assert!(!inherited.config.background_enabled);

    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 1,
                delete_enabled: false,
                background_enabled: false,
                ..Default::default()
            },
        )
        .await
        .expect("table maintenance override should persist");

    let overridden = store
        .get_effective_table_maintenance_config(bucket, "sales", "orders")
        .await
        .expect("effective maintenance override should load");

    assert_eq!(overridden.source, TableMaintenanceConfigSource::TableOverride);
    assert_eq!(overridden.config.retain_recent_metadata_files, 1);
    assert!(!overridden.config.delete_enabled);
    assert!(!overridden.config.background_enabled);
}

#[tokio::test]
async fn maintenance_config_accepts_background_enabled_worker_runtime_controls() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

    let bucket_config = store
        .put_table_bucket_maintenance_config(
            bucket,
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 1,
                delete_enabled: false,
                background_enabled: true,
                worker_paused: true,
                worker_lease_timeout_seconds: 60,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance bucket config should persist");
    assert!(bucket_config.background_enabled);
    assert!(bucket_config.worker_paused);
    assert_eq!(bucket_config.worker_lease_timeout_seconds, 60);

    let table_config = store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 1,
                delete_enabled: false,
                background_enabled: true,
                worker_paused: false,
                worker_lease_timeout_seconds: 120,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance table config should persist");
    assert!(table_config.background_enabled);
    assert!(!table_config.worker_paused);
    assert_eq!(table_config.worker_lease_timeout_seconds, 120);
}

#[tokio::test]
async fn maintenance_config_accepts_retry_and_quarantine_policy() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

    let config = store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 2,
                delete_enabled: false,
                background_enabled: false,
                max_retry_attempts: 3,
                retry_initial_backoff_seconds: 10,
                retry_max_backoff_seconds: 60,
                quarantine_enabled: true,
                quarantine_retention_seconds: 86_400,
                ..Default::default()
            },
        )
        .await
        .expect("retry and quarantine maintenance config should persist");

    assert_eq!(config.max_retry_attempts, 3);
    assert_eq!(config.retry_initial_backoff_seconds, 10);
    assert_eq!(config.retry_max_backoff_seconds, 60);
    assert!(config.quarantine_enabled);
    assert_eq!(config.quarantine_retention_seconds, 86_400);
}

#[tokio::test]
async fn maintenance_config_rejects_retry_backoff_above_limit() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;

    let initial_err = store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 1,
                delete_enabled: false,
                background_enabled: false,
                max_retry_attempts: 1,
                retry_initial_backoff_seconds: TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS.saturating_add(1),
                retry_max_backoff_seconds: TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS.saturating_add(1),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_matches!(initial_err, TableCatalogStoreError::Invalid(_));

    let max_err = store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 1,
                delete_enabled: false,
                background_enabled: false,
                max_retry_attempts: 1,
                retry_initial_backoff_seconds: TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS,
                retry_max_backoff_seconds: TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS.saturating_add(1),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_matches!(max_err, TableCatalogStoreError::Invalid(_));
}

#[tokio::test]
async fn maintenance_run_rejects_existing_invalid_retry_config_before_scheduling() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    let config_path = store
        .paths
        .table_maintenance_config_path(bucket, &namespace, &table, "table-id");
    store
        .write_entry(
            store.catalog_bucket(),
            &config_path,
            &TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 1,
                delete_enabled: false,
                background_enabled: false,
                max_retry_attempts: 1,
                retry_initial_backoff_seconds: u64::MAX,
                retry_max_backoff_seconds: u64::MAX,
                ..Default::default()
            },
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("invalid legacy maintenance config should be seeded");

    let err = store
        .run_table_metadata_maintenance(bucket, "sales", "orders", true, Some("worker-a".to_string()))
        .await
        .unwrap_err();

    assert_matches!(err, TableCatalogStoreError::Invalid(_));
}

#[tokio::test]
async fn maintenance_run_persists_latest_job_alias_with_worker_and_lease_context() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    store
        .put_table_bucket_maintenance_config(
            bucket,
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 0,
                delete_enabled: false,
                background_enabled: false,
                ..Default::default()
            },
        )
        .await
        .expect("bucket default maintenance config should persist");
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let report = store
        .run_table_metadata_maintenance(bucket, "sales", "orders", false, Some("worker-a".to_string()))
        .await
        .expect("metadata maintenance run should succeed");

    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
    assert_eq!(report.job.config_source, TableMaintenanceConfigSource::TableBucketDefault);
    assert_eq!(report.job.worker_id.as_deref(), Some("worker-a"));
    assert!(!report.job.lease_id.is_empty());
    assert_eq!(report.job.attempt, 1);
    assert_eq!(report.job.max_retry_attempts, 0);
    assert!(report.job.next_retry_after.is_none());
    assert!(report.job.heartbeat_at.is_some());
    assert!(report.job.started_at.is_some());
    assert!(report.job.finished_at.is_some());

    let latest = store
        .get_table_metadata_maintenance_report(bucket, "sales", "orders", "latest")
        .await
        .expect("latest maintenance lookup should succeed")
        .expect("latest maintenance job should be stored");
    let current_alias = store
        .get_table_metadata_maintenance_report(bucket, "sales", "orders", "current")
        .await
        .expect("current maintenance lookup should succeed")
        .expect("current maintenance job should be stored");

    assert_eq!(latest.job.job_id, report.job.job_id);
    assert_eq!(current_alias.job.job_id, report.job.job_id);
}

#[tokio::test]
async fn maintenance_delete_request_records_failed_job_when_delete_is_disabled() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 0,
                delete_enabled: false,
                background_enabled: false,
                max_retry_attempts: 2,
                retry_initial_backoff_seconds: 10,
                retry_max_backoff_seconds: 30,
                quarantine_enabled: true,
                quarantine_retention_seconds: 86_400,
                ..Default::default()
            },
        )
        .await
        .expect("table maintenance override should persist");
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let report = store
        .run_table_metadata_maintenance(bucket, "sales", "orders", true, Some("worker-a".to_string()))
        .await
        .expect("disabled delete request should still persist a failed maintenance job");

    assert_eq!(report.job.operation, TableMetadataMaintenanceOperation::Delete);
    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Failed);
    assert_eq!(report.job.config_source, TableMaintenanceConfigSource::TableOverride);
    assert_eq!(report.job.max_retry_attempts, 2);
    assert!(report.job.next_retry_after.is_some());
    assert!(report.job.quarantine_enabled);
    assert_eq!(report.job.quarantine_retention_seconds, 86_400);
    assert!(
        report
            .job
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("disabled"))
    );
    assert_eq!(
        report.job.recommended_actions,
        vec![
            TableMaintenanceRecommendedAction::EnableDelete,
            TableMaintenanceRecommendedAction::WaitForRetryBackoff,
        ]
    );
    assert!(backend.object_exists(bucket, &old).await.unwrap());

    let latest = store
        .get_table_metadata_maintenance_report(bucket, "sales", "orders", "latest")
        .await
        .expect("latest maintenance lookup should succeed")
        .expect("failed maintenance job should be stored");
    assert_eq!(latest.job.job_id, report.job.job_id);
    assert_eq!(latest.job.status, TableMetadataMaintenanceJobStatus::Failed);
    assert_eq!(latest.job.recommended_actions, report.job.recommended_actions);
}

#[tokio::test]
async fn maintenance_worker_run_skips_when_background_is_disabled() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    let before_worker_run = backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &table_path).await;

    let report = store
        .run_table_metadata_maintenance_worker_once(bucket, "sales", "orders", "worker-a".to_string())
        .await
        .expect("disabled background worker tick should report a safe no-op");

    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Disabled);
    assert_eq!(report.job.worker_id.as_deref(), Some("worker-a"));
    assert_eq!(
        report.job.recommended_actions,
        vec![TableMaintenanceRecommendedAction::EnableBackgroundMaintenance]
    );
    assert_eq!(report.job.deleted_metadata_file_count, 0);
    assert_eq!(
        backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &table_path).await,
        before_worker_run + 1
    );
    assert!(backend.object_exists(bucket, &old).await.unwrap());
}

#[tokio::test]
async fn maintenance_worker_run_honors_paused_config() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 0,
                delete_enabled: true,
                background_enabled: true,
                worker_paused: true,
                ..Default::default()
            },
        )
        .await
        .expect("paused background maintenance config should persist");

    let report = store
        .run_table_metadata_maintenance_worker_once(bucket, "sales", "orders", "worker-a".to_string())
        .await
        .expect("paused worker tick should report a safe no-op");

    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Paused);
    assert_eq!(report.job.operation, TableMetadataMaintenanceOperation::DryRun);
    assert_eq!(
        report.job.recommended_actions,
        vec![TableMaintenanceRecommendedAction::ResumeMaintenanceWorker]
    );
    assert_eq!(report.job.deleted_metadata_file_count, 0);
    assert!(backend.object_exists(bucket, &old).await.unwrap());
}

#[tokio::test]
async fn maintenance_worker_run_claims_table_entry_write_lock() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");
    let before_worker_run = backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &table_path).await;

    let report = store
        .run_table_metadata_maintenance_worker_once("analytics", "sales", "orders", "worker-a".to_string())
        .await
        .expect("worker run should complete");

    assert_eq!(report.job.worker_id.as_deref(), Some("worker-a"));
    assert_eq!(
        backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &table_path).await,
        before_worker_run + 2
    );
}

#[tokio::test]
async fn maintenance_scheduler_report_marks_disabled_default() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let report = store
        .get_table_maintenance_scheduler_report_at(bucket, "sales", "orders", OffsetDateTime::UNIX_EPOCH + Duration::seconds(100))
        .await
        .expect("scheduler report should load");

    assert_eq!(report.status, TableMaintenanceSchedulerStatus::Disabled);
    assert_eq!(report.config_source, TableMaintenanceConfigSource::Default);
    assert!(!report.background_enabled);
    assert_eq!(
        report.recommended_actions,
        vec![TableMaintenanceRecommendedAction::EnableBackgroundMaintenance]
    );
    assert!(report.current_job.is_none());
    assert!(report.audit_timeline.is_empty());
    assert!(!report.quarantine.active);
}

#[tokio::test]
async fn maintenance_scheduler_run_queues_one_durable_job() {
    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                retain_recent_metadata_files: 2,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");

    let result = store
        .run_table_maintenance_scheduler_once_at(bucket, "sales", "orders", "scheduler-a".to_string(), now)
        .await
        .expect("scheduler tick should queue maintenance");

    assert_eq!(result.report.job.status, TableMetadataMaintenanceJobStatus::Queued);
    assert_eq!(result.report.job.scheduler_id.as_deref(), Some("scheduler-a"));
    assert!(!result.report.job.scheduler_lease_id.is_empty());
    assert_eq!(result.report.job.retain_recent_metadata_files, 2);
    assert_eq!(result.scheduler.status, TableMaintenanceSchedulerStatus::Queued);
    assert_eq!(
        result.scheduler.current_job.as_ref().map(|job| job.job_id.as_str()),
        Some(result.report.job.job_id.as_str())
    );
    assert_eq!(
        result.report.audit_events.last().map(|event| event.action.clone()),
        Some(TableMaintenanceAuditAction::SchedulerQueued)
    );
}

#[tokio::test]
async fn maintenance_scheduler_run_persists_disabled_control_report() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let result = store
        .run_table_maintenance_scheduler_once_at(bucket, "sales", "orders", "scheduler-a".to_string(), now)
        .await
        .expect("disabled scheduler tick should persist a control report");

    assert_eq!(result.report.job.status, TableMetadataMaintenanceJobStatus::Disabled);
    assert_eq!(result.report.job.scheduler_id.as_deref(), Some("scheduler-a"));
    let stored = store
        .get_table_metadata_maintenance_report(bucket, "sales", "orders", &result.report.job.job_id)
        .await
        .expect("scheduler control report lookup should succeed")
        .expect("scheduler control report should be durable");
    assert_eq!(stored.job.job_id, result.report.job.job_id);
    assert_eq!(stored.job.status, TableMetadataMaintenanceJobStatus::Disabled);
    assert_eq!(
        stored.audit_events.last().map(|event| event.action.clone()),
        Some(TableMaintenanceAuditAction::SchedulerControl)
    );
    let scheduler = store
        .get_table_maintenance_scheduler_report_at(bucket, "sales", "orders", now)
        .await
        .expect("scheduler report should load");
    assert_eq!(scheduler.audit_timeline.len(), 1);
    assert_eq!(scheduler.audit_timeline[0].job_id, result.report.job.job_id);
}

#[tokio::test]
async fn maintenance_scheduler_run_reuses_active_queued_job() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                worker_lease_timeout_seconds: 300,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");

    let first = store
        .run_table_maintenance_scheduler_once_at(bucket, "sales", "orders", "scheduler-a".to_string(), now)
        .await
        .expect("first scheduler tick should queue maintenance");
    let second = store
        .run_table_maintenance_scheduler_once_at(
            bucket,
            "sales",
            "orders",
            "scheduler-b".to_string(),
            now + Duration::seconds(30),
        )
        .await
        .expect("second scheduler tick should reuse the queued job");

    assert_eq!(second.report.job.job_id, first.report.job.job_id);
    assert_eq!(second.report.job.scheduler_id.as_deref(), Some("scheduler-a"));
    assert_eq!(second.report.job.status, TableMetadataMaintenanceJobStatus::Queued);
    assert_eq!(second.scheduler.audit_timeline.len(), 1);
}

#[tokio::test]
async fn maintenance_worker_claims_queued_job_before_running_it() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");
    let queued = store
        .run_table_maintenance_scheduler_once_at(bucket, "sales", "orders", "scheduler-a".to_string(), now)
        .await
        .expect("scheduler tick should queue maintenance");

    let finished = store
        .run_table_metadata_maintenance_worker_once_at(
            bucket,
            "sales",
            "orders",
            "worker-a".to_string(),
            now + Duration::seconds(10),
        )
        .await
        .expect("worker tick should claim and finish the queued job");

    assert_eq!(finished.job.job_id, queued.report.job.job_id);
    assert_eq!(finished.job.status, TableMetadataMaintenanceJobStatus::Successful);
    assert_eq!(finished.job.worker_id.as_deref(), Some("worker-a"));
    assert_eq!(finished.job.attempt, 1);
    assert_eq!(finished.job.scheduler_id.as_deref(), Some("scheduler-a"));
    let actions = finished
        .audit_events
        .iter()
        .map(|event| event.action.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        actions,
        vec![
            TableMaintenanceAuditAction::Planned,
            TableMaintenanceAuditAction::SchedulerQueued,
            TableMaintenanceAuditAction::WorkerStarted,
            TableMaintenanceAuditAction::WorkerSucceeded,
        ]
    );
}

#[tokio::test]
async fn maintenance_worker_preserves_queued_dry_run_after_delete_is_enabled() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let table_root = format!("{}{}/", default_table_root_prefix(&namespace), table.as_str());
    let manifest_list = format!("{metadata_dir}/snap-10.avro");
    let manifest = format!("{metadata_dir}/manifest-10.avro");
    let data_file = format!("{table_root}data/part-00001.parquet");
    let orphan_data = format!("{table_root}data/orphan.parquet");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);
    let manifest_bytes = manifest_avro_bytes(&[(&data_file, 0)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &data_file, b"data".to_vec()).await;
    backend.seed_object(bucket, &orphan_data, b"orphan-data".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "manifest-list": manifest_list
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 10,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                delete_enabled: false,
                ..Default::default()
            },
        )
        .await
        .expect("dry-run background maintenance config should persist");
    let queued = store
        .run_table_maintenance_scheduler_once_at(bucket, "sales", "orders", "scheduler-a".to_string(), now)
        .await
        .expect("scheduler tick should queue dry-run maintenance");
    assert_eq!(queued.report.job.operation, TableMetadataMaintenanceOperation::DryRun);
    assert_eq!(queued.report.job.deletable_object_count, 1);

    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                delete_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("delete-enabled background maintenance config should persist");
    let finished = store
        .run_table_metadata_maintenance_worker_once_at(
            bucket,
            "sales",
            "orders",
            "worker-a".to_string(),
            now + Duration::seconds(10),
        )
        .await
        .expect("worker tick should preserve the queued dry-run operation");

    assert_eq!(finished.job.job_id, queued.report.job.job_id);
    assert_eq!(finished.job.operation, TableMetadataMaintenanceOperation::DryRun);
    assert_eq!(finished.job.status, TableMetadataMaintenanceJobStatus::Successful);
    assert_eq!(finished.job.deleted_object_count, 0);
    assert!(backend.object_exists(bucket, &orphan_data).await.unwrap());
}

#[tokio::test]
async fn maintenance_scheduler_run_recovers_expired_queued_job() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1000);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                worker_lease_timeout_seconds: 60,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");
    let first = store
        .run_table_maintenance_scheduler_once_at(bucket, "sales", "orders", "scheduler-a".to_string(), now)
        .await
        .expect("first scheduler tick should queue maintenance");

    let second = store
        .run_table_maintenance_scheduler_once_at(
            bucket,
            "sales",
            "orders",
            "scheduler-b".to_string(),
            now + Duration::seconds(120),
        )
        .await
        .expect("scheduler tick should recover expired queued maintenance");

    assert_ne!(second.report.job.job_id, first.report.job.job_id);
    let expired = store
        .get_table_metadata_maintenance_report(bucket, "sales", "orders", &first.report.job.job_id)
        .await
        .expect("expired queued job lookup should succeed")
        .expect("expired queued job should remain addressable");
    assert_eq!(expired.job.status, TableMetadataMaintenanceJobStatus::Failed);
    assert!(
        expired
            .job
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("scheduler lease expired"))
    );
    assert_eq!(
        expired.audit_events.last().map(|event| event.action.clone()),
        Some(TableMaintenanceAuditAction::SchedulerLeaseExpired)
    );
    assert_eq!(second.report.job.status, TableMetadataMaintenanceJobStatus::Queued);
    assert_eq!(second.report.job.scheduler_id.as_deref(), Some("scheduler-b"));
}

#[tokio::test]
async fn maintenance_scheduler_report_surfaces_active_backpressure_and_audit_timeline() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                worker_lease_timeout_seconds: 300,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");
    let mut running = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    running.job.status = TableMetadataMaintenanceJobStatus::Running;
    running.job.worker_id = Some("worker-a".to_string());
    running.job.lease_id = "lease-a".to_string();
    running.job.heartbeat_at = Some(maintenance_timestamp(now - Duration::seconds(10)));
    store
        .put_table_metadata_maintenance_report(&running)
        .await
        .expect("running maintenance report should be seeded");

    let report = store
        .get_table_maintenance_scheduler_report_at(bucket, "sales", "orders", now)
        .await
        .expect("scheduler report should load");

    assert_eq!(report.status, TableMaintenanceSchedulerStatus::Backpressured);
    assert_eq!(
        report.current_job.as_ref().map(|job| job.job_id.as_str()),
        Some(running.job.job_id.as_str())
    );
    assert_eq!(report.recommended_actions, vec![TableMaintenanceRecommendedAction::WaitForActiveWorker]);
    assert_eq!(report.audit_timeline.len(), 1);
    assert_eq!(report.audit_timeline[0].job_id, running.job.job_id);
    assert_eq!(report.audit_timeline[0].status, TableMetadataMaintenanceJobStatus::Running);
    assert_eq!(report.audit_timeline[0].worker_id.as_deref(), Some("worker-a"));
}

#[tokio::test]
async fn maintenance_scheduler_report_surfaces_quarantine_boundary() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                quarantine_enabled: true,
                quarantine_retention_seconds: 86_400,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");
    let mut failed = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    failed.job.status = TableMetadataMaintenanceJobStatus::Failed;
    failed.job.failure_reason = Some("quarantine retained failed cleanup candidates".to_string());
    failed.job.quarantine_enabled = true;
    failed.job.quarantine_retention_seconds = 86_400;
    failed.job.quarantined_object_count = 2;
    failed.job.finished_at = Some(maintenance_timestamp(OffsetDateTime::UNIX_EPOCH + Duration::seconds(90)));
    store
        .put_table_metadata_maintenance_report(&failed)
        .await
        .expect("failed maintenance report should be seeded");

    let report = store
        .get_table_maintenance_scheduler_report_at(bucket, "sales", "orders", OffsetDateTime::UNIX_EPOCH + Duration::seconds(100))
        .await
        .expect("scheduler report should load");

    assert_eq!(report.status, TableMaintenanceSchedulerStatus::Quarantined);
    assert!(report.quarantine.active);
    assert_eq!(report.quarantine.retention_seconds, 86_400);
    assert_eq!(report.quarantine.quarantined_object_count, 2);
    assert_eq!(report.quarantine.source_job_id.as_deref(), Some(failed.job.job_id.as_str()));
    assert!(
        report
            .recommended_actions
            .contains(&TableMaintenanceRecommendedAction::ReviewQuarantine)
    );
}

#[tokio::test]
async fn maintenance_quarantine_retry_clears_boundary_and_unblocks_scheduler() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let now = OffsetDateTime::now_utc();
    let (_namespace, _table, failed) =
        seed_quarantined_table_maintenance(&store, &backend, bucket, now, Some(now + Duration::seconds(300))).await;

    let result = store
        .apply_table_maintenance_quarantine_operation(
            bucket,
            "sales",
            "orders",
            &failed.job.job_id,
            TableMaintenanceQuarantineOperationRequest {
                action: TableMaintenanceQuarantineAction::Retry,
                reason: Some("operator reviewed retained candidates".to_string()),
            },
        )
        .await
        .expect("quarantine retry should update the current maintenance job");

    assert_eq!(result.action, TableMaintenanceQuarantineAction::Retry);
    assert_eq!(result.report.job.job_id, failed.job.job_id);
    assert_eq!(result.report.job.quarantined_object_count, 0);
    assert!(result.report.job.next_retry_after.is_none());
    let event = result
        .report
        .audit_events
        .last()
        .expect("quarantine retry should append an audit event");
    assert_eq!(event.action, TableMaintenanceAuditAction::QuarantineRetry);
    assert_eq!(event.actor, TableMaintenanceAuditActor::Operator);
    assert_eq!(event.reason.as_deref(), Some("operator reviewed retained candidates"));
    assert_eq!(event.before_status, Some(TableMetadataMaintenanceJobStatus::Failed));
    assert_eq!(event.after_status, Some(TableMetadataMaintenanceJobStatus::Failed));
    assert_eq!(event.before_quarantined_object_count, Some(2));
    assert_eq!(event.after_quarantined_object_count, Some(0));
    assert!(
        !result
            .report
            .job
            .recommended_actions
            .contains(&TableMaintenanceRecommendedAction::ReviewQuarantine)
    );
    assert_eq!(result.scheduler.status, TableMaintenanceSchedulerStatus::Ready);
    assert!(!result.scheduler.quarantine.active);
    let summary = result
        .scheduler
        .audit_timeline
        .iter()
        .find(|summary| summary.job_id == failed.job.job_id)
        .expect("scheduler timeline should include the retried job");
    assert_eq!(
        summary.audit_events.last().map(|event| event.action.clone()),
        Some(TableMaintenanceAuditAction::QuarantineRetry)
    );

    let current_report = store
        .get_table_metadata_maintenance_report(bucket, "sales", "orders", MAINTENANCE_JOB_ALIAS_CURRENT)
        .await
        .expect("current maintenance report should load")
        .expect("current maintenance report should exist");
    assert_eq!(current_report.job.job_id, failed.job.job_id);
    assert_eq!(current_report.job.quarantined_object_count, 0);
}

#[tokio::test]
async fn maintenance_quarantine_inspect_reports_without_mutating_current_job() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let now = OffsetDateTime::now_utc();
    let (_namespace, _table, failed) =
        seed_quarantined_table_maintenance(&store, &backend, bucket, now, Some(now + Duration::seconds(300))).await;

    let result = store
        .apply_table_maintenance_quarantine_operation(
            bucket,
            "sales",
            "orders",
            &failed.job.job_id,
            TableMaintenanceQuarantineOperationRequest {
                action: TableMaintenanceQuarantineAction::Inspect,
                reason: Some("ignored for inspect".to_string()),
            },
        )
        .await
        .expect("quarantine inspect should load the maintenance job");

    assert_eq!(result.action, TableMaintenanceQuarantineAction::Inspect);
    assert_eq!(result.report.job.job_id, failed.job.job_id);
    assert_eq!(result.report.job.quarantined_object_count, 2);
    let expected_retry_after = maintenance_timestamp(now + Duration::seconds(300));
    assert_eq!(result.report.job.next_retry_after.as_deref(), Some(expected_retry_after.as_str()));
    assert_eq!(result.scheduler.status, TableMaintenanceSchedulerStatus::RetryDeferred);

    let current_report = store
        .get_table_metadata_maintenance_report(bucket, "sales", "orders", MAINTENANCE_JOB_ALIAS_CURRENT)
        .await
        .expect("current maintenance report should load")
        .expect("current maintenance report should exist");
    assert_eq!(current_report.job.quarantined_object_count, 2);
    assert_eq!(current_report.audit_events, failed.audit_events);
}

#[tokio::test]
async fn maintenance_quarantine_release_preserves_retry_deferral() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let now = OffsetDateTime::now_utc();
    let (_namespace, _table, failed) =
        seed_quarantined_table_maintenance(&store, &backend, bucket, now, Some(now + Duration::seconds(300))).await;

    let result = store
        .apply_table_maintenance_quarantine_operation(
            bucket,
            "sales",
            "orders",
            &failed.job.job_id,
            TableMaintenanceQuarantineOperationRequest {
                action: TableMaintenanceQuarantineAction::Release,
                reason: Some("objects retained for later retry".to_string()),
            },
        )
        .await
        .expect("quarantine release should update the current maintenance job");

    assert_eq!(result.action, TableMaintenanceQuarantineAction::Release);
    assert_eq!(result.report.job.quarantined_object_count, 0);
    let expected_retry_after = maintenance_timestamp(now + Duration::seconds(300));
    assert_eq!(result.report.job.next_retry_after.as_deref(), Some(expected_retry_after.as_str()));
    assert_eq!(result.scheduler.status, TableMaintenanceSchedulerStatus::RetryDeferred);
    assert!(result.report.job.failure_reason.as_deref().is_some_and(|reason| {
        reason.contains("maintenance quarantine released by operator") && reason.contains("objects retained for later retry")
    }));
}

#[tokio::test]
async fn maintenance_quarantine_abandon_clears_boundary_and_retry() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let now = OffsetDateTime::now_utc();
    let (_namespace, _table, failed) =
        seed_quarantined_table_maintenance(&store, &backend, bucket, now, Some(now + Duration::seconds(300))).await;

    let result = store
        .apply_table_maintenance_quarantine_operation(
            bucket,
            "sales",
            "orders",
            &failed.job.job_id,
            TableMaintenanceQuarantineOperationRequest {
                action: TableMaintenanceQuarantineAction::Abandon,
                reason: Some("operator accepted retained objects".to_string()),
            },
        )
        .await
        .expect("quarantine abandon should update the current maintenance job");

    assert_eq!(result.action, TableMaintenanceQuarantineAction::Abandon);
    assert_eq!(result.report.job.quarantined_object_count, 0);
    assert!(result.report.job.next_retry_after.is_none());
    assert_eq!(result.scheduler.status, TableMaintenanceSchedulerStatus::Ready);
    assert!(result.report.job.failure_reason.as_deref().is_some_and(|reason| {
        reason.contains("maintenance quarantine abandoned by operator") && reason.contains("operator accepted retained objects")
    }));
}

#[tokio::test]
async fn maintenance_quarantine_rejects_mutating_non_current_job() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let now = OffsetDateTime::now_utc();
    let (_namespace, _table, old_failed) =
        seed_quarantined_table_maintenance(&store, &backend, bucket, now, Some(now + Duration::seconds(300))).await;
    let mut current_failed = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    current_failed.job.status = TableMetadataMaintenanceJobStatus::Failed;
    current_failed.job.quarantine_enabled = true;
    current_failed.job.quarantine_retention_seconds = 86_400;
    current_failed.job.quarantined_object_count = 1;
    store
        .put_table_metadata_maintenance_report(&current_failed)
        .await
        .expect("new current maintenance report should be seeded");

    let error = store
        .apply_table_maintenance_quarantine_operation(
            bucket,
            "sales",
            "orders",
            &old_failed.job.job_id,
            TableMaintenanceQuarantineOperationRequest {
                action: TableMaintenanceQuarantineAction::Release,
                reason: None,
            },
        )
        .await
        .expect_err("mutating a non-current quarantine job should fail");

    assert_eq!(error, TableCatalogStoreError::Conflict("maintenance job is not current".to_string()));
}

#[tokio::test]
async fn maintenance_worker_run_records_audit_timeline_events() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                background_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");

    let report = store
        .run_table_metadata_maintenance_worker_once(bucket, "sales", "orders", "worker-a".to_string())
        .await
        .expect("maintenance worker should finish");

    let actions = report
        .audit_events
        .iter()
        .map(|event| event.action.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        actions,
        vec![
            TableMaintenanceAuditAction::Planned,
            TableMaintenanceAuditAction::WorkerStarted,
            TableMaintenanceAuditAction::WorkerSucceeded,
        ]
    );
    assert_eq!(report.audit_events[1].actor, TableMaintenanceAuditActor::Worker);
    assert_eq!(report.audit_events[1].before_status, Some(TableMetadataMaintenanceJobStatus::Successful));
    assert_eq!(report.audit_events[2].after_status, Some(TableMetadataMaintenanceJobStatus::Successful));

    let scheduler = store
        .get_table_maintenance_scheduler_report(bucket, "sales", "orders")
        .await
        .expect("scheduler report should load");
    let summary = scheduler.current_job.expect("current job should be visible");
    assert_eq!(summary.job_id, report.job.job_id);
    assert_eq!(summary.audit_events, report.audit_events);
}

#[tokio::test]
async fn maintenance_heartbeat_appends_worker_audit_event() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    let mut running = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    running.job.status = TableMetadataMaintenanceJobStatus::Running;
    running.job.worker_id = Some("worker-a".to_string());
    running.job.lease_id = "lease-a".to_string();
    running.job.heartbeat_at = Some(maintenance_timestamp(now - Duration::seconds(10)));
    store
        .put_table_metadata_maintenance_report(&running)
        .await
        .expect("running maintenance report should be seeded");

    let heartbeat = store
        .heartbeat_table_metadata_maintenance_job_at(
            TableMaintenanceHeartbeatRef {
                table_bucket: bucket,
                namespace: "sales",
                table: "orders",
                job_id: &running.job.job_id,
                lease_id: "lease-a",
                worker_id: "worker-a",
            },
            now,
        )
        .await
        .expect("heartbeat should update the running job");

    let event = heartbeat.audit_events.last().expect("heartbeat should append an audit event");
    assert_eq!(event.action, TableMaintenanceAuditAction::WorkerHeartbeat);
    assert_eq!(event.actor, TableMaintenanceAuditActor::Worker);
    assert_eq!(event.before_status, Some(TableMetadataMaintenanceJobStatus::Running));
    assert_eq!(event.after_status, Some(TableMetadataMaintenanceJobStatus::Running));
}

#[tokio::test]
async fn maintenance_worker_run_defers_until_retry_after() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 0,
                delete_enabled: false,
                background_enabled: true,
                max_retry_attempts: 2,
                retry_initial_backoff_seconds: 60,
                retry_max_backoff_seconds: 60,
                ..Default::default()
            },
        )
        .await
        .expect("retry-enabled maintenance config should persist");
    let mut failed = store
        .run_table_metadata_maintenance(bucket, "sales", "orders", true, Some("worker-a".to_string()))
        .await
        .expect("delete failure should be recorded when delete is disabled");
    failed.job.next_retry_after = Some(maintenance_timestamp(now + Duration::seconds(30)));
    store
        .put_table_metadata_maintenance_report(&failed)
        .await
        .expect("failed retry report should be seeded");

    let deferred = store
        .run_table_metadata_maintenance_worker_once_at(bucket, "sales", "orders", "worker-b".to_string(), now)
        .await
        .expect("worker tick should defer while retry backoff is active");

    assert_eq!(deferred.job.job_id, failed.job.job_id);
    assert_eq!(deferred.job.status, TableMetadataMaintenanceJobStatus::Failed);
    assert_eq!(deferred.job.worker_id.as_deref(), Some("worker-a"));
    assert!(
        deferred
            .job
            .recommended_actions
            .contains(&TableMaintenanceRecommendedAction::WaitForRetryBackoff)
    );
    assert!(backend.object_exists(bucket, &old).await.unwrap());
}

#[tokio::test]
async fn maintenance_worker_run_backpressures_active_running_job() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 0,
                delete_enabled: false,
                background_enabled: true,
                worker_lease_timeout_seconds: 300,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");
    let mut running = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    running.job.status = TableMetadataMaintenanceJobStatus::Running;
    running.job.worker_id = Some("worker-a".to_string());
    running.job.lease_id = "lease-a".to_string();
    running.job.heartbeat_at = Some(maintenance_timestamp(now - Duration::seconds(10)));
    store
        .put_table_metadata_maintenance_report(&running)
        .await
        .expect("running maintenance report should be seeded");

    let report = store
        .run_table_metadata_maintenance_worker_once_at(bucket, "sales", "orders", "worker-b".to_string(), now)
        .await
        .expect("worker tick should return the active running job");

    assert_eq!(report.job.job_id, running.job.job_id);
    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Running);
    assert_eq!(report.job.worker_id.as_deref(), Some("worker-a"));
    assert_eq!(
        report.job.recommended_actions,
        vec![TableMaintenanceRecommendedAction::WaitForActiveWorker]
    );
}

#[tokio::test]
async fn maintenance_worker_run_recovers_expired_running_job() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(1000);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .put_table_maintenance_config(
            bucket,
            "sales",
            "orders",
            TableMaintenanceConfig {
                version: TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 0,
                delete_enabled: false,
                background_enabled: true,
                worker_lease_timeout_seconds: 60,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");
    let mut running = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    let expired_job_id = running.job.job_id.clone();
    running.job.status = TableMetadataMaintenanceJobStatus::Running;
    running.job.worker_id = Some("worker-a".to_string());
    running.job.lease_id = "lease-a".to_string();
    running.job.heartbeat_at = Some(maintenance_timestamp(now - Duration::seconds(120)));
    store
        .put_table_metadata_maintenance_report(&running)
        .await
        .expect("expired running maintenance report should be seeded");

    let report = store
        .run_table_metadata_maintenance_worker_once_at(bucket, "sales", "orders", "worker-b".to_string(), now)
        .await
        .expect("worker tick should recover expired running job and run again");

    assert_ne!(report.job.job_id, expired_job_id);
    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
    assert_eq!(report.job.worker_id.as_deref(), Some("worker-b"));

    let expired = store
        .get_table_metadata_maintenance_report(bucket, "sales", "orders", &expired_job_id)
        .await
        .expect("expired job lookup should succeed")
        .expect("expired job should remain addressable");
    assert_eq!(expired.job.status, TableMetadataMaintenanceJobStatus::Failed);
    assert!(
        expired
            .job
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("lease expired"))
    );
    assert_eq!(
        expired.job.recommended_actions,
        vec![TableMaintenanceRecommendedAction::InvestigateFailure]
    );
    let event = expired
        .audit_events
        .last()
        .expect("expired lease recovery should append an audit event");
    assert_eq!(event.action, TableMaintenanceAuditAction::WorkerLeaseExpired);
    assert_eq!(event.actor, TableMaintenanceAuditActor::Scheduler);
    assert_eq!(event.before_status, Some(TableMetadataMaintenanceJobStatus::Running));
    assert_eq!(event.after_status, Some(TableMetadataMaintenanceJobStatus::Failed));
}

#[tokio::test]
async fn maintenance_worker_heartbeat_updates_current_running_job() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let first = OffsetDateTime::UNIX_EPOCH + Duration::seconds(100);
    let second = OffsetDateTime::UNIX_EPOCH + Duration::seconds(130);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend
        .seed_object(
            bucket,
            &default_table_metadata_file_path(&namespace, &table, "00002.metadata.json"),
            br#"{"metadata-log":[]}"#.to_vec(),
        )
        .await;
    let mut running = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    running.job.status = TableMetadataMaintenanceJobStatus::Running;
    running.job.worker_id = Some("worker-a".to_string());
    running.job.lease_id = "lease-a".to_string();
    running.job.heartbeat_at = Some(maintenance_timestamp(first));
    let job_id = running.job.job_id.clone();
    store
        .put_table_metadata_maintenance_report(&running)
        .await
        .expect("running maintenance report should be seeded");

    let heartbeat = store
        .heartbeat_table_metadata_maintenance_job_at(
            TableMaintenanceHeartbeatRef {
                table_bucket: bucket,
                namespace: "sales",
                table: "orders",
                job_id: &job_id,
                lease_id: "lease-a",
                worker_id: "worker-a",
            },
            second,
        )
        .await
        .expect("heartbeat should update the current running job");

    assert_eq!(heartbeat.job.job_id, job_id);
    assert_eq!(heartbeat.job.status, TableMetadataMaintenanceJobStatus::Running);
    assert_eq!(heartbeat.job.heartbeat_at.as_deref(), Some(maintenance_timestamp(second).as_str()));
}

#[tokio::test]
async fn maintenance_reachability_reports_manifest_lists_as_manual_review() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let manifest_list = format!("{}/snap-10.avro", default_table_metadata_dir_path(&namespace, &table));

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend.seed_object(bucket, &manifest_list, b"avro".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [],
                "schemas": [],
                "partition-specs": [],
                "sort-orders": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "manifest-list": manifest_list
                    }
                ],
                "snapshot-log": [
                    {
                        "timestamp-ms": 1,
                        "snapshot-id": 10
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 10,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("metadata maintenance dry-run should succeed");

    assert_eq!(report.cleanup_candidate_locations, vec![old]);
    let manifest_report = report
        .referenced_object_reports
        .iter()
        .find(|object| object.object_location == manifest_list)
        .expect("manifest list should be reported as a referenced object");
    assert_eq!(manifest_report.object_kind, TableMetadataMaintenanceObjectKind::ManifestList);
    assert_eq!(manifest_report.state, TableMetadataMaintenanceObjectState::ManualReviewRequired);
    assert_eq!(
        manifest_report.reasons,
        vec![
            TableMetadataMaintenanceReason::ManifestList,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        ]
    );
    assert_eq!(
        report.reachability_graph.status,
        TableMaintenanceReachabilityGraphStatus::ManualReviewRequired
    );
    assert_eq!(report.reachability_graph.metadata_file_count, 2);
    assert_eq!(report.reachability_graph.manifest_list_count, 1);
    assert_eq!(report.reachability_graph.manual_review_count, 1);
    assert!(
        report
            .reachability_graph
            .reasons
            .contains(&TableMaintenanceReachabilityGraphReason::ManifestAvroReaderUnavailable)
    );
}

#[tokio::test]
async fn maintenance_reachability_expands_manifest_avro_references() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let table_root = format!("{}{}/", default_table_root_prefix(&namespace), table.as_str());
    let manifest_list = format!("{metadata_dir}/snap-10.avro");
    let manifest = format!("{metadata_dir}/manifest-10.avro");
    let data_file = format!("{table_root}data/part-00001.parquet");
    let delete_file = format!("{table_root}delete/pos-00001.parquet");
    let manifest_bytes = manifest_avro_bytes(&[(&data_file, 0), (&delete_file, 1)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &data_file, b"data".to_vec()).await;
    backend.seed_object(bucket, &delete_file, b"delete".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "manifest-list": manifest_list
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 10,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("metadata maintenance dry-run should succeed");

    assert_eq!(report.reachability_graph.status, TableMaintenanceReachabilityGraphStatus::Complete);
    assert_eq!(report.reachability_graph.manifest_list_count, 1);
    assert_eq!(report.reachability_graph.manifest_file_count, 1);
    assert_eq!(report.reachability_graph.data_file_count, 1);
    assert_eq!(report.reachability_graph.delete_file_count, 1);
    assert_eq!(report.reachability_graph.manual_review_count, 0);
    assert!(
        !report
            .reachability_graph
            .reasons
            .contains(&TableMaintenanceReachabilityGraphReason::ManifestAvroReaderUnavailable)
    );
    for (location, kind) in [
        (&manifest_list, TableMetadataMaintenanceObjectKind::ManifestList),
        (&manifest, TableMetadataMaintenanceObjectKind::ManifestFile),
        (&data_file, TableMetadataMaintenanceObjectKind::DataFile),
        (&delete_file, TableMetadataMaintenanceObjectKind::DeleteFile),
    ] {
        let referenced = report
            .referenced_object_reports
            .iter()
            .find(|object| object.object_location == *location)
            .expect("referenced object should be reported");
        assert_eq!(referenced.object_kind, kind);
        assert_eq!(referenced.state, TableMetadataMaintenanceObjectState::Retained);
    }
    assert!(report.cleanup_object_candidate_locations.is_empty());
    assert!(report.deletable_object_locations.is_empty());
}

#[tokio::test]
async fn maintenance_reachability_treats_v1_snapshot_manifests_as_reachable() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let data_dir = default_table_data_dir_path(&namespace, &table);
    let manifest = format!("{metadata_dir}/manifest-10.avro");
    let data_file = format!("{data_dir}/part-00001.parquet");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest, manifest_avro_bytes(&[(&data_file, 0)]))
        .await;
    backend.seed_object(bucket, &data_file, b"data".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "manifests": [manifest]
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 10,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("metadata maintenance dry-run should succeed");

    assert_eq!(report.reachability_graph.status, TableMaintenanceReachabilityGraphStatus::Complete);
    assert_eq!(report.reachability_graph.manifest_file_count, 1);
    assert_eq!(report.reachability_graph.data_file_count, 1);
    assert!(report.cleanup_object_candidate_locations.is_empty());
    assert!(report.deletable_object_locations.is_empty());
    for location in [&manifest, &data_file] {
        let referenced = report
            .referenced_object_reports
            .iter()
            .find(|object| object.object_location == *location)
            .expect("v1 manifest reference should be retained");
        assert_eq!(referenced.state, TableMetadataMaintenanceObjectState::Retained);
    }
}

#[tokio::test]
async fn maintenance_reachability_uses_table_warehouse_object_paths() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let manifest_list = "tables/table-id/metadata/snap-10.avro".to_string();
    let manifest = "tables/table-id/metadata/manifest-10.avro".to_string();
    let data_file = "tables/table-id/data/part-00001.parquet".to_string();
    let orphan_data = "tables/table-id/data/orphan.parquet".to_string();
    let manifest_bytes = manifest_avro_bytes(&[(&data_file, 0)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &data_file, b"data".to_vec()).await;
    backend.seed_object(bucket, &orphan_data, b"orphan".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "manifest-list": format!("s3://{bucket}/{manifest_list}")
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 10,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("metadata maintenance dry-run should succeed");

    assert_eq!(report.reachability_graph.status, TableMaintenanceReachabilityGraphStatus::Complete);
    assert!(
        report.referenced_object_reports.iter().any(
            |object| object.object_location == manifest_list && object.state == TableMetadataMaintenanceObjectState::Retained
        )
    );
    assert!(
        report
            .referenced_object_reports
            .iter()
            .any(|object| object.object_location == data_file && object.state == TableMetadataMaintenanceObjectState::Retained)
    );
    assert_eq!(report.cleanup_object_candidate_locations, vec![orphan_data.clone()]);
    assert_eq!(report.deletable_object_locations, vec![orphan_data]);
}

#[tokio::test]
async fn maintenance_reachability_fails_closed_when_retained_metadata_is_unreadable() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let orphan_data = format!("{}/orphan.parquet", default_table_data_dir_path(&namespace, &table));

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"not-json".to_vec()).await;
    backend.seed_object(bucket, &orphan_data, b"orphan".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [
                    {
                        "timestamp-ms": 1,
                        "metadata-file": old
                    }
                ],
                "snapshots": []
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("metadata maintenance dry-run should succeed");

    assert_eq!(
        report.reachability_graph.status,
        TableMaintenanceReachabilityGraphStatus::ManualReviewRequired
    );
    assert!(report.cleanup_object_candidate_locations.is_empty());
    assert!(report.deletable_object_locations.is_empty());
    let retained_metadata = report
        .referenced_object_reports
        .iter()
        .find(|object| object.object_location == old)
        .expect("unreadable retained metadata should be reported");
    assert_eq!(retained_metadata.object_kind, TableMetadataMaintenanceObjectKind::MetadataFile);
    assert_eq!(retained_metadata.state, TableMetadataMaintenanceObjectState::ManualReviewRequired);
    assert!(
        retained_metadata
            .reasons
            .contains(&TableMetadataMaintenanceReason::UnreadableMetadata)
    );
}

#[tokio::test]
async fn maintenance_dry_run_reports_unreachable_manifest_data_and_delete_candidates() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let table_root = format!("{}{}/", default_table_root_prefix(&namespace), table.as_str());
    let manifest_list = format!("{metadata_dir}/snap-10.avro");
    let manifest = format!("{metadata_dir}/manifest-10.avro");
    let data_file = format!("{table_root}data/part-00001.parquet");
    let orphan_manifest = format!("{metadata_dir}/manifest-orphan.avro");
    let orphan_data = format!("{table_root}data/orphan.parquet");
    let orphan_delete = format!("{table_root}delete/orphan-delete.parquet");
    let manifest_bytes = manifest_avro_bytes(&[(&data_file, 0)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &data_file, b"data".to_vec()).await;
    backend.seed_object(bucket, &orphan_manifest, manifest_avro_bytes(&[])).await;
    backend.seed_object(bucket, &orphan_data, b"orphan-data".to_vec()).await;
    backend.seed_object(bucket, &orphan_delete, b"orphan-delete".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "manifest-list": manifest_list
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 10,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("metadata maintenance dry-run should succeed");
    let candidates = report
        .cleanup_object_candidate_locations
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let deletable = report.deletable_object_locations.iter().cloned().collect::<BTreeSet<_>>();
    let expected = BTreeSet::from([orphan_data.clone(), orphan_delete.clone(), orphan_manifest.clone()]);

    assert_eq!(candidates, expected);
    assert_eq!(deletable, expected);
    assert_eq!(
        object_cleanup_report(&report, &orphan_manifest).object_kind,
        TableMetadataMaintenanceObjectKind::ManifestFile
    );
    assert_eq!(
        object_cleanup_report(&report, &orphan_data).object_kind,
        TableMetadataMaintenanceObjectKind::DataFile
    );
    assert_eq!(
        object_cleanup_report(&report, &orphan_delete).object_kind,
        TableMetadataMaintenanceObjectKind::DeleteFile
    );
}

#[tokio::test]
async fn maintenance_delete_removes_only_planned_unreachable_table_objects() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let table_root = format!("{}{}/", default_table_root_prefix(&namespace), table.as_str());
    let manifest_list = format!("{metadata_dir}/snap-10.avro");
    let manifest = format!("{metadata_dir}/manifest-10.avro");
    let data_file = format!("{table_root}data/part-00001.parquet");
    let orphan_manifest = format!("{metadata_dir}/manifest-orphan.avro");
    let orphan_data = format!("{table_root}data/orphan.parquet");
    let manifest_bytes = manifest_avro_bytes(&[(&data_file, 0)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &data_file, b"data".to_vec()).await;
    backend.seed_object(bucket, &orphan_manifest, manifest_avro_bytes(&[])).await;
    backend.seed_object(bucket, &orphan_data, b"orphan-data".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "manifest-list": manifest_list
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 10,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let deleted = store
        .delete_table_metadata_maintenance_candidates(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance delete should succeed");

    assert_eq!(
        deleted.deletable_object_locations.iter().cloned().collect::<BTreeSet<_>>(),
        BTreeSet::from([orphan_data.clone(), orphan_manifest.clone()])
    );
    assert!(backend.object_exists(bucket, &manifest_list).await.unwrap());
    assert!(backend.object_exists(bucket, &manifest).await.unwrap());
    assert!(backend.object_exists(bucket, &data_file).await.unwrap());
    assert!(!backend.object_exists(bucket, &orphan_manifest).await.unwrap());
    assert!(!backend.object_exists(bucket, &orphan_data).await.unwrap());
}

#[tokio::test]
async fn maintenance_dry_run_keeps_metadata_log_references() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let v1 = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let logged = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let v3 = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    for metadata in [&v1, &logged, &v3] {
        backend.seed_object(bucket, metadata, b"{}".to_vec()).await;
    }
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [
                    {
                        "timestamp-ms": 1,
                        "metadata-file": logged
                    }
                ]
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .unwrap();

    assert!(report.retained_metadata_locations.contains(&current));
    assert!(report.retained_metadata_locations.contains(&logged));
    assert_eq!(report.cleanup_candidate_locations, vec![v1, v3]);
}

#[tokio::test]
async fn maintenance_dry_run_keeps_metadata_for_protected_snapshot_refs() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let orphan = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let tagged = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let unreferenced = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &orphan, b"{}".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &tagged,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 10
            }))
            .unwrap(),
        )
        .await;
    backend
        .seed_object(
            bucket,
            &unreferenced,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 20
            }))
            .unwrap(),
        )
        .await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 30,
                "metadata-log": [],
                "refs": {
                    "main": {
                        "snapshot-id": 30,
                        "type": "branch"
                    },
                    "audit": {
                        "snapshot-id": 10,
                        "type": "tag"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .unwrap();

    assert!(report.retained_metadata_locations.contains(&tagged));
    assert_eq!(report.cleanup_candidate_locations, vec![orphan, unreferenced]);
}

#[tokio::test]
async fn snapshot_expiration_plan_retains_current_recent_and_protected_refs() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 30,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "timestamp-ms": 1000,
                        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-10.avro"
                    },
                    {
                        "snapshot-id": 20,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                    },
                    {
                        "snapshot-id": 30,
                        "timestamp-ms": 3000,
                        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-30.avro"
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 30,
                        "type": "branch"
                    },
                    "audit": {
                        "snapshot-id": 10,
                        "type": "tag"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_snapshot_expiration(
            bucket,
            "sales",
            "orders",
            TableSnapshotExpirationConfig {
                min_snapshots_to_keep: 1,
                max_snapshot_age_ms: 1,
            },
        )
        .await
        .expect("snapshot expiration planning should succeed");

    let current_snapshot = snapshot_expiration_report(&report, 30);
    assert_eq!(current_snapshot.state, TableSnapshotExpirationSnapshotState::Retained);
    assert!(
        current_snapshot
            .reasons
            .contains(&TableSnapshotExpirationReason::CurrentSnapshot)
    );

    let protected_snapshot = snapshot_expiration_report(&report, 10);
    assert_eq!(protected_snapshot.state, TableSnapshotExpirationSnapshotState::ManualReviewRequired);
    assert!(
        protected_snapshot
            .reasons
            .contains(&TableSnapshotExpirationReason::ProtectedSnapshotRef)
    );
    assert!(
        protected_snapshot
            .reasons
            .contains(&TableSnapshotExpirationReason::UserDefinedSnapshotRef)
    );

    let expired_snapshot = snapshot_expiration_report(&report, 20);
    assert_eq!(expired_snapshot.state, TableSnapshotExpirationSnapshotState::ExpirationCandidate);
    assert!(
        expired_snapshot
            .reasons
            .contains(&TableSnapshotExpirationReason::SnapshotAgeExpired)
    );
    assert_eq!(report.expiration_candidate_count, 1);
    assert_eq!(report.manual_review_count, 1);
}

#[tokio::test]
async fn snapshot_expiration_plan_fails_closed_for_table_retention_property_conflicts() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 20,
                "metadata-log": [],
                "properties": {
                    "history.expire.min-snapshots-to-keep": "5"
                },
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "timestamp-ms": 1000,
                        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-10.avro"
                    },
                    {
                        "snapshot-id": 20,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_snapshot_expiration(
            bucket,
            "sales",
            "orders",
            TableSnapshotExpirationConfig {
                min_snapshots_to_keep: 1,
                max_snapshot_age_ms: 1,
            },
        )
        .await
        .expect("snapshot expiration planning should succeed");

    assert_eq!(report.expiration_candidate_count, 0);
    assert_eq!(report.manual_review_count, 2);
    for snapshot in &report.snapshot_reports {
        assert_eq!(snapshot.state, TableSnapshotExpirationSnapshotState::ManualReviewRequired);
        assert!(
            snapshot
                .reasons
                .contains(&TableSnapshotExpirationReason::TableRetentionPropertyConflict)
        );
    }
}

#[tokio::test]
async fn snapshot_expiration_plan_requires_snapshot_timestamps() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 20,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-10.avro"
                    },
                    {
                        "snapshot-id": 20,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_snapshot_expiration(
            bucket,
            "sales",
            "orders",
            TableSnapshotExpirationConfig {
                min_snapshots_to_keep: 1,
                max_snapshot_age_ms: 1,
            },
        )
        .await
        .expect("snapshot expiration planning should succeed");

    let missing_timestamp = snapshot_expiration_report(&report, 10);
    assert_eq!(missing_timestamp.state, TableSnapshotExpirationSnapshotState::ManualReviewRequired);
    assert!(
        missing_timestamp
            .reasons
            .contains(&TableSnapshotExpirationReason::MissingSnapshotTimestamp)
    );
    assert_eq!(report.expiration_candidate_count, 0);
    assert_eq!(report.manual_review_count, 1);
}

#[tokio::test]
async fn compaction_plan_reports_manifest_reader_gap_without_rewrite_candidates() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 20,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 20,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_compaction(
            bucket,
            "sales",
            "orders",
            TableCompactionPlanningConfig {
                target_file_size_bytes: 512 * 1024 * 1024,
                small_file_threshold_bytes: 64 * 1024 * 1024,
                min_input_files: 2,
                max_rewrite_bytes_per_job: 1024 * 1024 * 1024,
            },
        )
        .await
        .expect("compaction planning should succeed");

    assert_eq!(report.status, TableCompactionPlanningStatus::ManualReviewRequired);
    assert_eq!(report.candidate_file_count, 0);
    assert_eq!(report.rewrite_group_count, 0);
    assert_eq!(report.manual_review_count, 1);
    let snapshot = compaction_snapshot_report(&report, 20);
    assert_eq!(snapshot.status, TableCompactionPlanningStatus::ManualReviewRequired);
    assert!(snapshot.reasons.contains(&TableCompactionPlanningReason::ManifestList));
    assert!(
        snapshot
            .reasons
            .contains(&TableCompactionPlanningReason::ManifestAvroReaderUnavailable)
    );
}

#[tokio::test]
async fn compaction_plan_reports_row_level_delete_files_without_rewrite_candidates() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let data_dir = default_table_data_dir_path(&namespace, &table);
    let delete_dir = default_table_delete_dir_path(&namespace, &table);
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
    let manifest_list = format!("{metadata_dir}/snap-20.avro");
    let manifest = format!("{metadata_dir}/manifest-20.avro");
    let data_file = format!("{data_dir}/part-left.parquet");
    let position_delete_file = format!("{delete_dir}/pos-left.parquet");
    let equality_delete_file = format!("{delete_dir}/eq-left.parquet");
    let manifest_bytes = manifest_avro_bytes(&[(&data_file, 0), (&position_delete_file, 1), (&equality_delete_file, 2)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &data_file, parquet_i32_bytes(&[1, 2])).await;
    backend
        .seed_object(bucket, &position_delete_file, b"position-delete".to_vec())
        .await;
    backend
        .seed_object(bucket, &equality_delete_file, b"equality-delete".to_vec())
        .await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 20,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 20,
                        "timestamp-ms": 2000,
                        "manifest-list": manifest_list
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
            }))
            .expect("current metadata should serialize"),
        )
        .await;

    let config = TableCompactionPlanningConfig {
        target_file_size_bytes: 512 * 1024 * 1024,
        small_file_threshold_bytes: 64 * 1024 * 1024,
        min_input_files: 2,
        max_rewrite_bytes_per_job: 1024 * 1024 * 1024,
    };
    let report = store
        .plan_table_compaction(bucket, "sales", "orders", config.clone())
        .await
        .expect("compaction planning should succeed");

    assert_eq!(report.status, TableCompactionPlanningStatus::ManualReviewRequired);
    assert_eq!(report.candidate_file_count, 1);
    assert_eq!(report.rewrite_group_count, 0);
    assert_eq!(report.manual_review_count, 1);
    assert_eq!(
        report.row_level_planning.status,
        TableRowLevelMaintenancePlanningStatus::ManualReviewRequired
    );
    assert_eq!(report.row_level_planning.delete_file_count, 2);
    assert_eq!(report.row_level_planning.position_delete_file_count, 1);
    assert_eq!(report.row_level_planning.equality_delete_file_count, 1);
    assert!(
        report
            .row_level_planning
            .reasons
            .contains(&TableRowLevelMaintenancePlanningReason::DeleteFileRewriteUnsupported)
    );
    assert!(
        report
            .row_level_planning
            .delete_files
            .iter()
            .any(|delete_file| delete_file.file_location == position_delete_file
                && delete_file.content == TableRowLevelDeleteFileContent::PositionDelete
                && delete_file.object_exists)
    );
    assert!(
        report
            .row_level_planning
            .delete_files
            .iter()
            .any(|delete_file| delete_file.file_location == equality_delete_file
                && delete_file.content == TableRowLevelDeleteFileContent::EqualityDelete
                && delete_file.object_exists)
    );
    let snapshot = compaction_snapshot_report(&report, 20);
    assert_eq!(snapshot.status, TableCompactionPlanningStatus::ManualReviewRequired);
    assert!(snapshot.reasons.contains(&TableCompactionPlanningReason::DeleteFile));
    assert!(
        snapshot
            .reasons
            .contains(&TableCompactionPlanningReason::RowLevelRewriteUnsupported)
    );

    let err = store
        .commit_table_compaction(bucket, "sales", "orders", config)
        .await
        .expect_err("delete-file compaction should fail closed");
    assert!(
        err.to_string().contains("compaction has no safe rewrite candidates"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn compaction_plan_requires_current_snapshot_metadata() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 30,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 20,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-20.avro"
                    }
                ]
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .plan_table_compaction(
            bucket,
            "sales",
            "orders",
            TableCompactionPlanningConfig {
                target_file_size_bytes: 512 * 1024 * 1024,
                small_file_threshold_bytes: 64 * 1024 * 1024,
                min_input_files: 2,
                max_rewrite_bytes_per_job: 1024 * 1024 * 1024,
            },
        )
        .await
        .expect("compaction planning should succeed");

    assert_eq!(report.status, TableCompactionPlanningStatus::ManualReviewRequired);
    assert_eq!(report.manual_review_count, 1);
    let snapshot = compaction_snapshot_report(&report, 30);
    assert!(
        snapshot
            .reasons
            .contains(&TableCompactionPlanningReason::MissingCurrentSnapshot)
    );
}

#[tokio::test]
async fn compaction_commit_rewrites_small_data_files_and_advances_pointer() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let data_dir = default_table_data_dir_path(&namespace, &table);
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
    let manifest_list = format!("{metadata_dir}/snap-20.avro");
    let manifest = format!("{metadata_dir}/manifest-20.avro");
    let left_data = format!("{data_dir}/part-left.parquet");
    let right_data = format!("{data_dir}/part-right.parquet");
    let retained_data = format!("{data_dir}/part-retained.parquet");
    let left_parquet = parquet_i32_bytes(&[1, 2]);
    let right_parquet = parquet_i32_bytes(&[3, 4]);
    let retained_values = (10..20_000).collect::<Vec<_>>();
    let retained_parquet = parquet_i32_bytes(&retained_values);
    let small_file_threshold_bytes = u64::try_from(left_parquet.len().max(right_parquet.len())).unwrap();
    let manifest_bytes = manifest_avro_bytes(&[(&left_data, 0), (&right_data, 0), (&retained_data, 0)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &left_data, left_parquet).await;
    backend.seed_object(bucket, &right_data, right_parquet).await;
    backend.seed_object(bucket, &retained_data, retained_parquet).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 2,
                "table-uuid": "table-uuid",
                "location": "s3://analytics/tables/table-id",
                "last-sequence-number": 7,
                "last-updated-ms": 2000,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 20,
                        "sequence-number": 7,
                        "timestamp-ms": 2000,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                ],
                "current-snapshot-id": 20,
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .commit_table_compaction(
            bucket,
            "sales",
            "orders",
            TableCompactionPlanningConfig {
                target_file_size_bytes: 64 * 1024,
                small_file_threshold_bytes,
                min_input_files: 2,
                max_rewrite_bytes_per_job: 128 * 1024,
            },
        )
        .await
        .expect("compaction rewrite should commit");

    assert_eq!(report.status, TableCompactionPlanningStatus::Committed);
    assert_eq!(report.candidate_file_count, 2);
    assert_eq!(report.rewrite_group_count, 1);
    assert_eq!(report.manual_review_count, 0);
    let committed_metadata = report
        .committed_metadata_location
        .as_ref()
        .expect("compaction should report committed metadata");
    assert_ne!(committed_metadata, &current);
    let rewrite_group = report.rewrite_groups.first().expect("rewrite group should be reported");
    assert_eq!(rewrite_group.input_file_locations, vec![left_data.clone(), right_data.clone()]);
    let output_file = rewrite_group
        .output_file_location
        .as_ref()
        .expect("rewrite group should include output data file");
    assert!(output_file.starts_with("s3://analytics/tables/table-id/data/"));
    let output_file_key =
        table_catalog_object_key_from_location(bucket, output_file).expect("output file should be inside the table bucket");
    assert!(output_file_key.starts_with("tables/table-id/data/"));
    let output_object = backend
        .read_object(bucket, &output_file_key)
        .await
        .unwrap()
        .expect("compacted data file should be written");
    assert_eq!(parquet_i32_values(output_object.data), vec![1, 2, 3, 4]);
    assert!(backend.object_exists(bucket, &left_data).await.unwrap());
    assert!(backend.object_exists(bucket, &right_data).await.unwrap());
    assert!(backend.object_exists(bucket, &retained_data).await.unwrap());

    let table_entry = store
        .load_table(bucket, "sales", "orders")
        .await
        .unwrap()
        .expect("table should still exist");
    assert_eq!(table_entry.metadata_location, *committed_metadata);
    assert_eq!(table_entry.generation, 2);
    let metadata_object = backend
        .read_object(bucket, committed_metadata)
        .await
        .unwrap()
        .expect("compaction metadata should be written");
    let metadata = serde_json::from_slice::<serde_json::Value>(&metadata_object.data).unwrap();
    assert_ne!(metadata.get("current-snapshot-id").and_then(serde_json::Value::as_i64), Some(20));
    assert_eq!(metadata.get("snapshots").and_then(serde_json::Value::as_array).unwrap().len(), 2);
    assert_eq!(
        metadata
            .get("metadata-log")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .last()
            .and_then(|entry| entry.get("metadata-file"))
            .and_then(serde_json::Value::as_str),
        Some(current.as_str())
    );
    assert_eq!(
        metadata
            .get("snapshot-log")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .last()
            .and_then(|entry| entry.get("snapshot-id"))
            .and_then(serde_json::Value::as_i64),
        metadata.get("current-snapshot-id").and_then(serde_json::Value::as_i64)
    );
    let current_manifest_list = metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .unwrap()
        .last()
        .and_then(|snapshot| snapshot.get("manifest-list"))
        .and_then(serde_json::Value::as_str)
        .unwrap();
    let manifest_list_object = backend
        .read_object(bucket, current_manifest_list)
        .await
        .unwrap()
        .expect("compaction manifest list should be written");
    let manifest_paths = manifest_paths_from_manifest_list_avro(&manifest_list_object.data).unwrap();
    assert_eq!(manifest_paths.len(), 1);
    let manifest_object = backend
        .read_object(bucket, &manifest_paths[0])
        .await
        .unwrap()
        .expect("compaction manifest should be written");
    let manifest_references = file_references_from_manifest_avro(&manifest_object.data).unwrap();
    let manifest_data_files = manifest_references
        .into_iter()
        .filter_map(|(location, kind)| (kind == TableMetadataMaintenanceObjectKind::DataFile).then_some(location))
        .collect::<BTreeSet<_>>();
    assert!(manifest_data_files.contains(output_file));
    assert!(manifest_data_files.contains(&retained_data));
    assert!(!manifest_data_files.contains(&left_data));
    assert!(!manifest_data_files.contains(&right_data));
}

#[tokio::test]
async fn compaction_commit_keeps_partition_rewrite_groups_isolated() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let data_dir = "tables/table-id/data";
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
    let manifest_list = format!("{metadata_dir}/snap-20.avro");
    let manifest = format!("{metadata_dir}/manifest-20.avro");
    let left_data = format!("{data_dir}/dt=2026-06-24/part-left.parquet");
    let right_data = format!("{data_dir}/dt=2026-06-24/part-right.parquet");
    let other_partition_data = format!("{data_dir}/dt=2026-06-25/part-only.parquet");
    let left_parquet = parquet_i32_bytes(&[1, 2]);
    let right_parquet = parquet_i32_bytes(&[3, 4]);
    let other_partition_parquet = parquet_i32_bytes(&[5, 6]);
    let small_file_threshold_bytes =
        u64::try_from(left_parquet.len().max(right_parquet.len()).max(other_partition_parquet.len())).unwrap();
    let manifest_bytes = manifest_avro_bytes_with_dt_partition(&[
        (&left_data, 0, "2026-06-24"),
        (&right_data, 0, "2026-06-24"),
        (&other_partition_data, 0, "2026-06-25"),
    ]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &left_data, left_parquet).await;
    backend.seed_object(bucket, &right_data, right_parquet).await;
    backend
        .seed_object(bucket, &other_partition_data, other_partition_parquet)
        .await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 2,
                "table-uuid": "table-uuid",
                "location": "s3://analytics/tables/table-id",
                "last-sequence-number": 7,
                "last-updated-ms": 2000,
                "schemas": [
                    {
                        "schema-id": 0,
                        "type": "struct",
                        "fields": [
                            {"id": 1, "name": "id", "required": true, "type": "int"},
                            {"id": 2, "name": "dt", "required": false, "type": "string"}
                        ]
                    }
                ],
                "current-schema-id": 0,
                "partition-specs": [
                    {
                        "spec-id": 0,
                        "fields": [
                            {
                                "source-id": 2,
                                "field-id": 1000,
                                "name": "dt",
                                "transform": "identity"
                            }
                        ]
                    }
                ],
                "default-spec-id": 0,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 20,
                        "sequence-number": 7,
                        "timestamp-ms": 2000,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                ],
                "current-snapshot-id": 20,
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .commit_table_compaction(
            bucket,
            "sales",
            "orders",
            TableCompactionPlanningConfig {
                target_file_size_bytes: 64 * 1024,
                small_file_threshold_bytes,
                min_input_files: 2,
                max_rewrite_bytes_per_job: 128 * 1024,
            },
        )
        .await
        .expect("partition-local compaction rewrite should commit");

    assert_eq!(report.status, TableCompactionPlanningStatus::Committed);
    assert_eq!(report.candidate_file_count, 3);
    assert_eq!(report.rewrite_group_count, 1);
    let rewrite_group = report.rewrite_groups.first().expect("rewrite group should be reported");
    assert_eq!(rewrite_group.input_file_locations, vec![left_data.clone(), right_data.clone()]);
    let output_file = rewrite_group
        .output_file_location
        .as_ref()
        .expect("rewrite group should include output data file");
    assert!(output_file.starts_with("s3://analytics/tables/table-id/data/dt=2026-06-24/"));
    let output_file_key =
        table_catalog_object_key_from_location(bucket, output_file).expect("output file should be inside the table bucket");
    let output_object = backend
        .read_object(bucket, &output_file_key)
        .await
        .unwrap()
        .expect("compacted partition data file should be written");
    assert_eq!(parquet_i32_values(output_object.data), vec![1, 2, 3, 4]);
    assert!(backend.object_exists(bucket, &other_partition_data).await.unwrap());

    let table_entry = store
        .load_table(bucket, "sales", "orders")
        .await
        .unwrap()
        .expect("table should still exist");
    let metadata_object = backend
        .read_object(bucket, &table_entry.metadata_location)
        .await
        .unwrap()
        .expect("compaction metadata should be written");
    let metadata = serde_json::from_slice::<serde_json::Value>(&metadata_object.data).unwrap();
    let current_manifest_list = metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .unwrap()
        .last()
        .and_then(|snapshot| snapshot.get("manifest-list"))
        .and_then(serde_json::Value::as_str)
        .unwrap();
    let manifest_list_object = backend
        .read_object(bucket, current_manifest_list)
        .await
        .unwrap()
        .expect("compaction manifest list should be written");
    let manifest_references = manifest_list_references_from_manifest_list_avro(&manifest_list_object.data).unwrap();
    assert_eq!(manifest_references.len(), 1);
    assert_eq!(manifest_references[0].partition_spec_id, Some(0));
    let manifest_object = backend
        .read_object(bucket, &manifest_references[0].manifest_path)
        .await
        .unwrap()
        .expect("compaction manifest should be written");
    let data_file_references = data_file_references_from_manifest_avro(&manifest_object.data).unwrap();
    let output_reference = data_file_references
        .iter()
        .find(|reference| reference.location == *output_file)
        .expect("compacted output should be present in the manifest");
    assert_eq!(
        output_reference.partition,
        vec![("dt".to_string(), apache_avro::types::Value::String("2026-06-24".to_string()))]
    );
    let retained_reference = data_file_references
        .iter()
        .find(|reference| reference.location == other_partition_data)
        .expect("retained partition file should stay in the manifest");
    assert_eq!(
        retained_reference.partition,
        vec![("dt".to_string(), apache_avro::types::Value::String("2026-06-25".to_string()))]
    );
}

#[tokio::test]
async fn compaction_commit_preserves_sort_order_and_keeps_groups_isolated() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let data_dir = default_table_data_dir_path(&namespace, &table);
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
    let manifest_list = format!("{metadata_dir}/snap-20.avro");
    let manifest = format!("{metadata_dir}/manifest-20.avro");
    let left_data = format!("{data_dir}/part-left.parquet");
    let right_data = format!("{data_dir}/part-right.parquet");
    let other_sort_data = format!("{data_dir}/part-other-sort.parquet");
    let left_parquet = parquet_i32_bytes(&[1, 2]);
    let right_parquet = parquet_i32_bytes(&[3, 4]);
    let other_sort_parquet = parquet_i32_bytes(&[5, 6]);
    let small_file_threshold_bytes =
        u64::try_from(left_parquet.len().max(right_parquet.len()).max(other_sort_parquet.len())).unwrap();
    let manifest_bytes =
        manifest_avro_bytes_with_sort_order(&[(&left_data, 0, 7), (&right_data, 0, 7), (&other_sort_data, 0, 8)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &left_data, left_parquet).await;
    backend.seed_object(bucket, &right_data, right_parquet).await;
    backend.seed_object(bucket, &other_sort_data, other_sort_parquet).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 2,
                "table-uuid": "table-uuid",
                "location": "s3://analytics/tables/table-id",
                "last-sequence-number": 7,
                "last-updated-ms": 2000,
                "sort-orders": [
                    {"order-id": 7, "fields": []},
                    {"order-id": 8, "fields": []}
                ],
                "default-sort-order-id": 7,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 20,
                        "sequence-number": 7,
                        "timestamp-ms": 2000,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                ],
                "current-snapshot-id": 20,
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store
        .commit_table_compaction(
            bucket,
            "sales",
            "orders",
            TableCompactionPlanningConfig {
                target_file_size_bytes: 64 * 1024,
                small_file_threshold_bytes,
                min_input_files: 2,
                max_rewrite_bytes_per_job: 128 * 1024,
            },
        )
        .await
        .expect("sort-aware compaction rewrite should commit");

    assert_eq!(report.status, TableCompactionPlanningStatus::Committed);
    assert_eq!(report.candidate_file_count, 3);
    assert_eq!(report.rewrite_group_count, 1);
    let rewrite_group = report.rewrite_groups.first().expect("rewrite group should be reported");
    assert_eq!(rewrite_group.sort_order_id, Some(7));
    assert_eq!(rewrite_group.input_file_locations, vec![left_data.clone(), right_data.clone()]);
    let output_file = rewrite_group
        .output_file_location
        .as_ref()
        .expect("rewrite group should include output data file");

    let table_entry = store
        .load_table(bucket, "sales", "orders")
        .await
        .unwrap()
        .expect("table should still exist");
    let metadata_object = backend
        .read_object(bucket, &table_entry.metadata_location)
        .await
        .unwrap()
        .expect("compaction metadata should be written");
    let metadata = serde_json::from_slice::<serde_json::Value>(&metadata_object.data).unwrap();
    let current_manifest_list = metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .unwrap()
        .last()
        .and_then(|snapshot| snapshot.get("manifest-list"))
        .and_then(serde_json::Value::as_str)
        .unwrap();
    let manifest_list_object = backend
        .read_object(bucket, current_manifest_list)
        .await
        .unwrap()
        .expect("compaction manifest list should be written");
    let manifest_references = manifest_list_references_from_manifest_list_avro(&manifest_list_object.data).unwrap();
    let manifest_object = backend
        .read_object(bucket, &manifest_references[0].manifest_path)
        .await
        .unwrap()
        .expect("compaction manifest should be written");
    let data_file_references = data_file_references_from_manifest_avro(&manifest_object.data).unwrap();
    let output_reference = data_file_references
        .iter()
        .find(|reference| reference.location == *output_file)
        .expect("compacted output should be present in the manifest");
    assert_eq!(output_reference.sort_order_id, Some(7));
    let retained_reference = data_file_references
        .iter()
        .find(|reference| reference.location == other_sort_data)
        .expect("different sort order file should stay in the manifest");
    assert_eq!(retained_reference.sort_order_id, Some(8));
}

#[tokio::test]
async fn compaction_commit_rejects_schema_mismatch_without_advancing_pointer() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let data_dir = default_table_data_dir_path(&namespace, &table);
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
    let manifest_list = format!("{metadata_dir}/snap-20.avro");
    let manifest = format!("{metadata_dir}/manifest-20.avro");
    let left_data = format!("{data_dir}/part-left.parquet");
    let right_data = format!("{data_dir}/part-right.parquet");
    let manifest_bytes = manifest_avro_bytes(&[(&left_data, 0), (&right_data, 0)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &left_data, parquet_i32_bytes(&[1, 2])).await;
    backend.seed_object(bucket, &right_data, parquet_i64_bytes(&[3, 4])).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 2,
                "table-uuid": "table-uuid",
                "location": "s3://analytics/tables/table-id",
                "last-sequence-number": 7,
                "last-updated-ms": 2000,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 20,
                        "sequence-number": 7,
                        "timestamp-ms": 2000,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                ],
                "current-snapshot-id": 20
            }))
            .unwrap(),
        )
        .await;

    let error = store
        .commit_table_compaction(
            bucket,
            "sales",
            "orders",
            TableCompactionPlanningConfig {
                target_file_size_bytes: 64 * 1024,
                small_file_threshold_bytes: 32 * 1024,
                min_input_files: 2,
                max_rewrite_bytes_per_job: 128 * 1024,
            },
        )
        .await
        .expect_err("schema mismatch should reject compaction commit");

    assert!(matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("schemas must match")));
    let table_entry = store
        .load_table(bucket, "sales", "orders")
        .await
        .unwrap()
        .expect("table should still exist");
    assert_eq!(table_entry.metadata_location, current);
}

#[tokio::test]
async fn compaction_commit_rejects_deleted_manifest_entries_without_advancing_pointer() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let data_dir = default_table_data_dir_path(&namespace, &table);
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
    let manifest_list = format!("{metadata_dir}/snap-20.avro");
    let manifest = format!("{metadata_dir}/manifest-20.avro");
    let left_data = format!("{data_dir}/part-left.parquet");
    let deleted_data = format!("{data_dir}/part-deleted.parquet");
    let manifest_bytes = manifest_avro_bytes_with_status(&[(&left_data, 0, 1), (&deleted_data, 0, 2)]);

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    backend.seed_object(bucket, &left_data, parquet_i32_bytes(&[1, 2])).await;
    backend.seed_object(bucket, &deleted_data, parquet_i32_bytes(&[3, 4])).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 2,
                "table-uuid": "table-uuid",
                "location": "s3://analytics/tables/table-id",
                "last-sequence-number": 7,
                "last-updated-ms": 2000,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 20,
                        "sequence-number": 7,
                        "timestamp-ms": 2000,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "overwrite"
                        }
                    }
                ],
                "current-snapshot-id": 20
            }))
            .unwrap(),
        )
        .await;

    let error = store
        .commit_table_compaction(
            bucket,
            "sales",
            "orders",
            TableCompactionPlanningConfig {
                target_file_size_bytes: 64 * 1024,
                small_file_threshold_bytes: 32 * 1024,
                min_input_files: 2,
                max_rewrite_bytes_per_job: 128 * 1024,
            },
        )
        .await
        .expect_err("deleted manifest entries should reject compaction commit");

    assert!(matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("no safe rewrite candidates")));
    let table_entry = store
        .load_table(bucket, "sales", "orders")
        .await
        .unwrap()
        .expect("table should still exist");
    assert_eq!(table_entry.metadata_location, current);
}

#[tokio::test]
async fn maintenance_dry_run_keeps_recent_metadata_files_and_ignores_non_metadata_objects() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let v1 = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let v2 = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let recent = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
    let manifest = format!("{}/snap-1.avro", default_table_metadata_dir_path(&namespace, &table));

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    for metadata in [&v1, &v2, &recent] {
        backend.seed_object(bucket, metadata, b"{}".to_vec()).await;
    }
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    backend.seed_object(bucket, &manifest, b"manifest".to_vec()).await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 2)
        .await
        .unwrap();

    assert!(report.retained_metadata_locations.contains(&recent));
    assert!(report.retained_metadata_locations.contains(&current));
    assert_eq!(report.cleanup_candidate_locations, vec![v1, v2]);
    assert!(!report.cleanup_candidate_locations.contains(&manifest));
}

#[tokio::test]
async fn maintenance_delete_removes_only_dry_run_metadata_candidates() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let retained = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
    let manifest = format!("{}/snap-1.avro", default_table_metadata_dir_path(&namespace, &table));

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend.seed_object(bucket, &retained, b"{}".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "metadata-log": [
                    {
                        "timestamp-ms": 1,
                        "metadata-file": retained
                    }
                ],
                "snapshots": [
                    {
                        "snapshot-id": 1,
                        "manifest-list": manifest
                    }
                ]
            }))
            .unwrap(),
        )
        .await;
    backend.seed_object(bucket, &manifest, manifest_list_avro_bytes(&[])).await;

    let report = store
        .delete_table_metadata_maintenance_candidates(bucket, "sales", "orders", 0)
        .await
        .unwrap();

    assert_eq!(report.cleanup_candidate_locations, vec![old.clone()]);
    assert!(report.cleanup_object_candidate_locations.is_empty());
    assert!(!backend.object_exists(bucket, &old).await.unwrap());
    assert!(backend.object_exists(bucket, &retained).await.unwrap());
    assert!(backend.object_exists(bucket, &current).await.unwrap());
    assert!(backend.object_exists(bucket, &manifest).await.unwrap());
}

#[tokio::test]
async fn maintenance_delete_skips_recent_uncommitted_metadata_candidates() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let fresh = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    backend
        .seed_object_with_mod_time(bucket, &fresh, br#"{"metadata-log":[]}"#.to_vec(), Some(OffsetDateTime::now_utc()))
        .await;

    let report = store
        .delete_table_metadata_maintenance_candidates(bucket, "sales", "orders", 0)
        .await
        .unwrap();

    assert_eq!(report.cleanup_candidate_locations, vec![old.clone()]);
    assert!(!backend.object_exists(bucket, &old).await.unwrap());
    assert!(backend.object_exists(bucket, &fresh).await.unwrap());
}

#[tokio::test]
async fn maintenance_delete_does_not_expand_beyond_planned_deletable_candidates() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let fresh = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    backend
        .seed_object_with_mod_time(bucket, &fresh, b"{}".to_vec(), Some(OffsetDateTime::now_utc()))
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .unwrap();
    assert_eq!(report.cleanup_candidate_locations, vec![old.clone(), fresh.clone()]);
    assert_eq!(report.deletable_metadata_locations, vec![old.clone()]);

    backend.seed_object(bucket, &fresh, b"{}".to_vec()).await;
    let deleted = store
        .delete_table_metadata_maintenance_report(bucket, "sales", "orders", report)
        .await
        .unwrap();

    assert_eq!(deleted.job.operation, TableMetadataMaintenanceOperation::Delete);
    assert_eq!(deleted.job.status, TableMetadataMaintenanceJobStatus::Successful);
    assert_eq!(deleted.job.deleted_metadata_file_count, 1);
    assert_eq!(deleted.cleanup_candidate_locations, vec![old.clone()]);
    assert_eq!(deleted.deletable_metadata_locations, vec![old.clone()]);
    assert_eq!(
        maintenance_object_report(&deleted, &old).state,
        TableMetadataMaintenanceObjectState::Deleted
    );
    assert_eq!(
        maintenance_object_report(&deleted, &fresh).state,
        TableMetadataMaintenanceObjectState::PendingSafetyWindow
    );
    assert!(!backend.object_exists(bucket, &old).await.unwrap());
    assert!(backend.object_exists(bucket, &fresh).await.unwrap());
}

#[tokio::test]
async fn maintenance_delete_conflicts_when_current_pointer_changes_before_delete() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let old = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let next = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .unwrap();
    assert_eq!(report.cleanup_candidate_locations, vec![old.clone()]);

    backend.seed_object(bucket, &next, br#"{"metadata-log":[]}"#.to_vec()).await;
    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-id".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current,
            new_metadata_location: next,
            requirements: Vec::new(),
            writer: Some("test".to_string()),
        })
        .await
        .unwrap();

    let err = store
        .delete_table_metadata_maintenance_report(bucket, "sales", "orders", report)
        .await
        .unwrap_err();

    assert_matches!(err, TableCatalogStoreError::Conflict(_));
    assert!(backend.object_exists(bucket, &old).await.unwrap());
}

#[tokio::test]
async fn export_catalog_entry_contains_table_identity_and_pointer() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;

    let export = store.export_table_catalog_entry(bucket, "sales", "orders").await.unwrap();

    assert_eq!(export.table_bucket.table_bucket, bucket);
    assert_eq!(export.namespace.namespace, "sales");
    assert_eq!(export.table.namespace, "sales");
    assert_eq!(export.table.table, "orders");
    assert_eq!(export.table.table_id, "table-id");
    assert_eq!(export.table.table_uuid, "table-uuid");
    assert_eq!(export.table.metadata_location, current);
    assert_eq!(export.table.version_token, "token-v1");
    assert_eq!(export.table.generation, 1);
}

#[tokio::test]
async fn export_catalog_entry_includes_backing_migration_manifest() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;

    let export = store.export_table_catalog_entry(bucket, "sales", "orders").await.unwrap();

    assert_eq!(export.backing_manifest.version, TABLE_CATALOG_BACKING_MANIFEST_VERSION);
    assert_eq!(export.backing_manifest.current.kind, TableCatalogBackingKind::ObjectBacked);
    assert_eq!(export.backing_manifest.current.authority, TableCatalogAuthority::RustfsSysObject);
    assert_eq!(
        export.backing_manifest.current.consistency,
        TableCatalogConsistencyMode::ConditionalObjectCas
    );
    assert_eq!(export.backing_manifest.current.wal.finalization_required_count, 0);
    assert_eq!(
        export.backing_manifest.migration.target_kind,
        TableCatalogBackingKind::DurableStrongSnapshot
    );
    assert_eq!(
        export.backing_manifest.migration.status,
        TableCatalogBackingMigrationStatus::ReadyToSnapshot
    );
    assert!(
        export
            .backing_manifest
            .migration
            .required_steps
            .contains(&TableCatalogBackingMigrationStep::ReplayCommitLog)
    );
    assert_eq!(
        export.backing_manifest.ha.writer_region_model,
        TableCatalogHaWriterModel::SingleActiveWriterRegion
    );
    assert!(!export.backing_manifest.ha.active_active_supported);
    let wire_manifest = serde_json::to_value(&export.backing_manifest).expect("backing manifest should serialize");
    assert_eq!(
        wire_manifest
            .pointer("/migration/target_kind")
            .and_then(serde_json::Value::as_str),
        Some("STRONG_KV_WAL")
    );
    assert!(
        wire_manifest
            .pointer("/migration/required_steps")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|steps| { steps.iter().any(|step| step.as_str() == Some("CUT_OVER_LINEARIZABLE_READS")) })
    );
}

#[tokio::test]
async fn object_table_catalog_store_serializes_namespace_drop_with_table_creation() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let metadata_location = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();

    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let create_store = store.clone();
    let table_entry = test_table_entry(bucket, &namespace, &table, metadata_location);
    let create = tokio::spawn(async move { create_store.create_table(table_entry).await });
    pause.wait_started().await;

    let namespace_path = store.paths.namespace_entry_path(bucket, &namespace);
    let namespace_lock_attempts = backend
        .write_lock_acquisition_count(RUSTFS_META_BUCKET, &namespace_path)
        .await;
    let drop_store = store.clone();
    let namespace_name = namespace.public_name();
    let drop_namespace = tokio::spawn(async move { drop_store.drop_namespace(bucket, &namespace_name).await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend
            .write_lock_acquisition_count(RUSTFS_META_BUCKET, &namespace_path)
            .await
            == namespace_lock_attempts
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("namespace drop should attempt the namespace catalog lock");
    assert!(!drop_namespace.is_finished(), "namespace drop must wait for table creation");

    pause.release();
    create.await.unwrap().unwrap();
    assert_matches!(
        drop_namespace.await.unwrap(),
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("is not empty")
    );
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn durable_strong_migration_dry_run_reports_ready_catalog_inventory() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let view = IdentifierSegment::parse("recent_orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let view_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current_metadata.clone()).await;
    store
        .create_view(test_view_entry(bucket, &namespace, &view, view_metadata))
        .await
        .unwrap();
    backend.seed_object(bucket, &next_metadata, b"{}".to_vec()).await;
    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: next_metadata,
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .unwrap();

    let report = store.plan_durable_strong_backing_migration(bucket).await.unwrap();

    assert_eq!(report.table_bucket, bucket);
    assert_eq!(report.source_kind, TableCatalogBackingKind::ObjectBacked);
    assert_eq!(report.target_kind, TableCatalogBackingKind::DurableStrongSnapshot);
    assert_eq!(report.status, TableCatalogBackingMigrationStatus::ReadyToSnapshot);
    assert_eq!(report.namespace_count, 1);
    assert_eq!(report.table_count, 1);
    assert_eq!(report.view_count, 1);
    assert_eq!(report.commit_log_count, 1);
    assert_eq!(report.idempotency_index_count, 1);
    assert_eq!(report.warehouse_prefix_count, 1);
    assert!(!report.object_backed_writes_fenced);
    assert!(!report.ready_to_enable_durable_strong);
    assert!(report.blockers.is_empty());
    assert!(
        report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::SnapshotObjectBackedCatalog)
    );
    assert!(
        !report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::EnableDurableStrongBacking)
    );
    assert_eq!(report.rollback.backing_config_key, ENV_TABLE_CATALOG_BACKING);
    assert_eq!(report.rollback.rollback_backing_value, TABLE_CATALOG_BACKING_OBJECT);
}

#[tokio::test]
async fn durable_strong_migration_preserves_table_backed_namespace() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, current_metadata).await;

    let namespace_path = object_store.paths.namespace_entry_path(bucket, &namespace);
    backend
        .delete_object(RUSTFS_META_BUCKET, &namespace_path)
        .await
        .expect("namespace marker should be removed");
    assert!(
        object_store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .expect("table-backed namespace lookup should succeed")
            .is_some()
    );
    let dry_run = object_store
        .plan_durable_strong_backing_migration(bucket)
        .await
        .expect("table-backed namespace dry run should succeed");
    assert_eq!(dry_run.namespace_count, 0);
    assert_eq!(dry_run.table_count, 1);

    let materialized = object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .expect("table-backed namespace should migrate");
    assert_eq!(materialized.namespace_count, 0);
    assert_eq!(materialized.table_count, 1);
    let strong_store = StrongTableCatalogStore::new(backend);
    assert!(
        strong_store
            .get_namespace(bucket, &namespace.public_name())
            .await
            .expect("table-backed namespace should load")
            .is_some()
    );
    assert!(
        strong_store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .expect("migrated table should load")
            .is_some()
    );
    assert_eq!(
        strong_store
            .list_namespace_children(bucket, None)
            .await
            .expect("table-backed namespace children should list")
            .iter()
            .map(|entry| entry.namespace.as_str())
            .collect::<Vec<_>>(),
        ["sales"]
    );
}

#[tokio::test]
async fn durable_strong_migration_preserves_view_backed_namespace() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let metadata_location = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");

    object_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    object_store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    object_store
        .create_view(test_view_entry(bucket, &namespace, &view, metadata_location))
        .await
        .unwrap();
    object_store.backfill_table_warehouse_index(bucket).await.unwrap();

    let namespace_path = object_store.paths.namespace_entry_path(bucket, &namespace);
    backend
        .delete_object(RUSTFS_META_BUCKET, &namespace_path)
        .await
        .expect("namespace marker should be removed");
    assert!(
        object_store
            .load_view(bucket, &namespace.public_name(), view.as_str())
            .await
            .expect("view-backed namespace lookup should succeed")
            .is_some()
    );
    let dry_run = object_store
        .plan_durable_strong_backing_migration(bucket)
        .await
        .expect("view-backed namespace dry run should succeed");
    assert_eq!(dry_run.namespace_count, 0);
    assert_eq!(dry_run.view_count, 1);

    let materialized = object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .expect("view-backed namespace should migrate");
    assert_eq!(materialized.namespace_count, 0);
    assert_eq!(materialized.view_count, 1);
    let strong_store = StrongTableCatalogStore::new(backend);
    assert!(
        strong_store
            .get_namespace(bucket, &namespace.public_name())
            .await
            .expect("view-backed namespace should load")
            .is_some()
    );
    assert!(
        strong_store
            .load_view(bucket, &namespace.public_name(), view.as_str())
            .await
            .expect("migrated view should load")
            .is_some()
    );
}

#[tokio::test]
async fn durable_strong_migration_rejects_orphan_idempotency_without_fencing_source() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, current_metadata.clone()).await;

    let orphan = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "orphan-commit".to_string(),
        idempotency_key: Some("orphan-request".to_string()),
        table_id: "table-id".to_string(),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        new_version_token: "token-v2".to_string(),
        previous_metadata_location: current_metadata.clone(),
        new_metadata_location: current_metadata,
        requirements: Vec::new(),
        status: CommitLogStatus::Committed,
        writer: None,
        created_at: None,
        updated_at: None,
    };
    let idempotency_path = object_store
        .paths
        .commit_idempotency_entry_path(bucket, "table-id", "orphan-request");
    backend
        .seed_object(RUSTFS_META_BUCKET, &idempotency_path, serde_json::to_vec(&orphan).unwrap())
        .await;

    let error = object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .unwrap_err();
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("has no commit record")
    );

    object_store
        .create_namespace(test_namespace_entry(bucket, &Namespace::parse("still_writable").unwrap()))
        .await
        .unwrap();
    object_store.put_table_bucket(test_bucket_entry("research")).await.unwrap();
}

#[tokio::test]
async fn durable_strong_migration_materializes_catalog_state_and_fences_object_writes() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let view = IdentifierSegment::parse("recent_orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let strong_metadata = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
    let view_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, current_metadata.clone()).await;
    object_store
        .create_view(test_view_entry(bucket, &namespace, &view, view_metadata))
        .await
        .unwrap();
    backend.seed_object(bucket, &next_metadata, b"{}".to_vec()).await;
    let object_commit = object_store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "object-commit".to_string(),
            idempotency_key: Some("object-request".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: next_metadata.clone(),
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .unwrap();

    let materialized = object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .unwrap();
    assert_eq!(materialized.status, TableCatalogBackingMigrationExecutionStatus::SnapshotMaterialized);
    assert_eq!(materialized.namespace_count, 1);
    assert_eq!(materialized.table_count, 1);
    assert_eq!(materialized.view_count, 1);
    assert_eq!(materialized.commit_log_count, 1);
    assert_eq!(materialized.idempotency_index_count, 1);
    assert!(materialized.object_backed_writes_fenced);
    assert!(materialized.ready_to_enable_durable_strong);

    let retry = object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .unwrap();
    assert_eq!(retry.status, TableCatalogBackingMigrationExecutionStatus::SnapshotAlreadyMaterialized);
    assert_eq!(retry.source_fingerprint, materialized.source_fingerprint);

    let migration = object_store.plan_durable_strong_backing_migration(bucket).await.unwrap();
    assert_eq!(migration.status, TableCatalogBackingMigrationStatus::SnapshotMaterialized);
    assert!(migration.object_backed_writes_fenced);
    assert!(migration.ready_to_enable_durable_strong);
    assert!(
        migration
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::EnableDurableStrongBacking)
    );

    let strong_store = StrongTableCatalogStore::new(backend.clone());
    let migrated_table = strong_store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    assert_eq!(migrated_table.metadata_location, next_metadata);
    assert_eq!(migrated_table.version_token, object_commit.table.version_token);
    assert_eq!(migrated_table.generation, 2);
    assert!(
        strong_store
            .load_view(bucket, "sales", "recent_orders")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        strong_store
            .get_commit_by_id(bucket, "table-id", "object-commit")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        strong_store
            .get_commit_by_idempotency_key(bucket, "table-id", "object-request")
            .await
            .unwrap()
            .is_some()
    );

    let object_err = object_store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "stale-object-commit".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: migrated_table.version_token.clone(),
            expected_metadata_location: migrated_table.metadata_location.clone(),
            new_metadata_location: strong_metadata.clone(),
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap_err();
    assert_matches!(object_err, TableCatalogStoreError::Conflict(message) if message.contains("writes are fenced"));

    backend.seed_object(bucket, &strong_metadata, b"{}".to_vec()).await;
    let strong_commit = strong_store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "strong-commit".to_string(),
            idempotency_key: Some("strong-request".to_string()),
            operation: "append".to_string(),
            expected_version_token: migrated_table.version_token,
            expected_metadata_location: migrated_table.metadata_location,
            new_metadata_location: strong_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap();
    assert_eq!(strong_commit.table.generation, 3);

    let cancel_err = object_store
        .cancel_durable_strong_backing_migration(bucket)
        .await
        .unwrap_err();
    assert_matches!(cancel_err, TableCatalogStoreError::Conflict(message) if message.contains("changed after materializing"));
}

#[tokio::test]
async fn durable_strong_migration_cancel_restores_object_backed_writes_before_target_changes() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, current_metadata.clone()).await;
    object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .unwrap();

    let cancelled = object_store.cancel_durable_strong_backing_migration(bucket).await.unwrap();
    assert_eq!(cancelled.status, TableCatalogBackingMigrationCancelStatus::FenceReleased);
    assert!(!cancelled.object_backed_writes_fenced);
    assert!(
        StrongTableCatalogStore::new(backend.clone())
            .load_table(bucket, "sales", "orders")
            .await
            .unwrap()
            .is_none()
    );
    object_store.put_table_bucket(test_bucket_entry("research")).await.unwrap();

    backend.seed_object(bucket, &next_metadata, b"{}".to_vec()).await;
    let result = object_store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "rollback-object-commit".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: next_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap();
    assert_eq!(result.table.generation, 2);
}

#[tokio::test]
async fn durable_strong_migration_cancel_rejects_unrelated_strong_snapshot_changes() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, current_metadata).await;
    object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .expect("migration should materialize the source bucket");

    StrongTableCatalogStore::new(backend.clone())
        .put_table_bucket(test_bucket_entry("research"))
        .await
        .expect("unrelated strong state should advance the global snapshot");

    let error = object_store
        .cancel_durable_strong_backing_migration(bucket)
        .await
        .expect_err("cancellation must not reopen object writes after any strong snapshot change");
    assert_matches!(
        error,
        TableCatalogStoreError::Conflict(message) if message.contains("snapshot advanced after materialization")
    );
    assert_matches!(
        object_store
            .create_namespace(test_namespace_entry(bucket, &Namespace::parse("blocked").unwrap()))
            .await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("writes are fenced")
    );
}

#[tokio::test]
async fn durable_strong_migration_cancel_serializes_with_strong_snapshot_writes() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, current_metadata).await;
    object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .expect("migration should materialize the source bucket");

    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let paused_put = backend.pause_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let strong_store = StrongTableCatalogStore::new(backend.clone());
    let strong_write = tokio::spawn(async move {
        strong_store
            .update_namespace_properties(
                bucket,
                &namespace.public_name(),
                NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "platform".to_string())]))
                    .expect("namespace update should validate"),
            )
            .await
    });
    paused_put.wait_started().await;

    let global_lock = object_store.paths.backing_migration_global_fence_lock_path();
    let lock_attempts = backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &global_lock).await;
    let cancel_store = object_store.clone();
    let cancel = tokio::spawn(async move { cancel_store.cancel_durable_strong_backing_migration(bucket).await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &global_lock).await == lock_attempts {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancellation should attempt the global migration write lock");
    assert!(!cancel.is_finished(), "cancellation must wait for the in-flight strong snapshot write");

    paused_put.release();
    strong_write
        .await
        .expect("strong writer task should join")
        .expect("strong snapshot write should complete");
    let error = cancel
        .await
        .expect("cancellation task should join")
        .expect_err("cancellation must reject the advanced strong snapshot");
    assert_matches!(
        error,
        TableCatalogStoreError::Conflict(message) if message.contains("changed after materializing")
    );
}

#[tokio::test]
async fn durable_strong_migration_cancel_retries_after_fence_delete_failure() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, current_metadata).await;
    object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .unwrap();

    let fence_path = object_store.paths.backing_migration_fence_path(bucket);
    backend.fail_delete_attempt(RUSTFS_META_BUCKET, &fence_path, 1).await;
    let first = object_store
        .cancel_durable_strong_backing_migration(bucket)
        .await
        .expect_err("fence deletion failure should leave object-backed writes fenced");
    assert_matches!(first, TableCatalogStoreError::Internal(message) if message.contains("injected delete failure"));
    assert!(
        StrongTableCatalogStore::new(backend.clone())
            .get_table_bucket(bucket)
            .await
            .expect("strong snapshot should remain readable")
            .is_none()
    );
    assert_matches!(
        object_store
            .create_namespace(test_namespace_entry(bucket, &Namespace::parse("blocked").unwrap()))
            .await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("writes are fenced")
    );

    let retry = object_store
        .cancel_durable_strong_backing_migration(bucket)
        .await
        .expect("cancellation should resume after the target snapshot was already removed");
    assert_eq!(retry.status, TableCatalogBackingMigrationCancelStatus::FenceReleased);
    object_store
        .create_namespace(test_namespace_entry(bucket, &Namespace::parse("unblocked").unwrap()))
        .await
        .expect("object-backed writes should resume after the retry removes the fence");
}

#[tokio::test]
async fn durable_strong_migration_waits_for_in_flight_catalog_writes() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    object_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    object_store.backfill_table_warehouse_index(bucket).await.unwrap();

    let namespace_path = object_store.paths.namespace_entry_path(bucket, &namespace);
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &namespace_path).await;
    let namespace_store = object_store.clone();
    let namespace_write = tokio::spawn(async move {
        namespace_store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
    });
    pause.wait_started().await;

    let migration_lock = object_store.paths.backing_migration_global_fence_lock_path();
    let migration_attempts = backend
        .write_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock)
        .await;
    let migration_store = object_store.clone();
    let migration = tokio::spawn(async move { migration_store.materialize_durable_strong_backing_migration(bucket).await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend
            .write_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock)
            .await
            == migration_attempts
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("migration should attempt to acquire its global write fence");
    assert!(
        !migration.is_finished(),
        "migration must wait for the catalog write's migration read permit"
    );

    pause.release();
    namespace_write.await.unwrap().unwrap();
    let report = migration.await.unwrap().unwrap();
    assert_eq!(report.namespace_count, 1);
    assert!(
        StrongTableCatalogStore::new(backend)
            .get_namespace(bucket, "sales")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn durable_strong_migration_waits_for_in_flight_maintenance_heartbeat() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let mut report = store
        .plan_table_metadata_maintenance(bucket, &namespace.public_name(), table.as_str(), 0)
        .await
        .expect("maintenance report should be planned");
    report.job.status = TableMetadataMaintenanceJobStatus::Running;
    report.job.worker_id = Some("worker-a".to_string());
    report.job.lease_id = "lease-a".to_string();
    store
        .put_table_metadata_maintenance_report(&report)
        .await
        .expect("running maintenance report should persist");

    let job_path = store
        .paths
        .table_maintenance_job_path(bucket, &namespace, &table, &report.job.table_id, &report.job.job_id);
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &job_path).await;
    let heartbeat_store = store.clone();
    let heartbeat_job_id = report.job.job_id.clone();
    let heartbeat = tokio::spawn(async move {
        heartbeat_store
            .heartbeat_table_metadata_maintenance_job(bucket, "sales", "orders", &heartbeat_job_id, "lease-a", "worker-a")
            .await
    });
    pause.wait_started().await;

    let migration_lock = store.paths.backing_migration_global_fence_lock_path();
    let migration_writes = backend
        .write_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock)
        .await;
    let migration_store = store.clone();
    let migration = tokio::spawn(async move { migration_store.materialize_durable_strong_backing_migration(bucket).await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend
            .write_lock_acquisition_count(RUSTFS_META_BUCKET, &migration_lock)
            .await
            == migration_writes
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("migration should attempt to acquire its global write fence");
    assert!(!migration.is_finished(), "migration must wait for the heartbeat's migration read permit");

    pause.release();
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, heartbeat)
        .await
        .expect("heartbeat should finish after its report write resumes")
        .expect("heartbeat task should join")
        .expect("heartbeat should succeed");
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, migration)
        .await
        .expect("migration should finish after the heartbeat releases its permit")
        .expect("migration task should join")
        .expect("migration should succeed");
}

#[tokio::test]
async fn object_only_catalog_mutations_hold_the_migration_read_permit() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let (namespace, table, report) =
        seed_quarantined_table_maintenance(&store, &backend, bucket, OffsetDateTime::UNIX_EPOCH, None).await;
    let lock_path = store.paths.backing_migration_fence_lock_path(bucket);
    let mut expected = backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await;

    store
        .put_external_catalog_bridge(ExternalCatalogBridgeEntry {
            version: TABLE_EXTERNAL_CATALOG_BRIDGE_VERSION,
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            catalog: "glue".to_string(),
            external_catalog_id: None,
            external_namespace: "sales".to_string(),
            external_table: "orders".to_string(),
            external_table_uuid: None,
            metadata_location: None,
            external_version_token: None,
            policy_mode: "rustfs-authoritative".to_string(),
            credential_mode: "rustfs-vended".to_string(),
            sync_mode: "manual".to_string(),
            rollback_strategy: "retain-current".to_string(),
            last_sync_status: None,
            last_synced_metadata_location: None,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        })
        .await
        .expect("external catalog bridge should persist");
    expected += 1;
    assert_eq!(backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await, expected);

    store
        .put_table_bucket_maintenance_config(bucket, TableMaintenanceConfig::default())
        .await
        .expect("table-bucket maintenance config should persist");
    expected += 1;
    assert_eq!(backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await, expected);

    store
        .put_table_maintenance_config(bucket, &namespace.public_name(), table.as_str(), TableMaintenanceConfig::default())
        .await
        .expect("table maintenance config should persist");
    expected += 1;
    assert_eq!(backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await, expected);

    store
        .put_table_metadata_maintenance_report(&report)
        .await
        .expect("maintenance report should persist");
    expected += 1;
    assert_eq!(backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await, expected);

    store
        .apply_table_maintenance_quarantine_operation(
            bucket,
            &namespace.public_name(),
            table.as_str(),
            &report.job.job_id,
            TableMaintenanceQuarantineOperationRequest {
                action: TableMaintenanceQuarantineAction::Retry,
                reason: Some("migration permit coverage".to_string()),
            },
        )
        .await
        .expect("quarantine retry should persist under one migration permit");
    expected += 1;
    assert_eq!(backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await, expected);

    store
        .backfill_table_warehouse_index(bucket)
        .await
        .expect("warehouse index backfill should remain idempotent");
    expected += 1;
    assert_eq!(backend.read_lock_acquisition_count(RUSTFS_META_BUCKET, &lock_path).await, expected);
}

#[tokio::test]
async fn object_catalog_rejects_misplaced_bridge_and_maintenance_state() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let bridge_path = store.paths.external_catalog_bridge_path(bucket, &namespace, &table);
    let misplaced_bridge = ExternalCatalogBridgeEntry {
        version: TABLE_EXTERNAL_CATALOG_BRIDGE_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: "invoices".to_string(),
        catalog: "glue".to_string(),
        external_catalog_id: None,
        external_namespace: "sales".to_string(),
        external_table: "orders".to_string(),
        external_table_uuid: None,
        metadata_location: None,
        external_version_token: None,
        policy_mode: "rustfs-authoritative".to_string(),
        credential_mode: "rustfs-vended".to_string(),
        sync_mode: "manual".to_string(),
        rollback_strategy: "retain-current".to_string(),
        last_sync_status: None,
        last_synced_metadata_location: None,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &bridge_path,
            serde_json::to_vec(&misplaced_bridge).expect("bridge should encode"),
        )
        .await;
    assert_matches!(
        store.get_external_catalog_bridge(bucket, "sales", "orders").await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("bridge identity")
    );

    let mut misplaced_report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("maintenance report should be planned");
    misplaced_report.job.table_id = "different-table-id".to_string();
    let current_job_path = store
        .paths
        .table_maintenance_current_job_path(bucket, &namespace, &table, "table-id");
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &current_job_path,
            serde_json::to_vec(&misplaced_report).expect("maintenance report should encode"),
        )
        .await;
    assert_matches!(
        store
            .get_table_metadata_maintenance_report(bucket, "sales", "orders", MAINTENANCE_JOB_ALIAS_CURRENT)
            .await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("maintenance report identity")
    );
}

#[tokio::test]
async fn durable_strong_migration_retry_recovers_after_fence_finalization_failure() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, current_metadata).await;

    let fence_path = object_store.paths.backing_migration_fence_path(bucket);
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &fence_path, 2).await;
    let first = object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .unwrap_err();
    assert_matches!(first, TableCatalogStoreError::Internal(message) if message.contains("injected put failure"));
    assert_matches!(
        object_store
            .create_namespace(test_namespace_entry(bucket, &Namespace::parse("blocked").unwrap()))
            .await
            .unwrap_err(),
        TableCatalogStoreError::Conflict(message) if message.contains("writes are fenced")
    );

    let retry = object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .unwrap();
    assert_eq!(retry.status, TableCatalogBackingMigrationExecutionStatus::SnapshotAlreadyMaterialized);
    assert!(retry.ready_to_enable_durable_strong);
}

#[tokio::test]
async fn durable_strong_migration_retry_recovers_after_initial_snapshot_write_failure() {
    let backend = TestCatalogObjectBackend {
        strong_runtime: Some(StrongTableCatalogRuntime::default()),
        ..TestCatalogObjectBackend::default()
    };
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    object_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    object_store.backfill_table_warehouse_index(bucket).await.unwrap();
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    backend.fail_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;

    assert_matches!(
        object_store.materialize_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("injected put failure")
    );

    let retry = object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .expect("migration retry should restore the known-absent target baseline");
    assert_eq!(retry.status, TableCatalogBackingMigrationExecutionStatus::SnapshotMaterialized);
    assert!(retry.ready_to_enable_durable_strong);
}

#[tokio::test]
async fn durable_strong_migration_cancel_recovers_after_initial_snapshot_write_failure() {
    let backend = TestCatalogObjectBackend {
        strong_runtime: Some(StrongTableCatalogRuntime::default()),
        ..TestCatalogObjectBackend::default()
    };
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    object_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    object_store.backfill_table_warehouse_index(bucket).await.unwrap();
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    backend.fail_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;

    assert_matches!(
        object_store.materialize_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("injected put failure")
    );
    assert_matches!(
        object_store
            .create_namespace(test_namespace_entry(bucket, &Namespace::parse("blocked").unwrap()))
            .await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("writes are fenced")
    );

    let cancelled = object_store
        .cancel_durable_strong_backing_migration(bucket)
        .await
        .expect("migration cancellation should restore the known-absent target baseline");
    assert_eq!(cancelled.status, TableCatalogBackingMigrationCancelStatus::FenceReleased);
    object_store
        .create_namespace(test_namespace_entry(bucket, &Namespace::parse("unblocked").unwrap()))
        .await
        .expect("object-backed writes should resume after cancellation");
}

#[tokio::test]
async fn durable_strong_migration_cancel_rejects_a_missing_materialized_snapshot() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    object_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    object_store.backfill_table_warehouse_index(bucket).await.unwrap();
    object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .expect("migration should materialize");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    backend
        .delete_object(RUSTFS_META_BUCKET, &snapshot_path)
        .await
        .expect("materialized snapshot should be removed for the fault injection");

    assert_matches!(
        object_store.cancel_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("snapshot is missing")
    );
    assert_matches!(
        object_store
            .create_namespace(test_namespace_entry(bucket, &Namespace::parse("blocked").unwrap()))
            .await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("writes are fenced")
    );
}

#[tokio::test]
async fn durable_strong_migration_cancel_fails_closed_for_a_v1_preparing_fence_without_a_snapshot() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    object_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    object_store.backfill_table_warehouse_index(bucket).await.unwrap();
    let fence_path = object_store.paths.backing_migration_fence_path(bucket);
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &fence_path,
            serde_json::to_vec(&serde_json::json!({
                "version": TABLE_CATALOG_MIGRATION_MIN_READ_VERSION,
                "table_bucket": bucket,
                "migration_id": "legacy-migration",
                "status": "PREPARING",
                "target_bucket_existed": false,
                "source_fingerprint": null,
                "target_snapshot_etag": null
            }))
            .expect("legacy migration fence should encode"),
        )
        .await;

    assert_matches!(
        object_store.cancel_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("snapshot is missing")
    );
    assert!(
        backend
            .object_exists(RUSTFS_META_BUCKET, &fence_path)
            .await
            .expect("legacy migration fence lookup should succeed")
    );
}

#[tokio::test]
async fn durable_strong_migration_rejects_unknown_bucket_and_global_fence_versions() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    object_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    object_store.backfill_table_warehouse_index(bucket).await.unwrap();
    let fence_path = object_store.paths.backing_migration_fence_path(bucket);
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &fence_path,
            serde_json::to_vec(&serde_json::json!({
                "version": TABLE_CATALOG_MIGRATION_VERSION + 1,
                "table_bucket": bucket,
                "migration_id": "future-migration",
                "status": "PREPARING",
                "target_bucket_existed": false,
                "source_fingerprint": null,
                "target_snapshot_etag": null
            }))
            .expect("future migration fence should encode"),
        )
        .await;
    assert_matches!(
        object_store.plan_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("invalid durable strong migration fence")
    );

    backend
        .delete_object(RUSTFS_META_BUCKET, &fence_path)
        .await
        .expect("future bucket fence should be removed");
    let global_fence_path = object_store.paths.backing_migration_global_fence_path();
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &global_fence_path,
            serde_json::to_vec(&serde_json::json!({
                "version": TABLE_CATALOG_MIGRATION_VERSION + 1,
                "migration_id": "future-global-migration"
            }))
            .expect("future global migration fence should encode"),
        )
        .await;
    assert_matches!(
        object_store.plan_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("invalid durable strong global migration fence")
    );
    assert_matches!(
        object_store.materialize_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("invalid durable strong global migration fence")
    );
    assert_matches!(
        object_store.cancel_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("invalid durable strong global migration fence")
    );
    assert!(
        backend
            .object_exists(RUSTFS_META_BUCKET, &global_fence_path)
            .await
            .expect("future global migration fence lookup should succeed")
    );
    assert!(
        !backend
            .object_exists(RUSTFS_META_BUCKET, &fence_path)
            .await
            .expect("bucket migration fence lookup should succeed")
    );
}

#[tokio::test]
async fn durable_strong_migration_rejects_commit_log_identity_outside_its_object_path() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current_metadata.clone()).await;
    backend.seed_object(bucket, &next_metadata, b"{}".to_vec()).await;
    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: next_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .expect("commit should succeed");

    let correct_path = store.paths.commit_log_entry_path(bucket, "table-id", "commit-1");
    let misplaced_path = store.paths.commit_log_entry_path(bucket, "table-id", "different-commit");
    let commit = backend
        .read_object(RUSTFS_META_BUCKET, &correct_path)
        .await
        .expect("commit lookup should succeed")
        .expect("commit should exist")
        .data;
    backend
        .delete_object(RUSTFS_META_BUCKET, &correct_path)
        .await
        .expect("correct commit path should be removed");
    backend.seed_object(RUSTFS_META_BUCKET, &misplaced_path, commit).await;

    assert_matches!(
        store.get_commit_by_id(bucket, "table-id", "different-commit").await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("commit log identity")
    );
    assert_matches!(
        store.plan_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("commit log identity")
    );
    assert!(
        !backend
            .object_exists(RUSTFS_META_BUCKET, &store.paths.backing_migration_fence_path(bucket))
            .await
            .expect("migration fence lookup should succeed")
    );
}

#[tokio::test]
async fn durable_strong_migration_rejects_idempotency_identity_outside_its_object_path() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current_metadata.clone()).await;
    backend.seed_object(bucket, &next_metadata, b"{}".to_vec()).await;
    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("request-1".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: next_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .expect("commit should succeed");

    let correct_path = store.paths.commit_idempotency_entry_path(bucket, "table-id", "request-1");
    let misplaced_path = store
        .paths
        .commit_idempotency_entry_path(bucket, "table-id", "different-request");
    let index = backend
        .read_object(RUSTFS_META_BUCKET, &correct_path)
        .await
        .expect("idempotency lookup should succeed")
        .expect("idempotency index should exist")
        .data;
    backend
        .delete_object(RUSTFS_META_BUCKET, &correct_path)
        .await
        .expect("correct idempotency path should be removed");
    backend.seed_object(RUSTFS_META_BUCKET, &misplaced_path, index).await;

    assert_matches!(
        store
            .get_commit_by_idempotency_key(bucket, "table-id", "different-request")
            .await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("commit idempotency identity")
    );
    assert_matches!(
        store.plan_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("commit idempotency identity")
    );
    assert!(
        !backend
            .object_exists(RUSTFS_META_BUCKET, &store.paths.backing_migration_fence_path(bucket))
            .await
            .expect("migration fence lookup should succeed")
    );
}

#[tokio::test]
async fn durable_strong_migration_cancel_preserves_preexisting_target_state() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, current_metadata).await;

    let strong_store = StrongTableCatalogStore::new(backend.clone());
    strong_store
        .put_table_bucket(object_store.get_table_bucket(bucket).await.unwrap().unwrap())
        .await
        .unwrap();
    strong_store
        .create_namespace(object_store.get_namespace(bucket, "sales").await.unwrap().unwrap())
        .await
        .unwrap();
    strong_store
        .create_table(object_store.load_table(bucket, "sales", "orders").await.unwrap().unwrap())
        .await
        .unwrap();

    let materialized = object_store
        .materialize_durable_strong_backing_migration(bucket)
        .await
        .unwrap();
    assert_eq!(
        materialized.status,
        TableCatalogBackingMigrationExecutionStatus::SnapshotAlreadyMaterialized
    );
    object_store.cancel_durable_strong_backing_migration(bucket).await.unwrap();
    assert!(strong_store.load_table(bucket, "sales", "orders").await.unwrap().is_some());
}

#[tokio::test]
async fn durable_strong_migration_requires_every_table_bucket_before_cutover() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend);
    for bucket in ["analytics", "research"] {
        object_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        object_store.backfill_table_warehouse_index(bucket).await.unwrap();
    }

    let first = object_store
        .materialize_durable_strong_backing_migration("analytics")
        .await
        .unwrap();
    assert!(!first.ready_to_enable_durable_strong);
    assert_matches!(
        object_store
            .put_table_bucket(test_bucket_entry("new-bucket"))
            .await
            .unwrap_err(),
        TableCatalogStoreError::Conflict(message) if message.contains("registry writes are fenced")
    );
    assert!(
        object_store
            .plan_durable_strong_backing_migration("analytics")
            .await
            .unwrap()
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::SnapshotRemainingTableBuckets)
    );

    let second = object_store
        .materialize_durable_strong_backing_migration("research")
        .await
        .unwrap();
    assert!(second.ready_to_enable_durable_strong);
    assert!(
        object_store
            .plan_durable_strong_backing_migration("analytics")
            .await
            .unwrap()
            .ready_to_enable_durable_strong
    );
}

#[tokio::test]
async fn durable_strong_migration_rejects_target_buckets_outside_the_source_inventory() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    object_store
        .put_table_bucket(test_bucket_entry("analytics"))
        .await
        .expect("source table bucket should be created");
    object_store
        .backfill_table_warehouse_index("analytics")
        .await
        .expect("source warehouse index should be ready");
    StrongTableCatalogStore::new(backend.clone())
        .put_table_bucket(test_bucket_entry("stale-target"))
        .await
        .expect("stale target table bucket should be seeded");

    let report = object_store
        .plan_durable_strong_backing_migration("analytics")
        .await
        .expect("migration preflight should report the stale target state");
    assert_eq!(report.status, TableCatalogBackingMigrationStatus::ManualReviewRequired);
    assert!(
        report
            .blockers
            .contains(&TableCatalogBackingMigrationBlocker::DurableStrongSnapshotChanged)
    );
    assert!(
        report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::ReviewDurableStrongSnapshot)
    );
    assert_matches!(
        object_store
            .materialize_durable_strong_backing_migration("analytics")
            .await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("not ready")
    );
    assert!(
        !backend
            .object_exists(RUSTFS_META_BUCKET, &object_store.paths.backing_migration_fence_path("analytics"),)
            .await
            .expect("migration fence lookup should succeed")
    );
}

#[tokio::test]
async fn durable_strong_migration_dry_run_reports_recovery_blockers() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current_metadata.clone()).await;
    backend.seed_object(bucket, &next_metadata, b"{}".to_vec()).await;
    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: next_metadata,
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .unwrap();
    let idempotency_path = store
        .paths
        .commit_idempotency_entry_path(bucket, "table-id", "client-request");
    backend.delete_object(RUSTFS_META_BUCKET, &idempotency_path).await.unwrap();
    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.namespace_entry_path(bucket, &namespace))
        .await
        .unwrap();

    let report = store.plan_durable_strong_backing_migration(bucket).await.unwrap();

    assert_eq!(report.status, TableCatalogBackingMigrationStatus::RecoveryRequired);
    assert_eq!(report.namespace_count, 0);
    assert_eq!(report.table_count, 1);
    assert_eq!(report.commit_log_count, 1);
    assert_eq!(report.idempotency_index_count, 0);
    assert!(
        report
            .blockers
            .contains(&TableCatalogBackingMigrationBlocker::CommitRecoveryRequired)
    );
    assert!(
        report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::RunCatalogRecovery)
    );
    assert!(
        !report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::SnapshotObjectBackedCatalog)
    );
    assert!(
        !report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::EnableDurableStrongBacking)
    );
    assert!(
        !report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::VerifyDurableStrongSnapshot)
    );
}

#[tokio::test]
async fn durable_strong_migration_dry_run_requires_ready_warehouse_index() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current_metadata).await;
    let state_path = store.paths.warehouse_index_state_path(bucket);
    backend.delete_object(RUSTFS_META_BUCKET, &state_path).await.unwrap();

    let report = store.plan_durable_strong_backing_migration(bucket).await.unwrap();

    assert_eq!(report.status, TableCatalogBackingMigrationStatus::RecoveryRequired);
    assert!(!report.warehouse_index_ready);
    assert!(
        report
            .blockers
            .contains(&TableCatalogBackingMigrationBlocker::WarehouseIndexBackfillRequired)
    );
    assert!(
        report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::BackfillWarehouseIndex)
    );
    assert!(
        !report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::SnapshotObjectBackedCatalog)
    );
    assert!(
        !report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::EnableDurableStrongBacking)
    );
    assert!(
        !report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::VerifyDurableStrongSnapshot)
    );
}

#[tokio::test]
async fn durable_strong_migration_rejects_overlapping_warehouse_prefixes_before_fencing() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let parent = IdentifierSegment::parse("orders").expect("table should parse");
    let child = IdentifierSegment::parse("order_items").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &parent, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &parent, current_metadata).await;

    let mut child_entry = test_table_entry(
        bucket,
        &namespace,
        &child,
        default_table_metadata_file_path(&namespace, &child, "00001.metadata.json"),
    );
    child_entry.table_id = "table-id-child".to_string();
    child_entry.table_uuid = "table-uuid-child".to_string();
    child_entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child");
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &child),
            &child_entry,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("legacy overlapping table should be seeded");

    assert_matches!(
        scan_table_data_plane_resource_for_object(
            &store,
            bucket,
            "tables/table-id/child/data/part-00001.parquet",
        )
        .await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("overlapping active table warehouse prefixes")
    );

    let report = store
        .plan_durable_strong_backing_migration(bucket)
        .await
        .expect("migration dry run should report the overlap");

    assert_eq!(report.status, TableCatalogBackingMigrationStatus::ManualReviewRequired);
    assert!(report.warehouse_index_ready);
    assert_eq!(report.warehouse_prefix_count, 2);
    assert!(
        report
            .blockers
            .contains(&TableCatalogBackingMigrationBlocker::DuplicateWarehousePrefix)
    );
    assert!(
        report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::ReviewDuplicateWarehousePrefixes)
    );
    assert_matches!(
        store.materialize_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    assert!(
        !backend
            .object_exists(RUSTFS_META_BUCKET, &store.paths.backing_migration_fence_path(bucket))
            .await
            .expect("migration fence lookup should succeed")
    );
}

#[tokio::test]
async fn durable_strong_migration_rejects_committed_log_outside_current_history() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current_metadata.clone()).await;
    let commit = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "commit-1".to_string(),
        idempotency_key: None,
        table_id: "table-id".to_string(),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        new_version_token: "token-v2".to_string(),
        previous_metadata_location: current_metadata,
        new_metadata_location: new_metadata,
        requirements: Vec::new(),
        status: CommitLogStatus::Committed,
        writer: None,
        created_at: None,
        updated_at: None,
    };
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.commit_log_entry_path(bucket, "table-id", "commit-1"),
            &commit,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("disconnected committed log should be seeded");

    let report = store
        .plan_durable_strong_backing_migration(bucket)
        .await
        .expect("migration dry run should report disconnected history");

    assert_eq!(report.status, TableCatalogBackingMigrationStatus::ManualReviewRequired);
    assert!(
        report
            .blockers
            .contains(&TableCatalogBackingMigrationBlocker::CommitManualReviewRequired)
    );
    assert_matches!(
        store.materialize_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("not ready")
    );
    assert!(
        !backend
            .object_exists(RUSTFS_META_BUCKET, &store.paths.backing_migration_fence_path(bucket))
            .await
            .expect("migration fence lookup should succeed")
    );
}

#[tokio::test]
async fn durable_strong_migration_rejects_duplicate_table_ids_before_fencing() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let orders = IdentifierSegment::parse("orders").expect("table should parse");
    let invoices = IdentifierSegment::parse("invoices").expect("table should parse");
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(
            bucket,
            &namespace,
            &orders,
            default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json"),
        ))
        .await
        .unwrap();
    let mut duplicate = test_table_entry(
        bucket,
        &namespace,
        &invoices,
        default_table_metadata_file_path(&namespace, &invoices, "00001.metadata.json"),
    );
    duplicate.table_uuid = "duplicate-table-uuid".to_string();
    duplicate.warehouse_location = format!("s3://{bucket}/tables/duplicate-table-location");
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.table_entry_path(bucket, &namespace, &invoices),
            &duplicate,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("legacy duplicate table id should be seeded");

    let report = store.plan_durable_strong_backing_migration(bucket).await.unwrap();

    assert_eq!(report.status, TableCatalogBackingMigrationStatus::ManualReviewRequired);
    assert!(
        report
            .blockers
            .contains(&TableCatalogBackingMigrationBlocker::DuplicateTableIdentity)
    );
    assert!(
        report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::ReviewDuplicateTableIdentities)
    );
    assert_matches!(
        store.materialize_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("not ready")
    );
    for fence_path in [
        store.paths.backing_migration_global_fence_path(),
        store.paths.backing_migration_fence_path(bucket),
    ] {
        assert!(
            !backend
                .object_exists(RUSTFS_META_BUCKET, &fence_path)
                .await
                .expect("migration fence lookup should succeed")
        );
    }
}

#[tokio::test]
async fn durable_strong_migration_rejects_table_view_collision_before_fencing() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let identifier = IdentifierSegment::parse("orders").expect("identifier should parse");
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(
            bucket,
            &namespace,
            &identifier,
            default_table_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
        ))
        .await
        .unwrap();
    let view = test_view_entry(
        bucket,
        &namespace,
        &identifier,
        default_view_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );
    store
        .write_entry(
            store.catalog_bucket(),
            &store.paths.view_entry_path(bucket, &namespace, &identifier),
            &view,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("legacy colliding view should be seeded");
    store.backfill_table_warehouse_index(bucket).await.unwrap();

    let report = store.plan_durable_strong_backing_migration(bucket).await.unwrap();

    assert_eq!(report.status, TableCatalogBackingMigrationStatus::ManualReviewRequired);
    assert!(
        report
            .blockers
            .contains(&TableCatalogBackingMigrationBlocker::TableViewIdentifierCollision)
    );
    assert!(
        report
            .recommended_actions
            .contains(&TableCatalogBackingMigrationAction::ReviewTableViewIdentifierCollisions)
    );
    assert_matches!(
        store.materialize_durable_strong_backing_migration(bucket).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("not ready")
    );
    for fence_path in [
        store.paths.backing_migration_global_fence_path(),
        store.paths.backing_migration_fence_path(bucket),
    ] {
        assert!(
            !backend
                .object_exists(RUSTFS_META_BUCKET, &fence_path)
                .await
                .expect("migration fence lookup should succeed")
        );
    }
}

#[tokio::test]
async fn strong_catalog_backing_commit_is_atomic_with_wal_and_idempotency() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

    let result = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata.clone(),
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .unwrap();

    assert_eq!(result.table.metadata_location, new_metadata);
    assert_eq!(result.table.generation, 2);
    assert_eq!(result.commit_log.status, CommitLogStatus::Committed);

    let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    assert_eq!(loaded.metadata_location, result.table.metadata_location);
    assert_eq!(loaded.version_token, result.table.version_token);
    assert_eq!(
        store
            .get_commit_by_id(bucket, "table-id", "commit-1")
            .await
            .unwrap()
            .unwrap()
            .status,
        CommitLogStatus::Committed
    );
    assert_eq!(
        store
            .get_commit_by_idempotency_key(bucket, "table-id", "client-request")
            .await
            .unwrap()
            .unwrap()
            .status,
        CommitLogStatus::Committed
    );

    let recovery = store.plan_table_commit_recovery(bucket, "sales", "orders").await.unwrap();
    assert_eq!(recovery.staged_before_table_update_count, 0);
    assert_eq!(recovery.finalization_required_count, 0);
    assert_eq!(recovery.idempotency_repair_required_count, 0);
    assert_eq!(recovery.manual_review_count, 0);
}

#[tokio::test]
async fn strong_catalog_pagination_uses_ordered_state_and_rejects_object_cursors() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let namespace_name = namespace.public_name();
    let one = NonZeroUsize::new(1).expect("page size should be non-zero");

    seed_catalog_list_entries(&store, bucket, &namespace).await;

    let first = store
        .list_namespaces_page(bucket, None, one)
        .await
        .expect("first strong namespace page should load");
    assert_eq!(first.entries[0].namespace, "analytics");
    let second = store
        .list_namespaces_page(bucket, first.next_cursor.as_deref(), one)
        .await
        .expect("second strong namespace page should load");
    assert_eq!(second.entries[0].namespace, "sales");
    assert!(second.next_cursor.is_none());

    let first = store
        .list_tables_page(bucket, &namespace_name, None, one)
        .await
        .expect("first strong table page should load");
    assert_eq!(first.entries[0].table, "alpha");
    assert!(first.next_cursor.as_deref().is_some_and(|cursor| cursor == "strong:alpha"));
    let second = store
        .list_tables_page(bucket, &namespace_name, first.next_cursor.as_deref(), one)
        .await
        .expect("second strong table page should load");
    assert_eq!(second.entries[0].table, "beta");
    assert!(second.next_cursor.is_none());

    let first = store
        .list_views_page(bucket, &namespace_name, None, one)
        .await
        .expect("first strong view page should load");
    assert_eq!(first.entries[0].view, "view_alpha");
    let second = store
        .list_views_page(bucket, &namespace_name, first.next_cursor.as_deref(), one)
        .await
        .expect("second strong view page should load");
    assert_eq!(second.entries[0].view, "view_beta");
    assert!(second.next_cursor.is_none());

    let exact = NonZeroUsize::new(2).expect("exact page size should be non-zero");
    assert!(
        store
            .list_namespaces_page(bucket, None, exact)
            .await
            .expect("exact strong namespace page should load")
            .next_cursor
            .is_none()
    );
    assert!(
        store
            .list_tables_page(bucket, &namespace_name, None, exact)
            .await
            .expect("exact strong table page should load")
            .next_cursor
            .is_none()
    );
    assert!(
        store
            .list_views_page(bucket, &namespace_name, None, exact)
            .await
            .expect("exact strong view page should load")
            .next_cursor
            .is_none()
    );

    assert!(matches!(
        store
            .list_tables_page(bucket, &namespace_name, Some("object:alpha"), one)
            .await,
        Err(TableCatalogStoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn strong_catalog_backing_replays_durable_commit_state_after_restart() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

    let result = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata.clone(),
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .expect("commit should succeed");

    let restarted = StrongTableCatalogStore::new(backend.clone());
    let loaded = restarted
        .load_table(bucket, "sales", "orders")
        .await
        .expect("table load should succeed")
        .expect("table should replay from durable state");
    assert_eq!(loaded.metadata_location, result.table.metadata_location);
    assert_eq!(loaded.version_token, result.table.version_token);
    assert_eq!(
        restarted
            .get_commit_by_id(bucket, "table-id", "commit-1")
            .await
            .expect("commit lookup should succeed")
            .expect("commit log should replay from durable state")
            .status,
        CommitLogStatus::Committed
    );
    assert_eq!(
        restarted
            .get_commit_by_idempotency_key(bucket, "table-id", "client-request")
            .await
            .expect("idempotency lookup should succeed")
            .expect("idempotency index should replay from durable state")
            .status,
        CommitLogStatus::Committed
    );

    let recovery = restarted
        .plan_table_commit_recovery(bucket, "sales", "orders")
        .await
        .expect("recovery report should replay from durable state");
    assert_eq!(recovery.finalized_count, 1);
    assert_eq!(recovery.finalization_required_count, 0);
    assert_eq!(recovery.idempotency_repair_required_count, 0);
    assert_eq!(recovery.manual_review_count, 0);
}

#[tokio::test]
async fn strong_catalog_exact_commit_replay_does_not_rewrite_snapshot() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    let request = TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        commit_id: "commit-1".to_string(),
        idempotency_key: Some("client-request".to_string()),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        expected_metadata_location: current_metadata,
        new_metadata_location: new_metadata,
        requirements: Vec::new(),
        writer: Some("pyiceberg/test".to_string()),
    };
    let committed = store.commit_table(request.clone()).await.expect("commit should succeed");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let put_count = backend.put_attempt_count(RUSTFS_META_BUCKET, &snapshot_path).await;

    let replay = store.commit_table(request).await.expect("exact replay should succeed");

    assert_eq!(replay, committed);
    assert_eq!(backend.put_attempt_count(RUSTFS_META_BUCKET, &snapshot_path).await, put_count);
}

#[tokio::test]
async fn strong_catalog_drop_removes_commit_indexes_before_restart() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .expect("commit should succeed");

    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("table should be dropped");

    let restarted = StrongTableCatalogStore::new(backend);
    assert!(
        restarted
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .expect("snapshot should hydrate after table drop")
            .is_none()
    );
    assert!(
        restarted
            .get_commit_by_id(bucket, "table-id", "commit-1")
            .await
            .expect("commit lookup should succeed")
            .is_none()
    );
    assert!(
        restarted
            .get_commit_by_idempotency_key(bucket, "table-id", "client-request")
            .await
            .expect("idempotency lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_backing_rejects_stale_snapshot_cas_after_concurrent_restart() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let first_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let stale_metadata = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &first_metadata, b"{}".to_vec()).await;
    backend.seed_object(bucket, &stale_metadata, b"{}".to_vec()).await;

    let stale_store = StrongTableCatalogStore::new(backend.clone());
    stale_store
        .load_table(bucket, "sales", "orders")
        .await
        .expect("stale store should hydrate")
        .expect("table should exist");

    let first_result = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request-1".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata.clone(),
            new_metadata_location: first_metadata.clone(),
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .expect("first commit should succeed");

    let err = stale_store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-2".to_string(),
            idempotency_key: Some("client-request-2".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: stale_metadata,
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .expect_err("stale snapshot CAS should fail");

    assert_matches!(err, TableCatalogStoreError::Conflict(_));
    let loaded = stale_store
        .load_table(bucket, "sales", "orders")
        .await
        .expect("stale store should reload after CAS conflict")
        .expect("table should still exist");
    assert_eq!(loaded.metadata_location, first_result.table.metadata_location);
    assert_eq!(loaded.version_token, first_result.table.version_token);
    assert!(
        stale_store
            .get_commit_by_id(bucket, "table-id", "commit-2")
            .await
            .expect("commit lookup should succeed")
            .is_none()
    );
    assert!(
        stale_store
            .get_commit_by_idempotency_key(bucket, "table-id", "client-request-2")
            .await
            .expect("idempotency lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_replays_identical_commit_after_snapshot_cas_loss() {
    let backend = TestCatalogObjectBackend::default();
    let first_store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    first_store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    first_store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    first_store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

    let request = TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        commit_id: "commit-1".to_string(),
        idempotency_key: Some("request-1".to_string()),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        expected_metadata_location: current_metadata,
        new_metadata_location: new_metadata,
        requirements: Vec::new(),
        writer: Some("pyiceberg/test".to_string()),
    };
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let first_request = request.clone();
    let commit_store = first_store.clone();
    let first_writer = tokio::spawn(async move {
        commit_store
            .commit_table_with_publication(first_request, &UnserializedTestPublication)
            .await
    });
    pause.wait_started().await;
    let winner = StrongTableCatalogStore::new(backend.clone())
        .commit_table_with_publication(request.clone(), &UnserializedTestPublication)
        .await;
    let winner = winner.expect("independent competing writer should commit the shared request");
    pause.release();
    let replay = first_writer
        .await
        .expect("first writer task should join")
        .expect("CAS loser should replay the durable winner");

    assert_eq!(replay.table.metadata_location, winner.table.metadata_location);
    assert_eq!(replay.table.version_token, winner.table.version_token);
    assert_eq!(replay.table.generation, winner.table.generation);
    assert_eq!(replay.commit_log, winner.commit_log);

    let second_metadata = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
    backend.seed_object(bucket, &second_metadata, b"{}".to_vec()).await;
    let second_request = TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        commit_id: "commit-2".to_string(),
        idempotency_key: Some("request-2".to_string()),
        operation: "append".to_string(),
        expected_version_token: replay.table.version_token.clone(),
        expected_metadata_location: replay.table.metadata_location.clone(),
        new_metadata_location: second_metadata,
        requirements: Vec::new(),
        writer: Some("pyiceberg/test".to_string()),
    };
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let commit_store = first_store.clone();
    let staged_request = second_request.clone();
    let staged_writer = tokio::spawn(async move {
        commit_store
            .commit_table_with_publication(staged_request, &UnserializedTestPublication)
            .await
    });
    pause.wait_started().await;
    let committed_winner = StrongTableCatalogStore::new(backend.clone())
        .commit_table_with_publication(second_request.clone(), &UnserializedTestPublication)
        .await
        .expect("independent competing writer should commit the second request");
    assert_eq!(committed_winner.commit_log.status, CommitLogStatus::Committed);
    let mut staged_snapshot = read_strong_snapshot(&backend).await;
    for record in staged_snapshot
        .commits
        .iter_mut()
        .chain(staged_snapshot.idempotency.iter_mut())
        .filter(|record| record.commit.commit_id == second_request.commit_id)
    {
        record.commit.status = CommitLogStatus::Staged;
    }
    seed_strong_snapshot(&backend, &staged_snapshot).await;
    pause.release();

    assert_matches!(
        staged_writer.await.expect("staged writer task should join"),
        Err(TableCatalogStoreError::Conflict(_))
    );
    assert_eq!(
        first_store
            .get_commit_by_id(bucket, "table-id", "commit-2")
            .await
            .expect("staged commit lookup should succeed")
            .expect("staged durable winner should remain visible")
            .status,
        CommitLogStatus::Staged
    );

    let finalized = first_store
        .commit_table(second_request)
        .await
        .expect("retry should durably finalize the staged winner");
    assert_eq!(finalized.commit_log.status, CommitLogStatus::Committed);
    assert_eq!(
        first_store
            .get_commit_by_id(bucket, "table-id", "commit-2")
            .await
            .expect("finalized commit lookup should succeed")
            .expect("finalized commit should exist")
            .status,
        CommitLogStatus::Committed
    );
}

#[tokio::test]
async fn strong_catalog_backing_refreshes_hydrated_reads_after_independent_commit() {
    let backend = TestCatalogObjectBackend::default();
    let writer = StrongTableCatalogStore::new(backend.clone());
    let reader = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    writer
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    writer
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    writer
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

    let loaded_before_commit = reader
        .load_table(bucket, "sales", "orders")
        .await
        .expect("reader should hydrate")
        .expect("table should exist");
    assert_eq!(loaded_before_commit.metadata_location, current_metadata);

    let result = writer
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request-1".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .expect("writer commit should succeed");

    let loaded_after_commit = reader
        .load_table(bucket, "sales", "orders")
        .await
        .expect("reader should refresh durable state")
        .expect("table should still exist");
    assert_eq!(loaded_after_commit.metadata_location, result.table.metadata_location);
    assert_eq!(loaded_after_commit.version_token, result.table.version_token);
}

#[tokio::test]
async fn strong_catalog_does_not_install_stale_concurrent_reload() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");

    let pause = backend.pause_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;
    let stale_store = store.clone();
    let stale_reload = tokio::spawn(async move { stale_store.reload_state_from_durable().await });
    pause.wait_started().await;

    let writer = store.clone();
    let namespace_name = namespace.public_name();
    let write = tokio::spawn(async move { writer.create_namespace(test_namespace_entry(bucket, &namespace)).await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        loop {
            let durable = read_strong_snapshot(&backend).await;
            if durable.namespaces.iter().any(|entry| entry.namespace == namespace_name) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("namespace snapshot should become durable while stale reload is paused");
    pause.release();
    write
        .await
        .expect("namespace writer task should join")
        .expect("namespace should be durably created while stale reload is paused");
    stale_reload
        .await
        .expect("stale reload task should join")
        .expect("stale reload must not replace the current snapshot");

    assert!(
        store
            .get_namespace(bucket, "sales")
            .await
            .expect("namespace lookup should succeed")
            .is_some()
    );
}

#[tokio::test]
async fn strong_catalog_bounds_reload_under_repeated_local_state_changes() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();

    store
        .put_table_bucket(test_bucket_entry("analytics"))
        .await
        .expect("table bucket should be created");

    let mut pause = backend.pause_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;
    let reload_store = store.clone();
    let reload = tokio::spawn(async move { reload_store.reload_state_from_durable().await });
    for attempt in 0..STRONG_TABLE_CATALOG_RELOAD_MAX_ATTEMPTS {
        pause.wait_started().await;
        let next_pause = if attempt + 1 < STRONG_TABLE_CATALOG_RELOAD_MAX_ATTEMPTS {
            Some(backend.pause_next_read(RUSTFS_META_BUCKET, &snapshot_path).await)
        } else {
            None
        };
        store.state.lock().await.snapshot_etag = Some(format!("concurrent-etag-{attempt}"));
        pause.release();
        if let Some(next_pause) = next_pause {
            pause = next_pause;
        }
    }

    let error = reload
        .await
        .expect("reload task should join")
        .expect_err("repeated local state changes must stop after the retry bound");
    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("changed repeatedly"));
}

#[tokio::test]
async fn strong_catalog_fails_closed_when_observed_snapshot_disappears() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    let snapshot = backend
        .read_object(RUSTFS_META_BUCKET, &snapshot_path)
        .await
        .expect("snapshot read should succeed")
        .expect("snapshot should exist");
    backend
        .delete_object(RUSTFS_META_BUCKET, &snapshot_path)
        .await
        .expect("snapshot deletion should be injected");

    assert_matches!(
        store.get_table_bucket(bucket).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );
    assert_matches!(
        store.put_table_bucket(test_bucket_entry("replacement")).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );
    assert!(store.state.lock().await.table_buckets.contains_key(bucket));
    assert!(
        backend
            .read_object(RUSTFS_META_BUCKET, &snapshot_path)
            .await
            .expect("snapshot lookup should succeed")
            .is_none()
    );
    backend.seed_object(RUSTFS_META_BUCKET, &snapshot_path, snapshot.data).await;
    assert_eq!(
        store
            .get_table_bucket(bucket)
            .await
            .expect("restored snapshot should reload")
            .expect("table bucket should be restored")
            .table_bucket,
        bucket
    );
}

#[tokio::test]
async fn strong_catalog_configured_mode_requires_snapshot_after_restart() {
    let backend = TestCatalogObjectBackend::default();
    let required = StrongTableCatalogStore::new_requiring_snapshot(backend.clone());
    assert_matches!(
        required.get_table_bucket("analytics").await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );

    let bootstrap = StrongTableCatalogStore::new(backend.clone());
    bootstrap
        .put_table_bucket(test_bucket_entry("analytics"))
        .await
        .expect("migration path should materialize the first snapshot");
    let restarted = StrongTableCatalogStore::new_requiring_snapshot(backend.clone());
    assert!(
        restarted
            .get_table_bucket("analytics")
            .await
            .expect("configured mode should load a materialized snapshot")
            .is_some()
    );

    backend
        .delete_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
        )
        .await
        .expect("snapshot deletion should be injected");
    let restarted_after_loss = StrongTableCatalogStore::new_requiring_snapshot(backend);
    assert_matches!(
        restarted_after_loss.get_table_bucket("analytics").await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );
}

#[tokio::test]
async fn strong_catalog_required_reload_does_not_reuse_empty_migration_state() {
    let runtime = StrongTableCatalogRuntime::default();
    let backend = TestCatalogObjectBackend {
        strong_runtime: Some(runtime),
        ..TestCatalogObjectBackend::default()
    };
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let pause = backend.pause_before_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;
    let migration = StrongTableCatalogStore::new(backend.clone());
    let migration_reload = tokio::spawn(async move { migration.get_table_bucket("analytics").await });
    pause.wait_started().await;

    let configured = StrongTableCatalogStore::new_requiring_snapshot(backend);
    let reload_attempts = configured.reload_lock_attempts_for_test();
    let configured_task = configured.clone();
    let configured_reload = tokio::spawn(async move { configured_task.get_table_bucket("analytics").await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while configured.reload_lock_attempts_for_test() == reload_attempts {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("configured reload should attempt the shared reload lock");
    assert!(
        !configured_reload.is_finished(),
        "configured reload should wait for the in-flight migration reload"
    );

    pause.release();
    assert!(
        migration_reload
            .await
            .expect("migration reload task should join")
            .expect("migration may hydrate an empty target")
            .is_none()
    );
    assert_matches!(
        configured_reload.await.expect("configured reload task should join"),
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );
}

#[tokio::test]
async fn strong_catalog_first_confirmed_write_requires_snapshot_after_reload_failure() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    assert!(
        store
            .get_table_bucket(bucket)
            .await
            .expect("migration target should hydrate without a snapshot")
            .is_none()
    );
    backend.fail_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("confirmed snapshot write should survive a local reload failure");
    assert!(!store.is_hydrated_for_test().await);
    backend
        .delete_object(RUSTFS_META_BUCKET, &snapshot_path)
        .await
        .expect("snapshot deletion should be injected");

    assert_matches!(
        store.get_table_bucket(bucket).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );
}

#[tokio::test]
async fn strong_catalog_first_ambiguous_write_requires_snapshot_after_recovery_failure() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    assert!(
        store
            .get_table_bucket(bucket)
            .await
            .expect("migration target should hydrate without a snapshot")
            .is_none()
    );
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    backend.fail_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect_err("ambiguous first write should report the original transport failure");
    assert!(!store.is_hydrated_for_test().await);
    backend
        .delete_object(RUSTFS_META_BUCKET, &snapshot_path)
        .await
        .expect("snapshot deletion should be injected");

    assert_matches!(
        store.get_table_bucket(bucket).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );
}

#[tokio::test]
async fn strong_catalog_does_not_expose_empty_state_during_ambiguous_first_write_recovery() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    store
        .get_table_bucket(bucket)
        .await
        .expect("migration target may start without a snapshot");
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let recovery_read = backend.pause_before_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;

    let writer = store.clone();
    let write = tokio::spawn(async move { writer.put_table_bucket(test_bucket_entry(bucket)).await });
    recovery_read.wait_started().await;
    backend
        .delete_object(RUSTFS_META_BUCKET, &snapshot_path)
        .await
        .expect("snapshot loss should be injected while recovery is in flight");

    let reload_attempts = store.reload_lock_attempts_for_test();
    let reader = store.clone();
    let load = tokio::spawn(async move { reader.get_table_bucket(bucket).await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while store.reload_lock_attempts_for_test() == reload_attempts {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reader should attempt the shared reload lock");
    assert!(
        !load.is_finished(),
        "a reader must wait for ambiguous first-write recovery instead of exposing the old empty state"
    );

    recovery_read.release();
    assert_matches!(
        write.await.expect("writer task should join"),
        Err(TableCatalogStoreError::Internal(message)) if message.contains("post-commit put failure")
    );
    assert_matches!(
        load.await.expect("reader task should join"),
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );
}

#[tokio::test]
async fn strong_catalog_first_write_conflict_requires_snapshot_after_recovery_failure() {
    let backend = TestCatalogObjectBackend::default();
    let stale = StrongTableCatalogStore::new(backend.clone());
    let winner = StrongTableCatalogStore::new(backend.clone());
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    stale
        .get_table_bucket("missing")
        .await
        .expect("stale writer should hydrate the empty state");

    let paused_put = backend.pause_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let stale_writer = stale.clone();
    let stale_write = tokio::spawn(async move { stale_writer.put_table_bucket(test_bucket_entry("stale")).await });
    paused_put.wait_started().await;
    winner
        .put_table_bucket(test_bucket_entry("winner"))
        .await
        .expect("winning writer should publish the first snapshot");
    backend.fail_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;
    paused_put.release();

    assert_matches!(
        stale_write.await.expect("stale writer task should join"),
        Err(TableCatalogStoreError::Conflict(_))
    );
    assert!(!stale.is_hydrated_for_test().await);
    backend
        .delete_object(RUSTFS_META_BUCKET, &snapshot_path)
        .await
        .expect("snapshot deletion should be injected");
    assert_matches!(
        stale.get_table_bucket("winner").await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );
}

#[tokio::test]
async fn strong_catalog_backing_skips_snapshot_body_when_etag_is_unchanged() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");

    backend.reset_call_counts().await;
    let loaded = store
        .get_table_bucket(bucket)
        .await
        .expect("table bucket load should succeed")
        .expect("table bucket should exist");

    assert_eq!(loaded.table_bucket, bucket);
    assert_eq!(backend.metadata_call_count().await, 1);
    assert_eq!(backend.read_call_count().await, 0);
    assert_eq!(backend.list_call_count().await, 0);
}

#[tokio::test]
async fn strong_catalog_reload_requires_etag_and_bounds_snapshot_reads() {
    let backend = TestCatalogObjectBackend::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let snapshot = test_strong_snapshot(bucket, &namespace, Vec::new(), Vec::new());
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    seed_strong_snapshot(&backend, &snapshot).await;
    backend.omit_etag_for_object(RUSTFS_META_BUCKET, &snapshot_path).await;

    let error = StrongTableCatalogStore::new(backend.clone())
        .get_table_bucket(bucket)
        .await
        .expect_err("a durable snapshot without an etag must fail closed");

    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("snapshot has no etag"));
    assert_eq!(
        backend.last_read_limit(RUSTFS_META_BUCKET, &snapshot_path).await,
        Some(STRONG_TABLE_CATALOG_SNAPSHOT_MAX_SIZE)
    );
}

#[tokio::test]
async fn strong_catalog_rejects_oversized_snapshot_before_publication() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let mut entry = test_bucket_entry("analytics");
    entry
        .properties
        .insert("oversized".to_string(), "x".repeat(STRONG_TABLE_CATALOG_SNAPSHOT_MAX_SIZE));

    let error = store
        .put_table_bucket(entry)
        .await
        .expect_err("an oversized durable snapshot must be rejected before publication");

    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("exceeds the maximum encoded size")
    );
    assert!(
        backend
            .read_object(RUSTFS_META_BUCKET, &snapshot_path)
            .await
            .expect("snapshot lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_coalesces_concurrent_snapshot_reloads() {
    let backend = TestCatalogObjectBackend::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let snapshot = test_strong_snapshot(bucket, &namespace, Vec::new(), Vec::new());
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    seed_strong_snapshot(&backend, &snapshot).await;
    backend.reset_call_counts().await;
    let store = StrongTableCatalogStore::new(backend.clone());
    let first = store.clone();
    let second = store.clone();
    let pause = backend.pause_before_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;
    let release = pause.clone();
    let release_task = tokio::spawn(async move {
        release.wait_started().await;
        release.release();
    });

    let (first_result, second_result) = tokio::join!(first.reload_state_from_durable(), second.reload_state_from_durable());
    release_task.await.expect("reload release task should join");

    first_result.expect("first reload should succeed");
    second_result.expect("coalesced reload should succeed");
    assert_eq!(backend.read_call_count().await, 1);
    assert_eq!(backend.metadata_call_count().await, 1);
}

#[tokio::test]
async fn strong_catalog_backing_resolves_data_plane_resource_without_catalog_scan() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata))
        .await
        .expect("table should be created");

    backend.reset_call_counts().await;
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/file.parquet")
        .await
        .expect("data-plane lookup should succeed")
        .expect("data-plane resource should resolve");

    assert_eq!(resource.table_bucket, bucket);
    assert_eq!(resource.namespace, namespace.public_name());
    assert_eq!(resource.table, table.as_str());
    assert_eq!(resource.table_id, "table-id");
    assert_eq!(resource.warehouse_object_prefix, "tables/table-id/");
    assert_eq!(backend.metadata_call_count().await, 1);
    assert_eq!(backend.read_call_count().await, 0);
    assert_eq!(backend.list_call_count().await, 0);
}

#[tokio::test]
async fn strong_catalog_data_plane_fails_closed_for_missing_bucket_snapshot() {
    let store = StrongTableCatalogStore::new(TestCatalogObjectBackend::default());

    let error = table_data_plane_resource_for_object(&store, "analytics", "tables/table-id/data/file.parquet")
        .await
        .expect_err("a table-enabled bucket missing from the strong snapshot must fail closed");

    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("no entry for table-enabled bucket"));
}

#[tokio::test]
async fn object_catalog_data_plane_fails_closed_for_missing_bucket_entry() {
    let store = ObjectTableCatalogStore::new(TestCatalogObjectBackend::default());

    let error = table_data_plane_resource_for_object(&store, "analytics", "tables/table-id/data/file.parquet")
        .await
        .expect_err("a table-enabled bucket missing from the object catalog must fail closed");

    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("no entry for table-enabled bucket"));
}

#[tokio::test]
async fn catalog_backings_fail_closed_for_inactive_table_bucket_data_plane() {
    let bucket = "analytics";
    let object = "tables/table-id/data/file.parquet";
    let mut inactive = test_bucket_entry(bucket);
    inactive.state = TableCatalogEntryState::Deleted;

    let object_store = ObjectTableCatalogStore::new(TestCatalogObjectBackend::default());
    object_store
        .put_table_bucket(inactive.clone())
        .await
        .expect("inactive object-backed bucket should be seeded");
    let object_error = table_data_plane_resource_for_object(&object_store, bucket, object)
        .await
        .expect_err("inactive object-backed table buckets must fail closed");
    assert_matches!(
        object_error,
        TableCatalogStoreError::Internal(message) if message.contains("inactive object-backed catalog entry")
    );

    let strong_store = StrongTableCatalogStore::new(TestCatalogObjectBackend::default());
    strong_store
        .put_table_bucket(inactive)
        .await
        .expect("inactive strong bucket should be seeded");
    let strong_error = table_data_plane_resource_for_object(&strong_store, bucket, object)
        .await
        .expect_err("inactive durable strong table buckets must fail closed");
    assert_matches!(
        strong_error,
        TableCatalogStoreError::Internal(message) if message.contains("inactive durable strong catalog entry")
    );
}

#[tokio::test]
async fn strong_catalog_data_plane_requires_v2_snapshot_after_fleet_confirmation() {
    let backend = TestCatalogObjectBackend::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let table_entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    seed_strong_snapshot(&backend, &test_strong_snapshot(bucket, &namespace, vec![table_entry], Vec::new())).await;
    let store = StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_VERSION);

    let error = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/file.parquet")
        .await
        .expect_err("fleet-confirmed readers must reject a version 1 snapshot on the data plane");
    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("requires a version 2 snapshot"));

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("a catalog write should upgrade the snapshot");
    assert_eq!(read_strong_snapshot(&backend).await.version, STRONG_TABLE_CATALOG_SNAPSHOT_VERSION);
    assert!(
        table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/file.parquet")
            .await
            .expect("version 2 data-plane lookup should succeed")
            .is_some()
    );
}

#[tokio::test]
async fn strong_catalog_backing_does_not_publish_warehouse_index_before_snapshot_persist() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");

    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let create_store = store.clone();
    let create_task = tokio::spawn(async move {
        let namespace = Namespace::parse("sales").expect("namespace should parse");
        let table = IdentifierSegment::parse("orders").expect("table should parse");
        create_store
            .create_table(test_table_entry("analytics", &namespace, &table, current_metadata))
            .await
    });
    pause.wait_started().await;

    let resource_before_commit = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/file.parquet")
        .await
        .expect("data-plane lookup should succeed while snapshot persist is paused");
    assert!(resource_before_commit.is_none());
    assert!(
        store
            .load_table(bucket, "sales", "orders")
            .await
            .expect("table load should succeed while snapshot persist is paused")
            .is_none()
    );

    pause.release();
    create_task
        .await
        .expect("create task should join")
        .expect("table should be created");

    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/file.parquet")
        .await
        .expect("data-plane lookup should succeed after snapshot persist")
        .expect("new table warehouse index should be visible after commit");
    assert_eq!(resource.table_bucket, bucket);
    assert_eq!(resource.namespace, namespace.public_name());
    assert_eq!(resource.table, table.as_str());
    assert_eq!(resource.table_id, "table-id");
    assert_eq!(resource.warehouse_object_prefix, "tables/table-id/");
}

#[tokio::test]
async fn strong_catalog_backing_rejects_duplicate_snapshot_warehouse_index_entries() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let orders = IdentifierSegment::parse("orders").expect("table should parse");
    let returns = IdentifierSegment::parse("returns").expect("table should parse");
    let mut returns_entry = test_table_entry(
        bucket,
        &namespace,
        &returns,
        default_table_metadata_file_path(&namespace, &returns, "00001.metadata.json"),
    );
    returns_entry.table_id = "table-id-2".to_string();
    returns_entry.table_uuid = "table-uuid-2".to_string();

    let snapshot = StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: vec![test_namespace_entry(bucket, &namespace)],
        tables: vec![
            test_table_entry(
                bucket,
                &namespace,
                &orders,
                default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json"),
            ),
            returns_entry,
        ],
        views: Vec::new(),
        commits: Vec::new(),
        idempotency: Vec::new(),
    };
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            serde_json::to_vec(&snapshot).expect("strong snapshot should encode"),
        )
        .await;

    let err = store
        .get_table_bucket(bucket)
        .await
        .expect_err("duplicate warehouse prefix should fail snapshot hydration");

    assert_matches!(err, TableCatalogStoreError::Invalid(message) if message.contains("overlapping active table warehouse location"));
}

#[test]
fn strong_catalog_snapshot_v2_requires_fleet_confirmation() {
    for (requested, fleet_confirmed, expected) in [
        (false, false, STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION),
        (true, false, STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION),
        (false, true, STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION),
        (true, true, STRONG_TABLE_CATALOG_SNAPSHOT_VERSION),
    ] {
        assert_eq!(strong_snapshot_write_version(requested, fleet_confirmed), expected);
    }
}

#[tokio::test]
async fn strong_catalog_reads_literal_v1_snapshot_fixture() {
    let backend = TestCatalogObjectBackend::default();
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            br#"{
                "version": 1,
                "table_buckets": [{
                    "version": 1,
                    "table_bucket": "analytics",
                    "catalog_type": "iceberg-rest",
                    "warehouse_root": "s3://analytics/",
                    "state": "ACTIVE",
                    "properties": {},
                    "created_at": null,
                    "updated_at": null
                }],
                "namespaces": [],
                "tables": [],
                "views": [],
                "commits": [],
                "idempotency": []
            }"#
            .to_vec(),
        )
        .await;

    let store = StrongTableCatalogStore::new(backend);
    let entry = store
        .get_table_bucket("analytics")
        .await
        .expect("literal version 1 snapshot should load")
        .expect("table bucket should exist");

    assert_eq!(entry, test_bucket_entry("analytics"));
}

#[tokio::test]
async fn strong_catalog_snapshot_upgrade_preserves_v2_and_rejects_stale_restore() {
    let backend = TestCatalogObjectBackend::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let snapshot = test_strong_snapshot(bucket, &namespace, Vec::new(), Vec::new());
    seed_strong_snapshot(&backend, &snapshot).await;

    let legacy =
        StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION);
    let mut legacy_bucket = test_bucket_entry(bucket);
    legacy_bucket.properties.insert("writer".to_string(), "legacy".to_string());
    legacy
        .put_table_bucket(legacy_bucket)
        .await
        .expect("legacy-compatible write should succeed");
    assert_eq!(
        read_strong_snapshot(&backend).await.version,
        STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION
    );

    let fleet = StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_VERSION);
    let mut upgraded_bucket = test_bucket_entry(bucket);
    upgraded_bucket.properties.insert("writer".to_string(), "fleet".to_string());
    fleet
        .put_table_bucket(upgraded_bucket)
        .await
        .expect("fleet-confirmed write should upgrade the snapshot");
    assert_eq!(read_strong_snapshot(&backend).await.version, STRONG_TABLE_CATALOG_SNAPSHOT_VERSION);

    let compatibility_writer =
        StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION);
    let mut post_upgrade_bucket = test_bucket_entry(bucket);
    post_upgrade_bucket
        .properties
        .insert("writer".to_string(), "compatibility".to_string());
    compatibility_writer
        .put_table_bucket(post_upgrade_bucket)
        .await
        .expect("current binary should preserve a previously upgraded snapshot");
    assert_eq!(read_strong_snapshot(&backend).await.version, STRONG_TABLE_CATALOG_SNAPSHOT_VERSION);

    let restored_snapshot = snapshot;
    seed_strong_snapshot(&backend, &restored_snapshot).await;
    let mut after_restore_bucket = test_bucket_entry(bucket);
    after_restore_bucket
        .properties
        .insert("writer".to_string(), "post-restore".to_string());
    let error = compatibility_writer
        .put_table_bucket(after_restore_bucket)
        .await
        .expect_err("a running writer that observed version 2 must reject stale version 1 content");
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("high-water version 2"));
    assert_eq!(read_strong_snapshot(&backend).await, restored_snapshot);
    assert_matches!(
        compatibility_writer.get_table_bucket(bucket).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("high-water version 2")
    );
}

#[tokio::test]
async fn shared_strong_runtime_preserves_snapshot_high_water_across_store_instances() {
    let runtime = StrongTableCatalogRuntime::default();
    let backend = TestCatalogObjectBackend {
        strong_runtime: Some(runtime),
        ..TestCatalogObjectBackend::default()
    };
    let bucket = "analytics";
    let writer = StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_VERSION);
    writer
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("fleet-confirmed writer should publish version 2");
    assert_eq!(read_strong_snapshot(&backend).await.version, STRONG_TABLE_CATALOG_SNAPSHOT_VERSION);

    let next_request =
        StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION);
    next_request
        .get_table_bucket(bucket)
        .await
        .expect("a new request should reuse the hydrated runtime")
        .expect("table bucket should exist");
    let mut restored = read_strong_snapshot(&backend).await;
    restored.version = STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION;
    seed_strong_snapshot(&backend, &restored).await;

    let after_restore =
        StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION);
    assert_matches!(
        after_restore.get_table_bucket(bucket).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("high-water version 2")
    );

    backend
        .delete_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
        )
        .await
        .expect("snapshot deletion should be injected");
    let after_deletion =
        StrongTableCatalogStore::new_with_snapshot_write_version(backend, STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION);
    assert_matches!(
        after_deletion.get_table_bucket(bucket).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("snapshot disappeared")
    );
}

#[tokio::test]
async fn strong_catalog_snapshot_rejects_unknown_fields_and_versions() {
    let backend = TestCatalogObjectBackend::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let snapshot = test_strong_snapshot(bucket, &namespace, Vec::new(), Vec::new());
    let mut value = serde_json::to_value(snapshot).expect("snapshot should encode");
    value
        .as_object_mut()
        .expect("snapshot should be an object")
        .insert("future-field".to_string(), serde_json::Value::Bool(true));
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            serde_json::to_vec(&value).expect("snapshot should encode"),
        )
        .await;

    let unknown_field = StrongTableCatalogStore::new(backend.clone())
        .get_table_bucket(bucket)
        .await
        .expect_err("unknown persisted fields must fail closed");
    assert_matches!(unknown_field, TableCatalogStoreError::Internal(message) if message.contains("unknown field"));

    let nested_unknown = serde_json::json!({
        "version": STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION,
        "table_buckets": [],
        "namespaces": [],
        "tables": [],
        "views": [],
        "commits": [{
            "table_bucket": bucket,
            "table_id": "table-id",
            "lookup_key": "commit-1",
            "commit": {
                "version": TABLE_CATALOG_ENTRY_VERSION,
                "commit_id": "commit-1",
                "idempotency_key": null,
                "table_id": "table-id",
                "operation": "append",
                "expected_version_token": "token-v1",
                "new_version_token": "token-v2",
                "previous_metadata_location": "metadata/00001.metadata.json",
                "new_metadata_location": "metadata/00002.metadata.json",
                "requirements": [],
                "status": "COMMITTED",
                "writer": null,
                "created_at": null,
                "updated_at": null
            },
            "future-field": true
        }],
        "idempotency": []
    });
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            serde_json::to_vec(&nested_unknown).expect("snapshot should encode"),
        )
        .await;
    let nested_unknown_field = StrongTableCatalogStore::new(backend.clone())
        .get_table_bucket(bucket)
        .await
        .expect_err("unknown commit record fields must fail closed");
    assert_matches!(nested_unknown_field, TableCatalogStoreError::Internal(message) if message.contains("unknown field"));

    value
        .as_object_mut()
        .expect("snapshot should be an object")
        .remove("future-field");
    value["version"] = serde_json::Value::from(STRONG_TABLE_CATALOG_SNAPSHOT_VERSION.saturating_add(1));
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            serde_json::to_vec(&value).expect("snapshot should encode"),
        )
        .await;
    let unsupported = StrongTableCatalogStore::new(backend)
        .get_table_bucket(bucket)
        .await
        .expect_err("unsupported snapshot versions must fail closed");
    assert_matches!(unsupported, TableCatalogStoreError::Invalid(message) if message.contains("unsupported"));
}

#[tokio::test]
async fn strong_catalog_rejects_overlapping_warehouse_prefixes_on_hydration() {
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let parent = IdentifierSegment::parse("orders").expect("table should parse");
    let child = IdentifierSegment::parse("orders_child").expect("table should parse");
    let parent_entry = test_table_entry(
        bucket,
        &namespace,
        &parent,
        default_table_metadata_file_path(&namespace, &parent, "00001.metadata.json"),
    );
    let mut child_entry = test_table_entry(
        bucket,
        &namespace,
        &child,
        default_table_metadata_file_path(&namespace, &child, "00001.metadata.json"),
    );
    child_entry.table_id = "child-table-id".to_string();
    child_entry.table_uuid = "child-table-uuid".to_string();
    child_entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child");
    let snapshot = test_strong_snapshot(bucket, &namespace, vec![parent_entry, child_entry], Vec::new());
    let error = strong_snapshot_hydration_error(snapshot).await;

    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("overlapping active table warehouse"));
}

#[tokio::test]
async fn strong_catalog_backing_does_not_publish_draft_when_persist_and_reload_fail() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    backend.fail_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    backend.fail_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;

    let err = store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect_err("namespace write should fail");

    assert_matches!(err, TableCatalogStoreError::Internal(_));
    let state = store.state.lock().await;
    assert!(
        !state
            .namespaces
            .contains_key(&StrongTableCatalogStore::<TestCatalogObjectBackend>::namespace_key(bucket, &namespace))
    );
    drop(state);

    assert!(
        store
            .list_namespaces(bucket)
            .await
            .expect("durable state should reload after transient failure")
            .is_empty()
    );
}

#[tokio::test]
async fn strong_catalog_recovers_resource_mutations_after_committed_put_response_loss() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let table_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let view_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let next_view_metadata = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("committed table bucket write should survive a lost PUT response");

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("committed namespace write should survive a lost PUT response");

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    store
        .update_namespace_properties(
            bucket,
            &namespace.public_name(),
            NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "platform".to_string())]))
                .expect("namespace update should validate"),
        )
        .await
        .expect("committed namespace update should survive a lost PUT response");

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    store
        .create_table(test_table_entry(bucket, &namespace, &table, table_metadata))
        .await
        .expect("committed table write should survive a lost PUT response");

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    store
        .create_view(test_view_entry(bucket, &namespace, &view, view_metadata.clone()))
        .await
        .expect("committed view write should survive a lost PUT response");

    backend
        .seed_object(
            bucket,
            &next_view_metadata,
            serde_json::to_vec(&serde_json::json!({
                "format-version": 1,
                "view-uuid": "view-uuid",
                "location": format!("s3://{bucket}/views/view-id")
            }))
            .expect("view metadata should encode"),
        )
        .await;
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let replaced = store
        .replace_view(ViewCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            view: view.as_str().to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: view_metadata,
            new_metadata_location: next_view_metadata.clone(),
        })
        .await
        .expect("committed view replacement should survive a lost PUT response");
    assert_eq!(replaced.view.metadata_location, next_view_metadata);
    assert_eq!(replaced.view.generation, 2);

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("committed table drop should survive a lost PUT response");

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    store
        .drop_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect("committed view drop should survive a lost PUT response");

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    store
        .drop_namespace(bucket, &namespace.public_name())
        .await
        .expect("committed namespace drop should survive a lost PUT response");

    let restarted = StrongTableCatalogStore::new(backend);
    assert!(
        restarted
            .get_table_bucket(bucket)
            .await
            .expect("table bucket lookup should succeed after restart")
            .is_some()
    );
    assert!(
        restarted
            .get_namespace(bucket, &namespace.public_name())
            .await
            .expect("namespace lookup should succeed after restart")
            .is_none()
    );
    assert!(
        restarted
            .list_tables(bucket, &namespace.public_name())
            .await
            .expect("table listing should succeed after restart")
            .is_empty()
    );
    assert!(
        restarted
            .list_views(bucket, &namespace.public_name())
            .await
            .expect("view listing should succeed after restart")
            .is_empty()
    );
}

#[tokio::test]
async fn strong_catalog_does_not_guess_view_history_from_a_concurrent_replacement() {
    let backend = TestCatalogObjectBackend::default();
    let bootstrap = StrongTableCatalogStore::new(backend.clone());
    let first = StrongTableCatalogStore::new(backend.clone());
    let second = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let initial_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let first_metadata = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");
    let second_metadata = default_view_metadata_file_path(&namespace, &view, "00003.metadata.json");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();

    bootstrap
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    bootstrap
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    bootstrap
        .create_view(test_view_entry(bucket, &namespace, &view, initial_metadata.clone()))
        .await
        .expect("view should be created");
    first
        .get_table_bucket(bucket)
        .await
        .expect("first store should hydrate")
        .expect("table bucket should exist");
    second
        .get_table_bucket(bucket)
        .await
        .expect("second store should hydrate")
        .expect("table bucket should exist");
    for metadata in [&first_metadata, &second_metadata] {
        backend
            .seed_object(
                bucket,
                metadata,
                serde_json::to_vec(&serde_json::json!({
                    "format-version": 1,
                    "view-uuid": "view-uuid",
                    "location": format!("s3://{bucket}/views/view-id")
                }))
                .expect("view metadata should encode"),
            )
            .await;
    }

    backend.fail_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let recovery_read = backend.pause_before_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;
    let first_replace_store = first.clone();
    let first_namespace_name = namespace.public_name();
    let first_view_name = view.as_str().to_string();
    let first_expected_metadata = initial_metadata.clone();
    let first_replace = tokio::spawn(async move {
        first_replace_store
            .replace_view_with_publication(
                ViewCommitRequest {
                    table_bucket: bucket.to_string(),
                    namespace: first_namespace_name,
                    view: first_view_name,
                    expected_version_token: "token-v1".to_string(),
                    expected_metadata_location: first_expected_metadata,
                    new_metadata_location: first_metadata,
                },
                false,
                &UnserializedTestPublication,
            )
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, recovery_read.wait_started())
        .await
        .expect("the first replacement should reach its paused recovery read");
    tokio::time::timeout(
        TABLE_CATALOG_TEST_TIMEOUT,
        second.replace_view_with_publication(
            ViewCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                view: view.as_str().to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: initial_metadata,
                new_metadata_location: second_metadata.clone(),
            },
            false,
            &UnserializedTestPublication,
        ),
    )
    .await
    .expect("the independent replacement should not wait for publication serialization")
    .expect("second writer should publish a different replacement");
    recovery_read.release();
    let error = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, first_replace)
        .await
        .expect("the first replacement should finish after its recovery read is released")
        .expect("first replacement task should join")
        .expect_err("a different generation-two view must not prove the first replacement succeeded");
    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("injected put failure"));

    let loaded = first
        .load_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect("view lookup should succeed")
        .expect("view should remain present");
    assert_eq!(loaded.metadata_location, second_metadata);
}

#[tokio::test]
async fn strong_catalog_recovers_view_drop_when_a_different_identity_is_recreated() {
    let backend = TestCatalogObjectBackend::default();
    let bootstrap = StrongTableCatalogStore::new(backend.clone());
    let first = StrongTableCatalogStore::new(backend.clone());
    let second = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let initial_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let replacement_metadata = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();

    bootstrap
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    bootstrap
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    bootstrap
        .create_view(test_view_entry(bucket, &namespace, &view, initial_metadata))
        .await
        .expect("view should be created");
    first
        .get_table_bucket(bucket)
        .await
        .expect("first store should hydrate")
        .expect("table bucket should exist");
    second
        .get_table_bucket(bucket)
        .await
        .expect("second store should hydrate")
        .expect("table bucket should exist");

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let drop_recovery_read = backend.pause_before_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;
    let first_drop = first.clone();
    let namespace_name = namespace.public_name();
    let view_name = view.as_str().to_string();
    let drop = tokio::spawn(async move { first_drop.drop_view(bucket, &namespace_name, &view_name).await });
    drop_recovery_read.wait_started().await;
    let mut replacement = test_view_entry(bucket, &namespace, &view, replacement_metadata);
    replacement.view_id = "replacement-view-id".to_string();
    replacement.view_uuid = "replacement-view-uuid".to_string();
    second
        .create_view(replacement.clone())
        .await
        .expect("second writer should recreate the dropped view");
    drop_recovery_read.release();
    drop.await
        .expect("drop task should join")
        .expect("lost drop response should recover when a different view identity was recreated");

    let loaded = first
        .load_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect("view lookup should succeed")
        .expect("replacement view should exist");
    assert_eq!(loaded.view_id, replacement.view_id);
    assert_eq!(loaded.view_uuid, replacement.view_uuid);
}

#[tokio::test]
async fn strong_catalog_recovers_migration_snapshot_mutations_after_committed_put_response_loss() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let source = StrongTableCatalogBucketSnapshot::new_for_test(test_bucket_entry(bucket));
    let source_fingerprint = table_catalog_bucket_snapshot_fingerprint(&source).expect("source snapshot should hash");

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let (snapshot_etag, created) = store
        .materialize_bucket_snapshot(source)
        .await
        .expect("committed migration materialization should survive a lost PUT response");
    assert!(created);
    assert!(!snapshot_etag.is_empty());

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    store
        .remove_bucket_snapshot_if_unchanged(bucket, &source_fingerprint)
        .await
        .expect("committed migration rollback should survive a lost PUT response");

    let restarted = StrongTableCatalogStore::new(backend);
    assert!(
        restarted
            .get_table_bucket(bucket)
            .await
            .expect("table bucket lookup should succeed after restart")
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_rejects_snapshot_with_unreachable_commit_log() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    let base_table = test_table_entry(bucket, &namespace, &table, current_metadata.clone());
    store.create_table(base_table.clone()).await.expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let recovery_read = backend.pause_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;
    let request = TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        commit_id: "commit-1".to_string(),
        idempotency_key: Some("client-request".to_string()),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        expected_metadata_location: current_metadata,
        new_metadata_location: new_metadata,
        requirements: Vec::new(),
        writer: Some("pyiceberg/test".to_string()),
    };
    let commit_store = store.clone();
    let commit = tokio::spawn(async move { commit_store.commit_table(request).await });
    recovery_read.wait_started().await;

    let mut inconsistent = read_strong_snapshot(&backend).await;
    inconsistent.tables = vec![base_table];
    seed_strong_snapshot(&backend, &inconsistent).await;
    recovery_read.release();

    let error = commit
        .await
        .expect("commit task should join")
        .expect_err("an unreachable commit log must not prove that the table pointer advanced");
    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("post-commit put failure"));
    assert_matches!(
        store.load_table(bucket, "sales", "orders").await,
        Err(TableCatalogStoreError::Invalid(message))
            if message.contains("not recoverable in the current table history")
    );
}

#[tokio::test]
async fn strong_catalog_post_commit_recovery_does_not_install_stale_descendant() {
    let backend = TestCatalogObjectBackend::default();
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let bucket = "analytics";
    let bootstrap = StrongTableCatalogStore::new(backend.clone());
    bootstrap
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");

    let first = StrongTableCatalogStore::new(backend.clone());
    let second = StrongTableCatalogStore::new(backend.clone());
    first
        .get_table_bucket(bucket)
        .await
        .expect("first store should hydrate")
        .expect("table bucket should exist");
    second
        .get_table_bucket(bucket)
        .await
        .expect("second store should hydrate")
        .expect("table bucket should exist");

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let recovery_read = backend.pause_before_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;
    let first_namespace = Namespace::parse("first").expect("namespace should parse");
    let first_store = first.clone();
    let first_write = tokio::spawn(async move {
        first_store
            .create_namespace(test_namespace_entry(bucket, &first_namespace))
            .await
    });
    recovery_read.wait_started().await;

    let second_namespace = Namespace::parse("second").expect("namespace should parse");
    second
        .create_namespace(test_namespace_entry(bucket, &second_namespace))
        .await
        .expect("second writer should publish a descendant snapshot");
    recovery_read.release();
    first_write
        .await
        .expect("first writer task should finish")
        .expect("lost response should recover as committed");

    let state = first.state.lock().await;
    assert!(state.namespaces.contains_key(&(bucket.to_string(), "first".to_string())));
    assert!(state.namespaces.contains_key(&(bucket.to_string(), "second".to_string())));
}

#[tokio::test]
async fn strong_catalog_materialization_recovers_etag_after_transient_reload_failure() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    assert!(
        store
            .get_table_bucket(bucket)
            .await
            .expect("empty durable snapshot should hydrate")
            .is_none()
    );
    backend.fail_next_read(RUSTFS_META_BUCKET, &snapshot_path).await;

    let (snapshot_etag, created) = store
        .materialize_bucket_snapshot(StrongTableCatalogBucketSnapshot::new_for_test(test_bucket_entry(bucket)))
        .await
        .expect("materialization should recover the committed snapshot etag");

    assert!(created);
    let durable = backend
        .read_object(RUSTFS_META_BUCKET, &snapshot_path)
        .await
        .expect("durable snapshot read should succeed")
        .expect("durable snapshot should exist");
    assert_eq!(snapshot_etag, durable.etag.expect("durable snapshot should have an etag"));
    assert!(
        store
            .get_table_bucket(bucket)
            .await
            .expect("materialized bucket should load")
            .is_some()
    );
}

#[tokio::test]
async fn strong_catalog_rejects_cross_bucket_state_before_materialization() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let mut source = StrongTableCatalogBucketSnapshot::new_for_test(test_bucket_entry("analytics"));
    source.push_commit_for_test(StrongCommitSnapshotRecord::new_for_test(
        "victim".to_string(),
        "table-id".to_string(),
        "commit-1".to_string(),
        CommitLogEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            table_id: "table-id".to_string(),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            new_version_token: "token-v2".to_string(),
            previous_metadata_location: "metadata/00001.metadata.json".to_string(),
            new_metadata_location: "metadata/00002.metadata.json".to_string(),
            requirements: Vec::new(),
            status: CommitLogStatus::Committed,
            writer: None,
            created_at: None,
            updated_at: None,
        },
    ));

    assert_matches!(
        store.materialize_bucket_snapshot(source).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("owned by victim")
    );
    assert!(
        backend
            .read_object(
                RUSTFS_META_BUCKET,
                &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            )
            .await
            .expect("snapshot lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_rejects_invalid_bucket_before_materialization() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let mut bucket = test_bucket_entry("analytics");
    bucket.catalog_type = "unsupported".to_string();

    assert_matches!(
        store
            .materialize_bucket_snapshot(StrongTableCatalogBucketSnapshot::new_for_test(bucket))
            .await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("catalog type")
    );
    assert!(
        backend
            .read_object(
                RUSTFS_META_BUCKET,
                &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            )
            .await
            .expect("snapshot lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_rejects_unreadable_commit_before_materialization() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let mut source = StrongTableCatalogBucketSnapshot::new_for_test(test_bucket_entry(bucket));
    source.push_commit_for_test(StrongCommitSnapshotRecord::new_for_test(
        bucket.to_string(),
        "table-id".to_string(),
        "commit-1".to_string(),
        CommitLogEntry {
            version: TABLE_CATALOG_ENTRY_VERSION.saturating_add(1),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            table_id: "table-id".to_string(),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            new_version_token: "token-v2".to_string(),
            previous_metadata_location: "metadata/00001.metadata.json".to_string(),
            new_metadata_location: "metadata/00002.metadata.json".to_string(),
            requirements: Vec::new(),
            status: CommitLogStatus::Committed,
            writer: None,
            created_at: None,
            updated_at: None,
        },
    ));

    assert_matches!(
        store.materialize_bucket_snapshot(source).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("unsupported commit log entry version")
    );
    assert!(
        backend
            .read_object(
                RUSTFS_META_BUCKET,
                &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            )
            .await
            .expect("snapshot lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_rejects_inactive_bucket_with_active_namespace_before_persisting() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    let before = read_strong_snapshot(&backend).await;
    let mut inactive = test_bucket_entry(bucket);
    inactive.state = TableCatalogEntryState::Deleted;

    assert_matches!(
        store.put_table_bucket(inactive).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("active namespace")
    );
    assert_eq!(read_strong_snapshot(&backend).await, before);
    assert_eq!(
        StrongTableCatalogStore::new(backend)
            .get_table_bucket(bucket)
            .await
            .expect("durable bucket should reload")
            .expect("table bucket should remain")
            .state,
        TableCatalogEntryState::Active
    );
}

#[tokio::test]
async fn strong_catalog_validates_candidate_snapshot_before_publication() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let put_attempts = backend.put_attempt_count(RUSTFS_META_BUCKET, &snapshot_path).await;
    let mut entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    entry.table_id.clear();

    assert_matches!(
        store.register_table(entry).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("table id cannot be empty")
    );
    assert_eq!(
        backend.put_attempt_count(RUSTFS_META_BUCKET, &snapshot_path).await,
        put_attempts,
        "an unreadable candidate must be rejected before snapshot publication"
    );
}

#[tokio::test]
async fn strong_catalog_backing_commit_conflict_keeps_pointer_and_wal_unchanged() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

    let err = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request".to_string()),
            operation: "append".to_string(),
            expected_version_token: "stale-token".to_string(),
            expected_metadata_location: current_metadata.clone(),
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .unwrap_err();

    assert_matches!(err, TableCatalogStoreError::Conflict(_));
    let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    assert_eq!(loaded.metadata_location, current_metadata);
    assert_eq!(loaded.version_token, "token-v1");
    assert!(
        store
            .get_commit_by_id(bucket, "table-id", "commit-1")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_commit_by_idempotency_key(bucket, "table-id", "client-request")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_backing_rejects_invalid_table_warehouse_location() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    let mut entry = test_table_entry(bucket, &namespace, &table, current_metadata);
    entry.warehouse_location = "s3://other-bucket/tables/table-id".to_string();

    let err = store.create_table(entry).await.unwrap_err();

    assert_matches!(err, TableCatalogStoreError::Invalid(_));
    assert!(store.load_table(bucket, "sales", "orders").await.unwrap().is_none());
}

#[tokio::test]
async fn strong_catalog_backing_rejects_duplicate_table_warehouse_location_on_register() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let orders = IdentifierSegment::parse("orders").unwrap();
    let customers = IdentifierSegment::parse("customers").unwrap();
    let orders_metadata = default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json");
    let customers_metadata = default_table_metadata_file_path(&namespace, &customers, "00001.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &orders, orders_metadata))
        .await
        .unwrap();
    let mut duplicate = test_table_entry(bucket, &namespace, &customers, customers_metadata);
    duplicate.table_id = "table-id-2".to_string();
    duplicate.table_uuid = "table-uuid-2".to_string();
    duplicate.warehouse_location = format!("s3://{bucket}/tables/table-id");

    let err = store.register_table(duplicate).await.unwrap_err();

    assert_matches!(err, TableCatalogStoreError::Conflict(_));
    assert!(store.load_table(bucket, "sales", "customers").await.unwrap().is_none());
}

#[tokio::test]
async fn strong_catalog_backing_rejects_duplicate_table_warehouse_location_on_commit_relocation() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let orders = IdentifierSegment::parse("orders").unwrap();
    let customers = IdentifierSegment::parse("customers").unwrap();
    let orders_metadata = default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json");
    let relocated_metadata = default_table_metadata_file_path(&namespace, &orders, "00002.metadata.json");
    let customers_metadata = default_table_metadata_file_path(&namespace, &customers, "00001.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &orders, orders_metadata.clone()))
        .await
        .unwrap();
    let mut customers_entry = test_table_entry(bucket, &namespace, &customers, customers_metadata);
    customers_entry.table_id = "table-id-2".to_string();
    customers_entry.table_uuid = "table-uuid-2".to_string();
    customers_entry.warehouse_location = format!("s3://{bucket}/tables/customer-id");
    store.create_table(customers_entry).await.unwrap();
    backend
        .seed_object(
            bucket,
            &relocated_metadata,
            serde_json::to_vec(&serde_json::json!({
                "location": "s3://analytics/tables/customer-id",
                "table-uuid": "table-uuid"
            }))
            .unwrap(),
        )
        .await;

    let err = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: orders.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request".to_string()),
            operation: "set-location".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: orders_metadata.clone(),
            new_metadata_location: relocated_metadata,
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .unwrap_err();

    assert_matches!(err, TableCatalogStoreError::Conflict(_));
    let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    assert_eq!(loaded.metadata_location, orders_metadata);
    assert_eq!(loaded.warehouse_location, format!("s3://{bucket}/tables/table-id"));
    assert!(
        store
            .get_commit_by_id(bucket, "table-id", "commit-1")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_relocation_checks_past_the_current_warehouse_prefix() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let orders = IdentifierSegment::parse("orders").expect("table should parse");
    let customers = IdentifierSegment::parse("customers").expect("table should parse");
    let orders_metadata = default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json");
    let relocated_metadata = default_table_metadata_file_path(&namespace, &orders, "00002.metadata.json");
    let customers_metadata = default_table_metadata_file_path(&namespace, &customers, "00001.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    let mut orders_entry = test_table_entry(bucket, &namespace, &orders, orders_metadata.clone());
    orders_entry.warehouse_location = format!("s3://{bucket}/tables/group/orders");
    store.create_table(orders_entry).await.unwrap();
    let mut customers_entry = test_table_entry(bucket, &namespace, &customers, customers_metadata);
    customers_entry.table_id = "table-id-2".to_string();
    customers_entry.table_uuid = "table-uuid-2".to_string();
    customers_entry.warehouse_location = format!("s3://{bucket}/tables/group/customers");
    store.create_table(customers_entry).await.unwrap();
    backend
        .seed_object(
            bucket,
            &relocated_metadata,
            serde_json::to_vec(&serde_json::json!({
                "location": "s3://analytics/tables/group",
                "table-uuid": "table-uuid"
            }))
            .unwrap(),
        )
        .await;

    let error = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: orders.as_str().to_string(),
            commit_id: "commit-parent-relocation".to_string(),
            idempotency_key: None,
            operation: "set-location".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: orders_metadata,
            new_metadata_location: relocated_metadata,
            requirements: Vec::new(),
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .expect_err("parent relocation must detect the sibling after skipping the table's current prefix");

    assert_matches!(error, TableCatalogStoreError::Conflict(message) if message.contains("overlaps an active table"));
}

#[tokio::test]
async fn catalog_backings_reject_table_view_identifier_collisions() {
    for mode in [TableCatalogBackingMode::ObjectBacked, TableCatalogBackingMode::DurableStrong] {
        let store = ConfiguredTableCatalogStore::new_for_test(TestCatalogObjectBackend::default(), mode);
        let bucket = format!("collision-{mode:?}").to_ascii_lowercase();
        let table_first = Namespace::parse("table_first").expect("namespace should parse");
        let view_first = Namespace::parse("view_first").expect("namespace should parse");
        let identifier = IdentifierSegment::parse("orders").expect("identifier should parse");
        store
            .put_table_bucket(test_bucket_entry(&bucket))
            .await
            .expect("table bucket should be created");
        for namespace in [&table_first, &view_first] {
            store
                .create_namespace(test_namespace_entry(&bucket, namespace))
                .await
                .expect("namespace should be created");
        }

        store
            .create_table(test_table_entry(
                &bucket,
                &table_first,
                &identifier,
                default_table_metadata_file_path(&table_first, &identifier, "00001.metadata.json"),
            ))
            .await
            .expect("table should be created");
        let view_error = store
            .create_view(test_view_entry(
                &bucket,
                &table_first,
                &identifier,
                default_view_metadata_file_path(&table_first, &identifier, "00001.metadata.json"),
            ))
            .await
            .expect_err("view must not reuse a table identifier");
        assert_matches!(view_error, TableCatalogStoreError::Conflict(_));

        store
            .create_view(test_view_entry(
                &bucket,
                &view_first,
                &identifier,
                default_view_metadata_file_path(&view_first, &identifier, "00001.metadata.json"),
            ))
            .await
            .expect("view should be created");
        let mut table_entry = test_table_entry(
            &bucket,
            &view_first,
            &identifier,
            default_table_metadata_file_path(&view_first, &identifier, "00001.metadata.json"),
        );
        table_entry.table_id = "table-id-2".to_string();
        table_entry.table_uuid = "table-uuid-2".to_string();
        table_entry.warehouse_location = format!("s3://{bucket}/tables/table-id-2");
        let table_error = store
            .create_table(table_entry)
            .await
            .expect_err("table must not reuse a view identifier");
        assert_matches!(table_error, TableCatalogStoreError::Conflict(_));
    }
}

#[tokio::test]
async fn catalog_backings_persist_table_format_upgrade_and_replay() {
    for mode in [TableCatalogBackingMode::ObjectBacked, TableCatalogBackingMode::DurableStrong] {
        let backend = TestCatalogObjectBackend::default();
        let store = ConfiguredTableCatalogStore::new_for_test(backend.clone(), mode);
        let bucket = format!("format-upgrade-{mode:?}").to_ascii_lowercase();
        let namespace = Namespace::parse("sales").expect("namespace should parse");
        let table = IdentifierSegment::parse("orders").expect("table should parse");
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let next_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        store
            .put_table_bucket(test_bucket_entry(&bucket))
            .await
            .expect("table bucket should be created");
        store
            .create_namespace(test_namespace_entry(&bucket, &namespace))
            .await
            .expect("namespace should be created");
        let mut entry = test_table_entry(&bucket, &namespace, &table, current_metadata.clone());
        entry.format_version = 1;
        store.create_table(entry).await.expect("v1 table should be created");
        backend
            .seed_object(
                &bucket,
                &next_metadata,
                serde_json::to_vec(&serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": format!("s3://{bucket}/tables/table-id")
                }))
                .expect("target metadata should encode"),
            )
            .await;
        let request = TableCommitRequest {
            table_bucket: bucket.clone(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "format-upgrade-commit".to_string(),
            idempotency_key: Some("format-upgrade-request".to_string()),
            operation: "upgrade-format-version".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: next_metadata,
            requirements: Vec::new(),
            writer: Some("iceberg-rest/test".to_string()),
        };

        let committed = store
            .commit_table(request.clone())
            .await
            .expect("format upgrade should commit");
        assert_eq!(committed.table.format_version, 2);
        let replay = store
            .commit_table(request)
            .await
            .expect("exact format upgrade replay should succeed");
        assert_eq!(replay, committed);

        let restarted = ConfiguredTableCatalogStore::new_for_test(backend, mode);
        let loaded = restarted
            .load_table(&bucket, &namespace.public_name(), table.as_str())
            .await
            .expect("restarted catalog should load")
            .expect("upgraded table should persist");
        assert_eq!(loaded.format_version, 2);
        assert_eq!(loaded.metadata_location, committed.table.metadata_location);
    }
}

#[tokio::test]
async fn catalog_backings_hide_and_reject_mutation_of_inactive_resources() {
    for mode in [TableCatalogBackingMode::ObjectBacked, TableCatalogBackingMode::DurableStrong] {
        let backend = TestCatalogObjectBackend::default();
        let store = ConfiguredTableCatalogStore::new_for_test(backend.clone(), mode);
        let bucket = format!("inactive-{mode:?}").to_ascii_lowercase();
        let namespace = Namespace::parse("sales").expect("namespace should parse");
        let table = IdentifierSegment::parse("orders").expect("table should parse");
        let view = IdentifierSegment::parse("summary").expect("view should parse");
        let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let current_view_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
        let new_view_metadata = default_view_metadata_file_path(&namespace, &view, "00002.metadata.json");
        store
            .put_table_bucket(test_bucket_entry(&bucket))
            .await
            .expect("table bucket should be created");
        store
            .create_namespace(test_namespace_entry(&bucket, &namespace))
            .await
            .expect("namespace should be created");
        let mut table_entry = test_table_entry(&bucket, &namespace, &table, current_metadata.clone());
        table_entry.state = TableCatalogEntryState::Deleted;
        store
            .create_table(table_entry)
            .await
            .expect("inactive table should be retained");
        let mut view_entry = test_view_entry(&bucket, &namespace, &view, current_view_metadata.clone());
        view_entry.state = TableCatalogEntryState::Deleted;
        store.create_view(view_entry).await.expect("inactive view should be retained");

        assert!(
            store
                .load_table(&bucket, &namespace.public_name(), table.as_str())
                .await
                .expect("table lookup should succeed")
                .is_none()
        );
        assert!(
            store
                .load_view(&bucket, &namespace.public_name(), view.as_str())
                .await
                .expect("view lookup should succeed")
                .is_none()
        );
        assert!(
            store
                .list_tables(&bucket, &namespace.public_name())
                .await
                .expect("tables should list")
                .is_empty()
        );
        assert!(
            store
                .list_all_tables(&bucket)
                .await
                .expect("all tables should list")
                .is_empty()
        );
        assert!(
            store
                .list_views(&bucket, &namespace.public_name())
                .await
                .expect("views should list")
                .is_empty()
        );
        assert_matches!(
            store.drop_namespace(&bucket, &namespace.public_name()).await,
            Err(TableCatalogStoreError::Conflict(_))
        );

        backend.seed_object(&bucket, &new_metadata, b"{}".to_vec()).await;
        let commit_error = store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.clone(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                commit_id: "commit-1".to_string(),
                idempotency_key: Some("request-1".to_string()),
                operation: "append".to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_metadata,
                new_metadata_location: new_metadata,
                requirements: Vec::new(),
                writer: Some("pyiceberg/test".to_string()),
            })
            .await
            .expect_err("inactive table must reject commits");
        assert_matches!(commit_error, TableCatalogStoreError::NotFound(_));

        let view_error = store
            .replace_view(ViewCommitRequest {
                table_bucket: bucket.clone(),
                namespace: namespace.public_name(),
                view: view.as_str().to_string(),
                expected_version_token: "token-v1".to_string(),
                expected_metadata_location: current_view_metadata,
                new_metadata_location: new_view_metadata,
            })
            .await
            .expect_err("inactive view must reject replacement");
        assert_matches!(view_error, TableCatalogStoreError::NotFound(_));
    }
}

#[tokio::test]
async fn strong_catalog_rejects_invalid_inactive_resources_before_snapshot_write() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let view = IdentifierSegment::parse("summary").expect("view should parse");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");

    let mut table_entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    table_entry.state = TableCatalogEntryState::Deleted;
    table_entry.metadata_location = "data/inactive.parquet".to_string();
    assert_matches!(store.create_table(table_entry).await, Err(TableCatalogStoreError::Invalid(_)));

    let mut view_entry = test_view_entry(
        bucket,
        &namespace,
        &view,
        default_view_metadata_file_path(&namespace, &view, "00001.metadata.json"),
    );
    view_entry.state = TableCatalogEntryState::Deleted;
    view_entry.metadata_location = "data/inactive-view.parquet".to_string();
    assert_matches!(store.create_view(view_entry).await, Err(TableCatalogStoreError::Invalid(_)));

    let durable = read_strong_snapshot(&backend).await;
    assert!(durable.tables.is_empty());
    assert!(durable.views.is_empty());
    let restarted = StrongTableCatalogStore::new(backend);
    assert!(
        restarted
            .list_all_tables(bucket)
            .await
            .expect("tables should list")
            .is_empty()
    );
    assert!(
        restarted
            .list_views(bucket, &namespace.public_name())
            .await
            .expect("views should list")
            .is_empty()
    );
}

#[tokio::test]
async fn object_catalog_concurrent_table_view_creation_has_one_winner() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let identifier = IdentifierSegment::parse("orders").expect("identifier should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    let table_entry = test_table_entry(
        bucket,
        &namespace,
        &identifier,
        default_table_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );
    let view_entry = test_view_entry(
        bucket,
        &namespace,
        &identifier,
        default_view_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );

    let (table_result, view_result) = tokio::join!(store.create_table(table_entry), store.create_view(view_entry));

    assert_eq!(usize::from(table_result.is_ok()) + usize::from(view_result.is_ok()), 1);
    assert!(table_result.is_ok() || matches!(table_result, Err(TableCatalogStoreError::Conflict(_))));
    assert!(view_result.is_ok() || matches!(view_result, Err(TableCatalogStoreError::Conflict(_))));
    let table_exists = store
        .load_table(bucket, &namespace.public_name(), identifier.as_str())
        .await
        .expect("table lookup should succeed")
        .is_some();
    let view_exists = store
        .load_view(bucket, &namespace.public_name(), identifier.as_str())
        .await
        .expect("view lookup should succeed")
        .is_some();
    assert_ne!(table_exists, view_exists);
}

#[tokio::test]
async fn strong_catalog_independent_table_view_creation_has_one_winner() {
    let backend = TestCatalogObjectBackend::default();
    let bootstrap = StrongTableCatalogStore::new(backend.clone());
    let table_store = StrongTableCatalogStore::new(backend.clone());
    let view_store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let identifier = IdentifierSegment::parse("orders").expect("identifier should parse");
    bootstrap
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    bootstrap
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    table_store
        .get_namespace(bucket, &namespace.public_name())
        .await
        .expect("table writer should hydrate")
        .expect("namespace should exist");
    view_store
        .get_namespace(bucket, &namespace.public_name())
        .await
        .expect("view writer should hydrate")
        .expect("namespace should exist");

    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let paused_put = backend.pause_next_put(RUSTFS_META_BUCKET, &snapshot_path).await;
    let table_entry = test_table_entry(
        bucket,
        &namespace,
        &identifier,
        default_table_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );
    let table_write = tokio::spawn(async move {
        table_store
            .register_table_with_publication(table_entry, &UnserializedTestPublication)
            .await
    });
    paused_put.wait_started().await;

    view_store
        .create_view(test_view_entry(
            bucket,
            &namespace,
            &identifier,
            default_view_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
        ))
        .await
        .expect("view writer should win the snapshot CAS");
    paused_put.release();
    assert_matches!(
        table_write.await.expect("table writer task should join"),
        Err(TableCatalogStoreError::Conflict(_))
    );

    let reader = StrongTableCatalogStore::new(backend);
    assert!(
        reader
            .load_table(bucket, &namespace.public_name(), identifier.as_str())
            .await
            .expect("table lookup should succeed")
            .is_none()
    );
    assert!(
        reader
            .load_view(bucket, &namespace.public_name(), identifier.as_str())
            .await
            .expect("view lookup should succeed")
            .is_some()
    );
}

#[tokio::test]
async fn strong_catalog_quarantines_and_repairs_legacy_identifier_collisions() {
    let backend = TestCatalogObjectBackend::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let identifier = IdentifierSegment::parse("orders").expect("identifier should parse");
    let table = test_table_entry(
        bucket,
        &namespace,
        &identifier,
        default_table_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );
    let view = test_view_entry(
        bucket,
        &namespace,
        &identifier,
        default_view_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );
    let second_identifier = IdentifierSegment::parse("returns").expect("identifier should parse");
    let mut second_table = test_table_entry(
        bucket,
        &namespace,
        &second_identifier,
        default_table_metadata_file_path(&namespace, &second_identifier, "00001.metadata.json"),
    );
    second_table.table_id = "table-id-2".to_string();
    second_table.table_uuid = "table-uuid-2".to_string();
    second_table.warehouse_location = format!("s3://{bucket}/tables/table-id-2");
    let mut second_view = test_view_entry(
        bucket,
        &namespace,
        &second_identifier,
        default_view_metadata_file_path(&namespace, &second_identifier, "00001.metadata.json"),
    );
    second_view.view_id = "view-id-2".to_string();
    second_view.view_uuid = "view-uuid-2".to_string();
    second_view.warehouse_location = format!("s3://{bucket}/views/view-id-2");
    let snapshot = test_strong_snapshot(bucket, &namespace, vec![table.clone(), second_table], vec![view, second_view]);
    seed_strong_snapshot(&backend, &snapshot).await;
    let store =
        StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION);

    let error = store
        .load_table(bucket, &namespace.public_name(), identifier.as_str())
        .await
        .expect_err("ambiguous legacy identifiers must fail closed");
    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("operator cleanup"));
    assert_matches!(
        store.list_views(bucket, &namespace.public_name()).await,
        Err(TableCatalogStoreError::Internal(_))
    );
    assert_matches!(store.list_all_tables(bucket).await, Err(TableCatalogStoreError::Internal(_)));

    let unrelated = Namespace::parse("unrelated").expect("namespace should parse");
    assert_matches!(
        store.create_namespace(test_namespace_entry(bucket, &unrelated)).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("collision cleanup")
    );
    let v2_writer =
        StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_VERSION);
    let blocked = Namespace::parse("blocked").expect("namespace should parse");
    assert_matches!(
        v2_writer.create_namespace(test_namespace_entry(bucket, &blocked)).await,
        Err(TableCatalogStoreError::Conflict(_))
    );

    store
        .drop_view(bucket, &namespace.public_name(), identifier.as_str())
        .await
        .expect("dropping one ambiguous resource must make cleanup progress");
    assert_eq!(
        read_strong_snapshot(&backend).await.version,
        STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION
    );
    assert_matches!(
        v2_writer.create_namespace(test_namespace_entry(bucket, &unrelated)).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    store
        .drop_view(bucket, &namespace.public_name(), second_identifier.as_str())
        .await
        .expect("dropping the final ambiguous resource must finish cleanup");
    assert_eq!(
        read_strong_snapshot(&backend).await.version,
        STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION
    );
    v2_writer
        .create_namespace(test_namespace_entry(bucket, &unrelated))
        .await
        .expect("ordinary writes should resume after collision cleanup");
    assert_eq!(read_strong_snapshot(&backend).await.version, STRONG_TABLE_CATALOG_SNAPSHOT_VERSION);
    assert_eq!(
        store
            .load_table(bucket, &namespace.public_name(), identifier.as_str())
            .await
            .expect("repaired table should load"),
        Some(table.clone())
    );

    let restarted =
        StrongTableCatalogStore::new_with_snapshot_write_version(backend, STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION);
    assert_eq!(
        restarted
            .load_table(bucket, &namespace.public_name(), identifier.as_str())
            .await
            .expect("repaired snapshot should survive restart"),
        Some(table)
    );
    assert!(
        restarted
            .get_namespace(bucket, &unrelated.public_name())
            .await
            .expect("unrelated namespace lookup should succeed")
            .is_some()
    );
}

#[tokio::test]
async fn strong_catalog_rejects_restored_v1_collision_after_observing_v2() {
    let backend = TestCatalogObjectBackend::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let identifier = IdentifierSegment::parse("orders").expect("identifier should parse");
    let mut upgraded = test_strong_snapshot(bucket, &namespace, Vec::new(), Vec::new());
    upgraded.version = STRONG_TABLE_CATALOG_SNAPSHOT_VERSION;
    seed_strong_snapshot(&backend, &upgraded).await;
    let store = StrongTableCatalogStore::new(backend.clone());
    store
        .get_table_bucket(bucket)
        .await
        .expect("version 2 snapshot should load")
        .expect("table bucket should exist");

    let table = test_table_entry(
        bucket,
        &namespace,
        &identifier,
        default_table_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );
    let view = test_view_entry(
        bucket,
        &namespace,
        &identifier,
        default_view_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );
    seed_strong_snapshot(&backend, &test_strong_snapshot(bucket, &namespace, vec![table], vec![view])).await;

    let error = store
        .get_table_bucket(bucket)
        .await
        .expect_err("a restored v1 collision must not replace observed v2 state");
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("version 2"));
}

#[tokio::test]
async fn strong_catalog_v2_snapshot_rejects_table_view_identifier_collision() {
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let identifier = IdentifierSegment::parse("orders").expect("identifier should parse");
    let table = test_table_entry(
        bucket,
        &namespace,
        &identifier,
        default_table_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );
    let view = test_view_entry(
        bucket,
        &namespace,
        &identifier,
        default_view_metadata_file_path(&namespace, &identifier, "00001.metadata.json"),
    );
    let mut snapshot = test_strong_snapshot(bucket, &namespace, vec![table], vec![view]);
    snapshot.version = STRONG_TABLE_CATALOG_SNAPSHOT_VERSION;

    let error = strong_snapshot_hydration_error(snapshot).await;

    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("table/view"));
}

#[tokio::test]
async fn strong_catalog_snapshot_rejects_corrupt_resource_ownership() {
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let orders = IdentifierSegment::parse("orders").expect("table should parse");
    let returns = IdentifierSegment::parse("returns").expect("table should parse");
    let mut invalid_metadata = test_table_entry(
        bucket,
        &namespace,
        &orders,
        default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json"),
    );
    invalid_metadata.metadata_location = "data/part.parquet".to_string();
    let error =
        strong_snapshot_hydration_error(test_strong_snapshot(bucket, &namespace, vec![invalid_metadata], Vec::new())).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("invalid metadata location"));

    let mut inactive_table = test_table_entry(
        bucket,
        &namespace,
        &orders,
        default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json"),
    );
    inactive_table.state = TableCatalogEntryState::Deleted;
    inactive_table.metadata_location = "data/inactive.parquet".to_string();
    let mut invalid_inactive_table_snapshot = test_strong_snapshot(bucket, &namespace, vec![inactive_table], Vec::new());
    invalid_inactive_table_snapshot.version = STRONG_TABLE_CATALOG_SNAPSHOT_VERSION;
    let error = strong_snapshot_hydration_error(invalid_inactive_table_snapshot).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("invalid metadata location"));

    let view = IdentifierSegment::parse("summary").expect("view should parse");
    let mut empty_view_id = test_view_entry(
        bucket,
        &namespace,
        &view,
        default_view_metadata_file_path(&namespace, &view, "00001.view-metadata.json"),
    );
    empty_view_id.view_id.clear();
    let error = strong_snapshot_hydration_error(test_strong_snapshot(bucket, &namespace, Vec::new(), vec![empty_view_id])).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("view id cannot be empty"));

    let mut inactive_view = test_view_entry(
        bucket,
        &namespace,
        &view,
        default_view_metadata_file_path(&namespace, &view, "00001.metadata.json"),
    );
    inactive_view.state = TableCatalogEntryState::Deleted;
    inactive_view.metadata_location = "data/inactive-view.parquet".to_string();
    let mut invalid_inactive_view_snapshot = test_strong_snapshot(bucket, &namespace, Vec::new(), vec![inactive_view]);
    invalid_inactive_view_snapshot.version = STRONG_TABLE_CATALOG_SNAPSHOT_VERSION;
    let error = strong_snapshot_hydration_error(invalid_inactive_view_snapshot).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("invalid metadata location"));

    let resource_backed_namespace = test_table_entry(
        bucket,
        &namespace,
        &orders,
        default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json"),
    );
    let resource_backed_snapshot = StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: Vec::new(),
        tables: vec![resource_backed_namespace],
        views: Vec::new(),
        commits: Vec::new(),
        idempotency: Vec::new(),
    };
    let backend = TestCatalogObjectBackend::default();
    seed_strong_snapshot(&backend, &resource_backed_snapshot).await;
    let store = StrongTableCatalogStore::new(backend);
    assert!(
        store
            .get_namespace(bucket, &namespace.public_name())
            .await
            .expect("resource-backed namespace should load")
            .is_some()
    );

    let mut inactive_resource = test_table_entry(
        bucket,
        &namespace,
        &orders,
        default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json"),
    );
    inactive_resource.state = TableCatalogEntryState::Deleted;
    let inactive_resource_snapshot = StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: Vec::new(),
        tables: vec![inactive_resource],
        views: Vec::new(),
        commits: Vec::new(),
        idempotency: Vec::new(),
    };
    let error = strong_snapshot_hydration_error(inactive_resource_snapshot).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("no active namespace"));

    let child = Namespace::parse("sales.daily").expect("child namespace should parse");
    let mut inactive_parent = test_namespace_entry(bucket, &namespace);
    inactive_parent.state = TableCatalogEntryState::Deleted;
    let inactive_parent_snapshot = StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: vec![inactive_parent, test_namespace_entry(bucket, &child)],
        tables: Vec::new(),
        views: Vec::new(),
        commits: Vec::new(),
        idempotency: Vec::new(),
    };
    let error = strong_snapshot_hydration_error(inactive_parent_snapshot).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("inactive namespace"));

    let mut inactive_bucket = test_bucket_entry(bucket);
    inactive_bucket.state = TableCatalogEntryState::Deleted;
    let inactive_bucket_snapshot = StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION,
        table_buckets: vec![inactive_bucket],
        namespaces: vec![test_namespace_entry(bucket, &namespace)],
        tables: Vec::new(),
        views: Vec::new(),
        commits: Vec::new(),
        idempotency: Vec::new(),
    };
    let error = strong_snapshot_hydration_error(inactive_bucket_snapshot).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("no active table bucket"));

    let orders_entry = test_table_entry(
        bucket,
        &namespace,
        &orders,
        default_table_metadata_file_path(&namespace, &orders, "00001.metadata.json"),
    );
    let mut duplicate_table_id = test_table_entry(
        bucket,
        &namespace,
        &returns,
        default_table_metadata_file_path(&namespace, &returns, "00001.metadata.json"),
    );
    duplicate_table_id.warehouse_location = format!("s3://{bucket}/tables/returns-id");
    let error = strong_snapshot_hydration_error(test_strong_snapshot(
        bucket,
        &namespace,
        vec![orders_entry, duplicate_table_id],
        Vec::new(),
    ))
    .await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("table ids"));
}

#[tokio::test]
async fn strong_catalog_v1_inactive_resources_are_hidden_and_cleanup_only() {
    let backend = TestCatalogObjectBackend::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let view = IdentifierSegment::parse("summary").expect("view should parse");
    let mut table_entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    table_entry.state = TableCatalogEntryState::Deleted;
    table_entry.warehouse_location = "legacy-invalid-location".to_string();
    table_entry.metadata_location = "legacy-invalid-metadata".to_string();
    let mut view_entry = test_view_entry(
        bucket,
        &namespace,
        &view,
        default_view_metadata_file_path(&namespace, &view, "00001.metadata.json"),
    );
    view_entry.state = TableCatalogEntryState::Deleted;
    view_entry.warehouse_location = "legacy-invalid-view-location".to_string();
    view_entry.metadata_location = "legacy-invalid-view-metadata".to_string();
    let snapshot = StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: Vec::new(),
        tables: vec![table_entry],
        views: vec![view_entry],
        commits: Vec::new(),
        idempotency: Vec::new(),
    };
    seed_strong_snapshot(&backend, &snapshot).await;
    let store =
        StrongTableCatalogStore::new_with_snapshot_write_version(backend.clone(), STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION);

    assert!(store.list_all_tables(bucket).await.expect("tables should list").is_empty());
    assert!(
        store
            .list_views(bucket, &namespace.public_name())
            .await
            .expect("views should list")
            .is_empty()
    );
    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("legacy inactive table should be removable");
    store
        .drop_view(bucket, &namespace.public_name(), view.as_str())
        .await
        .expect("legacy inactive view should be removable");

    let cleaned = read_strong_snapshot(&backend).await;
    assert!(cleaned.tables.is_empty());
    assert!(cleaned.views.is_empty());
    assert_eq!(cleaned.version, STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION);
}

#[tokio::test]
async fn strong_catalog_snapshot_rejects_mismatched_commit_indexes() {
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let table_entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    let commit = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "commit-1".to_string(),
        idempotency_key: Some("request-1".to_string()),
        table_id: table_entry.table_id.clone(),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        new_version_token: "token-v2".to_string(),
        previous_metadata_location: table_entry.metadata_location.clone(),
        new_metadata_location: default_table_metadata_file_path(&namespace, &table, "00002.metadata.json"),
        requirements: Vec::new(),
        status: CommitLogStatus::Committed,
        writer: Some("pyiceberg/test".to_string()),
        created_at: None,
        updated_at: None,
    };
    let record = |lookup_key: &str, commit: CommitLogEntry| {
        StrongCommitSnapshotRecord::new_for_test(bucket.to_string(), table_entry.table_id.clone(), lookup_key.to_string(), commit)
    };
    let snapshot = |commits, idempotency| StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: vec![test_namespace_entry(bucket, &namespace)],
        tables: vec![table_entry.clone()],
        views: Vec::new(),
        commits,
        idempotency,
    };

    let error = strong_snapshot_hydration_error(snapshot(vec![record("wrong", commit.clone())], Vec::new())).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("snapshot owner"));

    let error = strong_snapshot_hydration_error(snapshot(vec![record("commit-1", commit.clone())], Vec::new())).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("idempotency index"));

    let mut mismatched = commit.clone();
    mismatched.new_version_token = "token-v3".to_string();
    let error = strong_snapshot_hydration_error(snapshot(
        vec![record("commit-1", commit.clone())],
        vec![record("request-1", mismatched)],
    ))
    .await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("matching commit"));

    let error = strong_snapshot_hydration_error(snapshot(
        vec![record("commit-1", commit.clone())],
        vec![record("request-1", commit.clone()), record("request-1", commit)],
    ))
    .await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("duplicate idempotency"));
}

#[tokio::test]
async fn strong_catalog_snapshot_rejects_commit_outside_current_history() {
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let table_entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    let commit = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "commit-1".to_string(),
        idempotency_key: None,
        table_id: table_entry.table_id.clone(),
        operation: "append".to_string(),
        expected_version_token: table_entry.version_token.clone(),
        new_version_token: "token-v2".to_string(),
        previous_metadata_location: table_entry.metadata_location.clone(),
        new_metadata_location: default_table_metadata_file_path(&namespace, &table, "00002.metadata.json"),
        requirements: Vec::new(),
        status: CommitLogStatus::Committed,
        writer: None,
        created_at: None,
        updated_at: None,
    };
    let snapshot = StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: vec![test_namespace_entry(bucket, &namespace)],
        tables: vec![table_entry.clone()],
        views: Vec::new(),
        commits: vec![StrongCommitSnapshotRecord::new_for_test(
            bucket.to_string(),
            table_entry.table_id,
            commit.commit_id.clone(),
            commit,
        )],
        idempotency: Vec::new(),
    };

    let error = strong_snapshot_hydration_error(snapshot).await;
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message.contains("not recoverable in the current table history")
    );
}

#[tokio::test]
async fn strong_catalog_snapshot_discards_dropped_table_commit_indexes() {
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let commit = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "commit-1".to_string(),
        idempotency_key: Some("request-1".to_string()),
        table_id: "dropped-table-id".to_string(),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        new_version_token: "token-v2".to_string(),
        previous_metadata_location: "tables/dropped-table-id/metadata/00001.metadata.json".to_string(),
        new_metadata_location: "tables/dropped-table-id/metadata/00002.metadata.json".to_string(),
        requirements: Vec::new(),
        status: CommitLogStatus::Committed,
        writer: Some("pyiceberg/test".to_string()),
        created_at: None,
        updated_at: None,
    };
    let record = |lookup_key: &str, commit: CommitLogEntry| {
        StrongCommitSnapshotRecord::new_for_test(
            bucket.to_string(),
            "dropped-table-id".to_string(),
            lookup_key.to_string(),
            commit,
        )
    };
    let snapshot = StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_MIN_READ_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: vec![test_namespace_entry(bucket, &namespace)],
        tables: Vec::new(),
        views: Vec::new(),
        commits: vec![record("commit-1", commit.clone())],
        idempotency: vec![record("request-1", commit)],
    };
    let mut strict_snapshot = snapshot.clone();
    strict_snapshot.version = STRONG_TABLE_CATALOG_SNAPSHOT_VERSION;
    let error = strong_snapshot_hydration_error(strict_snapshot).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("no owning table"));

    let mut unsupported_idempotency = snapshot.clone();
    unsupported_idempotency.commits.clear();
    unsupported_idempotency.idempotency[0].commit.version = u16::MAX;
    let error = strong_snapshot_hydration_error(unsupported_idempotency).await;
    assert_matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("commit idempotency entry version"));

    let backend = TestCatalogObjectBackend::default();
    seed_strong_snapshot(&backend, &snapshot).await;

    let store = StrongTableCatalogStore::new(backend);
    store
        .get_table_bucket(bucket)
        .await
        .expect("legacy dropped-table indexes should not break hydration")
        .expect("table bucket should exist");
    assert!(
        store
            .get_commit_by_id(bucket, "dropped-table-id", "commit-1")
            .await
            .expect("commit lookup should succeed")
            .is_none()
    );
    assert!(
        store
            .get_commit_by_idempotency_key(bucket, "dropped-table-id", "request-1")
            .await
            .expect("idempotency lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn diagnostics_backing_manifest_requires_recovery_before_migration() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &commit_path, 2).await;

    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap();

    let diagnostics = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

    assert_eq!(diagnostics.backing_manifest.current.wal.finalization_required_count, 1);
    assert_eq!(
        diagnostics.backing_manifest.migration.status,
        TableCatalogBackingMigrationStatus::RecoveryRequired
    );
    assert!(
        diagnostics
            .backing_manifest
            .migration
            .blockers
            .contains(&TableCatalogBackingMigrationBlocker::CommitRecoveryRequired)
    );
}

#[tokio::test]
async fn consistency_check_reports_missing_metadata_object() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;

    let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

    assert_eq!(report.catalog.table.metadata_location, current.clone());
    assert_eq!(report.current_metadata_status, TableMetadataPointerStatus::MissingObject);
    assert_eq!(report.recovery_status, TableCatalogRecoveryStatus::ReadOnlyRecommended);
    assert_eq!(report.recommended_actions, vec![TableCatalogRecoveryAction::RestoreCurrentMetadataObject]);
    assert!(report.orphan_metadata_candidate_locations.is_empty());
}

#[tokio::test]
async fn consistency_check_reports_invalid_metadata_location() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let invalid_metadata = ".rustfs-table/warehouses/default/namespaces/sales/tables/other/metadata/00001.metadata.json";

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, invalid_metadata.to_string()))
        .await
        .unwrap();

    let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

    assert_eq!(report.catalog.table.metadata_location, invalid_metadata);
    assert_eq!(report.current_metadata_status, TableMetadataPointerStatus::InvalidLocation);
    assert!(report.orphan_metadata_candidate_locations.is_empty());
}

#[tokio::test]
async fn orphan_metadata_scan_does_not_treat_largest_version_as_committed() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let uncommitted = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    backend
        .seed_object(bucket, &uncommitted, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

    assert_eq!(report.current_metadata_status, TableMetadataPointerStatus::Valid);
    assert_eq!(report.catalog.table.metadata_location, current);
    assert_eq!(report.orphan_metadata_candidate_locations, vec![uncommitted]);
}

#[tokio::test]
async fn orphan_metadata_scan_keeps_metadata_for_protected_snapshot_refs() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let orphan = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let tagged = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let current = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.seed_object(bucket, &orphan, b"{}".to_vec()).await;
    backend
        .seed_object(
            bucket,
            &tagged,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 10
            }))
            .unwrap(),
        )
        .await;
    backend
        .seed_object(
            bucket,
            &current,
            serde_json::to_vec(&serde_json::json!({
                "current-snapshot-id": 20,
                "metadata-log": [],
                "refs": {
                    "audit": {
                        "snapshot-id": 10,
                        "type": "tag"
                    }
                }
            }))
            .unwrap(),
        )
        .await;

    let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

    assert_eq!(report.current_metadata_status, TableMetadataPointerStatus::Valid);
    assert_eq!(report.orphan_metadata_candidate_locations, vec![orphan]);
}

#[tokio::test]
async fn object_table_catalog_store_commits_with_token_match_and_writes_log() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

    let result = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client/%2f\nrequest".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata.clone(),
            requirements: vec![serde_json::json!({"type": "assert-table-uuid", "uuid": "table-uuid"})],
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .unwrap();

    assert_eq!(result.table.metadata_location, new_metadata);
    assert_ne!(result.table.version_token, "token-v1");
    assert_eq!(result.table.generation, 2);
    assert_eq!(result.commit_log.status, CommitLogStatus::Committed);

    let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    assert_eq!(loaded.metadata_location, result.table.metadata_location);
    assert_eq!(loaded.version_token, result.table.version_token);
    assert!(
        store
            .get_commit_by_id(bucket, "table-id", "commit-1")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .get_commit_by_idempotency_key(bucket, "table-id", "client/%2f\nrequest")
            .await
            .unwrap()
            .is_some()
    );
}

async fn assert_catalog_rejects_metadata_changed_after_validation<S>(store: &S, backend: &TestCatalogObjectBackend, bucket: &str)
where
    S: TableCatalogStore + ?Sized,
{
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    let validated_metadata = serde_json::json!({"format-version": 2, "properties": {"owner": "validated"}});
    let changed_metadata = serde_json::json!({"format-version": 2, "properties": {"owner": "replaced"}});
    backend
        .seed_object(
            bucket,
            &new_metadata,
            serde_json::to_vec(&changed_metadata).expect("changed metadata should encode"),
        )
        .await;

    let error = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "digest-commit".to_string(),
            idempotency_key: Some("digest-request".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata.clone(),
            new_metadata_location: new_metadata,
            requirements: vec![serde_json::json!({
                "type": TABLE_METADATA_DIGEST_REQUIREMENT_TYPE,
                "sha256": canonical_json_sha256(&validated_metadata).expect("metadata digest should build")
            })],
            writer: Some("test-client".to_string()),
        })
        .await
        .expect_err("metadata changed after validation must not publish");

    assert_matches!(error, TableCatalogStoreError::Conflict(_));
    let unchanged = store
        .load_table(bucket, "sales", "orders")
        .await
        .expect("table lookup should succeed")
        .expect("table should remain");
    assert_eq!(unchanged.metadata_location, current_metadata);
    assert_eq!(unchanged.version_token, "token-v1");
    assert_eq!(unchanged.generation, 1);
}

#[tokio::test]
async fn catalog_backings_reject_metadata_changed_after_validation() {
    let object_backend = TestCatalogObjectBackend::default();
    assert_catalog_rejects_metadata_changed_after_validation(
        &ObjectTableCatalogStore::new(object_backend.clone()),
        &object_backend,
        "object-catalog",
    )
    .await;

    let strong_backend = TestCatalogObjectBackend::default();
    assert_catalog_rejects_metadata_changed_after_validation(
        &StrongTableCatalogStore::new(strong_backend.clone()),
        &strong_backend,
        "strong-catalog",
    )
    .await;
}

#[tokio::test]
async fn object_table_catalog_store_syncs_warehouse_location_from_committed_metadata() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    store.backfill_table_warehouse_index(bucket).await.unwrap();
    backend.reset_call_counts().await;
    backend
        .seed_object(
            bucket,
            &new_metadata,
            serde_json::to_vec(&serde_json::json!({
                "location": "s3://analytics/tables/relocated-table-id",
                "table-uuid": "table-uuid"
            }))
            .unwrap(),
        )
        .await;

    let result = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "set-location".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata,
            requirements: vec![serde_json::json!({"type": "assert-table-uuid", "uuid": "table-uuid"})],
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .unwrap();

    assert_eq!(result.table.warehouse_location, "s3://analytics/tables/relocated-table-id");
    backend.reset_call_counts().await;
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/relocated-table-id/data/part-00001.parquet")
        .await
        .expect("data-plane resource lookup should succeed")
        .expect("relocated table warehouse object should resolve to the table");
    assert_eq!(resource.table, "orders");
    assert_eq!(resource.warehouse_object_prefix, "tables/relocated-table-id/");
    let old_resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("old table warehouse lookup should succeed");
    assert!(old_resource.is_none());
    assert_eq!(backend.list_call_count().await, 1);
}

#[tokio::test]
async fn object_table_catalog_store_reuses_old_prefix_after_failed_relocation_index_delete() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let next_table = IdentifierSegment::parse("returns").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let old_index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    let old_entry = test_table_entry(bucket, &namespace, &table, current_metadata.clone());
    store
        .create_table(old_entry.clone())
        .await
        .expect("old table should be created");
    backend
        .seed_object(
            bucket,
            &new_metadata,
            serde_json::to_vec(&serde_json::json!({
                "location": "s3://analytics/tables/relocated-table-id",
                "table-uuid": "table-uuid"
            }))
            .unwrap(),
        )
        .await;
    backend.fail_delete_attempt(RUSTFS_META_BUCKET, &old_index_path, 1).await;

    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "set-location".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata.clone(),
            new_metadata_location: new_metadata,
            requirements: vec![serde_json::json!({"type": "assert-table-uuid", "uuid": "table-uuid"})],
            writer: Some("pyiceberg/test".to_string()),
        })
        .await
        .unwrap();

    let mut next_entry = test_table_entry(bucket, &namespace, &next_table, current_metadata);
    next_entry.table_id = "next-table-id".to_string();
    next_entry.warehouse_location = format!("s3://{bucket}/tables/table-id");
    store
        .create_table(next_entry)
        .await
        .expect("stale old warehouse index should not block prefix reuse");

    let (index, _) = store
        .read_entry::<TableWarehouseIndexEntry>(store.catalog_bucket(), &old_index_path)
        .await
        .unwrap()
        .expect("reused prefix index should exist");
    assert_eq!(index.table, "returns");
    assert_eq!(index.table_id, "next-table-id");

    store
        .delete_table_warehouse_index(&old_entry)
        .await
        .expect("old owner should not delete reused prefix index");
    let reused_resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("reused prefix lookup should succeed")
        .expect("reused prefix should still resolve to the new table");
    assert_eq!(reused_resource.table, "returns");
    assert_eq!(reused_resource.table_id, "next-table-id");
}

#[tokio::test]
async fn object_table_catalog_store_does_not_advance_table_when_idempotency_staging_fails() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let idempotency_key = "client-request";
    let idempotency_path = TableCatalogObjectPaths::default().commit_idempotency_entry_path(bucket, "table-id", idempotency_key);

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &idempotency_path, 1).await;

    let err = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some(idempotency_key.to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata.clone(),
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap_err();

    assert_matches!(err, TableCatalogStoreError::Internal(_));
    let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    assert_eq!(loaded.metadata_location, current_metadata);
    assert_eq!(loaded.version_token, "token-v1");
    let staged = store.get_commit_by_id(bucket, "table-id", "commit-1").await.unwrap().unwrap();
    assert_eq!(staged.status, CommitLogStatus::Staged);
}

#[tokio::test]
async fn object_table_catalog_store_recovers_staged_commit_after_post_cas_finalization_failure() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &commit_path, 2).await;

    let request = TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        commit_id: "commit-1".to_string(),
        idempotency_key: None,
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        expected_metadata_location: current_metadata,
        new_metadata_location: new_metadata.clone(),
        requirements: Vec::new(),
        writer: None,
    };

    let result = store.commit_table(request.clone()).await.unwrap();

    assert_eq!(result.table.metadata_location, new_metadata);
    assert_eq!(result.commit_log.status, CommitLogStatus::Committed);
    let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    assert_eq!(loaded.version_token, result.table.version_token);
    let staged = store.get_commit_by_id(bucket, "table-id", "commit-1").await.unwrap().unwrap();
    assert_eq!(staged.status, CommitLogStatus::Staged);

    let retry = store.commit_table(request).await.unwrap();
    assert_eq!(retry.table.version_token, result.table.version_token);
    assert_eq!(retry.commit_log.status, CommitLogStatus::Committed);
    let committed = store.get_commit_by_id(bucket, "table-id", "commit-1").await.unwrap().unwrap();
    assert_eq!(committed.status, CommitLogStatus::Committed);
}

#[test]
fn failed_commit_does_not_prove_historical_staged_commit() {
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table_name = IdentifierSegment::parse("orders").expect("table should parse");
    let mut table = test_table_entry(
        "analytics",
        &namespace,
        &table_name,
        default_table_metadata_file_path(&namespace, &table_name, "00003.metadata.json"),
    );
    table.version_token = "token-v3".to_string();
    table.generation = 3;
    let target = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "commit-1".to_string(),
        idempotency_key: None,
        table_id: table.table_id.clone(),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        new_version_token: "token-v2".to_string(),
        previous_metadata_location: default_table_metadata_file_path(&namespace, &table_name, "00001.metadata.json"),
        new_metadata_location: default_table_metadata_file_path(&namespace, &table_name, "00002.metadata.json"),
        requirements: Vec::new(),
        status: CommitLogStatus::Staged,
        writer: None,
        created_at: None,
        updated_at: None,
    };
    let failed = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "commit-2".to_string(),
        idempotency_key: None,
        table_id: table.table_id.clone(),
        operation: "append".to_string(),
        expected_version_token: "token-v2".to_string(),
        new_version_token: "token-v3".to_string(),
        previous_metadata_location: target.new_metadata_location.clone(),
        new_metadata_location: table.metadata_location.clone(),
        requirements: Vec::new(),
        status: CommitLogStatus::Failed,
        writer: None,
        created_at: None,
        updated_at: None,
    };

    let commits = [target.clone(), failed.clone()];
    assert!(!TableCommitHistoryIndex::new(&table, commits.iter()).proves_committed(&target));
    let recovery = table_commit_recovery_entry(&table, &failed, None, false);
    assert_eq!(recovery.recovery_state, TableCommitRecoveryState::ManualReview);
}

#[test]
fn committed_commit_outside_current_history_requires_manual_review() {
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table_name = IdentifierSegment::parse("orders").expect("table should parse");
    let table = test_table_entry(
        "analytics",
        &namespace,
        &table_name,
        default_table_metadata_file_path(&namespace, &table_name, "00001.metadata.json"),
    );
    let commit = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "commit-1".to_string(),
        idempotency_key: None,
        table_id: table.table_id.clone(),
        operation: "append".to_string(),
        expected_version_token: table.version_token.clone(),
        new_version_token: "token-v2".to_string(),
        previous_metadata_location: table.metadata_location.clone(),
        new_metadata_location: default_table_metadata_file_path(&namespace, &table_name, "00002.metadata.json"),
        requirements: Vec::new(),
        status: CommitLogStatus::Committed,
        writer: None,
        created_at: None,
        updated_at: None,
    };

    let history = TableCommitHistoryIndex::new(&table, [&commit]);
    assert!(!history.proves_committed(&commit));
    let recovery = table_commit_recovery_entry(&table, &commit, None, false);
    assert_eq!(recovery.recovery_state, TableCommitRecoveryState::ManualReview);
    assert!(recovery.reason.contains("not reachable"));
}

#[test]
fn commit_history_index_scales_across_a_long_table_history() {
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table_name = IdentifierSegment::parse("orders").expect("table should parse");
    let commit_count = 10_000;
    let commits = (0..commit_count)
        .map(|index| CommitLogEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            commit_id: format!("commit-{index:05}"),
            idempotency_key: None,
            table_id: "table-id".to_string(),
            operation: "append".to_string(),
            expected_version_token: format!("token-{index:05}"),
            new_version_token: format!("token-{:05}", index + 1),
            previous_metadata_location: format!("metadata/v{index:05}.metadata.json"),
            new_metadata_location: format!("metadata/v{:05}.metadata.json", index + 1),
            requirements: Vec::new(),
            status: CommitLogStatus::Staged,
            writer: None,
            created_at: None,
            updated_at: None,
        })
        .collect::<Vec<_>>();
    let mut table = test_table_entry("analytics", &namespace, &table_name, format!("metadata/v{commit_count:05}.metadata.json"));
    table.version_token = format!("token-{commit_count:05}");

    let history = TableCommitHistoryIndex::new(&table, commits.iter());

    assert!(commits.iter().all(|commit| history.proves_committed(commit)));
}

#[test]
fn commit_history_index_rejects_ambiguous_states_and_cycles() {
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table_name = IdentifierSegment::parse("orders").expect("table should parse");
    let metadata = |version| default_table_metadata_file_path(&namespace, &table_name, &format!("{version:05}.metadata.json"));
    let commit = |commit_id: &str, previous: u8, next: u8| CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: commit_id.to_string(),
        idempotency_key: None,
        table_id: "table-id".to_string(),
        operation: "append".to_string(),
        expected_version_token: format!("token-v{previous}"),
        new_version_token: format!("token-v{next}"),
        previous_metadata_location: metadata(previous),
        new_metadata_location: metadata(next),
        requirements: Vec::new(),
        status: CommitLogStatus::Committed,
        writer: None,
        created_at: None,
        updated_at: None,
    };

    let mut table = test_table_entry("analytics", &namespace, &table_name, metadata(2));
    table.version_token = "token-v2".to_string();
    let first = commit("commit-1", 1, 2);
    let duplicate = commit("commit-2", 3, 2);
    let ambiguous = TableCommitHistoryIndex::new(&table, [&first, &duplicate]);
    assert!(!ambiguous.proves_committed(&first));
    assert!(!ambiguous.proves_committed(&duplicate));

    table.metadata_location = metadata(3);
    table.version_token = "token-v3".to_string();
    let forward = commit("commit-3", 2, 3);
    let backward = commit("commit-4", 3, 2);
    let cyclic = TableCommitHistoryIndex::new(&table, [&forward, &backward]);
    assert!(!cyclic.proves_committed(&forward));
    assert!(!cyclic.proves_committed(&backward));
}

#[tokio::test]
async fn object_table_catalog_store_recovers_historical_staged_commit_after_later_commit() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let first_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let second_metadata = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
    let first_commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &first_metadata, b"{}".to_vec()).await;
    backend.seed_object(bucket, &second_metadata, b"{}".to_vec()).await;
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &first_commit_path, 2).await;

    let first_request = TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        commit_id: "commit-1".to_string(),
        idempotency_key: Some("request-1".to_string()),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        expected_metadata_location: current_metadata,
        new_metadata_location: first_metadata.clone(),
        requirements: Vec::new(),
        writer: None,
    };
    let first = store
        .commit_table(first_request.clone())
        .await
        .expect("first commit should publish despite finalization failure");
    assert_eq!(
        store
            .get_commit_by_id(bucket, "table-id", "commit-1")
            .await
            .expect("first commit lookup should succeed")
            .expect("first commit record should exist")
            .status,
        CommitLogStatus::Staged
    );

    let second = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-2".to_string(),
            idempotency_key: Some("request-2".to_string()),
            operation: "append".to_string(),
            expected_version_token: first.table.version_token,
            expected_metadata_location: first_metadata,
            new_metadata_location: second_metadata.clone(),
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .expect("second commit should succeed");

    let report = store
        .plan_table_commit_recovery(bucket, "sales", "orders")
        .await
        .expect("commit recovery plan should succeed");
    assert_eq!(report.finalization_required_count, 1);
    assert_eq!(report.manual_review_count, 0);
    assert_eq!(
        report
            .commits
            .iter()
            .find(|commit| commit.commit_id == "commit-1")
            .expect("first commit should be reported")
            .recovery_state,
        TableCommitRecoveryState::FinalizationRequired
    );

    let replay = store
        .commit_table(first_request)
        .await
        .expect("historical commit retry should finalize");
    assert_eq!(replay.table.metadata_location, second_metadata);
    assert_eq!(replay.table.version_token, second.table.version_token);
    assert_eq!(replay.commit_log.status, CommitLogStatus::Committed);
    assert_eq!(
        store
            .get_commit_by_id(bucket, "table-id", "commit-1")
            .await
            .expect("first commit lookup should succeed")
            .expect("first commit record should exist")
            .status,
        CommitLogStatus::Committed
    );
}

#[tokio::test]
async fn table_commit_recovery_reports_post_cas_staged_commit() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &commit_path, 2).await;

    let result = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata.clone(),
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap();
    assert_eq!(result.commit_log.status, CommitLogStatus::Committed);

    let report = store.plan_table_commit_recovery(bucket, "sales", "orders").await.unwrap();

    assert_eq!(report.current_metadata_location, new_metadata);
    assert_eq!(report.finalization_required_count, 1);
    assert_eq!(report.manual_review_count, 0);
    assert_eq!(report.commits.len(), 1);
    assert_eq!(report.commits[0].commit_id, "commit-1");
    assert_eq!(report.commits[0].status, CommitLogStatus::Staged);
    assert_eq!(report.commits[0].recovery_state, TableCommitRecoveryState::FinalizationRequired);
}

#[tokio::test]
async fn diagnostics_report_includes_table_commit_recovery_state() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &commit_path, 2).await;

    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap();

    let report = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();

    assert_eq!(report.recovery_status, TableCatalogRecoveryStatus::Recoverable);
    assert_eq!(report.recommended_actions, vec![TableCatalogRecoveryAction::RunCommitRecovery]);
    assert_eq!(report.commit_recovery.finalization_required_count, 1);
    assert_eq!(report.commit_recovery.commits.len(), 1);
    assert_eq!(
        report.commit_recovery.commits[0].recovery_state,
        TableCommitRecoveryState::FinalizationRequired
    );
}

#[tokio::test]
async fn table_commit_recovery_finalizes_post_cas_staged_commit() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let commit_path = TableCatalogObjectPaths::default().commit_log_entry_path(bucket, "table-id", "commit-1");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &commit_path, 2).await;

    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap();

    let report = store.recover_table_commits(bucket, "sales", "orders").await.unwrap();

    assert_eq!(report.finalized_count, 1);
    assert_eq!(report.finalization_required_count, 0);
    let committed = store.get_commit_by_id(bucket, "table-id", "commit-1").await.unwrap().unwrap();
    assert_eq!(committed.status, CommitLogStatus::Committed);
}

#[tokio::test]
async fn table_commit_recovery_reports_staged_commit_after_table_cas_failure() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let table_path = TableCatalogObjectPaths::default().table_entry_path(bucket, &namespace, &table);

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &current_metadata, b"{}".to_vec()).await;
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &table_path, 2).await;

    let err = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata.clone(),
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap_err();
    assert_matches!(err, TableCatalogStoreError::Internal(_));
    let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    assert_eq!(loaded.metadata_location, current_metadata);

    let report = store.plan_table_commit_recovery(bucket, "sales", "orders").await.unwrap();
    assert_eq!(report.staged_before_table_update_count, 1);
    assert_eq!(report.finalization_required_count, 0);
    assert_eq!(report.commits[0].recovery_state, TableCommitRecoveryState::StagedBeforeTableUpdate);

    let diagnostics = store.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();
    assert_eq!(diagnostics.current_metadata_status, TableMetadataPointerStatus::Valid);
    assert_eq!(diagnostics.recovery_status, TableCatalogRecoveryStatus::Recoverable);
    assert_eq!(diagnostics.recommended_actions, vec![TableCatalogRecoveryAction::RetryCommit]);
}

#[tokio::test]
async fn table_commit_recovery_repairs_stale_idempotency_index_after_partial_finalization() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let idempotency_path =
        TableCatalogObjectPaths::default().commit_idempotency_entry_path(bucket, "table-id", "client-request-1");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    backend.fail_put_attempt(RUSTFS_META_BUCKET, &idempotency_path, 2).await;

    let result = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: Some("client-request-1".to_string()),
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current_metadata,
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap();
    assert_eq!(result.commit_log.status, CommitLogStatus::Committed);
    let stale_index = store
        .get_commit_by_idempotency_key(bucket, "table-id", "client-request-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stale_index.status, CommitLogStatus::Staged);

    let report = store.plan_table_commit_recovery(bucket, "sales", "orders").await.unwrap();
    assert_eq!(report.idempotency_repair_required_count, 1);
    assert_eq!(report.manual_review_count, 0);
    assert_eq!(report.commits[0].recovery_state, TableCommitRecoveryState::IdempotencyIndexRepairRequired);
    assert_eq!(report.commits[0].idempotency_index_status, TableCommitIdempotencyIndexStatus::Stale);

    let repaired = store.recover_table_commits(bucket, "sales", "orders").await.unwrap();

    assert_eq!(repaired.finalized_count, 1);
    assert_eq!(repaired.idempotency_repair_required_count, 0);
    let repaired_index = store
        .get_commit_by_idempotency_key(bucket, "table-id", "client-request-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repaired_index.status, CommitLogStatus::Committed);
}

#[tokio::test]
async fn table_commit_recovery_does_not_overwrite_conflicting_idempotency_index() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let paths = TableCatalogObjectPaths::default();
    let commit_path = paths.commit_log_entry_path(bucket, "table-id", "commit-1");
    let idempotency_path = paths.commit_idempotency_entry_path(bucket, "table-id", "client-request-1");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    let request = TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        commit_id: "commit-1".to_string(),
        idempotency_key: Some("client-request-1".to_string()),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        expected_metadata_location: current_metadata,
        new_metadata_location: new_metadata,
        requirements: Vec::new(),
        writer: None,
    };
    let committed = store.commit_table(request.clone()).await.expect("commit should succeed");
    let mut conflicting_index = committed.commit_log;
    conflicting_index.new_version_token = "different-token".to_string();
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &idempotency_path,
            serde_json::to_vec(&conflicting_index).expect("conflicting index should encode"),
        )
        .await;
    let commit_before = backend
        .read_object(RUSTFS_META_BUCKET, &commit_path)
        .await
        .expect("commit record lookup should succeed")
        .expect("commit record should exist")
        .data;
    let index_before = backend
        .read_object(RUSTFS_META_BUCKET, &idempotency_path)
        .await
        .expect("idempotency index lookup should succeed")
        .expect("idempotency index should exist")
        .data;

    let retry_error = store
        .commit_table(request)
        .await
        .expect_err("conflicting index must fail closed");
    assert_matches!(retry_error, TableCatalogStoreError::Conflict(_));
    let planned = store
        .plan_table_commit_recovery(bucket, "sales", "orders")
        .await
        .expect("commit recovery plan should succeed");
    assert_eq!(planned.manual_review_count, 1);
    assert_eq!(planned.commits[0].recovery_state, TableCommitRecoveryState::ManualReview);
    assert_eq!(
        planned.commits[0].idempotency_index_status,
        TableCommitIdempotencyIndexStatus::Conflicting
    );

    let recovered = store
        .recover_table_commits(bucket, "sales", "orders")
        .await
        .expect("commit recovery should succeed");
    assert_eq!(recovered.manual_review_count, 1);
    assert_eq!(
        backend
            .read_object(RUSTFS_META_BUCKET, &commit_path)
            .await
            .expect("commit record lookup should succeed")
            .expect("commit record should remain")
            .data,
        commit_before
    );
    assert_eq!(
        backend
            .read_object(RUSTFS_META_BUCKET, &idempotency_path)
            .await
            .expect("idempotency index lookup should succeed")
            .expect("conflicting index should remain")
            .data,
        index_before
    );
}

#[tokio::test]
async fn strong_table_commit_retry_rejects_token_only_idempotency_corruption() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .expect("table should be created");
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;
    let request = TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        commit_id: "commit-1".to_string(),
        idempotency_key: Some("client-request-1".to_string()),
        operation: "append".to_string(),
        expected_version_token: "token-v1".to_string(),
        expected_metadata_location: current_metadata,
        new_metadata_location: new_metadata,
        requirements: Vec::new(),
        writer: None,
    };
    store
        .commit_table(request.clone())
        .await
        .expect("initial strong catalog commit should succeed");

    let snapshot_path = StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path();
    let snapshot_object = backend
        .read_object(RUSTFS_META_BUCKET, &snapshot_path)
        .await
        .expect("strong snapshot lookup should succeed")
        .expect("strong snapshot should exist");
    let mut snapshot = serde_json::from_slice::<serde_json::Value>(&snapshot_object.data).expect("strong snapshot should decode");
    assert_eq!(snapshot["commits"].as_array().map(Vec::len), Some(1));
    assert_eq!(snapshot["idempotency"].as_array().map(Vec::len), Some(1));
    snapshot["idempotency"][0]["commit"]["new_version_token"] = serde_json::json!("different-token");
    let conflicting_snapshot = serde_json::to_vec(&snapshot).expect("conflicting strong snapshot should encode");
    backend
        .seed_object(RUSTFS_META_BUCKET, &snapshot_path, conflicting_snapshot.clone())
        .await;

    let retry_error = store
        .commit_table(request)
        .await
        .expect_err("token-only idempotency mismatch must fail closed in strong mode");
    assert_matches!(
        retry_error,
        TableCatalogStoreError::Invalid(message) if message.contains("has no matching commit")
    );
    assert_eq!(
        backend
            .read_object(RUSTFS_META_BUCKET, &snapshot_path)
            .await
            .expect("strong snapshot lookup should succeed")
            .expect("strong snapshot should remain")
            .data,
        conflicting_snapshot
    );
}

#[tokio::test]
async fn object_table_catalog_store_rejects_stale_commit_token() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, current_metadata.clone()))
        .await
        .unwrap();
    backend.seed_object(bucket, &new_metadata, b"{}".to_vec()).await;

    let err = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "commit-1".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "stale-token".to_string(),
            expected_metadata_location: current_metadata.clone(),
            new_metadata_location: new_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap_err();

    assert_matches!(err, TableCatalogStoreError::Conflict(_));
    let loaded = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    assert_eq!(loaded.metadata_location, current_metadata);
    assert_eq!(loaded.version_token, "token-v1");
    assert!(
        store
            .get_commit_by_id(bucket, "table-id", "commit-1")
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn namespace_marker_path_stays_under_default_reserved_boundary() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();

    assert_eq!(
        default_namespace_marker_path(&namespace),
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/namespace.json"
    );
    assert_eq!(namespace.public_name(), "analytics.daily_events");
}

#[test]
fn namespace_marker_path_extracts_public_name() {
    assert_eq!(
        namespace_name_from_marker_path(".rustfs-table/warehouses/default/namespaces/analytics/daily_events/namespace.json"),
        Some("analytics.daily_events".to_string())
    );
    assert_eq!(
        namespace_name_from_marker_path(
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
        ),
        None
    );
    assert_eq!(
        namespace_name_from_marker_path(".rustfs-table/warehouses/other/namespaces/analytics/daily_events/namespace.json"),
        None
    );
}

#[test]
fn namespace_marker_json_uses_stable_catalog_defaults() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();
    let marker = serde_json::to_value(NamespaceMarker::new(&namespace)).unwrap();

    assert_eq!(marker["version"], TABLE_NAMESPACE_MARKER_VERSION);
    assert_eq!(marker["namespace"], "analytics.daily_events");
    assert!(!namespace_marker_json(&namespace).unwrap().is_empty());
}

#[test]
fn table_marker_path_stays_under_namespace_reserved_boundary() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();
    let table = IdentifierSegment::parse("events").unwrap();

    assert_eq!(
        default_table_root_prefix(&namespace),
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/"
    );
    assert_eq!(
        default_table_marker_path(&namespace, &table),
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/table.json"
    );
}

#[test]
fn table_marker_path_extracts_table_name() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();

    assert_eq!(
        table_name_from_marker_path(
            &namespace,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/table.json"
        ),
        Some("events".to_string())
    );
    assert_eq!(
        table_name_from_marker_path(
            &namespace,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/current.json"
        ),
        None
    );
    assert_eq!(
        table_name_from_marker_path(
            &namespace,
            ".rustfs-table/warehouses/default/namespaces/analytics/other/tables/events/table.json"
        ),
        None
    );
}

#[test]
fn table_marker_json_uses_stable_catalog_defaults() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();
    let table = IdentifierSegment::parse("events").unwrap();
    let marker = serde_json::to_value(TableMarker::new(&namespace, &table)).unwrap();

    assert_eq!(marker["version"], TABLE_RESOURCE_MARKER_VERSION);
    assert_eq!(marker["namespace"], "analytics.daily_events");
    assert_eq!(marker["name"], "events");
    assert!(marker["metadata_location"].is_null());
    assert!(!table_marker_json(&namespace, &table).unwrap().is_empty());
}

#[test]
fn table_current_pointer_path_stays_under_table_boundary() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();
    let table = IdentifierSegment::parse("events").unwrap();

    assert_eq!(
        default_table_metadata_dir_path(&namespace, &table),
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata"
    );
    assert_eq!(
        default_table_current_pointer_path(&namespace, &table),
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
    );
    assert_eq!(
        default_table_lifecycle_path(&namespace, &table),
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/lifecycle.json"
    );
}

#[test]
fn table_metadata_file_path_stays_under_metadata_boundary() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();
    let table = IdentifierSegment::parse("events").unwrap();
    let table_identifier =
        TableIdentifier::new(IdentifierSegment::parse(DEFAULT_WAREHOUSE_ID).unwrap(), namespace.clone(), table.clone());

    assert_eq!(
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
    );
    assert_eq!(
        TablePathResolver::default().metadata_file_path(&table_identifier, "00001.metadata.json"),
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
    );
}

#[test]
fn table_metadata_file_name_validation_rejects_unsafe_names() {
    assert!(is_valid_table_metadata_file_name("00001.metadata.json"));
    assert!(is_valid_table_metadata_file_name("00001.gz.metadata.json"));
    assert!(is_valid_table_metadata_file_name("00001.metadata.json.gz"));
    assert!(is_valid_table_metadata_file_name("v1-4f2c_metadata.json"));

    assert!(!is_valid_table_metadata_file_name(""));
    assert!(!is_valid_table_metadata_file_name(".metadata.json"));
    assert!(!is_valid_table_metadata_file_name("00001.metadata"));
    assert!(!is_valid_table_metadata_file_name("00001.JSON"));
    assert!(!is_valid_table_metadata_file_name("../current.json"));
    assert!(!is_valid_table_metadata_file_name("nested/00001.json"));
    assert!(!is_valid_table_metadata_file_name("nested%2f00001.json"));
    assert!(!is_valid_table_metadata_file_name("00001\\metadata.json"));
    assert!(!is_valid_table_metadata_file_name("00001\nmetadata.json"));
}

#[test]
fn warehouse_prefix_overlap_distinguishes_nested_and_sibling_tables() {
    assert!(warehouse_object_prefixes_overlap("tables/table-id/", "tables/table-id/child/"));
    assert!(warehouse_object_prefixes_overlap("tables/table-id/child/", "tables/table-id/"));
    assert!(!warehouse_object_prefixes_overlap("tables/table-id/", "tables/table-id-other/"));
}

#[test]
fn table_metadata_location_validation_stays_inside_metadata_dir() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();
    let table = IdentifierSegment::parse("events").unwrap();

    assert!(is_valid_table_metadata_location(
        &namespace,
        &table,
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
    ));
    assert!(!is_valid_table_metadata_location(&namespace, &table, ""));
    assert!(!is_valid_table_metadata_location(
        &namespace,
        &table,
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
    ));
    assert!(!is_valid_table_metadata_location(
        &namespace,
        &table,
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/other/metadata/00001.json"
    ));
    assert!(!is_valid_table_metadata_location(
        &namespace,
        &table,
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/../current.json"
    ));
    assert!(!is_valid_table_metadata_location(
        &namespace,
        &table,
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/nested/00001.json"
    ));
}

#[test]
fn metadata_location_from_metadata_file_path_extracts_table_metadata_only() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();
    let table = IdentifierSegment::parse("events").unwrap();

    assert_eq!(
        metadata_location_from_metadata_file_path(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
        ),
        Some(
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
                .to_string()
        )
    );
    assert_eq!(
        metadata_location_from_metadata_file_path(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/current.json"
        ),
        None
    );
    assert_eq!(
        metadata_location_from_metadata_file_path(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/nested/00001.metadata.json"
        ),
        None
    );
    assert_eq!(
        metadata_location_from_metadata_file_path(
            &namespace,
            &table,
            ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/other/metadata/00001.metadata.json"
        ),
        None
    );
}

#[test]
fn table_metadata_pointer_json_round_trips() {
    let location =
        ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.json".to_string();
    let data = table_metadata_pointer_json(location.clone()).unwrap();
    let pointer = parse_table_metadata_pointer(&data).unwrap();

    assert_eq!(pointer.version, TABLE_METADATA_POINTER_VERSION);
    assert_eq!(pointer.metadata_location, location);
}

#[test]
fn object_mutation_entrypoints_call_reserved_prefix_guard() {
    let source = include_str!("../app/object_usecase.rs");
    let delete_object = source
        .split_once("pub async fn execute_delete_object")
        .and_then(|(_, remainder)| remainder.split_once("pub async fn execute_head_object"))
        .map(|(delete_object, _)| delete_object)
        .expect("delete object entrypoint should remain in the object usecase");

    for expected in [
        "validate_object_key(&key, request_method_name)?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
        "validate_object_key(&key, \"COPY (dest)\")?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
        "if let Err(err) = validate_table_catalog_object_mutation(&bucket, &obj_id.key).await",
        "validate_table_catalog_object_mutation(&bucket, &object).await?;",
        "validate_object_key(&key, \"PUT\")?;\n        validate_table_catalog_object_mutation(&bucket, &key).await?;",
        "validate_table_catalog_object_mutation(&bucket, &fpath).await?;",
    ] {
        assert!(source.contains(expected), "missing object mutation guard: {expected}");
    }
    assert!(
        delete_object.contains("validate_object_key(&key, \"DELETE\")?;")
            && delete_object.contains("validate_table_catalog_object_mutation(&bucket, &key).await?;"),
        "delete object entrypoint must validate the object key and reserved catalog prefix"
    );
}

#[test]
fn multipart_mutation_entrypoints_call_reserved_prefix_guard() {
    let source = include_str!("../app/multipart_usecase.rs");

    assert_eq!(
        source
            .matches("validate_table_catalog_object_mutation(&bucket, &key).await?;")
            .count(),
        4
    );
}

#[test]
fn object_metadata_mutation_entrypoints_call_reserved_prefix_guard() {
    let source = include_str!("../storage/ecfs.rs");

    assert_eq!(
        source
            .matches("validate_table_catalog_object_mutation(&bucket, &object).await?;")
            .count(),
        2
    );
    assert_eq!(
        source
            .matches("validate_table_catalog_object_mutation(&bucket, &key).await?;")
            .count(),
        2
    );
}

#[test]
fn identifier_segment_accepts_conservative_catalog_names() {
    for value in [
        "a",
        "a1",
        "a-b",
        "a_b",
        "abc123",
        "a23456789012345678901234567890123456789012345678901234567890123",
    ] {
        assert_eq!(IdentifierSegment::parse(value).unwrap().as_str(), value);
    }
}

#[test]
fn identifier_segment_rejects_ambiguous_or_unsafe_names() {
    for value in [
        "",
        ".",
        "..",
        "Upper",
        "has.dot",
        "has/slash",
        "has\\slash",
        "has%2fslash",
        "-leading",
        "trailing-",
        "_leading",
        "trailing_",
        "has space",
        "name\nbreak",
    ] {
        assert!(IdentifierSegment::parse(value).is_err(), "value should be rejected: {value:?}");
    }

    let too_long = "a".repeat(IdentifierSegment::MAX_LEN + 1);
    assert!(IdentifierSegment::parse(too_long).is_err());
}

#[test]
fn namespace_uses_dot_syntax_for_public_identity_and_slash_for_storage() {
    let namespace = Namespace::parse("analytics.daily_events").unwrap();

    assert_eq!(namespace.segments().len(), 2);
    assert_eq!(namespace.storage_id(), "analytics/daily_events");
}

#[test]
fn namespace_length_is_bounded_for_catalog_paths_and_page_tokens() {
    let mut segments = vec!["a".repeat(63); 8];
    segments[0].push('a');
    let max_length_namespace = segments.join(".");
    assert_eq!(max_length_namespace.len(), Namespace::MAX_LEN);
    Namespace::parse(&max_length_namespace).expect("namespace at the maximum length should parse");

    let namespace = format!("{max_length_namespace}.a");
    assert_eq!(
        Namespace::parse(&namespace),
        Err(CatalogIdentifierError::NamespaceTooLong { max: Namespace::MAX_LEN })
    );
}

#[test]
fn namespace_from_segments_preserves_rest_boundaries_and_length_limit() {
    let namespace = Namespace::from_segments(vec!["analytics".to_string(), "daily_events".to_string()])
        .expect("multipart namespace should parse");
    assert_eq!(namespace.public_name(), "analytics.daily_events");
    assert_eq!(synthetic_namespace_entry("warehouse", &namespace).namespace_id, "analytics/daily_events");
    assert!(Namespace::from_segments(vec!["analytics.daily_events".to_string()]).is_err());

    let mut segments = vec!["a".repeat(63); 8];
    segments[0].push('a');
    Namespace::from_segments(segments.clone()).expect("namespace at the maximum length should parse");
    segments.push("a".to_string());
    assert_eq!(
        Namespace::from_segments(segments),
        Err(CatalogIdentifierError::NamespaceTooLong { max: Namespace::MAX_LEN })
    );
}

#[test]
fn namespace_property_update_and_limits_reject_ambiguous_or_oversized_state() {
    let overlap = NamespacePropertiesUpdate::try_new(
        vec!["owner".to_string()],
        BTreeMap::from([("owner".to_string(), "platform".to_string())]),
    )
    .expect_err("overlapping update should fail");
    assert_matches!(overlap, NamespacePropertiesUpdateError::Overlap(key) if key == "owner");

    let duplicate = NamespacePropertiesUpdate::try_new(vec!["owner".to_string(), "owner".to_string()], BTreeMap::new())
        .expect_err("duplicate removal should fail");
    assert_matches!(duplicate, NamespacePropertiesUpdateError::DuplicateRemoval(key) if key == "owner");

    let mut exact_total = BTreeMap::new();
    for index in 0..15 {
        exact_total.insert(format!("k{index:02}"), "v".repeat(NAMESPACE_PROPERTY_VALUE_MAX_LEN));
    }
    let used = exact_total.iter().map(|(key, value)| key.len() + value.len()).sum::<usize>();
    let final_key = "k15".to_string();
    exact_total.insert(
        final_key.clone(),
        "v".repeat(NAMESPACE_PROPERTIES_MAX_TOTAL_BYTES - used - final_key.len()),
    );
    assert!(validate_namespace_properties(&exact_total).is_ok());

    exact_total
        .get_mut(&final_key)
        .expect("final property should exist")
        .push('v');
    assert_matches!(validate_namespace_properties(&exact_total), Err(TableCatalogStoreError::Invalid(_)));
    assert_matches!(
        validate_namespace_properties(&BTreeMap::from([(String::new(), "value".to_string())])),
        Err(TableCatalogStoreError::Invalid(_))
    );
    assert_matches!(
        validate_namespace_properties(&BTreeMap::from([("k".repeat(NAMESPACE_PROPERTY_KEY_MAX_LEN + 1), "value".to_string(),)])),
        Err(TableCatalogStoreError::Invalid(_))
    );
    assert_matches!(
        validate_namespace_properties(&BTreeMap::from([
            ("owner".to_string(), "v".repeat(NAMESPACE_PROPERTY_VALUE_MAX_LEN + 1),)
        ])),
        Err(TableCatalogStoreError::Invalid(_))
    );
    let too_many = (0..=NAMESPACE_PROPERTIES_MAX_ENTRIES)
        .map(|index| (format!("key{index}"), "value".to_string()))
        .collect();
    assert_matches!(validate_namespace_properties(&too_many), Err(TableCatalogStoreError::Invalid(_)));
}

#[tokio::test]
async fn configured_object_catalog_rejects_namespace_property_update_without_mutation() {
    let backend = TestCatalogObjectBackend::default();
    let store = ConfiguredTableCatalogStore::new_for_test(backend, TableCatalogBackingMode::ObjectBacked);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let mut entry = test_namespace_entry(bucket, &namespace);
    entry.properties = BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]);
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket entry should be seeded");
    store.create_namespace(entry).await.expect("namespace should be created");

    let result = store
        .update_namespace_properties(
            bucket,
            "sales",
            NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "platform".to_string())]))
                .expect("namespace update should validate"),
        )
        .await
        .expect_err("object-backed namespace property update should be unsupported");
    assert_matches!(result, TableCatalogStoreError::Unsupported(_));
    let stored = store
        .get_namespace(bucket, "sales")
        .await
        .expect("namespace lookup should succeed")
        .expect("namespace should remain");
    assert_eq!(stored.properties.get("owner").map(String::as_str), Some("lakehouse"));
}

#[tokio::test]
async fn strong_catalog_namespace_property_update_survives_restart_and_failed_write() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket entry should be seeded");
    let mut entry = test_namespace_entry(bucket, &namespace);
    entry.properties.insert("owner".to_string(), "lakehouse".to_string());
    store.create_namespace(entry).await.expect("namespace should be created");

    let result = store
        .update_namespace_properties(
            bucket,
            "sales",
            NamespacePropertiesUpdate::try_new(
                vec!["missing".to_string()],
                BTreeMap::from([("owner".to_string(), "platform".to_string())]),
            )
            .expect("namespace update should validate"),
        )
        .await
        .expect("namespace properties should update");
    assert_eq!(result.updated, vec!["owner".to_string()]);
    assert_eq!(result.missing, vec!["missing".to_string()]);

    let restarted = StrongTableCatalogStore::new(backend.clone());
    let stored = restarted
        .get_namespace(bucket, "sales")
        .await
        .expect("namespace lookup after restart should succeed")
        .expect("namespace should survive restart");
    assert_eq!(stored.properties.get("owner").map(String::as_str), Some("platform"));

    backend
        .fail_next_put(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
        )
        .await;
    let no_op = restarted
        .update_namespace_properties(
            bucket,
            "sales",
            NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "platform".to_string())]))
                .expect("namespace update should validate"),
        )
        .await
        .expect("unchanged namespace properties should not write a snapshot");
    assert_eq!(no_op.updated, vec!["owner".to_string()]);
    let error = restarted
        .update_namespace_properties(
            bucket,
            "sales",
            NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "failed-update".to_string())]))
                .expect("namespace update should validate"),
        )
        .await
        .expect_err("failed snapshot write should fail namespace update");
    assert_matches!(error, TableCatalogStoreError::Internal(_));

    let after_failure = StrongTableCatalogStore::new(backend)
        .get_namespace(bucket, "sales")
        .await
        .expect("namespace lookup after failed write should succeed")
        .expect("namespace should remain");
    assert_eq!(after_failure.properties.get("owner").map(String::as_str), Some("platform"));
}

#[tokio::test]
async fn namespace_properties_load_legacy_values_but_reject_oversized_writes() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let strong_store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    object_store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("object-backed bucket should be created");
    strong_store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("strong bucket should be created");

    let mut oversized = test_namespace_entry(bucket, &namespace);
    oversized
        .properties
        .insert("owner".to_string(), "x".repeat(NAMESPACE_PROPERTY_VALUE_MAX_LEN + 1));
    assert_matches!(
        object_store.create_namespace(oversized.clone()).await,
        Err(TableCatalogStoreError::Invalid(_))
    );
    assert_matches!(
        strong_store.create_namespace(oversized.clone()).await,
        Err(TableCatalogStoreError::Invalid(_))
    );

    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &object_store.paths.namespace_entry_path(bucket, &namespace),
            serde_json::to_vec(&oversized).expect("legacy namespace should serialize"),
        )
        .await;
    let legacy = object_store
        .get_namespace(bucket, "sales")
        .await
        .expect("legacy namespace lookup should succeed")
        .expect("legacy namespace should remain readable");
    assert_eq!(
        legacy.properties.get("owner").map(String::len),
        Some(NAMESPACE_PROPERTY_VALUE_MAX_LEN + 1)
    );

    strong_store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    assert_matches!(
        strong_store
            .update_namespace_properties(
                bucket,
                "sales",
                NamespacePropertiesUpdate::try_new(Vec::new(), oversized.properties)
                    .expect("property update request should validate structurally"),
            )
            .await,
        Err(TableCatalogStoreError::Invalid(_))
    );
    let stored = strong_store
        .get_namespace(bucket, "sales")
        .await
        .expect("namespace lookup should succeed")
        .expect("namespace should remain");
    assert!(stored.properties.is_empty());
}

#[tokio::test]
async fn object_catalog_rejects_mismatched_namespace_storage_identity() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales.daily").expect("namespace should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("object-backed bucket should be created");
    let mut entry = test_namespace_entry(bucket, &namespace);
    entry.namespace_id = "sales.daily".to_string();
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &store.paths.namespace_entry_path(bucket, &namespace),
            serde_json::to_vec(&entry).expect("namespace should serialize"),
        )
        .await;

    assert_matches!(store.get_namespace(bucket, "sales.daily").await, Err(TableCatalogStoreError::Invalid(_)));
}

#[tokio::test]
async fn strong_catalog_rejects_mismatched_namespace_storage_identity() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales.daily").expect("namespace should parse");
    let mut entry = test_namespace_entry(bucket, &namespace);
    entry.namespace_id = "sales.daily".to_string();
    let snapshot = StrongTableCatalogSnapshot {
        version: STRONG_TABLE_CATALOG_SNAPSHOT_VERSION,
        table_buckets: vec![test_bucket_entry(bucket)],
        namespaces: vec![entry],
        tables: Vec::new(),
        views: Vec::new(),
        commits: Vec::new(),
        idempotency: Vec::new(),
    };
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
            serde_json::to_vec(&snapshot).expect("strong snapshot should encode"),
        )
        .await;

    assert_matches!(store.get_namespace(bucket, "sales.daily").await, Err(TableCatalogStoreError::Invalid(_)));
}

#[tokio::test]
async fn catalog_backings_expose_implicit_parents_and_protect_child_namespaces() {
    let bucket = "analytics";
    let parent = Namespace::parse("sales").expect("parent namespace should parse");
    let child = Namespace::parse("sales.daily").expect("child namespace should parse");

    let object_backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(object_backend.clone());
    object_store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("object-backed bucket should be created");
    object_store
        .create_namespace(test_namespace_entry(bucket, &child))
        .await
        .expect("object-backed child namespace should be created");
    let implicit_parent = object_store
        .get_namespace(bucket, &parent.public_name())
        .await
        .expect("object-backed parent lookup should succeed")
        .expect("implicit object-backed parent should exist");
    assert_eq!(implicit_parent.namespace, parent.public_name());
    assert!(implicit_parent.properties.is_empty());
    assert_matches!(
        object_store.drop_namespace(bucket, &parent.public_name()).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    assert_matches!(
        object_store.create_namespace(test_namespace_entry(bucket, &parent)).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    let mut inactive_parent = test_namespace_entry(bucket, &parent);
    inactive_parent.state = TableCatalogEntryState::Deleted;
    object_backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &object_store.paths.namespace_entry_path(bucket, &parent),
            serde_json::to_vec(&inactive_parent).expect("inactive parent should serialize"),
        )
        .await;
    object_store
        .create_namespace(test_namespace_entry(bucket, &parent))
        .await
        .expect("inactive object-backed parent should be replaceable");
    assert_eq!(
        object_store
            .list_namespaces_under(bucket, &parent.public_name())
            .await
            .expect("object-backed descendants should list")
            .len(),
        2
    );

    let strong_backend = TestCatalogObjectBackend::default();
    let strong_store = StrongTableCatalogStore::new(strong_backend.clone());
    strong_store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("strong bucket should be created");
    strong_store
        .create_namespace(test_namespace_entry(bucket, &child))
        .await
        .expect("strong child namespace should be created");
    let implicit_parent = strong_store
        .get_namespace(bucket, &parent.public_name())
        .await
        .expect("strong parent lookup should succeed")
        .expect("implicit strong parent should exist");
    assert_eq!(implicit_parent.namespace, parent.public_name());
    assert_matches!(
        strong_store.create_namespace(test_namespace_entry(bucket, &parent)).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    strong_store
        .update_namespace_properties(
            bucket,
            &parent.public_name(),
            NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "platform".to_string())]))
                .expect("namespace update should validate"),
        )
        .await
        .expect("implicit strong parent should materialize on property update");
    let restarted = StrongTableCatalogStore::new(strong_backend);
    let materialized_parent = restarted
        .get_namespace(bucket, &parent.public_name())
        .await
        .expect("materialized parent lookup should succeed")
        .expect("materialized parent should exist");
    assert_eq!(materialized_parent.properties.get("owner").map(String::as_str), Some("platform"));
    assert_matches!(
        restarted.drop_namespace(bucket, &parent.public_name()).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
}

#[tokio::test]
async fn object_catalog_namespace_replacement_is_fenced_by_observed_etag() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");

    let recreated = Namespace::parse("recreated").expect("namespace should parse");
    let recreated_path = store.paths.namespace_entry_path(bucket, &recreated);
    let mut inactive = test_namespace_entry(bucket, &recreated);
    inactive.state = TableCatalogEntryState::Deleted;
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &recreated_path,
            serde_json::to_vec(&inactive).expect("inactive namespace should serialize"),
        )
        .await;
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &recreated_path).await;
    let stale_store = store.clone();
    let stale_entry = test_namespace_entry(bucket, &recreated);
    let stale_recreate = tokio::spawn(async move { stale_store.create_namespace(stale_entry).await });
    pause.wait_started().await;

    let mut winner = test_namespace_entry(bucket, &recreated);
    winner.properties.insert("owner".to_string(), "winner".to_string());
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &recreated_path,
            serde_json::to_vec(&winner).expect("winning namespace should serialize"),
        )
        .await;
    pause.release();
    assert_matches!(
        stale_recreate.await.expect("stale namespace recreation task should finish"),
        Err(TableCatalogStoreError::Conflict(_))
    );
    let stored = store
        .get_namespace(bucket, &recreated.public_name())
        .await
        .expect("winning namespace should load")
        .expect("winning namespace should remain");
    assert_eq!(stored.properties.get("owner").map(String::as_str), Some("winner"));
}

async fn assert_direct_namespace_child_contract<S>(store: &S, bucket: &str, cursor_prefix: &str)
where
    S: TableCatalogStore + ?Sized,
{
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    for name in ["alpha.deep.leaf", "alpha-beta", "beta", "sales.daily"] {
        let namespace = Namespace::parse(name).expect("namespace should parse");
        store
            .create_namespace(test_namespace_entry(bucket, &namespace))
            .await
            .expect("namespace should be created");
    }

    let one = NonZeroUsize::new(1).expect("page size should be non-zero");
    let first = store
        .list_namespace_children_page(bucket, None, None, one)
        .await
        .expect("first root child page should load");
    assert_eq!(
        first.entries.iter().map(|entry| entry.namespace.as_str()).collect::<Vec<_>>(),
        ["alpha-beta"]
    );
    let cursor = first.next_cursor.expect("first root child page should continue");
    assert!(cursor.starts_with(cursor_prefix));
    let second = store
        .list_namespace_children_page(bucket, None, Some(&cursor), one)
        .await
        .expect("second root child page should load");
    assert_eq!(
        second
            .entries
            .iter()
            .map(|entry| entry.namespace.as_str())
            .collect::<Vec<_>>(),
        ["alpha"]
    );

    let exact = store
        .list_namespace_children_page(bucket, None, None, NonZeroUsize::new(4).expect("page size should be non-zero"))
        .await
        .expect("exact root child page should load");
    assert_eq!(
        exact.entries.iter().map(|entry| entry.namespace.as_str()).collect::<Vec<_>>(),
        ["alpha-beta", "alpha", "beta", "sales"]
    );
    assert!(exact.next_cursor.is_none());

    let truncated = store
        .list_namespace_children_page(bucket, None, None, NonZeroUsize::new(3).expect("page size should be non-zero"))
        .await
        .expect("truncated root child page should load");
    assert_eq!(truncated.entries.len(), 3);
    let final_page = store
        .list_namespace_children_page(bucket, None, truncated.next_cursor.as_deref(), one)
        .await
        .expect("final root child page should load");
    assert_eq!(
        final_page
            .entries
            .iter()
            .map(|entry| entry.namespace.as_str())
            .collect::<Vec<_>>(),
        ["sales"]
    );
    assert!(final_page.next_cursor.is_none());

    let children = store
        .list_namespace_children(bucket, Some("alpha"))
        .await
        .expect("implicit parent children should list");
    assert_eq!(children.iter().map(|entry| entry.namespace.as_str()).collect::<Vec<_>>(), ["alpha.deep"]);
    let exact_child = store
        .list_namespace_children_page(bucket, Some("alpha"), None, one)
        .await
        .expect("exact parent child page should load");
    assert_eq!(exact_child.entries.len(), 1);
    assert!(exact_child.next_cursor.is_none());
    assert_matches!(
        store
            .list_namespace_children_page(bucket, Some("alpha"), Some(&cursor), one)
            .await,
        Err(TableCatalogStoreError::Invalid(_))
    );
    assert_matches!(
        store.list_namespace_children(bucket, Some("missing")).await,
        Err(TableCatalogStoreError::NotFound(_))
    );
}

#[tokio::test]
async fn catalog_backings_page_direct_namespace_children_with_scoped_cursors() {
    assert_direct_namespace_child_contract(
        &ObjectTableCatalogStore::new(TestCatalogObjectBackend::default()),
        "object-catalog",
        OBJECT_CATALOG_LIST_CURSOR_PREFIX,
    )
    .await;
    assert_direct_namespace_child_contract(
        &StrongTableCatalogStore::new(TestCatalogObjectBackend::default()),
        "strong-catalog",
        STRONG_CATALOG_LIST_CURSOR_PREFIX,
    )
    .await;
}

#[tokio::test]
async fn object_catalog_direct_child_page_skips_the_rest_of_a_visible_subtree() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");

    for index in 0..=TABLE_CATALOG_LIST_MAX_KEYS {
        let namespace = Namespace::parse(&format!("alpha.n{index:04}.leaf")).expect("namespace should parse");
        let path = store.paths.namespace_entry_path(bucket, &namespace);
        backend
            .seed_object(
                RUSTFS_META_BUCKET,
                &path,
                serde_json::to_vec(&test_namespace_entry(bucket, &namespace)).expect("namespace should serialize"),
            )
            .await;
    }
    let beta = Namespace::parse("beta").expect("namespace should parse");
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &store.paths.namespace_entry_path(bucket, &beta),
            serde_json::to_vec(&test_namespace_entry(bucket, &beta)).expect("namespace should serialize"),
        )
        .await;
    backend.reset_call_counts().await;

    let page = store
        .list_namespace_children_page(bucket, None, None, NonZeroUsize::new(1).expect("page size should be non-zero"))
        .await
        .expect("direct child page should load");
    assert_eq!(page.entries.iter().map(|entry| entry.namespace.as_str()).collect::<Vec<_>>(), ["alpha"]);
    assert!(page.next_cursor.is_some());
    assert_eq!(backend.read_call_count().await, 2);
    assert_eq!(backend.list_call_count().await, 2);
}

async fn assert_implicit_parent_resources<S>(store: &S, bucket: &str)
where
    S: TableCatalogStore + ?Sized,
{
    let table_parent = Namespace::parse("sales").expect("table parent should parse");
    let view_parent = Namespace::parse("reports").expect("view parent should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let namespaces = store.list_namespaces(bucket).await.expect("explicit namespaces should list");
    assert!(!namespaces.iter().any(|entry| entry.namespace == table_parent.public_name()));
    assert!(!namespaces.iter().any(|entry| entry.namespace == view_parent.public_name()));
    assert!(
        store
            .get_namespace(bucket, &table_parent.public_name())
            .await
            .expect("implicit table parent should load")
            .is_some()
    );
    assert!(
        store
            .get_namespace(bucket, &view_parent.public_name())
            .await
            .expect("implicit view parent should load")
            .is_some()
    );
    assert!(
        store
            .load_table(bucket, &table_parent.public_name(), table.as_str())
            .await
            .expect("implicit parent table should load")
            .is_some()
    );
    assert!(
        store
            .load_view(bucket, &view_parent.public_name(), view.as_str())
            .await
            .expect("implicit parent view should load")
            .is_some()
    );
    assert_matches!(
        store.create_namespace(test_namespace_entry(bucket, &table_parent)).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    assert_matches!(
        store.create_namespace(test_namespace_entry(bucket, &view_parent)).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    let tables = store
        .list_all_tables(bucket)
        .await
        .expect("table-backed namespaces should not hide tables");
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].namespace, table_parent.public_name());
    assert_eq!(tables[0].table, table.as_str());
}

async fn create_resources_in_implicit_parents<S>(store: &S, bucket: &str)
where
    S: TableCatalogStore + ?Sized,
{
    let table_parent = Namespace::parse("sales").expect("table parent should parse");
    let table_child = Namespace::parse("sales.daily").expect("table child should parse");
    let view_parent = Namespace::parse("reports").expect("view parent should parse");
    let view_child = Namespace::parse("reports.daily").expect("view child should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &table_child))
        .await
        .expect("table child namespace should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &view_child))
        .await
        .expect("view child namespace should be created");
    store
        .create_table(test_table_entry(
            bucket,
            &table_parent,
            &table,
            default_table_metadata_file_path(&table_parent, &table, "00001.metadata.json"),
        ))
        .await
        .expect("table creation should accept its implicit parent");
    store
        .create_view(test_view_entry(
            bucket,
            &view_parent,
            &view,
            default_view_metadata_file_path(&view_parent, &view, "00001.view.json"),
        ))
        .await
        .expect("view creation should accept its implicit parent");
    assert_implicit_parent_resources(store, bucket).await;

    store
        .drop_namespace(bucket, &table_child.public_name())
        .await
        .expect("table child namespace should be removable");
    store
        .drop_namespace(bucket, &view_child.public_name())
        .await
        .expect("view child namespace should be removable");
    assert_implicit_parent_resources(store, bucket).await;
    let roots = store
        .list_namespace_children(bucket, None)
        .await
        .expect("resource-backed root namespaces should list");
    assert_eq!(
        roots.iter().map(|entry| entry.namespace.as_str()).collect::<Vec<_>>(),
        ["reports", "sales"]
    );
    assert_matches!(
        store.drop_namespace(bucket, &table_parent.public_name()).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
}

async fn assert_inactive_namespace_rejects_resource_creation<S>(store: &S, bucket: &str)
where
    S: TableCatalogStore + ?Sized,
{
    let namespace = Namespace::parse("inactive").expect("inactive namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let mut entry = test_namespace_entry(bucket, &namespace);
    entry.state = TableCatalogEntryState::Deleted;

    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(entry)
        .await
        .expect("inactive namespace marker should be seeded");
    let table_error = store
        .create_table(test_table_entry(
            bucket,
            &namespace,
            &table,
            default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
        ))
        .await
        .expect_err("inactive namespace must reject table creation");
    assert_matches!(table_error, TableCatalogStoreError::NotFound(_));
    let view_error = store
        .create_view(test_view_entry(
            bucket,
            &namespace,
            &view,
            default_view_metadata_file_path(&namespace, &view, "00001.view.json"),
        ))
        .await
        .expect_err("inactive namespace must reject view creation");
    assert_matches!(view_error, TableCatalogStoreError::NotFound(_));
    assert!(
        store
            .list_namespaces(bucket)
            .await
            .expect("namespaces should list")
            .is_empty()
    );
}

async fn assert_inactive_table_bucket_rejects_catalog_creation<S>(store: &S, bucket: &str)
where
    S: TableCatalogStore + ?Sized,
{
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    let mut inactive = test_bucket_entry(bucket);
    inactive.state = TableCatalogEntryState::Deleted;
    store
        .put_table_bucket(inactive)
        .await
        .expect("inactive table bucket marker should be seeded");

    assert_matches!(
        store.create_namespace(test_namespace_entry(bucket, &namespace)).await,
        Err(TableCatalogStoreError::NotFound(_))
    );
    assert_matches!(
        store
            .create_table(test_table_entry(
                bucket,
                &namespace,
                &table,
                default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
            ))
            .await,
        Err(TableCatalogStoreError::NotFound(_))
    );
    assert_matches!(
        store
            .create_view(test_view_entry(
                bucket,
                &namespace,
                &view,
                default_view_metadata_file_path(&namespace, &view, "00001.view.json"),
            ))
            .await,
        Err(TableCatalogStoreError::NotFound(_))
    );
}

#[tokio::test]
async fn catalog_backings_keep_implicit_parents_implicit_during_resource_creation() {
    let bucket = "analytics";
    let object_store = ObjectTableCatalogStore::new(TestCatalogObjectBackend::default());
    create_resources_in_implicit_parents(&object_store, bucket).await;

    let strong_backend = TestCatalogObjectBackend::default();
    let strong_store = StrongTableCatalogStore::new(strong_backend.clone());
    create_resources_in_implicit_parents(&strong_store, bucket).await;
    let restarted = StrongTableCatalogStore::new(strong_backend);
    assert_implicit_parent_resources(&restarted, bucket).await;
}

#[tokio::test]
async fn strong_catalog_namespace_drop_rejects_retained_inactive_resources() {
    let store = StrongTableCatalogStore::new(TestCatalogObjectBackend::default());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .expect("namespace should be created");
    let mut entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    entry.state = TableCatalogEntryState::Deleted;
    store.create_table(entry).await.expect("inactive table should be retained");

    assert_matches!(
        store.drop_namespace(bucket, &namespace.public_name()).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
}

#[tokio::test]
async fn catalog_backings_reject_resource_creation_in_inactive_namespace() {
    assert_inactive_namespace_rejects_resource_creation(
        &ObjectTableCatalogStore::new(TestCatalogObjectBackend::default()),
        "object-catalog",
    )
    .await;
    assert_inactive_namespace_rejects_resource_creation(
        &StrongTableCatalogStore::new(TestCatalogObjectBackend::default()),
        "strong-catalog",
    )
    .await;
}

#[tokio::test]
async fn catalog_backings_reject_creation_in_inactive_table_bucket() {
    assert_inactive_table_bucket_rejects_catalog_creation(
        &ObjectTableCatalogStore::new(TestCatalogObjectBackend::default()),
        "object-catalog",
    )
    .await;
    assert_inactive_table_bucket_rejects_catalog_creation(
        &StrongTableCatalogStore::new(TestCatalogObjectBackend::default()),
        "strong-catalog",
    )
    .await;
}

#[test]
fn resolver_builds_paths_under_reserved_table_boundary() {
    let table = TableIdentifier::new(
        IdentifierSegment::parse("warehouse1").unwrap(),
        Namespace::parse("analytics.daily").unwrap(),
        IdentifierSegment::parse("events").unwrap(),
    );
    let resolver = TablePathResolver::default();

    assert_eq!(
        resolver.current_pointer_path(&table),
        ".rustfs-table/warehouses/warehouse1/namespaces/analytics/daily/tables/events/current.json"
    );
    assert_eq!(
        resolver.metadata_dir_path(&table),
        ".rustfs-table/warehouses/warehouse1/namespaces/analytics/daily/tables/events/metadata"
    );
}

#[tokio::test]
async fn strong_catalog_table_rename_is_atomic_and_preserves_stable_table_state() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let source_namespace = Namespace::parse("sales").expect("source namespace should parse");
    let destination_namespace = Namespace::parse("curated").expect("destination namespace should parse");
    let source_table = IdentifierSegment::parse("orders").expect("source table should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &source_namespace))
        .await
        .expect("source namespace should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &destination_namespace))
        .await
        .expect("destination namespace should be created");
    let source = test_table_entry(
        bucket,
        &source_namespace,
        &source_table,
        default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json"),
    );
    store
        .create_table(source.clone())
        .await
        .expect("source table should be created");

    store
        .rename_table(bucket, "sales", "orders", "curated", "orders_v2")
        .await
        .expect("table should rename");

    assert!(
        store
            .load_table(bucket, "sales", "orders")
            .await
            .expect("source lookup should succeed")
            .is_none()
    );
    let destination = store
        .load_table(bucket, "curated", "orders_v2")
        .await
        .expect("destination lookup should succeed")
        .expect("renamed table should exist");
    assert_eq!(destination.table_id, source.table_id);
    assert_eq!(destination.table_uuid, source.table_uuid);
    assert_eq!(destination.warehouse_location, source.warehouse_location);
    assert_eq!(destination.metadata_location, source.metadata_location);
    assert_eq!(destination.version_token, source.version_token);
    assert_eq!(destination.generation, source.generation);
    let old_manifest = format!(
        "{}/manifest-00001.avro",
        default_table_metadata_dir_path(&source_namespace, &source_table)
    );
    assert_eq!(
        table_maintenance_object_kind_for_entry(&destination, None, &old_manifest),
        Some(TableMetadataMaintenanceObjectKind::ManifestFile)
    );
    let resource = store
        .resolve_table_data_plane_resource(bucket, "tables/table-id/data/part.parquet")
        .await
        .expect("data-plane lookup should succeed")
        .expect("renamed table should own its warehouse prefix");
    assert_eq!(resource.namespace, "curated");
    assert_eq!(resource.table, "orders_v2");

    let mut replacement = test_table_entry(
        bucket,
        &source_namespace,
        &source_table,
        default_table_metadata_file_path(&source_namespace, &source_table, "00001-replacement.metadata.json"),
    );
    replacement.table_id = "replacement-table-id".to_string();
    replacement.table_uuid = "replacement-table-uuid".to_string();
    replacement.warehouse_location = "s3://analytics/tables/replacement-table-id".to_string();
    store
        .create_table(replacement.clone())
        .await
        .expect("the source identifier should be reusable after rename");
    let recreated_source = store
        .load_table(bucket, "sales", "orders")
        .await
        .expect("recreated source lookup should succeed")
        .expect("recreated source should exist");
    assert_eq!(recreated_source.table_id, replacement.table_id);
    assert_ne!(recreated_source.table_id, destination.table_id);
    assert_ne!(recreated_source.table_uuid, destination.table_uuid);
    assert_ne!(recreated_source.warehouse_location, destination.warehouse_location);
    assert_ne!(recreated_source.metadata_location, destination.metadata_location);

    let next_metadata_location =
        table_metadata_file_path_for_entry(&destination, "00002.metadata.json").expect("next metadata path should resolve");
    backend.seed_object(bucket, &next_metadata_location, b"{}".to_vec()).await;
    let committed = store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: "curated".to_string(),
            table: "orders_v2".to_string(),
            commit_id: "rename-followup-commit".to_string(),
            idempotency_key: Some("rename-followup-commit".to_string()),
            operation: "append".to_string(),
            expected_version_token: destination.version_token,
            expected_metadata_location: destination.metadata_location,
            new_metadata_location: next_metadata_location.clone(),
            requirements: Vec::new(),
            writer: Some("rename-test".to_string()),
        })
        .await
        .expect("renamed table should accept a commit in its stable metadata directory");
    assert_eq!(committed.table.metadata_location, next_metadata_location);

    let restarted = StrongTableCatalogStore::new(backend);
    let restarted_source = restarted
        .load_table(bucket, "sales", "orders")
        .await
        .expect("source lookup after restart should succeed")
        .expect("recreated source should survive restart");
    assert_eq!(restarted_source.table_id, replacement.table_id);
    let restarted_destination = restarted
        .load_table(bucket, "curated", "orders_v2")
        .await
        .expect("destination lookup after restart should succeed")
        .expect("destination should survive restart");
    assert_eq!(restarted_destination.metadata_location, committed.table.metadata_location);
    assert_eq!(
        table_metadata_file_path_for_entry(&restarted_destination, "00003.metadata.json")
            .expect("stable metadata path should survive restart"),
        default_table_metadata_file_path(&source_namespace, &source_table, "00003.metadata.json")
    );
}

#[tokio::test]
async fn strong_catalog_table_rename_rejects_missing_and_conflicting_destinations() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend);
    let bucket = "analytics";
    let source_namespace = Namespace::parse("sales").expect("source namespace should parse");
    let destination_namespace = Namespace::parse("curated").expect("destination namespace should parse");
    let source_table = IdentifierSegment::parse("orders").expect("source table should parse");
    let destination_table = IdentifierSegment::parse("orders_v2").expect("destination table should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &source_namespace))
        .await
        .expect("source namespace should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &destination_namespace))
        .await
        .expect("destination namespace should be created");
    store
        .create_table(test_table_entry(
            bucket,
            &source_namespace,
            &source_table,
            default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json"),
        ))
        .await
        .expect("source table should be created");
    let mut existing = test_table_entry(
        bucket,
        &destination_namespace,
        &destination_table,
        default_table_metadata_file_path(&destination_namespace, &destination_table, "00001.metadata.json"),
    );
    existing.table_id = "destination-table-id".to_string();
    existing.table_uuid = "destination-table-uuid".to_string();
    existing.warehouse_location = "s3://analytics/tables/destination-table-id".to_string();
    store
        .create_table(existing)
        .await
        .expect("destination table should be created");

    assert_matches!(
        store.rename_table(bucket, "sales", "orders", "curated", "orders_v2").await,
        Err(TableCatalogStoreError::AlreadyExists(_))
    );
    assert_matches!(
        store.rename_table(bucket, "sales", "orders", "missing", "orders_v3").await,
        Err(TableCatalogStoreError::NamespaceNotFound(_))
    );
    assert_matches!(
        store.rename_table(bucket, "sales", "missing", "curated", "orders_v3").await,
        Err(TableCatalogStoreError::TableNotFound(_))
    );

    let destination_view = IdentifierSegment::parse("orders_view").expect("view should parse");
    store
        .create_view(test_view_entry(
            bucket,
            &destination_namespace,
            &destination_view,
            default_view_metadata_file_path(&destination_namespace, &destination_view, "00001.metadata.json"),
        ))
        .await
        .expect("destination view should be created");
    assert_matches!(
        store.rename_table(bucket, "sales", "orders", "curated", "orders_view").await,
        Err(TableCatalogStoreError::AlreadyExists(_))
    );
    assert!(
        store
            .load_table(bucket, "sales", "orders")
            .await
            .expect("source lookup should succeed")
            .is_some()
    );
}

#[tokio::test]
async fn strong_catalog_table_rename_does_not_publish_failed_snapshot() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let source_namespace = Namespace::parse("sales").expect("source namespace should parse");
    let destination_namespace = Namespace::parse("curated").expect("destination namespace should parse");
    let source_table = IdentifierSegment::parse("orders").expect("source table should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &source_namespace))
        .await
        .expect("source namespace should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &destination_namespace))
        .await
        .expect("destination namespace should be created");
    store
        .create_table(test_table_entry(
            bucket,
            &source_namespace,
            &source_table,
            default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json"),
        ))
        .await
        .expect("source table should be created");
    backend
        .fail_next_put(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
        )
        .await;

    assert_matches!(
        store.rename_table(bucket, "sales", "orders", "curated", "orders_v2").await,
        Err(TableCatalogStoreError::Internal(_))
    );
    assert!(
        store
            .load_table(bucket, "sales", "orders")
            .await
            .expect("source lookup should succeed")
            .is_some()
    );
    assert!(
        store
            .load_table(bucket, "curated", "orders_v2")
            .await
            .expect("destination lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn strong_catalog_table_rename_returns_success_after_committed_snapshot_reload_failure() {
    let backend = TestCatalogObjectBackend::default();
    let store = StrongTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let source_namespace = Namespace::parse("sales").expect("source namespace should parse");
    let destination_namespace = Namespace::parse("curated").expect("destination namespace should parse");
    let source_table = IdentifierSegment::parse("orders").expect("source table should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("bucket should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &source_namespace))
        .await
        .expect("source namespace should be created");
    store
        .create_namespace(test_namespace_entry(bucket, &destination_namespace))
        .await
        .expect("destination namespace should be created");
    store
        .create_table(test_table_entry(
            bucket,
            &source_namespace,
            &source_table,
            default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json"),
        ))
        .await
        .expect("source table should be created");
    backend
        .fail_next_read(
            RUSTFS_META_BUCKET,
            &StrongTableCatalogStore::<TestCatalogObjectBackend>::snapshot_object_path(),
        )
        .await;

    store
        .rename_table(bucket, "sales", "orders", "curated", "orders_v2")
        .await
        .expect("durably committed rename should succeed despite local reload failure");
    assert!(!store.is_hydrated_for_test().await);
    assert!(
        store
            .load_table(bucket, "curated", "orders_v2")
            .await
            .expect("destination lookup should reload durable state")
            .is_some()
    );
}

#[tokio::test]
async fn configured_object_catalog_rejects_table_rename() {
    let store =
        ConfiguredTableCatalogStore::new_for_test(TestCatalogObjectBackend::default(), TableCatalogBackingMode::ObjectBacked);

    assert_matches!(
        store
            .rename_table("analytics", "sales", "orders", "curated", "orders_v2")
            .await,
        Err(TableCatalogStoreError::Unsupported(_))
    );
}
