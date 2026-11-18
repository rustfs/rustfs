use super::*;

async fn seed_commit(
    store: &ObjectTableCatalogStore<TestCatalogObjectBackend>,
    bucket: &str,
    table_id: &str,
    id: &str,
    receipt: bool,
) -> (String, Option<String>) {
    let record = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_id: table_id.to_string(),
        commit_id: id.to_string(),
        idempotency_key: receipt.then(|| format!("request-{id}")),
        operation: "append".to_string(),
        expected_version_token: "previous".to_string(),
        new_version_token: format!("version-{id}"),
        previous_metadata_location: format!("tables/{table_id}/metadata/previous.metadata.json"),
        new_metadata_location: format!("tables/{table_id}/metadata/{id}.metadata.json"),
        requirements: Vec::new(),
        status: CommitLogStatus::Staged,
        writer: None,
        created_at: None,
        updated_at: None,
    };
    let bytes = serde_json::to_vec(&record).unwrap();
    let key = store.paths.commit_log_entry_path(bucket, table_id, id);
    store.backend.seed_object(RUSTFS_META_BUCKET, &key, bytes.clone()).await;
    let receipt = record
        .idempotency_key
        .map(|k| store.paths.commit_idempotency_entry_path(bucket, table_id, &k));
    if let Some(receipt) = &receipt {
        store.backend.seed_object(RUSTFS_META_BUCKET, receipt, bytes).await;
    }
    (key, receipt)
}

async fn finish_bucket_cleanup(
    store: &ObjectTableCatalogStore<TestCatalogObjectBackend>,
    bucket: &str,
) -> TableCatalogStoreResult<()> {
    let publication = store.backend.with_held_cleanup_publication(bucket).await?;
    let catalog = ConfiguredTableCatalogStore::ObjectBacked(store.clone());
    let mut plan = catalog
        .prepare_empty_table_bucket_cleanup(test_bucket_entry(bucket), &publication)
        .await?;
    catalog.execute_empty_table_bucket_cleanup(&mut plan, Uuid::new_v4()).await
}

#[tokio::test]
async fn drop_reclaims_only_its_commit_history_after_confirming_table_removal() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let ns = Namespace::parse("sales").unwrap();
    let name = IdentifierSegment::parse("orders").unwrap();
    seed_table_for_metadata_maintenance(
        &store,
        bucket,
        &ns,
        &name,
        default_table_metadata_file_path(&ns, &name, "00001.metadata.json"),
    )
    .await;
    let entry_path = store.paths.table_entry_path(bucket, &ns, &name);
    let (log, receipt) = seed_commit(&store, bucket, "table-id", "same-commit", true).await;
    let (other_log, other_receipt) = seed_commit(&store, bucket, "other-table-id", "same-commit", true).await;
    let receipt = receipt.unwrap();
    backend.fail_delete_attempt(RUSTFS_META_BUCKET, &entry_path, 1).await;
    assert!(store.drop_table(bucket, "sales", "orders").await.is_err());
    for key in [&entry_path, &log, &receipt] {
        assert!(backend.object_exists(RUSTFS_META_BUCKET, key).await.unwrap());
    }
    backend.fail_after_next_delete(RUSTFS_META_BUCKET, &entry_path).await;
    store.drop_table(bucket, "sales", "orders").await.unwrap();
    for key in [&entry_path, &log, &receipt] {
        assert!(!backend.object_exists(RUSTFS_META_BUCKET, key).await.unwrap());
    }
    for key in [other_log, other_receipt.unwrap()] {
        assert!(backend.object_exists(RUSTFS_META_BUCKET, &key).await.unwrap());
    }
}

async fn seed_maintenance_history(store: &ObjectTableCatalogStore<TestCatalogObjectBackend>, bucket: &str, table_ids: &[String]) {
    let fixture = ConditionalMaintenanceFixture::new(TableCatalogBackingMode::ObjectBacked, 0).await;
    let mut report = fixture.run("worker", 0, fixture.now).await.unwrap();
    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
    let ns = Namespace::parse("sales").unwrap();
    for (index, id) in table_ids.iter().take(4).enumerate() {
        let table = IdentifierSegment::parse(format!("table_{index}")).unwrap();
        report.job.table_bucket = bucket.to_string();
        report.job.namespace = ns.public_name();
        report.job.table = table.as_str().to_string();
        report.job.table_id.clone_from(id);
        for attempt in 0..[2, 2, 2, 1][index] {
            report.job.job_id = format!("job-{attempt}");
            let named = store
                .paths
                .table_maintenance_job_path(bucket, &ns, &table, id, &report.job.job_id);
            store
                .backend
                .seed_object(RUSTFS_META_BUCKET, &named, serde_json::to_vec(&report).unwrap())
                .await;
        }
        let bytes = serde_json::to_vec(&report).unwrap();
        for alias in [
            store.paths.table_maintenance_current_job_path(bucket, &ns, &table, id),
            store.paths.table_maintenance_latest_job_path(bucket, &ns, &table, id),
        ] {
            store.backend.seed_object(RUSTFS_META_BUCKET, &alias, bytes.clone()).await;
        }
        if index == 0 {
            let config = store.paths.table_maintenance_config_path(bucket, &ns, &table, id);
            store
                .backend
                .seed_object(
                    RUSTFS_META_BUCKET,
                    &config,
                    serde_json::to_vec(&TableMaintenanceConfig::default()).unwrap(),
                )
                .await;
        }
    }
}

async fn assert_legacy_bucket_cleanup(bucket: &str, index_count: usize, commit_owners: usize, commits: usize, receipts: usize) {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store.backfill_table_warehouse_index(bucket).await.unwrap();
    let mut ids = Vec::new();
    for index in 0..index_count {
        let id = Uuid::new_v4().to_string();
        let record = TableWarehouseIndexEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: bucket.to_string(),
            namespace: "sales".to_string(),
            table: format!("table_{index}"),
            table_id: id.clone(),
            warehouse_object_prefix: format!("tables/{id}/"),
            state: TableCatalogEntryState::Deleted,
        };
        let key = store
            .paths
            .warehouse_index_entry_path(bucket, &record.warehouse_object_prefix);
        backend
            .seed_object(RUSTFS_META_BUCKET, &key, serde_json::to_vec(&record).unwrap())
            .await;
        ids.push(id);
    }
    for index in 0..commits {
        seed_commit(&store, bucket, &ids[index % commit_owners], &index.to_string(), index < receipts).await;
    }
    if receipts > 0 {
        seed_maintenance_history(&store, bucket, &ids).await;
    }
    finish_bucket_cleanup(&store, bucket).await.unwrap();
    let marker_path = store.paths.table_bucket_entry_path(bucket);
    let remaining = backend
        .list_objects(RUSTFS_META_BUCKET, marker_path.strip_suffix(TABLE_BUCKET_ENTRY_FILE).unwrap())
        .await
        .unwrap();
    assert_eq!(remaining, vec![marker_path]);
    let marker = store.get_table_bucket(bucket).await.unwrap().unwrap();
    assert_eq!(marker.state, TableCatalogEntryState::Deleted);
}

#[tokio::test]
async fn cleanup_db_with_repeated_commits_for_dropped_tables() {
    assert_legacy_bucket_cleanup("warehouse-db", 450, 9, 34, 0).await;
}

#[tokio::test]
async fn cleanup_jmdb_with_one_commit_per_dropped_table() {
    assert_legacy_bucket_cleanup("warehouse-jmdb", 49, 47, 47, 0).await;
}

#[tokio::test]
async fn cleanup_jm_with_commits_receipts_and_maintenance_history() {
    assert_legacy_bucket_cleanup("warehouse-jm", 758, 68, 642, 2).await;
}

#[tokio::test]
async fn dropping_a_rename_source_tombstone_preserves_the_live_tables_commit_history() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let ns = Namespace::parse("sales").unwrap();
    let dest = IdentifierSegment::parse("new_name").unwrap();
    let source = IdentifierSegment::parse("old_name").unwrap();
    seed_table_for_metadata_maintenance(
        &store,
        bucket,
        &ns,
        &dest,
        default_table_metadata_file_path(&ns, &dest, "00001.metadata.json"),
    )
    .await;
    let (log, _) = seed_commit(&store, bucket, "table-id", "before-rename", false).await;
    let mut tombstone = store.load_table(bucket, "sales", "new_name").await.unwrap().unwrap();
    tombstone.table = source.as_str().to_string();
    tombstone.state = TableCatalogEntryState::Deleted;
    tombstone.warehouse_location = format!("s3://{bucket}/old_location/");
    let key = store.paths.table_entry_path(bucket, &ns, &source);
    backend
        .seed_object(RUSTFS_META_BUCKET, &key, serde_json::to_vec(&tombstone).unwrap())
        .await;
    store.drop_table(bucket, "sales", "old_name").await.unwrap();
    assert!(backend.object_exists(RUSTFS_META_BUCKET, &log).await.unwrap());
    assert!(store.load_table(bucket, "sales", "new_name").await.unwrap().is_some());
}

#[tokio::test]
async fn cleanup_retries_an_interrupted_drop_with_only_an_idempotency_receipt_left() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let ns = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store.create_namespace(test_namespace_entry(bucket, &ns)).await.unwrap();
    let mut entry = test_table_entry(bucket, &ns, &table, default_table_metadata_file_path(&ns, &table, "00001.metadata.json"));
    entry.table_id = Uuid::new_v4().to_string();
    entry.warehouse_location = format!("s3://{bucket}/tables/{}", entry.table_id);
    store.create_table(entry.clone()).await.unwrap();
    let (log, receipt) = seed_commit(&store, bucket, &entry.table_id, "interrupted", true).await;
    let receipt = receipt.unwrap();
    backend.fail_delete_attempt(RUSTFS_META_BUCKET, &receipt, 1).await;
    assert!(store.drop_table(bucket, "sales", "orders").await.is_err());
    assert!(store.load_table(bucket, "sales", "orders").await.unwrap().is_none());
    assert!(!backend.object_exists(RUSTFS_META_BUCKET, &log).await.unwrap());
    assert!(backend.object_exists(RUSTFS_META_BUCKET, &receipt).await.unwrap());
    store.drop_namespace(bucket, "sales").await.unwrap();
    finish_bucket_cleanup(&store, bucket).await.unwrap();
    assert!(!backend.object_exists(RUSTFS_META_BUCKET, &receipt).await.unwrap());
}

#[tokio::test]
async fn cleanup_rejects_misfiled_history_and_unfinished_maintenance_before_deletion() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    let (log, _) = seed_commit(&store, bucket, "owner", "commit", false).await;
    let bytes = backend.read_object(RUSTFS_META_BUCKET, &log).await.unwrap().unwrap().data;
    let wrong = store.paths.commit_log_entry_path(bucket, "other-owner", "commit");
    backend.seed_object(RUSTFS_META_BUCKET, &wrong, bytes).await;
    assert_matches!(finish_bucket_cleanup(&store, bucket).await, Err(TableCatalogStoreError::Invalid(_)));
    backend.delete_object(RUSTFS_META_BUCKET, &wrong).await.unwrap();

    let fixture = ConditionalMaintenanceFixture::new(TableCatalogBackingMode::ObjectBacked, 0).await;
    let mut report = fixture.queued.clone();
    let ns = Namespace::parse(&report.job.namespace).unwrap();
    let name = IdentifierSegment::parse(&report.job.table).unwrap();
    let current = store
        .paths
        .table_maintenance_current_job_path(bucket, &ns, &name, &report.job.table_id);
    for status in [
        TableMetadataMaintenanceJobStatus::Queued,
        TableMetadataMaintenanceJobStatus::Running,
    ] {
        report.job.status = status;
        backend
            .seed_object(RUSTFS_META_BUCKET, &current, serde_json::to_vec(&report).unwrap())
            .await;
        assert_matches!(finish_bucket_cleanup(&store, bucket).await, Err(TableCatalogStoreError::Conflict(message)) if message.contains("maintenance job"));
        assert!(backend.object_exists(RUSTFS_META_BUCKET, &log).await.unwrap());
        assert!(backend.object_exists(RUSTFS_META_BUCKET, &current).await.unwrap());
    }
    report.job.status = TableMetadataMaintenanceJobStatus::Successful;
    report.job.table_id = "different-owner".to_string();
    backend
        .seed_object(RUSTFS_META_BUCKET, &current, serde_json::to_vec(&report).unwrap())
        .await;
    assert_matches!(finish_bucket_cleanup(&store, bucket).await, Err(TableCatalogStoreError::Invalid(_)));
    assert!(backend.object_exists(RUSTFS_META_BUCKET, &log).await.unwrap());
}
