use super::*;
use tokio_util::sync::CancellationToken;

async fn interrupted_rename(
    backend: &TestCatalogObjectBackend,
    bucket: &'static str,
    stage: TableRenameIntentState,
) -> (TableEntry, String) {
    let store = ObjectTableCatalogStore::new(backend.clone());
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, metadata).await;
    store
        .create_namespace(test_namespace_entry(bucket, &Namespace::parse("curated").unwrap()))
        .await
        .unwrap();
    let source = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    let bucket_path = store.paths.table_bucket_entry_path(bucket);
    let marker_attempt = backend.put_attempt_count(RUSTFS_META_BUCKET, &bucket_path).await + 1;
    let pause = backend
        .pause_put_attempt(RUSTFS_META_BUCKET, &bucket_path, marker_attempt)
        .await;
    let rename = tokio::spawn(async move { store.rename_table(bucket, "sales", "orders", "curated", "orders_v2").await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .unwrap();
    let intent_path = backend
        .state
        .lock()
        .await
        .objects
        .iter()
        .find_map(|((_, key), record)| {
            serde_json::from_slice::<TableRenameIntent>(&record.data)
                .ok()
                .filter(|intent| intent.table_bucket == bucket)
                .map(|_| key.clone())
        })
        .expect("prepared intent precedes the bucket marker");
    if stage == TableRenameIntentState::Completed {
        backend
            .fail_put_attempt(RUSTFS_META_BUCKET, &bucket_path, marker_attempt + 1)
            .await;
    } else {
        let attempt = match stage {
            TableRenameIntentState::Prepared => 2,
            TableRenameIntentState::MaintenancePrepared => 3,
            TableRenameIntentState::SourceFenced => 4,
            TableRenameIntentState::DestinationWritten => 5,
            TableRenameIntentState::SourceTombstoned => 6,
            TableRenameIntentState::IndexPublished => 7,
            TableRenameIntentState::DestinationPublished => 8,
            TableRenameIntentState::Completed => unreachable!(),
        };
        backend.fail_put_attempt(RUSTFS_META_BUCKET, &intent_path, attempt).await;
    }
    pause.release();
    assert!(
        tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, rename)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let store = ObjectTableCatalogStore::new(backend.clone());
    let (intent, _) = store
        .read_entry::<TableRenameIntent>(RUSTFS_META_BUCKET, &intent_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(intent.state, stage);
    assert_matches!(store.list_tables(bucket, "sales").await, Err(TableCatalogStoreError::Unavailable(_)));
    (source, intent_path)
}

async fn wait_for_recovery(store: &ObjectTableCatalogStore<TestCatalogObjectBackend>, bucket: &str) {
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        loop {
            if store
                .get_table_bucket(bucket)
                .await
                .unwrap()
                .unwrap()
                .active_rename_id
                .is_none()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background recovery should restore read availability without another mutation");
}

#[tokio::test]
async fn background_rename_recovery_resumes_every_durable_phase_after_restart() {
    for stage in [
        TableRenameIntentState::Prepared,
        TableRenameIntentState::MaintenancePrepared,
        TableRenameIntentState::SourceFenced,
        TableRenameIntentState::DestinationWritten,
        TableRenameIntentState::SourceTombstoned,
        TableRenameIntentState::IndexPublished,
        TableRenameIntentState::DestinationPublished,
        TableRenameIntentState::Completed,
    ] {
        let backend = TestCatalogObjectBackend::default();
        let (source, intent_path) = interrupted_rename(&backend, "analytics", stage).await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        let cancellation = CancellationToken::new();
        let recovered_store = store.clone();
        let mut runtime =
            CatalogBackgroundTask::start(&cancellation, |stop| super::super::rename_recovery::run(recovered_store, stop));
        wait_for_recovery(&store, "analytics").await;
        runtime.request_stop();
        runtime.join().await.unwrap().unwrap();
        assert!(store.list_tables("analytics", "sales").await.unwrap().is_empty());
        let destination = store.load_table("analytics", "curated", "orders_v2").await.unwrap().unwrap();
        assert_eq!(destination.table_id, source.table_id);
        assert_eq!(destination.table_uuid, source.table_uuid);
        assert_eq!(destination.metadata_location, source.metadata_location);
        assert_eq!(destination.warehouse_location, source.warehouse_location);
        assert_eq!(destination.generation, source.generation);
        assert_eq!(destination.version_token, source.version_token);
        assert_eq!(
            store
                .read_entry::<TableRenameIntent>(RUSTFS_META_BUCKET, &intent_path)
                .await
                .unwrap()
                .unwrap()
                .0
                .state,
            TableRenameIntentState::Completed
        );
        store.drop_namespace("analytics", "sales").await.unwrap();
        assert!(store.get_namespace("analytics", "sales").await.unwrap().is_none());
        assert_eq!(store.load_table("analytics", "curated", "orders_v2").await.unwrap(), Some(destination));
    }
}

#[tokio::test]
async fn background_rename_recovery_retains_invalid_intents_and_recovers_other_buckets() {
    for corrupt in [false, true] {
        let backend = TestCatalogObjectBackend::default();
        let (_, invalid_intent) = interrupted_rename(&backend, "broken", TableRenameIntentState::IndexPublished).await;
        let (_, valid_intent) = interrupted_rename(&backend, "healthy", TableRenameIntentState::IndexPublished).await;
        if corrupt {
            backend
                .seed_object(RUSTFS_META_BUCKET, &invalid_intent, b"{broken".to_vec())
                .await;
        } else {
            backend
                .state
                .lock()
                .await
                .objects
                .remove(&(RUSTFS_META_BUCKET.to_string(), invalid_intent.clone()));
        }
        let store = ObjectTableCatalogStore::new(backend.clone());
        let marker_before = store.get_table_bucket("broken").await.unwrap().unwrap();
        let broken_key = (RUSTFS_META_BUCKET.to_string(), invalid_intent.clone());
        let reads_before = backend.state.lock().await.read_attempts[&broken_key];
        let cancellation = CancellationToken::new();
        let worker_store = store.clone();
        let mut runtime =
            CatalogBackgroundTask::start(&cancellation, |stop| super::super::rename_recovery::run(worker_store, stop));
        wait_for_recovery(&store, "healthy").await;
        tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
            while backend.state.lock().await.read_attempts[&broken_key] == reads_before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the invalid intent must also be attempted");
        runtime.request_stop();
        runtime.join().await.unwrap().unwrap();
        assert_eq!(store.get_table_bucket("broken").await.unwrap(), Some(marker_before));
        assert_matches!(store.list_tables("broken", "curated").await, Err(TableCatalogStoreError::Unavailable(_)));
        assert_matches!(store.drop_namespace("broken", "sales").await, Err(TableCatalogStoreError::Unavailable(_)));
        assert_eq!(
            store
                .read_entry::<TableRenameIntent>(RUSTFS_META_BUCKET, &valid_intent)
                .await
                .unwrap()
                .unwrap()
                .0
                .state,
            TableRenameIntentState::Completed
        );
    }
}

#[tokio::test(start_paused = true)]
async fn background_rename_recovery_drains_on_shutdown_and_serializes_competing_workers() {
    let backend = TestCatalogObjectBackend::default();
    let (_, intent_path) = interrupted_rename(&backend, "analytics", TableRenameIntentState::IndexPublished).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket_path = store.paths.table_bucket_entry_path("analytics");
    let pause = backend
        .pause_put_attempt(
            RUSTFS_META_BUCKET,
            &bucket_path,
            backend.put_attempt_count(RUSTFS_META_BUCKET, &bucket_path).await + 1,
        )
        .await;
    let cancellation = CancellationToken::new();
    let worker_store = store.clone();
    let mut runtime = CatalogBackgroundTask::start(&cancellation, |stop| super::super::rename_recovery::run(worker_store, stop));
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .unwrap();
    let publication_lock = default_table_bucket_publication_lock_path();
    let before = backend.write_lock_acquisition_count("analytics", &publication_lock).await;
    let other_store = store.clone();
    let competing = tokio::spawn(async move { other_store.recover_pending_table_rename("analytics").await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.write_lock_acquisition_count("analytics", &publication_lock).await == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancellation.cancel();
    assert!(
        tokio::time::timeout(StdDuration::from_millis(10), runtime.join())
            .await
            .is_err()
    );
    assert!(!competing.is_finished());
    pause.release();
    runtime.join().await.unwrap().unwrap();
    competing.await.unwrap().unwrap();
    assert!(
        store
            .get_table_bucket("analytics")
            .await
            .unwrap()
            .unwrap()
            .active_rename_id
            .is_none()
    );
    assert_eq!(
        store
            .read_entry::<TableRenameIntent>(RUSTFS_META_BUCKET, &intent_path)
            .await
            .unwrap()
            .unwrap()
            .0
            .state,
        TableRenameIntentState::Completed
    );
}

#[tokio::test]
async fn rename_recovery_discovery_skips_bucket_children_and_advances_past_invalid_entries() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    for bucket in ["first", "second"] {
        store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        let root = store
            .paths
            .table_bucket_entry_path(bucket)
            .trim_end_matches(TABLE_BUCKET_ENTRY_FILE)
            .to_string();
        for index in 0..1100 {
            backend
                .seed_object(RUSTFS_META_BUCKET, &format!("{root}renames/{index}.json"), Vec::new())
                .await;
        }
    }
    let broken = store.paths.table_bucket_entry_path("broken");
    backend.seed_object(RUSTFS_META_BUCKET, &broken, b"{}".to_vec()).await;
    backend.reset_call_counts().await;
    let mut cursor = None;
    let mut found = BTreeSet::new();
    let mut failures = 0;
    for _ in 0..4 {
        match store.next_table_bucket_for_rename_recovery(&mut cursor).await {
            Ok(Some(entry)) => {
                found.insert(entry.table_bucket);
            }
            Err(_) => {
                failures += 1;
            }
            Ok(None) => break,
        }
    }
    assert_eq!(found, BTreeSet::from(["first".to_string(), "second".to_string()]));
    assert_eq!(failures, 1);
    assert!(cursor.is_none());
    assert_eq!(backend.list_call_count().await, 4);
}

#[tokio::test]
async fn drop_namespace_validates_table_and_view_states_across_pages() {
    for view in [false, true] {
        for state in [
            TableCatalogEntryState::Active,
            TableCatalogEntryState::Renaming,
            TableCatalogEntryState::Deleting,
            TableCatalogEntryState::Deleted,
        ] {
            let backend = TestCatalogObjectBackend::default();
            let store = ObjectTableCatalogStore::new(backend.clone());
            let namespace = Namespace::parse("sales").unwrap();
            let table = IdentifierSegment::parse("orders").unwrap();
            store.put_table_bucket(test_bucket_entry("analytics")).await.unwrap();
            store
                .create_namespace(test_namespace_entry("analytics", &namespace))
                .await
                .unwrap();
            let path = if view {
                store.paths.view_entry_path("analytics", &namespace, &table)
            } else {
                store.paths.table_entry_path("analytics", &namespace, &table)
            };
            let prefix = if view {
                store.paths.view_entries_prefix("analytics", &namespace)
            } else {
                store.paths.table_entries_prefix("analytics", &namespace)
            };
            for index in 0..TABLE_CATALOG_LIST_MAX_KEYS {
                backend
                    .seed_object(RUSTFS_META_BUCKET, &format!("{prefix}0000-spacer/{index:04}.json"), Vec::new())
                    .await;
            }
            let data = if view {
                let mut entry = test_view_entry("analytics", &namespace, &table, "metadata.json".to_string());
                entry.state = state.clone();
                serde_json::to_vec(&entry).unwrap()
            } else {
                let mut entry = test_table_entry("analytics", &namespace, &table, "metadata.json".to_string());
                entry.state = state.clone();
                serde_json::to_vec(&entry).unwrap()
            };
            backend.seed_object(RUSTFS_META_BUCKET, &path, data).await;
            let before = backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap();
            let result = store.drop_namespace("analytics", "sales").await;
            if state == TableCatalogEntryState::Deleted {
                result.unwrap();
                assert!(store.get_namespace("analytics", "sales").await.unwrap().is_none());
            } else {
                assert_matches!(result, Err(TableCatalogStoreError::Conflict(_)));
                assert!(store.get_namespace("analytics", "sales").await.unwrap().is_some());
            }
            assert_eq!(backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap(), before);
        }
    }
}

#[tokio::test]
async fn drop_namespace_rejects_invalid_tombstones_and_read_failures() {
    for view in [false, true] {
        for failure in ["decode", "identity", "version", "read"] {
            let backend = TestCatalogObjectBackend::default();
            let store = ObjectTableCatalogStore::new(backend.clone());
            let namespace = Namespace::parse("sales").unwrap();
            let table = IdentifierSegment::parse("orders").unwrap();
            store.put_table_bucket(test_bucket_entry("analytics")).await.unwrap();
            store
                .create_namespace(test_namespace_entry("analytics", &namespace))
                .await
                .unwrap();
            let (path, mut entry) = if view {
                (
                    store.paths.view_entry_path("analytics", &namespace, &table),
                    serde_json::to_value(test_view_entry("analytics", &namespace, &table, "metadata.json".to_string())).unwrap(),
                )
            } else {
                (
                    store.paths.table_entry_path("analytics", &namespace, &table),
                    serde_json::to_value(test_table_entry("analytics", &namespace, &table, "metadata.json".to_string())).unwrap(),
                )
            };
            entry["state"] = serde_json::json!("DELETED");
            if failure == "identity" {
                entry["namespace"] = serde_json::json!("curated");
            }
            if failure == "version" {
                entry["version"] = serde_json::json!(999);
            }
            let data = if failure == "decode" {
                b"{broken".to_vec()
            } else {
                serde_json::to_vec(&entry).unwrap()
            };
            backend.seed_object(RUSTFS_META_BUCKET, &path, data).await;
            if failure == "read" {
                backend.fail_next_read(RUSTFS_META_BUCKET, &path).await;
            }
            assert!(store.drop_namespace("analytics", "sales").await.is_err());
            assert!(store.get_namespace("analytics", "sales").await.unwrap().is_some());
        }
    }
}

#[tokio::test]
async fn rename_recovery_respects_restore_barriers_and_publication_lock_failure() {
    let backend = TestCatalogObjectBackend::default();
    interrupted_rename(&backend, "analytics", TableRenameIntentState::IndexPublished).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let restore_path = store.paths.catalog_restore_state_path("analytics");
    let restore = serde_json::json!({
        "version": 1, "table_bucket": "analytics", "restore_epoch": Uuid::new_v4().to_string(),
        "restore_id": "active-restore", "backup_id": "backup", "request_fingerprint": "a".repeat(64), "state": "PREPARED"
    });
    backend
        .seed_object(RUSTFS_META_BUCKET, &restore_path, serde_json::to_vec(&restore).unwrap())
        .await;
    let before = maintenance_test_object_versions(&backend).await;
    assert_matches!(
        store.recover_pending_table_rename("analytics").await,
        Err(TableCatalogStoreError::Unavailable(_))
    );
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
    backend
        .state
        .lock()
        .await
        .objects
        .remove(&(RUSTFS_META_BUCKET.to_string(), restore_path));
    *backend.fail_write_lock_path.lock().await = Some((
        default_table_bucket_publication_lock_path(),
        TableCatalogStoreError::Unavailable("injected publication lock failure".to_string()),
    ));
    let before = maintenance_test_object_versions(&backend).await;
    assert_matches!(
        store.recover_pending_table_rename("analytics").await,
        Err(TableCatalogStoreError::Unavailable(_))
    );
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
    store.recover_pending_table_rename("analytics").await.unwrap();
    assert!(
        store
            .get_table_bucket("analytics")
            .await
            .unwrap()
            .unwrap()
            .active_rename_id
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn background_rename_recovery_backs_off_and_retries_without_client_mutations() {
    let backend = TestCatalogObjectBackend::default();
    let (_, intent_path) = interrupted_rename(&backend, "analytics", TableRenameIntentState::IndexPublished).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    backend.fail_next_read(RUSTFS_META_BUCKET, &intent_path).await;
    let key = (RUSTFS_META_BUCKET.to_string(), intent_path);
    let reads_before = backend.state.lock().await.read_attempts[&key];
    let cancellation = CancellationToken::new();
    let worker_store = store.clone();
    let mut runtime = CatalogBackgroundTask::start(&cancellation, |stop| super::super::rename_recovery::run(worker_store, stop));
    while backend.state.lock().await.read_attempts[&key] == reads_before {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    let list_calls = backend.list_call_count().await;
    tokio::time::advance(StdDuration::from_secs(6)).await;
    while backend.list_call_count().await == list_calls {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        backend.state.lock().await.read_attempts[&key],
        reads_before + 1,
        "the next scan must respect the retry delay"
    );
    assert!(
        store
            .get_table_bucket("analytics")
            .await
            .unwrap()
            .unwrap()
            .active_rename_id
            .is_some()
    );
    tokio::task::yield_now().await;
    tokio::time::advance(StdDuration::from_secs(6)).await;
    wait_for_recovery(&store, "analytics").await;
    runtime.request_stop();
    runtime.join().await.unwrap().unwrap();
}

#[tokio::test]
async fn renamed_table_can_commit_and_back_up_after_dropping_its_source_namespace() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let mut source = test_table_entry(bucket, &namespace, &table, current.clone());
    source.table_uuid = Uuid::new_v4().to_string();
    let mut metadata = crate::table_catalog::test_support::table_metadata_json(&source.table_uuid, &source.warehouse_location);
    metadata["format-version"] = serde_json::json!(source.format_version);
    let metadata = serde_json::to_vec(&metadata).unwrap();
    backend.seed_object(bucket, &current, metadata.clone()).await;
    backend.seed_object(bucket, &next, metadata.clone()).await;
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &Namespace::parse("curated").unwrap()))
        .await
        .unwrap();
    store.create_table(source.clone()).await.unwrap();
    store
        .rename_table(bucket, "sales", "orders", "curated", "orders_v2")
        .await
        .unwrap();
    store.drop_namespace(bucket, "sales").await.unwrap();
    let resource = store
        .resolve_table_metadata_data_plane_resource(bucket, &current)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resource.namespace, "curated");
    assert_eq!(resource.table, "orders_v2");
    assert_eq!(resource.table_id, source.table_id);
    store
        .commit_table(TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: "curated".to_string(),
            table: "orders_v2".to_string(),
            commit_id: "post-rename-commit".to_string(),
            idempotency_key: None,
            operation: "update-metadata".to_string(),
            expected_version_token: source.version_token,
            expected_metadata_location: current.clone(),
            new_metadata_location: next.clone(),
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .unwrap();
    let renamed = store.load_table(bucket, "curated", "orders_v2").await.unwrap().unwrap();
    assert_eq!(renamed.metadata_location, next);
    assert_eq!(renamed.table_uuid, source.table_uuid);
    assert_eq!(renamed.warehouse_location, source.warehouse_location);
    let backup = store.create_object_catalog_backup(bucket, None).await.unwrap();
    assert!(!backup.backup_id.is_empty());
    assert_eq!(backend.read_object(bucket, &current).await.unwrap().unwrap().data, metadata);
    assert_eq!(backend.read_object(bucket, &next).await.unwrap().unwrap().data, metadata);
}

#[tokio::test(start_paused = true)]
async fn background_rename_recovery_continues_beyond_one_scan_batch() {
    let backend = TestCatalogObjectBackend::default();
    interrupted_rename(&backend, "analytics", TableRenameIntentState::IndexPublished).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let pending_path = store.paths.table_bucket_entry_path("analytics");
    let mut preceding = 0;
    for index in 0..1000 {
        let bucket = format!("bucket-{index:04}");
        if store.paths.table_bucket_entry_path(&bucket) < pending_path {
            store.put_table_bucket(test_bucket_entry(&bucket)).await.unwrap();
            preceding += 1;
            if preceding == 33 {
                break;
            }
        }
    }
    assert_eq!(preceding, 33);
    backend.reset_call_counts().await;
    let cancellation = CancellationToken::new();
    let worker_store = store.clone();
    let mut runtime = CatalogBackgroundTask::start(&cancellation, |stop| super::super::rename_recovery::run(worker_store, stop));
    while backend.list_call_count().await < 32 {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    assert!(
        store
            .get_table_bucket("analytics")
            .await
            .unwrap()
            .unwrap()
            .active_rename_id
            .is_some()
    );
    tokio::time::advance(StdDuration::from_secs(6)).await;
    wait_for_recovery(&store, "analytics").await;
    runtime.request_stop();
    runtime.join().await.unwrap().unwrap();
}
