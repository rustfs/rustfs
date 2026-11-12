use super::identifier::{
    TableIdentifier, TablePathResolver, default_namespace_marker_path, default_table_current_pointer_path,
    default_table_lifecycle_path, default_table_marker_path, default_table_root_prefix, is_valid_table_metadata_file_name,
    namespace_name_from_marker_path, table_name_from_marker_path, validate_object_mutation,
};
use super::test_support::{NoopTableCatalogStore, TestCatalogObjectBackend};
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

#[path = "tests/dropped_table_history.rs"]
mod dropped_table_history;

#[path = "tests/superseded_commit_recovery.rs"]
mod superseded_commit_recovery;

const TABLE_CATALOG_TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

#[path = "tests/object_backup_validation.rs"]
mod object_backup_validation;

#[path = "tests/object_maintenance_equivalence.rs"]
mod object_maintenance_equivalence;

#[path = "tests/object_backup_transfer.rs"]
mod object_backup_transfer;

#[tokio::test]
async fn catalog_backings_fence_direct_commits_with_publication_lock() {
    let object_backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(object_backend.clone());
    assert_direct_commit_uses_publication_lock(&object_store, &object_backend).await;
}

#[tokio::test]
async fn catalog_backings_stop_view_replacement_after_publication_fence_loss() {
    let object_backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(object_backend.clone());
    assert_view_replacement_rechecks_publication_fence(&object_store, &object_backend).await;
}

#[tokio::test]
async fn catalog_backings_fence_direct_table_drop() {
    let object_backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(object_backend.clone());
    assert_direct_drop_uses_publication_locks(object_store, &object_backend).await;
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
}

#[tokio::test]
async fn catalog_backings_reject_empty_resource_ids_and_duplicate_table_ids() {
    assert_resource_id_registration_contract(&ObjectTableCatalogStore::new(TestCatalogObjectBackend::default())).await;
}

async fn empty_catalog_cleanup_guard_fixture() -> (TestCatalogObjectBackend, TableCatalogBackupReport, String) {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_path = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let table_uuid = Uuid::new_v4().to_string();
    let warehouse_location = format!("s3://{bucket}/tables/{table_uuid}");
    let metadata = super::test_support::table_metadata_json(&table_uuid, &warehouse_location);
    backend
        .seed_object(bucket, &metadata_path, serde_json::to_vec(&metadata).unwrap())
        .await;
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    let mut entry = test_table_entry(bucket, &namespace, &table, metadata_path.clone());
    entry.table_id = table_uuid.clone();
    entry.table_uuid = table_uuid;
    entry.warehouse_location = warehouse_location;
    store.create_table(entry).await.unwrap();
    let backup = store.create_object_catalog_backup(bucket, None).await.unwrap();
    store.drop_table(bucket, "sales", "orders").await.unwrap();
    store.drop_namespace(bucket, "sales").await.unwrap();

    let publication = backend.with_held_cleanup_publication(bucket).await.unwrap();
    assert_eq!(TableCommitPublication::catalog_migration_read_permit_status(&publication), Some(true));
    assert!(TableCommitPublication::holds_table_bucket(&publication, bucket));
    let configured = ConfiguredTableCatalogStore::ObjectBacked(store);
    let cleanup = tokio::time::timeout(
        TABLE_CATALOG_TEST_TIMEOUT,
        configured.prepare_empty_table_bucket_cleanup(test_bucket_entry(bucket), &publication),
    )
    .await
    .expect("cleanup planning must reuse the held read permit")
    .expect("the metadata/index fixture must be valid before adding a restore or legacy control");
    assert_eq!(cleanup.metadata_objects().collect::<Vec<_>>(), vec![metadata_path.as_str()]);
    cleanup.ensure_held().unwrap();
    drop(cleanup);
    TableCommitPublication::complete(&publication);
    (backend, backup, metadata_path)
}

async fn assert_empty_catalog_cleanup_objects_unchanged(
    backend: &TestCatalogObjectBackend,
    before: &BTreeMap<(String, String), TableCatalogObject>,
    metadata_path: &str,
) {
    let after = maintenance_test_object_versions(backend).await;
    assert_eq!(&after, before, "cleanup changed catalog or metadata while authority was unavailable");
    assert!(after.contains_key(&("analytics".to_string(), metadata_path.to_string())));
    let paths = TableCatalogObjectPaths::default();
    let indexes = paths.warehouse_index_entries_prefix("analytics");
    assert!(
        after
            .keys()
            .any(|(bucket, key)| bucket == RUSTFS_META_BUCKET && key.starts_with(&indexes))
    );
    let marker = &after[&(RUSTFS_META_BUCKET.to_string(), paths.table_bucket_entry_path("analytics"))];
    let entry: TableBucketEntry = serde_json::from_slice(&marker.data).unwrap();
    assert_eq!(entry.state, TableCatalogEntryState::Active, "cleanup disabled the catalog");
}

#[tokio::test]
async fn empty_catalog_cleanup_rejects_persisted_restore_with_held_read_permit() {
    let (backend, backup, metadata_path) = empty_catalog_cleanup_guard_fixture().await;
    let bucket = "analytics";
    let paths = TableCatalogObjectPaths::default();
    let barrier_path = paths.catalog_restore_state_path(bucket);
    let restore_id = "cleanup-persisted-restore";
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &barrier_path).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    assert!(matches!(
        store
            .restore_object_catalog_backup_with_id(bucket, &backup.backup_id, None, true, Some(restore_id))
            .await,
        Err(TableCatalogStoreError::Internal(_))
    ));
    let barrier = backend.read_object(RUSTFS_META_BUCKET, &barrier_path).await.unwrap().unwrap();
    let intent = backend
        .read_object(RUSTFS_META_BUCKET, &paths.catalog_restore_intent_path(bucket, restore_id))
        .await
        .unwrap()
        .unwrap();
    let barrier: serde_json::Value = serde_json::from_slice(&barrier.data).unwrap();
    let intent: serde_json::Value = serde_json::from_slice(&intent.data).unwrap();
    assert_eq!(barrier["state"], "PREPARED");
    assert_eq!(barrier["restore_id"], restore_id);
    assert_eq!(barrier["backup_id"], backup.backup_id);
    assert_eq!(barrier["restore_epoch"], intent["restore_epoch"]);

    let publication = backend.with_held_cleanup_publication(bucket).await.unwrap();
    assert_eq!(TableCommitPublication::catalog_migration_read_permit_status(&publication), Some(true));
    assert!(TableCommitPublication::holds_table_bucket(&publication, bucket));
    let before = maintenance_test_object_versions(&backend).await;
    let configured = ConfiguredTableCatalogStore::ObjectBacked(store);
    let result = tokio::time::timeout(
        TABLE_CATALOG_TEST_TIMEOUT,
        configured.prepare_empty_table_bucket_cleanup(test_bucket_entry(bucket), &publication),
    )
    .await
    .expect("cleanup must reject the persisted barrier without reacquiring its held permit");
    match result {
        Err(TableCatalogStoreError::Unavailable(message)) => {
            assert!(message.contains("interrupted catalog restore"), "{message}")
        }
        Err(error) => panic!("cleanup rejected for the wrong reason: {error:?}"),
        Ok(_) => panic!("cleanup accepted an active persisted restore"),
    }
    assert_empty_catalog_cleanup_objects_unchanged(&backend, &before, &metadata_path).await;
}

#[tokio::test]
async fn empty_catalog_cleanup_rejects_legacy_control_with_held_read_permit() {
    let paths = TableCatalogObjectPaths::default();
    for (label, path) in [
        ("legacy global catalog fence", paths.legacy_backing_migration_global_fence_path()),
        ("legacy catalog snapshot", paths.legacy_catalog_snapshot_path()),
        ("legacy per-bucket catalog fence", paths.legacy_backing_migration_fence_path("analytics")),
        ("legacy catalog restore intent", paths.legacy_catalog_restore_intent_path("analytics")),
    ] {
        let (backend, _, metadata_path) = empty_catalog_cleanup_guard_fixture().await;
        let publication = backend.with_held_cleanup_publication("analytics").await.unwrap();
        assert_eq!(TableCommitPublication::catalog_migration_read_permit_status(&publication), Some(true));
        assert!(TableCommitPublication::holds_table_bucket(&publication, "analytics"));
        backend
            .seed_object(RUSTFS_META_BUCKET, &path, b"legacy-control-state".to_vec())
            .await;
        let before = maintenance_test_object_versions(&backend).await;
        let configured = ConfiguredTableCatalogStore::ObjectBacked(ObjectTableCatalogStore::new(backend.clone()));
        let result = tokio::time::timeout(
            TABLE_CATALOG_TEST_TIMEOUT,
            configured.prepare_empty_table_bucket_cleanup(test_bucket_entry("analytics"), &publication),
        )
        .await
        .expect("cleanup must reject legacy controls without reacquiring its held permit");
        match result {
            Err(TableCatalogStoreError::Unavailable(message)) => assert!(message.contains(label), "{label}: {message}"),
            Err(error) => panic!("{label}: cleanup rejected for the wrong reason: {error:?}"),
            Ok(_) => panic!("{label}: cleanup accepted legacy control state"),
        }
        assert_empty_catalog_cleanup_objects_unchanged(&backend, &before, &metadata_path).await;
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_claims_queued_job_and_replays_without_writes() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        let fixture = ConditionalMaintenanceFixture::new(mode, 0).await;
        let completed = fixture.run("worker-a", 0, fixture.now).await.unwrap();
        assert_eq!(completed.job.job_id, fixture.queued.job.job_id);
        assert_eq!(completed.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(completed.job.attempt, 1);
        assert_eq!(completed.job.max_retry_attempts, 0);
        assert_eq!(completed.job.worker_id.as_deref(), Some("worker-a"));
        assert_eq!(completed.job.scheduler_id, fixture.queued.job.scheduler_id);
        assert_eq!(completed.job.deleted_metadata_file_count, 1);
        assert_eq!(completed.cleanup_candidate_locations, fixture.queued.cleanup_candidate_locations);
        assert_eq!(
            completed.job.cleanup_watermark_unix_seconds,
            fixture.queued.job.cleanup_watermark_unix_seconds
        );
        assert!(
            !fixture
                .backend
                .object_exists("analytics", &fixture.old_metadata)
                .await
                .unwrap()
        );
        fixture.assert_report_views(&completed).await;
        let before = maintenance_test_object_versions(&fixture.backend).await;
        let replayed = fixture.run("worker-a", 0, fixture.now + Duration::seconds(1)).await.unwrap();
        assert_eq!(replayed, completed);
        assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
        assert_eq!(fixture.delete_attempts().await, 1);
        assert_eq!(
            completed
                .audit_events
                .iter()
                .filter(|event| event.action == TableMaintenanceAuditAction::WorkerStarted)
                .count(),
            1
        );
        assert_eq!(
            completed
                .audit_events
                .iter()
                .filter(|event| event.action == TableMaintenanceAuditAction::WorkerSucceeded)
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_samples_time_after_publication_wait_and_report_reads() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for expired in [false, true] {
            let mut fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
            let config = fixture
                .store
                .get_table_maintenance_config("analytics", "sales", "orders")
                .await
                .unwrap();
            let deadline = fixture.now;
            let lease_timeout = Duration::seconds(i64::try_from(config.worker_lease_timeout_seconds).unwrap());
            fixture.queued.job.scheduled_at = Some(maintenance_timestamp(deadline - lease_timeout));
            fixture.seed_report(&fixture.queued).await;
            let fixture = Arc::new(fixture);
            let before = maintenance_test_object_versions(&fixture.backend).await;
            let publication_path = default_table_bucket_publication_lock_path();
            let publication_guard = fixture
                .backend
                .acquire_write_lock("analytics", &publication_path)
                .await
                .unwrap();
            let attempts = fixture
                .backend
                .write_lock_acquisition_count("analytics", &publication_path)
                .await;
            let final_read = fixture
                .backend
                .pause_next_read(RUSTFS_META_BUCKET, &fixture.report_paths()[2])
                .await;
            let clock = Arc::new(std::sync::Mutex::new((deadline - Duration::seconds(2), 0_usize)));
            let worker_clock = Arc::clone(&clock);
            let worker_fixture = Arc::clone(&fixture);
            let worker = tokio::spawn(async move {
                let condition = TableMaintenanceWorkerCondition {
                    expected_job_id: worker_fixture.queued.job.job_id.clone(),
                    expected_attempt: 0,
                };
                let clock = move || {
                    let mut state = worker_clock.lock().unwrap();
                    state.1 += 1;
                    state.0
                };
                match &worker_fixture.store {
                    ConfiguredTableCatalogStore::ObjectBacked(store) => {
                        store
                            .run_table_metadata_maintenance_worker_once_conditionally_with_clock(
                                "analytics",
                                "sales",
                                "orders",
                                "worker-a".to_string(),
                                &condition,
                                clock,
                            )
                            .await
                    }
                }
            });
            tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
                while fixture
                    .backend
                    .write_lock_acquisition_count("analytics", &publication_path)
                    .await
                    == attempts
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("conditional worker must reach publication lock contention");
            assert!(!worker.is_finished(), "worker must wait for publication ownership");
            assert_eq!(clock.lock().unwrap().1, 0, "claim time must not precede publication ownership");
            assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
            let claim_time = if expired {
                deadline
            } else {
                deadline - Duration::nanoseconds(1)
            };
            clock.lock().unwrap().0 = claim_time;
            drop(publication_guard);
            tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, final_read.wait_started())
                .await
                .expect("conditional worker must read its final persisted report");
            assert_eq!(clock.lock().unwrap().1, 0, "claim time must follow every persisted report read");
            assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
            final_read.release();
            let result = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, worker)
                .await
                .expect("conditional worker must finish after publication and report reads")
                .unwrap();
            assert_eq!(clock.lock().unwrap().1, 1, "claim validation must sample the clock once");
            if expired {
                assert_matches!(result, Err(TableCatalogStoreError::Conflict(reason))
                    if reason == "maintenance job is not eligible for a conditional claim; inspect its status and persisted attempt budget");
                assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
                fixture.assert_report_views(&fixture.queued).await;
                assert_eq!(fixture.queued.job.status, TableMetadataMaintenanceJobStatus::Queued);
                assert_eq!(fixture.queued.job.attempt, 0);
                assert_eq!(fixture.delete_attempts().await, 0);
                assert!(
                    fixture
                        .backend
                        .object_exists("analytics", &fixture.old_metadata)
                        .await
                        .unwrap()
                );
            } else {
                let completed = result.expect("a queued lease remains claimable immediately before its deadline");
                assert_eq!(completed.job.status, TableMetadataMaintenanceJobStatus::Successful);
                assert_eq!(completed.job.attempt, 1);
                assert_eq!(completed.job.started_at, Some(maintenance_timestamp(claim_time)));
                assert_eq!(completed.job.heartbeat_at, Some(maintenance_timestamp(claim_time)));
                assert_eq!(
                    completed
                        .audit_events
                        .iter()
                        .filter(|event| event.action == TableMaintenanceAuditAction::WorkerStarted)
                        .map(|event| event.timestamp.clone())
                        .collect::<Vec<_>>(),
                    vec![maintenance_timestamp(claim_time)],
                );
                assert_eq!(completed.job.deleted_metadata_file_count, 1);
                assert_eq!(fixture.delete_attempts().await, 1);
                assert!(
                    !fixture
                        .backend
                        .object_exists("analytics", &fixture.old_metadata)
                        .await
                        .unwrap()
                );
                fixture.assert_report_views(&completed).await;
            }
        }
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_retries_failed_job_with_persisted_plan_and_budget() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        let mut fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
        let failed = fixture.fail_first_attempt().await;
        let before = maintenance_test_object_versions(&fixture.backend).await;
        assert_eq!(fixture.run("worker-a", 0, fixture.now).await.unwrap(), failed);
        assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
        let retry_at = OffsetDateTime::parse(
            failed.job.next_retry_after.as_deref().unwrap(),
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        assert_matches!(
            fixture.run("worker-b", 1, retry_at - Duration::seconds(1)).await,
            Err(TableCatalogStoreError::Conflict(_))
        );
        assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
        let mut config = fixture
            .store
            .get_table_maintenance_config("analytics", "sales", "orders")
            .await
            .unwrap();
        config.max_retry_attempts = 10;
        fixture
            .store
            .put_table_maintenance_config("analytics", "sales", "orders", config)
            .await
            .unwrap();
        fixture.store = ConfiguredTableCatalogStore::new_for_test(fixture.backend.clone(), mode);
        let completed = fixture.run("worker-b", 1, retry_at).await.unwrap();
        assert_eq!(completed.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(completed.job.job_id, failed.job.job_id);
        assert_eq!(completed.job.attempt, 2);
        assert_eq!(completed.job.max_retry_attempts, 2);
        assert_ne!(completed.job.lease_id, failed.job.lease_id);
        assert_eq!(completed.cleanup_candidate_locations, failed.cleanup_candidate_locations);
        assert_eq!(completed.deletable_metadata_locations, failed.deletable_metadata_locations);
        assert_eq!(completed.job.cleanup_watermark_unix_seconds, failed.job.cleanup_watermark_unix_seconds);
        assert_eq!(fixture.delete_attempts().await, 2);
        fixture.assert_report_views(&completed).await;
        let before = maintenance_test_object_versions(&fixture.backend).await;
        assert_eq!(fixture.run("worker-b", 1, retry_at).await.unwrap(), completed);
        assert_matches!(fixture.run("worker-a", 0, retry_at).await, Err(TableCatalogStoreError::Conflict(_)));
        assert_matches!(
            fixture
                .store
                .heartbeat_table_metadata_maintenance_job(
                    "analytics",
                    "sales",
                    "orders",
                    &failed.job.job_id,
                    &failed.job.lease_id,
                    "worker-a",
                )
                .await,
            Err(TableCatalogStoreError::Conflict(_))
        );
        assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_does_not_reset_exhausted_budget_after_reload() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for budget in [0, 1, 2] {
            let mut fixture = ConditionalMaintenanceFixture::new(mode, budget).await;
            let mut failed = fixture.fail_first_attempt().await;
            if budget == 2 {
                fixture
                    .backend
                    .fail_delete_attempt("analytics", &fixture.old_metadata, 2)
                    .await;
                let due = OffsetDateTime::parse(
                    failed.job.next_retry_after.as_deref().unwrap(),
                    &time::format_description::well_known::Rfc3339,
                )
                .unwrap();
                fixture
                    .run("worker-b", 1, due)
                    .await
                    .expect_err("second delete failure must be surfaced");
                failed = fixture.report(&failed.job.job_id).await;
                assert_eq!(failed.job.attempt, 2);
            }
            assert_eq!(failed.job.next_retry_after, None);
            let mut config = fixture
                .store
                .get_table_maintenance_config("analytics", "sales", "orders")
                .await
                .unwrap();
            config.max_retry_attempts = 10;
            fixture
                .store
                .put_table_maintenance_config("analytics", "sales", "orders", config)
                .await
                .unwrap();
            fixture.store = ConfiguredTableCatalogStore::new_for_test(fixture.backend.clone(), mode);
            let before = maintenance_test_object_versions(&fixture.backend).await;
            assert_matches!(
                fixture
                    .run("worker-c", failed.job.attempt, fixture.now + Duration::hours(1))
                    .await,
                Err(TableCatalogStoreError::Conflict(_))
            );
            assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
            assert_eq!(fixture.delete_attempts().await, usize::from(budget.max(1)));
            fixture.assert_report_views(&failed).await;
        }
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_rejects_unsafe_persisted_states_without_effects() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for scenario in [
            "job",
            "attempt",
            "expired-queued",
            "expired-running",
            "quarantine",
            "pointer",
            "generation",
            "retention",
            "paused",
            "disabled",
            "delete-disabled",
        ] {
            let fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
            let mut report = fixture.queued.clone();
            let mut attempt = 0;
            match scenario {
                "job" => report.job.job_id = "different-job".to_string(),
                "attempt" => attempt = 2,
                "expired-queued" => report.job.scheduled_at = Some(maintenance_timestamp(fixture.now - Duration::hours(1))),
                "expired-running" => {
                    report.job.status = TableMetadataMaintenanceJobStatus::Running;
                    report.job.attempt = 1;
                    report.job.worker_id = Some("worker-a".to_string());
                    report.job.lease_id = "expired-lease".to_string();
                    report.job.heartbeat_at = Some(maintenance_timestamp(fixture.now - Duration::hours(1)));
                }
                "quarantine" => report.job.quarantined_object_count = 1,
                "pointer" => report.job.current_metadata_location.push_str(".stale"),
                "generation" => report.job.current_generation += 1,
                "retention" => report.job.retain_recent_metadata_files += 1,
                "paused" | "disabled" | "delete-disabled" => {
                    let mut config = fixture
                        .store
                        .get_table_maintenance_config("analytics", "sales", "orders")
                        .await
                        .unwrap();
                    match scenario {
                        "paused" => config.worker_paused = true,
                        "disabled" => config.background_enabled = false,
                        _ => config.delete_enabled = false,
                    }
                    fixture
                        .store
                        .put_table_maintenance_config("analytics", "sales", "orders", config)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
            fixture.seed_report(&report).await;
            let before = maintenance_test_object_versions(&fixture.backend).await;
            assert_matches!(
                fixture.run("worker-a", attempt, fixture.now).await,
                Err(TableCatalogStoreError::Conflict(_)),
                "{mode:?}: {scenario}"
            );
            assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before, "{mode:?}: {scenario}");
            assert_eq!(fixture.delete_attempts().await, 0, "{mode:?}: {scenario}");
            assert_eq!(
                fixture.store.load_table("analytics", "sales", "orders").await.unwrap(),
                Some(fixture.entry)
            );
        }
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_claim_publication_failures_do_not_repeat_effects() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for target in 0..3 {
            for after_write in [false, true] {
                let fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
                let path = &fixture.report_paths()[target];
                if after_write {
                    fixture.backend.fail_after_next_put(RUSTFS_META_BUCKET, path).await;
                } else {
                    fixture.backend.fail_next_put(RUSTFS_META_BUCKET, path).await;
                }
                fixture
                    .run("worker-a", 0, fixture.now)
                    .await
                    .expect_err("claim publication fault must be surfaced");
                assert_eq!(fixture.delete_attempts().await, 0);
                let before = maintenance_test_object_versions(&fixture.backend).await;
                let replay = fixture.run("worker-a", 0, fixture.now).await;
                if target == 0 && !after_write {
                    let completed = replay.expect("unpublished claim can safely retry");
                    assert_eq!(completed.job.attempt, 1);
                    assert_eq!(completed.job.status, TableMetadataMaintenanceJobStatus::Successful);
                    assert_eq!(fixture.delete_attempts().await, 1);
                    fixture.assert_report_views(&completed).await;
                } else if target == 2 && after_write {
                    let running = replay.expect("fully published running claim can be replayed");
                    assert_eq!(running.job.attempt, 1);
                    assert_eq!(running.job.status, TableMetadataMaintenanceJobStatus::Running);
                    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
                    fixture.assert_report_views(&running).await;
                } else {
                    assert_matches!(replay, Err(TableCatalogStoreError::Conflict(_)));
                    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
                    assert_eq!(fixture.delete_attempts().await, 0);
                }
            }
        }
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_terminal_publication_faults_never_repeat_deletion() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for target in 0..3 {
            for after_write in [false, true] {
                let fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
                let path = fixture.report_paths()[target].clone();
                let attempt = fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &path).await + 2;
                if after_write {
                    fixture
                        .backend
                        .state
                        .lock()
                        .await
                        .fail_after_put_attempts
                        .entry((RUSTFS_META_BUCKET.to_string(), path))
                        .or_default()
                        .insert(attempt);
                } else {
                    fixture.backend.fail_put_attempt(RUSTFS_META_BUCKET, &path, attempt).await;
                }
                fixture
                    .run("worker-a", 0, fixture.now)
                    .await
                    .expect_err("terminal publication fault must be surfaced");
                assert_eq!(fixture.delete_attempts().await, 1);
                assert!(
                    !fixture
                        .backend
                        .object_exists("analytics", &fixture.old_metadata)
                        .await
                        .unwrap()
                );
                let before = maintenance_test_object_versions(&fixture.backend).await;
                assert_matches!(fixture.run("worker-b", 0, fixture.now).await, Err(TableCatalogStoreError::Conflict(_)));
                assert_matches!(fixture.run("worker-a", 1, fixture.now).await, Err(TableCatalogStoreError::Conflict(_)));
                assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
                let replay = fixture.run("worker-a", 0, fixture.now).await.unwrap();
                assert_eq!(replay.job.job_id, fixture.queued.job.job_id);
                assert_eq!(replay.job.attempt, 1);
                if target == 0 && !after_write {
                    assert_eq!(replay.job.status, TableMetadataMaintenanceJobStatus::Running);
                    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
                } else {
                    assert_eq!(replay.job.status, TableMetadataMaintenanceJobStatus::Successful);
                    assert_eq!(replay.job.deleted_metadata_file_count, 1);
                }
                fixture.assert_report_views(&replay).await;
                let repaired = maintenance_test_object_versions(&fixture.backend).await;
                assert_eq!(fixture.run("worker-a", 0, fixture.now).await.unwrap(), replay);
                assert_eq!(maintenance_test_object_versions(&fixture.backend).await, repaired);
                assert_eq!(fixture.delete_attempts().await, 1);
            }
        }
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_terminal_repair_requires_exact_running_predecessor() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for mismatch in ["lease", "attempt", "worker", "candidate", "audit", "terminal"] {
            let fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
            let paths = fixture.report_paths();
            let attempt = fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &paths[1]).await + 2;
            fixture.backend.fail_put_attempt(RUSTFS_META_BUCKET, &paths[1], attempt).await;
            fixture
                .run("worker-a", 0, fixture.now)
                .await
                .expect_err("latest terminal write must fail");
            let object = fixture
                .backend
                .read_object(RUSTFS_META_BUCKET, &paths[2])
                .await
                .unwrap()
                .unwrap();
            let mut running: TableMetadataMaintenanceReport = serde_json::from_slice(&object.data).unwrap();
            assert_eq!(running.job.status, TableMetadataMaintenanceJobStatus::Running);
            match mismatch {
                "lease" => running.job.lease_id = "other-lease".to_string(),
                "attempt" => running.job.attempt += 1,
                "worker" => running.job.worker_id = Some("worker-b".to_string()),
                "candidate" => running.cleanup_candidate_locations.clear(),
                "audit" => {
                    running.audit_events.pop();
                }
                "terminal" => running.job.status = TableMetadataMaintenanceJobStatus::Failed,
                _ => unreachable!(),
            }
            fixture
                .backend
                .seed_object(RUSTFS_META_BUCKET, &paths[2], serde_json::to_vec(&running).unwrap())
                .await;
            let before = maintenance_test_object_versions(&fixture.backend).await;
            assert_matches!(
                fixture.run("worker-a", 0, fixture.now).await,
                Err(TableCatalogStoreError::Conflict(_)),
                "{mode:?}: {mismatch}"
            );
            assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
            assert_eq!(fixture.delete_attempts().await, 1);
        }
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_failed_terminal_publication_can_be_repaired_and_retried() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        let fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
        let latest = fixture.report_paths()[1].clone();
        let attempt = fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &latest).await + 2;
        fixture.backend.fail_put_attempt(RUSTFS_META_BUCKET, &latest, attempt).await;
        fixture
            .backend
            .fail_delete_attempt("analytics", &fixture.old_metadata, 1)
            .await;
        fixture
            .run("worker-a", 0, fixture.now)
            .await
            .expect_err("delete and terminal publication faults must surface");
        let repaired = fixture.run("worker-a", 0, fixture.now).await.unwrap();
        assert_eq!(repaired.job.status, TableMetadataMaintenanceJobStatus::Failed);
        assert_eq!(repaired.job.attempt, 1);
        assert_eq!(fixture.delete_attempts().await, 1);
        fixture.assert_report_views(&repaired).await;
        let due = OffsetDateTime::parse(
            repaired.job.next_retry_after.as_deref().unwrap(),
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let completed = fixture.run("worker-b", 1, due).await.unwrap();
        assert_eq!(completed.job.status, TableMetadataMaintenanceJobStatus::Successful);
        assert_eq!(completed.job.attempt, 2);
        assert_eq!(fixture.delete_attempts().await, 2);
        fixture.assert_report_views(&completed).await;
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_serializes_competing_claims_and_same_worker_replays() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for same_worker in [false, true] {
            let fixture = Arc::new(ConditionalMaintenanceFixture::new(mode, 2).await);
            let second_worker = if same_worker { "worker-a" } else { "worker-b" };
            let named_path = fixture.report_paths()[0].clone();
            let before = maintenance_test_object_versions(&fixture.backend).await;
            let puts_before = fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &named_path).await;
            let pause = fixture.backend.pause_next_put(RUSTFS_META_BUCKET, &named_path).await;
            let first_fixture = Arc::clone(&fixture);
            let first = tokio::spawn(async move { first_fixture.run("worker-a", 0, first_fixture.now).await });
            let (first, second) = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
                pause.wait_started().await;
                // The first Running write has not committed: both workers must
                // contend while every durable report still describes Queued.
                let publication_path = default_table_bucket_publication_lock_path();
                let attempts = fixture
                    .backend
                    .write_lock_acquisition_count("analytics", &publication_path)
                    .await;
                let second_fixture = Arc::clone(&fixture);
                let second = tokio::spawn(async move { second_fixture.run(second_worker, 0, second_fixture.now).await });
                while fixture
                    .backend
                    .write_lock_acquisition_count("analytics", &publication_path)
                    .await
                    == attempts
                {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    fixture
                        .backend
                        .write_lock_acquisition_count("analytics", &publication_path)
                        .await,
                    attempts + 1,
                );
                assert!(!first.is_finished(), "the first claim must remain paused before publication");
                assert!(!second.is_finished(), "the competing claim must wait for publication ownership");
                assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
                assert_eq!(fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &named_path).await, puts_before + 1);
                assert_eq!(fixture.delete_attempts().await, 0);
                pause.release();
                (first.await.unwrap(), second.await.unwrap())
            })
            .await
            .expect("concurrent claims must complete without deadlock");
            let winner = if same_worker {
                let first = first.unwrap();
                assert_eq!(second.unwrap(), first);
                first
            } else {
                match (first, second) {
                    (Ok(report), Err(TableCatalogStoreError::Conflict(_)))
                    | (Err(TableCatalogStoreError::Conflict(_)), Ok(report)) => report,
                    other => panic!("exactly one competing claim must win: {other:?}"),
                }
            };
            assert_eq!(winner.job.attempt, 1);
            assert_eq!(winner.job.status, TableMetadataMaintenanceJobStatus::Successful);
            assert_eq!(fixture.delete_attempts().await, 1);
            fixture.assert_report_views(&winner).await;
        }
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_running_retry_rejects_previous_lease_and_takeover() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        let fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
        let failed = fixture.fail_first_attempt().await;
        let due = OffsetDateTime::parse(
            failed.job.next_retry_after.as_deref().unwrap(),
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let current = fixture.report_paths()[2].clone();
        fixture.backend.fail_after_next_put(RUSTFS_META_BUCKET, &current).await;
        fixture
            .run("worker-b", 1, due)
            .await
            .expect_err("response after complete Running claim is lost");
        let running = fixture.run("worker-b", 1, due).await.unwrap();
        assert_eq!(running.job.status, TableMetadataMaintenanceJobStatus::Running);
        assert_eq!(running.job.attempt, 2);
        assert_ne!(running.job.lease_id, failed.job.lease_id);
        let before = maintenance_test_object_versions(&fixture.backend).await;
        assert_matches!(
            fixture
                .store
                .heartbeat_table_metadata_maintenance_job(
                    "analytics",
                    "sales",
                    "orders",
                    &failed.job.job_id,
                    &failed.job.lease_id,
                    "worker-a",
                )
                .await,
            Err(TableCatalogStoreError::Conflict(_))
        );
        assert_matches!(fixture.run("worker-a", 1, due).await, Err(TableCatalogStoreError::Conflict(_)));
        assert_matches!(fixture.run("worker-c", 2, due).await, Err(TableCatalogStoreError::Conflict(_)));
        assert_matches!(
            fixture.run("worker-b", 1, due + Duration::hours(1)).await,
            Err(TableCatalogStoreError::Conflict(_))
        );
        assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
        assert_eq!(fixture.delete_attempts().await, 1);
        fixture.assert_report_views(&running).await;
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_rejects_invalid_conditions_before_writes() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for invalid in ["empty-worker", "overflow", "current", "latest", "../job", ""] {
            let mut fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
            let mut worker = "worker-a";
            let mut attempt = 0;
            match invalid {
                "empty-worker" => worker = " \t ",
                "overflow" => attempt = u16::MAX,
                job_id => fixture.queued.job.job_id = job_id.to_string(),
            }
            let before = maintenance_test_object_versions(&fixture.backend).await;
            assert_matches!(
                fixture.run(worker, attempt, fixture.now).await,
                Err(TableCatalogStoreError::Invalid(_)),
                "{mode:?}: {invalid}"
            );
            assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
            assert_eq!(fixture.delete_attempts().await, 0);
        }
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_requires_every_persisted_report_view() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for missing in 0..3 {
            let fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
            fixture
                .backend
                .state
                .lock()
                .await
                .objects
                .remove(&(RUSTFS_META_BUCKET.to_string(), fixture.report_paths()[missing].clone()));
            let before = maintenance_test_object_versions(&fixture.backend).await;
            assert_matches!(fixture.run("worker-a", 0, fixture.now).await, Err(TableCatalogStoreError::Conflict(_)));
            assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
            assert_eq!(fixture.delete_attempts().await, 0);
        }
    }
}

#[tokio::test]
async fn conditional_maintenance_worker_rejects_invalid_failed_retry_state() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        for invalid in [
            "missing-backoff",
            "malformed-backoff",
            "zero-attempt",
            "invalid-budget",
            "exhausted",
        ] {
            let fixture = ConditionalMaintenanceFixture::new(mode, 2).await;
            let mut failed = fixture.fail_first_attempt().await;
            match invalid {
                "missing-backoff" => failed.job.next_retry_after = None,
                "malformed-backoff" => failed.job.next_retry_after = Some("not-a-timestamp".to_string()),
                "zero-attempt" => failed.job.attempt = 0,
                "invalid-budget" => failed.job.max_retry_attempts = 11,
                "exhausted" => failed.job.attempt = failed.job.max_retry_attempts,
                _ => unreachable!(),
            }
            fixture.seed_report(&failed).await;
            let before = maintenance_test_object_versions(&fixture.backend).await;
            assert_matches!(
                fixture
                    .run("worker-b", failed.job.attempt, fixture.now + Duration::hours(1))
                    .await,
                Err(TableCatalogStoreError::Conflict(_)),
                "{mode:?}: {invalid}"
            );
            assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
            assert_eq!(fixture.delete_attempts().await, 1);
        }
    }
}

#[tokio::test]
async fn catalog_diagnostics_follows_renamed_and_custom_metadata_directories() {
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let original_table = IdentifierSegment::parse("original_orders").unwrap();
    let original_dir = default_table_metadata_dir_path(&namespace, &original_table);
    for (metadata_dir, use_uri) in [(original_dir.as_str(), false), ("tables/table-id/metadata", true)] {
        for mode in [TableCatalogBackingMode::ObjectBacked] {
            let backend = TestCatalogObjectBackend::default();
            let current = format!("{metadata_dir}/00005.metadata.json");
            let logged = format!("{metadata_dir}/00004.metadata.json");
            let pinned = format!("{metadata_dir}/00003.metadata.json");
            let unreadable = format!("{metadata_dir}/00002.metadata.json");
            let orphan = format!("{metadata_dir}/00001.metadata.json");
            let foreign = default_table_metadata_file_path(&namespace, &table, "foreign.metadata.json");
            let location = if use_uri {
                format!("s3://{bucket}/{current}")
            } else {
                current.clone()
            };
            let object_store = ObjectTableCatalogStore::new(backend.clone());
            seed_table_for_metadata_maintenance(&object_store, bucket, &namespace, &table, location).await;
            backend
                .seed_object(
                    bucket,
                    &current,
                    serde_json::to_vec(&serde_json::json!({
                        "metadata-log": [{"metadata-file": format!("s3://{bucket}/{logged}")}],
                        "current-snapshot-id": 2,
                        "refs": {"keep": {"snapshot-id": 1, "type": "tag"}}
                    }))
                    .unwrap(),
                )
                .await;
            backend.seed_object(bucket, &logged, b"{}".to_vec()).await;
            backend
                .seed_object(bucket, &pinned, br#"{"current-snapshot-id":1}"#.to_vec())
                .await;
            backend.seed_object(bucket, &unreadable, b"invalid JSON".to_vec()).await;
            backend.seed_object(bucket, &orphan, b"{}".to_vec()).await;
            backend.seed_object(bucket, &foreign, b"{}".to_vec()).await;

            let report = ConfiguredTableCatalogStore::new_for_test(backend, mode)
                .diagnose_table_catalog(bucket, "sales", "orders", 0)
                .await
                .expect("diagnostics should follow the persisted metadata directory");
            assert_eq!(report.current_metadata_status, TableMetadataPointerStatus::Valid);
            assert_eq!(report.recovery_status, TableCatalogRecoveryStatus::Healthy);
            assert_eq!(report.orphan_metadata_candidate_locations, vec![orphan]);
            assert_eq!(report.catalog.backing_manifest, report.backing_manifest);
        }
    }
}

#[tokio::test]
async fn catalog_backings_fail_closed_for_inactive_table_bucket_data_plane() {
    let bucket = "analytics";
    let object = "tables/table-id/data/file.parquet";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
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
    let object_error = table_metadata_data_plane_resource_for_object(&object_store, bucket, &metadata)
        .await
        .expect_err("metadata access must reject an inactive object-backed table bucket");
    assert_matches!(
        object_error,
        TableCatalogStoreError::Internal(message) if message.contains("inactive object-backed catalog entry")
    );
}

#[tokio::test]
async fn catalog_backings_reject_table_view_identifier_collisions() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
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
    for mode in [TableCatalogBackingMode::ObjectBacked] {
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
    for mode in [TableCatalogBackingMode::ObjectBacked] {
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
async fn catalog_backings_reject_metadata_changed_after_validation() {
    let object_backend = TestCatalogObjectBackend::default();
    assert_catalog_rejects_metadata_changed_after_validation(
        &ObjectTableCatalogStore::new(object_backend.clone()),
        &object_backend,
        "object-catalog",
    )
    .await;
}

#[tokio::test]
async fn namespace_properties_load_legacy_values_but_reject_oversized_writes() {
    let backend = TestCatalogObjectBackend::default();
    let object_store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    object_store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("object-backed bucket should be created");

    let mut oversized = test_namespace_entry(bucket, &namespace);
    oversized
        .properties
        .insert("owner".to_string(), "x".repeat(NAMESPACE_PROPERTY_VALUE_MAX_LEN + 1));
    assert_matches!(
        object_store.create_namespace(oversized.clone()).await,
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
}

#[tokio::test]
async fn catalog_backings_page_direct_namespace_children_with_scoped_cursors() {
    assert_direct_namespace_child_contract(
        &ObjectTableCatalogStore::new(TestCatalogObjectBackend::default()),
        "object-catalog",
        OBJECT_CATALOG_LIST_CURSOR_PREFIX,
    )
    .await;
}

#[tokio::test]
async fn catalog_backings_keep_implicit_parents_implicit_during_resource_creation() {
    let bucket = "analytics";
    let object_store = ObjectTableCatalogStore::new(TestCatalogObjectBackend::default());
    create_resources_in_implicit_parents(&object_store, bucket).await;
}

#[tokio::test]
async fn catalog_backings_reject_resource_creation_in_inactive_namespace() {
    assert_inactive_namespace_rejects_resource_creation(
        &ObjectTableCatalogStore::new(TestCatalogObjectBackend::default()),
        "object-catalog",
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
}

#[tokio::test]
async fn object_catalog_table_rename_preserves_identity_index_and_reuses_source_tombstone() {
    let backend = TestCatalogObjectBackend {
        content_addressed_etags: true,
        ..Default::default()
    };
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let source_namespace = Namespace::parse("sales").expect("source namespace should parse");
    let destination_namespace = Namespace::parse("curated").expect("destination namespace should parse");
    let source_table = IdentifierSegment::parse("orders").expect("source table should parse");
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &source_namespace))
        .await
        .unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &destination_namespace))
        .await
        .unwrap();
    let source = test_table_entry(
        bucket,
        &source_namespace,
        &source_table,
        default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json"),
    );
    store.create_table(source.clone()).await.unwrap();
    let bucket_object = store.paths.table_bucket_entry_path(bucket);
    let bucket_etag_before = store
        .read_entry::<TableBucketEntry>(RUSTFS_META_BUCKET, &bucket_object)
        .await
        .unwrap()
        .unwrap()
        .1
        .expect("table bucket should have an etag");

    store
        .rename_table(bucket, "sales", "orders", "curated", "orders_v2")
        .await
        .expect("object-backed table rename should complete");
    let bucket_etag_after = store
        .read_entry::<TableBucketEntry>(RUSTFS_META_BUCKET, &bucket_object)
        .await
        .unwrap()
        .unwrap()
        .1
        .expect("table bucket should have an etag");
    assert_ne!(bucket_etag_after, bucket_etag_before);

    assert!(store.load_table(bucket, "sales", "orders").await.unwrap().is_none());
    let destination = store
        .load_table(bucket, "curated", "orders_v2")
        .await
        .unwrap()
        .expect("destination table should exist");
    let mut expected_destination = source.clone();
    expected_destination.namespace = "curated".to_string();
    expected_destination.table = "orders_v2".to_string();
    assert_ne!(destination.updated_at, source.updated_at);
    expected_destination.updated_at.clone_from(&destination.updated_at);
    assert_eq!(destination, expected_destination);

    let source_object = store.paths.table_entry_path(bucket, &source_namespace, &source_table);
    let source_tombstone = store
        .read_entry::<TableEntry>(RUSTFS_META_BUCKET, &source_object)
        .await
        .unwrap()
        .expect("source tombstone should remain")
        .0;
    assert_eq!(source_tombstone.state, TableCatalogEntryState::Deleted);
    assert_eq!(source_tombstone.table_id, destination.table_id);
    assert!(
        store
            .get_table_bucket(bucket)
            .await
            .unwrap()
            .expect("table bucket should exist")
            .active_rename_id
            .is_none()
    );

    let destination_index = table_warehouse_index_entry(&destination).unwrap();
    let index_object = store
        .paths
        .warehouse_index_entry_path(bucket, &destination_index.warehouse_object_prefix);
    let persisted_index = store
        .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_object)
        .await
        .unwrap()
        .expect("warehouse index should exist")
        .0;
    assert_eq!(persisted_index, destination_index);
    let resource = store
        .resolve_table_data_plane_resource(bucket, "tables/table-id/data/part.parquet")
        .await
        .unwrap()
        .expect("renamed table should resolve its stable warehouse prefix");
    assert_eq!(resource.namespace, "curated");
    assert_eq!(resource.table, "orders_v2");
    store
        .backfill_table_warehouse_index(bucket)
        .await
        .expect("retained source tombstone should not make the warehouse index ambiguous");

    store
        .rename_table(bucket, "curated", "orders_v2", "sales", "orders")
        .await
        .expect("rename should conditionally replace the retained source tombstone");
    store
        .rename_table(bucket, "sales", "orders", "curated", "orders_v2")
        .await
        .expect("rename should conditionally replace a destination tombstone");

    let mut replacement = test_table_entry(
        bucket,
        &source_namespace,
        &source_table,
        default_table_metadata_file_path(&source_namespace, &source_table, "00002.metadata.json"),
    );
    replacement.table_id = "replacement-table-id".to_string();
    replacement.table_uuid = "replacement-table-uuid".to_string();
    replacement.warehouse_location = "s3://analytics/tables/replacement-table-id".to_string();
    store
        .create_table(replacement.clone())
        .await
        .expect("create should conditionally replace the source tombstone");
    assert_eq!(
        store
            .load_table(bucket, "sales", "orders")
            .await
            .unwrap()
            .expect("source identifier should be reusable"),
        replacement
    );
}

#[tokio::test]
async fn object_catalog_table_rename_recovers_after_destination_publish_and_fences_concurrent_mutations() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let source_namespace = Namespace::parse("sales").expect("source namespace should parse");
    let destination_namespace = Namespace::parse("curated").expect("destination namespace should parse");
    let source_table = IdentifierSegment::parse("orders").expect("source table should parse");
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &source_namespace))
        .await
        .unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &destination_namespace))
        .await
        .unwrap();
    let source = test_table_entry(
        bucket,
        &source_namespace,
        &source_table,
        default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json"),
    );
    store.create_table(source.clone()).await.unwrap();

    let source_object = store.paths.table_entry_path(bucket, &source_namespace, &source_table);
    let source_tombstone_attempt = backend.put_attempt_count(RUSTFS_META_BUCKET, &source_object).await + 2;
    let source_tombstone_pause = backend
        .pause_put_attempt(RUSTFS_META_BUCKET, &source_object, source_tombstone_attempt)
        .await;
    let rename_store = store.clone();
    let rename = tokio::spawn(async move {
        rename_store
            .rename_table(bucket, "sales", "orders", "curated", "orders_v2")
            .await
    });
    source_tombstone_pause.wait_started().await;

    let active_rename_id = store
        .get_table_bucket(bucket)
        .await
        .unwrap()
        .expect("table bucket should exist")
        .active_rename_id
        .expect("rename fence should be durable before destination publication");
    assert_matches!(
        store.load_table(bucket, "sales", "orders").await,
        Err(TableCatalogStoreError::Unavailable(_))
    );
    assert_matches!(store.list_tables(bucket, "sales").await, Err(TableCatalogStoreError::Unavailable(_)));
    assert_matches!(
        store
            .resolve_table_data_plane_resource(bucket, "tables/table-id/data/part.parquet")
            .await,
        Err(TableCatalogStoreError::Unavailable(_))
    );

    let publication_lock = default_table_bucket_publication_lock_path();
    let publication_attempts = backend.write_lock_acquisition_count(bucket, &publication_lock).await;
    let commit_store = store.clone();
    let commit = tokio::spawn(async move {
        commit_store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
                commit_id: "concurrent-commit".to_string(),
                idempotency_key: None,
                operation: "append".to_string(),
                expected_version_token: source.version_token,
                expected_metadata_location: source.metadata_location,
                new_metadata_location: "unused.metadata.json".to_string(),
                requirements: Vec::new(),
                writer: Some("rename-test".to_string()),
            })
            .await
    });
    let drop_store = store.clone();
    let drop_table = tokio::spawn(async move { drop_store.drop_table(bucket, "sales", "orders").await });
    let create_store = store.clone();
    let mut replacement = test_table_entry(
        bucket,
        &source_namespace,
        &source_table,
        default_table_metadata_file_path(&source_namespace, &source_table, "00002.metadata.json"),
    );
    replacement.table_id = "replacement-table-id".to_string();
    replacement.table_uuid = "replacement-table-uuid".to_string();
    replacement.warehouse_location = "s3://analytics/tables/replacement-table-id".to_string();
    let create = tokio::spawn(async move { create_store.create_table(replacement).await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.write_lock_acquisition_count(bucket, &publication_lock).await < publication_attempts + 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("concurrent mutations should reach the table-bucket publication fence");
    assert!(!commit.is_finished());
    assert!(!drop_table.is_finished());
    assert!(!create.is_finished());
    commit.abort();
    drop_table.abort();
    create.abort();

    rename.abort();
    source_tombstone_pause.release();
    let _ = rename.await;
    let destination_object = store.paths.table_entry_path(
        bucket,
        &destination_namespace,
        &IdentifierSegment::parse("orders_v2").expect("destination table should parse"),
    );
    let source_fence = store
        .read_entry::<TableEntry>(RUSTFS_META_BUCKET, &source_object)
        .await
        .unwrap()
        .expect("source rename fence should be durable")
        .0;
    let destination_fence = store
        .read_entry::<TableEntry>(RUSTFS_META_BUCKET, &destination_object)
        .await
        .unwrap()
        .expect("destination rename fence should be durable")
        .0;
    assert_eq!(source_fence.state, TableCatalogEntryState::Renaming);
    assert_eq!(destination_fence.state, TableCatalogEntryState::Renaming);
    assert_matches!(
        store.load_table(bucket, "curated", "orders_v2").await,
        Err(TableCatalogStoreError::Unavailable(_))
    );
    store
        .rename_table(bucket, "sales", "orders", "curated", "orders_v2")
        .await
        .expect("retrying the interrupted rename must return its recovered success");

    assert!(store.load_table(bucket, "sales", "orders").await.unwrap().is_none());
    let destination = store
        .load_table(bucket, "curated", "orders_v2")
        .await
        .unwrap()
        .expect("recovery should finish the destination publication");
    assert_eq!(destination.table_id, "table-id");
    assert!(
        store
            .get_table_bucket(bucket)
            .await
            .unwrap()
            .expect("table bucket should exist")
            .active_rename_id
            .is_none()
    );
    let intent_object = store.paths.table_rename_intent_path(bucket, &active_rename_id);
    let intent = store
        .read_entry::<TableRenameIntent>(RUSTFS_META_BUCKET, &intent_object)
        .await
        .unwrap()
        .expect("completed rename intent should be retained as a recovery record")
        .0;
    assert_eq!(intent.state, TableRenameIntentState::Completed);
}

#[tokio::test]
async fn disable_empty_table_bucket_supports_both_catalog_backings() {
    assert_disable_empty_table_bucket(&ObjectTableCatalogStore::new(TestCatalogObjectBackend::default())).await;
}

#[tokio::test]
async fn disable_empty_table_bucket_survives_restart_and_blocks_stale_initialization() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        let backend = TestCatalogObjectBackend::default();
        let store = ConfiguredTableCatalogStore::new_for_test(backend.clone(), mode);
        // Cover an interrupted enable that wrote the bucket marker but no entry.
        store.disable_empty_table_bucket(test_bucket_entry("disabled")).await.unwrap();
        let restarted = ConfiguredTableCatalogStore::new_for_test(backend, mode);
        assert_eq!(
            restarted.get_table_bucket("disabled").await.unwrap().unwrap().state,
            TableCatalogEntryState::Deleted
        );
        assert_matches!(
            restarted.ensure_table_bucket(test_bucket_entry("disabled")).await,
            Err(TableCatalogStoreError::NotFound(_))
        );
    }
}

#[tokio::test]
async fn disable_empty_table_bucket_serializes_with_namespace_creation() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        let store = ConfiguredTableCatalogStore::new_for_test(TestCatalogObjectBackend::default(), mode);
        let bucket = "disable-race";
        let namespace = Namespace::parse("sales").unwrap();
        store.ensure_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        let (create, disable) = tokio::join!(
            store.create_namespace(test_namespace_entry(bucket, &namespace)),
            store.disable_empty_table_bucket(test_bucket_entry(bucket)),
        );
        assert_ne!(create.is_ok(), disable.is_ok());
        if disable.is_ok() {
            assert!(store.list_namespaces(bucket).await.unwrap().is_empty());
        } else {
            assert_eq!(
                store.get_table_bucket(bucket).await.unwrap().unwrap().state,
                TableCatalogEntryState::Active
            );
        }
    }
}

#[tokio::test]
async fn disable_empty_table_bucket_failure_keeps_active_catalog() {
    for mode in [TableCatalogBackingMode::ObjectBacked] {
        let backend = TestCatalogObjectBackend::default();
        let store = ConfiguredTableCatalogStore::new_for_test(backend.clone(), mode);
        let bucket = "disable-failure";
        store.ensure_table_bucket(test_bucket_entry(bucket)).await.unwrap();
        let path = match mode {
            TableCatalogBackingMode::ObjectBacked => TableCatalogObjectPaths::default().table_bucket_entry_path(bucket),
        };
        *backend.fail_put_object_path.lock().await = Some(path);
        assert!(store.disable_empty_table_bucket(test_bucket_entry(bucket)).await.is_err());
        let restarted = ConfiguredTableCatalogStore::new_for_test(backend, mode);
        assert_eq!(
            restarted.get_table_bucket(bucket).await.unwrap().unwrap().state,
            TableCatalogEntryState::Active
        );
        restarted.disable_empty_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    }
}

#[test]
fn table_data_plane_index_miss_scan_rejects_catalogs_above_its_object_limit() {
    let objects = vec![
        "catalog/table-1/table-entry.json".to_string(),
        "catalog/namespace.json".to_string(),
        "catalog/table-2/table-entry.json".to_string(),
    ];

    // Only table entries count toward the limit; the namespace object is free.
    assert_eq!(
        bounded_table_entry_objects_for_data_plane_scan(objects.clone(), false, 2).unwrap(),
        vec![
            "catalog/table-1/table-entry.json".to_string(),
            "catalog/table-2/table-entry.json".to_string(),
        ]
    );
    assert_matches!(
        bounded_table_entry_objects_for_data_plane_scan(objects, false, 1),
        Err(TableCatalogStoreError::Unavailable(message)) if message.contains("1-catalog-object safety limit")
    );
    assert_matches!(
        bounded_table_entry_objects_for_data_plane_scan(vec!["catalog/table-entry.json".to_string()], true, 1),
        Err(TableCatalogStoreError::Unavailable(message)) if message.contains("1-catalog-object safety limit")
    );
}

#[test]
fn catalog_lock_authority_failures_are_typed_as_unavailable() {
    for error in [
        rustfs_lock::LockError::timeout("table-publication", StdDuration::from_secs(5)),
        rustfs_lock::LockError::Network {
            message: "peer unavailable".to_string(),
            source: Box::new(std::io::Error::other("peer unavailable")),
        },
        rustfs_lock::LockError::AlreadyLocked {
            resource: "table-publication".to_string(),
            owner: "another-node".to_string(),
        },
        rustfs_lock::LockError::InsufficientNodes {
            required: 3,
            available: 1,
        },
        rustfs_lock::LockError::QuorumNotReached {
            required: 3,
            achieved: 1,
        },
        rustfs_lock::LockError::QueueFull {
            message: "lock queue is full".to_string(),
        },
    ] {
        assert_matches!(
            super::store::catalog_lock_acquisition_error("acquire catalog table lock", error),
            TableCatalogStoreError::LockUnavailable { .. }
        );
    }

    assert_matches!(
        super::store::catalog_lock_acquisition_error(
            "acquire catalog table lock",
            rustfs_lock::LockError::configuration("invalid lock configuration"),
        ),
        TableCatalogStoreError::Internal(_)
    );
}

#[test]
fn catalog_storage_quorum_failures_are_typed_as_unavailable() {
    for error in [
        StorageError::ErasureReadQuorum,
        StorageError::ErasureWriteQuorum,
        StorageError::InsufficientReadQuorum(".rustfs.sys".to_string(), "snapshot.json".to_string()),
        StorageError::InsufficientWriteQuorum(".rustfs.sys".to_string(), "snapshot.json".to_string()),
        StorageError::NamespaceLockQuorumUnavailable {
            mode: "read",
            bucket: ".rustfs.sys".to_string(),
            object: "s3tables/catalog/catalog-state/snapshot.json".to_string(),
            required: 2,
            achieved: 0,
        },
    ] {
        assert_matches!(
            storage_error_to_catalog("stat catalog object", error),
            TableCatalogStoreError::Unavailable(_)
        );
    }

    assert_matches!(
        storage_error_to_catalog("stat catalog object", StorageError::FileCorrupt),
        TableCatalogStoreError::Internal(_)
    );
}

#[test]
fn catalog_storage_retryable_lock_failures_are_typed_as_unavailable() {
    for error in [
        rustfs_lock::LockError::timeout("catalog", StdDuration::from_secs(5)),
        rustfs_lock::LockError::network("peer unavailable", std::io::Error::other("peer unavailable")),
        rustfs_lock::LockError::already_locked("catalog", "another-node"),
        rustfs_lock::LockError::InsufficientNodes {
            required: 3,
            available: 1,
        },
        rustfs_lock::LockError::QuorumNotReached {
            required: 3,
            achieved: 1,
        },
        rustfs_lock::LockError::QueueFull {
            message: "lock queue is full".to_string(),
        },
    ] {
        assert_matches!(
            storage_error_to_catalog("stat catalog object", StorageError::Lock(error)),
            TableCatalogStoreError::LockUnavailable { .. }
        );
    }

    for error in [
        rustfs_lock::LockError::internal("lock backend failure"),
        rustfs_lock::LockError::configuration("invalid lock configuration"),
        rustfs_lock::LockError::resource_not_found("catalog"),
    ] {
        assert_matches!(
            storage_error_to_catalog("stat catalog object", StorageError::Lock(error)),
            TableCatalogStoreError::Internal(_)
        );
    }
}

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
        active_rename_id: None,
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

fn table_metadata_json_for_backup(bucket: &str, table_id: &str, table_uuid: &str) -> serde_json::Value {
    let mut metadata = table_metadata_json_for_validation();
    metadata["table-uuid"] = serde_json::Value::String(table_uuid.to_string());
    metadata["location"] = serde_json::Value::String(format!("s3://{bucket}/tables/{table_id}"));
    metadata
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "schema field 1 has an incompatible type evolution"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "schema field 1 cannot be reused after removal"
    );

    let mut stale_last_column_id = table_metadata_json_for_validation();
    stale_last_column_id["last-column-id"] = serde_json::Value::from(0);
    let error = validate_supported_table_metadata(&stale_last_column_id)
        .expect_err("last-column-id must cover nested and top-level assigned field IDs");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "last-column-id must be non-negative and cover every assigned schema field id"
    );
}

#[test]
fn iceberg_metadata_transition_preserves_assignment_watermarks_and_schema_history() {
    let current = table_metadata_json_for_validation();

    let mut lower_column_watermark = current.clone();
    lower_column_watermark["last-column-id"] = serde_json::Value::from(0);
    let error = validate_table_metadata_transition(&current, &lower_column_watermark)
        .expect_err("last-column-id must not decrease across commits");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "last-column-id must not decrease across table metadata commits"
    );

    let mut lower_partition_watermark = current.clone();
    lower_partition_watermark["last-partition-id"] = serde_json::Value::from(998);
    let error = validate_table_metadata_transition(&current, &lower_partition_watermark)
        .expect_err("last-partition-id must not decrease across commits");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "last-partition-id must not decrease across table metadata commits"
    );

    let mut current_sequence = current.clone();
    current_sequence["last-sequence-number"] = serde_json::Value::from(2);
    let mut lower_sequence_watermark = current_sequence.clone();
    lower_sequence_watermark["last-sequence-number"] = serde_json::Value::from(1);
    let error = validate_table_metadata_transition(&current_sequence, &lower_sequence_watermark)
        .expect_err("last-sequence-number must not decrease across commits");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "last-sequence-number must not decrease across table metadata commits"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "existing partition spec 0 must not be modified"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "existing sort order 1 must not be modified"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "existing snapshot 10 must not be modified"
    );

    let mut modified_history = current.clone();
    modified_history["schemas"][0]["fields"][0]["type"] = serde_json::Value::from("string");
    let error = validate_table_metadata_transition(&current, &modified_history)
        .expect_err("published schema definitions must be immutable");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "existing schema 0 must not be modified"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "schema field 1 cannot reuse a previously assigned field id"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "statistics contains duplicate entries for snapshot 10"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "statistics object count exceeds the commit limit"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "statistics bytes exceed the commit validation limit"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "statistics object is outside the table warehouse"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "table statistics object is not a Puffin file"
    );
}

#[tokio::test]
async fn snapshot_validation_accepts_non_parquet_partition_statistics() {
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
    let statistics = b"ORC".to_vec();
    target["partition-statistics"] = serde_json::json!([{
        "snapshot-id": 10,
        "statistics-path": "s3://warehouse/tables/table-id/metadata/partition-stats.parquet",
        "file-size-in-bytes": statistics.len()
    }]);
    backend
        .seed_object("warehouse", "tables/table-id/metadata/partition-stats.parquet", statistics)
        .await;
    let context = TableSnapshotGraphValidationContext::new(&backend, "warehouse", &entry);

    validate_table_snapshot_changes(&context, Some(&current), &target)
        .await
        .expect("Iceberg partition statistics are not restricted to Parquet");

    backend
        .seed_object("warehouse", "tables/table-id/metadata/partition-stats.parquet", b"BAD".to_vec())
        .await;
    let error = validate_table_snapshot_changes(&context, Some(&current), &target)
        .await
        .expect_err("partition statistics must use a supported table data-file envelope");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "partition statistics object is not a supported table data file"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "statistics file-size-in-bytes does not match the object"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "Iceberg v2 snapshot sequence-number must be between zero and last-sequence-number"
    );
    v2["last-sequence-number"] = serde_json::Value::from(0);
    v2["snapshots"][0]
        .as_object_mut()
        .expect("snapshot should be an object")
        .remove("sequence-number");
    let error = validate_supported_table_metadata(&v2).expect_err("Iceberg v2 snapshots must include sequence-number");
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "Iceberg v2 snapshot sequence-number is required"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "Iceberg v1 snapshot sequence-number must be zero when present"
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
        metadata_log_locations_matching(&metadata, "warehouse", |location| {
            is_valid_table_metadata_location(&namespace, &table, location)
        }),
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "snapshot manifest references missing partition spec 7"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "snapshot manifest references missing partition spec 7"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "snapshot manifest-list object is missing"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "manifest-list manifest_length does not match the manifest object"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "manifest partition-spec-id does not match its manifest-list entry"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "snapshot manifest traversal count exceeds the commit limit"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "new Iceberg v2 snapshots require manifest-list"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "manifest-list content does not match manifest file content"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "manifest-list manifest_length does not match the manifest object"
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
    assert_matches!(
        error,
        TableCatalogStoreError::Invalid(message) if message == "manifest partition-spec-id does not match its manifest-list entry"
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

pub(super) fn test_bucket_entry(bucket: &str) -> TableBucketEntry {
    TableBucketEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        catalog_type: TABLE_BUCKET_CATALOG_TYPE.to_string(),
        warehouse_root: format!("s3://{bucket}/"),
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        active_rename_id: None,
        created_at: None,
        updated_at: None,
    }
}

pub(super) fn test_namespace_entry(bucket: &str, namespace: &Namespace) -> NamespaceEntry {
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

pub(super) fn test_table_entry(
    bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    metadata_location: String,
) -> TableEntry {
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
async fn object_catalog_backup_is_idempotent_and_restorable() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_location = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    backend
        .seed_object(
            bucket,
            &metadata_location,
            serde_json::to_vec(&table_metadata_json_for_backup(bucket, "table-id", "table-uuid")).unwrap(),
        )
        .await;
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, metadata_location))
        .await
        .unwrap();

    let first = store.create_object_catalog_backup(bucket, None).await.unwrap();
    let second = store
        .create_object_catalog_backup(bucket, Some(&first.source_snapshot_etag))
        .await
        .unwrap();
    assert_eq!(first.backup_id, second.backup_id);
    assert_eq!(second.status, TableCatalogBackupStatus::AlreadyPresent);
    assert!(first.object_count >= 3);

    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .unwrap();
    store.drop_namespace(bucket, &namespace.public_name()).await.unwrap();
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .unwrap()
            .is_none()
    );

    let table_entry_path = TableCatalogObjectPaths::default().table_entry_path(bucket, &namespace, &table);
    backend.fail_next_put(RUSTFS_META_BUCKET, &table_entry_path).await;
    let interrupted_visibility = store
        .restore_object_catalog_backup_with_id(bucket, &first.backup_id, None, true, Some("first-restore"))
        .await
        .expect_err("restore should fail before publishing the table entry");
    assert!(
        matches!(interrupted_visibility, TableCatalogStoreError::Internal(message) if message.contains("injected put failure"))
    );
    assert!(matches!(
        store.load_table(bucket, &namespace.public_name(), table.as_str()).await,
        Err(TableCatalogStoreError::Unavailable(_))
    ));
    store
        .restore_object_catalog_backup_with_id(bucket, &first.backup_id, None, true, Some("first-restore"))
        .await
        .expect("retrying after a pre-publication interruption should converge");
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .unwrap()
            .is_some()
    );
    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .unwrap();
    store.drop_namespace(bucket, &namespace.public_name()).await.unwrap();

    backend.fail_after_next_put(RUSTFS_META_BUCKET, &table_entry_path).await;
    let interrupted = store
        .restore_object_catalog_backup_with_id(bucket, &first.backup_id, None, true, Some("second-restore"))
        .await
        .expect_err("restore should surface an ambiguous post-write interruption");
    assert!(
        matches!(interrupted, TableCatalogStoreError::Internal(message) if message.contains("injected post-commit put failure"))
    );

    let restored = store
        .restore_object_catalog_backup_with_id(bucket, &first.backup_id, None, true, Some("second-restore"))
        .await
        .expect("retrying an interrupted restore should converge");
    assert_eq!(restored.status, TableCatalogRestoreStatus::Restored);
    assert!(store.get_namespace(bucket, &namespace.public_name()).await.unwrap().is_some());
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .unwrap()
            .is_some()
    );

    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .unwrap();
    store.drop_namespace(bucket, &namespace.public_name()).await.unwrap();
    let table_root = TableCatalogObjectPaths::default()
        .table_bucket_entry_path(bucket)
        .rsplit_once('/')
        .map(|(prefix, _)| prefix.to_string())
        .expect("table bucket entry path should have a parent");
    let extra_path = format!("{table_root}/stale-object");
    backend.seed_object(RUSTFS_META_BUCKET, &extra_path, b"stale".to_vec()).await;
    backend.fail_after_next_delete(RUSTFS_META_BUCKET, &extra_path).await;
    let interrupted_delete = store
        .restore_object_catalog_backup_with_id(bucket, &first.backup_id, None, true, Some("third-restore"))
        .await
        .expect_err("restore should surface an ambiguous post-delete interruption");
    assert!(
        matches!(interrupted_delete, TableCatalogStoreError::Internal(message) if message.contains("injected post-commit delete failure"))
    );
    store
        .restore_object_catalog_backup_with_id(bucket, &first.backup_id, None, true, Some("third-restore"))
        .await
        .expect("retrying after interrupted replacement cleanup should converge");
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .unwrap()
            .is_some()
    );
    assert!(backend.read_object(RUSTFS_META_BUCKET, &extra_path).await.unwrap().is_none());
}

#[tokio::test]
async fn object_catalog_backup_restores_into_an_isolated_backend() {
    let source_backend = TestCatalogObjectBackend::default();
    let source_store = ObjectTableCatalogStore::new(source_backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_location = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    source_backend
        .seed_object(
            bucket,
            &metadata_location,
            serde_json::to_vec(&table_metadata_json_for_backup(bucket, "table-id", "table-uuid")).unwrap(),
        )
        .await;
    source_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    source_store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    source_store
        .create_table(test_table_entry(bucket, &namespace, &table, metadata_location))
        .await
        .unwrap();
    let backup = source_store.create_object_catalog_backup(bucket, None).await.unwrap();
    let backup_object = source_backend
        .read_object(RUSTFS_META_BUCKET, &backup.backup_path)
        .await
        .unwrap()
        .expect("source backup artifact should be readable");

    let target_backend = TestCatalogObjectBackend::default();
    target_backend
        .seed_object(RUSTFS_META_BUCKET, &backup.backup_path, backup_object.data)
        .await;
    let source_objects = source_backend.state.lock().await.objects.clone();
    for ((object_bucket, object), record) in source_objects {
        if object_bucket == bucket && !object.starts_with("tables/") {
            target_backend.seed_object(&object_bucket, &object, record.data).await;
        }
    }

    let target_store = ObjectTableCatalogStore::new(target_backend.clone());
    let restored = target_store
        .restore_object_catalog_backup(bucket, &backup.backup_id, None, false)
        .await
        .unwrap();
    assert_eq!(restored.status, TableCatalogRestoreStatus::Restored);
    assert!(
        target_store
            .get_namespace(bucket, &namespace.public_name())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        target_store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .unwrap()
            .is_some()
    );

    let repeated = target_store
        .restore_object_catalog_backup(bucket, &backup.backup_id, None, false)
        .await
        .unwrap();
    assert_eq!(repeated.status, TableCatalogRestoreStatus::AlreadyRestored);

    let target_entry_path = TableCatalogObjectPaths::default().table_bucket_entry_path(bucket);
    target_backend
        .seed_object(RUSTFS_META_BUCKET, &target_entry_path, b"changed-target".to_vec())
        .await;
    let error = target_store
        .restore_object_catalog_backup_with_id(bucket, &backup.backup_id, None, false, Some("new-restore"))
        .await
        .expect_err("restore must reject a changed target object without allow-replace");
    assert!(matches!(error, TableCatalogStoreError::Conflict(message) if message.contains("target object changed")));
    let target_object = target_backend
        .read_object(RUSTFS_META_BUCKET, &target_entry_path)
        .await
        .unwrap()
        .expect("changed target object should remain present");
    assert_eq!(target_object.data, b"changed-target");
}

#[tokio::test]
async fn object_catalog_backup_rejects_missing_current_metadata() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_location = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, metadata_location))
        .await
        .unwrap();
    let error = store.create_object_catalog_backup(bucket, None).await.unwrap_err();
    assert_matches!(error, TableCatalogStoreError::NotFound(message) if message.contains("backup object disappeared"));
}

#[tokio::test]
async fn object_catalog_backup_rejects_missing_snapshot_graph_object() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_location = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    backend.seed_object(bucket, &metadata_location, b"{}".to_vec()).await;
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, metadata_location.clone()))
        .await
        .unwrap();

    let mut metadata = table_metadata_json_for_backup(bucket, "table-id", "table-uuid");
    metadata["last-sequence-number"] = serde_json::Value::from(1);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": format!("s3://{bucket}/tables/table-id/metadata/snap-10.avro"),
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    backend
        .seed_object(bucket, &metadata_location, serde_json::to_vec(&metadata).unwrap())
        .await;

    let error = store
        .create_object_catalog_backup(bucket, None)
        .await
        .expect_err("backup must fail closed when the Iceberg snapshot graph is incomplete");
    assert!(matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("manifest-list object is missing")));
}

#[tokio::test]
async fn object_catalog_restore_rejects_missing_snapshot_data_before_catalog_write() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_location = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let manifest_list_location = default_table_metadata_file_path(&namespace, &table, "snap-10.avro");
    let manifest_location = default_table_metadata_file_path(&namespace, &table, "manifest-10.avro");
    let data_location = "tables/table-id/data/part-00001.parquet";
    let manifest_list_uri = format!("s3://{bucket}/{manifest_location}");
    let data_uri = format!("s3://{bucket}/{data_location}");
    let manifest = manifest_avro_bytes_with_status(&[(&data_uri, 0, 1)]);
    let manifest_list = manifest_list_avro_bytes_with_spec_and_content(&[(&manifest_list_uri, manifest.len())], 0, 0);

    backend
        .seed_object(
            bucket,
            &metadata_location,
            serde_json::to_vec(&table_metadata_json_for_backup(bucket, "table-id", "table-uuid")).unwrap(),
        )
        .await;
    backend.seed_object(bucket, &manifest_list_location, manifest_list).await;
    backend.seed_object(bucket, &manifest_location, manifest).await;
    backend.seed_object(bucket, data_location, b"row".to_vec()).await;
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(bucket, &namespace, &table, metadata_location.clone()))
        .await
        .unwrap();

    let mut metadata = table_metadata_json_for_backup(bucket, "table-id", "table-uuid");
    metadata["last-sequence-number"] = serde_json::Value::from(20);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 20,
        "timestamp-ms": 1,
        "manifest-list": format!("s3://{bucket}/{manifest_list_location}"),
        "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    backend
        .seed_object(bucket, &metadata_location, serde_json::to_vec(&metadata).unwrap())
        .await;

    let backup = store.create_object_catalog_backup(bucket, None).await.unwrap();
    backend.delete_object(bucket, data_location).await.unwrap();
    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .unwrap();
    store.drop_namespace(bucket, &namespace.public_name()).await.unwrap();

    let error = store
        .restore_object_catalog_backup(bucket, &backup.backup_id, None, true)
        .await
        .expect_err("restore must reject a missing referenced data object before catalog writes");
    assert!(
        matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("manifest referenced data file is missing"))
    );
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .unwrap()
            .is_none()
    );
}

async fn maintenance_test_object_versions(backend: &TestCatalogObjectBackend) -> BTreeMap<(String, String), TableCatalogObject> {
    backend
        .state
        .lock()
        .await
        .objects
        .iter()
        .map(|(key, object)| {
            (
                key.clone(),
                TableCatalogObject {
                    data: object.data.clone(),
                    body_revision: object.body_revision,
                    etag: Some(object.etag.clone()),
                    mod_time: object.mod_time,
                },
            )
        })
        .collect()
}

fn rename_maintenance_root(entry: &TableEntry) -> String {
    let config = TableCatalogObjectPaths::default().table_maintenance_config_path(
        &entry.table_bucket,
        &Namespace::parse(&entry.namespace).unwrap(),
        &IdentifierSegment::parse(&entry.table).unwrap(),
        &entry.table_id,
    );
    config.strip_suffix(MAINTENANCE_CONFIG_FILE).unwrap().to_string()
}

fn rename_maintenance_intent_path(bucket: &str) -> String {
    let entry_path = TableCatalogObjectPaths::default().table_bucket_entry_path(bucket);
    format!(
        "{}maintenance-rename-intent.json",
        entry_path.strip_suffix(TABLE_BUCKET_ENTRY_FILE).unwrap()
    )
}

async fn rename_maintenance_objects(
    backend: &TestCatalogObjectBackend,
    entry: &TableEntry,
) -> BTreeMap<String, serde_json::Value> {
    let prefix = rename_maintenance_root(entry);
    backend
        .state
        .lock()
        .await
        .objects
        .iter()
        .filter_map(|((bucket, path), object)| {
            if bucket != RUSTFS_META_BUCKET {
                return None;
            }
            path.strip_prefix(&prefix)
                .map(|suffix| (suffix.to_string(), serde_json::from_slice(&object.data).unwrap()))
        })
        .collect()
}

async fn assert_renamed_maintenance_objects(
    backend: &TestCatalogObjectBackend,
    entry: &TableEntry,
    expected: &BTreeMap<String, serde_json::Value>,
) {
    let renamed = expected
        .iter()
        .map(|(suffix, value)| {
            let mut value = value.clone();
            for field in ["job", "snapshot-expiration", "compaction"] {
                if let Some(owner) = value.get_mut(field).and_then(serde_json::Value::as_object_mut) {
                    owner.insert("namespace".to_string(), serde_json::Value::String(entry.namespace.clone()));
                    owner.insert("table".to_string(), serde_json::Value::String(entry.table.clone()));
                }
            }
            (suffix.clone(), value)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rename_maintenance_objects(backend, entry).await, renamed);
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
async fn table_metadata_data_plane_resource_resolves_only_current_metadata() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend.reset_call_counts().await;
    backend.state.lock().await.read_attempts.clear();

    let resource = table_metadata_data_plane_resource_for_object(&store, bucket, &current)
        .await
        .expect("metadata ownership lookup should succeed")
        .expect("current metadata should resolve to its table");
    assert_eq!(resource.namespace, "sales");
    assert_eq!(resource.table, "orders");
    assert_eq!(backend.list_call_count().await, 1);
    let paths = TableCatalogObjectPaths::default();
    // One bucket generation read, one directory mode probe, and one authoritative
    // table entry. The legacy fixture must never load a candidate object.
    assert_eq!(
        backend.state.lock().await.read_attempts,
        [
            paths.table_bucket_entry_path(bucket),
            paths.backing_migration_fence_path(bucket),
            paths.table_entry_path(bucket, &namespace, &table),
        ]
        .into_iter()
        .map(|key| ((RUSTFS_META_BUCKET.to_owned(), key), 1))
        .collect()
    );
    assert_eq!(backend.metadata_call_count().await, 1);

    let historical = default_table_metadata_file_path(&namespace, &table, "00000.metadata.json");
    assert!(
        table_metadata_data_plane_resource_for_object(&store, bucket, &historical)
            .await
            .expect("historical metadata lookup should succeed")
            .is_none()
    );
    assert!(
        table_metadata_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/file.parquet")
            .await
            .expect("ordinary object lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn table_metadata_data_plane_resource_follows_a_renamed_table() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    store
        .rename_table(bucket, "sales", "orders", "sales", "archived_orders")
        .await
        .expect("table rename should succeed");

    let resource = table_metadata_data_plane_resource_for_object(&store, bucket, &current)
        .await
        .expect("renamed metadata ownership lookup should succeed")
        .expect("current metadata should remain owned after rename");
    assert_eq!(resource.namespace, "sales");
    assert_eq!(resource.table, "archived_orders");
}

#[tokio::test]
async fn table_metadata_data_plane_resource_rejects_multiple_active_owners() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    store
        .rename_table(bucket, "sales", "orders", "sales", "archived_orders")
        .await
        .expect("table rename should succeed");

    let mut replacement = test_table_entry(bucket, &namespace, &table, current.clone());
    replacement.table_id = "replacement-table-id".to_string();
    replacement.table_uuid = "replacement-table-uuid".to_string();
    replacement.warehouse_location = format!("s3://{bucket}/tables/replacement-table-id");
    store
        .create_table(replacement)
        .await
        .expect("replacement table should be created");

    assert_matches!(
        table_metadata_data_plane_resource_for_object(&store, bucket, &current).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("multiple active tables")
    );
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

async fn cached_registration_fixture() -> (TestCatalogObjectBackend, ObjectTableCatalogStore<TestCatalogObjectBackend>, Namespace)
{
    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
    let store = ObjectTableCatalogStore::new(backend.clone());
    let namespace = Namespace::parse("sales").expect("test namespace is valid");
    store
        .put_table_bucket(test_bucket_entry("analytics"))
        .await
        .expect("bucket is created");
    store
        .create_namespace(test_namespace_entry("analytics", &namespace))
        .await
        .expect("namespace is created");
    (backend, store, namespace)
}

fn cached_registration_entry(namespace: &Namespace, name: &str) -> TableEntry {
    let table = IdentifierSegment::parse(name).expect("test table name is valid");
    let mut entry = test_table_entry(
        "analytics",
        namespace,
        &table,
        default_table_metadata_file_path(namespace, &table, "00001.metadata.json"),
    );
    entry.table_id = format!("id-{name}");
    entry.warehouse_location = format!("s3://analytics/tables/id-{name}");
    entry
}

#[tokio::test]
async fn object_catalog_registration_cache_bounds_reads_across_store_instances() {
    let (backend, _, namespace) = cached_registration_fixture().await;
    for number in 0..256 {
        let store = ObjectTableCatalogStore::new(backend.clone());
        backend.reset_call_counts().await;
        store
            .create_table(cached_registration_entry(&namespace, &format!("table_{number:04}")))
            .await
            .expect("each new store instance shares validated registration projections");
        if number >= 32 {
            let state = backend.state.lock().await;
            assert_eq!(state.list_calls, 2, "each registration inventories both catalog ranges");
            assert!(
                state.read_calls < 24,
                "registration used {} object reads at table {number}",
                state.read_calls
            );
            assert!(
                state.read_bytes < 256 * 1024,
                "registration read {} bytes at table {number}",
                state.read_bytes
            );
        }
    }
}

#[tokio::test]
async fn object_catalog_registration_cache_preserves_identity_and_overlap_checks() {
    let (backend, store, namespace) = cached_registration_fixture().await;
    let mut existing = cached_registration_entry(&namespace, "orders");
    existing.warehouse_location = "s3://analytics/shared/orders".to_string();
    store.create_table(existing.clone()).await.expect("initial table is created");
    backend
        .delete_object(RUSTFS_META_BUCKET, &store.paths.warehouse_index_entry_path("analytics", "shared/orders/"))
        .await
        .expect("simulate a missing materialized warehouse index");

    let mut duplicate = cached_registration_entry(&namespace, "duplicate");
    duplicate.table_id = existing.table_id.clone();
    assert_matches!(store.create_table(duplicate).await, Err(TableCatalogStoreError::Conflict(_)));
    for location in [
        "s3://analytics/shared",
        "s3://analytics/shared/orders",
        "s3://analytics/shared/orders/child",
    ] {
        let mut conflicting = cached_registration_entry(&namespace, "conflicting");
        conflicting.warehouse_location = location.to_string();
        assert_matches!(store.create_table(conflicting).await, Err(TableCatalogStoreError::Conflict(_)));
    }
    let mut adjacent = cached_registration_entry(&namespace, "adjacent");
    adjacent.warehouse_location = "s3://analytics/shared/orders2".to_string();
    store
        .create_table(adjacent)
        .await
        .expect("a different directory remains available");
}

#[tokio::test]
async fn object_catalog_registration_cache_rechecks_after_failed_write_and_drop() {
    let (backend, store, namespace) = cached_registration_fixture().await;
    store
        .create_table(cached_registration_entry(&namespace, "orders"))
        .await
        .expect("initial table is created");
    let returns = IdentifierSegment::parse("returns").expect("test name is valid");
    let table_path = store.paths.table_entry_path("analytics", &namespace, &returns);
    backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
    assert!(
        store
            .create_table(cached_registration_entry(&namespace, "returns"))
            .await
            .is_err()
    );
    assert!(
        store
            .load_table("analytics", "sales", "returns")
            .await
            .expect("table lookup succeeds")
            .is_none()
    );
    store
        .create_table(cached_registration_entry(&namespace, "returns"))
        .await
        .expect("retry reads the authoritative records");
    store
        .drop_table("analytics", "sales", "orders")
        .await
        .expect("drop removes the active owner");
    let mut replacement = cached_registration_entry(&namespace, "replacement");
    replacement.table_id = "id-orders".to_string();
    replacement.warehouse_location = "s3://analytics/tables/id-orders".to_string();
    store
        .create_table(replacement)
        .await
        .expect("a dropped owner no longer reserves its identity");
}

#[tokio::test]
async fn object_catalog_registration_cache_observes_pointer_commits_and_rename() {
    let (backend, store, namespace) = cached_registration_fixture().await;
    let entry = cached_registration_entry(&namespace, "orders");
    store.create_table(entry.clone()).await.expect("initial table is created");
    let metadata_directory = entry
        .metadata_location
        .rsplit_once('/')
        .expect("metadata has a parent directory")
        .0;
    let next_metadata = format!("{metadata_directory}/00002.metadata.json");
    backend
        .seed_object(
            "analytics",
            &next_metadata,
            serde_json::to_vec(&serde_json::json!({"location": entry.warehouse_location})).unwrap(),
        )
        .await;
    store
        .commit_table(TableCommitRequest {
            table_bucket: "analytics".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
            commit_id: "pointer-only".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: entry.version_token,
            expected_metadata_location: entry.metadata_location,
            new_metadata_location: next_metadata,
            requirements: Vec::new(),
            writer: None,
        })
        .await
        .expect("metadata pointer commit succeeds");
    backend.reset_call_counts().await;
    store
        .create_table(cached_registration_entry(&namespace, "returns"))
        .await
        .unwrap();
    assert_eq!(
        backend.list_call_count().await,
        2,
        "pointer changes are checked through fresh inventories"
    );

    store
        .rename_table("analytics", "sales", "orders", "sales", "renamed")
        .await
        .unwrap();
    backend.reset_call_counts().await;
    store
        .create_table(cached_registration_entry(&namespace, "shipments"))
        .await
        .unwrap();
    assert!(
        backend.list_call_count().await > 0,
        "rename is visible in the next authoritative inventory"
    );
    let mut duplicate = cached_registration_entry(&namespace, "duplicate");
    duplicate.table_id = "id-orders".to_string();
    assert_matches!(store.create_table(duplicate).await, Err(TableCatalogStoreError::Conflict(_)));
}

#[tokio::test]
async fn object_catalog_registration_cache_observes_an_independent_writer() {
    let (backend, store, namespace) = cached_registration_fixture().await;
    store
        .create_table(cached_registration_entry(&namespace, "orders"))
        .await
        .unwrap();
    let scan_store = ObjectTableCatalogStore::new(TestCatalogObjectBackend {
        registration_cache: TableCatalogRegistrationCache::default(),
        ..backend.clone()
    });
    scan_store
        .create_table(cached_registration_entry(&namespace, "returns"))
        .await
        .unwrap();
    backend.reset_call_counts().await;
    store
        .create_table(cached_registration_entry(&namespace, "shipments"))
        .await
        .unwrap();
    assert!(
        backend.list_call_count().await > 0,
        "an independent writer is visible in the next authoritative inventory"
    );
    let mut duplicate = cached_registration_entry(&namespace, "duplicate");
    duplicate.table_id = "id-returns".to_string();
    assert_matches!(store.create_table(duplicate).await, Err(TableCatalogStoreError::Conflict(_)));
}

#[tokio::test]
async fn object_catalog_registration_scans_warehouse_prefixes_once() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();

    let mut reads = Vec::new();
    for name in ["orders", "returns", "shipments"] {
        let table = IdentifierSegment::parse(name).expect("table should parse");
        let mut entry = test_table_entry(
            bucket,
            &namespace,
            &table,
            default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
        );
        entry.table_id = format!("table-{name}");
        entry.warehouse_location = format!("s3://{bucket}/tables/{name}");
        backend.reset_call_counts().await;
        store.create_table(entry).await.expect("table should be created");
        reads.push(backend.read_call_count().await);
        assert_eq!(backend.list_call_count().await, 2, "one table scan and one warehouse index scan");
    }
    assert_eq!(reads[2], reads[1], "unchanged validated objects should not require another GET");
    assert_eq!(store.list_tables(bucket, &namespace.public_name()).await.unwrap().len(), 3);
}

#[tokio::test]
async fn object_catalog_registration_bounds_concurrent_catalog_reads() {
    const MAX_CONCURRENT_READS: usize = 8;

    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();

    let mut table_objects = Vec::new();
    let mut index_objects = Vec::new();
    for number in 0..MAX_CONCURRENT_READS + 2 {
        let name = format!("orders_{number:02}");
        let table = IdentifierSegment::parse(&name).unwrap();
        let mut entry = test_table_entry(
            bucket,
            &namespace,
            &table,
            default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
        );
        entry.table_id = format!("table-{name}");
        entry.warehouse_location = format!("s3://{bucket}/tables/{name}");
        store.create_table(entry).await.unwrap();
        table_objects.push(store.paths.table_entry_path(bucket, &namespace, &table));
        index_objects.push(store.paths.warehouse_index_entry_path(bucket, &format!("tables/{name}/")));
    }

    backend.registration_cache.clear();
    let mut scans = Vec::new();
    for mut objects in [table_objects, index_objects] {
        objects.sort();
        let mut scan = Vec::new();
        for object in objects {
            let previous_reads = backend
                .state
                .lock()
                .await
                .read_attempts
                .get(&(RUSTFS_META_BUCKET.to_string(), object.clone()))
                .copied()
                .unwrap_or_default();
            let pause = backend.pause_before_next_read(RUSTFS_META_BUCKET, &object).await;
            scan.push((object, pause, previous_reads));
        }
        scans.push(scan);
    }

    let table = IdentifierSegment::parse("new_orders").unwrap();
    let mut entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    entry.table_id = "new-table-id".to_string();
    entry.warehouse_location = format!("s3://{bucket}/tables/new_orders");
    let registering_store = store.clone();
    let registration = tokio::spawn(async move { registering_store.create_table(entry).await });

    for scan in scans {
        for (_, pause, _) in scan.iter().take(MAX_CONCURRENT_READS) {
            tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
                .await
                .expect("independent catalog reads should start concurrently");
        }
        let state = backend.state.lock().await;
        let started = scan
            .iter()
            .filter(|(object, _, previous_reads)| {
                state
                    .read_attempts
                    .get(&(RUSTFS_META_BUCKET.to_string(), object.clone()))
                    .copied()
                    .unwrap_or_default()
                    > *previous_reads
            })
            .count();
        assert_eq!(started, MAX_CONCURRENT_READS, "catalog reads must respect the concurrency limit");
        drop(state);
        for (_, pause, _) in scan {
            pause.release();
        }
    }

    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, registration)
        .await
        .expect("registration should finish after its reads are released")
        .unwrap()
        .unwrap();
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn object_catalog_registration_propagates_catalog_scan_read_errors() {
    for fail_index_read in [false, true] {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let existing = IdentifierSegment::parse("orders").unwrap();
        seed_table_for_metadata_maintenance(
            &store,
            bucket,
            &namespace,
            &existing,
            default_table_metadata_file_path(&namespace, &existing, "00001.metadata.json"),
        )
        .await;
        let failed_object = if fail_index_read {
            store.paths.warehouse_index_entry_path(bucket, "tables/table-id/")
        } else {
            store.paths.table_entry_path(bucket, &namespace, &existing)
        };
        backend.fail_next_read(RUSTFS_META_BUCKET, &failed_object).await;

        let table = IdentifierSegment::parse("returns").unwrap();
        let mut entry = test_table_entry(
            bucket,
            &namespace,
            &table,
            default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
        );
        entry.table_id = "new-table-id".to_string();
        entry.warehouse_location = format!("s3://{bucket}/tables/new-table-id");
        assert_matches!(
            store.create_table(entry.clone()).await,
            Err(TableCatalogStoreError::Internal(message)) if message.contains(&failed_object)
        );
        assert!(
            store
                .load_table(bucket, &namespace.public_name(), table.as_str())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .read_object(
                    RUSTFS_META_BUCKET,
                    &store.paths.warehouse_index_entry_path(bucket, "tables/new-table-id/")
                )
                .await
                .unwrap()
                .is_none()
        );
        store
            .create_table(entry)
            .await
            .expect("a failed scan must release its read locks for retry");
    }
}

#[tokio::test]
async fn object_catalog_registration_reuses_locked_deleted_entry() {
    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    let mut entry = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    entry.state = TableCatalogEntryState::Deleted;
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &store.paths.table_entry_path(bucket, &namespace, &table),
            serde_json::to_vec(&entry).unwrap(),
        )
        .await;
    entry.state = TableCatalogEntryState::Active;
    entry.table_id = "replacement-table-id".to_string();
    store.create_table(entry.clone()).await.unwrap();
    assert_eq!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .unwrap(),
        Some(entry)
    );
}

#[tokio::test]
async fn configured_object_catalog_rejects_overlapping_registered_warehouse_prefixes() {
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

    let configured_backend = TestCatalogObjectBackend::default();
    let configured_store = ConfiguredTableCatalogStore::ObjectBacked(ObjectTableCatalogStore::new(configured_backend));
    configured_store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    configured_store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    configured_store
        .create_table(test_table_entry(
            bucket,
            &namespace,
            &parent_table,
            default_table_metadata_file_path(&namespace, &parent_table, "00001.metadata.json"),
        ))
        .await
        .unwrap();
    assert_matches!(configured_store.create_table(child_entry).await, Err(TableCatalogStoreError::Conflict(_)));
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

    let before = backend.state.lock().await.objects.clone();
    let read_only = store
        .resolve_table_data_plane_resource_for_authorization(bucket, "tables/table-id/child/data/part-00001.parquet")
        .await
        .expect("read-only lookup tolerates a stale deeper index")
        .expect("read-only lookup retains the parent protection");
    assert_eq!(read_only.table, "orders");
    assert_eq!(read_only.warehouse_object_prefix, "tables/table-id/");
    assert_eq!(backend.state.lock().await.objects, before, "authorization must not remove stale indexes");
    assert_eq!(backend.list_call_count().await, 0);

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
    let backup_fence_lock = store.paths.catalog_backup_fence_lock_path(bucket);
    let migration_permits_before = backend
        .read_lock_acquisition_count(RUSTFS_META_BUCKET, &backup_fence_lock)
        .await;
    backend.reset_call_counts().await;

    let resource = table_data_plane_resource_for_object(&store, bucket, object)
        .await
        .expect("missing ready index entries must use the authoritative table scan")
        .expect("the table scan must retain table-aware protection");

    assert_eq!(resource.table, "orders");
    assert_eq!(backend.list_call_count().await, 1);
    assert_eq!(
        backend
            .read_lock_acquisition_count(RUSTFS_META_BUCKET, &backup_fence_lock)
            .await,
        migration_permits_before + 1,
        "repairing a missing warehouse index must hold the object-backed migration permit"
    );
    assert!(
        store
            .read_entry::<TableWarehouseIndexEntry>(
                RUSTFS_META_BUCKET,
                &store.paths.warehouse_index_entry_path(bucket, "tables/table-id/"),
            )
            .await
            .expect("repaired warehouse index lookup should succeed")
            .is_some(),
        "the bounded scan should repair the missing index"
    );

    backend.reset_call_counts().await;
    let indexed = table_data_plane_resource_for_object(&store, bucket, object)
        .await
        .expect("repaired warehouse index lookup should succeed")
        .expect("repaired warehouse index should retain table-aware protection");
    assert_eq!(indexed.table, "orders");
    assert_eq!(backend.list_call_count().await, 0);
    assert_eq!(
        backend
            .read_lock_acquisition_count(RUSTFS_META_BUCKET, &backup_fence_lock)
            .await,
        migration_permits_before + 1,
        "an indexed lookup must not acquire a write permit"
    );
}

#[tokio::test]
async fn operator_backfill_reconciles_a_missing_ready_index_above_the_data_plane_limit() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let object = "tables/table-id/data/part-00001.parquet";
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend
        .delete_object(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("warehouse index entry should be removed");
    // Only table entries count toward the data-plane limit; seed enough real
    // entries that, with the table under test, the catalog exceeds it.
    let noise_namespace = Namespace::parse("noise").expect("namespace should parse");
    for index in 0..TABLE_DATA_PLANE_INDEX_MISS_SCAN_MAX_CATALOG_OBJECTS {
        let noise_table = IdentifierSegment::parse(format!("noise-{index}")).expect("table should parse");
        let mut entry = test_table_entry(
            bucket,
            &noise_namespace,
            &noise_table,
            default_table_metadata_file_path(&noise_namespace, &noise_table, "00001.metadata.json"),
        );
        entry.table_id = format!("noise-{index}");
        entry.table_uuid = format!("noise-uuid-{index}");
        entry.warehouse_location = format!("s3://{bucket}/tables/noise-{index}");
        backend
            .seed_object(
                RUSTFS_META_BUCKET,
                &store.paths.table_entry_path(bucket, &noise_namespace, &noise_table),
                serde_json::to_vec(&entry).expect("entry should encode"),
            )
            .await;
    }

    assert_matches!(
        store.resolve_table_data_plane_resource_for_authorization(bucket, object).await,
        Err(TableCatalogStoreError::Unavailable(message)) if message.contains("4096-catalog-object safety limit")
    );
    assert_matches!(
        table_data_plane_resource_for_object(&store, bucket, object).await,
        Err(TableCatalogStoreError::Unavailable(message)) if message.contains("4096-catalog-object safety limit")
    );
    store
        .backfill_table_warehouse_index(bucket)
        .await
        .expect("operator backfill should reconcile a ready index without the data-plane limit");
    assert!(
        store
            .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
            .await
            .expect("reconciled warehouse index lookup should succeed")
            .is_some(),
        "operator reconciliation must recreate the missing ready-state index"
    );

    backend.reset_call_counts().await;
    let resource = table_data_plane_resource_for_object(&store, bucket, object)
        .await
        .expect("reconciled index lookup should succeed")
        .expect("reconciled index should retain table-aware protection");
    assert_eq!(resource.table, "orders");
    assert_eq!(backend.list_call_count().await, 0);
}

async fn seed_noise_table_with_job_reports(
    store: &ObjectTableCatalogStore<TestCatalogObjectBackend>,
    bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    table_id: &str,
    reports: usize,
) {
    let mut entry = test_table_entry(
        bucket,
        namespace,
        table,
        default_table_metadata_file_path(namespace, table, "00001.metadata.json"),
    );
    entry.table_id = table_id.to_string();
    entry.table_uuid = format!("{table_id}-uuid");
    entry.warehouse_location = format!("s3://{bucket}/tables/{table_id}");
    store
        .backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &store.paths.table_entry_path(bucket, namespace, table),
            serde_json::to_vec(&entry).expect("entry should encode"),
        )
        .await;
    for job in 0..reports {
        store
            .backend
            .seed_object(
                RUSTFS_META_BUCKET,
                &store
                    .paths
                    .table_maintenance_job_path(bucket, namespace, table, table_id, &format!("job-{job}")),
                b"{}".to_vec(),
            )
            .await;
    }
}

#[tokio::test]
async fn table_data_plane_index_miss_scan_pages_scale_with_entries_not_job_reports() {
    let mut baseline_pages = None;
    for reports in [0, 80] {
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
        // Keep the same table inventory with and without historical job reports.
        let noise_namespace = Namespace::parse("noise").expect("namespace should parse");
        for index in 0..1000 {
            let noise_table = IdentifierSegment::parse(format!("noise-{index:04}")).expect("table should parse");
            seed_noise_table_with_job_reports(&store, bucket, &noise_namespace, &noise_table, &format!("noise-{index}"), reports)
                .await;
        }
        backend.reset_call_counts().await;

        let resource = table_data_plane_resource_for_object(&store, bucket, object)
            .await
            .expect("job history must not exhaust the index-miss listing budget")
            .expect("the scan must retain table-aware protection");
        assert_eq!(resource.table, "orders");
        let pages = backend.list_call_count().await;
        assert!(pages > 0, "the missing index must exercise enumeration");
        if let Some(expected) = baseline_pages {
            assert_eq!(pages, expected, "job history must not consume ownership listing pages");
        } else {
            baseline_pages = Some(pages);
        }
    }
}

#[tokio::test]
async fn table_entry_scan_preserves_nested_namespaces_named_like_maintenance() {
    for name in ["sales", "sales.tables.orders.maintenance"] {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse(name).expect("namespace should parse");
        let owner = IdentifierSegment::parse("zz-owner").expect("owner should parse");
        let current = default_table_metadata_file_path(&namespace, &owner, "00001.metadata.json");
        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &owner, current).await;
        backend
            .delete_object(RUSTFS_META_BUCKET, &store.paths.warehouse_index_entry_path(bucket, "tables/table-id/"))
            .await
            .expect("remove the owner index to exercise catalog fallback");
        for index in 0..1000 {
            let table = IdentifierSegment::parse(format!("table-{index:04}")).expect("table should parse");
            seed_noise_table_with_job_reports(&store, bucket, &namespace, &table, &format!("id-{index}"), 0).await;
        }
        let resource = store
            .resolve_table_data_plane_resource_for_authorization(bucket, "tables/table-id/data/part.parquet")
            .await
            .expect("registered table entries must remain enumerable")
            .unwrap_or_else(|| panic!("namespace {name} lost its table owner across a page boundary"));
        assert_eq!(resource.namespace, name);
        assert_eq!(resource.table, "zz-owner");
    }
}

#[tokio::test]
async fn object_maintenance_preserves_an_owner_after_nested_namespace_page_boundary() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let orphan = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    backend.seed_object(bucket, &orphan, br#"{"metadata-log":[]}"#.to_vec()).await;
    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("the unowned old metadata should initially be a cleanup candidate");
    assert_eq!(report.deletable_metadata_locations, vec![orphan.clone()]);

    let nested = Namespace::parse("sales.tables.orders.maintenance").expect("nested namespace should parse");
    store
        .create_namespace(test_namespace_entry(bucket, &nested))
        .await
        .expect("nested namespace should be created");
    for index in 0..1100 {
        let noise = IdentifierSegment::parse(format!("table-{index:04}")).expect("table should parse");
        seed_noise_table_with_job_reports(&store, bucket, &nested, &noise, &format!("noise-{index}"), 0).await;
    }
    let owner = IdentifierSegment::parse("zz-owner").expect("owner should parse");
    let mut other = test_table_entry(bucket, &nested, &owner, orphan.clone());
    other.table_id = "other-id".to_string();
    other.table_uuid = "other-uuid".to_string();
    other.warehouse_location = format!("s3://{bucket}/tables/other-id");
    store
        .create_table(other)
        .await
        .expect("another table may adopt the metadata before cleanup");
    let deletes_before = backend.state.lock().await.delete_attempts.clone();
    let result = store
        .delete_table_metadata_maintenance_report(bucket, "sales", "orders", report)
        .await;
    assert!(
        backend
            .object_exists(bucket, &orphan)
            .await
            .expect("read back the current owner's metadata")
    );
    assert_eq!(backend.state.lock().await.delete_attempts, deletes_before);
    assert_matches!(result, Err(TableCatalogStoreError::Conflict(message)) if message.contains("exclusive metadata"));
}

#[tokio::test]
async fn table_data_plane_index_miss_scan_bounds_a_job_report_flood_under_one_table() {
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
    // Historical report volume must not consume ownership pagination.
    for job in 0..70_000 {
        backend
            .seed_object(
                RUSTFS_META_BUCKET,
                &store
                    .paths
                    .table_maintenance_job_path(bucket, &namespace, &table, "table-id", &format!("job-{job}")),
                b"{}".to_vec(),
            )
            .await;
    }
    backend.reset_call_counts().await;

    let resource = table_data_plane_resource_for_object(&store, bucket, object)
        .await
        .expect("a report flood under one table must not exhaust the listing budget")
        .expect("the scan must retain table-aware protection");
    assert_eq!(resource.table, "orders");
    let pages = backend.list_call_count().await;
    assert!(pages <= 2, "report history must not consume {pages} ownership pages");
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
async fn active_catalog_owner_takes_precedence_over_a_warehouse_tombstone() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let prefix = "tables/table-id/";

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    let tombstone = TableWarehouseIndexEntry {
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
            &tombstone,
            TableCatalogPutPrecondition::Any,
        )
        .await
        .expect("warehouse tombstone should be seeded");

    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("active catalog owner should resolve ahead of the tombstone")
        .expect("active catalog owner should retain table-aware protection");

    assert_eq!(resource.table_id, "table-id");
    assert_eq!(resource.warehouse_object_prefix, prefix);
}

#[tokio::test]
async fn table_data_plane_resource_fails_closed_for_a_transient_ready_index() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let prefix = "tables/table-id/";

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    for state in [TableCatalogEntryState::Renaming, TableCatalogEntryState::Deleting] {
        let index = TableWarehouseIndexEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            table_id: "table-id".to_string(),
            warehouse_object_prefix: prefix.to_string(),
            state,
        };
        store
            .write_entry(
                store.catalog_bucket(),
                &store.paths.warehouse_index_entry_path(bucket, prefix),
                &index,
                TableCatalogPutPrecondition::Any,
            )
            .await
            .expect("transient warehouse index should be seeded");

        assert_matches!(
            table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet").await,
            Err(TableCatalogStoreError::Internal(message))
                if message.contains("invalid transient state while the index is authoritative")
        );
    }
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
    assert_eq!(backend.list_call_count().await, 1);
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
    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
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

    store
        .backfill_active_table_warehouse_index(bucket, &namespace.public_name(), table.as_str())
        .await
        .expect("active backfill should reuse its held table entry lock");
    backend
        .delete_object(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("backfilled index should be removed before testing a stale listing");

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
    // One read snapshots the bucket rename fence and one loads the page entry.
    assert_eq!(backend.read_call_count().await, 2);
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
            .list_tables_page(bucket, &namespace_name, Some("legacy:alpha"), one)
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
async fn object_catalog_prefix_reuse_preserves_protection_on_write_failure() {
    for failure in ["index", "table", "rollback", "ambiguous"] {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let namespace = Namespace::parse("sales").unwrap();
        let first = IdentifierSegment::parse("orders").unwrap();
        let second = IdentifierSegment::parse("returns").unwrap();
        let current = default_table_metadata_file_path(&namespace, &first, "00001.metadata.json");
        seed_table_for_metadata_maintenance(&store, bucket, &namespace, &first, current).await;
        store
            .drop_table(bucket, &namespace.public_name(), first.as_str())
            .await
            .unwrap();

        let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");
        let table_path = store.paths.table_entry_path(bucket, &namespace, &second);
        let index_attempts = backend.put_attempt_count(RUSTFS_META_BUCKET, &index_path).await;
        if failure == "index" {
            backend
                .fail_put_attempt(RUSTFS_META_BUCKET, &index_path, index_attempts + 2)
                .await;
        } else if failure == "ambiguous" {
            backend.fail_after_next_put(RUSTFS_META_BUCKET, &table_path).await;
        } else {
            backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
            if failure == "rollback" {
                backend
                    .fail_put_attempt(RUSTFS_META_BUCKET, &index_path, index_attempts + 3)
                    .await;
            }
        }

        let metadata = default_table_metadata_file_path(&namespace, &second, "00001.metadata.json");
        let mut entry = test_table_entry(bucket, &namespace, &second, metadata);
        entry.table_id = "replacement-table-id".to_string();
        entry.warehouse_location = format!("s3://{bucket}/tables/table-id");
        assert_matches!(store.create_table(entry.clone()).await, Err(TableCatalogStoreError::Internal(_)));
        assert_eq!(
            store
                .load_table(bucket, &namespace.public_name(), second.as_str())
                .await
                .unwrap()
                .is_some(),
            failure == "ambiguous"
        );

        let restarted = ObjectTableCatalogStore::new(backend.clone());
        let resource = table_data_plane_resource_for_object(&restarted, bucket, "tables/table-id/data/file.parquet").await;
        if failure == "rollback" {
            assert_matches!(resource, Err(TableCatalogStoreError::Internal(_)));
        } else {
            let resource = resource
                .expect("lookup should succeed")
                .expect("failed prefix reuse must retain protection");
            assert_eq!(
                resource.table_id,
                if failure == "ambiguous" {
                    "replacement-table-id"
                } else {
                    "table-id"
                },
                "failure at {failure}"
            );
        }
        let (index, _) = restarted
            .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
            .await
            .unwrap()
            .expect("failed prefix reuse must not remove the warehouse index");
        assert_eq!(
            index.state,
            if matches!(failure, "rollback" | "ambiguous") {
                TableCatalogEntryState::Active
            } else {
                TableCatalogEntryState::Deleted
            }
        );

        if failure == "rollback" {
            let third = IdentifierSegment::parse("retry_returns").unwrap();
            let mut retry = entry.clone();
            retry.table = third.as_str().to_string();
            retry.table_id = "retry-table-id".to_string();
            retry.metadata_location = default_table_metadata_file_path(&namespace, &third, "00001.metadata.json");
            backend
                .fail_next_put(RUSTFS_META_BUCKET, &store.paths.table_entry_path(bucket, &namespace, &third))
                .await;
            assert_matches!(restarted.create_table(retry).await, Err(TableCatalogStoreError::Internal(_)));
            assert_matches!(
                table_data_plane_resource_for_object(&restarted, bucket, "tables/table-id/data/file.parquet").await,
                Err(TableCatalogStoreError::Internal(_))
            );
        }
        if failure != "ambiguous" {
            restarted
                .create_table(entry)
                .await
                .expect("prefix reuse should remain retryable");
        }
        restarted
            .drop_table(bucket, &namespace.public_name(), second.as_str())
            .await
            .expect("replacement table should remain droppable");
        let resource = table_data_plane_resource_for_object(&restarted, bucket, "tables/table-id/data/file.parquet")
            .await
            .unwrap()
            .expect("replacement tombstone should protect the reused prefix");
        assert_eq!(resource.table_id, "replacement-table-id");
    }
}

#[tokio::test]
async fn object_table_catalog_store_keeps_table_when_drop_index_tombstone_write_fails() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    backend.fail_next_put(RUSTFS_META_BUCKET, &index_path).await;

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
async fn object_table_catalog_store_rejects_drop_when_deleted_warehouse_index_owner_changed() {
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
        state: TableCatalogEntryState::Deleted,
    };
    store
        .write_entry(store.catalog_bucket(), &index_path, &conflicting_index, TableCatalogPutPrecondition::Any)
        .await
        .expect("conflicting warehouse tombstone should be seeded");

    assert_matches!(
        store.drop_table(bucket, &namespace.public_name(), table.as_str()).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("owner changed")
    );
    assert!(
        store
            .load_table(bucket, &namespace.public_name(), table.as_str())
            .await
            .expect("retained table lookup should succeed")
            .is_some()
    );
    let (retained_index, _) = store
        .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("conflicting warehouse tombstone lookup should succeed")
        .expect("conflicting warehouse tombstone should remain present");
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
    let (tombstone, _) = store
        .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("warehouse tombstone lookup should succeed")
        .expect("warehouse tombstone should be created");
    assert_eq!(tombstone.state, TableCatalogEntryState::Deleted);
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("dropped table data-plane lookup should succeed")
        .expect("dropped warehouse prefix should remain protected");
    assert_eq!(resource.table_id, "table-id");
}

#[tokio::test]
async fn active_table_warehouse_prefix_takes_precedence_over_a_deleted_parent() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let parent = IdentifierSegment::parse("orders").expect("table should parse");
    let child = IdentifierSegment::parse("returns").expect("table should parse");
    let parent_metadata = default_table_metadata_file_path(&namespace, &parent, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &parent, parent_metadata).await;
    store
        .drop_table(bucket, &namespace.public_name(), parent.as_str())
        .await
        .expect("parent table should be dropped");

    let child_metadata = default_table_metadata_file_path(&namespace, &child, "00001.metadata.json");
    let mut child_entry = test_table_entry(bucket, &namespace, &child, child_metadata);
    child_entry.table_id = "child-table-id".to_string();
    child_entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child");
    store.create_table(child_entry).await.expect("child table should be created");

    let child_resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/child/data/file.parquet")
        .await
        .expect("child data-plane lookup should succeed")
        .expect("active child table should protect its prefix");
    assert_eq!(child_resource.table_id, "child-table-id");

    let parent_resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/orphan/file.parquet")
        .await
        .expect("deleted parent data-plane lookup should succeed")
        .expect("deleted parent should keep protecting its remaining prefix");
    assert_eq!(parent_resource.table_id, "table-id");

    store
        .drop_table(bucket, &namespace.public_name(), child.as_str())
        .await
        .unwrap();
    assert_matches!(
        table_data_plane_resource_for_object(&store, bucket, "tables/table-id/child/data/file.parquet").await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("overlapping deleted table warehouse indexes")
    );
    let grandchild = IdentifierSegment::parse("nested_returns").unwrap();
    let mut entry = test_table_entry(
        bucket,
        &namespace,
        &grandchild,
        default_table_metadata_file_path(&namespace, &grandchild, "00001.metadata.json"),
    );
    entry.table_id = "grandchild-table-id".to_string();
    entry.warehouse_location = format!("s3://{bucket}/tables/table-id/child/nested");
    store.create_table(entry).await.unwrap();
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/child/nested/data/file.parquet")
        .await
        .unwrap()
        .expect("active owner should take precedence over both tombstones");
    assert_eq!(resource.table_id, "grandchild-table-id");
}

#[tokio::test]
async fn object_table_catalog_store_restores_index_when_table_entry_delete_fails() {
    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    store
        .put_table_maintenance_config(bucket, "sales", "orders", TableMaintenanceConfig::default())
        .await
        .unwrap();
    let config = store
        .paths
        .table_maintenance_config_path(bucket, &namespace, &table, "table-id");
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
    assert!(backend.object_exists(RUSTFS_META_BUCKET, &config).await.unwrap());
    let (restored_index, _) = store
        .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("restored warehouse index lookup should succeed")
        .expect("warehouse index should be restored");
    assert_eq!(restored_index.table_id, "table-id");
    assert_eq!(restored_index.state, TableCatalogEntryState::Active);
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("restored index lookup should succeed")
        .expect("restored index should keep data-plane protection");
    assert_eq!(resource.table, "orders");

    store
        .drop_table(bucket, &namespace.public_name(), table.as_str())
        .await
        .unwrap();
    assert!(!backend.object_exists(RUSTFS_META_BUCKET, &config).await.unwrap());
}

#[tokio::test]
async fn object_table_catalog_store_uses_tombstone_when_drop_index_restore_fails() {
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
    let restore_attempt = backend.put_attempt_count(RUSTFS_META_BUCKET, &index_path).await + 2;
    backend
        .fail_put_attempt(RUSTFS_META_BUCKET, &index_path, restore_attempt)
        .await;

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
    let (tombstone, _) = store
        .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
        .await
        .expect("warehouse index lookup should succeed")
        .expect("the failed restore should retain the warehouse tombstone");
    assert_eq!(tombstone.state, TableCatalogEntryState::Deleted);
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("tombstone fallback should succeed")
        .expect("tombstone fallback should keep data-plane protection");
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
    store
        .put_table_maintenance_config(bucket, "sales", "orders", TableMaintenanceConfig::default())
        .await
        .unwrap();
    let config = store
        .paths
        .table_maintenance_config_path(bucket, &namespace, &table, "table-id");
    let previous_config = store
        .paths
        .table_maintenance_config_path(bucket, &namespace, &table, "previous-id");
    backend
        .seed_object(RUSTFS_META_BUCKET, &previous_config, b"previous config".to_vec())
        .await;
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
    assert!(!backend.object_exists(RUSTFS_META_BUCKET, &config).await.unwrap());
    assert!(backend.object_exists(RUSTFS_META_BUCKET, &previous_config).await.unwrap());
    let resource = table_data_plane_resource_for_object(&store, bucket, "tables/table-id/data/part-00001.parquet")
        .await
        .expect("dropped table data-plane lookup should succeed")
        .expect("dropped warehouse prefix should remain protected");
    assert_eq!(resource.table_id, "table-id");
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

#[test]
fn warehouse_locations_reject_the_reserved_catalog_prefix() {
    for location in [
        "s3://analytics/.rustfs-table",
        "s3://analytics/.rustfs-table/",
        "s3://analytics/.rustfs-table/warehouses/default",
    ] {
        let table_error = validate_table_warehouse_location("analytics", location).unwrap_err();
        assert!(matches!(
            table_error,
            TableCatalogStoreError::Invalid(message) if message.contains("reserved table catalog prefix")
        ));

        let view_error = validate_view_warehouse_location("analytics", location).unwrap_err();
        assert!(matches!(
            view_error,
            TableCatalogStoreError::Invalid(message) if message.contains("reserved table catalog prefix")
        ));
    }

    assert!(validate_table_warehouse_location("analytics", "s3://analytics/.rustfs-table-other/table-id").is_ok());
    assert!(validate_table_warehouse_location("analytics", "s3://analytics/user/.rustfs-table/table-id").is_ok());
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
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current).await;
    let bucket_config = TableMaintenanceConfig {
        retain_recent_metadata_files: 3,
        delete_enabled: true,
        ..Default::default()
    };
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &store.paths.table_bucket_maintenance_config_path(bucket),
            serde_json::to_vec(&bucket_config).unwrap(),
        )
        .await;

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
    backend.seed_object(bucket, &old, b"{}".to_vec()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;

    let report = store
        .run_table_metadata_maintenance(bucket, "sales", "orders", false, Some("worker-a".to_string()))
        .await
        .expect("metadata maintenance run should succeed");

    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
    assert_eq!(report.job.config_source, TableMaintenanceConfigSource::Default);
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
async fn object_maintenance_worker_delete_waits_for_table_publication() {
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
                background_enabled: true,
                delete_enabled: true,
                retain_recent_metadata_files: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let table_path = store.paths.table_entry_path(bucket, &namespace, &table);
    let table_locks = backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &table_path).await;
    let publication_path = default_table_publication_lock_path(&namespace, &table);
    let publication_guard = backend.acquire_write_lock(bucket, &publication_path).await.unwrap();
    let publication_locks = backend.write_lock_acquisition_count(bucket, &publication_path).await;
    let worker = tokio::spawn(async move {
        store
            .run_table_metadata_maintenance_worker_once(bucket, "sales", "orders", "worker-a".to_string())
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.write_lock_acquisition_count(bucket, &publication_path).await == publication_locks {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("object maintenance deletion must acquire the table publication lock");
    assert!(!worker.is_finished());
    assert_eq!(
        backend.write_lock_acquisition_count(RUSTFS_META_BUCKET, &table_path).await,
        table_locks + 3
    );
    assert!(backend.object_exists(bucket, &old).await.unwrap());
    assert!(backend.object_exists(bucket, &current).await.unwrap());
    drop(publication_guard);
    let report = worker.await.unwrap().unwrap();
    assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Successful);
    assert_eq!(report.job.deleted_metadata_file_count, 1);
    assert!(!backend.object_exists(bucket, &old).await.unwrap());
    assert!(backend.object_exists(bucket, &current).await.unwrap());
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

struct ConditionalMaintenanceFixture {
    backend: TestCatalogObjectBackend,
    store: ConfiguredTableCatalogStore<TestCatalogObjectBackend>,
    queued: TableMetadataMaintenanceReport,
    entry: TableEntry,
    old_metadata: String,
    now: OffsetDateTime,
}

impl ConditionalMaintenanceFixture {
    async fn new(mode: TableCatalogBackingMode, max_retry_attempts: u16) -> Self {
        let backend = TestCatalogObjectBackend::default();
        let store = ConfiguredTableCatalogStore::new_for_test(backend.clone(), mode);
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        let old_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        store.put_table_bucket(test_bucket_entry("analytics")).await.unwrap();
        store
            .create_namespace(test_namespace_entry("analytics", &namespace))
            .await
            .unwrap();
        store
            .create_table(test_table_entry("analytics", &namespace, &table, current.clone()))
            .await
            .unwrap();
        let entry = store.load_table("analytics", "sales", "orders").await.unwrap().unwrap();
        backend
            .seed_object("analytics", &current, br#"{"metadata-log":[]}"#.to_vec())
            .await;
        backend.seed_object("analytics", &old_metadata, b"{}".to_vec()).await;
        store
            .put_table_maintenance_config(
                "analytics",
                "sales",
                "orders",
                TableMaintenanceConfig {
                    background_enabled: true,
                    delete_enabled: true,
                    max_retry_attempts,
                    retry_initial_backoff_seconds: 1,
                    retry_max_backoff_seconds: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let queued = store
            .run_table_maintenance_scheduler_once("analytics", "sales", "orders", "scheduler-a".to_string())
            .await
            .unwrap()
            .report;
        assert_eq!(queued.job.status, TableMetadataMaintenanceJobStatus::Queued);
        assert_eq!(queued.deletable_metadata_locations, vec![old_metadata.clone()]);
        Self {
            backend,
            store,
            queued,
            entry,
            old_metadata,
            now: OffsetDateTime::now_utc(),
        }
    }

    async fn run(
        &self,
        worker: &str,
        attempt: u16,
        now: OffsetDateTime,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let condition = TableMaintenanceWorkerCondition {
            expected_job_id: self.queued.job.job_id.clone(),
            expected_attempt: attempt,
        };
        let ConfiguredTableCatalogStore::ObjectBacked(store) = &self.store;
        store
            .run_table_metadata_maintenance_worker_once_conditionally_with_clock(
                "analytics",
                "sales",
                "orders",
                worker.to_string(),
                &condition,
                || now,
            )
            .await
    }

    fn report_paths(&self) -> [String; 3] {
        let paths = TableCatalogObjectPaths::default();
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        [
            paths.table_maintenance_job_path("analytics", &namespace, &table, &self.entry.table_id, &self.queued.job.job_id),
            paths.table_maintenance_latest_job_path("analytics", &namespace, &table, &self.entry.table_id),
            paths.table_maintenance_current_job_path("analytics", &namespace, &table, &self.entry.table_id),
        ]
    }

    async fn seed_report(&self, report: &TableMetadataMaintenanceReport) {
        for path in self.report_paths() {
            self.backend
                .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(report).unwrap())
                .await;
        }
    }

    async fn report(&self, id: &str) -> TableMetadataMaintenanceReport {
        self.store
            .get_table_metadata_maintenance_report("analytics", "sales", "orders", id)
            .await
            .unwrap()
            .unwrap()
    }

    async fn assert_report_views(&self, expected: &TableMetadataMaintenanceReport) {
        for path in self.report_paths() {
            let object = self.backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
            let actual: TableMetadataMaintenanceReport = serde_json::from_slice(&object.data).unwrap();
            assert_eq!(&actual, expected, "report view {path}");
        }
        assert_eq!(
            self.store.load_table("analytics", "sales", "orders").await.unwrap(),
            Some(self.entry.clone())
        );
        assert!(
            self.backend
                .object_exists("analytics", &self.entry.metadata_location)
                .await
                .unwrap()
        );
    }

    async fn delete_attempts(&self) -> usize {
        self.backend
            .state
            .lock()
            .await
            .delete_attempts
            .get(&("analytics".to_string(), self.old_metadata.clone()))
            .copied()
            .unwrap_or_default()
    }

    async fn fail_first_attempt(&self) -> TableMetadataMaintenanceReport {
        self.backend.fail_delete_attempt("analytics", &self.old_metadata, 1).await;
        self.run("worker-a", 0, self.now)
            .await
            .expect_err("injected delete failure must be surfaced");
        let failed = self.report(&self.queued.job.job_id).await;
        assert_eq!(failed.job.status, TableMetadataMaintenanceJobStatus::Failed);
        assert_eq!(failed.job.attempt, 1);
        self.assert_report_views(&failed).await;
        failed
    }
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

    assert_matches!(
        error,
        TableCatalogStoreError::Conflict(message) if message == "maintenance job is not current"
    );
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
async fn object_only_catalog_mutations_hold_the_migration_read_permit() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let (namespace, table, report) =
        seed_quarantined_table_maintenance(&store, &backend, bucket, OffsetDateTime::UNIX_EPOCH, None).await;
    let lock_path = store.paths.catalog_backup_fence_lock_path(bucket);
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
async fn object_catalog_data_plane_fails_closed_for_missing_bucket_entry() {
    let store = ObjectTableCatalogStore::new(TestCatalogObjectBackend::default());
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");

    let error = table_data_plane_resource_for_object(&store, "analytics", "tables/table-id/data/file.parquet")
        .await
        .expect_err("a table-enabled bucket missing from the object catalog must fail closed");

    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("no entry for table-enabled bucket"));

    let error = table_metadata_data_plane_resource_for_object(&store, "analytics", &metadata)
        .await
        .expect_err("metadata access must reject a missing object-backed bucket entry");
    assert_matches!(error, TableCatalogStoreError::Internal(message) if message.contains("no entry for table-enabled bucket"));
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
    let invalid_metadata = "s3://other-bucket/tables/table-id/metadata/00001.metadata.json";

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

    backend.reset_call_counts().await;
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

    assert_eq!(
        backend.list_call_count().await,
        0,
        "an unchanged warehouse must not trigger a catalog scan"
    );
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
async fn object_table_catalog_store_syncs_warehouse_location_from_committed_metadata() {
    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
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
    tokio::time::timeout(std::time::Duration::from_secs(5), store.create_table(next_entry))
        .await
        .expect("stale warehouse prefix reuse completes")
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
    // Scope every assertion to one implementation method: a guard in a sibling
    // method (or its tests) must not satisfy another entrypoint's contract.
    fn method<'a>(source: &'a str, signature: &str) -> &'a str {
        source
            .split_once(signature)
            .unwrap_or_else(|| panic!("missing object mutation method: {signature}"))
            .1
            .split_once("\n    }\n")
            .unwrap_or_else(|| panic!("missing method boundary: {signature}"))
            .0
    }

    fn assert_ordered(body: &str, expected: &[&str]) {
        let mut remainder = body;
        for statement in expected {
            remainder = remainder
                .split_once(statement)
                .unwrap_or_else(|| panic!("missing or out-of-order object mutation guard: {statement}"))
                .1;
        }
    }

    let put = include_str!("../app/object/put.rs");
    let copy = include_str!("../app/object/copy.rs");
    let delete = include_str!("../app/object/delete.rs");
    let extract = include_str!("../app/object/extract.rs");
    let restore = include_str!("../app/object/restore.rs");

    for (source, signature, delegate) in [
        (put, "fn execute_put_object(", "self.execute_put_object_boxed(_fs, req)"),
        (put, "fn execute_put_object_boxed<'a>(", "self.execute_put_object_inner(_fs, req)"),
        (put, "fn execute_put_object_inner(", "self.execute_put_object_extract(req)"),
        (copy, "fn execute_copy_object(", "self.execute_copy_object_body_boxed(req)"),
        (copy, "fn execute_copy_object_body_boxed(", "self.execute_copy_object_body(req)"),
        (delete, "fn execute_delete_object(", "owner.execute_delete_object_owned(req)"),
        (delete, "fn execute_delete_objects(", "owner.execute_delete_objects_owned(req)"),
        (extract, "fn execute_put_object_extract(", "self.execute_put_object_extract_boxed(req)"),
        (
            extract,
            "fn execute_put_object_extract_boxed(",
            "self.execute_put_object_extract_inner(req)",
        ),
    ] {
        assert!(
            method(source, signature).contains(delegate),
            "{signature} must call its guarded implementation"
        );
    }

    let key_guard = "validate_table_catalog_object_mutation(&bucket, &key).await?;";
    let store_resolution = "let Some(store) = self.object_store()";
    for (source, signature, key_validation, mutation_boundary) in [
        (
            put,
            "fn execute_put_object_inner(",
            "validate_new_object_key(&key, request_method_name)?;",
            "self.put_object_core(write, body, start_time).await?;",
        ),
        (
            copy,
            "fn execute_copy_object_body(",
            "validate_new_object_key(&key, \"COPY (dest)\")?;",
            store_resolution,
        ),
        (
            delete,
            "fn execute_delete_object_owned(",
            "validate_object_key(&key, \"DELETE\")?;",
            store_resolution,
        ),
        (
            extract,
            "fn execute_put_object_extract_inner(",
            "validate_new_object_key(&key, \"PUT\")?;",
            store_resolution,
        ),
    ] {
        assert_ordered(method(source, signature), &[key_validation, key_guard, mutation_boundary]);
    }
    assert_ordered(
        method(restore, "fn execute_restore_object("),
        &[
            "validate_table_catalog_object_mutation(&bucket, &object).await?;",
            store_resolution,
        ],
    );
    assert_ordered(
        method(delete, "fn execute_delete_objects_owned("),
        &[
            "validate_object_key(&key, \"DeleteObjects\")",
            "if let Err(err) = validate_table_catalog_object_mutation(&bucket, &key).await {",
            "continue;\n            }",
            "authorized_deletes.push(AuthorizedDelete { idx, object });",
        ],
    );
    assert_ordered(
        method(extract, "fn execute_put_object_extract_inner("),
        &[
            "validate_extract_member_key(&fpath, extract_limits)",
            "extract_try!(validate_table_catalog_object_mutation(&bucket, &fpath).await);",
            "batch.push(ExtractPreparedMember {",
        ],
    );
}

const MULTIPART_KEY_GUARD: &str = "validate_table_catalog_object_mutation(&bucket, &key).await?;";
const OBJECT_METADATA_OBJECT_GUARD: &str = "validate_table_catalog_object_mutation(&bucket, &object).await?;";
const OBJECT_METADATA_KEY_GUARD: &str = MULTIPART_KEY_GUARD;

fn assert_multipart_reserved_prefix_guard_calls(source: &crate::guard_sources::SourceSet) {
    assert_eq!(
        source.count(MULTIPART_KEY_GUARD),
        4,
        "reserved-prefix guard calls across {}",
        source.describe()
    );
}

fn assert_object_metadata_reserved_prefix_guard_calls(source: &crate::guard_sources::SourceSet) {
    assert_eq!(
        source.count(OBJECT_METADATA_OBJECT_GUARD),
        2,
        "reserved-prefix guard calls across {}",
        source.describe()
    );
    assert_eq!(
        source.count(OBJECT_METADATA_KEY_GUARD),
        2,
        "reserved-prefix guard calls across {}",
        source.describe()
    );
}

#[test]
fn multipart_mutation_entrypoints_call_reserved_prefix_guard() {
    assert_multipart_reserved_prefix_guard_calls(&crate::guard_sources::SourceSet::production(
        "src/app/multipart_usecase.rs",
        "src/app/multipart_usecase",
    ));
}

#[test]
fn object_metadata_mutation_entrypoints_call_reserved_prefix_guard() {
    assert_object_metadata_reserved_prefix_guard_calls(&crate::guard_sources::SourceSet::production(
        "src/storage/ecfs.rs",
        "src/storage/legacy_edge",
    ));
}

/// `count` production calls of `guard`, one per method, for count-guard fixtures.
fn guarded_methods(guard: &str, count: usize) -> String {
    let methods: String = (0..count)
        .map(|index| format!("    async fn guarded_{index}(&self) {{\n        {guard}\n    }}\n"))
        .collect();
    format!("impl Usecase {{\n{methods}}}\n")
}

fn guard_fixture(files: &[(&str, &str)], file: &str, split_dir: &str) -> crate::guard_sources::SourceSet {
    let root = crate::guard_sources::fixture_root(files);
    crate::guard_sources::SourceSet::production_under(root.path(), file, split_dir)
}

#[test]
fn multipart_count_guard_fails_when_a_dropped_production_call_is_replaced_by_test_mentions() {
    let parent = format!(
        "{}\n#[cfg(test)]\nmod tests {{\n    fn mentions() {{\n        {MULTIPART_KEY_GUARD}\n    }}\n}}\n",
        guarded_methods(MULTIPART_KEY_GUARD, 2)
    );
    let source = guard_fixture(
        &[
            ("src/app/multipart_usecase.rs", parent.as_str()),
            ("src/app/multipart_usecase/complete.rs", guarded_methods(MULTIPART_KEY_GUARD, 1).as_str()),
        ],
        "src/app/multipart_usecase.rs",
        "src/app/multipart_usecase",
    );
    assert_eq!(
        source.count(MULTIPART_KEY_GUARD),
        3,
        "fixture: three production calls and one test mention"
    );

    let message = crate::guard_sources::panic_message(move || assert_multipart_reserved_prefix_guard_calls(&source));

    assert!(message.contains("reserved-prefix guard calls"), "{message}");
}

#[test]
fn multipart_count_guard_passes_when_calls_move_into_the_split_directory() {
    let source = guard_fixture(
        &[
            ("src/app/multipart_usecase.rs", guarded_methods(MULTIPART_KEY_GUARD, 1).as_str()),
            ("src/app/multipart_usecase/complete.rs", guarded_methods(MULTIPART_KEY_GUARD, 3).as_str()),
        ],
        "src/app/multipart_usecase.rs",
        "src/app/multipart_usecase",
    );

    assert_multipart_reserved_prefix_guard_calls(&source);
}

#[test]
fn object_metadata_key_count_guard_fails_when_a_moved_call_is_broken() {
    let moved_and_broken = guarded_methods(MULTIPART_KEY_GUARD, 1).replace("&key)", "key.as_str())");
    let parent = format!(
        "{}{}",
        guarded_methods(OBJECT_METADATA_OBJECT_GUARD, 2),
        guarded_methods(OBJECT_METADATA_KEY_GUARD, 1)
    );
    let source = guard_fixture(
        &[
            ("src/storage/ecfs.rs", parent.as_str()),
            ("src/storage/legacy_edge/object_write.rs", moved_and_broken.as_str()),
        ],
        "src/storage/ecfs.rs",
        "src/storage/legacy_edge",
    );

    let message = crate::guard_sources::panic_message(move || assert_object_metadata_reserved_prefix_guard_calls(&source));

    assert!(message.contains("src/storage/legacy_edge/object_write.rs"), "{message}");
}

#[test]
fn object_metadata_key_count_guard_passes_when_a_call_moves_intact() {
    let parent = format!(
        "{}{}",
        guarded_methods(OBJECT_METADATA_OBJECT_GUARD, 2),
        guarded_methods(OBJECT_METADATA_KEY_GUARD, 1)
    );
    let source = guard_fixture(
        &[
            ("src/storage/ecfs.rs", parent.as_str()),
            (
                "src/storage/legacy_edge/object_write.rs",
                guarded_methods(OBJECT_METADATA_KEY_GUARD, 1).as_str(),
            ),
        ],
        "src/storage/ecfs.rs",
        "src/storage/legacy_edge",
    );

    assert_object_metadata_reserved_prefix_guard_calls(&source);
}

#[test]
fn object_metadata_count_guard_fails_when_a_legacy_edge_test_replaces_a_production_call() {
    let parent = format!(
        "{}{}",
        guarded_methods(OBJECT_METADATA_OBJECT_GUARD, 1),
        guarded_methods(OBJECT_METADATA_KEY_GUARD, 2)
    );
    let source = guard_fixture(
        &[
            ("src/storage/ecfs.rs", parent.as_str()),
            ("src/storage/legacy_edge/tests/object_meta.rs", OBJECT_METADATA_OBJECT_GUARD),
        ],
        "src/storage/ecfs.rs",
        "src/storage/legacy_edge",
    );

    let message = crate::guard_sources::panic_message(move || assert_object_metadata_reserved_prefix_guard_calls(&source));

    assert!(message.contains("reserved-prefix guard calls"), "{message}");
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
async fn configured_object_catalog_updates_namespace_properties() {
    let backend = TestCatalogObjectBackend::default();
    let store = ConfiguredTableCatalogStore::new_for_test(backend, TableCatalogBackingMode::ObjectBacked);
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").expect("namespace should parse");
    let mut entry = test_namespace_entry(bucket, &namespace);
    entry.properties = BTreeMap::from([
        ("obsolete".to_string(), "true".to_string()),
        ("owner".to_string(), "lakehouse".to_string()),
    ]);
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket entry should be seeded");
    store.create_namespace(entry).await.expect("namespace should be created");

    let result = store
        .update_namespace_properties(
            bucket,
            "sales",
            NamespacePropertiesUpdate::try_new(
                vec!["obsolete".to_string(), "missing".to_string()],
                BTreeMap::from([
                    ("owner".to_string(), "platform".to_string()),
                    ("retention".to_string(), "30d".to_string()),
                ]),
            )
            .expect("namespace update should validate"),
        )
        .await
        .expect("object-backed namespace properties should update");
    assert_eq!(result.updated, vec!["owner".to_string(), "retention".to_string()]);
    assert_eq!(result.removed, vec!["obsolete".to_string()]);
    assert_eq!(result.missing, vec!["missing".to_string()]);
    let stored = store
        .get_namespace(bucket, "sales")
        .await
        .expect("namespace lookup should succeed")
        .expect("namespace should remain");
    assert_eq!(
        stored.properties,
        BTreeMap::from([
            ("owner".to_string(), "platform".to_string()),
            ("retention".to_string(), "30d".to_string()),
        ])
    );
}

#[tokio::test]
async fn object_catalog_namespace_property_update_materializes_implicit_parent() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let parent = Namespace::parse("sales").expect("parent namespace should parse");
    let child = Namespace::parse("sales.daily").expect("child namespace should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket entry should be seeded");
    let child_entry = test_namespace_entry(bucket, &child);
    store
        .create_namespace(child_entry.clone())
        .await
        .expect("child namespace should be created");

    let no_change = store
        .update_namespace_properties(
            bucket,
            &parent.public_name(),
            NamespacePropertiesUpdate::try_new(vec!["missing".to_string()], BTreeMap::new())
                .expect("namespace update should validate"),
        )
        .await
        .expect("implicit parent no-op should succeed");
    assert_eq!(no_change.missing, vec!["missing".to_string()]);
    let parent_path = store.paths.namespace_entry_path(bucket, &parent);
    assert!(
        store
            .read_entry::<NamespaceEntry>(store.catalog_bucket(), &parent_path)
            .await
            .expect("implicit parent lookup should succeed")
            .is_none()
    );

    let result = store
        .update_namespace_properties(
            bucket,
            &parent.public_name(),
            NamespacePropertiesUpdate::try_new(
                vec!["missing".to_string()],
                BTreeMap::from([("owner".to_string(), "platform".to_string())]),
            )
            .expect("namespace update should validate"),
        )
        .await
        .expect("implicit parent should materialize");

    assert_eq!(result.updated, vec!["owner".to_string()]);
    assert!(result.removed.is_empty());
    assert_eq!(result.missing, vec!["missing".to_string()]);
    let (materialized, _) = store
        .read_entry::<NamespaceEntry>(store.catalog_bucket(), &parent_path)
        .await
        .expect("materialized parent should load")
        .expect("parent should have an explicit entry");
    assert_eq!(materialized.properties.get("owner").map(String::as_str), Some("platform"));
    assert_eq!(
        store
            .get_namespace(bucket, &child.public_name())
            .await
            .expect("child lookup should succeed"),
        Some(child_entry)
    );
}

#[tokio::test]
async fn object_catalog_namespace_property_update_materializes_resource_only_parents() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let table_namespace = Namespace::parse("table_only").expect("table namespace should parse");
    let view_namespace = Namespace::parse("view_only").expect("view namespace should parse");
    let table = IdentifierSegment::parse("orders").expect("table should parse");
    let view = IdentifierSegment::parse("recent_orders").expect("view should parse");
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket entry should be seeded");

    let table_entry = test_table_entry(
        bucket,
        &table_namespace,
        &table,
        default_table_metadata_file_path(&table_namespace, &table, "00001.metadata.json"),
    );
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &store.paths.table_entry_path(bucket, &table_namespace, &table),
            serde_json::to_vec(&table_entry).expect("table entry should serialize"),
        )
        .await;
    let view_entry = test_view_entry(
        bucket,
        &view_namespace,
        &view,
        default_view_metadata_file_path(&view_namespace, &view, "00001.view.json"),
    );
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &store.paths.view_entry_path(bucket, &view_namespace, &view),
            serde_json::to_vec(&view_entry).expect("view entry should serialize"),
        )
        .await;

    let table_before = store
        .load_table(bucket, &table_namespace.public_name(), table.as_str())
        .await
        .expect("table should load before materializing its namespace");
    let view_before = store
        .load_view(bucket, &view_namespace.public_name(), view.as_str())
        .await
        .expect("view should load before materializing its namespace");

    for namespace in [&table_namespace, &view_namespace] {
        let result = store
            .update_namespace_properties(
                bucket,
                &namespace.public_name(),
                NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "platform".to_string())]))
                    .expect("namespace update should validate"),
            )
            .await
            .expect("active resource should prove the implicit namespace");
        assert_eq!(result.updated, vec!["owner".to_string()]);
        let materialized = store
            .get_namespace(bucket, &namespace.public_name())
            .await
            .expect("materialized namespace should load")
            .expect("materialized namespace should exist");
        assert_eq!(materialized.properties.get("owner").map(String::as_str), Some("platform"));
    }

    assert_eq!(
        store
            .load_table(bucket, &table_namespace.public_name(), table.as_str())
            .await
            .expect("table should load after materializing its namespace"),
        table_before
    );
    assert_eq!(
        store
            .load_view(bucket, &view_namespace.public_name(), view.as_str())
            .await
            .expect("view should load after materializing its namespace"),
        view_before
    );
}

#[tokio::test]
async fn object_catalog_namespace_property_update_requires_etag_and_skips_noop_writes() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket entry should be seeded");

    let etagless = Namespace::parse("etagless").expect("namespace should parse");
    let mut etagless_entry = test_namespace_entry(bucket, &etagless);
    etagless_entry.properties.insert("owner".to_string(), "lakehouse".to_string());
    store
        .create_namespace(etagless_entry)
        .await
        .expect("etagless namespace should be seeded");
    let etagless_path = store.paths.namespace_entry_path(bucket, &etagless);
    backend.omit_etag_for_object(RUSTFS_META_BUCKET, &etagless_path).await;
    let etagless_puts = backend.put_attempt_count(RUSTFS_META_BUCKET, &etagless_path).await;

    assert_matches!(
        store
            .update_namespace_properties(
                bucket,
                &etagless.public_name(),
                NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "platform".to_string())]),)
                    .expect("namespace update should validate"),
            )
            .await,
        Err(TableCatalogStoreError::Internal(_))
    );
    assert_eq!(backend.put_attempt_count(RUSTFS_META_BUCKET, &etagless_path).await, etagless_puts);
    let unchanged = store
        .get_namespace(bucket, &etagless.public_name())
        .await
        .expect("etagless namespace should still load")
        .expect("etagless namespace should remain");
    assert_eq!(unchanged.properties.get("owner").map(String::as_str), Some("lakehouse"));

    let no_op = Namespace::parse("no_op").expect("namespace should parse");
    let mut no_op_entry = test_namespace_entry(bucket, &no_op);
    no_op_entry.properties.insert("owner".to_string(), "lakehouse".to_string());
    store
        .create_namespace(no_op_entry)
        .await
        .expect("no-op namespace should be seeded");
    let no_op_path = store.paths.namespace_entry_path(bucket, &no_op);
    backend.fail_next_put(RUSTFS_META_BUCKET, &no_op_path).await;
    let puts_before_no_op = backend.put_attempt_count(RUSTFS_META_BUCKET, &no_op_path).await;

    let result = store
        .update_namespace_properties(
            bucket,
            &no_op.public_name(),
            NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]))
                .expect("namespace update should validate"),
        )
        .await
        .expect("unchanged namespace properties should not write");
    assert_eq!(result.updated, vec!["owner".to_string()]);
    assert_eq!(backend.put_attempt_count(RUSTFS_META_BUCKET, &no_op_path).await, puts_before_no_op);

    assert_matches!(
        store
            .update_namespace_properties(
                bucket,
                &no_op.public_name(),
                NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "platform".to_string())]),)
                    .expect("namespace update should validate"),
            )
            .await,
        Err(TableCatalogStoreError::Internal(_))
    );
    let unchanged = store
        .get_namespace(bucket, &no_op.public_name())
        .await
        .expect("namespace should load after failed write")
        .expect("namespace should remain");
    assert_eq!(unchanged.properties.get("owner").map(String::as_str), Some("lakehouse"));
}

#[tokio::test]
async fn object_catalog_namespace_property_update_rejects_missing_inactive_and_corrupt_entries() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    store
        .put_table_bucket(test_bucket_entry(bucket))
        .await
        .expect("table bucket entry should be seeded");
    let update = || {
        NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "platform".to_string())]))
            .expect("namespace update should validate")
    };

    assert_matches!(
        store.update_namespace_properties(bucket, "missing", update()).await,
        Err(TableCatalogStoreError::NotFound(_))
    );

    let inactive = Namespace::parse("inactive").expect("inactive namespace should parse");
    let mut inactive_entry = test_namespace_entry(bucket, &inactive);
    inactive_entry.state = TableCatalogEntryState::Deleted;
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &store.paths.namespace_entry_path(bucket, &inactive),
            serde_json::to_vec(&inactive_entry).expect("inactive namespace should encode"),
        )
        .await;
    assert_matches!(
        store
            .update_namespace_properties(bucket, &inactive.public_name(), update())
            .await,
        Err(TableCatalogStoreError::NotFound(_))
    );

    let corrupt = Namespace::parse("corrupt").expect("corrupt namespace should parse");
    backend
        .seed_object(RUSTFS_META_BUCKET, &store.paths.namespace_entry_path(bucket, &corrupt), b"{".to_vec())
        .await;
    assert_matches!(
        store
            .update_namespace_properties(bucket, &corrupt.public_name(), update())
            .await,
        Err(TableCatalogStoreError::Invalid(_))
    );

    let semantically_corrupt = Namespace::parse("semantically_corrupt").expect("corrupt namespace should parse");
    let mut semantically_corrupt_entry = test_namespace_entry(bucket, &semantically_corrupt);
    semantically_corrupt_entry.properties = (0..=NAMESPACE_PROPERTIES_MAX_ENTRIES)
        .map(|index| (format!("key{index}"), "value".to_string()))
        .collect();
    let semantically_corrupt_path = store.paths.namespace_entry_path(bucket, &semantically_corrupt);
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &semantically_corrupt_path,
            serde_json::to_vec(&semantically_corrupt_entry).expect("corrupt namespace should encode"),
        )
        .await;
    let put_attempts = backend
        .put_attempt_count(RUSTFS_META_BUCKET, &semantically_corrupt_path)
        .await;
    let repair_update =
        NamespacePropertiesUpdate::try_new(vec![format!("key{NAMESPACE_PROPERTIES_MAX_ENTRIES}")], BTreeMap::new())
            .expect("repair request should validate structurally");

    assert_matches!(
        store
            .update_namespace_properties(bucket, &semantically_corrupt.public_name(), repair_update,)
            .await,
        Err(TableCatalogStoreError::Invalid(_))
    );
    assert_eq!(
        backend
            .put_attempt_count(RUSTFS_META_BUCKET, &semantically_corrupt_path)
            .await,
        put_attempts
    );
    let persisted = store
        .read_entry::<NamespaceEntry>(RUSTFS_META_BUCKET, &semantically_corrupt_path)
        .await
        .expect("corrupt namespace lookup should succeed")
        .expect("corrupt namespace should remain")
        .0;
    assert_eq!(persisted.properties.len(), NAMESPACE_PROPERTIES_MAX_ENTRIES + 1);
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

#[tokio::test]
async fn object_catalog_namespace_property_update_is_fenced_by_observed_etag() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
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

    let namespace_path = store.paths.namespace_entry_path(bucket, &namespace);
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &namespace_path).await;
    let stale_store = store.clone();
    let stale_update = tokio::spawn(async move {
        stale_store
            .update_namespace_properties(
                bucket,
                "sales",
                NamespacePropertiesUpdate::try_new(Vec::new(), BTreeMap::from([("owner".to_string(), "stale".to_string())]))
                    .expect("namespace update should validate"),
            )
            .await
    });
    pause.wait_started().await;

    let mut winner = test_namespace_entry(bucket, &namespace);
    winner.properties.insert("owner".to_string(), "winner".to_string());
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &namespace_path,
            serde_json::to_vec(&winner).expect("winning namespace should encode"),
        )
        .await;
    pause.release();

    assert_matches!(
        stale_update.await.expect("stale namespace update task should finish"),
        Err(TableCatalogStoreError::Conflict(_))
    );
    let stored = store
        .get_namespace(bucket, &namespace.public_name())
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
async fn object_catalog_renamed_table_drop_rejects_same_id_index_at_a_stale_identifier() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let source_table = IdentifierSegment::parse("orders").unwrap();
    let destination_table = IdentifierSegment::parse("orders_v2").unwrap();
    let current = default_table_metadata_file_path(&namespace, &source_table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &source_table, current).await;
    store
        .rename_table(bucket, "sales", "orders", "sales", "orders_v2")
        .await
        .unwrap();
    let renamed = store.load_table(bucket, "sales", "orders_v2").await.unwrap().unwrap();
    let index_path = store.paths.warehouse_index_entry_path(bucket, "tables/table-id/");
    let (mut stale_index, _) = store
        .read_entry::<TableWarehouseIndexEntry>(RUSTFS_META_BUCKET, &index_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stale_index, table_warehouse_index_entry(&renamed).unwrap());
    stale_index.namespace = "sales".to_string();
    stale_index.table = "orders".to_string();
    store
        .write_entry(RUSTFS_META_BUCKET, &index_path, &stale_index, TableCatalogPutPrecondition::Any)
        .await
        .unwrap();
    let entry_path = store.paths.table_entry_path(bucket, &namespace, &destination_table);
    let before_entry = backend.read_object(RUSTFS_META_BUCKET, &entry_path).await.unwrap().unwrap();
    let before_index = backend.read_object(RUSTFS_META_BUCKET, &index_path).await.unwrap().unwrap();

    assert_matches!(
        store.drop_table(bucket, "sales", "orders_v2").await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("owner changed")
    );
    assert_eq!(backend.read_object(RUSTFS_META_BUCKET, &entry_path).await.unwrap(), Some(before_entry));
    assert_eq!(backend.read_object(RUSTFS_META_BUCKET, &index_path).await.unwrap(), Some(before_index));
    assert_eq!(store.load_table(bucket, "sales", "orders_v2").await.unwrap(), Some(renamed));
    assert!(store.load_table(bucket, "sales", "orders").await.unwrap().is_none());
}

#[tokio::test]
async fn object_catalog_source_recreation_after_rename_preserves_the_original_identity_and_receipts() {
    for destination_namespace in ["sales", "curated"] {
        let backend = TestCatalogObjectBackend {
            reject_reads_while_write_locked: true,
            ..Default::default()
        };
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let source_namespace = Namespace::parse("sales").unwrap();
        let source_table = IdentifierSegment::parse("orders").unwrap();
        let destination = Namespace::parse(destination_namespace).unwrap();
        let destination_table = IdentifierSegment::parse("orders_v2").unwrap();
        let current = default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json");
        let next = default_table_metadata_file_path(&source_namespace, &source_table, "00002.metadata.json");
        seed_table_for_metadata_maintenance(&store, bucket, &source_namespace, &source_table, current.clone()).await;
        if destination_namespace != "sales" {
            store
                .create_namespace(test_namespace_entry(bucket, &destination))
                .await
                .unwrap();
        }
        let original = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
        let mut metadata = super::test_support::table_metadata_json(&original.table_uuid, &original.warehouse_location);
        metadata["format-version"] = serde_json::json!(original.format_version);
        let metadata_bytes = serde_json::to_vec(&metadata).unwrap();
        backend.seed_object(bucket, &current, metadata_bytes.clone()).await;
        backend.seed_object(bucket, &next, metadata_bytes).await;
        backend
            .seed_object(bucket, "tables/table-id/data/retained.bin", b"original table data".to_vec())
            .await;
        store
            .commit_table(TableCommitRequest {
                table_bucket: bucket.to_string(),
                namespace: source_namespace.public_name(),
                table: source_table.as_str().to_string(),
                commit_id: "source-recreate-commit".to_string(),
                idempotency_key: Some("source-recreate-request".to_string()),
                operation: "update-metadata".to_string(),
                expected_version_token: original.version_token,
                expected_metadata_location: current.clone(),
                new_metadata_location: next.clone(),
                requirements: Vec::new(),
                writer: Some("source-recreate-test".to_string()),
            })
            .await
            .unwrap();
        store
            .rename_table(bucket, "sales", "orders", destination_namespace, "orders_v2")
            .await
            .unwrap();
        let restarted = ObjectTableCatalogStore::new(backend.clone());
        let renamed = restarted
            .load_table(bucket, destination_namespace, "orders_v2")
            .await
            .unwrap()
            .unwrap();
        let source_path = restarted.paths.table_entry_path(bucket, &source_namespace, &source_table);
        let (tombstone, _) = restarted
            .read_entry::<TableEntry>(RUSTFS_META_BUCKET, &source_path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tombstone.state, TableCatalogEntryState::Deleted);
        assert_eq!(tombstone.table_id, renamed.table_id);
        let before_commit = restarted
            .get_commit_by_id(bucket, &renamed.table_id, "source-recreate-commit")
            .await
            .unwrap()
            .unwrap();
        let before_idempotency = restarted
            .get_commit_by_idempotency_key(bucket, &renamed.table_id, "source-recreate-request")
            .await
            .unwrap()
            .unwrap();
        let renamed_path = restarted.paths.table_entry_path(bucket, &destination, &destination_table);
        let index_path = restarted.paths.warehouse_index_entry_path(bucket, "tables/table-id/");
        let before_entry = backend.read_object(RUSTFS_META_BUCKET, &renamed_path).await.unwrap().unwrap();
        let before_index = backend.read_object(RUSTFS_META_BUCKET, &index_path).await.unwrap().unwrap();
        let before_current = backend.read_object(bucket, &current).await.unwrap().unwrap();
        let before_next = backend.read_object(bucket, &next).await.unwrap().unwrap();
        let before_data = backend
            .read_object(bucket, "tables/table-id/data/retained.bin")
            .await
            .unwrap()
            .unwrap();
        let replacement_metadata =
            default_table_metadata_file_path(&source_namespace, &source_table, "00001-replacement.metadata.json");
        let mut replacement = test_table_entry(bucket, &source_namespace, &source_table, replacement_metadata.clone());
        replacement.table_id = "replacement-table-id".to_string();
        replacement.table_uuid = "replacement-table-uuid".to_string();
        replacement.warehouse_location = "s3://analytics/tables/replacement-table-id".to_string();
        let replacement_json = super::test_support::table_metadata_json(&replacement.table_uuid, &replacement.warehouse_location);
        backend
            .seed_object(bucket, &replacement_metadata, serde_json::to_vec(&replacement_json).unwrap())
            .await;

        restarted
            .create_table(replacement.clone())
            .await
            .expect("source recreation should not re-lock its retained tombstone");

        assert_ne!(replacement.table_id, renamed.table_id);
        assert_ne!(replacement.table_uuid, renamed.table_uuid);
        assert_ne!(replacement.warehouse_location, renamed.warehouse_location);
        assert_ne!(replacement.metadata_location, renamed.metadata_location);
        for reader in [restarted, ObjectTableCatalogStore::new(backend.clone())] {
            assert_eq!(reader.load_table(bucket, "sales", "orders").await.unwrap(), Some(replacement.clone()));
            assert_eq!(
                reader.load_table(bucket, destination_namespace, "orders_v2").await.unwrap(),
                Some(renamed.clone())
            );
            assert_eq!(
                reader
                    .get_commit_by_id(bucket, &renamed.table_id, "source-recreate-commit")
                    .await
                    .unwrap(),
                Some(before_commit.clone())
            );
            assert_eq!(
                reader
                    .get_commit_by_idempotency_key(bucket, &renamed.table_id, "source-recreate-request")
                    .await
                    .unwrap(),
                Some(before_idempotency.clone())
            );
            assert!(
                reader
                    .get_commit_by_id(bucket, &replacement.table_id, "source-recreate-commit")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                reader
                    .get_commit_by_idempotency_key(bucket, &replacement.table_id, "source-recreate-request")
                    .await
                    .unwrap()
                    .is_none()
            );
            let original_resource = reader
                .resolve_table_data_plane_resource(bucket, "tables/table-id/data/retained.bin")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(original_resource.table_id, renamed.table_id);
            assert_eq!(original_resource.namespace, destination_namespace);
            assert_eq!(original_resource.table, "orders_v2");
            let replacement_resource = reader
                .resolve_table_data_plane_resource(bucket, "tables/replacement-table-id/data/file.bin")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(replacement_resource.table_id, replacement.table_id);
            assert_eq!(replacement_resource.namespace, "sales");
            assert_eq!(replacement_resource.table, "orders");
        }
        assert_eq!(backend.read_object(RUSTFS_META_BUCKET, &renamed_path).await.unwrap(), Some(before_entry));
        assert_eq!(backend.read_object(RUSTFS_META_BUCKET, &index_path).await.unwrap(), Some(before_index));
        assert_eq!(backend.read_object(bucket, &current).await.unwrap(), Some(before_current));
        assert_eq!(backend.read_object(bucket, &next).await.unwrap(), Some(before_next));
        assert_eq!(
            backend
                .read_object(bucket, "tables/table-id/data/retained.bin")
                .await
                .unwrap(),
            Some(before_data)
        );
    }
}

#[tokio::test]
async fn object_catalog_source_recreation_keeps_identifier_and_warehouse_conflicts_fail_closed() {
    for existing_identifier in [true, false] {
        let backend = TestCatalogObjectBackend {
            reject_reads_while_write_locked: true,
            ..Default::default()
        };
        let store = ObjectTableCatalogStore::new(backend.clone());
        let bucket = "analytics";
        let source_namespace = Namespace::parse("sales").unwrap();
        let source_table = IdentifierSegment::parse("orders").unwrap();
        let destination_namespace = Namespace::parse("curated").unwrap();
        let destination_table = IdentifierSegment::parse("orders_v2").unwrap();
        let current = default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json");
        seed_table_for_metadata_maintenance(&store, bucket, &source_namespace, &source_table, current).await;
        store
            .create_namespace(test_namespace_entry(bucket, &destination_namespace))
            .await
            .unwrap();
        store
            .rename_table(bucket, "sales", "orders", "curated", "orders_v2")
            .await
            .unwrap();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let (namespace, table) = if existing_identifier {
            (&destination_namespace, &destination_table)
        } else {
            (&source_namespace, &source_table)
        };
        let mut candidate = test_table_entry(
            bucket,
            namespace,
            table,
            default_table_metadata_file_path(namespace, table, "00001-candidate.metadata.json"),
        );
        candidate.table_id = "candidate-table-id".to_string();
        candidate.table_uuid = "candidate-table-uuid".to_string();
        candidate.warehouse_location = if existing_identifier {
            "s3://analytics/tables/candidate-table-id".to_string()
        } else {
            "s3://analytics/tables/table-id/nested".to_string()
        };
        let paths = backend.list_objects(RUSTFS_META_BUCKET, "").await.unwrap();
        let mut before = Vec::new();
        for path in &paths {
            before.push((path.clone(), backend.read_object(RUSTFS_META_BUCKET, path).await.unwrap().unwrap()));
        }

        let error = store
            .create_table(candidate)
            .await
            .expect_err("conflicting registration must fail closed");

        assert_matches!(error, TableCatalogStoreError::Conflict(message) if if existing_identifier {
            message.contains("already exists")
        } else {
            message.contains("overlaps an active table")
        });
        assert_eq!(backend.list_objects(RUSTFS_META_BUCKET, "").await.unwrap(), paths);
        for (path, object) in before {
            assert_eq!(backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap(), Some(object));
        }
    }
}

#[tokio::test]
async fn object_catalog_warehouse_scans_keep_read_locks_for_unheld_table_entries() {
    let backend = TestCatalogObjectBackend {
        reject_reads_while_write_locked: true,
        ..Default::default()
    };
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let owner_table = IdentifierSegment::parse("orders").unwrap();
    let candidate_table = IdentifierSegment::parse("returns").unwrap();
    let current = default_table_metadata_file_path(&namespace, &owner_table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &owner_table, current).await;
    let owner = store.load_table(bucket, "sales", "orders").await.unwrap().unwrap();
    let owner_path = store.paths.table_entry_path(bucket, &namespace, &owner_table);
    let before_owner = backend.read_object(RUSTFS_META_BUCKET, &owner_path).await.unwrap().unwrap();
    let mut candidate = test_table_entry(
        bucket,
        &namespace,
        &candidate_table,
        default_table_metadata_file_path(&namespace, &candidate_table, "00001-candidate.metadata.json"),
    );
    candidate.table_id = "candidate-table-id".to_string();
    candidate.table_uuid = "candidate-table-uuid".to_string();
    candidate.warehouse_location = "s3://analytics/tables/candidate-table-id".to_string();
    let guard = backend.acquire_write_lock(RUSTFS_META_BUCKET, &owner_path).await.unwrap();

    assert_matches!(
        store.ensure_table_warehouse_location_available(&candidate).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("read attempted while its write lock is held") && message.contains(&owner_path)
    );
    assert_matches!(
        store.create_table(candidate.clone()).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("read attempted while its write lock is held") && message.contains(&owner_path)
    );
    drop(guard);

    assert_eq!(store.load_table(bucket, "sales", "orders").await.unwrap(), Some(owner));
    assert!(store.load_table(bucket, "sales", "returns").await.unwrap().is_none());
    assert_eq!(backend.read_object(RUSTFS_META_BUCKET, &owner_path).await.unwrap(), Some(before_owner));
    store.ensure_table_warehouse_location_available(&candidate).await.unwrap();
    store
        .create_table(candidate.clone())
        .await
        .expect("unlocked foreign entries should remain normally readable");
    assert_eq!(store.load_table(bucket, "sales", "returns").await.unwrap(), Some(candidate));
}

#[tokio::test]
async fn object_catalog_table_rename_rejects_missing_and_conflicting_destinations() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend);
    let bucket = "analytics";
    let source_namespace = Namespace::parse("sales").unwrap();
    let destination_namespace = Namespace::parse("curated").unwrap();
    let source_table = IdentifierSegment::parse("orders").unwrap();
    let destination_table = IdentifierSegment::parse("orders_v2").unwrap();
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &source_namespace))
        .await
        .unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &destination_namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(
            bucket,
            &source_namespace,
            &source_table,
            default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json"),
        ))
        .await
        .unwrap();

    assert_matches!(
        store.rename_table(bucket, "sales", "orders", "missing", "orders_v2").await,
        Err(TableCatalogStoreError::NamespaceNotFound(_))
    );
    assert_matches!(
        store.rename_table(bucket, "sales", "missing", "curated", "orders_v2").await,
        Err(TableCatalogStoreError::TableNotFound(_))
    );
    let mut existing = test_table_entry(
        bucket,
        &destination_namespace,
        &destination_table,
        default_table_metadata_file_path(&destination_namespace, &destination_table, "00001.metadata.json"),
    );
    existing.table_id = "destination-table-id".to_string();
    existing.table_uuid = "destination-table-uuid".to_string();
    existing.warehouse_location = "s3://analytics/tables/destination-table-id".to_string();
    store.create_table(existing).await.unwrap();
    assert_matches!(
        store.rename_table(bucket, "sales", "orders", "curated", "orders_v2").await,
        Err(TableCatalogStoreError::AlreadyExists(_))
    );
    let destination_view = IdentifierSegment::parse("orders_view").unwrap();
    store
        .create_view(test_view_entry(
            bucket,
            &destination_namespace,
            &destination_view,
            default_view_metadata_file_path(&destination_namespace, &destination_view, "00001.metadata.json"),
        ))
        .await
        .unwrap();
    assert_matches!(
        store.rename_table(bucket, "sales", "orders", "curated", "orders_view").await,
        Err(TableCatalogStoreError::AlreadyExists(_))
    );
    assert!(store.load_table(bucket, "sales", "orders").await.unwrap().is_some());
}

#[tokio::test]
async fn object_catalog_table_rename_fails_closed_around_durable_fence_creation() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let source_namespace = Namespace::parse("sales").unwrap();
    let destination_namespace = Namespace::parse("curated").unwrap();
    let source_table = IdentifierSegment::parse("orders").unwrap();
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &source_namespace))
        .await
        .unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &destination_namespace))
        .await
        .unwrap();
    store
        .create_table(test_table_entry(
            bucket,
            &source_namespace,
            &source_table,
            default_table_metadata_file_path(&source_namespace, &source_table, "00001.metadata.json"),
        ))
        .await
        .unwrap();

    let bucket_object = store.paths.table_bucket_entry_path(bucket);
    backend.fail_next_put(RUSTFS_META_BUCKET, &bucket_object).await;
    assert_matches!(
        store.rename_table(bucket, "sales", "orders", "curated", "orders_v2").await,
        Err(TableCatalogStoreError::Internal(_))
    );
    assert!(
        store
            .get_table_bucket(bucket)
            .await
            .unwrap()
            .expect("table bucket should remain")
            .active_rename_id
            .is_none()
    );
    assert!(store.load_table(bucket, "sales", "orders").await.unwrap().is_some());
    assert!(store.load_table(bucket, "curated", "orders_v2").await.unwrap().is_none());

    let mut fenced_bucket = store.get_table_bucket(bucket).await.unwrap().unwrap();
    fenced_bucket.active_rename_id = Some("missing-intent".to_string());
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &bucket_object,
            serde_json::to_vec(&fenced_bucket).expect("fenced bucket should serialize"),
        )
        .await;
    assert_matches!(
        store.rename_table(bucket, "sales", "orders", "curated", "orders_v2").await,
        Err(TableCatalogStoreError::Unavailable(_))
    );
    assert_eq!(
        store
            .get_table_bucket(bucket)
            .await
            .unwrap()
            .expect("table bucket should remain fail-closed")
            .active_rename_id
            .as_deref(),
        Some("missing-intent")
    );
    assert_matches!(
        store
            .resolve_table_data_plane_resource(bucket, "tables/table-id/data/part.parquet")
            .await,
        Err(TableCatalogStoreError::Unavailable(_))
    );
}

#[tokio::test]
async fn configured_object_catalog_dispatches_table_rename() {
    let store =
        ConfiguredTableCatalogStore::new_for_test(TestCatalogObjectBackend::default(), TableCatalogBackingMode::ObjectBacked);

    assert_matches!(
        store
            .rename_table("analytics", "sales", "orders", "curated", "orders_v2")
            .await,
        Err(TableCatalogStoreError::NotFound(_))
    );
}

async fn assert_disable_empty_table_bucket<S: TableCatalogStore>(store: &S) {
    let bucket = "disable-empty";
    let namespace = Namespace::parse("sales").unwrap();
    store.ensure_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    assert_matches!(
        store.disable_empty_table_bucket(test_bucket_entry(bucket)).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    assert_eq!(
        store.get_table_bucket(bucket).await.unwrap().unwrap().state,
        TableCatalogEntryState::Active
    );
    store.drop_namespace(bucket, "sales").await.unwrap();
    store.disable_empty_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store.disable_empty_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    assert_eq!(
        store.get_table_bucket(bucket).await.unwrap().unwrap().state,
        TableCatalogEntryState::Deleted
    );
    // A request that observed the old enabled marker cannot reactivate the entry.
    assert_matches!(
        store.ensure_table_bucket(test_bucket_entry(bucket)).await,
        Err(TableCatalogStoreError::NotFound(_))
    );
    assert_matches!(
        store.create_namespace(test_namespace_entry(bucket, &namespace)).await,
        Err(TableCatalogStoreError::NotFound(_))
    );
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
}

#[tokio::test]
async fn object_maintenance_refuses_roots_shared_after_cross_namespace_rename() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let destination = Namespace::parse("private").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let original_metadata = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let replacement_metadata = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, original_metadata.clone()).await;
    backend
        .seed_object(bucket, &original_metadata, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store
        .create_namespace(test_namespace_entry(bucket, &destination))
        .await
        .unwrap();
    store
        .rename_table(bucket, "sales", "orders", "private", "orders")
        .await
        .unwrap();
    let mut replacement = test_table_entry(bucket, &namespace, &table, replacement_metadata.clone());
    replacement.table_id = "replacement-id".to_string();
    replacement.table_uuid = "replacement-uuid".to_string();
    replacement.warehouse_location = format!("s3://{bucket}/tables/replacement-id");
    backend
        .seed_object(bucket, &replacement_metadata, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    store.create_table(replacement).await.unwrap();
    let delete_attempts_before = backend.state.lock().await.delete_attempts.clone();
    assert_matches!(store.plan_table_metadata_maintenance(bucket, "sales", "orders", 0).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("exclusive metadata"));
    assert_matches!(store.delete_table_metadata_maintenance_candidates(bucket, "sales", "orders", 0).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("exclusive metadata"));
    assert!(backend.object_exists(bucket, &original_metadata).await.unwrap());
    assert_eq!(
        store
            .load_table(bucket, "private", "orders")
            .await
            .unwrap()
            .unwrap()
            .metadata_location,
        original_metadata
    );
    assert_eq!(backend.state.lock().await.delete_attempts, delete_attempts_before);
}

#[tokio::test]
async fn object_maintenance_rechecks_root_ownership_under_publication_fence() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let other_namespace = Namespace::parse("private").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let orphan = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    backend.seed_object(bucket, &orphan, br#"{"metadata-log":[]}"#.to_vec()).await;
    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .unwrap();
    assert_eq!(report.deletable_metadata_locations, vec![orphan.clone()]);
    store
        .create_namespace(test_namespace_entry(bucket, &other_namespace))
        .await
        .unwrap();
    let mut other = test_table_entry(bucket, &other_namespace, &table, orphan.clone());
    other.table_id = "other-id".to_string();
    other.table_uuid = "other-uuid".to_string();
    other.warehouse_location = format!("s3://{bucket}/tables/other-id");
    store.create_table(other.clone()).await.unwrap();
    assert_matches!(store.delete_table_metadata_maintenance_report(bucket, "sales", "orders", report.clone()).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("exclusive metadata"));
    assert!(backend.object_exists(bucket, &orphan).await.unwrap());
    store.drop_table(bucket, "private", "orders").await.unwrap();

    let pause = backend.pause_next_delete(bucket, &orphan).await;
    let deleting_store = store.clone();
    let deleting = tokio::spawn(async move {
        deleting_store
            .delete_table_metadata_maintenance_report(bucket, "sales", "orders", report)
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .unwrap();
    let publication_lock = default_table_bucket_publication_lock_path();
    let attempts = backend.write_lock_acquisition_count(bucket, &publication_lock).await;
    let creating_store = store.clone();
    let creating = tokio::spawn(async move { creating_store.create_table(other).await });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        while backend.write_lock_acquisition_count(bucket, &publication_lock).await == attempts {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!creating.is_finished(), "new owner cannot publish between root validation and deletion");
    creating.abort();
    let _ = creating.await;
    pause.release();
    let deleted = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, deleting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(deleted.deletable_metadata_locations, vec![orphan.clone()]);
    assert!(!backend.object_exists(bucket, &orphan).await.unwrap());
    assert!(backend.object_exists(bucket, &current).await.unwrap());
}

#[tokio::test]
async fn object_maintenance_rejects_a_report_from_a_replaced_table_incarnation() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let orphan = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    backend.seed_object(bucket, &orphan, br#"{"metadata-log":[]}"#.to_vec()).await;
    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .unwrap();
    store.drop_table(bucket, "sales", "orders").await.unwrap();
    let mut replacement = test_table_entry(bucket, &namespace, &table, current.clone());
    replacement.table_id = "new-incarnation".to_string();
    replacement.table_uuid = "new-uuid".to_string();
    replacement.warehouse_location = format!("s3://{bucket}/tables/new-incarnation");
    store.create_table(replacement).await.unwrap();
    assert_matches!(store.delete_table_metadata_maintenance_report(bucket, "sales", "orders", report).await,
        Err(TableCatalogStoreError::Invalid(message)) if message.contains("identity does not match"));
    assert!(backend.object_exists(bucket, &current).await.unwrap());
    assert!(backend.object_exists(bucket, &orphan).await.unwrap());
}

#[tokio::test]
async fn maintenance_orphan_planning_and_deletion_never_read_object_bodies() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    seed_table_for_metadata_maintenance(&store, bucket, &namespace, &table, current.clone()).await;
    backend
        .seed_object(bucket, &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    let root = format!("{}{}/", default_table_root_prefix(&namespace), table.as_str());
    let old = format!("{root}data/old.parquet");
    let fresh = format!("{root}delete/fresh.parquet");
    let unknown = format!("{root}metadata/unknown.avro");
    backend.seed_object(bucket, &old, b"body must not be read".to_vec()).await;
    backend
        .seed_object_with_mod_time(bucket, &fresh, vec![1], Some(OffsetDateTime::now_utc()))
        .await;
    backend.seed_object_with_mod_time(bucket, &unknown, vec![2], None).await;
    for object in [&old, &fresh, &unknown] {
        backend.fail_next_read(bucket, object).await;
    }
    let report = store
        .plan_table_metadata_maintenance(bucket, "sales", "orders", 0)
        .await
        .expect("stat-only dry run");
    assert_eq!(report.deletable_object_locations, vec![old.clone()]);
    assert_eq!(
        object_cleanup_report(&report, &fresh).state,
        TableMetadataMaintenanceObjectState::PendingSafetyWindow
    );
    assert_eq!(
        object_cleanup_report(&report, &unknown).state,
        TableMetadataMaintenanceObjectState::PendingSafetyWindow
    );
    let deleted = store
        .delete_table_metadata_maintenance_candidates(bucket, "sales", "orders", 0)
        .await
        .expect("stat-only deletion recheck");
    assert_eq!(deleted.deletable_object_locations, vec![old.clone()]);
    assert!(!backend.object_exists(bucket, &old).await.unwrap());
    assert!(backend.object_exists(bucket, &fresh).await.unwrap());
    assert!(backend.object_exists(bucket, &unknown).await.unwrap());
    let state = backend.state.lock().await;
    for object in [&old, &fresh, &unknown] {
        assert_eq!(
            state.read_attempts.get(&(bucket.to_string(), object.to_string())),
            None,
            "orphan body must never be fetched"
        );
    }
}

#[tokio::test]
async fn compaction_planner_stats_data_files_without_reading_their_bodies() {
    let backend = TestCatalogObjectBackend::default();
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let metadata_dir = default_table_metadata_dir_path(&namespace, &table);
    let current = default_table_metadata_file_path(&namespace, &table, "00004.metadata.json");
    let entry = test_table_entry(bucket, &namespace, &table, current);
    let data_dir = default_table_data_dir_path(&namespace, &table);
    let files = [format!("{data_dir}/left.parquet"), format!("{data_dir}/right.parquet")];
    let manifest = format!("{metadata_dir}/manifest-20.avro");
    let manifest_list = format!("{metadata_dir}/snap-20.avro");
    let manifest_bytes = manifest_avro_bytes(&[(&files[0], 0), (&files[1], 0)]);
    backend
        .seed_object(bucket, &manifest_list, manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())]))
        .await;
    backend.seed_object(bucket, &manifest, manifest_bytes).await;
    for file in &files {
        backend.seed_object(bucket, file, vec![0; 8192]).await;
        backend.fail_next_read(bucket, file).await;
    }
    let metadata = serde_json::json!({
        "current-snapshot-id": 20,
        "snapshots": [{"snapshot-id": 20, "manifest-list": manifest_list}],
        "refs": {"main": {"snapshot-id": 20, "type": "branch"}}
    });
    backend.reset_call_counts().await;
    let report = table_compaction_planning_report(
        &backend,
        bucket,
        &namespace,
        &table,
        &entry,
        &metadata,
        TableCompactionPlanningConfig {
            target_file_size_bytes: 65536,
            small_file_threshold_bytes: 16384,
            min_input_files: 2,
            max_rewrite_bytes_per_job: 65536,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.status, TableCompactionPlanningStatus::RewriteCandidates);
    assert_eq!(report.candidate_file_count, 2);
    assert_eq!(backend.metadata_call_count().await, 2);
    assert_eq!(backend.read_call_count().await, 2, "only the manifest list and manifest may be read");
}
