// Copyright 2026 RustFS Team
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

async fn planning_fixture(
    metadata_location: &str,
) -> (TestCatalogObjectBackend, ObjectTableCatalogStore<TestCatalogObjectBackend>, TableEntry) {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let namespace = Namespace::parse("sales").expect("namespace");
    let table = IdentifierSegment::parse("orders").expect("table");
    seed_table_for_metadata_maintenance(&store, "analytics", &namespace, &table, metadata_location.to_string()).await;
    let object = table_catalog_object_key_from_location("analytics", metadata_location).expect("metadata object key");
    backend
        .seed_object(
            "analytics",
            &object,
            serde_json::to_vec(&table_metadata_json_for_backup("analytics", "table-id", "table-uuid"))
                .expect("valid table metadata"),
        )
        .await;
    let entry = store.load_table("analytics", "sales", "orders").await.unwrap().unwrap();
    (backend, store, entry)
}

#[tokio::test]
async fn maintenance_planning_after_rename_keeps_the_original_pointer_and_identity() {
    let namespace = Namespace::parse("sales").expect("namespace");
    let table = IdentifierSegment::parse("orders").expect("table");
    let current = default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");
    let orphan = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let (backend, store, original) = planning_fixture(&current).await;
    backend.seed_object("analytics", &orphan, b"{}".to_vec()).await;
    store
        .rename_table("analytics", "sales", "orders", "sales", "archived_orders")
        .await
        .expect("rename through the object catalog");
    let renamed = store
        .load_table("analytics", "sales", "archived_orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renamed.metadata_location, original.metadata_location);
    assert_eq!(renamed.table_id, original.table_id);
    assert_eq!(renamed.table_uuid, original.table_uuid);
    assert_eq!(renamed.version_token, original.version_token);
    assert_eq!(renamed.generation, original.generation);

    let before = maintenance_test_object_versions(&backend).await;
    let report = store
        .plan_table_metadata_maintenance("analytics", "sales", "archived_orders", 0)
        .await
        .expect("planning follows the renamed entry's persisted directory");
    assert_eq!(report.job.table, "archived_orders");
    assert_eq!(report.job.table_id, original.table_id);
    assert_eq!(report.current_metadata_location, current);
    assert_eq!(report.cleanup_candidate_locations, vec![orphan]);
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
    assert_eq!(store.load_table("analytics", "sales", "archived_orders").await.unwrap(), Some(renamed));
}

#[tokio::test]
async fn maintenance_planning_does_not_follow_a_recreated_source_name() {
    let current = "tables/table-id/metadata/00003.metadata.json";
    let orphan = "tables/table-id/metadata/00001.metadata.json";
    let (backend, store, original) = planning_fixture(current).await;
    backend.seed_object("analytics", orphan, b"{}".to_vec()).await;
    store
        .rename_table("analytics", "sales", "orders", "sales", "archived_orders")
        .await
        .unwrap();
    let recreated_current = "tables/recreated-id/metadata/00003.metadata.json";
    let recreated_orphan = "tables/recreated-id/metadata/00001.metadata.json";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let mut recreated = test_table_entry("analytics", &namespace, &table, recreated_current.to_string());
    recreated.table_id = "recreated-id".to_string();
    recreated.table_uuid = "recreated-uuid".to_string();
    recreated.warehouse_location = "s3://analytics/tables/recreated-id".to_string();
    backend
        .seed_object(
            "analytics",
            recreated_current,
            serde_json::to_vec(&table_metadata_json_for_backup("analytics", "recreated-id", "recreated-uuid")).unwrap(),
        )
        .await;
    backend.seed_object("analytics", recreated_orphan, b"{}".to_vec()).await;
    store
        .create_table(recreated.clone())
        .await
        .expect("recreate the old name with a new identity");

    let before = maintenance_test_object_versions(&backend).await;
    let destination_report = store
        .plan_table_metadata_maintenance("analytics", "sales", "archived_orders", 0)
        .await
        .expect("plan the renamed table");
    let source_report = store
        .plan_table_metadata_maintenance("analytics", "sales", "orders", 0)
        .await
        .expect("plan the recreated source");
    assert_eq!(destination_report.job.table_id, original.table_id);
    assert_eq!(destination_report.cleanup_candidate_locations, vec![orphan.to_string()]);
    assert_eq!(source_report.job.table_id, recreated.table_id);
    assert_eq!(source_report.cleanup_candidate_locations, vec![recreated_orphan.to_string()]);
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}

#[tokio::test]
async fn custom_metadata_uri_dry_run_never_marks_the_current_object_for_cleanup() {
    let current = "tables/table-id/metadata/00003.metadata.json";
    let orphan = "tables/table-id/metadata/00001.metadata.json";
    let uri = format!("s3://analytics/{current}");
    let (backend, store, entry) = planning_fixture(&uri).await;
    backend.seed_object("analytics", orphan, b"{}".to_vec()).await;
    let before = maintenance_test_object_versions(&backend).await;
    let report = store
        .plan_table_metadata_maintenance("analytics", "sales", "orders", 0)
        .await
        .expect("plan the same-bucket metadata URI");
    assert_eq!(report.current_metadata_location, uri);
    assert_eq!(report.cleanup_candidate_locations, vec![orphan.to_string()]);
    assert!(
        !report
            .deletable_metadata_locations
            .iter()
            .any(|location| location == current || location == &uri)
    );
    assert_eq!(store.load_table("analytics", "sales", "orders").await.unwrap(), Some(entry));
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}

#[tokio::test]
async fn maintenance_planning_rejects_a_metadata_directory_shared_by_active_owners() {
    let first_current = "tables/shared/metadata/orders.metadata.json";
    let second_current = "tables/shared/metadata/invoices.metadata.json";
    let (backend, store, _) = planning_fixture(first_current).await;
    let namespace = Namespace::parse("sales").unwrap();
    let second = IdentifierSegment::parse("invoices").unwrap();
    let mut entry = test_table_entry("analytics", &namespace, &second, second_current.to_string());
    entry.table_id = "invoice-id".to_string();
    entry.table_uuid = "invoice-uuid".to_string();
    entry.warehouse_location = "s3://analytics/tables/invoice-id".to_string();
    backend
        .seed_object(
            "analytics",
            second_current,
            serde_json::to_vec(&table_metadata_json_for_backup("analytics", "invoice-id", "invoice-uuid")).unwrap(),
        )
        .await;
    store
        .create_table(entry)
        .await
        .expect("the catalog can represent distinct warehouses with external metadata");
    let before = maintenance_test_object_versions(&backend).await;
    let result = store.plan_table_metadata_maintenance("analytics", "sales", "orders", 0).await;
    assert!(
        matches!(result, Err(TableCatalogStoreError::Conflict(_) | TableCatalogStoreError::Invalid(_))),
        "a shared directory must not turn another active table's current metadata into a cleanup candidate"
    );
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
    assert!(backend.object_exists("analytics", second_current).await.unwrap());
}

async fn seed_history_report(fixture: &ConditionalMaintenanceFixture, index: usize) -> String {
    let mut report = fixture.queued.clone();
    report.job.job_id = format!("history-{index:04}");
    report.job.status = TableMetadataMaintenanceJobStatus::Successful;
    report.job.finished_at = Some(maintenance_timestamp(fixture.now));
    let object = TableCatalogObjectPaths::default().table_maintenance_job_path(
        "analytics",
        &Namespace::parse("sales").unwrap(),
        &IdentifierSegment::parse("orders").unwrap(),
        &fixture.entry.table_id,
        &report.job.job_id,
    );
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &object, serde_json::to_vec(&report).unwrap())
        .await;
    object
}

#[tokio::test]
async fn scheduler_history_accepts_4096_objects_and_rejects_the_next_without_a_partial_report() {
    let fixture = ConditionalMaintenanceFixture::new(TableCatalogBackingMode::ObjectBacked, 0).await;
    for index in 1..4096 {
        seed_history_report(&fixture, index).await;
    }
    let store = ObjectTableCatalogStore::new(fixture.backend.clone());
    let before = maintenance_test_object_versions(&fixture.backend).await;
    let report = store
        .get_table_maintenance_scheduler_report_at("analytics", "sales", "orders", fixture.now)
        .await
        .expect("the exact history budget remains readable");
    assert_eq!(
        report.current_job.as_ref().map(|job| job.job_id.as_str()),
        Some(fixture.queued.job.job_id.as_str())
    );
    assert!(!report.audit_timeline.is_empty());
    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);

    seed_history_report(&fixture, 4096).await;
    let before = maintenance_test_object_versions(&fixture.backend).await;
    let error = store
        .get_table_maintenance_scheduler_report_at("analytics", "sales", "orders", fixture.now)
        .await
        .expect_err("one history object beyond the budget must fail closed");
    assert!(matches!(error, TableCatalogStoreError::Unavailable(message) if message.contains("4096")));
    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
}

#[tokio::test]
async fn scheduler_history_rejects_a_listed_report_that_disappears_before_read() {
    let fixture = ConditionalMaintenanceFixture::new(TableCatalogBackingMode::ObjectBacked, 0).await;
    let object = seed_history_report(&fixture, 1).await;
    let pause = fixture.backend.pause_before_next_read(RUSTFS_META_BUCKET, &object).await;
    let backend = fixture.backend.clone();
    let now = fixture.now;
    let reader = tokio::spawn(async move {
        ObjectTableCatalogStore::new(backend)
            .get_table_maintenance_scheduler_report_at("analytics", "sales", "orders", now)
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .expect("history listing reached the chosen report read");
    fixture
        .backend
        .state
        .lock()
        .await
        .objects
        .remove(&(RUSTFS_META_BUCKET.to_string(), object));
    let before = maintenance_test_object_versions(&fixture.backend).await;
    pause.release();
    let result = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, reader)
        .await
        .expect("scheduler read finishes")
        .expect("scheduler task joins");
    assert!(matches!(result, Err(TableCatalogStoreError::Unavailable(message)) if message.contains("history changed")));
    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
}

#[tokio::test]
async fn scheduler_history_rejects_invalid_listing_pages_without_persistent_changes() {
    for scenario in [
        "oversized",
        "empty-truncated",
        "foreign-prefix",
        "repeated-cursor",
        "duplicate-object",
    ] {
        let fixture = ConditionalMaintenanceFixture::new(TableCatalogBackingMode::ObjectBacked, 0).await;
        let paths = TableCatalogObjectPaths::default();
        let namespace = Namespace::parse("sales").unwrap();
        let table = IdentifierSegment::parse("orders").unwrap();
        let jobs_prefix = paths.table_maintenance_jobs_prefix("analytics", &namespace, &table, &fixture.entry.table_id);
        let job_path = paths.table_maintenance_job_path(
            "analytics",
            &namespace,
            &table,
            &fixture.entry.table_id,
            &fixture.queued.job.job_id,
        );
        let pages = match scenario {
            "oversized" => vec![TableCatalogObjectListPage {
                objects: vec![job_path; TABLE_CATALOG_LIST_MAX_KEYS + 1],
                is_truncated: false,
            }],
            "empty-truncated" => vec![TableCatalogObjectListPage {
                objects: Vec::new(),
                is_truncated: true,
            }],
            "foreign-prefix" => vec![TableCatalogObjectListPage {
                objects: vec![format!(
                    "{}/foreign-owner/jobs/report.json",
                    paths.table_bucket_entry_path("unrelated")
                )],
                is_truncated: false,
            }],
            "repeated-cursor" => vec![
                TableCatalogObjectListPage {
                    objects: vec![job_path.clone()],
                    is_truncated: true,
                },
                TableCatalogObjectListPage {
                    objects: vec![job_path],
                    is_truncated: false,
                },
            ],
            "duplicate-object" => vec![TableCatalogObjectListPage {
                objects: vec![job_path.clone(), job_path],
                is_truncated: false,
            }],
            _ => unreachable!(),
        };
        fixture
            .backend
            .state
            .lock()
            .await
            .list_page_overrides
            .entry((RUSTFS_META_BUCKET.to_string(), jobs_prefix))
            .or_default()
            .extend(pages);
        let before = maintenance_test_object_versions(&fixture.backend).await;
        let result = ObjectTableCatalogStore::new(fixture.backend.clone())
            .get_table_maintenance_scheduler_report_at("analytics", "sales", "orders", fixture.now)
            .await;
        assert!(
            matches!(result, Err(TableCatalogStoreError::Invalid(_) | TableCatalogStoreError::Unavailable(_))),
            "invalid {scenario} history listing must not return a scheduler report"
        );
        assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before, "{scenario}");
    }
}

struct SidecarRenameFixture {
    backend: TestCatalogObjectBackend,
    store: ObjectTableCatalogStore<TestCatalogObjectBackend>,
    source: TableEntry,
    report: TableMetadataMaintenanceReport,
    now: OffsetDateTime,
}

impl SidecarRenameFixture {
    async fn new() -> Self {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let now = OffsetDateTime::now_utc();
        let (_, _, mut report) = seed_quarantined_table_maintenance(&store, &backend, "analytics", now, None).await;
        let source = store.load_table("analytics", "sales", "orders").await.unwrap().unwrap();
        backend
            .seed_object(
                "analytics",
                &source.metadata_location,
                serde_json::to_vec(&table_metadata_json_for_backup("analytics", &source.table_id, &source.table_uuid)).unwrap(),
            )
            .await;
        report.snapshot_expiration = Some(
            store
                .plan_table_snapshot_expiration(
                    "analytics",
                    "sales",
                    "orders",
                    TableSnapshotExpirationConfig {
                        min_snapshots_to_keep: 1,
                        max_snapshot_age_ms: 86_400_000,
                    },
                )
                .await
                .expect("snapshot expiration report should be produced through the store"),
        );
        report.compaction = Some(
            store
                .plan_table_compaction(
                    "analytics",
                    "sales",
                    "orders",
                    TableCompactionPlanningConfig {
                        target_file_size_bytes: 1_048_576,
                        small_file_threshold_bytes: 524_288,
                        min_input_files: 2,
                        max_rewrite_bytes_per_job: 16_777_216,
                    },
                )
                .await
                .expect("compaction report should be produced through the store"),
        );
        store.put_table_metadata_maintenance_report(&report).await.unwrap();
        let report = store
            .get_table_metadata_maintenance_report("analytics", "sales", "orders", &report.job.job_id)
            .await
            .unwrap()
            .expect("seeded maintenance report remains readable");
        Self {
            backend,
            store,
            source,
            report,
            now,
        }
    }

    fn destination(&self) -> TableEntry {
        let mut destination = self.source.clone();
        destination.table = "archived_orders".to_string();
        destination
    }

    fn destination_config_path(&self) -> String {
        format!("{}{}", rename_maintenance_root(&self.destination()), MAINTENANCE_CONFIG_FILE)
    }

    fn source_config_path(&self) -> String {
        format!("{}{}", rename_maintenance_root(&self.source), MAINTENANCE_CONFIG_FILE)
    }

    async fn assert_pending(&self) {
        let before = maintenance_test_object_versions(&self.backend).await;
        let restarted = ObjectTableCatalogStore::new(self.backend.clone());
        for table in ["orders", "archived_orders"] {
            assert!(matches!(
                restarted.load_table("analytics", "sales", table).await,
                Err(TableCatalogStoreError::Unavailable(_))
            ));
        }
        assert!(matches!(
            restarted.get_table_maintenance_config("analytics", "sales", "orders").await,
            Err(TableCatalogStoreError::Unavailable(_))
        ));
        assert!(matches!(
            restarted
                .put_table_maintenance_config("analytics", "sales", "orders", TableMaintenanceConfig::default())
                .await,
            Err(TableCatalogStoreError::Unavailable(_))
        ));
        let bucket_object = self
            .backend
            .read_object(
                RUSTFS_META_BUCKET,
                &TableCatalogObjectPaths::default().table_bucket_entry_path("analytics"),
            )
            .await
            .unwrap()
            .unwrap();
        let bucket: TableBucketEntry = serde_json::from_slice(&bucket_object.data).unwrap();
        assert!(
            bucket.active_rename_id.is_some(),
            "the bucket fence must outlive interrupted sidecar publication"
        );
        assert_eq!(maintenance_test_object_versions(&self.backend).await, before);
    }

    async fn retry(&self) -> TableCatalogStoreResult<()> {
        ObjectTableCatalogStore::new(self.backend.clone())
            .rename_table("analytics", "sales", "orders", "sales", "archived_orders")
            .await
    }
}

#[tokio::test]
async fn table_rename_preserves_maintenance_configuration_reports_and_quarantine() {
    let fixture = SidecarRenameFixture::new().await;
    let expected = rename_maintenance_objects(&fixture.backend, &fixture.source).await;
    let configuration = fixture
        .store
        .get_table_maintenance_config("analytics", "sales", "orders")
        .await
        .unwrap();
    fixture
        .retry()
        .await
        .expect("rename should migrate the complete maintenance owner");
    let destination = fixture
        .store
        .load_table("analytics", "sales", "archived_orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(destination.table_id, fixture.source.table_id);
    assert_eq!(destination.table_uuid, fixture.source.table_uuid);
    assert_eq!(destination.metadata_location, fixture.source.metadata_location);
    assert_eq!(destination.warehouse_location, fixture.source.warehouse_location);
    assert_eq!(destination.version_token, fixture.source.version_token);
    assert_eq!(destination.generation, fixture.source.generation);
    assert_renamed_maintenance_objects(&fixture.backend, &destination, &expected).await;
    assert!(rename_maintenance_objects(&fixture.backend, &fixture.source).await.is_empty());
    assert_eq!(
        fixture
            .store
            .get_table_maintenance_config("analytics", "sales", "archived_orders")
            .await
            .unwrap(),
        configuration
    );
    for id in [
        MAINTENANCE_JOB_ALIAS_CURRENT,
        MAINTENANCE_JOB_ALIAS_LATEST,
        fixture.report.job.job_id.as_str(),
    ] {
        let report = fixture
            .store
            .get_table_metadata_maintenance_report("analytics", "sales", "archived_orders", id)
            .await
            .unwrap()
            .expect("renamed maintenance report should remain readable through every alias");
        let mut expected_report = fixture.report.clone();
        expected_report.job.table = destination.table.clone();
        expected_report.snapshot_expiration.as_mut().unwrap().table = destination.table.clone();
        expected_report.compaction.as_mut().unwrap().table = destination.table.clone();
        assert_eq!(report, expected_report);
    }
    let scheduler = fixture
        .store
        .get_table_maintenance_scheduler_report_at("analytics", "sales", "archived_orders", fixture.now)
        .await
        .expect("scheduler must retain the renamed quarantine and history");
    assert!(scheduler.quarantine.active);
    assert_eq!(scheduler.quarantine.quarantined_object_count, fixture.report.job.quarantined_object_count);
    assert!(
        scheduler
            .audit_timeline
            .iter()
            .any(|entry| entry.job_id == fixture.report.job.job_id)
    );
    let plan = fixture
        .store
        .plan_table_metadata_maintenance("analytics", "sales", "archived_orders", 0)
        .await
        .unwrap();
    assert_eq!(plan.current_metadata_location, fixture.source.metadata_location);
    assert!(
        !fixture
            .backend
            .object_exists(RUSTFS_META_BUCKET, &rename_maintenance_intent_path("analytics"))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn interrupted_maintenance_copy_is_fenced_and_resumes_after_store_recreation() {
    for post_write in [false, true] {
        let fixture = SidecarRenameFixture::new().await;
        let expected = rename_maintenance_objects(&fixture.backend, &fixture.source).await;
        let destination = fixture.destination_config_path();
        if post_write {
            fixture.backend.fail_after_next_put(RUSTFS_META_BUCKET, &destination).await;
        } else {
            fixture.backend.fail_next_put(RUSTFS_META_BUCKET, &destination).await;
        }
        assert!(fixture.retry().await.is_err(), "the injected sidecar write interruption must be surfaced");
        fixture.assert_pending().await;
        fixture
            .retry()
            .await
            .expect("retrying the same rename on a fresh store must resume the frozen plan");
        let renamed = fixture
            .store
            .load_table("analytics", "sales", "archived_orders")
            .await
            .unwrap()
            .unwrap();
        assert_renamed_maintenance_objects(&fixture.backend, &renamed, &expected).await;
        assert!(rename_maintenance_objects(&fixture.backend, &fixture.source).await.is_empty());
    }
}

#[tokio::test]
async fn maintenance_rename_recovers_after_source_cleanup_delete_response_loss() {
    let fixture = SidecarRenameFixture::new().await;
    let expected = rename_maintenance_objects(&fixture.backend, &fixture.source).await;
    fixture
        .backend
        .fail_after_next_delete(RUSTFS_META_BUCKET, &fixture.source_config_path())
        .await;
    assert!(
        fixture.retry().await.is_err(),
        "lost source cleanup response must leave recoverable ownership"
    );
    fixture.assert_pending().await;
    fixture
        .retry()
        .await
        .expect("a fresh store should finish the original cleanup");
    let renamed = fixture
        .store
        .load_table("analytics", "sales", "archived_orders")
        .await
        .unwrap()
        .unwrap();
    assert_renamed_maintenance_objects(&fixture.backend, &renamed, &expected).await;
    assert!(rename_maintenance_objects(&fixture.backend, &fixture.source).await.is_empty());
    assert!(
        !fixture
            .backend
            .object_exists(RUSTFS_META_BUCKET, &rename_maintenance_intent_path("analytics"))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn maintenance_rename_cannot_overwrite_a_changed_destination_after_interruption() {
    let fixture = SidecarRenameFixture::new().await;
    let expected_source = rename_maintenance_objects(&fixture.backend, &fixture.source).await;
    let path = fixture.destination_config_path();
    fixture.backend.fail_after_next_put(RUSTFS_META_BUCKET, &path).await;
    assert!(fixture.retry().await.is_err());
    let mut configuration = fixture
        .store
        .backend
        .read_object(RUSTFS_META_BUCKET, &fixture.source_config_path())
        .await
        .unwrap()
        .map(|object| serde_json::from_slice::<TableMaintenanceConfig>(&object.data).unwrap())
        .unwrap();
    configuration.background_enabled = false;
    let winner = serde_json::to_vec(&configuration).unwrap();
    fixture.backend.seed_object(RUSTFS_META_BUCKET, &path, winner.clone()).await;
    let before = maintenance_test_object_versions(&fixture.backend).await;
    assert!(matches!(fixture.retry().await, Err(TableCatalogStoreError::Conflict(_))));
    assert_eq!(
        fixture
            .backend
            .read_object(RUSTFS_META_BUCKET, &path)
            .await
            .unwrap()
            .unwrap()
            .data,
        winner
    );
    assert_eq!(rename_maintenance_objects(&fixture.backend, &fixture.source).await, expected_source);
    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
    fixture.assert_pending().await;
}

#[tokio::test]
async fn maintenance_rename_cannot_delete_a_recreated_source_cleanup_object() {
    let fixture = SidecarRenameFixture::new().await;
    let path = fixture.source_config_path();
    let configuration = fixture
        .store
        .get_table_maintenance_config("analytics", "sales", "orders")
        .await
        .unwrap();
    fixture.backend.fail_after_next_delete(RUSTFS_META_BUCKET, &path).await;
    assert!(fixture.retry().await.is_err());
    let winner = serde_json::to_vec(&configuration).unwrap();
    fixture.backend.seed_object(RUSTFS_META_BUCKET, &path, winner.clone()).await;
    let before = maintenance_test_object_versions(&fixture.backend).await;
    assert!(matches!(fixture.retry().await, Err(TableCatalogStoreError::Conflict(_))));
    assert_eq!(
        fixture
            .backend
            .read_object(RUSTFS_META_BUCKET, &path)
            .await
            .unwrap()
            .unwrap()
            .data,
        winner
    );
    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
    fixture.assert_pending().await;
}

#[tokio::test]
async fn renaming_a_table_without_sidecars_persists_and_cleans_its_empty_plan() {
    let (backend, store, original) = planning_fixture("tables/table-id/metadata/00003.metadata.json").await;
    let intent_path = rename_maintenance_intent_path("analytics");
    assert!(!backend.object_exists(RUSTFS_META_BUCKET, &intent_path).await.unwrap());
    store
        .rename_table("analytics", "sales", "orders", "sales", "archived_orders")
        .await
        .unwrap();
    let renamed = store
        .load_table("analytics", "sales", "archived_orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renamed.metadata_location, original.metadata_location);
    assert!(rename_maintenance_objects(&backend, &renamed).await.is_empty());
    assert!(!backend.object_exists(RUSTFS_META_BUCKET, &intent_path).await.unwrap());
    assert_eq!(
        backend
            .state
            .lock()
            .await
            .put_attempts
            .get(&(RUSTFS_META_BUCKET.to_string(), intent_path))
            .copied()
            .unwrap_or_default(),
        1,
        "even an empty plan must be durable before maintenance preparation is published"
    );
}

#[tokio::test]
async fn missing_empty_maintenance_plan_after_preparation_is_not_reconstructed() {
    let (backend, store, _) = planning_fixture("tables/table-id/metadata/00003.metadata.json").await;
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let source_path = store.paths.table_entry_path("analytics", &namespace, &table);
    backend.fail_next_put(RUSTFS_META_BUCKET, &source_path).await;
    store
        .rename_table("analytics", "sales", "orders", "sales", "archived_orders")
        .await
        .expect_err("stop after durable maintenance preparation but before source fencing");
    let rename_id = store
        .get_table_bucket("analytics")
        .await
        .unwrap()
        .unwrap()
        .active_rename_id
        .expect("the publication fence remains active");
    let rename_path = store.paths.table_rename_intent_path("analytics", &rename_id);
    let (intent, _) = store
        .read_entry::<TableRenameIntent>(RUSTFS_META_BUCKET, &rename_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(intent.state, TableRenameIntentState::MaintenancePrepared);
    let maintenance_path = rename_maintenance_intent_path("analytics");
    assert!(
        backend
            .state
            .lock()
            .await
            .objects
            .remove(&(RUSTFS_META_BUCKET.to_string(), maintenance_path))
            .is_some()
    );
    let before = maintenance_test_object_versions(&backend).await;
    let retry = ObjectTableCatalogStore::new(backend.clone())
        .rename_table("analytics", "sales", "orders", "sales", "archived_orders")
        .await;
    assert!(matches!(retry, Err(TableCatalogStoreError::Unavailable(_))));
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
    assert!(matches!(
        store.load_table("analytics", "sales", "orders").await,
        Err(TableCatalogStoreError::Unavailable(_))
    ));
}

struct ConcurrentCleanupFixture {
    backend: TestCatalogObjectBackend,
    entries: [TableEntry; 2],
    reports: [TableMetadataMaintenanceReport; 2],
    orphans: [String; 2],
}

async fn concurrent_cleanup_fixture() -> ConcurrentCleanupFixture {
    let first_current = "tables/table-id/metadata/00003.metadata.json";
    let second_current = "tables/invoice-id/metadata/00003.metadata.json";
    let (mut backend, store, first) = planning_fixture(first_current).await;
    let namespace = Namespace::parse("sales").unwrap();
    let invoice_table = IdentifierSegment::parse("invoices").unwrap();
    let mut second = test_table_entry("analytics", &namespace, &invoice_table, second_current.to_string());
    second.table_id = "invoice-id".to_string();
    second.table_uuid = "invoice-uuid".to_string();
    second.warehouse_location = "s3://analytics/tables/invoice-id".to_string();
    backend
        .seed_object(
            "analytics",
            second_current,
            serde_json::to_vec(&table_metadata_json_for_backup("analytics", &second.table_id, &second.table_uuid)).unwrap(),
        )
        .await;
    store.create_table(second.clone()).await.unwrap();
    let orphans = [
        "tables/table-id/metadata/00001.metadata.json".to_string(),
        "tables/invoice-id/metadata/00001.metadata.json".to_string(),
    ];
    for orphan in &orphans {
        backend.seed_object("analytics", orphan, b"{}".to_vec()).await;
    }
    let reports = [
        store
            .plan_table_metadata_maintenance("analytics", "sales", "orders", 0)
            .await
            .unwrap(),
        store
            .plan_table_metadata_maintenance("analytics", "sales", "invoices", 0)
            .await
            .unwrap(),
    ];
    for (report, orphan) in reports.iter().zip(&orphans) {
        assert_eq!(report.deletable_metadata_locations, vec![orphan.clone()]);
    }
    // Both new stores share the original lock map, while reads now exercise the
    // real object read-lock boundary that can deadlock under nested owner scans.
    backend.serialize_object_reads = true;
    ConcurrentCleanupFixture {
        backend,
        entries: [first, second],
        reports,
        orphans,
    }
}

#[tokio::test]
async fn concurrent_metadata_cleanups_do_not_acquire_each_others_entry_read_locks() {
    let fixture = concurrent_cleanup_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let owner_prefix = paths.namespace_entries_prefix("analytics");
    let first_scan = fixture.backend.pause_next_list_page(RUSTFS_META_BUCKET, &owner_prefix).await;
    let second_scan = fixture.backend.pause_next_list_page(RUSTFS_META_BUCKET, &owner_prefix).await;
    let before = maintenance_test_object_versions(&fixture.backend).await;
    let first_backend = fixture.backend.clone();
    let first_report = fixture.reports[0].clone();
    let first_cleanup = tokio::spawn(async move {
        ObjectTableCatalogStore::new(first_backend)
            .delete_table_metadata_maintenance_report("analytics", "sales", "orders", first_report)
            .await
    });
    let second_backend = fixture.backend.clone();
    let second_report = fixture.reports[1].clone();
    let second_cleanup = tokio::spawn(async move {
        ObjectTableCatalogStore::new(second_backend)
            .delete_table_metadata_maintenance_report("analytics", "sales", "invoices", second_report)
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async {
        tokio::join!(first_scan.wait_started(), second_scan.wait_started());
    })
    .await
    .expect("both cleanups must reach owner listing while holding their own entry write lock");
    for entry in &fixture.entries {
        let path = paths.table_entry_path(
            "analytics",
            &Namespace::parse(&entry.namespace).unwrap(),
            &IdentifierSegment::parse(&entry.table).unwrap(),
        );
        assert!(fixture.backend.write_lock_is_held(RUSTFS_META_BUCKET, &path).await);
    }
    first_scan.release();
    second_scan.release();
    let (first_result, second_result) =
        tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, async { tokio::join!(first_cleanup, second_cleanup) })
            .await
            .expect("independent cleanups must finish instead of waiting on each other's entry lock");
    for result in [first_result, second_result] {
        let report = result.expect("cleanup task joins").expect("independent cleanup succeeds");
        assert_eq!(report.job.deleted_metadata_file_count, 1);
    }
    let mut expected = before;
    for orphan in &fixture.orphans {
        expected.remove(&("analytics".to_string(), orphan.clone()));
        assert!(!fixture.backend.object_exists("analytics", orphan).await.unwrap());
    }
    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, expected);
    let store = ObjectTableCatalogStore::new(fixture.backend.clone());
    for entry in fixture.entries {
        assert_eq!(store.load_table("analytics", &entry.namespace, &entry.table).await.unwrap(), Some(entry));
    }
}

#[tokio::test]
async fn metadata_cleanup_rejects_an_owner_that_disappears_after_publication_fenced_listing() {
    let fixture = concurrent_cleanup_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let owner = &fixture.entries[1];
    let owner_path = paths.table_entry_path(
        "analytics",
        &Namespace::parse(&owner.namespace).unwrap(),
        &IdentifierSegment::parse(&owner.table).unwrap(),
    );
    let pause = fixture.backend.pause_before_next_read(RUSTFS_META_BUCKET, &owner_path).await;
    let backend = fixture.backend.clone();
    let report = fixture.reports[0].clone();
    let cleanup = tokio::spawn(async move {
        ObjectTableCatalogStore::new(backend)
            .delete_table_metadata_maintenance_report("analytics", "sales", "orders", report)
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .expect("cleanup lists the second owner and reaches its object read");
    assert!(
        fixture
            .backend
            .state
            .lock()
            .await
            .objects
            .remove(&(RUSTFS_META_BUCKET.to_string(), owner_path))
            .is_some()
    );
    let before = maintenance_test_object_versions(&fixture.backend).await;
    pause.release();
    let result = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, cleanup)
        .await
        .expect("missing-owner cleanup returns")
        .expect("cleanup task joins");
    assert!(matches!(result, Err(TableCatalogStoreError::Conflict(message)) if message.contains("disappeared")));
    assert_eq!(maintenance_test_object_versions(&fixture.backend).await, before);
    for orphan in &fixture.orphans {
        assert!(fixture.backend.object_exists("analytics", orphan).await.unwrap());
    }
    assert_eq!(
        ObjectTableCatalogStore::new(fixture.backend.clone())
            .load_table("analytics", "sales", "orders")
            .await
            .unwrap(),
        Some(fixture.entries[0].clone())
    );
}
