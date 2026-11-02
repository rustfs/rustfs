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

#[path = "backup_capture_bounds_tests.rs"]
mod capture_bounds;
#[path = "backup_commit_recovery_store_tests.rs"]
mod commit_recovery_store;
#[path = "backup_maintenance_store_tests.rs"]
mod maintenance_store;
#[path = "backup_reference_revalidation_store_tests.rs"]
mod reference_revalidation_store;

#[tokio::test]
async fn restore_request_cancellation_before_replacement_delete_resumes_the_original_plan() {
    let (backend, backup, metadata_path) = backup_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let expected_entry = store
        .load_table("analytics", "sales", "orders")
        .await
        .expect("load original table")
        .expect("original table exists");
    let original_metadata = backend
        .read_object("analytics", &metadata_path)
        .await
        .expect("read original immutable metadata")
        .expect("metadata exists");
    backend
        .seed_object("unrelated-bucket", "keep.bin", b"unrelated data must remain unchanged".to_vec())
        .await;

    store
        .drop_table("analytics", "sales", "orders")
        .await
        .expect("remove table before restoring backup");
    store
        .drop_namespace("analytics", "sales")
        .await
        .expect("remove namespace before restoring backup");
    let paths = TableCatalogObjectPaths::default();
    let bucket_path = paths.table_bucket_entry_path("analytics");
    let (catalog_prefix, _) = bucket_path.rsplit_once('/').expect("catalog bucket has a parent");
    // This sorts before ordinary catalog records in the frozen deletion plan.
    let extra_path = format!("{catalog_prefix}/000-cancel-before-delete.json");
    backend
        .seed_object(RUSTFS_META_BUCKET, &extra_path, b"stale target catalog object".to_vec())
        .await;
    let original_extra = backend
        .read_object(RUSTFS_META_BUCKET, &extra_path)
        .await
        .expect("read extra target object")
        .expect("extra target object exists");
    let restore_id = "cancel-before-replacement-delete";
    let barrier_path = paths.catalog_restore_state_path("analytics");
    let intent_path = paths.catalog_restore_intent_path("analytics", restore_id);
    let pause = backend.pause_next_delete(RUSTFS_META_BUCKET, &extra_path).await;
    let worker_backend = backend.clone();
    let backup_id = backup.backup_id.clone();
    let request = tokio::spawn(async move {
        ObjectTableCatalogStore::new(worker_backend)
            .restore_object_catalog_backup_with_id("analytics", &backup_id, None, true, Some(restore_id))
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .expect("restore reached the first replacement delete");
    let interrupted_barrier = restore_control_json(&backend, &barrier_path).await;
    let interrupted_intent = restore_control_json(&backend, &intent_path).await;
    assert_eq!(interrupted_barrier["state"], "APPLYING");
    assert_eq!(interrupted_barrier["restore_id"], restore_id);
    assert_eq!(interrupted_barrier["restore_epoch"], interrupted_intent["restore_epoch"]);
    assert!(
        interrupted_intent["deletes"]
            .as_array()
            .expect("frozen deletion plan")
            .iter()
            .any(|entry| entry["object"] == extra_path),
        "the paused delete must already be authorized by the durable plan"
    );
    assert_eq!(
        backend
            .read_object(RUSTFS_META_BUCKET, &extra_path)
            .await
            .expect("read paused delete target"),
        Some(original_extra.clone()),
        "the paused request deleted its target before cancellation"
    );

    request.abort();
    let cancelled = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, request)
        .await
        .expect("cancelled request released its task and fencing guards")
        .expect_err("the paused restore request must be cancelled");
    assert!(cancelled.is_cancelled());
    assert_eq!(
        backend
            .read_object(RUSTFS_META_BUCKET, &extra_path)
            .await
            .expect("read cancelled delete target"),
        Some(original_extra),
        "cancelling before delete must preserve its original bytes and ETag"
    );
    let cancelled_versions = maintenance_test_object_versions(&backend).await;
    assert_pending_restore_rejects_catalog_access(&backend).await;
    assert_eq!(maintenance_test_object_versions(&backend).await, cancelled_versions);

    let restarted = ObjectTableCatalogStore::new(backend.clone());
    let restored = tokio::time::timeout(
        TABLE_CATALOG_TEST_TIMEOUT,
        restarted.restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some(restore_id)),
    )
    .await
    .expect("retry must not retain locks from the cancelled request")
    .expect("the same request resumes its durable replacement plan");
    assert_eq!(restored.status, TableCatalogRestoreStatus::Restored);
    assert!(
        !backend
            .object_exists(RUSTFS_META_BUCKET, &extra_path)
            .await
            .expect("inspect completed deletion")
    );
    let completed_barrier = restore_control_json(&backend, &barrier_path).await;
    let completed_intent = restore_control_json(&backend, &intent_path).await;
    assert_eq!(completed_barrier["state"], "COMPLETED");
    assert_eq!(completed_barrier["restore_epoch"], interrupted_barrier["restore_epoch"]);
    assert_eq!(
        completed_intent["writes"], interrupted_intent["writes"],
        "retry replaced the original write plan"
    );
    assert_eq!(
        completed_intent["deletes"], interrupted_intent["deletes"],
        "retry replaced the original delete plan"
    );
    let loaded = restarted
        .load_table("analytics", "sales", "orders")
        .await
        .expect("load completed restore")
        .expect("table restored");
    assert_eq!(loaded.table_uuid, expected_entry.table_uuid);
    assert_eq!(loaded.warehouse_location, expected_entry.warehouse_location);
    assert_eq!(loaded.metadata_location, expected_entry.metadata_location);
    assert_eq!(loaded.version_token, expected_entry.version_token);
    assert_eq!(loaded.generation, expected_entry.generation);
    assert_eq!(loaded, expected_entry);
    assert_eq!(
        backend
            .read_object("analytics", &metadata_path)
            .await
            .expect("read restored metadata"),
        Some(original_metadata),
        "recovery rewrote immutable Iceberg metadata"
    );

    let mut expected_versions = cancelled_versions;
    expected_versions.remove(&(RUSTFS_META_BUCKET.to_string(), extra_path));
    let mut actual_versions = maintenance_test_object_versions(&backend).await;
    for path in [&barrier_path, &intent_path] {
        expected_versions.remove(&(RUSTFS_META_BUCKET.to_string(), path.clone()));
        actual_versions.remove(&(RUSTFS_META_BUCKET.to_string(), path.clone()));
    }
    assert_eq!(
        actual_versions, expected_versions,
        "resuming deletion rewrote already-applied catalog objects or unrelated data"
    );

    let completed_versions = maintenance_test_object_versions(&backend).await;
    let replay = restarted
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some(restore_id))
        .await
        .expect("completed request replays its original receipt");
    let mut expected_receipt = restored;
    expected_receipt.status = TableCatalogRestoreStatus::AlreadyRestored;
    assert_eq!(replay, expected_receipt);
    assert_eq!(maintenance_test_object_versions(&backend).await, completed_versions);
}

#[tokio::test]
async fn completed_restore_sealed_publication_response_loss_replays_the_original_receipt() {
    let (backend, backup) = isolated_restore_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let restore_id = "sealed-completion-response-loss";
    let table_path = paths.table_entry_path(
        "analytics",
        &Namespace::parse("sales").expect("namespace"),
        &IdentifierSegment::parse("orders").expect("table"),
    );
    let intent_path = paths.catalog_restore_intent_path("analytics", restore_id);
    let barrier_path = paths.catalog_restore_state_path("analytics");
    let table_pause = backend.pause_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let worker_backend = backend.clone();
    let backup_id = backup.backup_id.clone();
    let worker = tokio::spawn(async move {
        ObjectTableCatalogStore::new(worker_backend)
            .restore_object_catalog_backup_with_id("analytics", &backup_id, None, false, Some(restore_id))
            .await
    });

    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, table_pause.wait_started())
        .await
        .expect("restore reached table publication");
    let applied_intent_pause = backend.pause_next_put(RUSTFS_META_BUCKET, &intent_path).await;
    table_pause.release();
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, applied_intent_pause.wait_started())
        .await
        .expect("restore reached catalog-applied intent");
    let applied_barrier_pause = backend.pause_next_put(RUSTFS_META_BUCKET, &barrier_path).await;
    applied_intent_pause.release();
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, applied_barrier_pause.wait_started())
        .await
        .expect("restore reached catalog-applied barrier");
    let completed_intent_pause = backend.pause_next_put(RUSTFS_META_BUCKET, &intent_path).await;
    applied_barrier_pause.release();
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, completed_intent_pause.wait_started())
        .await
        .expect("restore reached completion receipt");
    assert_eq!(restore_control_json(&backend, &barrier_path).await["state"], "CATALOG_APPLIED");
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &barrier_path).await;
    completed_intent_pause.release();
    let failed = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, worker)
        .await
        .expect("restore returned after the final response was lost")
        .expect("restore worker joined");
    assert!(
        matches!(failed, Err(TableCatalogStoreError::Internal(message)) if message.contains("injected post-commit put failure"))
    );

    let barrier = restore_control_json(&backend, &barrier_path).await;
    assert_eq!(barrier["state"], "COMPLETED");
    assert!(barrier["completed_intent_etag"].as_str().is_some());
    assert!(barrier["backup_etag"].as_str().is_some());
    let before = maintenance_test_object_versions(&backend).await;
    let restarted = ObjectTableCatalogStore::new(backend.clone());
    let receipt = restarted
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
        .await
        .expect("retry reads the completed receipt after losing its publication response");
    assert_eq!(receipt.status, TableCatalogRestoreStatus::AlreadyRestored);
    assert_eq!(receipt.backup_id, backup.backup_id);
    assert_eq!(receipt.restored_snapshot_etag, backup.source_snapshot_etag);
    assert_eq!(receipt.object_count, backup.object_count);
    assert_eq!(receipt.previous_snapshot_etag, None);
    assert_eq!(
        restarted
            .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
            .await
            .expect("subsequent retry returns the same receipt"),
        receipt
    );
    assert!(
        restarted
            .load_table("analytics", "sales", "orders")
            .await
            .expect("published table read")
            .is_some()
    );
    assert_eq!(
        maintenance_test_object_versions(&backend).await,
        before,
        "receipt replay rewrote persisted state"
    );
}

fn restore_path_read_count(attempts: &BTreeMap<(String, String), usize>, path: &str) -> usize {
    attempts
        .get(&(RUSTFS_META_BUCKET.to_string(), path.to_string()))
        .copied()
        .unwrap_or_default()
}

#[tokio::test]
async fn completed_restore_reads_validate_sealed_control_versions_without_loading_large_payloads() {
    let (backend, backup) = isolated_restore_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let restore_id = "sealed-control-read-budget";
    store
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
        .await
        .expect("restore publishes a completed barrier");
    let paths = TableCatalogObjectPaths::default();
    let barrier_path = paths.catalog_restore_state_path("analytics");
    let intent_path = paths.catalog_restore_intent_path("analytics", restore_id);
    let barrier = restore_control_json(&backend, &barrier_path).await;
    assert!(barrier["completed_intent_etag"].as_str().is_some_and(|etag| !etag.is_empty()));
    assert!(barrier["backup_etag"].as_str().is_some_and(|etag| !etag.is_empty()));
    let before_versions = maintenance_test_object_versions(&backend).await;
    let (before_reads, before_metadata_calls) = {
        let state = backend.state.lock().await;
        (state.read_attempts.clone(), state.metadata_calls)
    };

    let table = store
        .load_table("analytics", "sales", "orders")
        .await
        .expect("table load")
        .expect("table");
    assert_eq!(table.table_uuid, "table-uuid");
    assert!(
        store
            .get_namespace("analytics", "sales")
            .await
            .expect("namespace load")
            .is_some()
    );

    let state = backend.state.lock().await;
    for path in [&intent_path, &backup.backup_path] {
        assert_eq!(
            restore_path_read_count(&state.read_attempts, path) - restore_path_read_count(&before_reads, path),
            0,
            "ordinary catalog reads loaded a sealed control payload: {path}"
        );
    }
    assert_eq!(
        restore_path_read_count(&state.read_attempts, &barrier_path) - restore_path_read_count(&before_reads, &barrier_path),
        4,
        "table and namespace reads must observe the small barrier before and after access"
    );
    // Each of the four barrier observations checks both sealed object versions.
    // The table read also checks its existing rename-publication bucket version.
    assert_eq!(state.metadata_calls - before_metadata_calls, 9);
    drop(state);
    assert_eq!(
        backend.last_read_limit(RUSTFS_META_BUCKET, &barrier_path).await,
        Some(16 * 1024),
        "ordinary catalog reads must keep the restore control payload bounded"
    );
    assert_eq!(maintenance_test_object_versions(&backend).await, before_versions);
}

#[tokio::test]
async fn completed_restore_rejects_missing_or_replaced_sealed_controls_without_writing() {
    for control in ["intent", "artifact"] {
        for missing in [false, true] {
            let (backend, backup) = isolated_restore_fixture().await;
            let store = ObjectTableCatalogStore::new(backend.clone());
            let restore_id = "sealed-control-corruption";
            store
                .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
                .await
                .expect("complete restore before control loss");
            let path = if control == "intent" {
                TableCatalogObjectPaths::default().catalog_restore_intent_path("analytics", restore_id)
            } else {
                backup.backup_path.clone()
            };
            if missing {
                backend
                    .state
                    .lock()
                    .await
                    .objects
                    .remove(&(RUSTFS_META_BUCKET.to_string(), path.clone()));
            } else {
                backend
                    .seed_object(RUSTFS_META_BUCKET, &path, b"tampered-control-payload".to_vec())
                    .await;
            }
            let before = maintenance_test_object_versions(&backend).await;
            assert_restore_unavailable(store.load_table("analytics", "sales", "orders").await, control);
            assert_restore_unavailable(store.get_namespace("analytics", "sales").await, control);
            assert_restore_unavailable(
                store
                    .create_namespace(test_namespace_entry("analytics", &Namespace::parse("forbidden").expect("namespace")))
                    .await,
                control,
            );
            assert_eq!(
                maintenance_test_object_versions(&backend).await,
                before,
                "rejected {control} access wrote state"
            );
        }
    }
}

#[tokio::test]
async fn completed_restore_legacy_barrier_reads_still_validate_full_intent_and_artifact() {
    let (backend, backup) = isolated_restore_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let restore_id = "legacy-completed-barrier";
    store
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some(restore_id))
        .await
        .expect("complete restore");
    let paths = TableCatalogObjectPaths::default();
    let barrier_path = paths.catalog_restore_state_path("analytics");
    let intent_path = paths.catalog_restore_intent_path("analytics", restore_id);
    let mut barrier = restore_control_json(&backend, &barrier_path).await;
    let fields = barrier.as_object_mut().expect("barrier object");
    assert!(fields.remove("completed_intent_etag").is_some());
    assert!(fields.remove("backup_etag").is_some());
    backend
        .seed_object(RUSTFS_META_BUCKET, &barrier_path, serde_json::to_vec(&barrier).expect("legacy barrier"))
        .await;
    let before_versions = maintenance_test_object_versions(&backend).await;
    let before_reads = backend.state.lock().await.read_attempts.clone();

    assert!(
        store
            .load_table("analytics", "sales", "orders")
            .await
            .expect("legacy barrier table load")
            .is_some()
    );
    {
        let state = backend.state.lock().await;
        for path in [&intent_path, &backup.backup_path, &barrier_path] {
            assert_eq!(
                restore_path_read_count(&state.read_attempts, path) - restore_path_read_count(&before_reads, path),
                2,
                "legacy barrier must fully verify controls before and after a table read: {path}"
            );
        }
    }
    assert_eq!(
        maintenance_test_object_versions(&backend).await,
        before_versions,
        "legacy read rewrote control format"
    );

    backend.seed_object(RUSTFS_META_BUCKET, &intent_path, b"{}".to_vec()).await;
    let before_corrupt_read = maintenance_test_object_versions(&backend).await;
    assert!(store.load_table("analytics", "sales", "orders").await.is_err());
    assert!(store.get_namespace("analytics", "sales").await.is_err());
    assert!(
        store
            .create_namespace(test_namespace_entry("analytics", &Namespace::parse("forbidden").expect("namespace")))
            .await
            .is_err()
    );
    assert_eq!(maintenance_test_object_versions(&backend).await, before_corrupt_read);
}

#[tokio::test]
async fn completed_restore_rejects_incomplete_sealed_version_pairs() {
    for absent_field in ["completed_intent_etag", "backup_etag"] {
        let (backend, backup) = isolated_restore_fixture().await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        store
            .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("partial-control-seal"))
            .await
            .expect("complete restore");
        let barrier_path = TableCatalogObjectPaths::default().catalog_restore_state_path("analytics");
        let mut barrier = restore_control_json(&backend, &barrier_path).await;
        assert!(
            barrier
                .as_object_mut()
                .expect("barrier object")
                .remove(absent_field)
                .is_some()
        );
        backend
            .seed_object(RUSTFS_META_BUCKET, &barrier_path, serde_json::to_vec(&barrier).expect("partial barrier"))
            .await;
        let before = maintenance_test_object_versions(&backend).await;
        assert!(store.load_table("analytics", "sales", "orders").await.is_err());
        assert!(
            store
                .create_namespace(test_namespace_entry("analytics", &Namespace::parse("forbidden").expect("namespace")))
                .await
                .is_err()
        );
        assert_eq!(maintenance_test_object_versions(&backend).await, before);
    }
}

async fn backup_fixture() -> (TestCatalogObjectBackend, TableCatalogBackupReport, String) {
    backup_fixture_with_backend(TestCatalogObjectBackend::default()).await
}

async fn backup_fixture_with_backend(
    backend: TestCatalogObjectBackend,
) -> (TestCatalogObjectBackend, TableCatalogBackupReport, String) {
    let store = ObjectTableCatalogStore::new(backend.clone());
    let namespace = Namespace::parse("sales").expect("fixture namespace");
    let table = IdentifierSegment::parse("orders").expect("fixture table");
    let metadata_path = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    backend
        .seed_object(
            "analytics",
            &metadata_path,
            serde_json::to_vec(&table_metadata_json_for_backup("analytics", "table-id", "table-uuid")).expect("fixture metadata"),
        )
        .await;
    store
        .put_table_bucket(test_bucket_entry("analytics"))
        .await
        .expect("fixture bucket");
    store
        .create_namespace(test_namespace_entry("analytics", &namespace))
        .await
        .expect("fixture namespace");
    store
        .create_table(test_table_entry("analytics", &namespace, &table, metadata_path.clone()))
        .await
        .expect("fixture table");
    let backup = store
        .create_object_catalog_backup("analytics", None)
        .await
        .expect("fixture backup");
    (backend, backup, metadata_path)
}

#[tokio::test]
async fn restore_validates_the_entire_artifact_before_any_target_write() {
    let (source, backup, _) = backup_fixture().await;
    let bytes = source
        .read_object(RUSTFS_META_BUCKET, &backup.backup_path)
        .await
        .expect("read artifact")
        .expect("artifact exists")
        .data;
    for field in [
        "sha256",
        "size_bytes",
        "bucket",
        "object",
        "backup_id",
        "source_catalog_etag",
        "duplicate",
    ] {
        let mut artifact: serde_json::Value = serde_json::from_slice(&bytes).expect("decode fixture artifact");
        let objects = artifact["objects"].as_array_mut().expect("artifact objects");
        let last = objects.last_mut().expect("nonempty backup");
        match field {
            "sha256" => last["sha256"] = serde_json::json!("corrupt"),
            "size_bytes" => last["size_bytes"] = serde_json::json!(u64::MAX),
            "bucket" => last["bucket"] = serde_json::json!("unrelated-bucket"),
            "object" => last["object"] = serde_json::json!("../outside.json"),
            "duplicate" => objects.push(objects[0].clone()),
            "backup_id" => artifact["backup_id"] = serde_json::json!("wrong"),
            "source_catalog_etag" => artifact["source_catalog_etag"] = serde_json::json!("wrong"),
            _ => unreachable!(),
        }
        let target = TestCatalogObjectBackend::default();
        target
            .seed_object(
                RUSTFS_META_BUCKET,
                &backup.backup_path,
                serde_json::to_vec(&artifact).expect("encode tampering"),
            )
            .await;
        let before = maintenance_test_object_versions(&target).await;
        let result = ObjectTableCatalogStore::new(target.clone())
            .restore_object_catalog_backup("analytics", &backup.backup_id, None, true)
            .await;
        assert!(result.is_err(), "tampered {field} was accepted");
        assert!(
            maintenance_test_object_versions(&target).await == before,
            "tampered {field} mutated target"
        );
    }
}

#[tokio::test]
async fn restore_never_overwrites_changed_iceberg_metadata_even_with_allow_replace() {
    let (backend, backup, metadata_path) = backup_fixture().await;
    let mut metadata = table_metadata_json_for_backup("analytics", "table-id", "table-uuid");
    metadata["properties"] = serde_json::json!({"changed": "after-backup"});
    backend
        .seed_object("analytics", &metadata_path, serde_json::to_vec(&metadata).expect("changed metadata"))
        .await;
    let before = maintenance_test_object_versions(&backend).await;
    let result = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup("analytics", &backup.backup_id, None, true)
        .await;
    assert!(
        matches!(result, Err(TableCatalogStoreError::Conflict(_))),
        "changed immutable metadata must conflict"
    );
    assert!(
        maintenance_test_object_versions(&backend).await == before,
        "restore changed immutable metadata or catalog"
    );
}

#[tokio::test]
async fn repeated_restore_does_not_rewrite_identical_objects_or_change_etags() {
    let (source, backup, _) = backup_fixture().await;
    let artifact = source
        .read_object(RUSTFS_META_BUCKET, &backup.backup_path)
        .await
        .expect("read backup")
        .expect("backup");
    let target = TestCatalogObjectBackend::default();
    target
        .seed_object(RUSTFS_META_BUCKET, &backup.backup_path, artifact.data)
        .await;
    let store = ObjectTableCatalogStore::new(target.clone());
    store
        .restore_object_catalog_backup("analytics", &backup.backup_id, None, false)
        .await
        .expect("initial restore");
    let before = maintenance_test_object_versions(&target).await;
    let repeated = store
        .restore_object_catalog_backup("analytics", &backup.backup_id, None, false)
        .await
        .expect("repeat restore");
    assert_eq!(repeated.status, TableCatalogRestoreStatus::AlreadyRestored);
    assert!(
        maintenance_test_object_versions(&target).await == before,
        "idempotent retry changed object versions"
    );
}

#[tokio::test]
async fn restore_rejects_etagless_replacement_before_any_mutation() {
    let (backend, backup, _) = backup_fixture().await;
    let path = TableCatalogObjectPaths::default().table_bucket_entry_path("analytics");
    backend.seed_object(RUSTFS_META_BUCKET, &path, b"changed".to_vec()).await;
    backend
        .state
        .lock()
        .await
        .etagless_objects
        .insert((RUSTFS_META_BUCKET.to_string(), path));
    let before = maintenance_test_object_versions(&backend).await;
    let result = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup("analytics", &backup.backup_id, None, true)
        .await;
    assert!(result.is_err(), "replacement without an ETag must fail closed");
    assert!(
        maintenance_test_object_versions(&backend).await == before,
        "failed replacement partially mutated target"
    );
}

#[tokio::test]
async fn captured_v1_artifact_restores_without_reencoding() {
    // A fixed synthetic artifact produced before strict restore validation.
    // Its digest and embedded bytes must remain readable without migration.
    let artifact = include_bytes!("fixtures/object-catalog-backup-v1.json");
    let backup_id = "83e3024f278b0abeecdbde378068980c3d5c9a2a5ddaa915495d5b398d1d05c2";
    let backend = TestCatalogObjectBackend::default();
    let path = TableCatalogObjectPaths::default().catalog_backup_path("analytics", backup_id);
    backend.seed_object(RUSTFS_META_BUCKET, &path, artifact.to_vec()).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let restored = store
        .restore_object_catalog_backup("analytics", backup_id, None, false)
        .await
        .expect("read version 1 artifact");
    assert_eq!(restored.backup_id, backup_id);
    let table = store
        .load_table("analytics", "sales", "orders")
        .await
        .expect("load restored table")
        .expect("table exists");
    assert_eq!(table.table_uuid, "table-uuid");
    assert_eq!(table.warehouse_location, "s3://analytics/tables/table-id");
    assert_eq!(table.version_token, "token-v1");
    assert_eq!(table.generation, 1);
    assert_eq!(
        backend
            .read_object(RUSTFS_META_BUCKET, &path)
            .await
            .expect("read artifact")
            .expect("artifact")
            .data,
        artifact
    );
}

#[tokio::test]
async fn backup_rejects_current_metadata_for_another_identity_before_sealing() {
    for field in ["table-uuid", "location"] {
        let (backend, _, metadata_path) = backup_fixture().await;
        let mut metadata = table_metadata_json_for_backup("analytics", "table-id", "table-uuid");
        metadata[field] = serde_json::json!(if field == "location" {
            "s3://analytics/tables/other"
        } else {
            "other-uuid"
        });
        backend
            .seed_object("analytics", &metadata_path, serde_json::to_vec(&metadata).expect("metadata"))
            .await;
        let before = maintenance_test_object_versions(&backend).await;
        let result = ObjectTableCatalogStore::new(backend.clone())
            .create_object_catalog_backup("analytics", None)
            .await;
        assert!(
            matches!(result, Err(TableCatalogStoreError::Invalid(_))),
            "mismatched {field} was accepted"
        );
        assert!(
            maintenance_test_object_versions(&backend).await == before,
            "mismatched identity wrote an artifact"
        );
    }
}

#[tokio::test]
async fn repeated_backup_returns_the_original_creation_timestamp() {
    let (backend, mut first, _) = backup_fixture().await;
    let repeated = ObjectTableCatalogStore::new(backend)
        .create_object_catalog_backup("analytics", None)
        .await
        .expect("repeat backup");
    first.status = TableCatalogBackupStatus::AlreadyPresent;
    assert_eq!(repeated, first);
}

#[tokio::test]
async fn restore_cas_rejects_replacement_after_preflight() {
    let (backend, backup, _) = backup_fixture().await;
    let path = TableCatalogObjectPaths::default().table_bucket_entry_path("analytics");
    backend
        .seed_object(RUSTFS_META_BUCKET, &path, b"observed-target".to_vec())
        .await;
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &path).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let restore = tokio::spawn(async move {
        store
            .restore_object_catalog_backup("analytics", &backup.backup_id, None, true)
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .expect("restore reached CAS");
    backend
        .seed_object(RUSTFS_META_BUCKET, &path, b"concurrent-winner".to_vec())
        .await;
    pause.release();
    let result = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, restore)
        .await
        .expect("restore completed")
        .expect("restore task");
    assert!(matches!(result, Err(TableCatalogStoreError::Conflict(_))));
    assert_eq!(
        backend
            .read_object(RUSTFS_META_BUCKET, &path)
            .await
            .expect("read winner")
            .expect("winner")
            .data,
        b"concurrent-winner"
    );
}

async fn isolated_restore_fixture() -> (TestCatalogObjectBackend, TableCatalogBackupReport) {
    let (source, backup, _) = backup_fixture().await;
    let artifact = source
        .read_object(RUSTFS_META_BUCKET, &backup.backup_path)
        .await
        .expect("read backup artifact")
        .expect("backup artifact exists");
    let target = TestCatalogObjectBackend::default();
    target
        .seed_object(RUSTFS_META_BUCKET, &backup.backup_path, artifact.data)
        .await;
    (target, backup)
}

async fn restore_control_json(backend: &TestCatalogObjectBackend, path: &str) -> serde_json::Value {
    let object = backend
        .read_object(RUSTFS_META_BUCKET, path)
        .await
        .expect("read restore control object")
        .expect("restore control object exists");
    serde_json::from_slice(&object.data).expect("restore control object is JSON")
}

async fn restore_published_object_versions(backend: &TestCatalogObjectBackend) -> BTreeMap<(String, String), TableCatalogObject> {
    let bucket_path = TableCatalogObjectPaths::default().table_bucket_entry_path("analytics");
    let (catalog_prefix, _) = bucket_path.rsplit_once('/').expect("catalog bucket path has a prefix");
    maintenance_test_object_versions(backend)
        .await
        .into_iter()
        .filter(|((bucket, object), _)| {
            bucket == "analytics" || (bucket == RUSTFS_META_BUCKET && object.starts_with(catalog_prefix))
        })
        .collect()
}

fn assert_restore_unavailable<T>(result: TableCatalogStoreResult<T>, operation: &str) {
    assert!(
        matches!(result, Err(TableCatalogStoreError::Unavailable(_))),
        "{operation} exposed a partially restored catalog"
    );
}

async fn assert_pending_restore_rejects_catalog_access(backend: &TestCatalogObjectBackend) {
    let restarted = ObjectTableCatalogStore::new(backend.clone());
    assert_restore_unavailable(restarted.get_table_bucket("analytics").await, "bucket load");
    assert_restore_unavailable(restarted.get_namespace("analytics", "sales").await, "namespace load");
    assert_restore_unavailable(restarted.list_namespaces("analytics").await, "namespace list");
    assert_restore_unavailable(
        restarted.list_namespaces_page("analytics", None, NonZeroUsize::MIN).await,
        "namespace page",
    );
    assert_restore_unavailable(restarted.list_namespace_children("analytics", None).await, "namespace children");
    assert_restore_unavailable(restarted.list_tables("analytics", "sales").await, "table list");
    assert_restore_unavailable(restarted.load_table("analytics", "sales", "orders").await, "table load");
    assert_restore_unavailable(restarted.list_views("analytics", "sales").await, "view list");
    assert_restore_unavailable(restarted.load_view("analytics", "sales", "recent_orders").await, "view load");
    let before = maintenance_test_object_versions(backend).await;
    assert_restore_unavailable(
        restarted
            .create_namespace(test_namespace_entry("analytics", &Namespace::parse("forbidden").expect("new namespace")))
            .await,
        "namespace create",
    );
    assert_eq!(
        maintenance_test_object_versions(backend).await,
        before,
        "a rejected writer changed persisted state"
    );
}

#[tokio::test]
async fn restore_intent_write_failure_does_not_publish_any_catalog_or_metadata() {
    let (backend, backup) = isolated_restore_fixture().await;
    let intent = TableCatalogObjectPaths::default().catalog_restore_intent_path("analytics", "intent-write-failure");
    backend.fail_next_put(RUSTFS_META_BUCKET, &intent).await;
    let before = restore_published_object_versions(&backend).await;
    let result = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("intent-write-failure"))
        .await;
    assert!(matches!(result, Err(TableCatalogStoreError::Internal(_))));
    assert_eq!(restore_published_object_versions(&backend).await, before);
    ObjectTableCatalogStore::new(backend)
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("intent-write-failure"))
        .await
        .expect("the same request can retry before any target publication");
}

#[tokio::test]
async fn interrupted_restore_survives_store_recreation_and_hides_partial_catalog() {
    let (backend, backup) = isolated_restore_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let namespace = Namespace::parse("sales").expect("fixture namespace");
    let table = IdentifierSegment::parse("orders").expect("fixture table");
    let table_path = paths.table_entry_path("analytics", &namespace, &table);
    backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let failed = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("restart-before-table"))
        .await;
    assert!(matches!(failed, Err(TableCatalogStoreError::Internal(_))));
    let pending = restore_control_json(&backend, &paths.catalog_restore_state_path("analytics")).await;
    assert_ne!(pending["state"], "COMPLETED");
    let epoch = pending["restore_epoch"].clone();
    assert!(epoch.as_str().is_some_and(|value| !value.is_empty()));
    assert_pending_restore_rejects_catalog_access(&backend).await;

    let before = maintenance_test_object_versions(&backend).await;
    let different_request = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("another-restore"))
        .await;
    assert!(matches!(different_request, Err(TableCatalogStoreError::Unavailable(_))));
    assert_eq!(maintenance_test_object_versions(&backend).await, before);

    let restored = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("restart-before-table"))
        .await
        .expect("the same request resumes its durable plan after restart");
    assert_eq!(restored.status, TableCatalogRestoreStatus::Restored);
    let completed = restore_control_json(&backend, &paths.catalog_restore_state_path("analytics")).await;
    assert_eq!(completed["restore_epoch"], epoch);
    assert_eq!(completed["state"], "COMPLETED");
    let loaded = ObjectTableCatalogStore::new(backend)
        .load_table("analytics", "sales", "orders")
        .await
        .expect("load fully published catalog")
        .expect("restored table exists");
    assert_eq!(loaded.table_uuid, "table-uuid");
    assert_eq!(
        loaded.metadata_location,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json")
    );
    assert_eq!(loaded.version_token, "token-v1");
    assert_eq!(loaded.generation, 1);
}

#[tokio::test]
async fn restore_retry_keeps_the_original_target_etag_after_store_recreation() {
    let (backend, backup, _) = backup_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let target = paths.table_bucket_entry_path("analytics");
    let mut original = test_bucket_entry("analytics");
    original
        .properties
        .insert("restore-target".to_string(), "original".to_string());
    backend
        .seed_object(RUSTFS_META_BUCKET, &target, serde_json::to_vec(&original).expect("original target"))
        .await;
    backend.fail_next_put(RUSTFS_META_BUCKET, &target).await;
    let failed = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some("etag-bound-retry"))
        .await;
    assert!(matches!(failed, Err(TableCatalogStoreError::Internal(_))));

    let mut winner = test_bucket_entry("analytics");
    winner
        .properties
        .insert("restore-target".to_string(), "concurrent-winner".to_string());
    let winning_bytes = serde_json::to_vec(&winner).expect("competing target");
    backend.seed_object(RUSTFS_META_BUCKET, &target, winning_bytes.clone()).await;
    let before = maintenance_test_object_versions(&backend).await;
    for _ in 0..2 {
        let retry = ObjectTableCatalogStore::new(backend.clone())
            .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some("etag-bound-retry"))
            .await;
        assert!(matches!(retry, Err(TableCatalogStoreError::Conflict(_))));
        assert_eq!(maintenance_test_object_versions(&backend).await, before);
        assert_eq!(
            backend
                .read_object(RUSTFS_META_BUCKET, &target)
                .await
                .expect("read concurrent winner")
                .expect("winner remains")
                .data,
            winning_bytes
        );
    }
    assert_pending_restore_rejects_catalog_access(&backend).await;
}

#[tokio::test]
async fn restore_put_response_loss_resumes_after_restart_without_rewriting_published_table() {
    let (backend, backup) = isolated_restore_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let table_path = paths.table_entry_path(
        "analytics",
        &Namespace::parse("sales").expect("namespace"),
        &IdentifierSegment::parse("orders").expect("table"),
    );
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let result = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("table-response-loss"))
        .await;
    assert!(matches!(result, Err(TableCatalogStoreError::Internal(_))));
    let table_before = backend
        .read_object(RUSTFS_META_BUCKET, &table_path)
        .await
        .expect("read table whose write response was lost")
        .expect("table write committed");
    assert_pending_restore_rejects_catalog_access(&backend).await;
    ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("table-response-loss"))
        .await
        .expect("restart resolves the ambiguous table publication");
    assert_eq!(
        backend
            .read_object(RUSTFS_META_BUCKET, &table_path)
            .await
            .expect("read published table"),
        Some(table_before),
        "retry rewrote an already committed table entry"
    );
}

#[tokio::test]
async fn restore_delete_response_loss_remains_hidden_until_restart_finishes_original_plan() {
    let (backend, backup, _) = backup_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let bucket_path = paths.table_bucket_entry_path("analytics");
    let (root, _) = bucket_path.rsplit_once('/').expect("catalog bucket prefix");
    let stale_object = format!("{root}/stale-restore-object");
    backend
        .seed_object(RUSTFS_META_BUCKET, &stale_object, b"stale".to_vec())
        .await;
    backend.fail_after_next_delete(RUSTFS_META_BUCKET, &stale_object).await;
    let result = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some("delete-response-loss"))
        .await;
    assert!(matches!(result, Err(TableCatalogStoreError::Internal(_))));
    assert!(
        backend
            .read_object(RUSTFS_META_BUCKET, &stale_object)
            .await
            .expect("read deleted extra")
            .is_none()
    );
    assert_pending_restore_rejects_catalog_access(&backend).await;
    ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some("delete-response-loss"))
        .await
        .expect("restart recognizes the completed conditional deletion");
    assert!(
        backend
            .read_object(RUSTFS_META_BUCKET, &stale_object)
            .await
            .expect("read deleted extra")
            .is_none()
    );
    assert!(
        ObjectTableCatalogStore::new(backend)
            .load_table("analytics", "sales", "orders")
            .await
            .expect("load completed restore")
            .is_some()
    );
}

#[tokio::test]
async fn completed_restore_replays_its_receipt_without_rolling_back_later_catalog_updates() {
    let (backend, backup) = isolated_restore_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let mut first = store
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("completed-replay"))
        .await
        .expect("complete initial restore");
    let namespace = Namespace::parse("later_namespace").expect("later namespace");
    store
        .create_namespace(test_namespace_entry("analytics", &namespace))
        .await
        .expect("later normal catalog mutation");
    let before = maintenance_test_object_versions(&backend).await;
    let replay = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("completed-replay"))
        .await
        .expect("replay completed restore receipt");
    first.status = TableCatalogRestoreStatus::AlreadyRestored;
    assert_eq!(replay, first);
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
    assert!(
        store
            .get_namespace("analytics", &namespace.public_name())
            .await
            .expect("load later namespace")
            .is_some()
    );
    let conflict = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some("completed-replay"))
        .await;
    assert!(matches!(conflict, Err(TableCatalogStoreError::Conflict(_))));
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}

#[tokio::test]
async fn corrupt_restore_barrier_or_intent_fails_closed_before_any_further_write() {
    for corrupt_intent in [false, true] {
        let (backend, backup) = isolated_restore_fixture().await;
        let paths = TableCatalogObjectPaths::default();
        let table_path = paths.table_entry_path(
            "analytics",
            &Namespace::parse("sales").expect("namespace"),
            &IdentifierSegment::parse("orders").expect("table"),
        );
        backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
        ObjectTableCatalogStore::new(backend.clone())
            .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("corrupt-control"))
            .await
            .expect_err("prepare interrupted restore");
        let corrupt_path = if corrupt_intent {
            paths.catalog_restore_intent_path("analytics", "corrupt-control")
        } else {
            paths.catalog_restore_state_path("analytics")
        };
        backend.seed_object(RUSTFS_META_BUCKET, &corrupt_path, b"{}".to_vec()).await;
        let before = maintenance_test_object_versions(&backend).await;
        let restarted = ObjectTableCatalogStore::new(backend.clone());
        assert!(restarted.load_table("analytics", "sales", "orders").await.is_err());
        assert!(restarted.list_namespaces("analytics").await.is_err());
        assert!(
            restarted
                .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("corrupt-control"))
                .await
                .is_err()
        );
        assert_eq!(maintenance_test_object_versions(&backend).await, before);
    }
}

#[tokio::test]
async fn restore_catalog_finalization_failure_is_recoverable_after_all_target_writes() {
    let (backend, backup) = isolated_restore_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let table_path = paths.table_entry_path(
        "analytics",
        &Namespace::parse("sales").expect("namespace"),
        &IdentifierSegment::parse("orders").expect("table"),
    );
    let intent_path = paths.catalog_restore_intent_path("analytics", "finalization-failure");
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let worker_backend = backend.clone();
    let backup_id = backup.backup_id.clone();
    let worker = tokio::spawn(async move {
        ObjectTableCatalogStore::new(worker_backend)
            .restore_object_catalog_backup_with_id("analytics", &backup_id, None, false, Some("finalization-failure"))
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .expect("restore reached table publication");
    backend.fail_next_put(RUSTFS_META_BUCKET, &intent_path).await;
    pause.release();
    let failed = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, worker)
        .await
        .expect("restore returned after finalization failure")
        .expect("restore worker joined");
    assert!(matches!(failed, Err(TableCatalogStoreError::Internal(_))));
    assert!(
        backend
            .read_object(RUSTFS_META_BUCKET, &table_path)
            .await
            .expect("read unpublished restored entry")
            .is_some()
    );
    let before = restore_published_object_versions(&backend).await;
    assert_pending_restore_rejects_catalog_access(&backend).await;
    let receipt = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("finalization-failure"))
        .await
        .expect("restart can finalize catalog whose target writes succeeded");
    assert_eq!(receipt.status, TableCatalogRestoreStatus::Restored);
    assert_eq!(restore_published_object_versions(&backend).await, before);
    let completed = restore_control_json(&backend, &paths.catalog_restore_state_path("analytics")).await;
    assert_eq!(completed["state"], "COMPLETED");
}

#[tokio::test]
async fn namespace_table_and_view_reads_cannot_cross_a_restore_with_identical_bucket_bytes() {
    for resource in ["namespace", "table", "view"] {
        let (backend, _, _) = backup_fixture_with_backend(TestCatalogObjectBackend::content_addressed()).await;
        let paths = TableCatalogObjectPaths::default();
        let namespace = Namespace::parse("sales").expect("namespace");
        let table = IdentifierSegment::parse("orders").expect("table");
        let view = IdentifierSegment::parse("recent_orders").expect("view");
        let store = ObjectTableCatalogStore::new(backend.clone());
        let view_path = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
        let view_entry = test_view_entry("analytics", &namespace, &view, view_path.clone());
        let view_metadata = serde_json::json!({
            "format-version": 1,
            "view-uuid": view_entry.view_uuid,
            "location": view_entry.warehouse_location,
            "schemas": [{
                "type": "struct", "schema-id": 0,
                "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
            }],
            "current-schema-id": 0,
            "current-version-id": 1,
            "versions": [{
                "version-id": 1, "timestamp-ms": 1, "schema-id": 0, "summary": {},
                "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}],
                "default-namespace": ["sales"]
            }],
            "version-log": [{"timestamp-ms": 1, "version-id": 1}],
            "properties": {}
        });
        backend
            .seed_object(
                "analytics",
                &view_path,
                serde_json::to_vec(&view_metadata).expect("valid fixture view metadata"),
            )
            .await;
        store.create_view(view_entry).await.expect("seed view before backup");
        let backup = store
            .create_object_catalog_backup("analytics", None)
            .await
            .expect("backup includes view");
        let bucket_path = paths.table_bucket_entry_path("analytics");
        let original_bucket = backend
            .read_object(RUSTFS_META_BUCKET, &bucket_path)
            .await
            .expect("read bucket before restore")
            .expect("bucket exists");
        let read_path = match resource {
            "namespace" => paths.namespace_entry_path("analytics", &namespace),
            "table" => paths.table_entry_path("analytics", &namespace, &table),
            "view" => paths.view_entry_path("analytics", &namespace, &view),
            _ => unreachable!(),
        };
        let pause = backend.pause_next_read(RUSTFS_META_BUCKET, &read_path).await;
        let reader_backend = backend.clone();
        let reader = tokio::spawn(async move {
            let reader = ObjectTableCatalogStore::new(reader_backend);
            match resource {
                "namespace" => reader.get_namespace("analytics", "sales").await.map(|_| ()),
                "table" => reader.load_table("analytics", "sales", "orders").await.map(|_| ()),
                "view" => reader.load_view("analytics", "sales", "recent_orders").await.map(|_| ()),
                _ => unreachable!(),
            }
        });
        tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
            .await
            .expect("catalog read captured entry before restore");
        store
            .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("read-across-restore"))
            .await
            .expect("complete restore while a reader retains old catalog entry");
        assert_eq!(
            backend
                .read_object(RUSTFS_META_BUCKET, &bucket_path)
                .await
                .expect("read identical bucket"),
            Some(original_bucket),
            "the test requires unchanged bucket bytes and content-addressed ETag"
        );
        pause.release();
        let result = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, reader)
            .await
            .expect("reader resumed")
            .expect("reader joined");
        assert_restore_unavailable(result, resource);
    }
}

#[tokio::test]
async fn restore_publication_response_loss_replays_a_receipt_without_rewriting_target_objects() {
    let (backend, backup) = isolated_restore_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let table_path = paths.table_entry_path(
        "analytics",
        &Namespace::parse("sales").expect("namespace"),
        &IdentifierSegment::parse("orders").expect("table"),
    );
    let state_path = paths.catalog_restore_state_path("analytics");
    let pause = backend.pause_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let worker_backend = backend.clone();
    let backup_id = backup.backup_id.clone();
    let worker = tokio::spawn(async move {
        ObjectTableCatalogStore::new(worker_backend)
            .restore_object_catalog_backup_with_id("analytics", &backup_id, None, false, Some("publication-response-loss"))
            .await
    });
    tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .expect("restore reached table publication");
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &state_path).await;
    pause.release();
    let response = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, worker)
        .await
        .expect("restore returned after publication response loss")
        .expect("restore worker joined");
    if let Err(error) = response {
        assert!(matches!(error, TableCatalogStoreError::Internal(_)));
    }
    let before = restore_published_object_versions(&backend).await;
    let recovered = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("publication-response-loss"))
        .await
        .expect("restart confirms publication after its response was lost");
    assert_eq!(restore_published_object_versions(&backend).await, before);
    let all_completed = maintenance_test_object_versions(&backend).await;
    let replay = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("publication-response-loss"))
        .await
        .expect("subsequent replay returns the same receipt");
    let mut expected_receipt = recovered;
    expected_receipt.status = TableCatalogRestoreStatus::AlreadyRestored;
    assert_eq!(replay, expected_receipt);
    assert_eq!(maintenance_test_object_versions(&backend).await, all_completed);
    assert_eq!(restore_control_json(&backend, &state_path).await["state"], "COMPLETED");
}

#[tokio::test]
async fn restore_retry_does_not_delete_an_object_recreated_after_lost_delete_response() {
    let (backend, backup, _) = backup_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let bucket_path = paths.table_bucket_entry_path("analytics");
    let (root, _) = bucket_path.rsplit_once('/').expect("catalog bucket prefix");
    let stale_object = format!("{root}/stale-delete-target");
    backend
        .seed_object(RUSTFS_META_BUCKET, &stale_object, b"stale".to_vec())
        .await;
    backend.fail_after_next_delete(RUSTFS_META_BUCKET, &stale_object).await;
    ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some("delete-etag-bound-retry"))
        .await
        .expect_err("prepare lost delete response");
    backend
        .seed_object(RUSTFS_META_BUCKET, &stale_object, b"recreated-winner".to_vec())
        .await;
    let before = maintenance_test_object_versions(&backend).await;
    let retry = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, true, Some("delete-etag-bound-retry"))
        .await;
    assert!(matches!(retry, Err(TableCatalogStoreError::Conflict(_))));
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
    assert_eq!(
        backend
            .read_object(RUSTFS_META_BUCKET, &stale_object)
            .await
            .expect("read replacement")
            .expect("replacement remains")
            .data,
        b"recreated-winner"
    );
    assert_pending_restore_rejects_catalog_access(&backend).await;
}

#[tokio::test]
async fn completed_restore_receipt_survives_a_later_restore_epoch() {
    let (backend, backup) = isolated_restore_fixture().await;
    let mut first = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("earlier-restore"))
        .await
        .expect("first restore");
    ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("later-restore"))
        .await
        .expect("a deliberate second restore has an independent identity");
    let state_path = TableCatalogObjectPaths::default().catalog_restore_state_path("analytics");
    let later_state = restore_control_json(&backend, &state_path).await;
    assert_eq!(later_state["restore_id"], "later-restore");
    let before = maintenance_test_object_versions(&backend).await;
    let replay = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("earlier-restore"))
        .await
        .expect("older request replays its own receipt");
    first.status = TableCatalogRestoreStatus::AlreadyRestored;
    assert_eq!(replay, first);
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
    assert_eq!(restore_control_json(&backend, &state_path).await, later_state);
}

#[tokio::test]
async fn restore_barrier_write_failure_cannot_publish_targets_before_retry() {
    let (backend, backup) = isolated_restore_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let barrier_path = paths.catalog_restore_state_path("analytics");
    backend.fail_next_put(RUSTFS_META_BUCKET, &barrier_path).await;
    let before = restore_published_object_versions(&backend).await;
    let failed = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("barrier-write-failure"))
        .await;
    assert!(matches!(failed, Err(TableCatalogStoreError::Internal(_))));
    assert_eq!(restore_published_object_versions(&backend).await, before);
    ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("barrier-write-failure"))
        .await
        .expect("same request can install its barrier after restart");
    assert!(
        ObjectTableCatalogStore::new(backend)
            .load_table("analytics", "sales", "orders")
            .await
            .expect("read completed restore")
            .is_some()
    );
}

#[tokio::test]
async fn legacy_catalog_control_state_fails_closed_without_silent_object_fallback() {
    for legacy_path in ["bucket", "global", "intent", "snapshot"] {
        let (backend, backup, _) = backup_fixture().await;
        let paths = TableCatalogObjectPaths::default();
        let path = match legacy_path {
            "bucket" => paths.legacy_backing_migration_fence_path("analytics"),
            "global" => paths.legacy_backing_migration_global_fence_path(),
            "intent" => paths.legacy_catalog_restore_intent_path("analytics"),
            "snapshot" => paths.legacy_catalog_snapshot_path(),
            _ => unreachable!(),
        };
        backend
            .seed_object(RUSTFS_META_BUCKET, &path, b"legacy-control-state".to_vec())
            .await;
        let before = maintenance_test_object_versions(&backend).await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        assert!(matches!(
            store.load_table("analytics", "sales", "orders").await,
            Err(TableCatalogStoreError::Unavailable(_))
        ));
        assert!(matches!(
            store
                .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("legacy-state"))
                .await,
            Err(TableCatalogStoreError::Unavailable(_))
        ));
        assert_eq!(maintenance_test_object_versions(&backend).await, before);
    }
}

#[tokio::test]
async fn maintenance_plan_uses_the_persisted_entry_metadata_directory() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let namespace = Namespace::parse("sales").expect("namespace");
    let table = IdentifierSegment::parse("orders").expect("table");
    let current = "tables/table-id/metadata/00003.metadata.json".to_string();
    let previous = "tables/table-id/metadata/00002.metadata.json".to_string();
    backend
        .seed_object("analytics", &current, br#"{"metadata-log":[]}"#.to_vec())
        .await;
    backend.seed_object("analytics", &previous, b"{}".to_vec()).await;
    store.put_table_bucket(test_bucket_entry("analytics")).await.expect("bucket");
    store
        .create_namespace(test_namespace_entry("analytics", &namespace))
        .await
        .expect("namespace");
    store
        .create_table(test_table_entry("analytics", &namespace, &table, current.clone()))
        .await
        .expect("table");

    let report = store
        .plan_table_metadata_maintenance("analytics", "sales", "orders", 0)
        .await
        .expect("entry-aware maintenance planning");
    assert_eq!(report.current_metadata_location, current);
    assert_eq!(report.cleanup_candidate_locations, vec![previous]);
}

#[tokio::test]
async fn legacy_per_bucket_control_state_does_not_block_an_unrelated_bucket() {
    let (backend, _, _) = backup_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &paths.legacy_backing_migration_fence_path("analytics"),
            b"malformed legacy fence".to_vec(),
        )
        .await;

    let store = ObjectTableCatalogStore::new(backend.clone());
    store
        .put_table_bucket(test_bucket_entry("unrelated"))
        .await
        .expect("a per-bucket legacy marker must not fence another bucket");
    assert!(
        store
            .get_table_bucket("unrelated")
            .await
            .expect("load unrelated bucket")
            .is_some()
    );
    assert!(matches!(
        store.load_table("analytics", "sales", "orders").await,
        Err(TableCatalogStoreError::Unavailable(_))
    ));
}

#[tokio::test]
async fn removing_legacy_control_state_restores_object_catalog_access() {
    let (backend, _, _) = backup_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let marker = paths.legacy_catalog_restore_intent_path("analytics");
    backend
        .seed_object(RUSTFS_META_BUCKET, &marker, b"unknown legacy intent".to_vec())
        .await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    assert!(matches!(
        store.load_table("analytics", "sales", "orders").await,
        Err(TableCatalogStoreError::Unavailable(_))
    ));

    backend
        .delete_object(RUSTFS_META_BUCKET, &marker)
        .await
        .expect("remove the explicit recovery marker");
    assert!(
        store
            .load_table("analytics", "sales", "orders")
            .await
            .expect("object catalog access after marker removal")
            .is_some()
    );
}

#[tokio::test]
async fn restore_lost_barrier_write_response_keeps_catalog_hidden_until_same_request_recovers() {
    let (backend, backup) = isolated_restore_fixture().await;
    let paths = TableCatalogObjectPaths::default();
    let barrier_path = paths.catalog_restore_state_path("analytics");
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &barrier_path).await;
    let before = restore_published_object_versions(&backend).await;
    let failed = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("barrier-response-loss"))
        .await;
    assert!(matches!(failed, Err(TableCatalogStoreError::Internal(_))));
    assert_eq!(restore_published_object_versions(&backend).await, before);
    assert_pending_restore_rejects_catalog_access(&backend).await;
    ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &backup.backup_id, None, false, Some("barrier-response-loss"))
        .await
        .expect("restart identifies an installed barrier whose write response was lost");
    assert_eq!(restore_control_json(&backend, &barrier_path).await["state"], "COMPLETED");
}

async fn backup_fixture_after_namespace_update() -> (TestCatalogObjectBackend, TableCatalogBackupReport, String) {
    let (backend, backup, _) = backup_fixture().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    store
        .update_namespace_properties(
            "analytics",
            "sales",
            NamespacePropertiesUpdate::try_new(
                Vec::new(),
                BTreeMap::from([("owner".to_string(), "changed-after-backup".to_string())]),
            )
            .expect("namespace update"),
        )
        .await
        .expect("advance target catalog after source backup");
    let current = store
        .create_object_catalog_backup("analytics", None)
        .await
        .expect("capture current target watermark");
    assert_ne!(backup.source_snapshot_etag, current.source_snapshot_etag);
    (backend, backup, current.source_snapshot_etag)
}

#[tokio::test]
async fn restore_expected_watermark_binds_the_current_target_instead_of_the_backup_source() {
    let (backend, backup, _) = backup_fixture_after_namespace_update().await;
    let before = maintenance_test_object_versions(&backend).await;
    let stale_source = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id(
            "analytics",
            &backup.backup_id,
            Some(&backup.source_snapshot_etag),
            true,
            Some("stale-source-watermark"),
        )
        .await;
    assert!(matches!(stale_source, Err(TableCatalogStoreError::Conflict(_))));
    assert_eq!(
        maintenance_test_object_versions(&backend).await,
        before,
        "a backup source watermark authorized replacement of a different target"
    );
}

#[tokio::test]
async fn restore_persists_the_target_watermark_and_binds_it_to_its_idempotent_receipt() {
    let (backend, backup, target_watermark) = backup_fixture_after_namespace_update().await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let mut restored = store
        .restore_object_catalog_backup_with_id(
            "analytics",
            &backup.backup_id,
            Some(&target_watermark),
            true,
            Some("current-target-watermark"),
        )
        .await
        .expect("the current target watermark authorizes restore of an older backup");
    assert_eq!(restored.previous_snapshot_etag.as_deref(), Some(target_watermark.as_str()));
    assert_eq!(restored.restored_snapshot_etag, backup.source_snapshot_etag);
    assert!(
        store
            .get_namespace("analytics", "sales")
            .await
            .expect("load restored namespace")
            .expect("namespace restored")
            .properties
            .is_empty()
    );
    let before = maintenance_test_object_versions(&backend).await;
    let replay = ObjectTableCatalogStore::new(backend.clone())
        .restore_object_catalog_backup_with_id(
            "analytics",
            &backup.backup_id,
            Some(&target_watermark),
            true,
            Some("current-target-watermark"),
        )
        .await
        .expect("same request replays the original target-bound receipt");
    restored.status = TableCatalogRestoreStatus::AlreadyRestored;
    assert_eq!(replay, restored);
    assert_eq!(maintenance_test_object_versions(&backend).await, before);

    for (expected, allow_replace) in [
        (Some(backup.source_snapshot_etag.as_str()), true),
        (Some(target_watermark.as_str()), false),
        (None, true),
    ] {
        let changed_request = ObjectTableCatalogStore::new(backend.clone())
            .restore_object_catalog_backup_with_id(
                "analytics",
                &backup.backup_id,
                expected,
                allow_replace,
                Some("current-target-watermark"),
            )
            .await;
        assert!(matches!(changed_request, Err(TableCatalogStoreError::Conflict(_))));
        assert_eq!(maintenance_test_object_versions(&backend).await, before);
    }
}
