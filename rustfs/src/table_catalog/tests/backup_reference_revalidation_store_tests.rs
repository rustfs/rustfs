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
use crate::table_catalog::test_support::{
    TestCatalogObjectPause, manifest_avro_bytes as reference_manifest_bytes,
    manifest_list_avro_bytes as reference_manifest_list_bytes,
};

const REFERENCE_DATA: &str = "tables/table-id/data/revalidation.parquet";
const REFERENCE_MANIFEST: &str = "tables/table-id/metadata/revalidation.avro";
const REFERENCE_LIST: &str = "tables/table-id/metadata/revalidation-list.avro";
const REFERENCE_STATISTICS: &str = "tables/table-id/metadata/revalidation-statistics.puffin";

struct ReferenceRestoreMutation {
    object: String,
    original: Vec<u8>,
    changed: Vec<u8>,
}

struct ReferenceRestoreFixture {
    backend: TestCatalogObjectBackend,
    backup: TableCatalogBackupReport,
    expected_table: TableEntry,
    mutations: Vec<ReferenceRestoreMutation>,
}

#[derive(Debug, PartialEq)]
struct ReferenceRestoreWrites {
    objects: BTreeMap<(String, String), TableCatalogObject>,
    puts: BTreeMap<(String, String), usize>,
    deletes: BTreeMap<(String, String), usize>,
}

async fn reference_restore_writes(backend: &TestCatalogObjectBackend) -> ReferenceRestoreWrites {
    let objects = maintenance_test_object_versions(backend).await;
    let state = backend.state.lock().await;
    ReferenceRestoreWrites {
        objects,
        puts: state.put_attempts.clone(),
        deletes: state.delete_attempts.clone(),
    }
}

// Seed a valid source graph, then cross the real raw-import and controlled-prepare boundaries.
async fn reference_restore_fixture() -> ReferenceRestoreFixture {
    let (source, _, current_path) = backup_fixture().await;
    let store = ObjectTableCatalogStore::new(source.clone());
    let namespace = Namespace::parse("sales").expect("reference fixture namespace");
    let view = IdentifierSegment::parse("recent_orders").expect("reference fixture view");
    let history_path = default_table_metadata_file_path(
        &namespace,
        &IdentifierSegment::parse("orders").expect("reference fixture table"),
        "00000.metadata.json",
    );
    let mut history = table_metadata_json_for_backup("analytics", "table-id", "table-uuid");
    history["properties"]["reference-watermark"] = serde_json::json!("a");
    let history_bytes = serde_json::to_vec(&history).expect("serialize valid history metadata");
    history["properties"]["reference-watermark"] = serde_json::json!("b");
    let changed_history = serde_json::to_vec(&history).expect("serialize same-length valid changed history");
    let mut metadata = table_metadata_json_for_backup("analytics", "table-id", "table-uuid");
    metadata["last-sequence-number"] = serde_json::json!(7);
    metadata["last-updated-ms"] = serde_json::json!(2);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 42, "sequence-number": 7, "timestamp-ms": 2,
        "manifest-list": format!("s3://analytics/{REFERENCE_LIST}"), "summary": {"operation": "append"}
    }]);
    metadata["current-snapshot-id"] = serde_json::json!(42);
    metadata["snapshot-log"] = serde_json::json!([{"timestamp-ms": 2, "snapshot-id": 42}]);
    metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 42}});
    metadata["metadata-log"] = serde_json::json!([{
        "timestamp-ms": 1, "metadata-file": format!("s3://analytics/{history_path}")
    }]);
    metadata["statistics"] = serde_json::json!([{
        "snapshot-id": 42, "statistics-path": format!("s3://analytics/{REFERENCE_STATISTICS}"),
        "file-size-in-bytes": 8, "file-footer-size-in-bytes": 4, "blob-metadata": []
    }]);
    validate_supported_table_metadata(&metadata).expect("reference fixture must contain valid nonempty metadata");
    let data_location = format!("s3://analytics/{REFERENCE_DATA}");
    let manifest = reference_manifest_bytes(&[(&data_location, 0, 1, 42, 7)]);
    let manifest_location = format!("s3://analytics/{REFERENCE_MANIFEST}");
    source.seed_object("analytics", REFERENCE_DATA, b"x".to_vec()).await;
    source
        .seed_object("analytics", REFERENCE_STATISTICS, b"PFA1PFA1".to_vec())
        .await;
    source.seed_object("analytics", &history_path, history_bytes.clone()).await;
    source
        .seed_object(
            "analytics",
            REFERENCE_LIST,
            reference_manifest_list_bytes(&[(&manifest_location, manifest.len())], 7, 42),
        )
        .await;
    source.seed_object("analytics", REFERENCE_MANIFEST, manifest).await;
    source
        .seed_object(
            "analytics",
            &current_path,
            serde_json::to_vec(&metadata).expect("serialize current metadata"),
        )
        .await;

    let view_path = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let entry = test_view_entry("analytics", &namespace, &view, view_path.clone());
    let mut view_metadata = serde_json::json!({
        "format-version": 1, "view-uuid": entry.view_uuid, "location": entry.warehouse_location,
        "schemas": [{"type": "struct", "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
        "current-schema-id": 0, "current-version-id": 1,
        "versions": [{"version-id": 1, "timestamp-ms": 1, "schema-id": 0, "summary": {},
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}],
            "default-namespace": ["sales"]}],
        "version-log": [{"timestamp-ms": 1, "version-id": 1}], "properties": {}
    });
    validate_supported_view_metadata(&view_metadata).expect("reference fixture view metadata is valid");
    let view_bytes = serde_json::to_vec(&view_metadata).expect("serialize view metadata");
    view_metadata["versions"][0]["representations"][0]["sql"] = serde_json::json!("SELECT 2");
    let changed_view = serde_json::to_vec(&view_metadata).expect("serialize same-length valid changed view");
    source.seed_object("analytics", &view_path, view_bytes.clone()).await;
    store
        .create_view(entry)
        .await
        .expect("create the source view through the store");
    let expected_table = store
        .load_table("analytics", "sales", "orders")
        .await
        .expect("source table read")
        .expect("source table");
    let backup = store
        .create_object_catalog_backup("analytics", None)
        .await
        .expect("capture real v2 nonempty reference inventory");
    assert_eq!(backup.source_snapshot_version, 2);
    let artifact = store
        .export_object_catalog_backup("analytics", &backup.backup_id)
        .await
        .expect("export original artifact bytes");
    let decoded: serde_json::Value = serde_json::from_slice(&artifact).expect("artifact is JSON");
    let target = TestCatalogObjectBackend::default();
    let target_store = ObjectTableCatalogStore::new(target.clone());
    target_store
        .import_object_catalog_backup("analytics", artifact.clone())
        .await
        .expect("import artifact without catalog publication");
    let references = decoded["references"].as_array().expect("v2 artifact has references");
    assert!(references.iter().any(|reference| reference["kind"] == "DATA_FILE"));
    for reference in references {
        let object = reference["object"].as_str().expect("reference key");
        if object == current_path {
            // The immutable current metadata remains a real restore-plan write.
            continue;
        }
        let payload = source
            .read_object("analytics", object)
            .await
            .expect("read source reference")
            .expect("source reference exists");
        target_store
            .prepare_object_catalog_backup_object("analytics", &backup.backup_id, object, payload.data)
            .await
            .expect("prepare exact referenced bytes without publishing the catalog");
    }
    assert!(
        target_store
            .load_table("analytics", "sales", "orders")
            .await
            .expect("empty target read")
            .is_none()
    );
    let mutations = vec![
        ReferenceRestoreMutation {
            object: REFERENCE_DATA.to_string(),
            original: b"x".to_vec(),
            changed: b"y".to_vec(),
        },
        ReferenceRestoreMutation {
            object: history_path,
            original: history_bytes,
            changed: changed_history,
        },
        ReferenceRestoreMutation {
            object: view_path,
            original: view_bytes,
            changed: changed_view,
        },
        ReferenceRestoreMutation {
            object: REFERENCE_STATISTICS.to_string(),
            original: b"PFA1PFA1".to_vec(),
            changed: b"PFA1PFA2".to_vec(),
        },
    ];
    for mutation in &mutations {
        assert_eq!(
            mutation.original.len(),
            mutation.changed.len(),
            "content, rather than size, is the regression oracle"
        );
        assert_ne!(mutation.original, mutation.changed);
    }
    ReferenceRestoreFixture {
        backend: target,
        backup,
        expected_table,
        mutations,
    }
}

type ReferenceRestoreTask = tokio::task::JoinHandle<TableCatalogStoreResult<TableCatalogRestoreReport>>;

async fn wait_reference_restore_pause(pause: &TestCatalogObjectPause, worker: &mut ReferenceRestoreTask) {
    if tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, pause.wait_started())
        .await
        .is_err()
    {
        worker.abort();
        pause.release();
        let _ = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, worker).await;
        panic!("restore did not reach its bounded table-publication barrier");
    }
}

async fn join_reference_restore(mut worker: ReferenceRestoreTask) -> TableCatalogStoreResult<TableCatalogRestoreReport> {
    match tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, &mut worker).await {
        Ok(joined) => joined.expect("restore worker must not panic"),
        Err(_) => {
            worker.abort();
            let _ = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, worker).await;
            panic!("restore worker did not finish within its bounded operation window");
        }
    }
}

fn assert_reference_watermark_conflict(result: TableCatalogStoreResult<TableCatalogRestoreReport>) {
    assert!(
        matches!(result, Err(TableCatalogStoreError::Conflict(message)) if message.contains("watermark")),
        "changed reference bytes must fail the inventory watermark before restore completion"
    );
}

async fn reference_restore_assert_published(fixture: &ReferenceRestoreFixture, restore_id: &str) {
    let store = ObjectTableCatalogStore::new(fixture.backend.clone());
    assert_eq!(
        store
            .load_table("analytics", "sales", "orders")
            .await
            .expect("fully published table read"),
        Some(fixture.expected_table.clone())
    );
    assert!(
        store
            .load_view("analytics", "sales", "recent_orders")
            .await
            .expect("fully published view read")
            .is_some()
    );
    let paths = TableCatalogObjectPaths::default();
    assert_eq!(
        restore_control_json(&fixture.backend, &paths.catalog_restore_state_path("analytics")).await["state"],
        "COMPLETED"
    );
    assert_eq!(
        restore_control_json(&fixture.backend, &paths.catalog_restore_intent_path("analytics", restore_id)).await["state"],
        "COMPLETED"
    );
}

#[tokio::test]
async fn interrupted_v2_restore_revalidates_every_reference_before_resuming_the_frozen_plan() {
    for changed_reference in 0..4 {
        let fixture = reference_restore_fixture().await;
        let paths = TableCatalogObjectPaths::default();
        let restore_id = "interrupted-reference-watermark";
        let table_path = paths.table_entry_path(
            "analytics",
            &Namespace::parse("sales").expect("namespace"),
            &IdentifierSegment::parse("orders").expect("table"),
        );
        let pause = fixture.backend.pause_next_put(RUSTFS_META_BUCKET, &table_path).await;
        let worker_backend = fixture.backend.clone();
        let backup_id = fixture.backup.backup_id.clone();
        let mut worker = tokio::spawn(async move {
            ObjectTableCatalogStore::new(worker_backend)
                .restore_object_catalog_backup_with_id("analytics", &backup_id, None, false, Some(restore_id))
                .await
        });
        wait_reference_restore_pause(&pause, &mut worker).await;
        worker.abort();
        pause.release();
        let cancelled = tokio::time::timeout(TABLE_CATALOG_TEST_TIMEOUT, worker)
            .await
            .expect("cancelled restore releases its publication and backup guards")
            .expect_err("restore request was cancelled");
        assert!(cancelled.is_cancelled());
        let intent_path = paths.catalog_restore_intent_path("analytics", restore_id);
        assert_eq!(restore_control_json(&fixture.backend, &intent_path).await["state"], "PREPARED");
        assert_eq!(
            restore_control_json(&fixture.backend, &paths.catalog_restore_state_path("analytics")).await["state"],
            "APPLYING"
        );
        let mutation = &fixture.mutations[changed_reference];
        fixture
            .backend
            .seed_object("analytics", &mutation.object, mutation.changed.clone())
            .await;
        let before = reference_restore_writes(&fixture.backend).await;
        assert_reference_watermark_conflict(
            ObjectTableCatalogStore::new(fixture.backend.clone())
                .restore_object_catalog_backup_with_id("analytics", &fixture.backup.backup_id, None, false, Some(restore_id))
                .await,
        );
        assert_eq!(
            reference_restore_writes(&fixture.backend).await,
            before,
            "a failed resume must neither advance controls nor mutate the frozen catalog targets"
        );
        assert_pending_restore_rejects_catalog_access(&fixture.backend).await;
        fixture
            .backend
            .seed_object("analytics", &mutation.object, mutation.original.clone())
            .await;
        let restored = ObjectTableCatalogStore::new(fixture.backend.clone())
            .restore_object_catalog_backup_with_id("analytics", &fixture.backup.backup_id, None, false, Some(restore_id))
            .await
            .expect("the original restore-id resumes after exact reference bytes are repaired");
        assert_eq!(restored.status, TableCatalogRestoreStatus::Restored);
        reference_restore_assert_published(&fixture, restore_id).await;
    }
}

#[tokio::test]
async fn v2_restore_final_verification_rejects_reference_changes_after_preflight() {
    for changed_reference in 0..4 {
        let fixture = reference_restore_fixture().await;
        let paths = TableCatalogObjectPaths::default();
        let restore_id = "final-reference-watermark";
        let table_path = paths.table_entry_path(
            "analytics",
            &Namespace::parse("sales").expect("namespace"),
            &IdentifierSegment::parse("orders").expect("table"),
        );
        let pause = fixture.backend.pause_next_put(RUSTFS_META_BUCKET, &table_path).await;
        let worker_backend = fixture.backend.clone();
        let backup_id = fixture.backup.backup_id.clone();
        let mut worker = tokio::spawn(async move {
            ObjectTableCatalogStore::new(worker_backend)
                .restore_object_catalog_backup_with_id("analytics", &backup_id, None, false, Some(restore_id))
                .await
        });
        wait_reference_restore_pause(&pause, &mut worker).await;
        let mutation = &fixture.mutations[changed_reference];
        fixture
            .backend
            .seed_object("analytics", &mutation.object, mutation.changed.clone())
            .await;
        pause.release();
        assert_reference_watermark_conflict(join_reference_restore(worker).await);
        assert_eq!(
            restore_control_json(&fixture.backend, &paths.catalog_restore_state_path("analytics")).await["state"],
            "APPLYING"
        );
        assert_eq!(
            restore_control_json(&fixture.backend, &paths.catalog_restore_intent_path("analytics", restore_id)).await["state"],
            "PREPARED"
        );
        assert_pending_restore_rejects_catalog_access(&fixture.backend).await;
        fixture
            .backend
            .seed_object("analytics", &mutation.object, mutation.original.clone())
            .await;
        let restored = ObjectTableCatalogStore::new(fixture.backend.clone())
            .restore_object_catalog_backup_with_id("analytics", &fixture.backup.backup_id, None, false, Some(restore_id))
            .await
            .expect("the same frozen plan finishes after exact reference repair");
        assert_eq!(restored.status, TableCatalogRestoreStatus::Restored);
        reference_restore_assert_published(&fixture, restore_id).await;
    }
}

#[tokio::test]
async fn completed_v2_restore_receipt_replays_after_later_reference_changes_without_revalidation_or_writes() {
    let fixture = reference_restore_fixture().await;
    let restore_id = "completed-reference-watermark";
    let mut receipt = ObjectTableCatalogStore::new(fixture.backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &fixture.backup.backup_id, None, false, Some(restore_id))
        .await
        .expect("complete a healthy nonempty v2 restore");
    reference_restore_assert_published(&fixture, restore_id).await;
    for mutation in &fixture.mutations {
        fixture
            .backend
            .seed_object("analytics", &mutation.object, mutation.changed.clone())
            .await;
    }
    let before = reference_restore_writes(&fixture.backend).await;
    let replay = ObjectTableCatalogStore::new(fixture.backend.clone())
        .restore_object_catalog_backup_with_id("analytics", &fixture.backup.backup_id, None, false, Some(restore_id))
        .await
        .expect("a completed receipt records the original request rather than authorizing another restore");
    receipt.status = TableCatalogRestoreStatus::AlreadyRestored;
    assert_eq!(replay, receipt);
    assert_eq!(
        reference_restore_writes(&fixture.backend).await,
        before,
        "completed receipt replay must not validate, repair, or overwrite later reference state"
    );
    reference_restore_assert_published(&fixture, restore_id).await;
}
