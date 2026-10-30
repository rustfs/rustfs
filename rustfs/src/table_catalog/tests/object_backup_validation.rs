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

async fn backup_fixture() -> (TestCatalogObjectBackend, TableCatalogBackupReport, String) {
    let backend = TestCatalogObjectBackend::default();
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
