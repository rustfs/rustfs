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
use serde::{Deserialize, Serialize};
use sha2::Digest;

const FIXTURE_BACKUP_ID: &str = "83e3024f278b0abeecdbde378068980c3d5c9a2a5ddaa915495d5b398d1d05c2";
const TRANSFER_LIMIT: usize = 64 * 1024 * 1024;

fn fixture_bytes() -> Vec<u8> {
    include_bytes!("fixtures/object-catalog-backup-v1.json").to_vec()
}

fn fixture_path() -> String {
    TableCatalogObjectPaths::default().catalog_backup_path("analytics", FIXTURE_BACKUP_ID)
}

// Keep the v1 field order when constructing self-consistent negative fixtures.
// Their digests are valid so the test reaches identity validation, not only hashing.
#[derive(Serialize, Deserialize)]
struct TransferFixtureObject {
    bucket: String,
    object: String,
    size_bytes: u64,
    etag: Option<String>,
    sha256: String,
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct TransferFixture {
    version: u16,
    backup_id: String,
    table_bucket: String,
    created_at: String,
    source_catalog_etag: String,
    objects: Vec<TransferFixtureObject>,
}

fn digest(data: &[u8]) -> String {
    hex_simd::encode_to_string(sha2::Sha256::digest(data), hex_simd::AsciiCase::Lower)
}

fn reseal_fixture(fixture: &mut TransferFixture) -> Vec<u8> {
    for object in &mut fixture.objects {
        object.size_bytes = u64::try_from(object.data.len()).unwrap();
        object.sha256 = digest(&object.data);
    }
    fixture
        .objects
        .sort_by(|left, right| (&left.bucket, &left.object).cmp(&(&right.bucket, &right.object)));
    fixture.source_catalog_etag = digest(
        &serde_json::to_vec(
            &fixture
                .objects
                .iter()
                .map(|object| (&object.bucket, &object.object, &object.sha256))
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    );
    fixture.backup_id = digest(
        &serde_json::to_vec(&(fixture.version, &fixture.table_bucket, &fixture.source_catalog_etag, &fixture.objects)).unwrap(),
    );
    serde_json::to_vec(fixture).unwrap()
}

#[tokio::test]
async fn export_preserves_the_saved_v1_bytes_and_creation_timestamp() {
    let backend = TestCatalogObjectBackend::default();
    let mut bytes = b" \n".to_vec();
    bytes.extend(fixture_bytes());
    bytes.extend_from_slice(b"\n ");
    backend.seed_object(RUSTFS_META_BUCKET, &fixture_path(), bytes.clone()).await;
    let before = maintenance_test_object_versions(&backend).await;
    let exported = ObjectTableCatalogStore::new(backend.clone())
        .export_object_catalog_backup("analytics", FIXTURE_BACKUP_ID)
        .await
        .expect("export original bytes without reencoding");
    assert_eq!(exported, bytes);
    let exported: TransferFixture = serde_json::from_slice(&exported).unwrap();
    assert_eq!(exported.created_at, "2026-10-30T03:24:02.276322305Z");
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}

#[tokio::test]
async fn import_only_creates_an_artifact_and_exact_retries_do_not_rewrite_it() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bytes = fixture_bytes();
    let mut first = store.import_object_catalog_backup("analytics", bytes.clone()).await.unwrap();
    assert_eq!(first.status, TableCatalogBackupStatus::Created);
    assert_eq!(first.backup_id, FIXTURE_BACKUP_ID);
    assert_eq!(first.source_snapshot_version, 1);
    assert_eq!(first.created_at, "2026-10-30T03:24:02.276322305Z");
    let before = maintenance_test_object_versions(&backend).await;
    assert_eq!(before.len(), 1, "import must not publish any catalog or metadata objects");
    assert_eq!(before.get(&(RUSTFS_META_BUCKET.to_string(), fixture_path())).unwrap().data, bytes);
    assert!(store.get_table_bucket("analytics").await.unwrap().is_none());
    assert!(store.load_table("analytics", "sales", "orders").await.unwrap().is_none());
    let repeated = store
        .import_object_catalog_backup("analytics", fixture_bytes())
        .await
        .unwrap();
    first.status = TableCatalogBackupStatus::AlreadyPresent;
    assert_eq!(repeated, first);
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}

#[tokio::test]
async fn same_identity_with_different_saved_bytes_is_a_conflict() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    store
        .import_object_catalog_backup("analytics", fixture_bytes())
        .await
        .unwrap();
    let before = maintenance_test_object_versions(&backend).await;
    let mut different_bytes = fixture_bytes();
    different_bytes.push(b'\n');
    let error = store
        .import_object_catalog_backup("analytics", different_bytes)
        .await
        .unwrap_err();
    assert!(matches!(error, TableCatalogStoreError::Conflict(_)));
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}

#[tokio::test]
async fn invalid_or_foreign_artifacts_do_not_write_any_objects() {
    let fixture = fixture_bytes();
    let mut corrupt: serde_json::Value = serde_json::from_slice(&fixture).unwrap();
    corrupt["objects"][0]["sha256"] = serde_json::json!("corrupt");
    let mut unknown: serde_json::Value = serde_json::from_slice(&fixture).unwrap();
    unknown["force"] = serde_json::json!(true);
    for (bucket, bytes) in [
        ("analytics", Vec::new()),
        ("analytics", b"not-json".to_vec()),
        ("analytics", serde_json::to_vec(&corrupt).unwrap()),
        ("analytics", serde_json::to_vec(&unknown).unwrap()),
        ("other-bucket", fixture),
    ] {
        let backend = TestCatalogObjectBackend::default();
        let result = ObjectTableCatalogStore::new(backend.clone())
            .import_object_catalog_backup(bucket, bytes)
            .await;
        assert!(result.is_err());
        assert!(maintenance_test_object_versions(&backend).await.is_empty());
    }
}

#[tokio::test]
async fn pure_metadata_identity_validation_precedes_import_storage() {
    for field in ["table-uuid", "location", "format-version", "empty-metadata"] {
        let mut artifact: TransferFixture = serde_json::from_slice(&fixture_bytes()).unwrap();
        let metadata = artifact
            .objects
            .iter_mut()
            .find(|object| object.bucket == "analytics")
            .unwrap();
        if field == "empty-metadata" {
            metadata.data.clear();
        } else {
            let mut value: serde_json::Value = serde_json::from_slice(&metadata.data).unwrap();
            value[field] = match field {
                "table-uuid" => serde_json::json!("another-table-uuid"),
                "location" => serde_json::json!("s3://analytics/tables/another-id"),
                "format-version" => serde_json::json!(0),
                _ => unreachable!(),
            };
            metadata.data = serde_json::to_vec(&value).unwrap();
        }
        let bytes = reseal_fixture(&mut artifact);
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let result = store.import_object_catalog_backup("analytics", bytes.clone()).await;
        assert!(matches!(result, Err(TableCatalogStoreError::Invalid(_))), "invalid {field} was imported");
        assert!(maintenance_test_object_versions(&backend).await.is_empty());
        let path = TableCatalogObjectPaths::default().catalog_backup_path("analytics", &artifact.backup_id);
        backend.seed_object(RUSTFS_META_BUCKET, &path, bytes).await;
        let before = maintenance_test_object_versions(&backend).await;
        assert!(matches!(
            store.export_object_catalog_backup("analytics", &artifact.backup_id).await,
            Err(TableCatalogStoreError::Invalid(_)),
        ));
        assert_eq!(maintenance_test_object_versions(&backend).await, before);
    }
}

#[tokio::test]
async fn missing_embedded_current_metadata_is_rejected_before_import() {
    let mut artifact: TransferFixture = serde_json::from_slice(&fixture_bytes()).unwrap();
    artifact.objects.retain(|object| object.bucket != "analytics");
    let bytes = reseal_fixture(&mut artifact);
    let backend = TestCatalogObjectBackend::default();
    let result = ObjectTableCatalogStore::new(backend.clone())
        .import_object_catalog_backup("analytics", bytes)
        .await;
    assert!(matches!(result, Err(TableCatalogStoreError::NotFound(_))));
    assert!(maintenance_test_object_versions(&backend).await.is_empty());
}

#[tokio::test]
async fn ambiguous_import_put_is_confirmed_without_rewriting_the_artifact() {
    let backend = TestCatalogObjectBackend::default();
    let path = fixture_path();
    backend.fail_after_next_put(RUSTFS_META_BUCKET, &path).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let first = store
        .import_object_catalog_backup("analytics", fixture_bytes())
        .await
        .expect("a matching saved artifact proves an ambiguous import completed");
    assert_eq!(first.status, TableCatalogBackupStatus::AlreadyPresent);
    let before = maintenance_test_object_versions(&backend).await;
    assert_eq!(before.len(), 1);
    let repeated = ObjectTableCatalogStore::new(backend.clone())
        .import_object_catalog_backup("analytics", fixture_bytes())
        .await
        .unwrap();
    assert_eq!(repeated, first);
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
    assert_eq!(backend.state.lock().await.put_attempts[&(RUSTFS_META_BUCKET.to_string(), path)], 1);
}

#[tokio::test]
async fn import_failure_before_put_leaves_no_artifact_and_can_be_retried() {
    let backend = TestCatalogObjectBackend::default();
    backend.fail_next_put(RUSTFS_META_BUCKET, &fixture_path()).await;
    let result = ObjectTableCatalogStore::new(backend.clone())
        .import_object_catalog_backup("analytics", fixture_bytes())
        .await;
    assert!(matches!(result, Err(TableCatalogStoreError::Internal(_))));
    assert!(maintenance_test_object_versions(&backend).await.is_empty());
    let retried = ObjectTableCatalogStore::new(backend.clone())
        .import_object_catalog_backup("analytics", fixture_bytes())
        .await
        .unwrap();
    assert_eq!(retried.status, TableCatalogBackupStatus::Created);
    assert_eq!(maintenance_test_object_versions(&backend).await.len(), 1);
}

#[tokio::test]
async fn concurrent_identical_imports_create_exactly_one_artifact_version() {
    let backend = TestCatalogObjectBackend::default();
    let first_store = ObjectTableCatalogStore::new(backend.clone());
    let second_store = ObjectTableCatalogStore::new(backend.clone());
    let (first, second) = tokio::join!(
        first_store.import_object_catalog_backup("analytics", fixture_bytes()),
        second_store.import_object_catalog_backup("analytics", fixture_bytes()),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(
        usize::from(first.status == TableCatalogBackupStatus::Created)
            + usize::from(second.status == TableCatalogBackupStatus::Created),
        1,
    );
    assert_eq!(first.backup_id, second.backup_id);
    assert_eq!(first.created_at, second.created_at);
    assert_eq!(maintenance_test_object_versions(&backend).await.len(), 1);
    assert_eq!(
        backend.state.lock().await.put_attempts[&(RUSTFS_META_BUCKET.to_string(), fixture_path())],
        1,
    );
}

#[tokio::test]
async fn export_rejects_a_saved_artifact_under_the_wrong_backup_identity() {
    let backend = TestCatalogObjectBackend::default();
    let path = TableCatalogObjectPaths::default().catalog_backup_path("analytics", "wrong-backup");
    backend.seed_object(RUSTFS_META_BUCKET, &path, fixture_bytes()).await;
    let before = maintenance_test_object_versions(&backend).await;
    let result = ObjectTableCatalogStore::new(backend.clone())
        .export_object_catalog_backup("analytics", "wrong-backup")
        .await;
    assert!(matches!(result, Err(TableCatalogStoreError::Invalid(_))));
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}

#[tokio::test]
async fn artifact_import_does_not_require_the_external_snapshot_graph_to_be_prepared() {
    let mut artifact: TransferFixture = serde_json::from_slice(&fixture_bytes()).unwrap();
    let metadata = artifact
        .objects
        .iter_mut()
        .find(|object| object.bucket == "analytics")
        .unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&metadata.data).unwrap();
    value["last-sequence-number"] = serde_json::json!(1);
    value["snapshots"] = serde_json::json!([{
        "snapshot-id": 1,
        "sequence-number": 1,
        "timestamp-ms": 1,
        "manifest-list": "s3://analytics/tables/table-id/metadata/snap-1.avro",
        "summary": {"operation": "append"}
    }]);
    value["current-snapshot-id"] = serde_json::json!(1);
    value["snapshot-log"] = serde_json::json!([{"timestamp-ms": 1, "snapshot-id": 1}]);
    value["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 1}});
    metadata.data = serde_json::to_vec(&value).unwrap();
    let bytes = reseal_fixture(&mut artifact);
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let imported = store
        .import_object_catalog_backup("analytics", bytes)
        .await
        .expect("import stores only the candidate artifact before object preparation");
    let before = maintenance_test_object_versions(&backend).await;
    assert_eq!(before.len(), 1);
    let restore = store
        .restore_object_catalog_backup_with_id("analytics", &imported.backup_id, None, false, Some("unprepared-graph"))
        .await;
    assert!(restore.is_err(), "publishing still requires the external graph");
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}

#[tokio::test]
async fn pending_restore_rejects_artifact_export_and_import_without_mutation() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    store
        .import_object_catalog_backup("analytics", fixture_bytes())
        .await
        .unwrap();
    let table_path = TableCatalogObjectPaths::default().table_entry_path(
        "analytics",
        &Namespace::parse("sales").unwrap(),
        &IdentifierSegment::parse("orders").unwrap(),
    );
    backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
    store
        .restore_object_catalog_backup_with_id("analytics", FIXTURE_BACKUP_ID, None, false, Some("transfer-pending"))
        .await
        .expect_err("prepare interrupted restore");
    let before = maintenance_test_object_versions(&backend).await;
    assert!(matches!(
        store.export_object_catalog_backup("analytics", FIXTURE_BACKUP_ID).await,
        Err(TableCatalogStoreError::Unavailable(_)),
    ));
    assert!(matches!(
        store.import_object_catalog_backup("analytics", fixture_bytes()).await,
        Err(TableCatalogStoreError::Unavailable(_)),
    ));
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}

#[tokio::test]
async fn legacy_control_state_rejects_both_transfer_operations() {
    let paths = TableCatalogObjectPaths::default();
    for marker in [
        paths.legacy_backing_migration_fence_path("analytics"),
        paths.legacy_backing_migration_global_fence_path(),
        paths.legacy_catalog_restore_intent_path("analytics"),
        paths.legacy_catalog_snapshot_path(),
    ] {
        let backend = TestCatalogObjectBackend::default();
        backend
            .seed_object(RUSTFS_META_BUCKET, &fixture_path(), fixture_bytes())
            .await;
        backend
            .seed_object(RUSTFS_META_BUCKET, &marker, b"legacy-control".to_vec())
            .await;
        let before = maintenance_test_object_versions(&backend).await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        assert!(matches!(
            store.export_object_catalog_backup("analytics", FIXTURE_BACKUP_ID).await,
            Err(TableCatalogStoreError::Unavailable(_)),
        ));
        assert!(matches!(
            store.import_object_catalog_backup("analytics", fixture_bytes()).await,
            Err(TableCatalogStoreError::Unavailable(_)),
        ));
        assert_eq!(maintenance_test_object_versions(&backend).await, before);
    }
}

#[tokio::test]
async fn transfer_body_limit_accepts_the_exact_boundary_and_rejects_one_more_byte() {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let mut exact = fixture_bytes();
    exact.resize(TRANSFER_LIMIT, b' ');
    let imported = store
        .import_object_catalog_backup("analytics", exact)
        .await
        .expect("a valid artifact at the exact encoded-body limit is accepted");
    assert_eq!(imported.status, TableCatalogBackupStatus::Created);
    let before = maintenance_test_object_versions(&backend).await;
    let mut oversized = fixture_bytes();
    oversized.resize(TRANSFER_LIMIT + 1, b' ');
    let result = store.import_object_catalog_backup("analytics", oversized).await;
    assert!(matches!(result, Err(TableCatalogStoreError::Invalid(_))));
    assert_eq!(maintenance_test_object_versions(&backend).await, before);
}
