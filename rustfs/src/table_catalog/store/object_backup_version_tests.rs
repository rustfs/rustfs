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
use crate::table_catalog::test_support::TestCatalogObjectBackend;

async fn inventory_backup_fixture() -> ObjectCatalogBackup {
    let mut backup: ObjectCatalogBackup =
        serde_json::from_slice(include_bytes!("../tests/fixtures/object-catalog-backup-v1.json"))
            .expect("captured historical v1 artifact");
    let backend = TestCatalogObjectBackend::default();
    for object in &backup.objects {
        backend.seed_object(&object.bucket, &object.object, object.data.clone()).await;
    }
    let store = ObjectTableCatalogStore::new(backend);
    backup.references = Some(
        store
            .capture_object_catalog_backup_references(&backup.table_bucket, &backup.objects)
            .await
            .expect("valid source reference graph"),
    );
    backup.version = OBJECT_CATALOG_BACKUP_INVENTORY_VERSION;
    reseal_inventory_backup(&mut backup);
    backup
}

fn reseal_inventory_backup(backup: &mut ObjectCatalogBackup) {
    backup.backup_id = catalog_backup_inventory_identity(
        &backup.table_bucket,
        &backup.source_catalog_etag,
        &backup.objects,
        backup.references.as_deref().expect("v2 inventory"),
    )
    .expect("inventory identity");
}

fn fixture_table(backup: &ObjectCatalogBackup) -> TableEntry {
    serde_json::from_slice(
        &backup
            .objects
            .iter()
            .find(|object| object.object.ends_with(TABLE_ENTRY_FILE))
            .expect("fixture table entry")
            .data,
    )
    .expect("fixture table identity")
}

#[tokio::test]
async fn version_two_raw_transfer_and_inline_metadata_restore_preserve_the_iceberg_identity() {
    let backup = inventory_backup_fixture().await;
    let entry = fixture_table(&backup);
    let bytes = serde_json::to_vec_pretty(&backup).expect("versioned artifact bytes");
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let imported = store
        .import_object_catalog_backup(&backup.table_bucket, bytes.clone())
        .await
        .expect("import candidate");
    assert_eq!(imported.source_snapshot_version, 2);
    assert_eq!(imported.verified_object_count, backup.references.as_ref().expect("references").len());
    assert_eq!(
        store
            .export_object_catalog_backup(&backup.table_bucket, &backup.backup_id)
            .await
            .expect("raw export"),
        bytes
    );
    assert!(
        store
            .get_table_bucket(&backup.table_bucket)
            .await
            .expect("no publication during transfer")
            .is_none()
    );
    let metadata_key =
        table_catalog_object_key_from_location(&entry.table_bucket, &entry.metadata_location).expect("metadata key");
    assert!(
        !backend
            .object_exists(&entry.table_bucket, &metadata_key)
            .await
            .expect("metadata initially absent")
    );
    store
        .restore_object_catalog_backup_with_id(&backup.table_bucket, &backup.backup_id, None, false, Some("inventory-v2"))
        .await
        .expect("restore can publish its validated inline metadata without a separate prepare request");
    assert_eq!(
        store
            .load_table(&entry.table_bucket, &entry.namespace, &entry.table)
            .await
            .expect("load restored table"),
        Some(entry)
    );
    let payload = backup
        .objects
        .iter()
        .find(|object| object.bucket == backup.table_bucket && object.object == metadata_key)
        .expect("inline metadata payload");
    assert_eq!(
        backend
            .read_object(&payload.bucket, &payload.object)
            .await
            .expect("read immutable restored metadata")
            .expect("restored metadata")
            .data,
        payload.data
    );
}

#[tokio::test]
async fn backup_versions_require_their_exact_inventory_shape_before_any_backend_access() {
    let backup = inventory_backup_fixture().await;
    for mode in [
        "v1-with-inventory",
        "v2-without-inventory",
        "null-inventory",
        "unknown-version",
    ] {
        let mut value = serde_json::to_value(&backup).expect("artifact JSON");
        match mode {
            "v1-with-inventory" => value["version"] = serde_json::json!(1),
            "v2-without-inventory" => {
                value.as_object_mut().expect("object").remove("references");
            }
            "null-inventory" => value["references"] = serde_json::Value::Null,
            "unknown-version" => value["version"] = serde_json::json!(99),
            _ => unreachable!("fixed test case"),
        }
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        let error = store
            .import_object_catalog_backup(&backup.table_bucket, serde_json::to_vec(&value).expect("test body"))
            .await
            .expect_err("unsupported shape cannot be persisted");
        assert!(matches!(error, TableCatalogStoreError::Invalid(_)), "{mode}: {error:?}");
        let state = backend.state.lock().await;
        assert!(state.objects.is_empty(), "{mode}");
        assert!(state.put_attempts.is_empty(), "{mode}");
        assert!(state.delete_attempts.is_empty(), "{mode}");
    }
}

#[tokio::test]
async fn a_self_consistent_candidate_with_an_extra_reference_cannot_publish_a_catalog() {
    let mut backup = inventory_backup_fixture().await;
    let entry = fixture_table(&backup);
    let references = backup.references.as_mut().expect("inventory");
    references.push(ObjectCatalogBackupReference {
        bucket: backup.table_bucket.clone(),
        object: format!("{}unreachable.parquet", table_warehouse_object_prefix(&entry).expect("warehouse prefix")),
        kind: ObjectCatalogBackupReferenceKind::DataFile,
        size_bytes: 1,
        etag: Some("source-version".to_string()),
        sha256: Some(backup_sha256(&[1])),
    });
    references.sort_by(|left, right| left.bucket.cmp(&right.bucket).then_with(|| left.object.cmp(&right.object)));
    reseal_inventory_backup(&mut backup);
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    store
        .import_object_catalog_backup(&backup.table_bucket, serde_json::to_vec(&backup).expect("candidate"))
        .await
        .expect("raw transfer may preserve a candidate before its external graph is prepared");
    let (before_objects, before_puts, before_deletes) = {
        let state = backend.state.lock().await;
        (state.objects.clone(), state.put_attempts.clone(), state.delete_attempts.clone())
    };
    let error = store
        .restore_object_catalog_backup_with_id(&backup.table_bucket, &backup.backup_id, None, false, Some("extra-reference"))
        .await
        .expect_err("a forged graph must not be published");
    assert!(matches!(error, TableCatalogStoreError::Conflict(message) if message.contains("reference graph")));
    let state = backend.state.lock().await;
    assert_eq!(state.objects.len(), 1, "only the previously imported artifact may exist");
    assert_eq!(state.objects, before_objects, "rejected restore changed persisted objects");
    assert_eq!(state.put_attempts, before_puts, "rejected restore attempted a write");
    assert_eq!(state.delete_attempts, before_deletes, "rejected restore attempted a deletion");
}

#[tokio::test]
async fn declared_reference_byte_overflow_is_rejected_before_saving_the_artifact() {
    let mut backup = inventory_backup_fixture().await;
    let entry = fixture_table(&backup);
    let references = backup.references.as_mut().expect("inventory");
    references.push(ObjectCatalogBackupReference {
        bucket: backup.table_bucket.clone(),
        object: format!("{}oversized.parquet", table_warehouse_object_prefix(&entry).expect("warehouse prefix")),
        kind: ObjectCatalogBackupReferenceKind::DataFile,
        size_bytes: u64::MAX,
        etag: Some("large-source-version".to_string()),
        sha256: None,
    });
    references.sort_by(|left, right| left.bucket.cmp(&right.bucket).then_with(|| left.object.cmp(&right.object)));
    reseal_inventory_backup(&mut backup);
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let error = store
        .import_object_catalog_backup(&backup.table_bucket, serde_json::to_vec(&backup).expect("overflow body"))
        .await
        .expect_err("overflow cannot leave an artifact stored before reporting failure");
    assert!(matches!(error, TableCatalogStoreError::Invalid(message) if message.contains("byte total overflows")));
    let state = backend.state.lock().await;
    assert!(state.objects.is_empty());
    assert!(state.put_attempts.is_empty());
}
