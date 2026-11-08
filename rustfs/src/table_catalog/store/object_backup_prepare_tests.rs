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

const BUCKET: &str = "analytics";

fn original_fixture() -> ObjectCatalogBackup {
    serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/table_catalog/tests/fixtures/object-catalog-backup-v1.json"
    )))
    .unwrap()
}

fn preparation_reference(object: &str, kind: ObjectCatalogBackupReferenceKind, data: &[u8]) -> ObjectCatalogBackupReference {
    ObjectCatalogBackupReference {
        bucket: BUCKET.to_string(),
        object: object.to_string(),
        kind,
        size_bytes: u64::try_from(data.len()).unwrap(),
        etag: Some("original-source-etag".to_string()),
        sha256: Some(backup_sha256(data)),
    }
}

fn reseal_preparation_fixture(backup: &mut ObjectCatalogBackup) {
    backup.version = 2;
    backup
        .objects
        .sort_by(|left, right| (&left.bucket, &left.object).cmp(&(&right.bucket, &right.object)));
    backup.source_catalog_etag = catalog_backup_fingerprint(&backup.objects).unwrap();
    let references = backup.references.as_mut().unwrap();
    references.sort_by(|left, right| (&left.bucket, &left.object).cmp(&(&right.bucket, &right.object)));
    backup.backup_id =
        catalog_backup_inventory_identity(&backup.table_bucket, &backup.source_catalog_etag, &backup.objects, references)
            .unwrap();
}

fn preparation_fixture() -> ObjectCatalogBackup {
    let mut backup = original_fixture();
    backup.references = Some(
        backup
            .objects
            .iter()
            .filter(|object| object.bucket == BUCKET)
            .map(|object| preparation_reference(&object.object, ObjectCatalogBackupReferenceKind::TableMetadata, &object.data))
            .collect(),
    );
    reseal_preparation_fixture(&mut backup);
    backup
}

fn current_payload(backup: &ObjectCatalogBackup) -> &ObjectCatalogBackupObject {
    backup.objects.iter().find(|object| object.bucket == BUCKET).unwrap()
}

async fn save_preparation_fixture(backend: &TestCatalogObjectBackend, backup: &ObjectCatalogBackup) {
    backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &TableCatalogObjectPaths::default().catalog_backup_path(BUCKET, &backup.backup_id),
            serde_json::to_vec(backup).unwrap(),
        )
        .await;
}

async fn object_versions(backend: &TestCatalogObjectBackend) -> BTreeMap<(String, String), (Vec<u8>, String)> {
    backend
        .state
        .lock()
        .await
        .objects
        .iter()
        .map(|(key, record)| (key.clone(), (record.data.clone(), record.etag.clone())))
        .collect()
}

async fn system_versions(backend: &TestCatalogObjectBackend) -> BTreeMap<(String, String), (Vec<u8>, String)> {
    object_versions(backend)
        .await
        .into_iter()
        .filter(|((bucket, _), _)| bucket == RUSTFS_META_BUCKET)
        .collect()
}

async fn pending_preparation_controls(
    backend: &TestCatalogObjectBackend,
    backup: &ObjectCatalogBackup,
    barrier_phase: ObjectCatalogRestoreStateKind,
    intent_phase: ObjectCatalogRestoreStateKind,
) -> (String, String) {
    let paths = TableCatalogObjectPaths::default();
    let restore_id = "prepare-recovery";
    let epoch = Uuid::new_v4().to_string();
    let fingerprint = restore_request_fingerprint(BUCKET, restore_id, &backup.backup_id, None, false).unwrap();
    let intent = ObjectCatalogRestoreIntent {
        version: OBJECT_CATALOG_RESTORE_STATE_VERSION,
        table_bucket: BUCKET.to_string(),
        restore_epoch: epoch.clone(),
        restore_id: restore_id.to_string(),
        backup_id: backup.backup_id.clone(),
        request_fingerprint: fingerprint.clone(),
        expected_catalog_etag: None,
        allow_replace: false,
        source_catalog_etag: backup.source_catalog_etag.clone(),
        state: intent_phase,
        previous_snapshot_etag: None,
        restored_snapshot_etag: backup.source_catalog_etag.clone(),
        object_count: backup.objects.len(),
        maintenance_object_count: 0,
        writes: backup
            .objects
            .iter()
            .map(|object| ObjectCatalogRestoreWrite {
                bucket: object.bucket.clone(),
                object: object.object.clone(),
                expected_etag: None,
                already_present: false,
            })
            .collect(),
        deletes: Vec::new(),
        created_at: "2026-10-31T00:00:00Z".to_string(),
        updated_at: "2026-10-31T00:00:00Z".to_string(),
    };
    let state = ObjectCatalogRestoreState {
        version: OBJECT_CATALOG_RESTORE_STATE_VERSION,
        table_bucket: BUCKET.to_string(),
        restore_epoch: epoch,
        restore_id: restore_id.to_string(),
        backup_id: backup.backup_id.clone(),
        request_fingerprint: fingerprint,
        state: barrier_phase,
        completed_intent_etag: None,
        backup_etag: None,
    };
    let intent_path = paths.catalog_restore_intent_path(BUCKET, restore_id);
    let state_path = paths.catalog_restore_state_path(BUCKET);
    backend
        .seed_object(RUSTFS_META_BUCKET, &intent_path, serde_json::to_vec(&intent).unwrap())
        .await;
    backend
        .seed_object(RUSTFS_META_BUCKET, &state_path, serde_json::to_vec(&state).unwrap())
        .await;
    (state_path, intent_path)
}

#[tokio::test]
async fn preparation_only_creates_the_referenced_payload_and_replays_without_rewriting_it() {
    let backend = TestCatalogObjectBackend::default();
    let backup = preparation_fixture();
    save_preparation_fixture(&backend, &backup).await;
    let before = object_versions(&backend).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let payload = current_payload(&backup);
    let prepared = store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
        .await
        .unwrap();
    assert!(matches!(prepared.status, TableCatalogBackupObjectPreparationStatus::Prepared));
    assert_eq!(prepared.size_bytes, payload.size_bytes);
    assert_eq!(prepared.sha256, payload.sha256);
    assert_ne!(prepared.etag, backup.references.as_ref().unwrap()[0].etag);
    let after = object_versions(&backend).await;
    assert_eq!(after.len(), before.len() + 1);
    assert_eq!(system_versions(&backend).await, before);
    let retried = store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
        .await
        .unwrap();
    assert!(matches!(retried.status, TableCatalogBackupObjectPreparationStatus::AlreadyPresent));
    assert_eq!(retried.etag, prepared.etag);
    assert_eq!(object_versions(&backend).await, after);
    assert_eq!(
        backend
            .state
            .lock()
            .await
            .put_attempts
            .get(&(BUCKET.to_string(), payload.object.clone())),
        Some(&1)
    );
}

#[tokio::test]
async fn preparation_accepts_identical_existing_bytes_with_a_different_target_etag() {
    let backend = TestCatalogObjectBackend::default();
    let backup = preparation_fixture();
    let payload = current_payload(&backup);
    save_preparation_fixture(&backend, &backup).await;
    backend.seed_object(BUCKET, &payload.object, payload.data.clone()).await;
    let before = object_versions(&backend).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let report = store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
        .await
        .unwrap();
    assert!(matches!(report.status, TableCatalogBackupObjectPreparationStatus::AlreadyPresent));
    assert_ne!(report.etag, backup.references.as_ref().unwrap()[0].etag);
    assert_eq!(object_versions(&backend).await, before);
    assert!(backend.state.lock().await.put_attempts.is_empty());
}

#[tokio::test]
async fn preparation_never_overwrites_unknown_existing_bytes() {
    let backend = TestCatalogObjectBackend::default();
    let backup = preparation_fixture();
    let payload = current_payload(&backup);
    save_preparation_fixture(&backend, &backup).await;
    backend
        .seed_object(BUCKET, &payload.object, b"unknown-existing-metadata".to_vec())
        .await;
    let before = object_versions(&backend).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    assert!(matches!(
        store
            .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
            .await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    assert_eq!(object_versions(&backend).await, before);
    assert!(backend.state.lock().await.put_attempts.is_empty());
}

#[tokio::test]
async fn preparation_rejects_an_existing_object_without_a_stable_etag() {
    let backend = TestCatalogObjectBackend::default();
    let backup = preparation_fixture();
    let payload = current_payload(&backup);
    save_preparation_fixture(&backend, &backup).await;
    backend.seed_object(BUCKET, &payload.object, payload.data.clone()).await;
    backend.omit_etag_for_object(BUCKET, &payload.object).await;
    let before = object_versions(&backend).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    assert!(matches!(
        store
            .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
            .await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    assert_eq!(object_versions(&backend).await, before);
    assert!(backend.state.lock().await.put_attempts.is_empty());
}

#[tokio::test]
async fn preparation_rejects_an_existing_object_that_changes_during_comparison() {
    let backend = TestCatalogObjectBackend::default();
    let backup = preparation_fixture();
    let payload = current_payload(&backup);
    save_preparation_fixture(&backend, &backup).await;
    backend.seed_object(BUCKET, &payload.object, payload.data.clone()).await;
    let before = system_versions(&backend).await;
    let pause = backend.pause_next_read(BUCKET, &payload.object).await;
    let task_backend = backend.clone();
    let backup_id = backup.backup_id.clone();
    let object = payload.object.clone();
    let bytes = payload.data.clone();
    let task = tokio::spawn(async move {
        ObjectTableCatalogStore::new(task_backend)
            .prepare_object_catalog_backup_object(BUCKET, &backup_id, &object, bytes)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.wait_started())
        .await
        .unwrap();
    backend.seed_object(BUCKET, &payload.object, payload.data.clone()).await;
    pause.release();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(TableCatalogStoreError::Conflict(_))));
    assert_eq!(system_versions(&backend).await, before);
    assert!(backend.state.lock().await.put_attempts.is_empty());
}

#[tokio::test]
async fn preparation_confirms_an_ambiguous_put_without_repeating_the_write() {
    let backend = TestCatalogObjectBackend::default();
    let backup = preparation_fixture();
    let payload = current_payload(&backup);
    save_preparation_fixture(&backend, &backup).await;
    let before = system_versions(&backend).await;
    backend.fail_after_next_put(BUCKET, &payload.object).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let report = store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
        .await
        .unwrap();
    assert!(matches!(report.status, TableCatalogBackupObjectPreparationStatus::AlreadyPresent));
    let first = object_versions(&backend).await;
    let retry = store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
        .await
        .unwrap();
    assert_eq!(retry.etag, report.etag);
    assert_eq!(system_versions(&backend).await, before);
    assert_eq!(object_versions(&backend).await, first);
    assert_eq!(
        backend
            .state
            .lock()
            .await
            .put_attempts
            .get(&(BUCKET.to_string(), payload.object.clone())),
        Some(&1)
    );
}

#[tokio::test]
async fn preparation_failure_before_put_has_no_effects_and_can_be_retried() {
    let backend = TestCatalogObjectBackend::default();
    let backup = preparation_fixture();
    let payload = current_payload(&backup);
    save_preparation_fixture(&backend, &backup).await;
    let before = object_versions(&backend).await;
    backend.fail_next_put(BUCKET, &payload.object).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    assert!(
        store
            .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
            .await
            .is_err()
    );
    assert_eq!(object_versions(&backend).await, before);
    let retry = store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
        .await
        .unwrap();
    assert!(matches!(retry.status, TableCatalogBackupObjectPreparationStatus::Prepared));
}

#[tokio::test]
async fn preparation_rejects_bad_bodies_unlisted_keys_and_system_bucket_without_writes() {
    let backup = preparation_fixture();
    let payload = current_payload(&backup);
    let mut corrupt = payload.data.clone();
    corrupt[0] ^= 1;
    for (bucket, object, data) in [
        (BUCKET, payload.object.as_str(), corrupt),
        (BUCKET, payload.object.as_str(), vec![0; OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE + 1]),
        (BUCKET, "tables/table-id/data/unlisted.parquet", vec![1]),
        (BUCKET, "../outside", vec![1]),
        (RUSTFS_META_BUCKET, payload.object.as_str(), payload.data.clone()),
    ] {
        let backend = TestCatalogObjectBackend::default();
        save_preparation_fixture(&backend, &backup).await;
        let before = object_versions(&backend).await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        assert!(
            store
                .prepare_object_catalog_backup_object(bucket, &backup.backup_id, object, data)
                .await
                .is_err()
        );
        assert_eq!(object_versions(&backend).await, before);
        assert!(backend.state.lock().await.put_attempts.is_empty());
    }
}

#[tokio::test]
async fn inventory_membership_does_not_authorize_an_unowned_or_wrong_kind_key() {
    for (object, kind) in [
        ("unowned/data/object.parquet", ObjectCatalogBackupReferenceKind::DataFile),
        ("tables/table-id/data/wrong-role.parquet", ObjectCatalogBackupReferenceKind::ManifestFile),
    ] {
        let backend = TestCatalogObjectBackend::default();
        let mut backup = preparation_fixture();
        backup
            .references
            .as_mut()
            .unwrap()
            .push(preparation_reference(object, kind, b"x"));
        reseal_preparation_fixture(&mut backup);
        save_preparation_fixture(&backend, &backup).await;
        let before = object_versions(&backend).await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        assert!(
            store
                .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, object, b"x".to_vec())
                .await
                .is_err()
        );
        assert_eq!(object_versions(&backend).await, before);
        assert!(backend.state.lock().await.put_attempts.is_empty());
    }
}

#[tokio::test]
async fn same_backup_preparation_preserves_a_pending_restore_barrier_and_plan() {
    let backend = TestCatalogObjectBackend::default();
    let backup = preparation_fixture();
    save_preparation_fixture(&backend, &backup).await;
    pending_preparation_controls(
        &backend,
        &backup,
        ObjectCatalogRestoreStateKind::Applying,
        ObjectCatalogRestoreStateKind::Prepared,
    )
    .await;
    let before = system_versions(&backend).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let payload = current_payload(&backup);
    store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
        .await
        .unwrap();
    assert_eq!(system_versions(&backend).await, before);
    assert!(matches!(
        store.ensure_no_active_restore(BUCKET).await,
        Err(TableCatalogStoreError::Unavailable(_))
    ));
    store
        .restore_object_catalog_backup_with_id(BUCKET, &backup.backup_id, None, false, Some("prepare-recovery"))
        .await
        .unwrap();
    store.ensure_no_active_restore(BUCKET).await.unwrap();
}

#[tokio::test]
async fn preparation_after_completed_restore_repairs_only_the_missing_payload() {
    let backend = TestCatalogObjectBackend::default();
    let backup = preparation_fixture();
    save_preparation_fixture(&backend, &backup).await;
    let payload = current_payload(&backup);
    let store = ObjectTableCatalogStore::new(backend.clone());
    store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
        .await
        .unwrap();
    store
        .restore_object_catalog_backup_with_id(BUCKET, &backup.backup_id, None, false, Some("completed-prepare"))
        .await
        .unwrap();
    store.ensure_no_active_restore(BUCKET).await.unwrap();
    let before = system_versions(&backend).await;
    backend
        .state
        .lock()
        .await
        .objects
        .remove(&(BUCKET.to_string(), payload.object.clone()));
    let report = store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
        .await
        .unwrap();
    assert!(matches!(report.status, TableCatalogBackupObjectPreparationStatus::Prepared));
    assert_eq!(system_versions(&backend).await, before);
    store.ensure_no_active_restore(BUCKET).await.unwrap();
}

#[tokio::test]
async fn view_metadata_preparation_requires_the_active_view_identity() {
    for valid_identity in [true, false] {
        let backend = TestCatalogObjectBackend::default();
        let mut backup = preparation_fixture();
        let namespace = Namespace::parse("sales").unwrap();
        let view = IdentifierSegment::parse("summary").unwrap();
        let path = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
        let entry = ViewEntry {
            version: TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: BUCKET.to_string(),
            namespace: namespace.public_name(),
            view: view.as_str().to_string(),
            view_id: "view-id".to_string(),
            view_uuid: "view-uuid".to_string(),
            format: "ICEBERG_VIEW".to_string(),
            format_version: 1,
            warehouse_location: "s3://analytics/views/view-id".to_string(),
            metadata_location: path.clone(),
            version_token: "view-token".to_string(),
            generation: 1,
            state: TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        };
        let entry_data = serde_json::to_vec(&entry).unwrap();
        backup.objects.push(ObjectCatalogBackupObject {
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: TableCatalogObjectPaths::default().view_entry_path(BUCKET, &namespace, &view),
            size_bytes: u64::try_from(entry_data.len()).unwrap(),
            etag: Some("source-view-entry-etag".to_string()),
            sha256: backup_sha256(&entry_data),
            data: entry_data,
        });
        let metadata = serde_json::json!({
            "format-version": 1, "view-uuid": if valid_identity { "view-uuid" } else { "foreign-view" },
            "location": "s3://analytics/views/view-id", "current-schema-id": 0, "current-version-id": 1,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
            "versions": [{"version-id": 1, "timestamp-ms": 1, "schema-id": 0, "summary": {},
                "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}], "default-namespace": ["sales"]}],
            "version-log": [{"timestamp-ms": 1, "version-id": 1}], "properties": {}
        });
        let data = serde_json::to_vec(&metadata).unwrap();
        backup.references.as_mut().unwrap().push(preparation_reference(
            &path,
            ObjectCatalogBackupReferenceKind::ViewMetadata,
            &data,
        ));
        reseal_preparation_fixture(&mut backup);
        save_preparation_fixture(&backend, &backup).await;
        let before = object_versions(&backend).await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        let result = store
            .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &path, data)
            .await;
        if valid_identity {
            assert!(result.is_ok());
            assert_eq!(system_versions(&backend).await, before);
        } else {
            assert!(matches!(result, Err(TableCatalogStoreError::Invalid(_))));
            assert_eq!(object_versions(&backend).await, before);
        }
    }
}

#[tokio::test]
async fn pending_wrong_backup_epoch_request_or_plan_cannot_prepare_an_object() {
    for case in 0..4 {
        let backend = TestCatalogObjectBackend::default();
        let backup = preparation_fixture();
        save_preparation_fixture(&backend, &backup).await;
        let (state_path, intent_path) = pending_preparation_controls(
            &backend,
            &backup,
            ObjectCatalogRestoreStateKind::Applying,
            ObjectCatalogRestoreStateKind::Prepared,
        )
        .await;
        let path = if case == 3 { &intent_path } else { &state_path };
        let bytes = backend.read_object(RUSTFS_META_BUCKET, path).await.unwrap().unwrap().data;
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        match case {
            0 => value["backup_id"] = serde_json::json!("another-backup"),
            1 => value["restore_epoch"] = serde_json::json!(Uuid::new_v4().to_string()),
            2 => value["request_fingerprint"] = serde_json::json!("0".repeat(64)),
            _ => {
                value["writes"].as_array_mut().unwrap().pop();
            }
        }
        backend
            .seed_object(RUSTFS_META_BUCKET, path, serde_json::to_vec(&value).unwrap())
            .await;
        let before = object_versions(&backend).await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        let payload = current_payload(&backup);
        assert!(
            store
                .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
                .await
                .is_err()
        );
        assert_eq!(object_versions(&backend).await, before);
        assert!(backend.state.lock().await.put_attempts.is_empty());
    }
}

#[tokio::test]
async fn preparation_requires_prepared_history_before_using_its_different_warehouse_scope() {
    let backend = TestCatalogObjectBackend::default();
    let mut backup = preparation_fixture();
    let current_index = backup.objects.iter().position(|object| object.bucket == BUCKET).unwrap();
    let current = backup.objects[current_index].object.clone();
    let history = current.replace("00001.metadata.json", "00000.metadata.json");
    let mut metadata: serde_json::Value = serde_json::from_slice(&backup.objects[current_index].data).unwrap();
    let mut historical = metadata.clone();
    historical["location"] = serde_json::json!("s3://analytics/archive/table-id");
    let historical = serde_json::to_vec(&historical).unwrap();
    metadata["metadata-log"] = serde_json::json!([{"timestamp-ms": 0, "metadata-file": format!("s3://{BUCKET}/{history}")}]);
    let current_data = serde_json::to_vec(&metadata).unwrap();
    backup.objects[current_index].data = current_data.clone();
    backup.objects[current_index].size_bytes = u64::try_from(current_data.len()).unwrap();
    backup.objects[current_index].sha256 = backup_sha256(&current_data);
    let old_data = "archive/table-id/data/part.parquet";
    backup.references = Some(vec![
        preparation_reference(&current, ObjectCatalogBackupReferenceKind::TableMetadata, &current_data),
        preparation_reference(&history, ObjectCatalogBackupReferenceKind::TableMetadata, &historical),
        preparation_reference(old_data, ObjectCatalogBackupReferenceKind::DataFile, b"x"),
    ]);
    reseal_preparation_fixture(&mut backup);
    save_preparation_fixture(&backend, &backup).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    let before = object_versions(&backend).await;
    assert!(matches!(
        store
            .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, old_data, b"x".to_vec())
            .await,
        Err(TableCatalogStoreError::Unavailable(_))
    ));
    assert_eq!(object_versions(&backend).await, before);
    store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &history, historical)
        .await
        .unwrap();
    let controls = system_versions(&backend).await;
    store
        .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, old_data, b"x".to_vec())
        .await
        .unwrap();
    assert_eq!(system_versions(&backend).await, controls);
}

#[tokio::test]
async fn v1_artifact_does_not_gain_an_implicit_object_preparation_capability() {
    let backend = TestCatalogObjectBackend::default();
    let backup = original_fixture();
    save_preparation_fixture(&backend, &backup).await;
    let before = object_versions(&backend).await;
    let payload = current_payload(&backup);
    let store = ObjectTableCatalogStore::new(backend.clone());
    assert!(matches!(
        store
            .prepare_object_catalog_backup_object(BUCKET, &backup.backup_id, &payload.object, payload.data.clone())
            .await,
        Err(TableCatalogStoreError::Unsupported(_))
    ));
    assert_eq!(object_versions(&backend).await, before);
}
