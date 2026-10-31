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
    TestCatalogObjectBackend, manifest_avro_bytes, manifest_list_avro_entries_with_content, table_metadata_json,
};

const BUCKET: &str = "analytics";
const DATA: &str = "tables/table-id/data/part.parquet";
const DATA_MANIFEST: &str = "tables/table-id/metadata/data.avro";
const MANIFEST_LIST: &str = "tables/table-id/metadata/snap-42.avro";
const TABLE_STATISTICS: &str = "tables/table-id/metadata/statistics.puffin";
const PARTITION_STATISTICS: &str = "tables/table-id/metadata/partition-statistics.orc";
const HISTORY_MANIFEST_LIST: &str = "tables/table-id/metadata/snap-20.avro";
const HISTORY_MANIFEST: &str = "tables/table-id/metadata/history.avro";
const HISTORY_DATA: &str = "tables/table-id/data/history.parquet";
const HISTORY_STATISTICS: &str = "tables/table-id/metadata/history-statistics.puffin";

struct InventoryFixture {
    backend: TestCatalogObjectBackend,
    objects: Vec<ObjectCatalogBackupObject>,
    current: String,
    history: String,
    unretained_history: String,
    view: String,
}

async fn embedded_object(backend: &TestCatalogObjectBackend, bucket: &str, object: &str) -> ObjectCatalogBackupObject {
    let stored = backend.read_object(bucket, object).await.unwrap().unwrap();
    ObjectCatalogBackupObject {
        bucket: bucket.to_string(),
        object: object.to_string(),
        size_bytes: u64::try_from(stored.data.len()).unwrap(),
        etag: stored.etag,
        sha256: backup_sha256(&stored.data),
        data: stored.data,
    }
}

async fn inventory_fixture() -> InventoryFixture {
    let backend = TestCatalogObjectBackend::default();
    let paths = TableCatalogObjectPaths::default();
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let view = IdentifierSegment::parse("summary").unwrap();
    let current = default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let history = default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let unretained_history = default_table_metadata_file_path(&namespace, &table, "00000.metadata.json");
    let view_metadata = default_view_metadata_file_path(&namespace, &view, "00001.metadata.json");
    let entry = TableEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: BUCKET.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: "table-id".to_string(),
        table_uuid: "table-uuid".to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: format!("s3://{BUCKET}/tables/table-id"),
        metadata_location: current.clone(),
        version_token: "token-v1".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    let view_entry = ViewEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: BUCKET.to_string(),
        namespace: namespace.public_name(),
        view: view.as_str().to_string(),
        view_id: "view-id".to_string(),
        view_uuid: "view-uuid".to_string(),
        format: "ICEBERG_VIEW".to_string(),
        format_version: 1,
        warehouse_location: format!("s3://{BUCKET}/views/view-id"),
        metadata_location: view_metadata.clone(),
        version_token: "view-token-v1".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    let mut current_metadata = table_metadata_json(&entry.table_uuid, &entry.warehouse_location);
    current_metadata["last-sequence-number"] = serde_json::json!(7);
    current_metadata["last-updated-ms"] = serde_json::json!(2);
    current_metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 42,
        "sequence-number": 7,
        "timestamp-ms": 2,
        "manifest-list": format!("s3://{BUCKET}/{MANIFEST_LIST}"),
        "summary": {"operation": "append"}
    }]);
    current_metadata["current-snapshot-id"] = serde_json::json!(42);
    current_metadata["snapshot-log"] = serde_json::json!([{"timestamp-ms": 2, "snapshot-id": 42}]);
    current_metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 42}});
    current_metadata["metadata-log"] = serde_json::json!([{
        "timestamp-ms": 1,
        "metadata-file": format!("s3://{BUCKET}/{history}")
    }]);
    current_metadata["statistics"] = serde_json::json!([{
        "snapshot-id": 42,
        "statistics-path": format!("s3://{BUCKET}/{TABLE_STATISTICS}"),
        "file-size-in-bytes": 8,
        "file-footer-size-in-bytes": 4,
        "blob-metadata": []
    }]);
    current_metadata["partition-statistics"] = serde_json::json!([{
        "snapshot-id": 42,
        "statistics-path": format!("s3://{BUCKET}/{PARTITION_STATISTICS}"),
        "file-size-in-bytes": 3
    }]);
    let mut historical_metadata = table_metadata_json(&entry.table_uuid, &entry.warehouse_location);
    historical_metadata["last-sequence-number"] = serde_json::json!(6);
    historical_metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 20, "sequence-number": 6, "timestamp-ms": 1,
        "manifest-list": format!("s3://{BUCKET}/{HISTORY_MANIFEST_LIST}"),
        "summary": {"operation": "append"}
    }]);
    historical_metadata["current-snapshot-id"] = serde_json::json!(20);
    historical_metadata["snapshot-log"] = serde_json::json!([{"timestamp-ms": 1, "snapshot-id": 20}]);
    historical_metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 20}});
    historical_metadata["statistics"] = serde_json::json!([{
        "snapshot-id": 20,
        "statistics-path": format!("s3://{BUCKET}/{HISTORY_STATISTICS}"),
        "file-size-in-bytes": 8, "file-footer-size-in-bytes": 4, "blob-metadata": []
    }]);
    historical_metadata["metadata-log"] = serde_json::json!([{
        "timestamp-ms": 0,
        "metadata-file": format!("s3://{BUCKET}/{unretained_history}")
    }]);
    let view_metadata_json = serde_json::json!({
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
    let mut manifests = Vec::new();
    for (manifest, object, content) in [
        (DATA_MANIFEST, DATA, 0),
        ("tables/table-id/metadata/positions.avro", "tables/table-id/data/positions.parquet", 1),
        ("tables/table-id/metadata/equality.avro", "tables/table-id/data/equality.parquet", 2),
    ] {
        let location = format!("s3://{BUCKET}/{object}");
        let bytes = manifest_avro_bytes(&[(&location, content, 1, 42, 7)]);
        manifests.push((format!("s3://{BUCKET}/{manifest}"), bytes.len(), if content == 0 { 0 } else { 1 }));
        backend.seed_object(BUCKET, manifest, bytes).await;
        backend.seed_object(BUCKET, object, vec![1]).await;
    }
    let manifest_entries = manifests
        .iter()
        .map(|(path, size, content)| (path.as_str(), *size, 0, *content, 7, 42))
        .collect::<Vec<_>>();
    backend
        .seed_object(BUCKET, MANIFEST_LIST, manifest_list_avro_entries_with_content(&manifest_entries))
        .await;
    let history_data_location = format!("s3://{BUCKET}/{HISTORY_DATA}");
    let history_manifest_bytes = manifest_avro_bytes(&[(&history_data_location, 0, 1, 20, 6)]);
    let history_manifest_location = format!("s3://{BUCKET}/{HISTORY_MANIFEST}");
    backend
        .seed_object(
            BUCKET,
            HISTORY_MANIFEST_LIST,
            manifest_list_avro_entries_with_content(&[(&history_manifest_location, history_manifest_bytes.len(), 0, 0, 6, 20)]),
        )
        .await;
    backend.seed_object(BUCKET, HISTORY_MANIFEST, history_manifest_bytes).await;
    backend.seed_object(BUCKET, HISTORY_DATA, vec![1]).await;
    backend.seed_object(BUCKET, HISTORY_STATISTICS, b"PFA1PFA1".to_vec()).await;
    backend.seed_object(BUCKET, TABLE_STATISTICS, b"PFA1PFA1".to_vec()).await;
    backend.seed_object(BUCKET, PARTITION_STATISTICS, b"ORC".to_vec()).await;
    backend
        .seed_object(BUCKET, &current, serde_json::to_vec(&current_metadata).unwrap())
        .await;
    backend
        .seed_object(BUCKET, &history, serde_json::to_vec(&historical_metadata).unwrap())
        .await;
    backend
        .seed_object(BUCKET, &view_metadata, serde_json::to_vec(&view_metadata_json).unwrap())
        .await;
    let table_path = paths.table_entry_path(BUCKET, &namespace, &table);
    let view_path = paths.view_entry_path(BUCKET, &namespace, &view);
    backend
        .seed_object(RUSTFS_META_BUCKET, &table_path, serde_json::to_vec(&entry).unwrap())
        .await;
    backend
        .seed_object(RUSTFS_META_BUCKET, &view_path, serde_json::to_vec(&view_entry).unwrap())
        .await;
    let mut objects = vec![
        embedded_object(&backend, RUSTFS_META_BUCKET, &table_path).await,
        embedded_object(&backend, RUSTFS_META_BUCKET, &view_path).await,
        embedded_object(&backend, BUCKET, &current).await,
    ];
    objects.sort_by(|left, right| (&left.bucket, &left.object).cmp(&(&right.bucket, &right.object)));
    InventoryFixture {
        backend,
        objects,
        current,
        history,
        unretained_history,
        view: view_metadata,
    }
}

async fn copied_backend_with_new_etags(source: &TestCatalogObjectBackend) -> TestCatalogObjectBackend {
    let objects = source
        .state
        .lock()
        .await
        .objects
        .iter()
        .map(|(key, value)| (key.clone(), value.data.clone()))
        .collect::<Vec<_>>();
    let target = TestCatalogObjectBackend::default();
    for _ in 0..=objects.len() {
        target.seed_object("unrelated", "etag-offset", vec![0]).await;
    }
    for ((bucket, object), bytes) in objects {
        target.seed_object(&bucket, &object, bytes).await;
    }
    target
}

#[tokio::test]
async fn inventory_captures_all_reference_kinds_and_only_direct_metadata_history() {
    let fixture = inventory_fixture().await;
    let store = ObjectTableCatalogStore::new(fixture.backend.clone());
    let references = store
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .unwrap();
    validate_object_catalog_backup_reference_shape(BUCKET, &references).unwrap();
    for kind in [
        ObjectCatalogBackupReferenceKind::TableMetadata,
        ObjectCatalogBackupReferenceKind::ViewMetadata,
        ObjectCatalogBackupReferenceKind::ManifestList,
        ObjectCatalogBackupReferenceKind::ManifestFile,
        ObjectCatalogBackupReferenceKind::DataFile,
        ObjectCatalogBackupReferenceKind::DeleteFile,
        ObjectCatalogBackupReferenceKind::TableStatistics,
        ObjectCatalogBackupReferenceKind::PartitionStatistics,
    ] {
        assert!(references.iter().any(|reference| reference.kind == kind), "missing {kind:?}");
    }
    for object in [&fixture.current, &fixture.history, &fixture.view] {
        assert_eq!(references.iter().filter(|reference| &reference.object == object).count(), 1);
    }
    for object in [HISTORY_MANIFEST_LIST, HISTORY_MANIFEST, HISTORY_DATA, HISTORY_STATISTICS] {
        assert_eq!(references.iter().filter(|reference| reference.object == object).count(), 1);
    }
    assert!(
        !references
            .iter()
            .any(|reference| reference.object == fixture.unretained_history)
    );
    assert_eq!(
        references
            .iter()
            .filter(|reference| reference.kind == ObjectCatalogBackupReferenceKind::DeleteFile)
            .count(),
        2
    );
    assert!(references.iter().all(|reference| reference.sha256.is_some()));
}

#[tokio::test]
async fn prepared_small_objects_verify_by_bytes_across_different_etags_without_writes() {
    let fixture = inventory_fixture().await;
    let source = ObjectTableCatalogStore::new(fixture.backend.clone());
    let references = source
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .unwrap();
    let target_backend = copied_backend_with_new_etags(&fixture.backend).await;
    let target = ObjectTableCatalogStore::new(target_backend.clone());
    let observed = target
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .unwrap();
    assert!(
        references
            .iter()
            .zip(&observed)
            .all(|(source, target)| source.etag != target.etag)
    );
    target_backend.reset_call_counts().await;
    target
        .validate_object_catalog_backup_references(BUCKET, &fixture.objects, &references)
        .await
        .unwrap();
    let state = target_backend.state.lock().await;
    assert!(state.put_attempts.is_empty());
    assert!(state.delete_attempts.is_empty());
}

#[tokio::test]
async fn deleted_only_missing_files_are_excluded_but_a_retained_snapshot_still_requires_its_live_file() {
    let mut fixture = inventory_fixture().await;
    let missing_deleted = "tables/table-id/data/expired.parquet";
    let shared_location = format!("s3://{BUCKET}/{DATA}");
    let missing_location = format!("s3://{BUCKET}/{missing_deleted}");
    let history_manifest = manifest_avro_bytes(&[(&shared_location, 0, 0, 20, 6)]);
    let history_manifest_location = format!("s3://{BUCKET}/{HISTORY_MANIFEST}");
    fixture
        .backend
        .seed_object(
            BUCKET,
            HISTORY_MANIFEST_LIST,
            manifest_list_avro_entries_with_content(&[(&history_manifest_location, history_manifest.len(), 0, 0, 6, 20)]),
        )
        .await;
    fixture.backend.seed_object(BUCKET, HISTORY_MANIFEST, history_manifest).await;
    let deleted_manifest = manifest_avro_bytes(&[(&missing_location, 0, 2, 42, 7), (&shared_location, 0, 2, 42, 7)]);
    fixture.backend.seed_object(BUCKET, DATA_MANIFEST, deleted_manifest).await;
    let mut manifest_entries = Vec::new();
    for (object, content) in [
        (DATA_MANIFEST, 0),
        ("tables/table-id/metadata/positions.avro", 1),
        ("tables/table-id/metadata/equality.avro", 1),
    ] {
        let size = fixture.backend.object_metadata(BUCKET, object).await.unwrap().unwrap().size;
        manifest_entries.push((format!("s3://{BUCKET}/{object}"), usize::try_from(size).unwrap(), content));
    }
    let manifest_references = manifest_entries
        .iter()
        .map(|(path, size, content)| (path.as_str(), *size, 0, *content, 7, 42))
        .collect::<Vec<_>>();
    fixture
        .backend
        .seed_object(BUCKET, MANIFEST_LIST, manifest_list_avro_entries_with_content(&manifest_references))
        .await;
    let index = fixture
        .objects
        .iter()
        .position(|object| object.bucket == BUCKET && object.object == fixture.current)
        .unwrap();
    let mut metadata: serde_json::Value = serde_json::from_slice(&fixture.objects[index].data).unwrap();
    let historical = fixture.backend.read_object(BUCKET, &fixture.history).await.unwrap().unwrap();
    let historical: serde_json::Value = serde_json::from_slice(&historical.data).unwrap();
    metadata["snapshots"]
        .as_array_mut()
        .unwrap()
        .insert(0, historical["snapshots"][0].clone());
    metadata["snapshot-log"] = serde_json::json!([
        {"timestamp-ms": 1, "snapshot-id": 20}, {"timestamp-ms": 2, "snapshot-id": 42}
    ]);
    metadata["refs"]["retained"] = serde_json::json!({"type": "branch", "snapshot-id": 20});
    fixture
        .backend
        .seed_object(BUCKET, &fixture.current, serde_json::to_vec(&metadata).unwrap())
        .await;
    fixture.objects[index] = embedded_object(&fixture.backend, BUCKET, &fixture.current).await;
    let entry = fixture
        .objects
        .iter()
        .find(|object| object.bucket == RUSTFS_META_BUCKET && object.object.ends_with(TABLE_ENTRY_FILE))
        .unwrap();
    let entry: TableEntry = serde_json::from_slice(&entry.data).unwrap();
    let context = TableSnapshotGraphValidationContext::new(&fixture.backend, BUCKET, &entry);
    fixture.backend.reset_call_counts().await;
    let reads_before = fixture.backend.state.lock().await.read_attempts.clone();
    let graph = validated_table_snapshot_graph_inventory(&context, &metadata).await.unwrap();
    assert!(!graph.contains_key(missing_deleted));
    assert_eq!(graph.get(DATA), Some(&TableMetadataMaintenanceObjectKind::DataFile));
    assert_eq!(graph.get(DATA_MANIFEST), Some(&TableMetadataMaintenanceObjectKind::ManifestFile));
    let state = fixture.backend.state.lock().await;
    for object in [MANIFEST_LIST, DATA_MANIFEST, HISTORY_MANIFEST_LIST, HISTORY_MANIFEST] {
        let key = (BUCKET.to_string(), object.to_string());
        let expected = reads_before.get(&key).copied().unwrap_or_default() + 1;
        assert_eq!(state.read_attempts.get(&key), Some(&expected), "repeated graph read: {object}");
    }
    drop(state);
    let store = ObjectTableCatalogStore::new(fixture.backend.clone());
    let references = store
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .unwrap();
    assert!(!references.iter().any(|reference| reference.object == missing_deleted));
    assert_eq!(references.iter().filter(|reference| reference.object == DATA).count(), 1);
    fixture
        .backend
        .state
        .lock()
        .await
        .objects
        .remove(&(BUCKET.to_string(), DATA.to_string()));
    assert!(matches!(
        validated_table_snapshot_graph_inventory(&context, &metadata).await,
        Err(TableCatalogStoreError::Invalid(message)) if message == "manifest referenced data file is missing"
    ));
    assert!(
        store
            .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn changed_small_content_is_rejected_even_when_size_and_graph_are_unchanged() {
    let fixture = inventory_fixture().await;
    let source = ObjectTableCatalogStore::new(fixture.backend.clone());
    let references = source
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .unwrap();
    let target_backend = copied_backend_with_new_etags(&fixture.backend).await;
    target_backend.seed_object(BUCKET, DATA, vec![9]).await;
    let target = ObjectTableCatalogStore::new(target_backend.clone());
    target_backend.reset_call_counts().await;
    assert!(matches!(
        target
            .validate_object_catalog_backup_references(BUCKET, &fixture.objects, &references)
            .await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    let state = target_backend.state.lock().await;
    assert!(state.put_attempts.is_empty());
    assert!(state.delete_attempts.is_empty());
}

#[tokio::test]
async fn absent_history_view_manifest_data_or_statistics_prevents_validation() {
    let fixture = inventory_fixture().await;
    let source = ObjectTableCatalogStore::new(fixture.backend.clone());
    let references = source
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .unwrap();
    for object in [
        fixture.history.as_str(),
        fixture.view.as_str(),
        MANIFEST_LIST,
        DATA_MANIFEST,
        DATA,
        TABLE_STATISTICS,
        PARTITION_STATISTICS,
    ] {
        let backend = copied_backend_with_new_etags(&fixture.backend).await;
        backend
            .state
            .lock()
            .await
            .objects
            .remove(&(BUCKET.to_string(), object.to_string()));
        backend.reset_call_counts().await;
        let store = ObjectTableCatalogStore::new(backend.clone());
        assert!(
            store
                .validate_object_catalog_backup_references(BUCKET, &fixture.objects, &references)
                .await
                .is_err(),
            "accepted missing {object}"
        );
        let state = backend.state.lock().await;
        assert!(state.put_attempts.is_empty());
        assert!(state.delete_attempts.is_empty());
    }
}

#[tokio::test]
async fn absent_inline_current_metadata_remains_a_planned_immutable_restore_write() {
    let fixture = inventory_fixture().await;
    let source = ObjectTableCatalogStore::new(fixture.backend.clone());
    let references = source
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .expect("source inventory");
    let backend = copied_backend_with_new_etags(&fixture.backend).await;
    backend
        .state
        .lock()
        .await
        .objects
        .remove(&(BUCKET.to_string(), fixture.current.clone()));
    backend.reset_call_counts().await;
    let target = ObjectTableCatalogStore::new(backend.clone());
    target
        .validate_object_catalog_backup_references(BUCKET, &fixture.objects, &references)
        .await
        .expect("the inline immutable metadata can be published later by the fenced restore plan");
    {
        let state = backend.state.lock().await;
        assert!(!state.objects.contains_key(&(BUCKET.to_string(), fixture.current.clone())));
        assert!(state.put_attempts.is_empty());
        assert!(state.delete_attempts.is_empty());
    }
    let mut changed = fixture
        .objects
        .iter()
        .find(|object| object.bucket == BUCKET && object.object == fixture.current)
        .expect("inline current metadata")
        .data
        .clone();
    changed[0] ^= 1;
    backend.seed_object(BUCKET, &fixture.current, changed).await;
    backend.reset_call_counts().await;
    assert!(matches!(
        target
            .validate_object_catalog_backup_references(BUCKET, &fixture.objects, &references)
            .await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    let state = backend.state.lock().await;
    assert!(state.put_attempts.is_empty());
    assert!(state.delete_attempts.is_empty());
}

#[tokio::test]
async fn reference_shape_is_rejected_before_backend_access() {
    let fixture = inventory_fixture().await;
    let source = ObjectTableCatalogStore::new(fixture.backend.clone());
    let references = source
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .unwrap();
    let mut cases = Vec::new();
    let mut duplicate = references.clone();
    duplicate.insert(0, duplicate[0].clone());
    cases.push(duplicate);
    let mut unsorted = references.clone();
    unsorted.reverse();
    cases.push(unsorted);
    for (foreign_bucket, invalid_key, missing_digest) in [(true, false, false), (false, true, false), (false, false, true)] {
        let mut invalid = references.clone();
        if foreign_bucket {
            invalid[0].bucket = "foreign".to_string();
        }
        if invalid_key {
            invalid[0].object = "../outside.parquet".to_string();
        }
        if missing_digest {
            invalid[0].sha256 = None;
        }
        cases.push(invalid);
    }
    for invalid in cases {
        let backend = TestCatalogObjectBackend::default();
        let store = ObjectTableCatalogStore::new(backend.clone());
        assert!(matches!(
            store
                .validate_object_catalog_backup_references(BUCKET, &fixture.objects, &invalid)
                .await,
            Err(TableCatalogStoreError::Invalid(_))
        ));
        let state = backend.state.lock().await;
        assert_eq!(state.read_calls, 0);
        assert_eq!(state.metadata_calls, 0);
        assert_eq!(state.list_calls, 0);
        assert!(state.put_attempts.is_empty());
    }
}

#[tokio::test]
async fn saved_inventory_cannot_omit_add_or_relabel_graph_objects() {
    let fixture = inventory_fixture().await;
    let source = ObjectTableCatalogStore::new(fixture.backend.clone());
    let references = source
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .unwrap();
    let mut omitted = references.clone();
    omitted.pop();
    let mut extra = references.clone();
    let mut unrelated = extra[0].clone();
    unrelated.object = "tables/table-id/data/unrelated.parquet".to_string();
    extra.push(unrelated);
    extra.sort_by(|left, right| (&left.bucket, &left.object).cmp(&(&right.bucket, &right.object)));
    let mut relabeled = references.clone();
    relabeled[0].kind = if relabeled[0].kind == ObjectCatalogBackupReferenceKind::DataFile {
        ObjectCatalogBackupReferenceKind::DeleteFile
    } else {
        ObjectCatalogBackupReferenceKind::DataFile
    };
    for invalid in [omitted, extra, relabeled] {
        assert!(matches!(
            source
                .validate_object_catalog_backup_references(BUCKET, &fixture.objects, &invalid)
                .await,
            Err(TableCatalogStoreError::Conflict(_))
        ));
    }
}

#[tokio::test]
async fn capture_rejects_malformed_manifest_and_wrong_history_or_view_identity() {
    for case in 0..3 {
        let fixture = inventory_fixture().await;
        let object = match case {
            0 => DATA_MANIFEST,
            1 => fixture.history.as_str(),
            _ => fixture.view.as_str(),
        };
        let stored = fixture.backend.read_object(BUCKET, object).await.unwrap().unwrap();
        let bytes = if case == 0 {
            b"invalid-avro".to_vec()
        } else {
            let mut value: serde_json::Value = serde_json::from_slice(&stored.data).unwrap();
            value[if case == 1 { "table-uuid" } else { "view-uuid" }] = serde_json::json!("another-identity");
            serde_json::to_vec(&value).unwrap()
        };
        fixture.backend.seed_object(BUCKET, object, bytes).await;
        fixture.backend.reset_call_counts().await;
        let store = ObjectTableCatalogStore::new(fixture.backend.clone());
        assert!(
            store
                .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
                .await
                .is_err()
        );
        assert!(fixture.backend.state.lock().await.put_attempts.is_empty());
    }
}

#[tokio::test]
async fn fingerprint_rejects_changed_etag_after_the_content_read() {
    let backend = TestCatalogObjectBackend::default();
    backend.seed_object(BUCKET, DATA, vec![1]).await;
    let pause = backend.pause_next_read(BUCKET, DATA).await;
    let reader_backend = backend.clone();
    let task = tokio::spawn(async move {
        ObjectTableCatalogStore::new(reader_backend)
            .object_catalog_backup_reference_fingerprint(BUCKET, DATA, ObjectCatalogBackupReferenceKind::DataFile)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.wait_started())
        .await
        .unwrap();
    backend.seed_object(BUCKET, DATA, vec![1]).await;
    pause.release();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(TableCatalogStoreError::Conflict(_))));
}

#[tokio::test]
async fn fingerprint_propagates_read_failure_and_rejects_etagless_objects() {
    let backend = TestCatalogObjectBackend::default();
    backend.seed_object(BUCKET, DATA, vec![1]).await;
    let store = ObjectTableCatalogStore::new(backend.clone());
    *backend.fail_read_object_path.lock().await = Some(DATA.to_string());
    assert!(matches!(
        store
            .object_catalog_backup_reference_fingerprint(BUCKET, DATA, ObjectCatalogBackupReferenceKind::DataFile)
            .await,
        Err(TableCatalogStoreError::Internal(_))
    ));
    backend.omit_etag_for_object(BUCKET, DATA).await;
    assert!(matches!(
        store
            .object_catalog_backup_reference_fingerprint(BUCKET, DATA, ObjectCatalogBackupReferenceKind::DataFile)
            .await,
        Err(TableCatalogStoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn large_objects_keep_the_size_etag_contract_and_are_not_read_into_memory() {
    let fixture = inventory_fixture().await;
    let length = usize::try_from(OBJECT_CATALOG_BACKUP_REFERENCE_HASH_MAX_SIZE).unwrap() + 1;
    fixture.backend.seed_object(BUCKET, DATA, vec![1; length]).await;
    fixture.backend.reset_call_counts().await;
    let source = ObjectTableCatalogStore::new(fixture.backend.clone());
    let references = source
        .capture_object_catalog_backup_references(BUCKET, &fixture.objects)
        .await
        .unwrap();
    let large = references.iter().find(|reference| reference.object == DATA).unwrap();
    assert_eq!(large.size_bytes, u64::try_from(length).unwrap());
    assert_eq!(large.sha256, None);
    assert_eq!(fixture.backend.last_read_limit(BUCKET, DATA).await, None);
    let backend = copied_backend_with_new_etags(&fixture.backend).await;
    let target = ObjectTableCatalogStore::new(backend.clone());
    assert!(matches!(
        target
            .validate_object_catalog_backup_references(BUCKET, &fixture.objects, &references)
            .await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    backend
        .state
        .lock()
        .await
        .objects
        .get_mut(&(BUCKET.to_string(), DATA.to_string()))
        .unwrap()
        .etag = large.etag.clone().unwrap();
    target
        .validate_object_catalog_backup_references(BUCKET, &fixture.objects, &references)
        .await
        .unwrap();
    assert_eq!(backend.last_read_limit(BUCKET, DATA).await, None);
}
