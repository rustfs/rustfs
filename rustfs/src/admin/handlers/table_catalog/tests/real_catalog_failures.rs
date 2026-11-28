//! Real ECStore coverage for Catalog conflicts, recovery, and backup transfer.

use super::*;
use crate::table_catalog::test_support::real_catalog_backend::{FaultPhase, RealCatalogBackend};
use crate::table_catalog::*;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use std::time::Duration;

const META: &str = crate::admin::storage_api::RUSTFS_META_BUCKET;
const NAMESPACE: &str = "sales";
const TABLE: &str = "orders";
const WAIT: Duration = Duration::from_secs(30);
type Backend = RealCatalogBackend<EcStoreTableCatalogObjectBackend<ECStore>>;
type Inventory = BTreeMap<(String, String), TableCatalogObject>;
type ManifestReference = (String, usize, i64, i64, usize);

struct RealScenario {
    fixture: BackupTransferHandlerFixture,
    backend: Backend,
    catalog: ObjectTableCatalogStore<Backend>,
    manifests: BTreeMap<i64, Vec<ManifestReference>>,
    rows: BTreeMap<String, Vec<i32>>,
    objects: BTreeMap<String, Vec<u8>>,
}

impl RealScenario {
    async fn new() -> Self {
        let bucket = format!("i6-{}", Uuid::new_v4().simple());
        let fixture = BackupTransferHandlerFixture::new(&bucket).await;
        let backend = RealCatalogBackend::new(fixture.backend.clone());
        let catalog = ObjectTableCatalogStore::new(backend.clone());
        catalog
            .put_table_bucket(table_bucket_entry_from_metadata_marker(&bucket))
            .await
            .unwrap();
        let namespace = Namespace::parse(NAMESPACE).unwrap();
        create_namespace_response(
            &catalog,
            &bucket,
            CreateNamespaceRequest {
                namespace: vec![NAMESPACE.into()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .unwrap();
        let request = serde_json::from_value(serde_json::json!({
            "name": TABLE,
            "schema": { "type": "struct", "schema-id": 0,
                "fields": [{"id": 1, "name": "id", "required": true, "type": "int"}] }
        }))
        .unwrap();
        create_table_response(
            &catalog,
            &TableCommitObjectBackend::trusted(backend.clone()),
            &bucket,
            &namespace,
            request,
            true,
        )
        .await
        .expect("real Catalog creation");
        let mut scenario = Self {
            fixture,
            backend,
            catalog,
            manifests: BTreeMap::new(),
            rows: BTreeMap::new(),
            objects: BTreeMap::new(),
        };
        let initial = scenario.head().await;
        let metadata = read(&scenario.backend, &bucket, &initial.metadata_location).await;
        scenario.objects.insert(initial.metadata_location, metadata.data);
        scenario
    }

    async fn head(&self) -> TableEntry {
        self.catalog
            .load_table(&self.fixture.bucket, NAMESPACE, TABLE)
            .await
            .unwrap()
            .unwrap()
    }

    async fn candidate(&mut self, base: &TableEntry, snapshot_id: i64, rows: &[i32]) -> String {
        assert!(!rows.is_empty());
        let bucket = self.fixture.bucket.clone();
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&read(&self.backend, &bucket, &base.metadata_location).await.data).unwrap();
        let sequence = metadata["last-sequence-number"].as_i64().unwrap() + 1;
        let timestamp = metadata["last-updated-ms"].as_i64().unwrap() + snapshot_id;
        let root = table_catalog_object_key_from_location(&bucket, &base.warehouse_location).unwrap();
        let data_key = format!("{root}/data/part-{snapshot_id}.parquet");
        let manifest_key = format!("{root}/metadata/manifest-{snapshot_id}.avro");
        let list_key = format!("{root}/metadata/snap-{snapshot_id}.avro");
        let metadata_key = table_metadata_file_path_for_entry(base, &format!("{snapshot_id:05}.metadata.json")).unwrap();
        let data = test_parquet_i32_bytes(rows);
        let data_uri = format!("s3://{bucket}/{data_key}");
        let manifest = manifest_with_sizes(&data_uri, snapshot_id, sequence, rows.len(), data.len());
        let parent = metadata["current-snapshot-id"].as_i64().filter(|id| *id >= 0);
        let mut manifests = parent
            .map(|id| self.manifests.get(&id).expect("known parent snapshot").clone())
            .unwrap_or_default();
        manifests.push((format!("s3://{bucket}/{manifest_key}"), manifest.len(), sequence, snapshot_id, rows.len()));
        let refs = manifests
            .iter()
            .map(|(path, length, seq, id, _)| (path.as_str(), *length, *seq, *id))
            .collect::<Vec<_>>();
        let list = manifest_list_with_rows(&refs, &manifests);
        let mut snapshot = serde_json::json!({
            "snapshot-id": snapshot_id, "sequence-number": sequence, "timestamp-ms": timestamp,
            "manifest-list": format!("s3://{bucket}/{list_key}"), "schema-id": 0, "summary": {"operation": "append"}
        });
        if let Some(parent) = parent {
            snapshot["parent-snapshot-id"] = parent.into();
        }
        metadata["snapshots"].as_array_mut().unwrap().push(snapshot);
        metadata["snapshot-log"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"timestamp-ms": timestamp, "snapshot-id": snapshot_id}));
        let prior_timestamp = metadata["last-updated-ms"].clone();
        metadata["metadata-log"].as_array_mut().unwrap().push(serde_json::json!({
            "timestamp-ms": prior_timestamp, "metadata-file": format!("s3://{bucket}/{}", base.metadata_location)
        }));
        metadata["current-snapshot-id"] = snapshot_id.into();
        metadata["last-sequence-number"] = sequence.into();
        metadata["last-updated-ms"] = timestamp.into();
        metadata["refs"]["main"] = serde_json::json!({"snapshot-id": snapshot_id, "type": "branch"});
        for (key, bytes) in [
            (data_key.clone(), data),
            (manifest_key, manifest),
            (list_key, list),
            (metadata_key.clone(), serde_json::to_vec(&metadata).unwrap()),
        ] {
            self.backend
                .put_object(&bucket, &key, bytes.clone(), TableCatalogPutPrecondition::IfAbsent)
                .await
                .expect("real candidate file publication");
            self.objects.insert(key, bytes);
        }
        self.rows.insert(data_key, rows.to_vec());
        self.manifests.insert(snapshot_id, manifests);
        metadata_key
    }

    fn commit_request(&self, base: &TableEntry, candidate: &str, commit_id: &str) -> TableCommitRequest {
        TableCommitRequest {
            table_bucket: self.fixture.bucket.clone(),
            namespace: NAMESPACE.into(),
            table: TABLE.into(),
            commit_id: commit_id.into(),
            idempotency_key: Some(format!("request-{commit_id}")),
            operation: "append".into(),
            expected_version_token: base.version_token.clone(),
            expected_metadata_location: base.metadata_location.clone(),
            new_metadata_location: candidate.into(),
            requirements: Vec::new(),
            writer: Some("i6-real-ecstore".into()),
        }
    }

    async fn commit(&self, base: &TableEntry, candidate: &str, commit_id: &str) -> TableEntry {
        self.catalog
            .commit_table(self.commit_request(base, candidate, commit_id))
            .await
            .expect("real Iceberg commit")
            .table
    }

    async fn assert_files(&self, backend: &impl TableCatalogObjectBackend) {
        for (key, expected) in &self.objects {
            let object = read(backend, &self.fixture.bucket, key).await;
            assert_eq!(&object.data, expected, "exact object bytes for {key}");
            assert!(object.etag.is_some(), "real ETag for {key}");
            assert!(object.body_revision.is_some(), "real body revision for {key}");
            if key.ends_with(".avro") {
                let values = apache_avro::Reader::new(object.data.as_slice())
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert!(!values.is_empty(), "nonempty real Avro for {key}");
            }
            if let Some(rows) = self.rows.get(key) {
                assert_eq!(parquet_rows(&object.data), *rows, "all Parquet rows for {key}");
            }
        }
    }

    async fn current_rows(&self, backend: &impl TableCatalogObjectBackend, head: &TableEntry) -> Vec<i32> {
        let metadata: serde_json::Value =
            serde_json::from_slice(&read(backend, &self.fixture.bucket, &head.metadata_location).await.data).unwrap();
        let snapshot = metadata["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .find(|snapshot| snapshot["snapshot-id"] == metadata["current-snapshot-id"])
            .unwrap();
        let list_key =
            table_catalog_object_key_from_location(&self.fixture.bucket, snapshot["manifest-list"].as_str().unwrap()).unwrap();
        let list = read(backend, &self.fixture.bucket, &list_key).await;
        let mut rows = Vec::new();
        for manifest in apache_avro::Reader::new(list.data.as_slice()).unwrap() {
            let manifest = manifest.unwrap();
            let uri = avro_string(&manifest, "manifest_path");
            let key = table_catalog_object_key_from_location(&self.fixture.bucket, uri).unwrap();
            let object = read(backend, &self.fixture.bucket, &key).await;
            for entry in apache_avro::Reader::new(object.data.as_slice()).unwrap() {
                let entry = entry.unwrap();
                let file = avro_field(&entry, "data_file");
                let data_key =
                    table_catalog_object_key_from_location(&self.fixture.bucket, avro_string(file, "file_path")).unwrap();
                let data = read(backend, &self.fixture.bucket, &data_key).await;
                let actual = parquet_rows(&data.data);
                assert_eq!(
                    avro_field(file, "record_count"),
                    &apache_avro::types::Value::Long(i64::try_from(actual.len()).unwrap())
                );
                assert_eq!(
                    avro_field(file, "file_size_in_bytes"),
                    &apache_avro::types::Value::Long(i64::try_from(data.data.len()).unwrap())
                );
                rows.extend(actual);
            }
        }
        rows.sort();
        rows
    }
}

fn avro_field<'a>(value: &'a apache_avro::types::Value, field: &str) -> &'a apache_avro::types::Value {
    let apache_avro::types::Value::Record(fields) = value else { panic!("Avro record required") };
    &fields.iter().find(|(name, _)| name == field).expect("Avro field").1
}
fn avro_string<'a>(value: &'a apache_avro::types::Value, field: &str) -> &'a str {
    let apache_avro::types::Value::String(value) = avro_field(value, field) else {
        panic!("Avro string required")
    };
    value
}
fn manifest_list_with_rows(references: &[(&str, usize, i64, i64)], manifests: &[ManifestReference]) -> Vec<u8> {
    let bytes = test_manifest_list_avro_entries(references);
    let reader = apache_avro::Reader::new(bytes.as_slice()).unwrap();
    let schema = reader.writer_schema().clone();
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).unwrap();
    for (value, (_, _, _, _, rows)) in reader.zip(manifests) {
        let mut value = value.unwrap();
        let apache_avro::types::Value::Record(fields) = &mut value else { panic!("manifest-list record") };
        fields.iter_mut().find(|(name, _)| name == "added_rows_count").unwrap().1 =
            apache_avro::types::Value::Long(i64::try_from(*rows).unwrap());
        writer.append_value(value).unwrap();
    }
    writer.into_inner().unwrap()
}

fn manifest_with_sizes(path: &str, snapshot: i64, sequence: i64, rows: usize, size: usize) -> Vec<u8> {
    let bytes = test_manifest_avro_bytes(&[(path, 0, 1, snapshot, sequence)]);
    let reader = apache_avro::Reader::new(bytes.as_slice()).unwrap();
    let schema = reader.writer_schema().clone();
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).unwrap();
    writer.add_user_metadata("partition-spec-id".into(), "0").unwrap();
    for value in reader {
        let mut value = value.unwrap();
        let apache_avro::types::Value::Record(fields) = &mut value else { panic!("manifest record") };
        let (_, apache_avro::types::Value::Record(data)) = fields.iter_mut().find(|(name, _)| name == "data_file").unwrap()
        else {
            panic!("data record")
        };
        for (name, value) in data {
            if name == "record_count" {
                *value = apache_avro::types::Value::Long(i64::try_from(rows).unwrap());
            }
            if name == "file_size_in_bytes" {
                *value = apache_avro::types::Value::Long(i64::try_from(size).unwrap());
            }
        }
        writer.append_value(value).unwrap();
    }
    writer.into_inner().unwrap()
}
fn parquet_rows(bytes: &[u8]) -> Vec<i32> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .unwrap()
        .build()
        .unwrap();
    reader
        .flat_map(|batch| {
            let batch = batch.unwrap();
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}
async fn read(backend: &impl TableCatalogObjectBackend, bucket: &str, key: &str) -> TableCatalogObject {
    backend
        .read_object(bucket, key)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("missing real object {bucket}/{key}"))
}
async fn inventory(backend: &impl TableCatalogObjectBackend, bucket: &str) -> Inventory {
    let mut result = BTreeMap::new();
    for bucket in [META, bucket] {
        for key in backend.list_objects(bucket, "").await.unwrap() {
            let object = read(backend, bucket, &key).await;
            result.insert((bucket.to_string(), key), object);
        }
    }
    result
}
async fn table_call(
    fixture: &BackupTransferHandlerFixture,
    suffix: &str,
    method: Method,
    body: serde_json::Value,
    operation: &impl Operation,
) -> S3Result<AdminResponse> {
    let path = format!("/iceberg/v1/{}/namespaces/{NAMESPACE}/tables/{TABLE}{suffix}", fixture.bucket);
    let request = table_catalog_request_builder(fixture.context.clone(), Some(fixture.root_credentials()), method, &path)
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .build();
    let mut router = matchit::Router::new();
    router
        .insert(format!("/iceberg/v1/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}{suffix}"), ())
        .unwrap();
    let matched = router.at(&path).unwrap();
    operation.call(request, matched.params).await
}
fn rest_commit(base: &TableEntry, candidate: &str, id: &str) -> serde_json::Value {
    serde_json::json!({"commit-id": id, "idempotency-key": format!("request-{id}"), "operation": "append",
        "expected-version-token": base.version_token, "expected-metadata-location": base.metadata_location,
        "new-metadata-location": candidate, "requirements": [], "updates": [], "writer": "i6-real-ecstore"})
}
async fn json_response(response: AdminResponse) -> serde_json::Value {
    let (status, _headers, body) = response_parts(response).await;
    assert_eq!(status, StatusCode::OK);
    serde_json::from_slice(&body).unwrap()
}
fn assert_error(error: S3Error, status: StatusCode, reason: &str) {
    assert_eq!(error.status_code(), Some(status));
    assert!(error.message().unwrap_or_default().contains(reason), "unexpected error: {error:?}");
}
async fn diagnostics(fixture: &BackupTransferHandlerFixture) -> serde_json::Value {
    json_response(
        table_call(
            fixture,
            "/catalog/diagnostics",
            Method::GET,
            serde_json::Value::Null,
            &GetTableCatalogDiagnosticsHandler {},
        )
        .await
        .unwrap(),
    )
    .await
}
async fn recovery(fixture: &BackupTransferHandlerFixture) -> serde_json::Value {
    json_response(
        table_call(
            fixture,
            "/catalog/recover",
            Method::POST,
            serde_json::Value::Null,
            &RecoverTableCatalogHandler {},
        )
        .await
        .unwrap(),
    )
    .await
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("bounded real Catalog operation")
}

fn catalog_lock_holders(manager: &rustfs_lock::FastObjectLockManager) -> Vec<serde_json::Value> {
    manager
        .list_locks_with_holder_counts()
        .into_iter()
        .map(|(info, holders)| {
            serde_json::json!({"key": info.key, "mode": info.mode, "owner": info.owner.as_ref(), "holders": holders})
        })
        .collect()
}

fn assert_catalog_lock_mode(
    manager: &rustfs_lock::FastObjectLockManager,
    key: &rustfs_lock::ObjectKey,
    expected: rustfs_lock::LockMode,
) {
    let info = manager
        .get_lock_info(key)
        .unwrap_or_else(|| panic!("expected held lock for {key:?}"));
    assert_eq!(info.mode, expected, "actual mode for {key:?}");
    let holders = manager
        .list_locks_with_holder_counts()
        .into_iter()
        .filter(|(info, _)| &info.key == key)
        .map(|(_, count)| count)
        .sum::<u32>();
    assert_eq!(holders, 1, "one actual guard for {key:?}");
}

async fn wait_for_catalog_locks_released(
    manager: &rustfs_lock::FastObjectLockManager,
    keys: &[rustfs_lock::ObjectKey],
    stage: &str,
) {
    tokio::time::timeout(WAIT, async {
        while keys.iter().any(|key| manager.get_lock_info(key).is_some()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "{stage}: {error}; expected released keys {keys:?}; global counts {:?}; holders {:?}",
            manager.current_lock_counts(),
            catalog_lock_holders(manager)
        )
    });
}

async fn object_mode(future: impl std::future::Future<Output = ()>) {
    temp_env::async_with_vars([(ENV_TABLE_CATALOG_BACKING, Some(TABLE_CATALOG_BACKING_OBJECT))], future).await;
}

#[tokio::test]
#[serial_test::serial]
async fn stale_token_commit_conflict_and_concurrent_writers_preserve_all_rows() {
    object_mode(async {
        let mut scenario = RealScenario::new().await;
        let initial = scenario.head().await;
        let first = scenario.candidate(&initial, 10, &[1, 2]).await;
        let a = scenario.commit(&initial, &first, "a").await;
        let loaded = json_response(
            table_call(&scenario.fixture, "", Method::GET, serde_json::Value::Null, &RestLoadTableHandler {})
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            loaded["metadata-location"],
            format!("s3://{}/{}", scenario.fixture.bucket, a.metadata_location)
        );
        let second = scenario.candidate(&a, 20, &[3, 4]).await;
        let b = scenario.commit(&a, &second, "b").await;
        let stale = scenario.candidate(&b, 30, &[5, 6]).await;
        let before = inventory(&scenario.backend, &scenario.fixture.bucket).await;
        let error = table_call(
            &scenario.fixture,
            "",
            Method::POST,
            rest_commit(&a, &stale, "stale"),
            &RestCommitTableHandler {},
        )
        .await
        .unwrap_err();
        assert_error(error, StatusCode::CONFLICT, "token");
        assert_eq!(scenario.head().await, b);
        assert_eq!(inventory(&scenario.backend, &scenario.fixture.bucket).await, before);

        let left = scenario.candidate(&b, 40, &[7, 8]).await;
        let right = scenario.candidate(&b, 50, &[9, 10]).await;
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for (key, id) in [(&left, "left"), (&right, "right")] {
            let request = scenario.commit_request(&b, key, id);
            let catalog = scenario.catalog.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                catalog.commit_table(request).await
            }));
        }
        bounded(barrier.wait()).await;
        let mut successes = Vec::new();
        let mut conflicts = 0;
        for task in tasks {
            match bounded(task).await.unwrap() {
                Ok(result) => successes.push(result.table),
                Err(TableCatalogStoreError::Conflict(message)) => {
                    assert!(message.contains("token"));
                    conflicts += 1;
                }
                Err(error) => panic!("unexpected concurrent commit error: {error:?}"),
            }
        }
        assert_eq!(successes.len(), 1);
        assert_eq!(conflicts, 1);
        let winner = &successes[0];
        assert_eq!(winner.generation, b.generation + 1);
        let retry_rows = if winner.metadata_location == left {
            vec![9, 10]
        } else {
            vec![7, 8]
        };
        let retry = scenario.candidate(winner, 60, &retry_rows).await;
        let final_head = scenario.commit(winner, &retry, "retry").await;
        assert_eq!(final_head.generation, b.generation + 2);
        assert_eq!(scenario.current_rows(&scenario.backend, &final_head).await, vec![1, 2, 3, 4, 7, 8, 9, 10]);
        scenario.assert_files(&scenario.backend).await;

        let path = format!("/iceberg/v1/{}/namespaces/{NAMESPACE}/tables/{TABLE}", scenario.fixture.bucket);
        let unauthorized =
            table_catalog_request_builder(scenario.fixture.context.clone(), Some(BACKUP_TRANSFER_READER), Method::POST, &path)
                .body(Body::from(serde_json::to_vec(&rest_commit(&final_head, &retry, "denied")).unwrap()))
                .build();
        let before = inventory(&scenario.backend, &scenario.fixture.bucket).await;
        let mut router = matchit::Router::new();
        router
            .insert("/iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}", ())
            .unwrap();
        let error = RestCommitTableHandler {}
            .call(unauthorized, router.at(&path).unwrap().params)
            .await
            .unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
        assert_eq!(inventory(&scenario.backend, &scenario.fixture.bucket).await, before);
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn missing_metadata_object_rejected_before_catalog_mutation() {
    object_mode(async {
        let mut scenario = RealScenario::new().await;
        let initial = scenario.head().await;
        let first = scenario.candidate(&initial, 10, &[1, 2]).await;
        let head = scenario.commit(&initial, &first, "a").await;
        let absent = table_metadata_file_path_for_entry(&head, "99999.metadata.json").unwrap();
        assert!(
            scenario
                .backend
                .read_object(&scenario.fixture.bucket, &absent)
                .await
                .unwrap()
                .is_none()
        );
        let before = inventory(&scenario.backend, &scenario.fixture.bucket).await;
        let error = table_call(
            &scenario.fixture,
            "",
            Method::POST,
            rest_commit(&head, &absent, "missing"),
            &RestCommitTableHandler {},
        )
        .await
        .unwrap_err();
        assert_error(error, StatusCode::BAD_REQUEST, "table metadata object not found");
        assert_eq!(scenario.head().await, head);
        assert_eq!(inventory(&scenario.backend, &scenario.fixture.bucket).await, before);
        scenario.assert_files(&scenario.backend).await;
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn diagnostics_after_finalization_gap_reports_real_published_head() {
    object_mode(async {
        let mut scenario = RealScenario::new().await;
        let a = scenario.head().await;
        let next = scenario.candidate(&a, 10, &[1, 2, 3]).await;
        let path = TableCatalogObjectPaths::default().commit_log_entry_path(&scenario.fixture.bucket, &a.table_id, "gap");
        scenario.backend.fail(META, &path, 2, FaultPhase::Before);
        let b = scenario.commit(&a, &next, "gap").await;
        scenario.backend.assert_consumed();
        let events = scenario
            .backend
            .events()
            .into_iter()
            .filter(|event| event.object == path)
            .collect::<Vec<_>>();
        assert_eq!(events.iter().filter(|event| event.phase == "attempted").count(), 2);
        assert_eq!(events.iter().filter(|event| event.phase == "inner-completed").count(), 1);
        let log = read(&scenario.backend, META, &path).await;
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&log.data).unwrap()["status"], "STAGED");
        assert_eq!(b.metadata_location, next);
        assert_eq!(b.generation, a.generation + 1);
        let before = inventory(&scenario.backend, &scenario.fixture.bucket).await;
        let report = diagnostics(&scenario.fixture).await;
        assert_eq!(report["commit_recovery"]["finalization_required_count"], 1);
        let commit = report["commit_recovery"]["commits"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["commit_id"] == "gap")
            .unwrap();
        assert_eq!(commit["recovery_state"], "FINALIZATION_REQUIRED");
        assert_eq!(scenario.head().await, b);
        assert_eq!(inventory(&scenario.backend, &scenario.fixture.bucket).await, before);
        assert_eq!(scenario.current_rows(&scenario.backend, &b).await, vec![1, 2, 3]);
        scenario.assert_files(&scenario.backend).await;
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn recovery_repairs_stale_and_missing_idempotency_indexes_and_preserves_conflicts() {
    object_mode(async {
        for case in ["stale", "missing", "conflicting"] {
            let mut scenario = RealScenario::new().await;
            let a = scenario.head().await;
            let next = scenario.candidate(&a, 10, &[1, 2, 3]).await;
            let paths = TableCatalogObjectPaths::default();
            let index_path = paths.commit_idempotency_entry_path(&scenario.fixture.bucket, &a.table_id, "request-recovery");
            let log_path = paths.commit_log_entry_path(&scenario.fixture.bucket, &a.table_id, "recovery");
            scenario.backend.fail(META, &index_path, 2, FaultPhase::Before);
            let b = scenario.commit(&a, &next, "recovery").await;
            scenario.backend.assert_consumed();
            let log = read(&scenario.backend, META, &log_path).await;
            let log_json: serde_json::Value = serde_json::from_slice(&log.data).unwrap();
            assert_eq!(log_json["status"], "COMMITTED");
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&read(&scenario.backend, META, &index_path).await.data).unwrap()["status"], "STAGED");
            if case == "missing" { scenario.backend.delete_object(META, &index_path).await.unwrap(); }
            if case == "conflicting" {
                let mut conflict = log_json.clone();
                conflict["new_version_token"] = "unrelated-token".into();
                scenario.backend.put_object(META, &index_path, serde_json::to_vec(&conflict).unwrap(), TableCatalogPutPrecondition::Any).await.unwrap();
            }
            let before = inventory(&scenario.backend, &scenario.fixture.bucket).await;
            let diagnostics = diagnostics(&scenario.fixture).await;
            let report = &diagnostics["commit_recovery"];
            assert_eq!(report["commits"][0]["idempotency_index_status"], case.to_uppercase());
            assert_eq!(report["manual_review_count"], usize::from(case == "conflicting"));
            assert_eq!(report["idempotency_repair_required_count"], usize::from(case != "conflicting"));
            let repaired = recovery(&scenario.fixture).await;
            assert_eq!(repaired["manual_review_count"], usize::from(case == "conflicting"));
            assert_eq!(repaired["idempotency_repair_required_count"], 0);
            assert_eq!(repaired["finalized_count"], usize::from(case != "conflicting"));
            let after = inventory(&scenario.backend, &scenario.fixture.bucket).await;
            for (key, object) in &before {
                if key.0 == META && (key.1 == index_path || key.1 == log_path) && case != "conflicting" { continue; }
                assert_eq!(after.get(key), Some(object), "recovery changed unrelated bytes/identity: {case} {key:?}");
            }
            assert_eq!(read(&scenario.backend, META, &log_path).await.data, log.data, "recovery preserves the committed log payload");
            if case == "conflicting" { assert_eq!(after, before); }
            else { assert_eq!(read(&scenario.backend, META, &index_path).await.data, log.data); }
            assert_eq!(scenario.head().await, b);
            scenario.assert_files(&scenario.backend).await;
        }
    }).await;
}

#[tokio::test]
#[serial_test::serial]
async fn maintenance_stale_plan_rejects_worker_claim_and_actual_delete() {
    object_mode(async {
        let mut scenario = RealScenario::new().await;
        let a = scenario.head().await;
        let first = scenario.candidate(&a, 10, &[1, 2]).await;
        let a = scenario.commit(&a, &first, "a").await;
        let orphan = table_metadata_file_path_for_entry(&a, "orphan.metadata.json").unwrap();
        let orphan_bytes = read(&scenario.backend, &scenario.fixture.bucket, &a.metadata_location)
            .await
            .data;
        scenario
            .backend
            .put_object(
                &scenario.fixture.bucket,
                &orphan,
                orphan_bytes.clone(),
                TableCatalogPutPrecondition::IfAbsent,
            )
            .await
            .unwrap();
        scenario.objects.insert(orphan.clone(), orphan_bytes);
        scenario
            .catalog
            .put_table_maintenance_config(
                &scenario.fixture.bucket,
                NAMESPACE,
                TABLE,
                TableMaintenanceConfig {
                    delete_enabled: true,
                    background_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let namespace = Namespace::parse(NAMESPACE).unwrap();
        let table = IdentifierSegment::parse(TABLE).unwrap();
        let paths = TableCatalogObjectPaths::default();
        let current_path = paths.table_maintenance_current_job_path(&scenario.fixture.bucket, &namespace, &table, &a.table_id);
        scenario.backend.fail(META, &current_path, 1, FaultPhase::After);
        let error = table_metadata_maintenance_response(
            &scenario.catalog,
            &scenario.backend,
            &scenario.fixture.bucket,
            &namespace,
            TABLE,
            serde_json::from_value(serde_json::json!({"delete": true})).unwrap(),
        )
        .await
        .unwrap_err();
        assert_error(error, StatusCode::INTERNAL_SERVER_ERROR, "I6 injected fault");
        scenario.backend.assert_consumed();
        let current = read(&scenario.backend, META, &current_path).await;
        let report: TableMetadataMaintenanceReport = serde_json::from_slice(&current.data).unwrap();
        assert_eq!(report.job.status, TableMetadataMaintenanceJobStatus::Running);
        assert_eq!(report.job.current_generation, a.generation);
        assert_eq!(report.current_metadata_location, a.metadata_location);
        assert_eq!(report.job.deleted_metadata_file_count, 0);
        assert_eq!(report.job.deleted_object_count, 0);
        assert!(report.cleanup_candidate_locations.contains(&orphan));
        assert!(report.job.safety_window_seconds > 0);
        assert!(!report.deletable_metadata_locations.contains(&orphan));
        for path in [
            paths.table_maintenance_latest_job_path(&scenario.fixture.bucket, &namespace, &table, &a.table_id),
            paths.table_maintenance_job_path(&scenario.fixture.bucket, &namespace, &table, &a.table_id, &report.job.job_id),
        ] {
            assert_eq!(
                read(&scenario.backend, META, &path).await.data,
                current.data,
                "all three Running aliases must exist"
            );
        }
        let next = scenario.candidate(&a, 20, &[3, 4]).await;
        let b = bounded(scenario.commit(&a, &next, "b")).await;
        let before = inventory(&scenario.backend, &scenario.fixture.bucket).await;
        let error = table_call(
            &scenario.fixture,
            "/maintenance/worker/run",
            Method::POST,
            serde_json::json!({
                "worker-id": "i6-worker", "expected-job-id": report.job.job_id, "expected-attempt": report.job.attempt
            }),
            &RunTableMaintenanceWorkerHandler {},
        )
        .await
        .unwrap_err();
        assert_error(
            error,
            StatusCode::CONFLICT,
            "maintenance job identity or catalog state changed before conditional worker claim",
        );
        assert_eq!(inventory(&scenario.backend, &scenario.fixture.bucket).await, before);
        assert_eq!(scenario.head().await, b);

        let (planned_tx, planned_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        let catalog = scenario.catalog.clone();
        let bucket = scenario.fixture.bucket.clone();
        let task = tokio::spawn(async move {
            crate::table_catalog::test_support::real_maintenance::plan_then_delete_after(
                &catalog, &bucket, NAMESPACE, TABLE, 0, planned_tx, resume_rx,
            )
            .await
        });
        let planned = bounded(planned_rx).await.unwrap();
        assert_eq!(planned.current_metadata_location, b.metadata_location);
        assert_eq!(planned.job.current_generation, b.generation);
        assert!(planned.cleanup_candidate_locations.contains(&orphan));
        assert_eq!(planned.job.safety_window_seconds, report.job.safety_window_seconds);
        let third = scenario.candidate(&b, 30, &[5, 6]).await;
        let c = bounded(scenario.commit(&b, &third, "c")).await;
        let before = inventory(&scenario.backend, &scenario.fixture.bucket).await;
        resume_tx.send(()).unwrap();
        let error = bounded(task).await.unwrap().unwrap_err();
        assert_error(
            catalog_store_error(error),
            StatusCode::CONFLICT,
            "current metadata location changed before maintenance delete",
        );
        assert_eq!(inventory(&scenario.backend, &scenario.fixture.bucket).await, before);
        assert_eq!(scenario.head().await, c);
        assert!(
            !scenario
                .backend
                .events()
                .iter()
                .any(|event| event.method.starts_with("delete") && event.phase == "inner-started")
        );
        scenario.assert_files(&scenario.backend).await;
        assert_eq!(scenario.current_rows(&scenario.backend, &c).await, vec![1, 2, 3, 4, 5, 6]);
        diagnostics(&scenario.fixture).await;
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn external_sync_conflict_and_uuid_mismatch_preserve_real_catalog() {
    object_mode(async {
        let mut scenario = RealScenario::new().await;
        let initial = scenario.head().await;
        let first = scenario.candidate(&initial, 10, &[1, 2]).await;
        let a = scenario.commit(&initial, &first, "a").await;
        let second = scenario.candidate(&a, 20, &[3, 4]).await;
        let b = scenario.commit(&a, &second, "b").await;
        let external = scenario.candidate(&b, 30, &[5, 6]).await;
        let mut body = serde_json::json!({
            "catalog": "glue", "external-catalog-id": "external-i6", "external-namespace": NAMESPACE,
            "external-table": TABLE, "external-table-uuid": b.table_uuid, "metadata-location": external,
            "external-version-token": "external-v3", "expected-version-token": a.version_token,
            "expected-metadata-location": a.metadata_location, "commit-id": "external-stale",
            "idempotency-key": "external-stale-request", "policy-mode": "rustfs-authoritative", "credential-mode": "rustfs-table-credentials"
        });
        let before = inventory(&scenario.backend, &scenario.fixture.bucket).await;
        let error = table_call(&scenario.fixture, "/catalog/external/sync", Method::POST, body.clone(), &SyncExternalCatalogBridgeHandler {}).await.unwrap_err();
        assert_error(error, StatusCode::CONFLICT, "token");
        assert_eq!(scenario.head().await, b);
        assert_eq!(inventory(&scenario.backend, &scenario.fixture.bucket).await, before);
        body["expected-version-token"] = b.version_token.clone().into();
        body["expected-metadata-location"] = b.metadata_location.clone().into();
        body["external-table-uuid"] = Uuid::new_v4().to_string().into();
        let error = table_call(&scenario.fixture, "/catalog/external/sync", Method::POST, body, &SyncExternalCatalogBridgeHandler {}).await.unwrap_err();
        assert_error(error, StatusCode::BAD_REQUEST, "external table uuid does not match");
        assert_eq!(inventory(&scenario.backend, &scenario.fixture.bucket).await, before);
        assert_eq!(scenario.head().await, b);
        scenario.assert_files(&scenario.backend).await;
    }).await;
}

struct RealBackup {
    id: String,
    path: String,
    artifact: Vec<u8>,
    references: BTreeMap<String, TableCatalogObject>,
    embedded: Vec<(String, String)>,
}

async fn capture_backup(scenario: &RealScenario) -> RealBackup {
    let backup = scenario
        .fixture
        .catalog
        .create_catalog_backup(&scenario.fixture.bucket, None)
        .await
        .unwrap();
    let artifact = read(&scenario.backend, META, &backup.backup_path).await;
    let before = inventory(&scenario.backend, &scenario.fixture.bucket).await;
    let response = call_catalog_backup_export_handler(scenario.fixture.operator_request(
        TABLE_CATALOG_PREFIX,
        "export",
        serde_json::to_vec(&serde_json::json!({"backup-id": backup.backup_id})).unwrap(),
    ))
    .await
    .unwrap();
    let bytes = private_backup_transfer_body(response).await;
    assert_eq!(bytes.as_ref(), artifact.data.as_slice());
    assert_eq!(inventory(&scenario.backend, &scenario.fixture.bucket).await, before);
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["version"], 2);
    let mut references = BTreeMap::new();
    for reference in value["references"].as_array().unwrap() {
        assert_eq!(reference["bucket"], scenario.fixture.bucket);
        let key = reference["object"].as_str().unwrap();
        let object = read(&scenario.backend, &scenario.fixture.bucket, key).await;
        assert_eq!(reference["size_bytes"].as_u64(), Some(u64::try_from(object.data.len()).unwrap()));
        if let Some(digest) = reference["sha256"].as_str() {
            assert_eq!(
                digest,
                hex_simd::encode_to_string(Sha256::digest(&object.data), hex_simd::AsciiCase::Lower)
            );
        }
        references.insert(key.to_string(), object);
    }
    assert!(references.keys().any(|key| key.ends_with(".parquet")));
    assert!(references.keys().filter(|key| key.ends_with(".avro")).count() >= 4);
    let embedded = value["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|object| {
            (
                object["bucket"].as_str().unwrap().to_string(),
                object["object"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    RealBackup {
        id: backup.backup_id,
        path: backup.backup_path,
        artifact: artifact.data,
        references,
        embedded,
    }
}
async fn import_backup(target: &BackupTransferHandlerFixture, backup: &RealBackup) {
    let first = json_response(
        call_catalog_backup_import_handler(target.operator_request(TABLE_CATALOG_PREFIX, "import", backup.artifact.clone()))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(first["status"], "CREATED");
    let before = inventory(&target.backend, &target.bucket).await;
    let mut repeated = json_response(
        call_catalog_backup_import_handler(target.operator_request(TABLE_CATALOG_PREFIX, "import", backup.artifact.clone()))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(repeated["status"], "ALREADY_PRESENT");
    repeated["status"] = first["status"].clone();
    assert_eq!(repeated, first);
    assert_eq!(inventory(&target.backend, &target.bucket).await, before);
    assert_eq!(read(&target.backend, META, &backup.path).await.data, backup.artifact);
}
async fn prepare_references(target: &BackupTransferHandlerFixture, backup: &RealBackup, skip: Option<&str>) {
    for (key, source) in &backup.references {
        if skip == Some(key.as_str()) {
            continue;
        }
        let result = json_response(
            call_catalog_backup_prepare_handler(target.operator_request(
                TABLE_CATALOG_PREFIX,
                "prepare",
                backup_object_prepare_body(&backup.id, key, &source.data),
            ))
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(result["status"], "PREPARED");
        let copied = read(&target.backend, &target.bucket, key).await;
        assert_eq!(copied.data, source.data, "transferred source bytes for {key}");
    }
    assert!(target.catalog.get_table_bucket(&target.bucket).await.unwrap().is_none());
}
async fn restore_handler(target: &BackupTransferHandlerFixture, backup_id: &str, restore_id: &str) -> S3Result<AdminResponse> {
    let request = table_catalog_request_builder(
        target.context.clone(),
        Some(target.root_credentials()),
        Method::POST,
        &format!("/iceberg/v1/{}/catalog/restore", target.bucket),
    )
    .body(Body::from(
        serde_json::to_vec(&serde_json::json!({"backup-id": backup_id, "restore-id": restore_id})).unwrap(),
    ))
    .build();
    call_catalog_restore_handler(request).await
}
async fn complete_controls(target: &BackupTransferHandlerFixture, backup: &RealBackup, restore_id: &str) -> serde_json::Value {
    let paths = TableCatalogObjectPaths::default();
    let state = read(&target.backend, META, &paths.catalog_restore_state_path(&target.bucket)).await;
    let intent = read(&target.backend, META, &paths.catalog_restore_intent_path(&target.bucket, restore_id)).await;
    let state_json: serde_json::Value = serde_json::from_slice(&state.data).unwrap();
    let intent_json: serde_json::Value = serde_json::from_slice(&intent.data).unwrap();
    assert_eq!(state_json["state"], "COMPLETED");
    assert_eq!(intent_json["state"], "COMPLETED");
    assert_eq!(state_json["restore_epoch"], intent_json["restore_epoch"]);
    Uuid::parse_str(state_json["restore_epoch"].as_str().unwrap()).unwrap();
    assert_eq!(state_json["restore_id"], restore_id);
    assert_eq!(state_json["backup_id"], backup.id);
    assert_eq!(state_json["completed_intent_etag"].as_str(), intent.etag.as_deref());
    assert_eq!(
        state_json["backup_etag"].as_str(),
        read(&target.backend, META, &backup.path).await.etag.as_deref()
    );
    assert_eq!(intent_json["object_count"], backup.embedded.len());
    state_json
}
async fn source_with_two_snapshots() -> RealScenario {
    let mut scenario = RealScenario::new().await;
    let initial = scenario.head().await;
    let first = scenario.candidate(&initial, 10, &[1, 2, 3]).await;
    let first = scenario.commit(&initial, &first, "first").await;
    let second = scenario.candidate(&first, 20, &[4, 5, 6]).await;
    let head = scenario.commit(&first, &second, "second").await;
    assert_eq!(scenario.current_rows(&scenario.backend, &head).await, vec![1, 2, 3, 4, 5, 6]);
    scenario
}

#[tokio::test]
#[serial_test::serial]
async fn backup_restore_transfers_two_snapshots_and_replays_receipt_after_new_head() {
    object_mode(async {
        let mut source = source_with_two_snapshots().await;
        let original_head = source.head().await;
        let backup = capture_backup(&source).await;
        let target = BackupTransferHandlerFixture::new(&source.fixture.bucket).await;
        assert!(!Arc::ptr_eq(&source.fixture.context.object_store(), &target.context.object_store()));
        import_backup(&target, &backup).await;
        prepare_references(&target, &backup, None).await;
        let restored = json_response(restore_handler(&target, &backup.id, "complete-transfer").await.unwrap()).await;
        assert_eq!(restored["status"], "RESTORED");
        assert_eq!(
            target
                .catalog
                .load_table(&target.bucket, NAMESPACE, TABLE)
                .await
                .unwrap()
                .unwrap(),
            original_head
        );
        assert!(
            target
                .catalog
                .get_namespace(&target.bucket, NAMESPACE)
                .await
                .unwrap()
                .is_some()
        );
        source.assert_files(&target.backend).await;
        assert_eq!(source.current_rows(&target.backend, &original_head).await, vec![1, 2, 3, 4, 5, 6]);
        let control = complete_controls(&target, &backup, "complete-transfer").await;
        let before = inventory(&target.backend, &target.bucket).await;
        let mut replay = json_response(restore_handler(&target, &backup.id, "complete-transfer").await.unwrap()).await;
        assert_eq!(replay["status"], "ALREADY_RESTORED");
        replay["status"] = restored["status"].clone();
        assert_eq!(replay, restored);
        assert_eq!(inventory(&target.backend, &target.bucket).await, before);

        let next = source.candidate(&original_head, 30, &[7, 8, 9]).await;
        for (key, bytes) in &source.objects {
            if target.backend.object_exists(&target.bucket, key).await.unwrap() {
                continue;
            }
            target
                .backend
                .put_object(&target.bucket, key, bytes.clone(), TableCatalogPutPrecondition::IfAbsent)
                .await
                .unwrap();
        }
        let target_catalog = ObjectTableCatalogStore::new(target.backend.clone());
        let updated = target_catalog
            .commit_table(source.commit_request(&original_head, &next, "after-restore"))
            .await
            .unwrap()
            .table;
        let before = inventory(&target.backend, &target.bucket).await;
        let replay = json_response(restore_handler(&target, &backup.id, "complete-transfer").await.unwrap()).await;
        assert_eq!(replay["status"], "ALREADY_RESTORED");
        assert_eq!(
            target_catalog
                .load_table(&target.bucket, NAMESPACE, TABLE)
                .await
                .unwrap()
                .unwrap(),
            updated
        );
        assert_eq!(inventory(&target.backend, &target.bucket).await, before);
        assert_eq!(complete_controls(&target, &backup, "complete-transfer").await, control);
        assert_eq!(source.current_rows(&target.backend, &updated).await, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        source.assert_files(&target.backend).await;
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn backup_restore_continues_with_new_catalog_objects_after_real_write_failures() {
    object_mode(async {
        let source = source_with_two_snapshots().await;
        let expected = source.head().await;
        let backup = capture_backup(&source).await;
        let paths = TableCatalogObjectPaths::default();
        for case in [
            "table-response-loss",
            "receipt-finalization",
            "control-finalization",
            "completed-response-loss",
        ] {
            let target = BackupTransferHandlerFixture::new(&source.fixture.bucket).await;
            import_backup(&target, &backup).await;
            prepare_references(&target, &backup, None).await;
            let table_path = paths.table_entry_path(
                &target.bucket,
                &Namespace::parse(NAMESPACE).unwrap(),
                &IdentifierSegment::parse(TABLE).unwrap(),
            );
            let intent_path = paths.catalog_restore_intent_path(&target.bucket, case);
            let state_path = paths.catalog_restore_state_path(&target.bucket);
            let fault = RealCatalogBackend::new(EcStoreTableCatalogObjectBackend::new(target.context.object_store()));
            let catalog = ObjectTableCatalogStore::new(fault.clone());
            let pause = match case {
                "table-response-loss" => {
                    fault.fail(META, &table_path, 1, FaultPhase::After);
                    None
                }
                "receipt-finalization" => Some(fault.pause(META, &table_path, 1, FaultPhase::Before)),
                "control-finalization" => {
                    fault.fail(META, &state_path, 4, FaultPhase::Before);
                    None
                }
                "completed-response-loss" => {
                    fault.fail(META, &state_path, 4, FaultPhase::After);
                    None
                }
                _ => unreachable!(),
            };
            let bucket = target.bucket.clone();
            let backup_id = backup.id.clone();
            let task = tokio::spawn(async move {
                catalog
                    .restore_object_catalog_backup_with_id(&bucket, &backup_id, None, false, Some(case))
                    .await
            });
            if let Some(pause) = pause {
                pause.wait().await;
                fault.fail(META, &intent_path, 1, FaultPhase::Before);
                pause.release();
            }
            let failure = bounded(task).await.unwrap().unwrap_err();
            assert!(
                matches!(failure, TableCatalogStoreError::Internal(ref message) if message.contains("I6 injected fault")),
                "{failure:?}"
            );
            fault.assert_consumed();
            let before = inventory(&target.backend, &target.bucket).await;
            assert!(before.contains_key(&(META.into(), table_path.clone())));
            let state: serde_json::Value = serde_json::from_slice(&before[&(META.into(), state_path.clone())].data).unwrap();
            let intent: serde_json::Value = serde_json::from_slice(&before[&(META.into(), intent_path.clone())].data).unwrap();
            assert_eq!(state["restore_epoch"], intent["restore_epoch"]);
            let (expected_state, expected_intent) = match case {
                "table-response-loss" | "receipt-finalization" => ("APPLYING", "PREPARED"),
                "control-finalization" => ("CATALOG_APPLIED", "COMPLETED"),
                "completed-response-loss" => ("COMPLETED", "COMPLETED"),
                _ => unreachable!(),
            };
            assert_eq!(state["state"], expected_state, "restore barrier at {case}");
            assert_eq!(intent["state"], expected_intent, "restore intent at {case}");
            if case != "completed-response-loss" {
                let error = target.catalog.load_table(&target.bucket, NAMESPACE, TABLE).await.unwrap_err();
                let expected_message = format!(
                    "table bucket {} has an interrupted catalog restore epoch {}; retry restore-id {case} with backup-id {}",
                    target.bucket,
                    state["restore_epoch"].as_str().unwrap(),
                    backup.id
                );
                assert!(
                    matches!(error, TableCatalogStoreError::Unavailable(ref message) if message == &expected_message),
                    "restore admission must identify its durable recovery controls: {error:?}"
                );
                let error = table_call(&target, "", Method::GET, serde_json::Value::Null, &RestLoadTableHandler {})
                    .await
                    .unwrap_err();
                assert_eq!(error.code().as_str(), ICEBERG_ERROR_REST);
                assert_eq!(error.message(), Some("table catalog is temporarily unavailable"));
                assert_error(error, StatusCode::SERVICE_UNAVAILABLE, "table catalog is temporarily unavailable");
            } else {
                json_response(
                    table_call(&target, "", Method::GET, serde_json::Value::Null, &RestLoadTableHandler {})
                        .await
                        .unwrap(),
                )
                .await;
            }
            // Keep ECStore and its directories; discard all Catalog and fault-controller state.
            drop(fault);
            let fresh = RealCatalogBackend::new(EcStoreTableCatalogObjectBackend::new(target.context.object_store()));
            let rebuilt = ObjectTableCatalogStore::new(fresh.clone());
            let receipt =
                bounded(rebuilt.restore_object_catalog_backup_with_id(&target.bucket, &backup.id, None, false, Some(case)))
                    .await
                    .unwrap();
            let after = inventory(&target.backend, &target.bucket).await;
            for key in &backup.embedded {
                if let Some(object) = before.get(key) {
                    assert_eq!(after.get(key), Some(object), "recovery rewrote published object {case} {key:?}");
                }
            }
            assert!(
                !fresh
                    .events()
                    .iter()
                    .any(|event| event.object == table_path && event.phase == "inner-started"),
                "retry must not issue another table PUT"
            );
            let completed = complete_controls(&target, &backup, case).await;
            assert_eq!(completed["restore_epoch"], state["restore_epoch"]);
            assert_eq!(rebuilt.load_table(&target.bucket, NAMESPACE, TABLE).await.unwrap().unwrap(), expected);
            source.assert_files(&target.backend).await;
            assert_eq!(source.current_rows(&target.backend, &expected).await, vec![1, 2, 3, 4, 5, 6]);
            let before_replay = inventory(&target.backend, &target.bucket).await;
            let mut replay = json_response(restore_handler(&target, &backup.id, case).await.unwrap()).await;
            assert_eq!(replay["status"], "ALREADY_RESTORED");
            let receipt_json = serde_json::to_value(receipt).unwrap();
            replay["status"] = receipt_json["status"].clone();
            assert_eq!(replay, receipt_json);
            assert_eq!(inventory(&target.backend, &target.bucket).await, before_replay);
        }
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn backup_restore_rejects_missing_changed_references_and_corrupt_artifacts_before_publication() {
    object_mode(async {
        let source = source_with_two_snapshots().await;
        let backup = capture_backup(&source).await;
        let metadata_key = source.head().await.metadata_location;
        let data_key = backup
            .references
            .keys()
            .find(|key| key.ends_with(".parquet"))
            .unwrap()
            .clone();
        for case in [
            "missing-reference",
            "changed-content",
            "changed-identity",
            "corrupt-artifact",
            "wrong-artifact-identity",
            "unowned-config-object",
        ] {
            let target = BackupTransferHandlerFixture::new(&source.fixture.bucket).await;
            if matches!(case, "corrupt-artifact" | "wrong-artifact-identity" | "unowned-config-object") {
                let mut artifact: serde_json::Value = serde_json::from_slice(&backup.artifact).unwrap();
                match case {
                    "corrupt-artifact" => artifact["objects"][0]["sha256"] = "corrupted".into(),
                    "wrong-artifact-identity" => artifact["backup_id"] = "foreign-backup".into(),
                    "unowned-config-object" => {
                        // An unexpected configuration payload cannot bypass the sealed artifact inventory.
                        let objects = artifact["objects"].as_array_mut().unwrap();
                        let mut config = objects[0].clone();
                        config["object"] = "s3tables/catalog/unowned-maintenance-config.json".into();
                        config["data"] = serde_json::json!([123, 125]);
                        config["size_bytes"] = 2.into();
                        config["sha256"] = hex_simd::encode_to_string(Sha256::digest(b"{}"), hex_simd::AsciiCase::Lower).into();
                        objects.push(config);
                    }
                    _ => unreachable!(),
                }
                let before = inventory(&target.backend, &target.bucket).await;
                let error = call_catalog_backup_import_handler(target.operator_request(
                    TABLE_CATALOG_PREFIX,
                    "import",
                    serde_json::to_vec(&artifact).unwrap(),
                ))
                .await
                .unwrap_err();
                assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST), "{case}: {error:?}");
                assert_eq!(inventory(&target.backend, &target.bucket).await, before);
                continue;
            }
            import_backup(&target, &backup).await;
            prepare_references(&target, &backup, (case == "missing-reference").then_some(data_key.as_str())).await;
            if case == "changed-content" {
                let mut bytes = backup.references[&data_key].data.clone();
                bytes[0] ^= 1;
                target
                    .backend
                    .put_object(&target.bucket, &data_key, bytes, TableCatalogPutPrecondition::Any)
                    .await
                    .unwrap();
            }
            if case == "changed-identity" {
                let mut metadata: serde_json::Value = serde_json::from_slice(&backup.references[&metadata_key].data).unwrap();
                metadata["table-uuid"] = Uuid::new_v4().to_string().into();
                target
                    .backend
                    .put_object(
                        &target.bucket,
                        &metadata_key,
                        serde_json::to_vec(&metadata).unwrap(),
                        TableCatalogPutPrecondition::Any,
                    )
                    .await
                    .unwrap();
            }
            let before = inventory(&target.backend, &target.bucket).await;
            let error = restore_handler(&target, &backup.id, case).await.unwrap_err();
            assert!(matches!(error.status_code(), Some(StatusCode::BAD_REQUEST | StatusCode::CONFLICT)));
            assert_eq!(
                inventory(&target.backend, &target.bucket).await,
                before,
                "failed preflight changed objects for {case}"
            );
            assert!(target.catalog.get_table_bucket(&target.bucket).await.unwrap().is_none());
        }
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn backup_restore_corrupt_controls_reject_recovery_without_writes() {
    object_mode(async {
        let source = source_with_two_snapshots().await;
        let backup = capture_backup(&source).await;
        let paths = TableCatalogObjectPaths::default();
        for control in ["state", "intent"] {
            let target = BackupTransferHandlerFixture::new(&source.fixture.bucket).await;
            import_backup(&target, &backup).await;
            prepare_references(&target, &backup, None).await;
            let table_path = paths.table_entry_path(
                &target.bucket,
                &Namespace::parse(NAMESPACE).unwrap(),
                &IdentifierSegment::parse(TABLE).unwrap(),
            );
            let fault = RealCatalogBackend::new(target.backend.clone());
            fault.fail(META, &table_path, 1, FaultPhase::Before);
            ObjectTableCatalogStore::new(fault.clone())
                .restore_object_catalog_backup_with_id(&target.bucket, &backup.id, None, false, Some("corrupt-control"))
                .await
                .expect_err("interrupt actual restore");
            fault.assert_consumed();
            let key = if control == "state" {
                paths.catalog_restore_state_path(&target.bucket)
            } else {
                paths.catalog_restore_intent_path(&target.bucket, "corrupt-control")
            };
            target
                .backend
                .put_object(META, &key, b"{}".to_vec(), TableCatalogPutPrecondition::Any)
                .await
                .unwrap();
            drop(fault);
            let before = inventory(&target.backend, &target.bucket).await;
            let fresh = ObjectTableCatalogStore::new(EcStoreTableCatalogObjectBackend::new(target.context.object_store()));
            assert!(fresh.load_table(&target.bucket, NAMESPACE, TABLE).await.is_err());
            assert!(fresh.list_namespaces(&target.bucket).await.is_err());
            let _error = restore_handler(&target, &backup.id, "corrupt-control").await.unwrap_err();
            assert_eq!(inventory(&target.backend, &target.bucket).await, before);
        }
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn backup_restore_reads_fixed_v1_artifact_without_reencoding() {
    object_mode(async {
        let artifact = include_bytes!("../../../../table_catalog/tests/fixtures/object-catalog-backup-v1.json");
        let backup_id = "83e3024f278b0abeecdbde378068980c3d5c9a2a5ddaa915495d5b398d1d05c2";
        assert_eq!(
            hex_simd::encode_to_string(Sha256::digest(artifact), hex_simd::AsciiCase::Lower),
            "932f8183fd2a57146ef3f9b14115cca101760032bd94bc47230516c54b9d5669"
        );
        let target = BackupTransferHandlerFixture::new("analytics").await;
        json_response(
            call_catalog_backup_import_handler(target.operator_request(TABLE_CATALOG_PREFIX, "import", artifact.to_vec()))
                .await
                .unwrap(),
        )
        .await;
        let restored = json_response(restore_handler(&target, backup_id, "fixed-v1").await.unwrap()).await;
        assert_eq!(restored["status"], "RESTORED");
        let table = target
            .catalog
            .load_table("analytics", NAMESPACE, TABLE)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(table.table_uuid, "table-uuid");
        assert_eq!(table.warehouse_location, "s3://analytics/tables/table-id");
        assert_eq!(table.version_token, "token-v1");
        assert_eq!(table.generation, 1);
        let path = TableCatalogObjectPaths::default().catalog_backup_path("analytics", backup_id);
        assert_eq!(read(&target.backend, META, &path).await.data, artifact);
        let decoded: serde_json::Value = serde_json::from_slice(artifact).unwrap();
        for embedded in decoded["objects"].as_array().unwrap() {
            let bytes: Vec<u8> = serde_json::from_value(embedded["data"].clone()).unwrap();
            assert_eq!(
                read(
                    &target.backend,
                    embedded["bucket"].as_str().unwrap(),
                    embedded["object"].as_str().unwrap()
                )
                .await
                .data,
                bytes
            );
        }
        let before = inventory(&target.backend, &target.bucket).await;
        let replay = json_response(restore_handler(&target, backup_id, "fixed-v1").await.unwrap()).await;
        assert_eq!(replay["status"], "ALREADY_RESTORED");
        assert_eq!(inventory(&target.backend, &target.bucket).await, before);
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn backup_import_rejects_disabled_target_configuration_without_writes() {
    object_mode(async {
        let source = source_with_two_snapshots().await;
        let backup = capture_backup(&source).await;
        let target = BackupTransferHandlerFixture::new(&source.fixture.bucket).await;
        let path = format!("/iceberg/v1/buckets/{}", target.bucket);
        let request =
            table_catalog_request_builder(target.context.clone(), Some(target.root_credentials()), Method::DELETE, &path).build();
        let mut router = matchit::Router::new();
        router.insert("/iceberg/v1/buckets/{warehouse}", ()).unwrap();
        let response = DisableTableBucketHandler {}
            .call(request, router.at(&path).unwrap().params)
            .await
            .unwrap();
        assert_eq!(json_response(response).await["enabled"], false);
        let before = inventory(&target.backend, &target.bucket).await;
        let error = call_catalog_backup_import_handler(target.operator_request(TABLE_CATALOG_PREFIX, "import", backup.artifact))
            .await
            .unwrap_err();
        assert_error(error, StatusCode::BAD_REQUEST, "not table-enabled");
        assert_eq!(inventory(&target.backend, &target.bucket).await, before);
    })
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn prepared_reader_commits_while_candidate_writer_is_actually_queued() {
    object_mode(async {
        let mut scenario = RealScenario::new().await;
        let initial = scenario.head().await;
        let first = scenario.candidate(&initial, 10, &[1, 2]).await;
        let base = scenario.commit(&initial, &first, "reader-base").await;
        let candidate = scenario.candidate(&base, 20, &[3, 4]).await;
        let bucket = scenario.fixture.bucket.clone();
        let manager = scenario
            .fixture
            .lock_manager
            .as_fast_lock_manager()
            .expect("real ECStore FastLockManager");
        let candidate_lock = rustfs_lock::ObjectKey::new(bucket.as_str(), candidate.as_str());
        // ECStore background services retain unrelated locks across idle waits.
        wait_for_catalog_locks_released(&manager, std::slice::from_ref(&candidate_lock), "before prepared-reader discovery")
            .await;
        let publication = TableCommitObjectBackend::trusted(scenario.backend.clone());
        let base_object = TableCatalogObjectBackend::read_object_limited(
            &publication,
            &bucket,
            &base.metadata_location,
            TABLE_METADATA_JSON_MAX_SIZE,
        )
        .await
        .unwrap()
        .unwrap();
        let candidate_object =
            TableCatalogObjectBackend::read_object_limited(&publication, &bucket, &candidate, TABLE_METADATA_JSON_MAX_SIZE)
                .await
                .unwrap()
                .unwrap();
        let base_metadata: serde_json::Value = serde_json::from_slice(&base_object.data).unwrap();
        let candidate_metadata: serde_json::Value = serde_json::from_slice(&candidate_object.data).unwrap();
        let proof = validate_table_snapshot_changes_with_publication(
            &TableSnapshotGraphValidationContext::new(&publication, &bucket, &base),
            Some(&base_metadata),
            &candidate_metadata,
        )
        .await
        .expect("typed proof from the real publication observer");
        let captured = publication.capture_graph_read_set().unwrap();
        for key in [&base.metadata_location, &candidate] {
            let observation = captured
                .objects()
                .iter()
                .find(|object| object.location() == (bucket.as_str(), key.as_str()))
                .expect("base and candidate belong to the captured observer");
            assert!(observation.body_revision().is_some());
        }
        assert!(captured.objects().iter().any(|object| object.location().1.ends_with(".avro")));
        assert!(
            captured
                .objects()
                .iter()
                .any(|object| object.location().1.ends_with(".parquet"))
        );
        let paths = TableCatalogObjectPaths::default();
        let namespace = Namespace::parse(NAMESPACE).unwrap();
        let table = IdentifierSegment::parse(TABLE).unwrap();
        let publication_locks = [
            (
                rustfs_lock::ObjectKey::new(META, paths.catalog_backup_global_fence_lock_path()),
                rustfs_lock::LockMode::Shared,
            ),
            (
                rustfs_lock::ObjectKey::new(META, paths.catalog_backup_fence_lock_path(&bucket)),
                rustfs_lock::LockMode::Shared,
            ),
            (
                rustfs_lock::ObjectKey::new(bucket.as_str(), default_table_bucket_publication_lock_path()),
                rustfs_lock::LockMode::Exclusive,
            ),
            (
                rustfs_lock::ObjectKey::new(bucket.as_str(), default_table_publication_lock_path(&namespace, &table)),
                rustfs_lock::LockMode::Exclusive,
            ),
            (
                rustfs_lock::ObjectKey::new(META, paths.table_entry_path(&bucket, &namespace, &table)),
                rustfs_lock::LockMode::Exclusive,
            ),
        ];
        let mut released_keys = captured
            .objects()
            .iter()
            .map(|observation| {
                let (bucket, object) = observation.location();
                rustfs_lock::ObjectKey::new(bucket, object)
            })
            .collect::<Vec<_>>();
        released_keys.extend(publication_locks.iter().map(|(key, _)| key.clone()));
        wait_for_catalog_locks_released(&manager, &released_keys, "before prepared-reader commit").await;

        // Preparation acquires the actual object guards before this unlocked HEAD.
        let pause = scenario.backend.pause_metadata_unlocked(&bucket, &candidate);
        let events_before_commit = scenario.backend.events().len();
        let catalog = scenario.catalog.clone();
        let request = scenario.commit_request(&base, &candidate, "queued-reader");
        let commit_publication = publication.clone();
        let mut commit = tokio::spawn(async move {
            catalog
                .commit_table_with_validated_graph(request, proof, &commit_publication)
                .await
        });
        tokio::select! {
            () = pause.wait() => {},
            result = &mut commit => panic!("commit ended before candidate HEAD boundary: {result:?}"),
        }
        assert!(TableCommitPublication::holds_table_bucket(&publication, &bucket));
        assert_catalog_lock_mode(&manager, &candidate_lock, rustfs_lock::LockMode::Shared);
        for (key, mode) in &publication_locks {
            assert_catalog_lock_mode(&manager, key, *mode);
        }
        let waiting_before = manager.current_lock_counts().1;
        let writer_backend = scenario.fixture.backend.clone();
        let writer_bucket = bucket.clone();
        let writer_key = candidate.clone();
        let writer_publication = publication.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(async move {
            started_tx.send(()).expect("writer start notification");
            let guard = writer_backend
                .acquire_write_lock(&writer_bucket, &writer_key)
                .await
                .expect("candidate writer lock");
            assert!(!guard.is_lock_lost(), "candidate writer acquired a live guard");
            assert!(!TableCommitPublication::holds_table_bucket(&writer_publication, &writer_bucket));
            assert!(!TableCommitPublication::holds_table(
                &writer_publication,
                &writer_bucket,
                NAMESPACE,
                TABLE
            ));
            acquired_tx.send(()).expect("writer acquired notification");
            bounded(release_rx).await.expect("writer release notification");
            drop(guard);
        });
        bounded(started_rx).await.expect("writer started");
        bounded(async {
            while manager.current_lock_counts().1 <= waiting_before {
                assert!(!writer.is_finished(), "writer must enter the actual candidate lock queue");
                tokio::task::yield_now().await;
            }
        })
        .await;
        let queued_counts = manager.current_lock_counts();
        assert!(queued_counts.1 > waiting_before);
        assert_catalog_lock_mode(&manager, &candidate_lock, rustfs_lock::LockMode::Shared);
        assert!(!writer.is_finished());
        assert!(!commit.is_finished());

        pause.release();
        let result = tokio::time::timeout(WAIT, commit)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "prepared-reader commit: {error}; lock counts {:?}; events {:?}",
                    manager.current_lock_counts(),
                    scenario.backend.events()
                )
            })
            .expect("commit task joined")
            .expect("prepared reads must complete with the candidate writer queued");
        tokio::time::timeout(WAIT, acquired_rx)
            .await
            .expect("candidate writer must acquire after publication completed")
            .expect("writer acquired after publication completed");
        assert_catalog_lock_mode(&manager, &candidate_lock, rustfs_lock::LockMode::Exclusive);
        assert!(!TableCommitPublication::holds_table_bucket(&publication, &bucket));
        assert!(matches!(
            TableCommitPublication::prepared_object_reader(&publication),
            Err(TableCatalogStoreError::Conflict(_))
        ));
        release_tx.send(()).expect("release actual candidate writer");
        tokio::time::timeout(WAIT, writer)
            .await
            .expect("candidate writer must release its guard")
            .expect("candidate writer task joined");
        wait_for_catalog_locks_released(&manager, &released_keys, "after prepared-reader writer release").await;
        let probe = tokio::time::timeout(WAIT, scenario.fixture.backend.acquire_write_lock(&bucket, &candidate))
            .await
            .expect("released candidate accepts another writer")
            .expect("candidate lock probe");
        assert!(!probe.is_lock_lost(), "candidate probe acquired a live guard");
        assert_catalog_lock_mode(&manager, &candidate_lock, rustfs_lock::LockMode::Exclusive);
        drop(probe);
        wait_for_catalog_locks_released(&manager, &released_keys, "after candidate lock probe release").await;
        scenario.backend.assert_consumed();
        let events = scenario.backend.events();
        let prepared_reads = &events[events_before_commit..];
        for (key, expected_reads) in [(&base.metadata_location, 1), (&candidate, 2)] {
            assert_eq!(
                prepared_reads
                    .iter()
                    .filter(|event| {
                        event.method == "read_object_unlocked_limited"
                            && event.bucket == bucket
                            && event.object == *key
                            && event.phase == "inner-completed"
                    })
                    .count(),
                expected_reads,
                "metadata preparation and exact candidate verification use borrowed object guards for {key}"
            );
        }
        let candidate_heads = events
            .iter()
            .filter(|event| event.method == "object_metadata_unlocked" && event.object == candidate)
            .collect::<Vec<_>>();
        assert!(candidate_heads.iter().any(|event| event.phase == "inner-completed"));
        assert_eq!(candidate_heads.iter().filter(|event| event.phase == "arrived").count(), 1);
        assert_eq!(candidate_heads.iter().filter(|event| event.phase == "released").count(), 1);
        assert_eq!(result.table.metadata_location, candidate);
        assert_eq!(result.table.generation, base.generation + 1);
        assert_eq!(result.commit_log.status, CommitLogStatus::Committed);
        assert_eq!(scenario.head().await, result.table);
        assert_eq!(read(&scenario.backend, &bucket, &base.metadata_location).await, base_object);
        assert_eq!(read(&scenario.backend, &bucket, &candidate).await, candidate_object);
        scenario.assert_files(&scenario.backend).await;
        assert_eq!(scenario.current_rows(&scenario.backend, &result.table).await, vec![1, 2, 3, 4]);
    })
    .await;
}
