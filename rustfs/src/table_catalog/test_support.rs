// Copyright 2024 RustFS Team
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

//! Shared table-catalog test fixtures (backlog#1837).
//!
//! Pure data constructors for Iceberg metadata JSON and avro
//! manifest-list/manifest bytes, shared by the store-level tests
//! (`table_catalog/tests.rs`) and the admin handler tests
//! (`admin/handlers/table_catalog/tests.rs`). The parameterized admin
//! variants are canonical; the store tests wrap them with their historical
//! fixed values (sequence 7 / snapshot 20), which keeps every produced byte
//! identical to the pre-extraction fixtures.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use time::OffsetDateTime;

use super::{
    StrongTableCatalogRuntime, TableCatalogObject, TableCatalogObjectBackend, TableCatalogObjectMetadata,
    TableCatalogPutPrecondition, TableCatalogStoreError, TableCatalogStoreResult, TableCommitPublication,
};

pub(crate) fn table_metadata_json(table_uuid: &str, location: &str) -> serde_json::Value {
    serde_json::json!({
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": location,
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

pub(crate) fn manifest_list_avro_bytes(manifest_paths: &[&str], sequence_number: i64, snapshot_id: i64) -> Vec<u8> {
    let manifests = manifest_paths
        .iter()
        .map(|manifest_path| (*manifest_path, 0, sequence_number, snapshot_id))
        .collect::<Vec<_>>();
    manifest_list_avro_entries_with_partition_specs(&manifests)
}

pub(crate) fn manifest_list_avro_entries(manifests: &[(&str, i64, i64)]) -> Vec<u8> {
    let manifests = manifests
        .iter()
        .map(|(manifest_path, sequence_number, snapshot_id)| (*manifest_path, 0, *sequence_number, *snapshot_id))
        .collect::<Vec<_>>();
    manifest_list_avro_entries_with_partition_specs(&manifests)
}

pub(crate) fn manifest_list_avro_entries_with_partition_specs(manifests: &[(&str, i32, i64, i64)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
            {
              "type": "record",
              "name": "manifest_file",
              "fields": [
                {"name": "manifest_path", "type": "string"},
                {"name": "manifest_length", "type": "long"},
                {"name": "partition_spec_id", "type": "int"},
                {"name": "content", "type": "int"},
                {"name": "sequence_number", "type": "long"},
                {"name": "min_sequence_number", "type": "long"},
                {"name": "added_snapshot_id", "type": "long"},
                {"name": "added_files_count", "type": "int"},
                {"name": "existing_files_count", "type": "int"},
                {"name": "deleted_files_count", "type": "int"},
                {"name": "added_rows_count", "type": "long"},
                {"name": "existing_rows_count", "type": "long"},
                {"name": "deleted_rows_count", "type": "long"}
              ]
            }
            "#,
    )
    .expect("manifest list avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("manifest list writer should initialize");
    for (manifest_path, partition_spec_id, sequence_number, snapshot_id) in manifests {
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                (
                    "manifest_path".to_string(),
                    apache_avro::types::Value::String((*manifest_path).to_string()),
                ),
                ("manifest_length".to_string(), apache_avro::types::Value::Long(1)),
                ("partition_spec_id".to_string(), apache_avro::types::Value::Int(*partition_spec_id)),
                ("content".to_string(), apache_avro::types::Value::Int(0)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                ("min_sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                ("added_snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ("added_files_count".to_string(), apache_avro::types::Value::Int(1)),
                ("existing_files_count".to_string(), apache_avro::types::Value::Int(0)),
                ("deleted_files_count".to_string(), apache_avro::types::Value::Int(0)),
                ("added_rows_count".to_string(), apache_avro::types::Value::Long(1)),
                ("existing_rows_count".to_string(), apache_avro::types::Value::Long(0)),
                ("deleted_rows_count".to_string(), apache_avro::types::Value::Long(0)),
            ]))
            .expect("manifest list record should append");
    }
    writer.into_inner().expect("manifest list avro bytes should flush")
}

pub(crate) fn manifest_avro_bytes(files: &[(&str, i32, i32, i64, i64)]) -> Vec<u8> {
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
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"}
                    ]
                  }
                }
              ]
            }
            "#,
    )
    .expect("manifest avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("manifest writer should initialize");
    for (file_path, content, status, snapshot_id, sequence_number) in files {
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(*status)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                ("file_sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(*content)),
                        ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                        ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                        ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                    ]),
                ),
            ]))
            .expect("manifest record should append");
    }
    writer.into_inner().expect("manifest avro bytes should flush")
}

pub(crate) fn nullable_long(value: Option<i64>) -> apache_avro::types::Value {
    match value {
        Some(value) => apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Long(value))),
        None => apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
    }
}

pub(crate) fn manifest_avro_bytes_with_nullable_sequences(files: &[(&str, i32, i32, i64, Option<i64>)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
            {
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": "long"},
                {"name": "sequence_number", "type": ["null", "long"], "default": null},
                {"name": "file_sequence_number", "type": ["null", "long"], "default": null},
                {
                  "name": "data_file",
                  "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                      {"name": "content", "type": "int"},
                      {"name": "file_path", "type": "string"},
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"}
                    ]
                  }
                }
              ]
            }
            "#,
    )
    .expect("manifest avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).expect("manifest writer should initialize");
    for (file_path, content, status, snapshot_id, sequence_number) in files {
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(*status)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ("sequence_number".to_string(), nullable_long(*sequence_number)),
                ("file_sequence_number".to_string(), nullable_long(*sequence_number)),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(*content)),
                        ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                        ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                        ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                    ]),
                ),
            ]))
            .expect("manifest record should append");
    }
    writer.into_inner().expect("manifest avro bytes should flush")
}

// --- Stateful object backend shared by the store and admin handler tests
// (backlog#1837 PR2). Superset instrumentation lands here incrementally;
// this is the store-side fake moved verbatim.

#[derive(Clone, Default)]
pub(crate) struct TestCatalogObjectBackend {
    pub(crate) state: Arc<tokio::sync::Mutex<TestCatalogObjectState>>,
    pub(crate) locks: TestCatalogObjectLocks,
    pub(crate) strong_runtime: Option<StrongTableCatalogRuntime>,
}

pub(crate) type TestCatalogObjectLockKey = (String, String);
pub(crate) type TestCatalogObjectLock = Arc<tokio::sync::RwLock<()>>;
pub(crate) type TestCatalogObjectLocks = Arc<tokio::sync::Mutex<BTreeMap<TestCatalogObjectLockKey, TestCatalogObjectLock>>>;

#[derive(Clone, Default)]
pub(crate) struct TestCatalogObjectPause {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl TestCatalogObjectPause {
    pub(crate) async fn wait_started(&self) {
        self.started.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Clone)]
pub(crate) struct BlockingObjectPublication {
    backend: TestCatalogObjectBackend,
    object: String,
    started: Arc<tokio::sync::Notify>,
    guard: Arc<parking_lot::Mutex<Option<Box<dyn Send>>>>,
}

impl BlockingObjectPublication {
    pub(crate) fn new(backend: TestCatalogObjectBackend, object: impl Into<String>) -> Self {
        Self {
            backend,
            object: object.into(),
            started: Arc::new(tokio::sync::Notify::new()),
            guard: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    pub(crate) async fn wait_started(&self) {
        self.started.notified().await;
    }
}

#[derive(Default)]
pub(crate) struct UnserializedTestPublication;

#[async_trait::async_trait]
impl TableCommitPublication for UnserializedTestPublication {
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
        true
    }

    fn complete(&self) {}
}

#[async_trait::async_trait]
impl TableCommitPublication for BlockingObjectPublication {
    async fn begin_table_bucket(&self, _table_bucket: &str) -> TableCatalogStoreResult<()> {
        Ok(())
    }

    async fn prepare(&self, table_bucket: &str, _namespace: &str, _table: &str) -> TableCatalogStoreResult<()> {
        self.started.notify_one();
        let guard = self.backend.acquire_read_lock(table_bucket, &self.object).await?;
        *self.guard.lock() = Some(guard);
        Ok(())
    }

    fn holds_table_bucket(&self, _table_bucket: &str) -> bool {
        true
    }

    fn holds_table(&self, _table_bucket: &str, _namespace: &str, _table: &str) -> bool {
        self.guard.lock().is_some()
    }

    fn complete(&self) {
        drop(self.guard.lock().take());
    }
}

#[derive(Default)]
pub(crate) struct TestCatalogObjectState {
    pub(crate) objects: BTreeMap<(String, String), TestCatalogObjectRecord>,
    pub(crate) etagless_objects: BTreeSet<(String, String)>,
    pub(crate) fail_read_attempts: BTreeMap<(String, String), BTreeSet<usize>>,
    pub(crate) pause_before_read_attempts: BTreeMap<(String, String), BTreeMap<usize, TestCatalogObjectPause>>,
    pub(crate) pause_read_attempts: BTreeMap<(String, String), BTreeMap<usize, TestCatalogObjectPause>>,
    pub(crate) read_attempts: BTreeMap<(String, String), usize>,
    pub(crate) read_limits: Vec<((String, String), usize)>,
    pub(crate) fail_put_attempts: BTreeMap<(String, String), BTreeSet<usize>>,
    pub(crate) fail_after_put_attempts: BTreeMap<(String, String), BTreeSet<usize>>,
    pub(crate) pause_put_attempts: BTreeMap<(String, String), BTreeMap<usize, TestCatalogObjectPause>>,
    pub(crate) fail_delete_attempts: BTreeMap<(String, String), BTreeSet<usize>>,
    pub(crate) fail_after_delete_attempts: BTreeMap<(String, String), BTreeSet<usize>>,
    pub(crate) put_attempts: BTreeMap<(String, String), usize>,
    pub(crate) delete_attempts: BTreeMap<(String, String), usize>,
    pub(crate) write_lock_acquisitions: BTreeMap<(String, String), usize>,
    pub(crate) read_lock_acquisitions: BTreeMap<(String, String), usize>,
    pub(crate) read_calls: usize,
    pub(crate) metadata_calls: usize,
    pub(crate) list_calls: usize,
    pub(crate) next_etag: u64,
}

#[derive(Clone)]
pub(crate) struct TestCatalogObjectRecord {
    pub(crate) data: Vec<u8>,
    pub(crate) etag: String,
    pub(crate) mod_time: Option<OffsetDateTime>,
}

impl TestCatalogObjectBackend {
    pub(crate) async fn seed_object(&self, bucket: &str, object: &str, data: Vec<u8>) {
        self.seed_object_with_mod_time(bucket, object, data, Some(OffsetDateTime::UNIX_EPOCH))
            .await;
    }

    pub(crate) async fn seed_object_with_mod_time(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        mod_time: Option<OffsetDateTime>,
    ) {
        let mut state = self.state.lock().await;
        let etag = state.next_etag();
        state
            .objects
            .insert((bucket.to_string(), object.to_string()), TestCatalogObjectRecord { data, etag, mod_time });
    }

    pub(crate) async fn fail_put_attempt(&self, bucket: &str, object: &str, attempt: usize) {
        let mut state = self.state.lock().await;
        state
            .fail_put_attempts
            .entry((bucket.to_string(), object.to_string()))
            .or_default()
            .insert(attempt);
    }

    pub(crate) async fn fail_delete_attempt(&self, bucket: &str, object: &str, attempt: usize) {
        let mut state = self.state.lock().await;
        state
            .fail_delete_attempts
            .entry((bucket.to_string(), object.to_string()))
            .or_default()
            .insert(attempt);
    }

    pub(crate) async fn list_call_count(&self) -> usize {
        self.state.lock().await.list_calls
    }

    pub(crate) async fn read_call_count(&self) -> usize {
        self.state.lock().await.read_calls
    }

    pub(crate) async fn metadata_call_count(&self) -> usize {
        self.state.lock().await.metadata_calls
    }

    pub(crate) async fn reset_call_counts(&self) {
        let mut state = self.state.lock().await;
        state.read_calls = 0;
        state.metadata_calls = 0;
        state.list_calls = 0;
    }

    pub(crate) async fn write_lock_acquisition_count(&self, bucket: &str, object: &str) -> usize {
        self.state
            .lock()
            .await
            .write_lock_acquisitions
            .get(&(bucket.to_string(), object.to_string()))
            .copied()
            .unwrap_or_default()
    }

    pub(crate) async fn read_lock_acquisition_count(&self, bucket: &str, object: &str) -> usize {
        self.state
            .lock()
            .await
            .read_lock_acquisitions
            .get(&(bucket.to_string(), object.to_string()))
            .copied()
            .unwrap_or_default()
    }

    pub(crate) async fn fail_next_read(&self, bucket: &str, object: &str) {
        let mut state = self.state.lock().await;
        let key = (bucket.to_string(), object.to_string());
        let next_attempt = state.read_attempts.get(&key).copied().unwrap_or_default() + 1;
        state.fail_read_attempts.entry(key).or_default().insert(next_attempt);
    }

    pub(crate) async fn pause_next_read(&self, bucket: &str, object: &str) -> TestCatalogObjectPause {
        let mut state = self.state.lock().await;
        let key = (bucket.to_string(), object.to_string());
        let next_attempt = state.read_attempts.get(&key).copied().unwrap_or_default() + 1;
        let pause = TestCatalogObjectPause::default();
        state
            .pause_read_attempts
            .entry(key)
            .or_default()
            .insert(next_attempt, pause.clone());
        pause
    }

    pub(crate) async fn pause_before_next_read(&self, bucket: &str, object: &str) -> TestCatalogObjectPause {
        let mut state = self.state.lock().await;
        let key = (bucket.to_string(), object.to_string());
        let next_attempt = state.read_attempts.get(&key).copied().unwrap_or_default() + 1;
        let pause = TestCatalogObjectPause::default();
        state
            .pause_before_read_attempts
            .entry(key)
            .or_default()
            .insert(next_attempt, pause.clone());
        pause
    }

    pub(crate) async fn omit_etag_for_object(&self, bucket: &str, object: &str) {
        self.state
            .lock()
            .await
            .etagless_objects
            .insert((bucket.to_string(), object.to_string()));
    }

    pub(crate) async fn last_read_limit(&self, bucket: &str, object: &str) -> Option<usize> {
        let key = (bucket.to_string(), object.to_string());
        self.state
            .lock()
            .await
            .read_limits
            .iter()
            .rev()
            .find_map(|(read_key, limit)| (read_key == &key).then_some(*limit))
    }

    pub(crate) async fn fail_next_put(&self, bucket: &str, object: &str) {
        let mut state = self.state.lock().await;
        let key = (bucket.to_string(), object.to_string());
        let next_attempt = state.put_attempts.get(&key).copied().unwrap_or_default() + 1;
        state.fail_put_attempts.entry(key).or_default().insert(next_attempt);
    }

    pub(crate) async fn fail_after_next_put(&self, bucket: &str, object: &str) {
        let mut state = self.state.lock().await;
        let key = (bucket.to_string(), object.to_string());
        let next_attempt = state.put_attempts.get(&key).copied().unwrap_or_default() + 1;
        state.fail_after_put_attempts.entry(key).or_default().insert(next_attempt);
    }

    pub(crate) async fn fail_after_next_delete(&self, bucket: &str, object: &str) {
        let mut state = self.state.lock().await;
        let key = (bucket.to_string(), object.to_string());
        let next_attempt = state.delete_attempts.get(&key).copied().unwrap_or_default() + 1;
        state.fail_after_delete_attempts.entry(key).or_default().insert(next_attempt);
    }

    pub(crate) async fn pause_next_put(&self, bucket: &str, object: &str) -> TestCatalogObjectPause {
        let mut state = self.state.lock().await;
        let key = (bucket.to_string(), object.to_string());
        let next_attempt = state.put_attempts.get(&key).copied().unwrap_or_default() + 1;
        let pause = TestCatalogObjectPause::default();
        state
            .pause_put_attempts
            .entry(key)
            .or_default()
            .insert(next_attempt, pause.clone());
        pause
    }

    pub(crate) async fn put_attempt_count(&self, bucket: &str, object: &str) -> usize {
        self.state
            .lock()
            .await
            .put_attempts
            .get(&(bucket.to_string(), object.to_string()))
            .copied()
            .unwrap_or_default()
    }
}

impl TestCatalogObjectState {
    pub(crate) fn next_etag(&mut self) -> String {
        self.next_etag += 1;
        format!("etag-{}", self.next_etag)
    }
}

#[async_trait::async_trait]
impl TableCatalogObjectBackend for TestCatalogObjectBackend {
    fn strong_catalog_runtime(&self) -> Option<StrongTableCatalogRuntime> {
        self.strong_runtime.clone()
    }

    async fn read_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        let key = (bucket.to_string(), object.to_string());
        let (attempt, pause_before) = {
            let mut state = self.state.lock().await;
            state.read_calls += 1;
            let attempt = {
                let attempts = state.read_attempts.entry(key.clone()).or_default();
                *attempts += 1;
                *attempts
            };
            if state
                .fail_read_attempts
                .get(&key)
                .is_some_and(|attempts| attempts.contains(&attempt))
            {
                return Err(TableCatalogStoreError::Internal(format!(
                    "injected read failure for {object} attempt {attempt}"
                )));
            }
            let pause = state
                .pause_before_read_attempts
                .get_mut(&key)
                .and_then(|attempts| attempts.remove(&attempt));
            (attempt, pause)
        };
        if let Some(pause) = pause_before {
            pause.started.notify_one();
            pause.release.notified().await;
        }
        let (result, pause) = {
            let mut state = self.state.lock().await;
            let etagless = state.etagless_objects.contains(&key);
            let result = state.objects.get(&key).map(|record| TableCatalogObject {
                data: record.data.clone(),
                etag: (!etagless).then(|| record.etag.clone()),
                mod_time: record.mod_time,
            });
            let pause = state
                .pause_read_attempts
                .get_mut(&key)
                .and_then(|attempts| attempts.remove(&attempt));
            (result, pause)
        };
        if let Some(pause) = pause {
            pause.started.notify_one();
            pause.release.notified().await;
        }
        Ok(result)
    }

    async fn read_object_limited(
        &self,
        bucket: &str,
        object: &str,
        max_size: usize,
    ) -> TableCatalogStoreResult<Option<TableCatalogObject>> {
        self.state
            .lock()
            .await
            .read_limits
            .push(((bucket.to_string(), object.to_string()), max_size));
        let result = self.read_object(bucket, object).await?;
        if result.as_ref().is_some_and(|object| object.data.len() > max_size) {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog object {bucket}/{object} exceeds the maximum size of {max_size} bytes"
            )));
        }
        Ok(result)
    }

    async fn object_metadata(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Option<TableCatalogObjectMetadata>> {
        let mut state = self.state.lock().await;
        state.metadata_calls += 1;
        let key = (bucket.to_string(), object.to_string());
        let etagless = state.etagless_objects.contains(&key);
        Ok(state.objects.get(&key).map(|record| TableCatalogObjectMetadata {
            etag: (!etagless).then(|| record.etag.clone()),
            mod_time: record.mod_time,
        }))
    }

    async fn object_exists(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<bool> {
        let state = self.state.lock().await;
        Ok(state.objects.contains_key(&(bucket.to_string(), object.to_string())))
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        precondition: TableCatalogPutPrecondition,
    ) -> TableCatalogStoreResult<()> {
        let key = (bucket.to_string(), object.to_string());
        let (attempt, pause) = {
            let mut state = self.state.lock().await;
            let attempt = {
                let attempts = state.put_attempts.entry(key.clone()).or_default();
                *attempts += 1;
                *attempts
            };
            if state
                .fail_put_attempts
                .get(&key)
                .is_some_and(|attempts| attempts.contains(&attempt))
            {
                return Err(TableCatalogStoreError::Internal(format!(
                    "injected put failure for {object} attempt {attempt}"
                )));
            }
            let pause = state
                .pause_put_attempts
                .get_mut(&key)
                .and_then(|attempts| attempts.remove(&attempt));
            (attempt, pause)
        };
        if let Some(pause) = pause {
            pause.started.notify_one();
            pause.release.notified().await;
        }

        let mut state = self.state.lock().await;
        match precondition {
            TableCatalogPutPrecondition::IfAbsent if state.objects.contains_key(&key) => {
                return Err(TableCatalogStoreError::Conflict(format!("object already exists: {object}")));
            }
            TableCatalogPutPrecondition::IfMatch(expected) => {
                let Some(current) = state.objects.get(&key) else {
                    return Err(TableCatalogStoreError::Conflict(format!("object is missing: {object}")));
                };
                if current.etag != expected {
                    return Err(TableCatalogStoreError::Conflict(format!("object changed: {object}")));
                }
            }
            _ => {}
        }

        let etag = state.next_etag();
        state.objects.insert(
            key.clone(),
            TestCatalogObjectRecord {
                data,
                etag,
                mod_time: Some(OffsetDateTime::now_utc()),
            },
        );
        if state
            .fail_after_put_attempts
            .get(&key)
            .is_some_and(|attempts| attempts.contains(&attempt))
        {
            return Err(TableCatalogStoreError::Internal(format!(
                "injected post-commit put failure for {object} attempt {attempt}"
            )));
        }
        Ok(())
    }

    async fn delete_object(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<()> {
        let mut state = self.state.lock().await;
        let key = (bucket.to_string(), object.to_string());
        let attempt = {
            let attempts = state.delete_attempts.entry(key.clone()).or_default();
            *attempts += 1;
            *attempts
        };
        if state
            .fail_delete_attempts
            .get(&key)
            .is_some_and(|attempts| attempts.contains(&attempt))
        {
            return Err(TableCatalogStoreError::Internal(format!(
                "injected delete failure for {object} attempt {attempt}"
            )));
        }
        state.objects.remove(&key);
        if state
            .fail_after_delete_attempts
            .get(&key)
            .is_some_and(|attempts| attempts.contains(&attempt))
        {
            return Err(TableCatalogStoreError::Internal(format!(
                "injected post-commit delete failure for {object} attempt {attempt}"
            )));
        }
        Ok(())
    }

    async fn list_objects(&self, bucket: &str, prefix: &str) -> TableCatalogStoreResult<Vec<String>> {
        let mut state = self.state.lock().await;
        state.list_calls += 1;
        Ok(state
            .objects
            .keys()
            .filter(|(entry_bucket, object)| entry_bucket == bucket && object.starts_with(prefix))
            .map(|(_, object)| object.clone())
            .collect())
    }

    async fn acquire_write_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Box<dyn Send>> {
        {
            let mut state = self.state.lock().await;
            *state
                .write_lock_acquisitions
                .entry((bucket.to_string(), object.to_string()))
                .or_default() += 1;
        }
        let lock = {
            let mut locks = self.locks.lock().await;
            locks
                .entry((bucket.to_string(), object.to_string()))
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::RwLock::new(())))
                .clone()
        };
        Ok(Box::new(lock.write_owned().await))
    }

    async fn acquire_read_lock(&self, bucket: &str, object: &str) -> TableCatalogStoreResult<Box<dyn Send>> {
        {
            let mut state = self.state.lock().await;
            *state
                .read_lock_acquisitions
                .entry((bucket.to_string(), object.to_string()))
                .or_default() += 1;
        }
        let lock = {
            let mut locks = self.locks.lock().await;
            locks
                .entry((bucket.to_string(), object.to_string()))
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::RwLock::new(())))
                .clone()
        };
        Ok(Box::new(lock.read_owned().await))
    }
}
