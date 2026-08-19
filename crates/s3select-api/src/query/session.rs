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

use crate::SelectObjectSnapshot;
use crate::query::Context;
use crate::{QueryError, QueryResult, object_store::EcObjectStore};
use datafusion::{
    arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    common::DataFusionError,
    execution::{SessionStateBuilder, config::SessionConfig, context::SessionState, runtime_env::RuntimeEnvBuilder},
    object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path},
    parquet::arrow::ArrowWriter,
    prelude::SessionContext,
};
use parking_lot::Mutex;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU8, Ordering},
};
use tokio::{
    sync::OwnedSemaphorePermit,
    task::AbortHandle,
    time::{Instant, sleep_until},
};
use tracing::error;

pub type QueryExecutionGuard = Arc<OwnedSemaphorePermit>;

/// A one-shot query admission reservation handed from the request boundary to
/// the dispatcher that owns the corresponding concurrency semaphore.
pub struct QueryAdmission {
    query_guard: Option<QueryExecutionGuard>,
}

impl QueryAdmission {
    pub fn new(query_guard: QueryExecutionGuard) -> Self {
        Self {
            query_guard: Some(query_guard),
        }
    }

    pub fn into_query_guard(mut self) -> Option<QueryExecutionGuard> {
        self.query_guard.take()
    }

    pub(crate) fn unmanaged() -> Self {
        Self { query_guard: None }
    }
}

#[derive(Clone, Default)]
pub struct QueryExecutionOwner {
    identity: Arc<()>,
}

impl QueryExecutionOwner {
    pub fn new() -> Self {
        Self { identity: Arc::new(()) }
    }
}

#[derive(Clone)]
pub struct QueryExecutionTracker {
    inner: Arc<QueryExecutionTrackerInner>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryExecutionStatus {
    Active,
    Finished,
    TimedOut,
}

const EXECUTION_SETTING_UP: u8 = 0;
const EXECUTION_ADMITTED: u8 = 1;
const EXECUTION_PLANNING: u8 = 2;
const EXECUTION_PLANNED: u8 = 3;
const EXECUTION_STARTING: u8 = 4;
const EXECUTION_RUNNING: u8 = 5;
const EXECUTION_FINISHED: u8 = 6;
const EXECUTION_TIMED_OUT: u8 = 7;

struct QueryExecutionTrackerInner {
    owner_identity: Arc<()>,
    query_guard: Mutex<Option<QueryExecutionGuard>>,
    deadline: Instant,
    timeout_seconds: u64,
    state: AtomicU8,
    deadline_task: Mutex<Option<AbortHandle>>,
}

impl QueryExecutionTracker {
    pub fn new(owner: &QueryExecutionOwner, query_guard: QueryExecutionGuard, deadline: Instant, timeout_seconds: u64) -> Self {
        let inner = Arc::new(QueryExecutionTrackerInner {
            owner_identity: Arc::clone(&owner.identity),
            query_guard: Mutex::new(Some(query_guard)),
            deadline,
            timeout_seconds,
            state: AtomicU8::new(EXECUTION_SETTING_UP),
            deadline_task: Mutex::new(None),
        });
        let deadline_inner = Arc::downgrade(&inner);
        let deadline_task = tokio::spawn(async move {
            sleep_until(deadline).await;
            if let Some(inner) = Weak::upgrade(&deadline_inner) {
                inner.expire_at_deadline();
            }
        });
        *inner.deadline_task.lock() = Some(deadline_task.abort_handle());

        Self { inner }
    }

    pub fn deadline(&self) -> Instant {
        self.inner.deadline
    }

    pub fn timeout_seconds(&self) -> u64 {
        self.inner.timeout_seconds
    }

    pub fn status(&self) -> QueryExecutionStatus {
        match self.inner.state.load(Ordering::Acquire) {
            EXECUTION_TIMED_OUT => QueryExecutionStatus::TimedOut,
            state if state < EXECUTION_FINISHED => QueryExecutionStatus::Active,
            _ => QueryExecutionStatus::Finished,
        }
    }

    pub fn is_owned_by(&self, owner: &QueryExecutionOwner) -> bool {
        Arc::ptr_eq(&self.inner.owner_identity, &owner.identity)
    }

    pub(crate) fn is_same_execution(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn mark_admitted(&self, owner: &QueryExecutionOwner) -> bool {
        self.transition(owner, EXECUTION_SETTING_UP, EXECUTION_ADMITTED)
    }

    pub fn claim_planning(&self, owner: &QueryExecutionOwner) -> bool {
        self.transition(owner, EXECUTION_ADMITTED, EXECUTION_PLANNING)
    }

    pub fn mark_planned(&self, owner: &QueryExecutionOwner) -> bool {
        self.transition(owner, EXECUTION_PLANNING, EXECUTION_PLANNED)
    }

    pub fn claim_execution(&self, owner: &QueryExecutionOwner) -> bool {
        self.transition(owner, EXECUTION_PLANNED, EXECUTION_STARTING)
    }

    pub fn mark_running(&self, owner: &QueryExecutionOwner) -> bool {
        self.transition(owner, EXECUTION_STARTING, EXECUTION_RUNNING)
    }

    pub fn handoff_deadline(&self, owner: &QueryExecutionOwner) {
        if !self.is_owned_by(owner) || self.inner.state.load(Ordering::Acquire) != EXECUTION_RUNNING {
            return;
        }
        if let Some(deadline_task) = self.inner.deadline_task.lock().take() {
            deadline_task.abort();
        }
    }

    pub fn finish(&self, owner: &QueryExecutionOwner) {
        if self.is_owned_by(owner) {
            self.inner.finish();
        }
    }

    pub fn expire(&self, owner: &QueryExecutionOwner) {
        if self.is_owned_by(owner) {
            self.inner.expire();
        }
    }

    pub(crate) fn query_guard(&self) -> Option<QueryExecutionGuard> {
        let query_guard = self.inner.query_guard.lock();
        if Instant::now() >= self.inner.deadline || self.status() != QueryExecutionStatus::Active {
            return None;
        }
        query_guard.clone()
    }

    fn transition(&self, owner: &QueryExecutionOwner, from: u8, to: u8) -> bool {
        self.is_owned_by(owner)
            && self
                .inner
                .state
                .compare_exchange(from, to, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }
}

impl QueryExecutionTrackerInner {
    fn finish(&self) {
        self.state.fetch_max(EXECUTION_FINISHED, Ordering::AcqRel);
        self.release();
    }

    fn expire(&self) {
        self.mark_timed_out();
        self.release();
    }

    fn expire_at_deadline(&self) {
        if self
            .mark_timed_out()
            .is_some_and(|state| matches!(state, EXECUTION_ADMITTED | EXECUTION_PLANNED))
        {
            self.release();
        }
    }

    fn mark_timed_out(&self) -> Option<u8> {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state < EXECUTION_FINISHED).then_some(EXECUTION_TIMED_OUT)
            })
            .ok()
    }

    fn release(&self) {
        self.query_guard.lock().take();
        if let Some(deadline_task) = self.deadline_task.lock().take() {
            deadline_task.abort();
        }
    }
}

impl Drop for QueryExecutionTrackerInner {
    fn drop(&mut self) {
        if let Some(deadline_task) = self.deadline_task.get_mut().take() {
            deadline_task.abort();
        }
    }
}

impl std::fmt::Debug for QueryExecutionTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryExecutionTracker")
            .field("deadline", &self.deadline())
            .field("timeout_seconds", &self.timeout_seconds())
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct SessionCtx {
    _desc: Arc<SessionCtxDesc>,
    inner: SessionState,
    query_tracker: Option<QueryExecutionTracker>,
}

impl SessionCtx {
    pub fn inner(&self) -> &SessionState {
        &self.inner
    }

    pub(crate) fn is_bound_to(&self, query_tracker: &QueryExecutionTracker) -> bool {
        self.query_tracker
            .as_ref()
            .is_some_and(|bound_tracker| bound_tracker.is_same_execution(query_tracker))
    }
}

#[derive(Clone)]
pub struct SessionCtxDesc {
    // maybe we need some info
}

pub struct SessionCtxFactory {
    pub is_test: bool,
    pub target_partitions: usize,
}

pub const DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

impl Default for SessionCtxFactory {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SessionCtxFactory {
    pub fn new(is_test: bool) -> Self {
        Self {
            is_test,
            target_partitions: 0,
        }
    }

    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = target_partitions;
        self
    }

    pub async fn create_session_ctx(&self, context: &Context) -> QueryResult<SessionCtx> {
        self.create_session_ctx_inner(context, None, None, DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES)
            .await
    }

    pub async fn create_session_ctx_with_tracker_and_memory_limit(
        &self,
        context: &Context,
        query_tracker: QueryExecutionTracker,
        memory_limit_bytes: usize,
    ) -> QueryResult<SessionCtx> {
        self.create_session_ctx_inner(context, None, Some(query_tracker), memory_limit_bytes)
            .await
    }

    pub async fn create_session_ctx_with_snapshot_and_tracker_and_memory_limit(
        &self,
        context: &Context,
        snapshot: Arc<SelectObjectSnapshot>,
        query_tracker: QueryExecutionTracker,
        memory_limit_bytes: usize,
    ) -> QueryResult<SessionCtx> {
        self.create_session_ctx_inner(context, Some(snapshot), Some(query_tracker), memory_limit_bytes)
            .await
    }

    async fn create_session_ctx_inner(
        &self,
        context: &Context,
        snapshot: Option<Arc<SelectObjectSnapshot>>,
        query_tracker: Option<QueryExecutionTracker>,
        memory_limit_bytes: usize,
    ) -> QueryResult<SessionCtx> {
        let df_session_ctx = self
            .build_df_session_context(context, snapshot, query_tracker.clone(), memory_limit_bytes)
            .await?;

        Ok(SessionCtx {
            _desc: Arc::new(SessionCtxDesc {}),
            inner: df_session_ctx.state(),
            query_tracker,
        })
    }

    async fn build_df_session_context(
        &self,
        context: &Context,
        snapshot: Option<Arc<SelectObjectSnapshot>>,
        query_tracker: Option<QueryExecutionTracker>,
        memory_limit_bytes: usize,
    ) -> QueryResult<SessionContext> {
        let path = format!("s3://{}", context.input.bucket);
        let store_url = url::Url::parse(&path).unwrap();
        let memory_limit_bytes = if memory_limit_bytes == 0 {
            DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES
        } else {
            memory_limit_bytes
        };
        let rt = RuntimeEnvBuilder::new().with_memory_limit(memory_limit_bytes, 1.0).build()?;
        let config = SessionConfig::new().with_target_partitions(self.target_partitions);
        let custom_two_byte_record_delimiter = context
            .input
            .request
            .input_serialization
            .csv
            .as_ref()
            .and_then(|csv| csv.record_delimiter.as_deref())
            .is_some_and(|delimiter| delimiter.len() == 2 && delimiter.as_bytes() != b"\r\n");
        let scan_range_requires_single_file_scan =
            context.input.request.scan_range.is_some() && context.input.request.input_serialization.parquet.is_none();
        let config = if custom_two_byte_record_delimiter || scan_range_requires_single_file_scan {
            config.with_repartition_file_scans(false)
        } else {
            config
        };
        let memory_pool = Arc::clone(&rt.memory_pool);
        let df_session_state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::new(rt))
            .with_default_features();

        let df_session_state = if self.is_test {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

            // Choose test data format based on what the request serialization specifies.
            let data_bytes: Vec<u8> = if context.input.request.input_serialization.parquet.is_some() {
                test_parquet_bytes()?
            } else if context.input.request.input_serialization.json.is_some() {
                // NDJSON: one JSON object per line — usable for both LINES and DOCUMENT
                // requests (DOCUMENT inputs are converted to NDJSON by EcObjectStore, but
                // in test mode we bypass EcObjectStore, so we put NDJSON here directly).
                b"{\"id\":1,\"name\":\"Alice\",\"age\":25,\"department\":\"HR\",\"salary\":5000}\n\
                   {\"id\":2,\"name\":\"Bob\",\"age\":30,\"department\":\"IT\",\"salary\":6000}\n\
                   {\"id\":3,\"name\":\"Charlie\",\"age\":35,\"department\":\"Finance\",\"salary\":7000}\n\
                   {\"id\":4,\"name\":\"Diana\",\"age\":22,\"department\":\"Marketing\",\"salary\":4500}\n\
                   {\"id\":5,\"name\":\"Eve\",\"age\":28,\"department\":\"IT\",\"salary\":5500}\n\
                   {\"id\":6,\"name\":\"Frank\",\"age\":40,\"department\":\"Finance\",\"salary\":8000}\n\
                   {\"id\":7,\"name\":\"Grace\",\"age\":26,\"department\":\"HR\",\"salary\":5200}\n\
                   {\"id\":8,\"name\":\"Henry\",\"age\":32,\"department\":\"IT\",\"salary\":6200}\n\
                   {\"id\":9,\"name\":\"Ivy\",\"age\":24,\"department\":\"Marketing\",\"salary\":4800}\n\
                   {\"id\":10,\"name\":\"Jack\",\"age\":38,\"department\":\"Finance\",\"salary\":7500}\n"
                    .to_vec()
            } else {
                b"id,name,age,department,salary\n\
                  1,Alice,25,HR,05000\n\
                  2,Bob,30,IT,6000\n\
                  3,Charlie,35,Finance,7000\n\
                  4,Diana,22,Marketing,4500\n\
                  5,Eve,28,IT,5500\n\
                  6,Frank,40,Finance,8000\n\
                  7,Grace,26,HR,5200\n\
                  8,Henry,32,IT,6200\n\
                  9,Ivy,24,Marketing,4800\n\
                  10,Jack,38,Finance,7500"
                    .to_vec()
            };

            let path = Path::from(context.input.key.clone());
            store.put(&path, data_bytes.into()).await.map_err(|e| {
                error!("put data into memory failed: {}", e.to_string());
                QueryError::from(DataFusionError::from(e))
            })?;

            df_session_state.with_object_store(&store_url, store).build()
        } else {
            let store: EcObjectStore = match query_tracker {
                Some(query_tracker) => {
                    EcObjectStore::new_with_query_tracker(context.input.clone(), memory_pool, query_tracker, snapshot)
                }
                None => EcObjectStore::new_with_memory_pool(context.input.clone(), memory_pool, snapshot),
            }
            .map_err(|err| QueryError::Datafusion {
                source: Box::new(DataFusionError::External(Box::new(err))),
            })?;
            df_session_state.with_object_store(&store_url, Arc::new(store)).build()
        };

        let df_session_ctx = SessionContext::new_with_state(df_session_state);

        Ok(df_session_ctx)
    }
}

fn test_parquet_bytes() -> QueryResult<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Int32, false),
    ]));
    let first_batch =
        test_parquet_batch(Arc::clone(&schema), &[1, 2], &["Alice", "Bob"], &[25, 30], &["HR", "IT"], &[5000, 6000])?;
    let second_batch = test_parquet_batch(
        Arc::clone(&schema),
        &[3, 4, 5],
        &["Charlie", "Diana", "Eve"],
        &[35, 22, 28],
        &["Finance", "Marketing", "IT"],
        &[7000, 4500, 5500],
    )?;

    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).map_err(DataFusionError::from)?;
        writer.write(&first_batch).map_err(DataFusionError::from)?;
        writer.flush().map_err(DataFusionError::from)?;
        writer.write(&second_batch).map_err(DataFusionError::from)?;
        writer.close().map_err(DataFusionError::from)?;
    }
    Ok(bytes)
}

fn test_parquet_batch(
    schema: Arc<Schema>,
    ids: &[i32],
    names: &[&str],
    ages: &[i32],
    departments: &[&str],
    salaries: &[i32],
) -> QueryResult<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Int32Array::from(ages.to_vec())),
            Arc::new(StringArray::from(departments.to_vec())),
            Arc::new(Int32Array::from(salaries.to_vec())),
        ],
    )
    .map_err(DataFusionError::from)
    .map_err(QueryError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_api::SelectPutObjReader;
    use crate::storage_api::object_store::ObjectIO as _;
    use datafusion::{
        datasource::{
            file_format::csv::CsvFormat,
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        },
        execution::memory_pool::MemoryLimit,
    };
    use http::HeaderMap;
    use s3s::dto::{
        CSVInput, CSVOutput, ExpressionType, InputSerialization, JSONInput, OutputSerialization, ParquetInput, ScanRange,
        SelectObjectContentInput, SelectObjectContentRequest,
    };
    use std::io::Write as _;

    fn test_context() -> Context {
        Context {
            input: Arc::new(SelectObjectContentInput {
                bucket: "test-bucket".to_string(),
                expected_bucket_owner: None,
                key: "test.csv".to_string(),
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                request: SelectObjectContentRequest {
                    expression: "SELECT * FROM S3Object".to_string(),
                    expression_type: ExpressionType::from_static("SQL"),
                    input_serialization: InputSerialization {
                        csv: Some(CSVInput::default()),
                        ..Default::default()
                    },
                    output_serialization: OutputSerialization {
                        csv: Some(CSVOutput::default()),
                        ..Default::default()
                    },
                    request_progress: None,
                    scan_range: None,
                },
            }),
        }
    }

    async fn prepare_test_snapshot(context: &Context) -> Arc<SelectObjectSnapshot> {
        let env = crate::storage_api::select_test_ecstore_env().await;
        Arc::new(
            env.ecstore
                .prepare_select_object_snapshot(&context.input.bucket, &context.input.key, &HeaderMap::new(), &Default::default())
                .await
                .expect("prepare SelectObjectContent snapshot"),
        )
    }

    #[test]
    fn session_factory_fields_remain_source_compatible() {
        let factory = SessionCtxFactory {
            is_test: true,
            target_partitions: 0,
        };

        assert!(factory.is_test);
        assert_eq!(factory.target_partitions, 0);
    }

    #[tokio::test]
    async fn session_factory_applies_target_partitions() {
        let factory = SessionCtxFactory::new(true).with_target_partitions(3);
        let session = factory
            .create_session_ctx(&test_context())
            .await
            .expect("session should be created with configured target partitions");

        assert_eq!(session.inner().config().target_partitions(), 3);
        assert!(session.inner().config().options().optimizer.repartition_file_scans);
    }

    #[tokio::test]
    async fn parquet_scan_range_keeps_file_repartitioning() {
        let mut context = test_context();
        let request = &mut Arc::make_mut(&mut context.input).request;
        request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(0),
        });
        request.input_serialization.csv = None;
        request.input_serialization.parquet = Some(ParquetInput {});

        let session = SessionCtxFactory::new(true)
            .with_target_partitions(2)
            .create_session_ctx(&context)
            .await
            .expect("Parquet ScanRange session should be created");

        assert!(session.inner().config().options().optimizer.repartition_file_scans);
    }

    #[tokio::test]
    async fn json_lines_scan_range_disables_file_repartitioning() {
        let mut context = test_context();
        let request = &mut Arc::make_mut(&mut context.input).request;
        request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(0),
        });
        request.input_serialization.csv = None;
        request.input_serialization.json = Some(JSONInput::default());

        let session = SessionCtxFactory::new(true)
            .with_target_partitions(2)
            .create_session_ctx(&context)
            .await
            .expect("JSON LINES ScanRange session should be created");

        assert!(!session.inner().config().options().optimizer.repartition_file_scans);
    }

    #[tokio::test]
    async fn csv_scan_range_disables_file_repartitioning() {
        let mut context = test_context();
        Arc::make_mut(&mut context.input).request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(0),
        });

        let session = SessionCtxFactory::new(true)
            .with_target_partitions(2)
            .create_session_ctx(&context)
            .await
            .expect("CSV ScanRange session should be created");

        assert!(!session.inner().config().options().optimizer.repartition_file_scans);
    }

    #[tokio::test]
    async fn two_byte_csv_record_delimiter_disables_file_scan_repartition() {
        let mut context = test_context();
        Arc::get_mut(&mut context.input)
            .expect("test context input should be uniquely owned")
            .request
            .input_serialization
            .csv
            .as_mut()
            .expect("test context should use CSV")
            .record_delimiter = Some("^Y".to_string());
        let session = SessionCtxFactory::new(true)
            .with_target_partitions(4)
            .create_session_ctx(&context)
            .await
            .expect("session should be created");

        assert!(!session.inner().config().options().optimizer.repartition_file_scans);
    }

    #[tokio::test]
    async fn crlf_record_delimiter_retains_file_scan_repartition() {
        let mut context = test_context();
        Arc::get_mut(&mut context.input)
            .expect("test context input should be uniquely owned")
            .request
            .input_serialization
            .csv
            .as_mut()
            .expect("test context should use CSV")
            .record_delimiter = Some("\r\n".to_string());
        let session = SessionCtxFactory::new(true)
            .with_target_partitions(4)
            .create_session_ctx(&context)
            .await
            .expect("session should be created");

        assert_eq!(
            session.inner().config().options().optimizer.repartition_file_scans,
            SessionConfig::new().options().optimizer.repartition_file_scans
        );
    }

    #[tokio::test]
    async fn session_factory_zero_target_partitions_uses_datafusion_default() {
        let factory = SessionCtxFactory::new(true);
        let session = factory
            .create_session_ctx(&test_context())
            .await
            .expect("session should be created with default target partitions");

        assert_eq!(session.inner().config().target_partitions(), SessionConfig::new().target_partitions());
    }

    #[tokio::test]
    async fn session_factory_applies_memory_limit() {
        let factory = SessionCtxFactory::new(true);
        let session = factory
            .create_session_ctx_inner(&test_context(), None, None, 1024)
            .await
            .expect("session should be created with a bounded memory pool");

        assert!(matches!(
            session.inner().runtime_env().memory_pool.memory_limit(),
            MemoryLimit::Finite(1024)
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn production_session_preserves_lazy_snapshot_entry() {
        let _env = crate::storage_api::select_test_ecstore_env().await;
        let session = SessionCtxFactory::new(false)
            .create_session_ctx(&test_context())
            .await
            .expect("legacy production session should install a lazy object store");

        assert_eq!(session.inner().config().target_partitions(), SessionConfig::new().target_partitions());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_tracked_production_session_preserves_lazy_snapshot_entry() {
        let _env = crate::storage_api::select_test_ecstore_env().await;
        let permit = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("query permit should be available");
        let tracker = QueryExecutionTracker::new(
            &QueryExecutionOwner::new(),
            Arc::new(permit),
            Instant::now() + std::time::Duration::from_secs(300),
            300,
        );
        let session = SessionCtxFactory::new(false)
            .create_session_ctx_with_tracker_and_memory_limit(
                &test_context(),
                tracker.clone(),
                DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES,
            )
            .await
            .expect("legacy tracked session should install a lazy object store");

        assert!(session.is_bound_to(&tracker));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn session_factory_propagates_query_guard_to_ec_store() {
        let env = crate::storage_api::select_test_ecstore_env().await;
        let mut context = test_context();
        Arc::make_mut(&mut context.input).bucket = "s3select-query-guard-snapshot".to_string();
        env.make_bucket(&context.input.bucket, false).await;
        let mut reader = SelectPutObjReader::from_vec(b"id,name\n1,Alice\n".to_vec());
        env.ecstore
            .put_object(&context.input.bucket, &context.input.key, &mut reader, &Default::default())
            .await
            .expect("put query guard fixture");
        let snapshot = prepare_test_snapshot(&context).await;

        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("query permit should be available");
        let query_guard = Arc::new(permit);
        let query_tracker = QueryExecutionTracker::new(
            &QueryExecutionOwner::new(),
            Arc::clone(&query_guard),
            Instant::now() + std::time::Duration::from_secs(300),
            300,
        );
        let session = SessionCtxFactory::new(false)
            .create_session_ctx_with_snapshot_and_tracker_and_memory_limit(
                &context,
                snapshot,
                query_tracker,
                DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES,
            )
            .await
            .expect("production session should be created with the query guard");

        assert!(Arc::strong_count(&query_guard) > 1);
        drop(session);
        assert_eq!(Arc::strong_count(&query_guard), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn session_factory_preserves_snapshot_binding_error_source() {
        let env = crate::storage_api::select_test_ecstore_env().await;
        let mut source_context = test_context();
        Arc::make_mut(&mut source_context.input).bucket = "s3select-session-snapshot-identity".to_string();
        Arc::make_mut(&mut source_context.input).key = "source.csv".to_string();
        env.make_bucket(&source_context.input.bucket, false).await;
        let mut reader = SelectPutObjReader::from_vec(b"source-marker\n".to_vec());
        env.ecstore
            .put_object(&source_context.input.bucket, &source_context.input.key, &mut reader, &Default::default())
            .await
            .expect("put snapshot identity fixture");
        let snapshot = prepare_test_snapshot(&source_context).await;

        let mut target_context = source_context.clone();
        Arc::make_mut(&mut target_context.input).key = "different.csv".to_string();
        let permit = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("query permit should be available");
        let tracker = QueryExecutionTracker::new(
            &QueryExecutionOwner::new(),
            Arc::new(permit),
            Instant::now() + std::time::Duration::from_secs(300),
            300,
        );

        let error = match SessionCtxFactory::new(false)
            .create_session_ctx_with_snapshot_and_tracker_and_memory_limit(
                &target_context,
                snapshot,
                tracker,
                DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES,
            )
            .await
        {
            Ok(_) => panic!("a session must reject a snapshot for a different object"),
            Err(error) => error,
        };

        assert!(error.is_snapshot_consistency_error());
        assert!(error.to_string().contains("snapshot consistency failure"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn scan_range_is_preserved_across_large_csv_partition_boundary() {
        const ROW_COUNT: usize = 200_000;
        const ROW_WIDTH: usize = 7;
        const SELECTED_ROW: usize = 100_000;

        let env = crate::storage_api::select_test_ecstore_env().await;
        let mut data = Vec::with_capacity(ROW_COUNT * ROW_WIDTH);
        for row in 0..ROW_COUNT {
            writeln!(&mut data, "{row:06}").expect("write fixed-width CSV test row");
        }
        assert!(data.len() > 1024 * 1024);

        let mut context = test_context();
        Arc::make_mut(&mut context.input).bucket = "s3select-scan-range-partition-snapshot".to_string();
        let selected_start = i64::try_from(SELECTED_ROW * ROW_WIDTH).expect("selected row offset should fit in i64");
        Arc::make_mut(&mut context.input).request.scan_range = Some(ScanRange {
            start: Some(selected_start),
            end: Some(selected_start),
        });
        env.make_bucket(&context.input.bucket, false).await;
        let mut reader = SelectPutObjReader::from_vec(data);
        env.ecstore
            .put_object(&context.input.bucket, &context.input.key, &mut reader, &Default::default())
            .await
            .expect("put large ScanRange CSV fixture");
        let snapshot = prepare_test_snapshot(&context).await;

        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("query permit should be available");
        let query_tracker = QueryExecutionTracker::new(
            &QueryExecutionOwner::new(),
            Arc::new(permit),
            Instant::now() + std::time::Duration::from_secs(300),
            300,
        );
        let session = SessionCtxFactory::new(false)
            .with_target_partitions(2)
            .create_session_ctx_with_snapshot_and_tracker_and_memory_limit(
                &context,
                snapshot,
                query_tracker,
                DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES,
            )
            .await
            .expect("create production ScanRange session");
        assert!(!session.inner().config().options().optimizer.repartition_file_scans);

        let table_path = ListingTableUrl::parse(format!("s3://{}/{}", context.input.bucket, context.input.key))
            .expect("parse ScanRange table URL");
        let listing_options =
            ListingOptions::new(Arc::new(CsvFormat::default().with_schema_infer_max_rec(0).with_has_header(false)))
                .with_file_extension(".csv");
        let schema = listing_options
            .infer_schema(session.inner(), &table_path)
            .await
            .expect("infer ScanRange CSV schema");
        let table = ListingTable::try_new(
            ListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .with_schema(schema),
        )
        .expect("build ScanRange listing table");
        let query_context = SessionContext::new_with_state(session.inner().clone());
        query_context
            .register_table("scan_input", Arc::new(table))
            .expect("register ScanRange listing table");

        let batches = query_context
            .sql("SELECT * FROM scan_input")
            .await
            .expect("plan ScanRange CSV query")
            .collect()
            .await
            .expect("execute ScanRange CSV query");
        let mut values = Vec::new();
        for batch in batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("ScanRange CSV column should be Utf8");
            for row in 0..batch.num_rows() {
                values.push(column.value(row).to_string());
            }
        }

        assert_eq!(values.len(), 1);
        assert_eq!(values, [format!("{SELECTED_ROW:06}")]);
    }

    #[tokio::test]
    async fn elapsed_deadline_does_not_yield_query_guard_before_timer_poll() {
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("query permit should be available");
        let query_tracker = QueryExecutionTracker::new(&QueryExecutionOwner::new(), Arc::new(permit), Instant::now(), 0);

        assert!(query_tracker.query_guard().is_none());
    }

    #[tokio::test]
    async fn session_factory_default_uses_bounded_memory() {
        let factory = SessionCtxFactory::default();
        let session = SessionCtxFactory::new(true)
            .create_session_ctx(&test_context())
            .await
            .expect("default session should be created with a bounded memory pool");

        assert!(!factory.is_test);
        assert_eq!(factory.target_partitions, 0);
        assert!(matches!(
            session.inner().runtime_env().memory_pool.memory_limit(),
            MemoryLimit::Finite(DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES)
        ));
    }
}
