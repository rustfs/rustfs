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

use crate::query::Context;
use crate::{QueryError, QueryResult, SelectStore, object_store::EcObjectStore};
use datafusion::{
    arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
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
        self.create_session_ctx_inner(context, Some(query_tracker), None, memory_limit_bytes)
            .await
    }

    #[cfg(test)]
    async fn create_session_ctx_with_tracker_and_store(
        &self,
        context: &Context,
        query_tracker: QueryExecutionTracker,
        store: Arc<SelectStore>,
    ) -> QueryResult<SessionCtx> {
        self.create_session_ctx_inner(context, Some(query_tracker), Some(store), DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES)
            .await
    }

    async fn create_session_ctx_inner(
        &self,
        context: &Context,
        query_tracker: Option<QueryExecutionTracker>,
        store: Option<Arc<SelectStore>>,
        memory_limit_bytes: usize,
    ) -> QueryResult<SessionCtx> {
        let df_session_ctx = self
            .build_df_session_context(context, query_tracker.clone(), store, memory_limit_bytes)
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
        query_tracker: Option<QueryExecutionTracker>,
        store: Option<Arc<SelectStore>>,
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
                QueryError::StoreError { e: e.to_string() }
            })?;

            df_session_state.with_object_store(&store_url, store).build()
        } else {
            let store: EcObjectStore = match query_tracker {
                Some(query_tracker) => {
                    EcObjectStore::new_with_query_tracker(context.input.clone(), memory_pool, query_tracker, store)
                }
                None => EcObjectStore::new_with_memory_pool(context.input.clone(), memory_pool),
            }
            .map_err(|_| QueryError::NotImplemented { err: String::new() })?;
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
        let mut writer =
            ArrowWriter::try_new(&mut bytes, schema, None).map_err(|e| QueryError::StoreError { e: e.to_string() })?;
        writer
            .write(&first_batch)
            .map_err(|e| QueryError::StoreError { e: e.to_string() })?;
        writer.flush().map_err(|e| QueryError::StoreError { e: e.to_string() })?;
        writer
            .write(&second_batch)
            .map_err(|e| QueryError::StoreError { e: e.to_string() })?;
        writer.close().map_err(|e| QueryError::StoreError { e: e.to_string() })?;
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
    .map_err(|e| QueryError::StoreError { e: e.to_string() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryLimit;
    use s3s::dto::{
        CSVInput, CSVOutput, ExpressionType, InputSerialization, OutputSerialization, SelectObjectContentInput,
        SelectObjectContentRequest,
    };

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn session_factory_propagates_query_guard_to_ec_store() {
        let temp_root = tempfile::tempdir().expect("create session test temp root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_root.path())
            .init_bucket_metadata(false)
            .build()
            .await;

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
            .create_session_ctx_with_tracker_and_store(&test_context(), query_tracker, Arc::clone(&env.ecstore))
            .await
            .expect("production session should be created with the query guard");

        assert!(Arc::strong_count(&query_guard) > 1);
        drop(session);
        assert_eq!(Arc::strong_count(&query_guard), 1);
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
