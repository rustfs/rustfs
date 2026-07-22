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
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;
use tracing::error;

pub type QueryExecutionGuard = Arc<OwnedSemaphorePermit>;

#[derive(Clone)]
pub struct SessionCtx {
    _desc: Arc<SessionCtxDesc>,
    inner: SessionState,
}

impl SessionCtx {
    pub fn inner(&self) -> &SessionState {
        &self.inner
    }
}

#[derive(Clone)]
pub struct SessionCtxDesc {
    // maybe we need some info
}

pub struct SessionCtxFactory {
    pub is_test: bool,
    pub target_partitions: usize,
    pub memory_limit_bytes: usize,
}

const DEFAULT_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;

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
            memory_limit_bytes: DEFAULT_MEMORY_LIMIT_BYTES,
        }
    }

    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = target_partitions;
        self
    }

    pub fn with_memory_limit_bytes(mut self, memory_limit_bytes: usize) -> Self {
        if memory_limit_bytes > 0 {
            self.memory_limit_bytes = memory_limit_bytes;
        }
        self
    }

    pub async fn create_session_ctx(&self, context: &Context) -> QueryResult<SessionCtx> {
        self.create_session_ctx_inner(context, None, None).await
    }

    pub async fn create_session_ctx_with_guard(
        &self,
        context: &Context,
        query_guard: QueryExecutionGuard,
    ) -> QueryResult<SessionCtx> {
        self.create_session_ctx_inner(context, Some(query_guard), None).await
    }

    #[cfg(test)]
    async fn create_session_ctx_with_guard_and_store(
        &self,
        context: &Context,
        query_guard: QueryExecutionGuard,
        store: Arc<SelectStore>,
    ) -> QueryResult<SessionCtx> {
        self.create_session_ctx_inner(context, Some(query_guard), Some(store)).await
    }

    async fn create_session_ctx_inner(
        &self,
        context: &Context,
        query_guard: Option<QueryExecutionGuard>,
        store: Option<Arc<SelectStore>>,
    ) -> QueryResult<SessionCtx> {
        let df_session_ctx = self.build_df_session_context(context, query_guard, store).await?;

        Ok(SessionCtx {
            _desc: Arc::new(SessionCtxDesc {}),
            inner: df_session_ctx.state(),
        })
    }

    async fn build_df_session_context(
        &self,
        context: &Context,
        query_guard: Option<QueryExecutionGuard>,
        store: Option<Arc<SelectStore>>,
    ) -> QueryResult<SessionContext> {
        let path = format!("s3://{}", context.input.bucket);
        let store_url = url::Url::parse(&path).unwrap();
        let rt = RuntimeEnvBuilder::new()
            .with_memory_limit(self.memory_limit_bytes, 1.0)
            .build()?;
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
            let store: EcObjectStore = match query_guard {
                Some(query_guard) => EcObjectStore::new_with_query_guard(context.input.clone(), memory_pool, query_guard, store),
                None => EcObjectStore::new(context.input.clone(), memory_pool),
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
        let factory = SessionCtxFactory::new(true).with_memory_limit_bytes(1024);
        let session = factory
            .create_session_ctx(&test_context())
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
        let session = SessionCtxFactory::new(false)
            .create_session_ctx_with_guard_and_store(&test_context(), Arc::clone(&query_guard), Arc::clone(&env.ecstore))
            .await
            .expect("production session should be created with the query guard");

        assert!(Arc::strong_count(&query_guard) > 1);
        drop(session);
        assert_eq!(Arc::strong_count(&query_guard), 1);
    }

    #[test]
    fn session_factory_default_uses_bounded_memory() {
        let factory = SessionCtxFactory::default();

        assert!(!factory.is_test);
        assert_eq!(factory.target_partitions, 0);
        assert_eq!(factory.memory_limit_bytes, DEFAULT_MEMORY_LIMIT_BYTES);
    }
}
