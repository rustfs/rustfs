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

use std::{
    future::Future,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{
        file_format::{csv::CsvFormat, json::JsonFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::Result as DFResult,
    execution::{RecordBatchStream, SendableRecordBatchStream},
    sql::sqlparser::parser::ParserError,
};
use futures::Stream;
use parking_lot::Mutex;
use rustfs_s3select_api::{
    QueryError, QueryResult,
    query::{
        Query,
        ast::ExtStatement,
        dispatcher::QueryDispatcher,
        execution::{Output, QueryStateMachine},
        function::FuncMetaManagerRef,
        logical_planner::{LogicalPlanner, Plan},
        parser::Parser,
        session::{SessionCtx, SessionCtxFactory},
    },
};
use s3s::dto::{FileHeaderInfo, SelectObjectContentInput};
use std::sync::LazyLock;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, Sleep, sleep_until, timeout_at},
};

use crate::{
    dispatcher::parquet_table::ParquetSelectTable,
    execution::factory::QueryExecutionFactoryRef,
    metadata::{ContextProviderExtension, MetadataProvider, TableHandleProviderRef, base_table::BaseTableProvider},
    sql::logical::planner::DefaultLogicalPlanner,
};

static IGNORE: LazyLock<FileHeaderInfo> = LazyLock::new(|| FileHeaderInfo::from_static(FileHeaderInfo::IGNORE));
static NONE: LazyLock<FileHeaderInfo> = LazyLock::new(|| FileHeaderInfo::from_static(FileHeaderInfo::NONE));
static USE: LazyLock<FileHeaderInfo> = LazyLock::new(|| FileHeaderInfo::from_static(FileHeaderInfo::USE));

#[derive(Clone)]
pub struct SimpleQueryDispatcher {
    input: Arc<SelectObjectContentInput>,
    // client for default tenant
    _default_table_provider: TableHandleProviderRef,
    session_factory: Arc<SessionCtxFactory>,
    // parser
    parser: Arc<dyn Parser + Send + Sync>,
    // get query execution factory
    query_execution_factory: QueryExecutionFactoryRef,
    func_manager: FuncMetaManagerRef,
    query_admission: Arc<Semaphore>,
    query_timeout: Duration,
}

#[async_trait]
impl QueryDispatcher for SimpleQueryDispatcher {
    async fn execute_query(&self, query: &Query) -> QueryResult<Output> {
        let permit = self
            .query_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueryError::QueryConcurrencyLimit)?;
        let deadline = Instant::now() + self.query_timeout;
        let output = timeout_at(deadline, async {
            let query_state_machine = self.build_query_state_machine(query.clone()).await?;
            let logical_plan = self.build_logical_plan(query_state_machine.clone()).await?;
            let Some(logical_plan) = logical_plan else {
                return Ok(Output::Nil(()));
            };
            self.execute_logical_plan(logical_plan, query_state_machine).await
        })
        .await
        .map_err(|_| QueryError::QueryTimeout {
            seconds: self.query_timeout.as_secs(),
        })??;

        match output {
            Output::StreamData(stream) => Ok(Output::StreamData(Box::pin(TrackedRecordBatchStream::new(
                stream,
                permit,
                deadline,
                self.query_timeout.as_secs(),
            )))),
            Output::Nil(()) => Ok(Output::Nil(())),
        }
    }

    async fn build_logical_plan(&self, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Option<Plan>> {
        let session = &query_state_machine.session;
        let query = &query_state_machine.query;

        let scheme_provider = self.build_scheme_provider(session).await?;

        let logical_planner = DefaultLogicalPlanner::new(&scheme_provider);

        let statements = self.parser.parse(query.content())?;

        // not allow multi statement
        if statements.len() > 1 {
            return Err(QueryError::MultiStatement {
                num: statements.len(),
                sql: query_state_machine.query.content().to_string(),
            });
        }

        let stmt = match statements.front() {
            Some(stmt) => stmt.clone(),
            None => {
                return Err(QueryError::Parser {
                    source: ParserError::ParserError("empty SQL expression".to_string()),
                });
            }
        };

        let logical_plan = self
            .statement_to_logical_plan(stmt, &logical_planner, query_state_machine)
            .await?;
        Ok(Some(logical_plan))
    }

    async fn execute_logical_plan(&self, logical_plan: Plan, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Output> {
        self.execute_logical_plan(logical_plan, query_state_machine).await
    }

    async fn build_query_state_machine(&self, query: Query) -> QueryResult<Arc<QueryStateMachine>> {
        let session = self.session_factory.create_session_ctx(query.context()).await?;

        let query_state_machine = Arc::new(QueryStateMachine::begin(query, session));
        Ok(query_state_machine)
    }
}

impl SimpleQueryDispatcher {
    async fn statement_to_logical_plan<S: ContextProviderExtension + Send + Sync>(
        &self,
        stmt: ExtStatement,
        logical_planner: &DefaultLogicalPlanner<'_, S>,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> QueryResult<Plan> {
        // begin analyze
        query_state_machine.begin_analyze();
        let logical_plan = logical_planner
            .create_logical_plan(stmt, &query_state_machine.session)
            .await?;
        query_state_machine.end_analyze();

        Ok(logical_plan)
    }

    async fn execute_logical_plan(&self, logical_plan: Plan, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Output> {
        let execution = self
            .query_execution_factory
            .create_query_execution(logical_plan, query_state_machine.clone())
            .await?;

        execution.start().await
    }

    async fn build_scheme_provider(&self, session: &SessionCtx) -> QueryResult<MetadataProvider> {
        if self.input.request.input_serialization.parquet.is_some() {
            let provider = ParquetSelectTable::try_new(session.inner(), self.input.as_ref()).await?;
            let current_session_table_provider = self.build_table_handle_provider()?;
            let metadata_provider =
                MetadataProvider::new(provider, current_session_table_provider, self.func_manager.clone(), session.clone());

            return Ok(metadata_provider);
        }

        let path = format!("s3://{}/{}", self.input.bucket, self.input.key);
        let table_path = ListingTableUrl::parse(path)?;
        let (listing_options, need_rename_volume_name, need_ignore_volume_name) =
            if let Some(csv) = self.input.request.input_serialization.csv.as_ref() {
                let mut need_rename_volume_name = false;
                let mut need_ignore_volume_name = false;
                let mut file_format = CsvFormat::default()
                    .with_schema_infer_max_rec(0)
                    .with_comment(
                        csv.comments
                            .clone()
                            .map(|c| c.as_bytes().first().copied().unwrap_or_default()),
                    )
                    .with_escape(
                        csv.quote_escape_character
                            .clone()
                            .map(|e| e.as_bytes().first().copied().unwrap_or_default()),
                    );
                if let Some(delimiter) = csv.field_delimiter.as_ref()
                    && delimiter.len() == 1
                {
                    file_format = file_format.with_delimiter(delimiter.as_bytes()[0]);
                }
                match csv.file_header_info.as_ref() {
                    Some(info) => {
                        if *info == *NONE {
                            file_format = file_format.with_has_header(false);
                            need_rename_volume_name = true;
                        } else if *info == *IGNORE {
                            file_format = file_format.with_has_header(true);
                            need_rename_volume_name = true;
                            need_ignore_volume_name = true;
                        } else if *info == *USE {
                            file_format = file_format.with_has_header(true);
                        } else {
                            return Err(QueryError::NotImplemented {
                                err: "unsupported FileHeaderInfo".to_string(),
                            });
                        }
                    }
                    _ => {
                        return Err(QueryError::NotImplemented {
                            err: "unsupported FileHeaderInfo".to_string(),
                        });
                    }
                }
                if let Some(quote) = csv.quote_character.as_ref() {
                    file_format = file_format.with_quote(quote.as_bytes().first().copied().unwrap_or_default());
                }
                (
                    ListingOptions::new(Arc::new(file_format)).with_file_extension(".csv"),
                    need_rename_volume_name,
                    need_ignore_volume_name,
                )
            } else if self.input.request.input_serialization.json.is_some() {
                let file_format = JsonFormat::default();
                // Use the actual file extension from the object key so that files stored
                // with a `.jsonl` suffix (newline-delimited JSON) are also matched by
                // DataFusion's listing/schema-inference logic. Falling back to ".json"
                // preserves behaviour for keys that have no extension.
                let file_ext = std::path::Path::new(&self.input.key)
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| format!(".{e}"))
                    .unwrap_or_else(|| ".json".to_string());
                (ListingOptions::new(Arc::new(file_format)).with_file_extension(file_ext), false, false)
            } else {
                return Err(QueryError::NotImplemented {
                    err: "not support this file type".to_string(),
                });
            };

        let resolve_schema = listing_options.infer_schema(session.inner(), &table_path).await?;
        let config = if need_rename_volume_name {
            let mut new_fields = Vec::new();
            for (i, field) in resolve_schema.fields().iter().enumerate() {
                let f_name = field.name();
                let mut_field = field.deref().clone();
                if f_name.starts_with("column_") {
                    let re_name = f_name.replace("column_", "_");
                    new_fields.push(mut_field.with_name(re_name));
                } else if need_ignore_volume_name {
                    let re_name = format!("_{}", i + 1);
                    new_fields.push(mut_field.with_name(re_name));
                } else {
                    new_fields.push(mut_field);
                }
            }
            let new_schema = Arc::new(Schema::new(new_fields).with_metadata(resolve_schema.metadata().clone()));
            ListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .with_schema(new_schema)
        } else {
            ListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .with_schema(resolve_schema)
        };
        // rename default
        let provider = Arc::new(ListingTable::try_new(config)?);
        let current_session_table_provider = self.build_table_handle_provider()?;
        let metadata_provider =
            MetadataProvider::new(provider, current_session_table_provider, self.func_manager.clone(), session.clone());

        Ok(metadata_provider)
    }

    fn build_table_handle_provider(&self) -> QueryResult<TableHandleProviderRef> {
        let current_session_table_provider: Arc<BaseTableProvider> = Arc::new(BaseTableProvider::default());

        Ok(current_session_table_provider)
    }
}

pub struct TrackedRecordBatchStream {
    state: Arc<TrackedRecordBatchState>,
    schema: SchemaRef,
    deadline: Pin<Box<Sleep>>,
    deadline_task: tokio::task::JoinHandle<()>,
    timeout_seconds: u64,
    done: bool,
}

struct TrackedRecordBatchState {
    inner: Mutex<Option<SendableRecordBatchStream>>,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    timed_out: AtomicBool,
}

impl TrackedRecordBatchState {
    fn finish(&self) {
        self.permit.lock().take();
        self.inner.lock().take();
    }

    fn expire(&self) {
        self.timed_out.store(true, Ordering::Release);
        self.finish();
    }
}

impl TrackedRecordBatchStream {
    fn new(inner: SendableRecordBatchStream, permit: OwnedSemaphorePermit, deadline: Instant, timeout_seconds: u64) -> Self {
        let schema = inner.schema();
        let state = Arc::new(TrackedRecordBatchState {
            inner: Mutex::new(Some(inner)),
            permit: Mutex::new(Some(permit)),
            timed_out: AtomicBool::new(false),
        });
        let deadline_state = Arc::clone(&state);
        let deadline_task = tokio::spawn(async move {
            sleep_until(deadline).await;
            deadline_state.expire();
        });
        Self {
            state,
            schema,
            deadline: Box::pin(sleep_until(deadline)),
            deadline_task,
            timeout_seconds,
            done: false,
        }
    }
}

impl Drop for TrackedRecordBatchStream {
    fn drop(&mut self) {
        self.state.finish();
        self.deadline_task.abort();
    }
}

impl RecordBatchStream for TrackedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for TrackedRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.state.timed_out.load(Ordering::Acquire) || self.deadline.as_mut().poll(cx).is_ready() {
            self.done = true;
            self.state.expire();
            self.deadline_task.abort();
            return Poll::Ready(Some(Err(datafusion::common::DataFusionError::External(Box::new(
                QueryError::QueryTimeout {
                    seconds: self.timeout_seconds,
                },
            )))));
        }
        let poll = {
            let mut inner = self.state.inner.lock();
            match inner.as_mut() {
                Some(inner) => inner.as_mut().poll_next(cx),
                None => Poll::Ready(None),
            }
        };
        if matches!(poll, Poll::Ready(None)) {
            self.done = true;
            self.state.finish();
            self.deadline_task.abort();
        }
        poll
    }
}

#[derive(Default, Clone)]
pub struct SimpleQueryDispatcherBuilder {
    input: Option<Arc<SelectObjectContentInput>>,
    default_table_provider: Option<TableHandleProviderRef>,
    session_factory: Option<Arc<SessionCtxFactory>>,
    parser: Option<Arc<dyn Parser + Send + Sync>>,

    query_execution_factory: Option<QueryExecutionFactoryRef>,

    func_manager: Option<FuncMetaManagerRef>,
    query_admission: Option<Arc<Semaphore>>,
    query_timeout: Option<Duration>,
}

impl SimpleQueryDispatcherBuilder {
    pub fn with_input(mut self, input: Arc<SelectObjectContentInput>) -> Self {
        self.input = Some(input);
        self
    }
    pub fn with_default_table_provider(mut self, default_table_provider: TableHandleProviderRef) -> Self {
        self.default_table_provider = Some(default_table_provider);
        self
    }

    pub fn with_session_factory(mut self, session_factory: Arc<SessionCtxFactory>) -> Self {
        self.session_factory = Some(session_factory);
        self
    }

    pub fn with_parser(mut self, parser: Arc<dyn Parser + Send + Sync>) -> Self {
        self.parser = Some(parser);
        self
    }

    pub fn with_query_execution_factory(mut self, query_execution_factory: QueryExecutionFactoryRef) -> Self {
        self.query_execution_factory = Some(query_execution_factory);
        self
    }

    pub fn with_func_manager(mut self, func_manager: FuncMetaManagerRef) -> Self {
        self.func_manager = Some(func_manager);
        self
    }

    pub fn with_query_admission(mut self, query_admission: Arc<Semaphore>) -> Self {
        self.query_admission = Some(query_admission);
        self
    }

    pub fn with_query_timeout(mut self, query_timeout: Duration) -> Self {
        self.query_timeout = Some(query_timeout);
        self
    }

    pub fn build(self) -> QueryResult<Arc<SimpleQueryDispatcher>> {
        let input = self.input.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of input".to_string(),
        })?;

        let session_factory = self.session_factory.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of session_factory".to_string(),
        })?;

        let parser = self.parser.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of parser".to_string(),
        })?;

        let query_execution_factory = self.query_execution_factory.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of query_execution_factory".to_string(),
        })?;

        let func_manager = self.func_manager.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of func_manager".to_string(),
        })?;

        let default_table_provider = self.default_table_provider.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of default_table_provider".to_string(),
        })?;

        let query_admission = self.query_admission.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of query_admission".to_string(),
        })?;

        let query_timeout = self.query_timeout.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of query_timeout".to_string(),
        })?;

        let dispatcher = Arc::new(SimpleQueryDispatcher {
            input,
            _default_table_provider: default_table_provider,
            session_factory,
            parser,
            query_execution_factory,
            func_manager,
            query_admission,
            query_timeout,
        });

        Ok(dispatcher)
    }
}

#[cfg(test)]
mod tests {
    use super::{SimpleQueryDispatcher, SimpleQueryDispatcherBuilder, TrackedRecordBatchStream};
    use crate::{
        execution::{
            factory::{QueryExecutionFactoryRef, SqlQueryExecutionFactory},
            scheduler::local::LocalScheduler,
        },
        function::simple_func_manager::SimpleFunctionMetadataManager,
        metadata::base_table::BaseTableProvider,
        sql::{optimizer::CascadeOptimizerBuilder, parser::DefaultParser},
    };
    use async_trait::async_trait;
    use datafusion::{
        arrow::{datatypes::Schema, record_batch::RecordBatch},
        common::DataFusionError,
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use futures::{StreamExt, stream};
    use rustfs_s3select_api::{
        QueryError, QueryResult,
        query::{
            Context as QueryContext, Query,
            dispatcher::QueryDispatcher,
            execution::{QueryExecutionFactory, QueryExecutionRef, QueryStateMachineRef},
            logical_planner::Plan,
            session::SessionCtxFactory,
        },
    };
    use s3s::dto::{
        CSVInput, CSVOutput, ExpressionType, FileHeaderInfo, InputSerialization, OutputSerialization, SelectObjectContentInput,
        SelectObjectContentRequest,
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::Poll,
        time::Duration,
    };
    use tokio::{sync::Semaphore, time::Instant};

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct PendingQueryExecutionFactory;

    #[async_trait]
    impl QueryExecutionFactory for PendingQueryExecutionFactory {
        async fn create_query_execution(
            &self,
            _plan: Plan,
            _query_state_machine: QueryStateMachineRef,
        ) -> QueryResult<QueryExecutionRef> {
            std::future::pending().await
        }
    }

    fn test_input() -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: "test-bucket".to_string(),
            expected_bucket_owner: None,
            key: "test.csv".to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: "SELECT * FROM S3Object".to_string(),
                expression_type: ExpressionType::from_static(ExpressionType::SQL),
                input_serialization: InputSerialization {
                    csv: Some(CSVInput {
                        file_header_info: Some(FileHeaderInfo::from_static(FileHeaderInfo::USE)),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                output_serialization: OutputSerialization {
                    csv: Some(CSVOutput::default()),
                    ..Default::default()
                },
                request_progress: None,
                scan_range: None,
            },
        }
    }

    fn test_dispatcher(
        admission: Arc<Semaphore>,
        query_timeout: Duration,
    ) -> (Arc<SimpleQueryDispatcher>, Arc<SelectObjectContentInput>) {
        let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
        let scheduler = Arc::new(LocalScheduler {});
        test_dispatcher_with_factory(admission, query_timeout, Arc::new(SqlQueryExecutionFactory::new(optimizer, scheduler)))
    }

    fn test_dispatcher_with_factory(
        admission: Arc<Semaphore>,
        query_timeout: Duration,
        query_execution_factory: QueryExecutionFactoryRef,
    ) -> (Arc<SimpleQueryDispatcher>, Arc<SelectObjectContentInput>) {
        let input = Arc::new(test_input());
        let dispatcher = SimpleQueryDispatcherBuilder::default()
            .with_input(Arc::clone(&input))
            .with_default_table_provider(Arc::new(BaseTableProvider::default()))
            .with_session_factory(Arc::new(SessionCtxFactory::new(true)))
            .with_parser(Arc::new(DefaultParser::default()))
            .with_query_execution_factory(query_execution_factory)
            .with_func_manager(Arc::new(SimpleFunctionMetadataManager::default()))
            .with_query_admission(admission)
            .with_query_timeout(query_timeout)
            .build()
            .expect("query dispatcher should build");
        (dispatcher, input)
    }

    #[tokio::test]
    async fn rejects_query_when_admission_is_saturated() {
        let admission = Arc::new(Semaphore::new(1));
        let _held_permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let (dispatcher, input) = test_dispatcher(admission, Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());

        let result = dispatcher.execute_query(&query).await;

        assert!(matches!(result, Err(QueryError::QueryConcurrencyLimit)));
    }

    #[tokio::test]
    async fn times_out_during_query_setup() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher_with_factory(
            Arc::clone(&admission),
            Duration::from_millis(1),
            Arc::new(PendingQueryExecutionFactory),
        );
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());

        let result = tokio::time::timeout(Duration::from_secs(1), dispatcher.execute_query(&query))
            .await
            .expect("dispatcher should enforce its query setup timeout");

        match result {
            Err(QueryError::QueryTimeout { seconds: 0 }) => {}
            Err(err) => panic!("unexpected query result: {err:?}"),
            Ok(_) => panic!("query setup unexpectedly completed"),
        }
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn query_stream_releases_permit_after_timeout() {
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let inner_dropped = Arc::new(AtomicBool::new(false));
        let drop_signal = DropSignal(Arc::clone(&inner_dropped));
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream::poll_fn(move |_| {
                let _drop_signal = &drop_signal;
                Poll::Pending::<Option<Result<RecordBatch, DataFusionError>>>
            }),
        ));
        let mut output = Box::pin(TrackedRecordBatchStream::new(inner, permit, Instant::now(), 300));

        let err = output
            .next()
            .await
            .expect("timeout error")
            .expect_err("expired query must fail");
        let DataFusionError::External(source) = err else {
            panic!("expected external query error");
        };
        assert!(matches!(
            source.downcast_ref::<QueryError>(),
            Some(QueryError::QueryTimeout { seconds: 300 })
        ));
        assert!(inner_dropped.load(Ordering::SeqCst));
        assert_eq!(admission.available_permits(), 1);
        assert!(output.next().await.is_none());
    }

    #[tokio::test]
    async fn query_deadline_releases_resources_without_polling_stream() {
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let inner_dropped = Arc::new(AtomicBool::new(false));
        let drop_signal = DropSignal(Arc::clone(&inner_dropped));
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream::poll_fn(move |_| {
                let _drop_signal = &drop_signal;
                Poll::Pending::<Option<Result<RecordBatch, DataFusionError>>>
            }),
        ));
        let _output = TrackedRecordBatchStream::new(inner, permit, Instant::now(), 300);

        let recovered_permit = tokio::time::timeout(Duration::from_secs(5), Arc::clone(&admission).acquire_owned())
            .await
            .expect("deadline should release the admission permit")
            .expect("admission semaphore should remain open");

        assert!(inner_dropped.load(Ordering::SeqCst));
        drop(recovered_permit);
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn query_stream_releases_permit_after_completion() {
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream::empty::<Result<RecordBatch, DataFusionError>>(),
        ));
        let mut output = Box::pin(TrackedRecordBatchStream::new(
            inner,
            permit,
            Instant::now() + Duration::from_secs(300),
            300,
        ));

        assert!(output.next().await.is_none());
        assert_eq!(admission.available_permits(), 1);
    }
}
