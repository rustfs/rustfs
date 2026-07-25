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
    sync::Arc,
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
    QueryError, QueryResult, S3SelectPolicyError,
    query::{
        Query,
        ast::ExtStatement,
        dispatcher::QueryDispatcher,
        execution::{Output, QueryStateMachine},
        function::FuncMetaManagerRef,
        logical_planner::{LogicalPlanner, Plan},
        parser::Parser,
        session::{
            DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES, QueryExecutionOwner, QueryExecutionStatus, QueryExecutionTracker, SessionCtx,
            SessionCtxFactory,
        },
    },
};
use s3s::dto::{FileHeaderInfo, SelectObjectContentInput};
use std::sync::LazyLock;
use tokio::{
    sync::Semaphore,
    time::{Instant, Sleep, sleep_until, timeout_at},
};

use crate::{
    dispatcher::parquet_table::ParquetSelectTable,
    execution::factory::QueryExecutionFactoryRef,
    instance::{DEFAULT_MAX_CONCURRENT_QUERIES, DEFAULT_QUERY_TIMEOUT_SECS},
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
    memory_limit_bytes: usize,
    query_admission: Arc<Semaphore>,
    query_timeout: Duration,
    query_execution_owner: QueryExecutionOwner,
}

struct QueryPhaseGuard<'a> {
    query_tracker: &'a QueryExecutionTracker,
    query_execution_owner: &'a QueryExecutionOwner,
    armed: bool,
}

impl<'a> QueryPhaseGuard<'a> {
    fn new(query_tracker: &'a QueryExecutionTracker, query_execution_owner: &'a QueryExecutionOwner) -> Self {
        Self {
            query_tracker,
            query_execution_owner,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for QueryPhaseGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.query_tracker.finish(self.query_execution_owner);
        }
    }
}

#[async_trait]
impl QueryDispatcher for SimpleQueryDispatcher {
    async fn execute_query(&self, query: &Query) -> QueryResult<Output> {
        let query_state_machine = self.build_query_state_machine(query.clone()).await?;
        let logical_plan = self.build_logical_plan(Arc::clone(&query_state_machine)).await?;
        let Some(logical_plan) = logical_plan else {
            return Ok(Output::Nil(()));
        };

        self.execute_logical_plan(logical_plan, query_state_machine).await
    }

    async fn build_logical_plan(&self, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Option<Plan>> {
        if !query_state_machine.tracker_matches_session() {
            return Err(QueryError::Cancel);
        }
        let query_tracker = query_state_machine.query_tracker().cloned().ok_or(QueryError::Cancel)?;
        if !query_tracker.claim_planning(&self.query_execution_owner) {
            return Err(self.query_tracker_error(&query_tracker));
        }
        let phase_guard = QueryPhaseGuard::new(&query_tracker, &self.query_execution_owner);
        let logical_plan = self
            .run_with_query_deadline(&query_tracker, async {
                let session = &query_state_machine.session;
                let query = &query_state_machine.query;

                let scheme_provider = self.build_scheme_provider(session).await?;
                let logical_planner = DefaultLogicalPlanner::new(&scheme_provider);
                let statements = self.parser.parse(query.content())?;

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
                Ok(logical_plan)
            })
            .await?;
        if !query_tracker.mark_planned(&self.query_execution_owner) {
            drop(logical_plan);
            return Err(self.query_tracker_error(&query_tracker));
        }
        phase_guard.disarm();
        Ok(Some(logical_plan))
    }

    async fn execute_logical_plan(&self, logical_plan: Plan, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Output> {
        if !query_state_machine.tracker_matches_session() {
            return Err(QueryError::Cancel);
        }
        let query_tracker = query_state_machine.query_tracker().cloned().ok_or(QueryError::Cancel)?;
        if !query_tracker.claim_execution(&self.query_execution_owner) {
            return Err(self.query_tracker_error(&query_tracker));
        }
        let phase_guard = QueryPhaseGuard::new(&query_tracker, &self.query_execution_owner);
        let output = self
            .run_with_query_deadline(&query_tracker, self.start_logical_plan(logical_plan, query_state_machine))
            .await?;

        match output {
            Output::StreamData(stream) => {
                if !query_tracker.mark_running(&self.query_execution_owner) {
                    drop(stream);
                    return Err(self.query_tracker_error(&query_tracker));
                }
                let stream = TrackedRecordBatchStream::new(stream, query_tracker.clone(), self.query_execution_owner.clone());
                phase_guard.disarm();
                Ok(Output::StreamData(Box::pin(stream)))
            }
            Output::Nil(()) => Ok(Output::Nil(())),
        }
    }

    async fn build_query_state_machine(&self, query: Query) -> QueryResult<Arc<QueryStateMachine>> {
        let permit = self
            .query_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueryError::from(S3SelectPolicyError::QueryConcurrencyLimit))?;
        let query_tracker = QueryExecutionTracker::new(
            &self.query_execution_owner,
            Arc::new(permit),
            Instant::now() + self.query_timeout,
            self.query_timeout.as_secs(),
        );
        let phase_guard = QueryPhaseGuard::new(&query_tracker, &self.query_execution_owner);
        let session = self
            .run_with_query_deadline(
                &query_tracker,
                self.session_factory.create_session_ctx_with_tracker_and_memory_limit(
                    query.context(),
                    query_tracker.clone(),
                    self.memory_limit_bytes,
                ),
            )
            .await?;
        if !query_tracker.mark_admitted(&self.query_execution_owner) {
            drop(session);
            return Err(self.query_tracker_error(&query_tracker));
        }
        phase_guard.disarm();
        Ok(Arc::new(QueryStateMachine::begin_tracked(query, session, query_tracker)?))
    }
}

impl SimpleQueryDispatcher {
    async fn run_with_query_deadline<T>(
        &self,
        query_tracker: &QueryExecutionTracker,
        future: impl Future<Output = QueryResult<T>>,
    ) -> QueryResult<T> {
        let deadline = query_tracker.deadline();
        let timeout_error = || {
            S3SelectPolicyError::QueryTimeout {
                seconds: query_tracker.timeout_seconds(),
            }
            .into()
        };
        match query_tracker.status() {
            QueryExecutionStatus::TimedOut => {
                drop(future);
                query_tracker.expire(&self.query_execution_owner);
                return Err(timeout_error());
            }
            QueryExecutionStatus::Finished => return Err(QueryError::Cancel),
            QueryExecutionStatus::Active if Instant::now() >= deadline => {
                drop(future);
                query_tracker.expire(&self.query_execution_owner);
                return Err(timeout_error());
            }
            QueryExecutionStatus::Active => {}
        }

        match timeout_at(deadline, future).await {
            Ok(result) => match query_tracker.status() {
                QueryExecutionStatus::TimedOut => {
                    drop(result);
                    query_tracker.expire(&self.query_execution_owner);
                    Err(timeout_error())
                }
                QueryExecutionStatus::Finished => {
                    drop(result);
                    Err(QueryError::Cancel)
                }
                QueryExecutionStatus::Active if Instant::now() >= deadline => {
                    drop(result);
                    query_tracker.expire(&self.query_execution_owner);
                    Err(timeout_error())
                }
                QueryExecutionStatus::Active => result,
            },
            Err(_) => {
                query_tracker.expire(&self.query_execution_owner);
                Err(timeout_error())
            }
        }
    }

    fn query_tracker_error(&self, query_tracker: &QueryExecutionTracker) -> QueryError {
        if !query_tracker.is_owned_by(&self.query_execution_owner) {
            return QueryError::Cancel;
        }
        match query_tracker.status() {
            QueryExecutionStatus::TimedOut => S3SelectPolicyError::QueryTimeout {
                seconds: query_tracker.timeout_seconds(),
            }
            .into(),
            QueryExecutionStatus::Active if Instant::now() >= query_tracker.deadline() => S3SelectPolicyError::QueryTimeout {
                seconds: query_tracker.timeout_seconds(),
            }
            .into(),
            QueryExecutionStatus::Active | QueryExecutionStatus::Finished => QueryError::Cancel,
        }
    }

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

    async fn start_logical_plan(&self, logical_plan: Plan, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Output> {
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
    done: bool,
}

struct TrackedRecordBatchState {
    inner: Mutex<Option<SendableRecordBatchStream>>,
    query_tracker: QueryExecutionTracker,
    query_execution_owner: QueryExecutionOwner,
}

impl TrackedRecordBatchState {
    fn finish(&self) {
        self.inner.lock().take();
        self.query_tracker.finish(&self.query_execution_owner);
    }

    fn expire(&self) {
        self.inner.lock().take();
        self.query_tracker.expire(&self.query_execution_owner);
    }
}

impl TrackedRecordBatchStream {
    fn new(
        inner: SendableRecordBatchStream,
        query_tracker: QueryExecutionTracker,
        query_execution_owner: QueryExecutionOwner,
    ) -> Self {
        let schema = inner.schema();
        let deadline = query_tracker.deadline();
        let state = Arc::new(TrackedRecordBatchState {
            inner: Mutex::new(Some(inner)),
            query_tracker,
            query_execution_owner,
        });
        let deadline_state = Arc::clone(&state);
        let deadline_task = tokio::spawn(async move {
            sleep_until(deadline).await;
            deadline_state.expire();
        });
        state.query_tracker.handoff_deadline(&state.query_execution_owner);
        Self {
            state,
            schema,
            deadline: Box::pin(sleep_until(deadline)),
            deadline_task,
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
        let deadline_at = self.state.query_tracker.deadline();
        let timeout_seconds = self.state.query_tracker.timeout_seconds();
        match self.state.query_tracker.status() {
            QueryExecutionStatus::TimedOut => {
                self.done = true;
                self.state.expire();
                self.deadline_task.abort();
                return Poll::Ready(Some(Err(query_timeout_error(timeout_seconds))));
            }
            QueryExecutionStatus::Finished => {
                self.done = true;
                self.state.finish();
                self.deadline_task.abort();
                return Poll::Ready(Some(Err(query_cancelled_error())));
            }
            QueryExecutionStatus::Active => {}
        }
        if self.deadline.as_mut().poll(cx).is_ready() {
            self.done = true;
            self.state.expire();
            self.deadline_task.abort();
            return Poll::Ready(Some(Err(query_timeout_error(timeout_seconds))));
        }
        let mut inner = self.state.inner.lock();
        let poll = match inner.as_mut() {
            Some(inner) => inner.as_mut().poll_next(cx),
            None => Poll::Ready(None),
        };
        let status = self.state.query_tracker.status();
        if status != QueryExecutionStatus::Active || Instant::now() >= deadline_at {
            drop(poll);
            inner.take();
            drop(inner);
            self.done = true;
            match status {
                QueryExecutionStatus::TimedOut | QueryExecutionStatus::Active => {
                    self.state.query_tracker.expire(&self.state.query_execution_owner);
                }
                QueryExecutionStatus::Finished => {
                    self.state.query_tracker.finish(&self.state.query_execution_owner);
                }
            }
            self.deadline_task.abort();
            return Poll::Ready(Some(Err(match status {
                QueryExecutionStatus::TimedOut | QueryExecutionStatus::Active => query_timeout_error(timeout_seconds),
                QueryExecutionStatus::Finished => query_cancelled_error(),
            })));
        }
        drop(inner);
        if matches!(poll, Poll::Ready(None)) {
            self.done = true;
            self.state.finish();
            self.deadline_task.abort();
        }
        poll
    }
}

fn query_timeout_error(timeout_seconds: u64) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::External(Box::new(S3SelectPolicyError::QueryTimeout {
        seconds: timeout_seconds,
    }))
}

fn query_cancelled_error() -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::External(Box::new(QueryError::Cancel))
}

#[derive(Default, Clone)]
pub struct SimpleQueryDispatcherBuilder {
    input: Option<Arc<SelectObjectContentInput>>,
    default_table_provider: Option<TableHandleProviderRef>,
    session_factory: Option<Arc<SessionCtxFactory>>,
    parser: Option<Arc<dyn Parser + Send + Sync>>,

    query_execution_factory: Option<QueryExecutionFactoryRef>,

    func_manager: Option<FuncMetaManagerRef>,
    memory_limit_bytes: Option<usize>,
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

    pub fn with_memory_limit_bytes(mut self, memory_limit_bytes: usize) -> Self {
        if memory_limit_bytes > 0 {
            self.memory_limit_bytes = Some(memory_limit_bytes);
        }
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

        let memory_limit_bytes = self.memory_limit_bytes.unwrap_or(DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES);
        let query_admission = self
            .query_admission
            .unwrap_or_else(|| Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_QUERIES)));
        let query_timeout = self
            .query_timeout
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS));

        let dispatcher = Arc::new(SimpleQueryDispatcher {
            input,
            _default_table_provider: default_table_provider,
            session_factory,
            parser,
            query_execution_factory,
            func_manager,
            memory_limit_bytes,
            query_admission,
            query_timeout,
            query_execution_owner: QueryExecutionOwner::new(),
        });

        Ok(dispatcher)
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryPhaseGuard, SimpleQueryDispatcher, SimpleQueryDispatcherBuilder, TrackedRecordBatchStream};
    use crate::{
        execution::{
            factory::{QueryExecutionFactoryRef, SqlQueryExecutionFactory},
            scheduler::local::LocalScheduler,
        },
        function::simple_func_manager::SimpleFunctionMetadataManager,
        instance::{DEFAULT_MAX_CONCURRENT_QUERIES, DEFAULT_QUERY_TIMEOUT_SECS},
        metadata::base_table::BaseTableProvider,
        sql::{optimizer::CascadeOptimizerBuilder, parser::DefaultParser},
    };
    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            datatypes::{Schema, SchemaRef},
            record_batch::RecordBatch,
        },
        common::DataFusionError,
        physical_plan::{RecordBatchStream, stream::RecordBatchStreamAdapter},
    };
    use futures::{StreamExt, stream};
    use rustfs_s3select_api::{
        QueryError, QueryResult, S3SelectPolicyError,
        query::{
            Context as QueryContext, Query,
            dispatcher::QueryDispatcher,
            execution::{
                Output, QueryExecution, QueryExecutionFactory, QueryExecutionRef, QueryStateMachine, QueryStateMachineRef,
            },
            logical_planner::Plan,
            session::{
                DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES, QueryExecutionOwner, QueryExecutionStatus, QueryExecutionTracker,
                SessionCtxFactory,
            },
        },
    };
    use s3s::dto::{
        CSVInput, CSVOutput, ExpressionType, FileHeaderInfo, InputSerialization, OutputSerialization, SelectObjectContentInput,
        SelectObjectContentRequest,
    };
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::Poll,
        time::Duration,
    };
    use tokio::{
        sync::{Barrier, Semaphore},
        time::Instant,
    };

    async fn wait_for_query_timeout(query_tracker: &QueryExecutionTracker) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while query_tracker.status() != QueryExecutionStatus::TimedOut {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("query deadline should expire");
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct PanickingSchemaStream {
        dropped: Arc<AtomicBool>,
        _drop_guard: BlockingDrop,
    }

    impl futures::Stream for PanickingSchemaStream {
        type Item = Result<RecordBatch, DataFusionError>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl RecordBatchStream for PanickingSchemaStream {
        fn schema(&self) -> SchemaRef {
            panic!("test stream schema panic");
        }
    }

    struct PanickingSchemaQueryExecutionFactory {
        dropped: Arc<AtomicBool>,
        drop_guard: std::sync::Mutex<Option<BlockingDrop>>,
    }

    #[async_trait]
    impl QueryExecutionFactory for PanickingSchemaQueryExecutionFactory {
        async fn create_query_execution(
            &self,
            _plan: Plan,
            _query_state_machine: QueryStateMachineRef,
        ) -> QueryResult<QueryExecutionRef> {
            let drop_guard = self
                .drop_guard
                .lock()
                .expect("panic factory mutex should not be poisoned")
                .take()
                .expect("panic factory should be called once");
            Ok(Arc::new(PanickingSchemaQueryExecution {
                dropped: Arc::clone(&self.dropped),
                drop_guard: std::sync::Mutex::new(Some(drop_guard)),
            }))
        }
    }

    struct PanickingSchemaQueryExecution {
        dropped: Arc<AtomicBool>,
        drop_guard: std::sync::Mutex<Option<BlockingDrop>>,
    }

    #[async_trait]
    impl QueryExecution for PanickingSchemaQueryExecution {
        async fn start(&self) -> QueryResult<Output> {
            let drop_guard = self
                .drop_guard
                .lock()
                .expect("panic execution mutex should not be poisoned")
                .take()
                .expect("panic execution should start once");
            Ok(Output::StreamData(Box::pin(PanickingSchemaStream {
                dropped: Arc::clone(&self.dropped),
                _drop_guard: drop_guard,
            })))
        }

        fn cancel(&self) -> QueryResult<()> {
            Ok(())
        }
    }

    impl Drop for PanickingSchemaStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    struct BlockingDrop {
        started: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            let _ = self.started.send(());
            self.release.recv().expect("release blocking drop");
        }
    }

    struct BlockingError {
        started: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl Drop for BlockingError {
        fn drop(&mut self) {
            let _ = self.started.send(());
            self.release
                .lock()
                .expect("blocking error mutex should not be poisoned")
                .recv()
                .expect("release blocking error");
        }
    }

    impl std::fmt::Debug for BlockingError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("BlockingError").finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for BlockingError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("blocking drop test error")
        }
    }

    impl std::error::Error for BlockingError {}

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

    struct DropBlockingPendingQueryExecutionFactory {
        drop_guard: std::sync::Mutex<Option<BlockingDrop>>,
    }

    #[async_trait]
    impl QueryExecutionFactory for DropBlockingPendingQueryExecutionFactory {
        async fn create_query_execution(
            &self,
            _plan: Plan,
            _query_state_machine: QueryStateMachineRef,
        ) -> QueryResult<QueryExecutionRef> {
            let _drop_guard = self
                .drop_guard
                .lock()
                .expect("pending factory mutex should not be poisoned")
                .take()
                .expect("pending factory should be called once");
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

    fn test_query_tracker(
        permit: tokio::sync::OwnedSemaphorePermit,
        deadline: Instant,
        timeout_seconds: u64,
    ) -> (QueryExecutionOwner, QueryExecutionTracker) {
        let owner = QueryExecutionOwner::new();
        let query_tracker = QueryExecutionTracker::new(&owner, Arc::new(permit), deadline, timeout_seconds);
        assert!(query_tracker.mark_admitted(&owner));
        assert!(query_tracker.claim_planning(&owner));
        assert!(query_tracker.mark_planned(&owner));
        assert!(query_tracker.claim_execution(&owner));
        assert!(query_tracker.mark_running(&owner));
        (owner, query_tracker)
    }

    #[test]
    fn builder_uses_query_limit_defaults_when_omitted() {
        let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
        let scheduler = Arc::new(LocalScheduler {});
        let dispatcher = SimpleQueryDispatcherBuilder::default()
            .with_input(Arc::new(test_input()))
            .with_default_table_provider(Arc::new(BaseTableProvider::default()))
            .with_session_factory(Arc::new(SessionCtxFactory::new(true)))
            .with_parser(Arc::new(DefaultParser::default()))
            .with_query_execution_factory(Arc::new(SqlQueryExecutionFactory::new(optimizer, scheduler)))
            .with_func_manager(Arc::new(SimpleFunctionMetadataManager::default()))
            .build()
            .expect("legacy builder chain should use default query limits");

        assert_eq!(dispatcher.query_admission.available_permits(), DEFAULT_MAX_CONCURRENT_QUERIES);
        assert_eq!(dispatcher.query_timeout, Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS));
        assert_eq!(dispatcher.memory_limit_bytes, DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES);
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

        assert!(matches!(
            result,
            Err(ref err) if matches!(err.s3_select_policy_error(), Some(S3SelectPolicyError::QueryConcurrencyLimit))
        ));
    }

    #[tokio::test]
    async fn staged_query_rejects_when_admission_is_saturated() {
        let admission = Arc::new(Semaphore::new(1));
        let _held_permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let (dispatcher, input) = test_dispatcher(admission, Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());

        let result = dispatcher.build_query_state_machine(query).await;

        assert!(matches!(
            result,
            Err(ref err) if matches!(err.s3_select_policy_error(), Some(S3SelectPolicyError::QueryConcurrencyLimit))
        ));
    }

    #[tokio::test]
    async fn untracked_query_state_machine_cannot_bypass_limits() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher(admission, Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let session = SessionCtxFactory::new(true)
            .create_session_ctx(query.context())
            .await
            .expect("untracked session should be available for compatibility");
        let query_state_machine = Arc::new(QueryStateMachine::begin(query, session));

        let result = dispatcher.build_logical_plan(query_state_machine).await;

        assert!(matches!(result, Err(QueryError::Cancel)));
    }

    #[tokio::test]
    async fn staged_query_rejects_unbound_session() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher(Arc::clone(&admission), Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let untracked_session = SessionCtxFactory::new(true)
            .create_session_ctx(query.context())
            .await
            .expect("untracked session should be available");
        let mut query_state_machine = dispatcher
            .build_query_state_machine(query)
            .await
            .expect("staged query should acquire admission");
        let query_tracker = query_state_machine
            .query_tracker()
            .cloned()
            .expect("staged query should retain its tracker");
        assert!(matches!(
            QueryStateMachine::begin_tracked(query_state_machine.query.clone(), untracked_session.clone(), query_tracker),
            Err(QueryError::Cancel)
        ));
        Arc::get_mut(&mut query_state_machine)
            .expect("test should hold the only state machine reference")
            .session = untracked_session;

        assert!(matches!(
            dispatcher.build_logical_plan(query_state_machine).await,
            Err(QueryError::Cancel)
        ));
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn staged_execution_rejects_session_replaced_after_planning() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher(Arc::clone(&admission), Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let untracked_session = SessionCtxFactory::new(true)
            .create_session_ctx(query.context())
            .await
            .expect("untracked session should be available");
        let mut query_state_machine = dispatcher
            .build_query_state_machine(query)
            .await
            .expect("staged query should acquire admission");
        let logical_plan = dispatcher
            .build_logical_plan(Arc::clone(&query_state_machine))
            .await
            .expect("staged query should build a logical plan")
            .expect("select query should produce a logical plan");
        Arc::get_mut(&mut query_state_machine)
            .expect("test should hold the only state machine reference")
            .session = untracked_session;

        assert!(matches!(
            dispatcher.execute_logical_plan(logical_plan, query_state_machine).await,
            Err(QueryError::Cancel)
        ));
        assert_eq!(admission.available_permits(), 1);
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

        assert!(matches!(
            result,
            Err(ref err) if matches!(err.s3_select_policy_error(), Some(S3SelectPolicyError::QueryTimeout { seconds: 0 }))
        ));
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn staged_query_execution_rejects_expired_tracker() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher_with_factory(
            Arc::clone(&admission),
            Duration::from_secs(300),
            Arc::new(PendingQueryExecutionFactory),
        );
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let query_state_machine = dispatcher
            .build_query_state_machine(query)
            .await
            .expect("staged query should acquire admission");
        let logical_plan = dispatcher
            .build_logical_plan(Arc::clone(&query_state_machine))
            .await
            .expect("staged query should build a logical plan")
            .expect("select query should produce a logical plan");
        query_state_machine
            .query_tracker()
            .expect("staged query should retain its tracker")
            .expire(&dispatcher.query_execution_owner);

        assert!(matches!(
            dispatcher.execute_logical_plan(logical_plan, query_state_machine).await,
            Err(ref err) if matches!(err.s3_select_policy_error(), Some(S3SelectPolicyError::QueryTimeout { seconds: 300 }))
        ));
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn staged_query_stream_owns_shared_admission_tracker() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher(Arc::clone(&admission), Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let query_state_machine = dispatcher
            .build_query_state_machine(query)
            .await
            .expect("staged query should acquire admission");
        let retained_state_machine = Arc::clone(&query_state_machine);
        let logical_plan = dispatcher
            .build_logical_plan(Arc::clone(&query_state_machine))
            .await
            .expect("staged query should build a logical plan")
            .expect("select query should produce a logical plan");
        let output = dispatcher
            .execute_logical_plan(logical_plan, query_state_machine)
            .await
            .expect("staged query should start execution");

        assert!(Arc::clone(&admission).try_acquire_owned().is_err());
        drop(output);
        assert_eq!(admission.available_permits(), 1);
        assert!(matches!(
            dispatcher.build_logical_plan(retained_state_machine).await,
            Err(QueryError::Cancel)
        ));
    }

    #[tokio::test]
    async fn staged_query_rejects_duplicate_phase_calls() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher(Arc::clone(&admission), Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let query_state_machine = dispatcher
            .build_query_state_machine(query)
            .await
            .expect("staged query should acquire admission");
        let logical_plan = dispatcher
            .build_logical_plan(Arc::clone(&query_state_machine))
            .await
            .expect("staged query should build a logical plan")
            .expect("select query should produce a logical plan");

        assert!(matches!(
            dispatcher.build_logical_plan(Arc::clone(&query_state_machine)).await,
            Err(QueryError::Cancel)
        ));

        let duplicate_plan = logical_plan.clone();
        let output = dispatcher
            .execute_logical_plan(logical_plan, Arc::clone(&query_state_machine))
            .await
            .expect("first staged execution should start");
        assert!(matches!(
            dispatcher.execute_logical_plan(duplicate_plan, query_state_machine).await,
            Err(QueryError::Cancel)
        ));
        assert_eq!(admission.available_permits(), 0);

        drop(output);
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_planning_claim_has_single_winner() {
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let owner = QueryExecutionOwner::new();
        let query_tracker = QueryExecutionTracker::new(&owner, Arc::new(permit), Instant::now() + Duration::from_secs(300), 300);
        assert!(query_tracker.mark_admitted(&owner));
        const CONTENDERS: usize = 16;
        let barrier = Arc::new(Barrier::new(CONTENDERS + 1));
        let mut claims = Vec::with_capacity(CONTENDERS);
        for _ in 0..CONTENDERS {
            let task_barrier = Arc::clone(&barrier);
            let task_tracker = query_tracker.clone();
            let task_owner = owner.clone();
            claims.push(tokio::spawn(async move {
                task_barrier.wait().await;
                task_tracker.claim_planning(&task_owner)
            }));
        }
        barrier.wait().await;

        let mut successful_claims = 0;
        for claim in claims {
            successful_claims += usize::from(claim.await.expect("planning claim task should finish"));
        }
        assert_eq!(successful_claims, 1);
        query_tracker.finish(&owner);
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn staged_query_deadline_releases_retained_admission() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher(Arc::clone(&admission), Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission semaphore should remain open");
        let query_tracker = QueryExecutionTracker::new(
            &dispatcher.query_execution_owner,
            Arc::new(permit),
            Instant::now() + Duration::from_millis(100),
            1,
        );
        let session = SessionCtxFactory::new(true)
            .create_session_ctx_with_tracker_and_memory_limit(
                query.context(),
                query_tracker.clone(),
                DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES,
            )
            .await
            .expect("tracked test session should be available");
        assert!(query_tracker.mark_admitted(&dispatcher.query_execution_owner));
        let query_state_machine = Arc::new(
            QueryStateMachine::begin_tracked(query, session, query_tracker)
                .expect("tracked state machine should accept its bound session"),
        );

        wait_for_query_timeout(
            query_state_machine
                .query_tracker()
                .expect("tracked query should retain its tracker"),
        )
        .await;

        assert_eq!(admission.available_permits(), 1);
        assert!(matches!(
            dispatcher.build_logical_plan(query_state_machine).await,
            Err(ref err) if matches!(err.s3_select_policy_error(), Some(S3SelectPolicyError::QueryTimeout { seconds: 1 }))
        ));
    }

    #[tokio::test]
    async fn planned_query_deadline_releases_retained_admission() {
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let owner = QueryExecutionOwner::new();
        let query_tracker = QueryExecutionTracker::new(&owner, Arc::new(permit), Instant::now() + Duration::from_millis(10), 1);
        assert!(query_tracker.mark_admitted(&owner));
        assert!(query_tracker.claim_planning(&owner));
        assert!(query_tracker.mark_planned(&owner));

        wait_for_query_timeout(&query_tracker).await;

        assert_eq!(query_tracker.status(), QueryExecutionStatus::TimedOut);
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn active_phase_deadline_waits_for_phase_drop() {
        let setup_admission = Arc::new(Semaphore::new(1));
        let setup_permit = Arc::clone(&setup_admission)
            .acquire_owned()
            .await
            .expect("setup permit should be available");
        let setup_owner = QueryExecutionOwner::new();
        let setup_tracker =
            QueryExecutionTracker::new(&setup_owner, Arc::new(setup_permit), Instant::now() + Duration::from_millis(10), 1);
        let setup_guard = QueryPhaseGuard::new(&setup_tracker, &setup_owner);

        wait_for_query_timeout(&setup_tracker).await;

        assert_eq!(setup_tracker.status(), QueryExecutionStatus::TimedOut);
        assert_eq!(setup_admission.available_permits(), 0);
        drop(setup_guard);
        assert_eq!(setup_admission.available_permits(), 1);

        let planning_admission = Arc::new(Semaphore::new(1));
        let planning_permit = Arc::clone(&planning_admission)
            .acquire_owned()
            .await
            .expect("planning permit should be available");
        let planning_owner = QueryExecutionOwner::new();
        let planning_tracker =
            QueryExecutionTracker::new(&planning_owner, Arc::new(planning_permit), Instant::now() + Duration::from_millis(10), 1);
        assert!(planning_tracker.mark_admitted(&planning_owner));
        assert!(planning_tracker.claim_planning(&planning_owner));
        let planning_guard = QueryPhaseGuard::new(&planning_tracker, &planning_owner);

        wait_for_query_timeout(&planning_tracker).await;

        assert_eq!(planning_tracker.status(), QueryExecutionStatus::TimedOut);
        assert_eq!(planning_admission.available_permits(), 0);
        drop(planning_guard);
        assert_eq!(planning_admission.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn post_deadline_result_drops_before_admission_release() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, _) = test_dispatcher(Arc::clone(&admission), Duration::from_secs(300));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let query_tracker = QueryExecutionTracker::new(
            &dispatcher.query_execution_owner,
            Arc::new(permit),
            Instant::now() + Duration::from_millis(10),
            1,
        );
        let (drop_started_tx, drop_started_rx) = std::sync::mpsc::channel();
        let (release_drop_tx, release_drop_rx) = std::sync::mpsc::channel();
        let task_dispatcher = Arc::clone(&dispatcher);
        let task_tracker = query_tracker.clone();
        let task = tokio::spawn(async move {
            let _phase_guard = QueryPhaseGuard::new(&task_tracker, &task_dispatcher.query_execution_owner);
            task_dispatcher
                .run_with_query_deadline(&task_tracker, async move {
                    std::thread::sleep(Duration::from_millis(20));
                    Ok(BlockingDrop {
                        started: drop_started_tx,
                        release: release_drop_rx,
                    })
                })
                .await
        });
        let drop_started = tokio::task::spawn_blocking(move || drop_started_rx.recv())
            .await
            .expect("drop observer task should finish");
        drop_started.expect("post-deadline result should be dropped");

        assert_eq!(admission.available_permits(), 0);
        release_drop_tx.send(()).expect("release result drop");
        assert!(matches!(
            task.await.expect("deadline task should finish"),
            Err(ref err) if matches!(err.s3_select_policy_error(), Some(S3SelectPolicyError::QueryTimeout { seconds: 1 }))
        ));
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn staged_query_planning_error_releases_admission() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher(Arc::clone(&admission), Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM".to_string());
        let query_state_machine = dispatcher
            .build_query_state_machine(query)
            .await
            .expect("staged query should acquire admission");
        let retained_state_machine = Arc::clone(&query_state_machine);

        assert!(matches!(
            dispatcher.build_logical_plan(query_state_machine).await,
            Err(QueryError::Parser { .. })
        ));
        assert_eq!(admission.available_permits(), 1);
        assert!(matches!(
            dispatcher.build_logical_plan(retained_state_machine).await,
            Err(QueryError::Cancel)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_execution_start_drops_future_before_releasing_admission() {
        let admission = Arc::new(Semaphore::new(1));
        let (drop_started_tx, drop_started_rx) = std::sync::mpsc::channel();
        let (release_drop_tx, release_drop_rx) = std::sync::mpsc::channel();
        let (dispatcher, input) = test_dispatcher_with_factory(
            Arc::clone(&admission),
            Duration::from_secs(300),
            Arc::new(DropBlockingPendingQueryExecutionFactory {
                drop_guard: std::sync::Mutex::new(Some(BlockingDrop {
                    started: drop_started_tx,
                    release: release_drop_rx,
                })),
            }),
        );
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let query_state_machine = dispatcher
            .build_query_state_machine(query)
            .await
            .expect("staged query should acquire admission");
        let logical_plan = dispatcher
            .build_logical_plan(Arc::clone(&query_state_machine))
            .await
            .expect("staged query should build a logical plan")
            .expect("select query should produce a logical plan");
        let retained_state_machine = Arc::clone(&query_state_machine);
        let task_dispatcher = Arc::clone(&dispatcher);
        let mut execution =
            Box::pin(async move { task_dispatcher.execute_logical_plan(logical_plan, query_state_machine).await });

        assert!(futures::poll!(execution.as_mut()).is_pending());
        let drop_task = tokio::task::spawn_blocking(move || drop(execution));
        tokio::task::spawn_blocking(move || drop_started_rx.recv())
            .await
            .expect("drop observer task should finish")
            .expect("cancelled execution future should be dropped");

        assert_eq!(admission.available_permits(), 0);
        release_drop_tx.send(()).expect("release execution future drop");
        drop_task.await.expect("execution drop task should finish");
        assert_eq!(admission.available_permits(), 1);
        assert!(matches!(
            dispatcher.build_logical_plan(retained_state_machine).await,
            Err(QueryError::Cancel)
        ));
    }

    #[tokio::test]
    async fn forged_query_tracker_cannot_bypass_dispatcher_admission() {
        let admission = Arc::new(Semaphore::new(1));
        let (dispatcher, input) = test_dispatcher(Arc::clone(&admission), Duration::from_secs(300));
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let forged_permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("forged permit should be available");
        let forged_owner = QueryExecutionOwner::new();
        let forged_tracker =
            QueryExecutionTracker::new(&forged_owner, Arc::new(forged_permit), Instant::now() + Duration::from_secs(300), 300);
        assert!(forged_tracker.mark_admitted(&forged_owner));
        forged_tracker.finish(&dispatcher.query_execution_owner);
        forged_tracker.expire(&dispatcher.query_execution_owner);
        assert_eq!(forged_tracker.status(), QueryExecutionStatus::Active);
        assert_eq!(admission.available_permits(), 0);
        let session = SessionCtxFactory::new(true)
            .create_session_ctx_with_tracker_and_memory_limit(
                query.context(),
                forged_tracker.clone(),
                DEFAULT_S3SELECT_MEMORY_LIMIT_BYTES,
            )
            .await
            .expect("tracked test session should be available");
        let query_state_machine = Arc::new(
            QueryStateMachine::begin_tracked(query, session, forged_tracker)
                .expect("forged state machine should accept its own bound session"),
        );
        let retained_state_machine = Arc::clone(&query_state_machine);

        assert!(matches!(
            dispatcher.build_logical_plan(query_state_machine).await,
            Err(QueryError::Cancel)
        ));
        assert_eq!(admission.available_permits(), 0);
        drop(retained_state_machine);
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
        let (owner, query_tracker) = test_query_tracker(permit, Instant::now() + Duration::from_millis(10), 300);
        let mut output = Box::pin(TrackedRecordBatchStream::new(inner, query_tracker, owner));

        let err = output
            .next()
            .await
            .expect("timeout error")
            .expect_err("expired query must fail");
        let DataFusionError::External(source) = err else {
            panic!("expected external query error");
        };
        assert!(matches!(
            source.downcast_ref::<S3SelectPolicyError>(),
            Some(S3SelectPolicyError::QueryTimeout { seconds: 300 })
        ));
        assert!(inner_dropped.load(Ordering::SeqCst));
        assert_eq!(admission.available_permits(), 1);
        assert!(output.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_handoff_panic_releases_admission_after_inner_drop() {
        let admission = Arc::new(Semaphore::new(1));
        let inner_dropped = Arc::new(AtomicBool::new(false));
        let (drop_started_tx, drop_started_rx) = std::sync::mpsc::channel();
        let (release_drop_tx, release_drop_rx) = std::sync::mpsc::channel();
        let (dispatcher, input) = test_dispatcher_with_factory(
            Arc::clone(&admission),
            Duration::from_secs(300),
            Arc::new(PanickingSchemaQueryExecutionFactory {
                dropped: Arc::clone(&inner_dropped),
                drop_guard: std::sync::Mutex::new(Some(BlockingDrop {
                    started: drop_started_tx,
                    release: release_drop_rx,
                })),
            }),
        );
        let query = Query::new(QueryContext { input }, "SELECT * FROM S3Object".to_string());
        let query_state_machine = dispatcher
            .build_query_state_machine(query)
            .await
            .expect("staged query should acquire admission");
        let logical_plan = dispatcher
            .build_logical_plan(Arc::clone(&query_state_machine))
            .await
            .expect("staged query should build a logical plan")
            .expect("select query should produce a logical plan");
        let retained_state_machine = Arc::clone(&query_state_machine);
        let task_dispatcher = Arc::clone(&dispatcher);
        let task = tokio::spawn(async move { task_dispatcher.execute_logical_plan(logical_plan, query_state_machine).await });
        tokio::task::spawn_blocking(move || drop_started_rx.recv())
            .await
            .expect("drop observer task should finish")
            .expect("panicking stream should start dropping");

        assert!(inner_dropped.load(Ordering::SeqCst));
        assert_eq!(admission.available_permits(), 0);
        release_drop_tx.send(()).expect("release stream drop");
        let Err(join_error) = task.await else {
            panic!("stream schema should panic");
        };
        assert!(join_error.is_panic());
        assert_eq!(admission.available_permits(), 1);
        assert_eq!(
            retained_state_machine
                .query_tracker()
                .expect("staged query should retain its tracker")
                .status(),
            QueryExecutionStatus::Finished
        );
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
        let (owner, query_tracker) = test_query_tracker(permit, Instant::now() + Duration::from_millis(10), 300);
        let _output = TrackedRecordBatchStream::new(inner, query_tracker, owner);

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
        let (owner, query_tracker) = test_query_tracker(permit, Instant::now() + Duration::from_secs(300), 300);
        let mut output = Box::pin(TrackedRecordBatchStream::new(inner, query_tracker, owner));

        assert!(output.next().await.is_none());
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn query_timeout_during_inner_poll_returns_error() {
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream::pending::<Result<RecordBatch, DataFusionError>>(),
        ));
        let (owner, query_tracker) = test_query_tracker(permit, Instant::now() + Duration::from_secs(300), 300);
        let mut output = Box::pin(TrackedRecordBatchStream::new(inner, query_tracker, owner));
        let deadline_state = Arc::clone(&output.state);
        let poll_state = Arc::clone(&deadline_state);
        *deadline_state.inner.lock() = Some(Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream::poll_fn(move |_| {
                poll_state.query_tracker.expire(&poll_state.query_execution_owner);
                Poll::Ready(None::<Result<RecordBatch, DataFusionError>>)
            }),
        )));

        let err = output
            .next()
            .await
            .expect("timeout error")
            .expect_err("timeout racing with inner poll must fail");
        let DataFusionError::External(source) = err else {
            panic!("expected external query error");
        };
        assert!(matches!(
            source.downcast_ref::<S3SelectPolicyError>(),
            Some(S3SelectPolicyError::QueryTimeout { seconds: 300 })
        ));
        assert_eq!(admission.available_permits(), 1);
        assert!(output.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeout_drops_late_stream_item_before_releasing_admission() {
        let admission = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&admission)
            .acquire_owned()
            .await
            .expect("admission permit should be available");
        let (drop_started_tx, drop_started_rx) = std::sync::mpsc::channel();
        let (release_drop_tx, release_drop_rx) = std::sync::mpsc::channel();
        let mut drop_guard = Some(BlockingError {
            started: drop_started_tx,
            release: std::sync::Mutex::new(release_drop_rx),
        });
        let inner = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream::poll_fn(move |_| {
                std::thread::sleep(Duration::from_millis(20));
                Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                    drop_guard.take().expect("late item should be returned once"),
                )))))
            }),
        ));
        let (owner, query_tracker) = test_query_tracker(permit, Instant::now() + Duration::from_millis(10), 1);
        let mut output = Box::pin(TrackedRecordBatchStream::new(inner, query_tracker, owner));
        let task = tokio::spawn(async move {
            let result = output.next().await;
            (result, output)
        });
        tokio::task::spawn_blocking(move || drop_started_rx.recv())
            .await
            .expect("drop observer task should finish")
            .expect("late stream item should start dropping");

        assert_eq!(admission.available_permits(), 0);
        release_drop_tx.send(()).expect("release late item drop");
        let (result, mut output) = task.await.expect("stream poll task should finish");
        let error = result
            .expect("timeout error should be returned")
            .expect_err("late item must be replaced by a timeout");
        let DataFusionError::External(source) = error else {
            panic!("expected external query error");
        };
        assert!(matches!(
            source.downcast_ref::<S3SelectPolicyError>(),
            Some(S3SelectPolicyError::QueryTimeout { seconds: 1 })
        ));
        assert_eq!(admission.available_permits(), 1);
        assert!(output.next().await.is_none());
    }
}
