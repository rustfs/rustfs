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
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{
        file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::Result as DFResult,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::{Stream, StreamExt};
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

use crate::{
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
}

#[async_trait]
impl QueryDispatcher for SimpleQueryDispatcher {
    async fn execute_query(&self, query: &Query) -> QueryResult<Output> {
        let query_state_machine = { self.build_query_state_machine(query.clone()).await? };

        let logical_plan = self.build_logical_plan(query_state_machine.clone()).await?;
        let logical_plan = match logical_plan {
            Some(plan) => plan,
            None => return Ok(Output::Nil(())),
        };
        let result = self.execute_logical_plan(logical_plan, query_state_machine).await?;
        Ok(result)
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
            None => return Ok(None),
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

        match execution.start().await {
            Ok(Output::StreamData(stream)) => Ok(Output::StreamData(Box::pin(TrackedRecordBatchStream { inner: stream }))),
            Ok(nil @ Output::Nil(_)) => Ok(nil),
            Err(err) => Err(err),
        }
    }

    async fn build_scheme_provider(&self, session: &SessionCtx) -> QueryResult<MetadataProvider> {
        let path = format!("s3://{}/{}", self.input.bucket, self.input.key);
        let table_path = ListingTableUrl::parse(path)?;
        let (listing_options, need_rename_volume_name, need_ignore_volume_name) =
            if let Some(csv) = self.input.request.input_serialization.csv.as_ref() {
                let mut need_rename_volume_name = false;
                let mut need_ignore_volume_name = false;
                let mut file_format = CsvFormat::default()
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
                if let Some(delimiter) = csv.field_delimiter.as_ref() {
                    if delimiter.len() == 1 {
                        file_format = file_format.with_delimiter(delimiter.as_bytes()[0]);
                    }
                }
                // TODO waiting for processing @junxiang Mu
                // if csv.file_header_info.is_some() {}
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
            } else if self.input.request.input_serialization.parquet.is_some() {
                let file_format = ParquetFormat::new();
                (ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet"), false, false)
            } else if self.input.request.input_serialization.json.is_some() {
                let file_format = JsonFormat::default();
                (ListingOptions::new(Arc::new(file_format)).with_file_extension(".json"), false, false)
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
    inner: SendableRecordBatchStream,
}

impl RecordBatchStream for TrackedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for TrackedRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
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

        let dispatcher = Arc::new(SimpleQueryDispatcher {
            input,
            _default_table_provider: default_table_provider,
            session_factory,
            parser,
            query_execution_factory,
            func_manager,
        });

        Ok(dispatcher)
    }
}
