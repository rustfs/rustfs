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

use std::sync::Arc;

use api::{
    QueryResult,
    query::{
        Query, dispatcher::QueryDispatcher, execution::QueryStateMachineRef, logical_planner::Plan, session::SessionCtxFactory,
    },
    server::dbms::{DatabaseManagerSystem, QueryHandle},
};
use async_trait::async_trait;
use derive_builder::Builder;
use s3s::dto::SelectObjectContentInput;

use crate::{
    dispatcher::manager::SimpleQueryDispatcherBuilder,
    execution::{factory::SqlQueryExecutionFactory, scheduler::local::LocalScheduler},
    function::simple_func_manager::SimpleFunctionMetadataManager,
    metadata::base_table::BaseTableProvider,
    sql::{optimizer::CascadeOptimizerBuilder, parser::DefaultParser},
};

#[derive(Builder)]
pub struct RustFSms<D: QueryDispatcher> {
    // query dispatcher & query execution
    query_dispatcher: Arc<D>,
}

#[async_trait]
impl<D> DatabaseManagerSystem for RustFSms<D>
where
    D: QueryDispatcher,
{
    async fn execute(&self, query: &Query) -> QueryResult<QueryHandle> {
        let result = self.query_dispatcher.execute_query(query).await?;

        Ok(QueryHandle::new(query.clone(), result))
    }

    async fn build_query_state_machine(&self, query: Query) -> QueryResult<QueryStateMachineRef> {
        let query_state_machine = self.query_dispatcher.build_query_state_machine(query).await?;

        Ok(query_state_machine)
    }

    async fn build_logical_plan(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Option<Plan>> {
        let logical_plan = self.query_dispatcher.build_logical_plan(query_state_machine).await?;

        Ok(logical_plan)
    }

    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<QueryHandle> {
        let query = query_state_machine.query.clone();
        let result = self
            .query_dispatcher
            .execute_logical_plan(logical_plan, query_state_machine)
            .await?;

        Ok(QueryHandle::new(query.clone(), result))
    }
}

pub async fn make_rustfsms(input: Arc<SelectObjectContentInput>, is_test: bool) -> QueryResult<impl DatabaseManagerSystem> {
    // init Function Manager, we can define some UDF if need
    let func_manager = SimpleFunctionMetadataManager::default();
    // TODO session config need load global system config
    let session_factory = Arc::new(SessionCtxFactory { is_test });
    let parser = Arc::new(DefaultParser::default());
    let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
    // TODO wrap, and num_threads configurable
    let scheduler = Arc::new(LocalScheduler {});

    let query_execution_factory = Arc::new(SqlQueryExecutionFactory::new(optimizer, scheduler));

    let default_table_provider = Arc::new(BaseTableProvider::default());

    let query_dispatcher = SimpleQueryDispatcherBuilder::default()
        .with_input(input)
        .with_func_manager(Arc::new(func_manager))
        .with_default_table_provider(default_table_provider)
        .with_session_factory(session_factory)
        .with_parser(parser)
        .with_query_execution_factory(query_execution_factory)
        .build()?;

    let mut builder = RustFSmsBuilder::default();

    let db_server = builder.query_dispatcher(query_dispatcher).build().expect("build db server");

    Ok(db_server)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::{
        query::{Context, Query},
        server::dbms::DatabaseManagerSystem,
    };
    use datafusion::{arrow::util::pretty, assert_batches_eq};
    use s3s::dto::{
        CSVInput, CSVOutput, ExpressionType, FieldDelimiter, FileHeaderInfo, InputSerialization, OutputSerialization,
        RecordDelimiter, SelectObjectContentInput, SelectObjectContentRequest,
    };

    use crate::instance::make_rustfsms;

    #[tokio::test]
    #[ignore]
    async fn test_simple_sql() {
        let sql = "select * from S3Object";
        let input = Arc::new(SelectObjectContentInput {
            bucket: "dandan".to_string(),
            expected_bucket_owner: None,
            key: "test.csv".to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: sql.to_string(),
                expression_type: ExpressionType::from_static("SQL"),
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
        });
        let db = make_rustfsms(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input }, sql.to_string());

        let result = db.execute(&query).await.unwrap();

        let results = result.result().chunk_result().await.unwrap().to_vec();

        let expected = [
            "+----------------+---------+-----+------------+--------+",
            "| id             | name    | age | department | salary |",
            "+----------------+---------+-----+------------+--------+",
            "|             1  | Alice   | 25  | HR         | 5000   |",
            "|             2  | Bob     | 30  | IT         | 6000   |",
            "|             3  | Charlie | 35  | Finance    | 7000   |",
            "|             4  | Diana   | 22  | Marketing  | 4500   |",
            "|             5  | Eve     | 28  | IT         | 5500   |",
            "|             6  | Frank   | 40  | Finance    | 8000   |",
            "|             7  | Grace   | 26  | HR         | 5200   |",
            "|             8  | Henry   | 32  | IT         | 6200   |",
            "|             9  | Ivy     | 24  | Marketing  | 4800   |",
            "|             10 | Jack    | 38  | Finance    | 7500   |",
            "+----------------+---------+-----+------------+--------+",
        ];

        assert_batches_eq!(expected, &results);
        pretty::print_batches(&results).unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_func_sql() {
        let sql = "SELECT * FROM S3Object s";
        let input = Arc::new(SelectObjectContentInput {
            bucket: "dandan".to_string(),
            expected_bucket_owner: None,
            key: "test.csv".to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: sql.to_string(),
                expression_type: ExpressionType::from_static("SQL"),
                input_serialization: InputSerialization {
                    csv: Some(CSVInput {
                        file_header_info: Some(FileHeaderInfo::from_static(FileHeaderInfo::IGNORE)),
                        field_delimiter: Some(FieldDelimiter::from("╦")),
                        record_delimiter: Some(RecordDelimiter::from("\n")),
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
        });
        let db = make_rustfsms(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input }, sql.to_string());

        let result = db.execute(&query).await.unwrap();

        let results = result.result().chunk_result().await.unwrap().to_vec();
        pretty::print_batches(&results).unwrap();
    }
}
