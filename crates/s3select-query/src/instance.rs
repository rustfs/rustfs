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
    sync::{Arc, LazyLock},
    time::Duration,
};

use async_trait::async_trait;
use derive_builder::Builder;
use rustfs_s3select_api::{
    QueryResult,
    query::{
        Query, dispatcher::QueryDispatcher, execution::QueryStateMachineRef, logical_planner::Plan, session::SessionCtxFactory,
    },
    server::dbms::{DatabaseManagerSystem, QueryHandle},
};
use s3s::dto::SelectObjectContentInput;
use tokio::sync::Semaphore;

use crate::{
    dispatcher::manager::SimpleQueryDispatcherBuilder,
    execution::{factory::SqlQueryExecutionFactory, scheduler::local::LocalScheduler},
    function::simple_func_manager::SimpleFunctionMetadataManager,
    metadata::base_table::BaseTableProvider,
    sql::{optimizer::CascadeOptimizerBuilder, parser::DefaultParser},
};

const ENV_RUSTFS_S3SELECT_TARGET_PARTITIONS: &str = "RUSTFS_S3SELECT_TARGET_PARTITIONS";
const ENV_RUSTFS_S3SELECT_MEMORY_LIMIT_BYTES: &str = "RUSTFS_S3SELECT_MEMORY_LIMIT_BYTES";
const ENV_RUSTFS_S3SELECT_QUERY_TIMEOUT_SECS: &str = "RUSTFS_S3SELECT_QUERY_TIMEOUT_SECS";
const ENV_RUSTFS_S3SELECT_MAX_CONCURRENT_QUERIES: &str = "RUSTFS_S3SELECT_MAX_CONCURRENT_QUERIES";
const DEFAULT_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 300;
const DEFAULT_MAX_CONCURRENT_QUERIES: usize = 4;
const MAX_QUERY_TIMEOUT_SECS: u64 = 24 * 60 * 60;
const TEST_MAX_CONCURRENT_QUERIES: usize = 1024;

static QUERY_ADMISSION: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(S3SelectRuntimeConfig::from_env().max_concurrent_queries)));

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct S3SelectRuntimeConfig {
    target_partitions: usize,
    memory_limit_bytes: usize,
    query_timeout: Duration,
    max_concurrent_queries: usize,
}

impl Default for S3SelectRuntimeConfig {
    fn default() -> Self {
        Self {
            target_partitions: 0,
            memory_limit_bytes: DEFAULT_MEMORY_LIMIT_BYTES,
            query_timeout: Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS),
            max_concurrent_queries: DEFAULT_MAX_CONCURRENT_QUERIES,
        }
    }
}

impl S3SelectRuntimeConfig {
    fn from_env() -> Self {
        Self {
            target_partitions: target_partitions_from_env_value(
                std::env::var(ENV_RUSTFS_S3SELECT_TARGET_PARTITIONS).ok().as_deref(),
            ),
            memory_limit_bytes: bounded_usize_from_env_value(
                std::env::var(ENV_RUSTFS_S3SELECT_MEMORY_LIMIT_BYTES).ok().as_deref(),
                DEFAULT_MEMORY_LIMIT_BYTES,
                usize::MAX,
            ),
            query_timeout: Duration::from_secs(bounded_u64_from_env_value(
                std::env::var(ENV_RUSTFS_S3SELECT_QUERY_TIMEOUT_SECS).ok().as_deref(),
                DEFAULT_QUERY_TIMEOUT_SECS,
                MAX_QUERY_TIMEOUT_SECS,
            )),
            max_concurrent_queries: bounded_usize_from_env_value(
                std::env::var(ENV_RUSTFS_S3SELECT_MAX_CONCURRENT_QUERIES).ok().as_deref(),
                DEFAULT_MAX_CONCURRENT_QUERIES,
                Semaphore::MAX_PERMITS,
            ),
        }
    }
}

fn target_partitions_from_env_value(value: Option<&str>) -> usize {
    value.and_then(|value| value.parse::<usize>().ok()).unwrap_or(0)
}

fn bounded_usize_from_env_value(value: Option<&str>, default: usize, max: usize) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=max).contains(value))
        .unwrap_or(default)
}

fn bounded_u64_from_env_value(value: Option<&str>, default: u64, max: u64) -> u64 {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| (1..=max).contains(value))
        .unwrap_or(default)
}

fn query_admission(is_test: bool) -> Arc<Semaphore> {
    if is_test {
        Arc::new(Semaphore::new(TEST_MAX_CONCURRENT_QUERIES))
    } else {
        Arc::clone(&QUERY_ADMISSION)
    }
}

pub async fn make_rustfsms(input: Arc<SelectObjectContentInput>, is_test: bool) -> QueryResult<impl DatabaseManagerSystem> {
    // init Function Manager, we can define some UDF if need
    let func_manager = SimpleFunctionMetadataManager::default();
    let runtime_config = S3SelectRuntimeConfig::from_env();
    let session_factory = Arc::new(
        SessionCtxFactory::new(is_test)
            .with_target_partitions(runtime_config.target_partitions)
            .with_memory_limit_bytes(runtime_config.memory_limit_bytes),
    );
    let parser = Arc::new(DefaultParser::default());
    let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
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
        .with_query_admission(query_admission(is_test))
        .with_query_timeout(runtime_config.query_timeout)
        .build()?;

    let mut builder = RustFSmsBuilder::default();

    let db_server = builder.query_dispatcher(query_dispatcher).build().expect("build db server");

    Ok(db_server)
}

pub async fn make_rustfsms_with_components(
    input: Arc<SelectObjectContentInput>,
    is_test: bool,
    func_manager: Arc<SimpleFunctionMetadataManager>,
    parser: Arc<DefaultParser>,
    query_execution_factory: Arc<SqlQueryExecutionFactory>,
    default_table_provider: Arc<BaseTableProvider>,
) -> QueryResult<impl DatabaseManagerSystem> {
    let runtime_config = S3SelectRuntimeConfig::from_env();
    let session_factory = Arc::new(
        SessionCtxFactory::new(is_test)
            .with_target_partitions(runtime_config.target_partitions)
            .with_memory_limit_bytes(runtime_config.memory_limit_bytes),
    );

    let query_dispatcher = SimpleQueryDispatcherBuilder::default()
        .with_input(input)
        .with_func_manager(func_manager)
        .with_default_table_provider(default_table_provider)
        .with_session_factory(session_factory)
        .with_parser(parser)
        .with_query_execution_factory(query_execution_factory)
        .with_query_admission(query_admission(is_test))
        .with_query_timeout(runtime_config.query_timeout)
        .build()?;

    let mut builder = RustFSmsBuilder::default();

    let db_server = builder.query_dispatcher(query_dispatcher).build().expect("build db server");

    Ok(db_server)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{arrow::util::pretty, assert_batches_eq};
    use rustfs_s3select_api::query::{Context, Query};
    use s3s::dto::{
        CSVInput, CSVOutput, ExpressionType, FieldDelimiter, FileHeaderInfo, InputSerialization, OutputSerialization,
        RecordDelimiter, SelectObjectContentInput, SelectObjectContentRequest,
    };

    use crate::get_global_db;

    use super::{
        DEFAULT_MAX_CONCURRENT_QUERIES, DEFAULT_MEMORY_LIMIT_BYTES, DEFAULT_QUERY_TIMEOUT_SECS, MAX_QUERY_TIMEOUT_SECS,
        S3SelectRuntimeConfig, bounded_u64_from_env_value, bounded_usize_from_env_value, target_partitions_from_env_value,
    };

    #[test]
    fn parses_target_partitions_from_env_value() {
        assert_eq!(target_partitions_from_env_value(Some("4")), 4);
        assert_eq!(target_partitions_from_env_value(Some("0")), 0);
        assert_eq!(target_partitions_from_env_value(Some("not-a-number")), 0);
        assert_eq!(target_partitions_from_env_value(None), 0);
    }

    #[test]
    fn default_runtime_config_uses_datafusion_default_partitions() {
        let config = S3SelectRuntimeConfig::default();

        assert_eq!(config.target_partitions, 0);
        assert_eq!(config.memory_limit_bytes, DEFAULT_MEMORY_LIMIT_BYTES);
        assert_eq!(config.query_timeout.as_secs(), DEFAULT_QUERY_TIMEOUT_SECS);
        assert_eq!(config.max_concurrent_queries, DEFAULT_MAX_CONCURRENT_QUERIES);
    }

    #[test]
    fn resource_limits_reject_invalid_and_out_of_range_values() {
        assert_eq!(bounded_usize_from_env_value(Some("1024"), 64, 2048), 1024);
        assert_eq!(bounded_usize_from_env_value(Some("0"), 64, 2048), 64);
        assert_eq!(bounded_usize_from_env_value(Some("4096"), 64, 2048), 64);
        assert_eq!(bounded_usize_from_env_value(Some("invalid"), 64, 2048), 64);
        assert_eq!(bounded_u64_from_env_value(Some("30"), 300, MAX_QUERY_TIMEOUT_SECS), 30);
        assert_eq!(bounded_u64_from_env_value(Some("0"), 300, MAX_QUERY_TIMEOUT_SECS), 300);
        assert_eq!(bounded_u64_from_env_value(Some("86401"), 300, MAX_QUERY_TIMEOUT_SECS), 300);
        assert_eq!(bounded_u64_from_env_value(None, 300, MAX_QUERY_TIMEOUT_SECS), 300);
    }

    #[tokio::test]
    #[ignore]
    async fn test_simple_sql() {
        let sql = "select * from S3Object";
        let input = SelectObjectContentInput {
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
        };
        let db = get_global_db(input.clone(), true).await.expect("operation should succeed");
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await.expect("operation should succeed");

        let results = result
            .result()
            .chunk_result()
            .await
            .expect("operation should succeed")
            .to_vec();

        let expected = [
            "+----+---------+-----+------------+--------+",
            "| id | name    | age | department | salary |",
            "+----+---------+-----+------------+--------+",
            "| 1  | Alice   | 25  | HR         | 05000  |",
            "| 2  | Bob     | 30  | IT         | 6000   |",
            "| 3  | Charlie | 35  | Finance    | 7000   |",
            "| 4  | Diana   | 22  | Marketing  | 4500   |",
            "| 5  | Eve     | 28  | IT         | 5500   |",
            "| 6  | Frank   | 40  | Finance    | 8000   |",
            "| 7  | Grace   | 26  | HR         | 5200   |",
            "| 8  | Henry   | 32  | IT         | 6200   |",
            "| 9  | Ivy     | 24  | Marketing  | 4800   |",
            "| 10 | Jack    | 38  | Finance    | 7500   |",
            "+----+---------+-----+------------+--------+",
        ];

        assert_batches_eq!(expected, &results);
        pretty::print_batches(&results).expect("operation should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn test_func_sql() {
        let sql = "SELECT * FROM S3Object s";
        let input = SelectObjectContentInput {
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
        };
        let db = get_global_db(input.clone(), true).await.expect("operation should succeed");
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await.expect("operation should succeed");

        let results = result
            .result()
            .chunk_result()
            .await
            .expect("operation should succeed")
            .to_vec();
        pretty::print_batches(&results).expect("operation should succeed");
    }
}
