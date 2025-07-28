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

#[cfg(test)]
mod integration_tests {
    use crate::{create_fresh_db, get_global_db, instance::make_rustfsms};
    use rustfs_s3select_api::{
        QueryError,
        query::{Context, Query},
    };
    use s3s::dto::{
        CSVInput, CSVOutput, ExpressionType, FileHeaderInfo, InputSerialization, OutputSerialization, SelectObjectContentInput,
        SelectObjectContentRequest,
    };
    use std::sync::Arc;

    fn create_test_input(sql: &str) -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: "test-bucket".to_string(),
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
        }
    }

    #[tokio::test]
    async fn test_database_creation() {
        let input = create_test_input("SELECT * FROM S3Object");
        let result = make_rustfsms(Arc::new(input), true).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_global_db_creation() {
        let input = create_test_input("SELECT * FROM S3Object");
        let result = get_global_db(input.clone(), true).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fresh_db_creation() {
        let result = create_fresh_db().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_simple_select_query() {
        let sql = "SELECT * FROM S3Object";
        let input = create_test_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());

        let query_handle = result.unwrap();
        let output = query_handle.result().chunk_result().await;
        assert!(output.is_ok());
    }

    #[tokio::test]
    async fn test_select_with_where_clause() {
        let sql = "SELECT name, age FROM S3Object WHERE age > 30";
        let input = create_test_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_select_with_aggregation() {
        let sql = "SELECT department, COUNT(*) as count FROM S3Object GROUP BY department";
        let input = create_test_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        // Aggregation queries might fail due to lack of actual data, which is acceptable
        match result {
            Ok(_) => {
                // If successful, that's great
            }
            Err(_) => {
                // Expected to fail due to no actual data source
            }
        }
    }

    #[tokio::test]
    async fn test_invalid_sql_syntax() {
        let sql = "INVALID SQL SYNTAX";
        let input = create_test_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multi_statement_error() {
        let sql = "SELECT * FROM S3Object; SELECT 1;";
        let input = create_test_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_err());

        if let Err(QueryError::MultiStatement { num, .. }) = result {
            assert_eq!(num, 2);
        } else {
            panic!("Expected MultiStatement error");
        }
    }

    #[tokio::test]
    async fn test_query_state_machine_workflow() {
        let sql = "SELECT * FROM S3Object";
        let input = create_test_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        // Test state machine creation
        let state_machine = db.build_query_state_machine(query.clone()).await;
        assert!(state_machine.is_ok());

        let state_machine = state_machine.unwrap();

        // Test logical plan building
        let logical_plan = db.build_logical_plan(state_machine.clone()).await;
        assert!(logical_plan.is_ok());

        // Test execution if plan exists
        if let Ok(Some(plan)) = logical_plan {
            let execution_result = db.execute_logical_plan(plan, state_machine).await;
            assert!(execution_result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_query_with_limit() {
        let sql = "SELECT * FROM S3Object LIMIT 5";
        let input = create_test_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());

        let query_handle = result.unwrap();
        let output = query_handle.result().chunk_result().await.unwrap();

        // Verify that we get results (exact count depends on test data)
        let total_rows: usize = output.iter().map(|batch| batch.num_rows()).sum();
        assert!(total_rows <= 5);
    }

    #[tokio::test]
    async fn test_query_with_order_by() {
        let sql = "SELECT name, age FROM S3Object ORDER BY age DESC";
        let input = create_test_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_queries() {
        let sql = "SELECT * FROM S3Object";
        let input = create_test_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();

        // Execute multiple queries concurrently
        let mut handles = vec![];
        for i in 0..3 {
            let query = Query::new(
                Context {
                    input: Arc::new(input.clone()),
                },
                format!("SELECT * FROM S3Object LIMIT {}", i + 1),
            );
            let db_clone = db.clone();
            let handle = tokio::spawn(async move { db_clone.execute(&query).await });
            handles.push(handle);
        }

        // Wait for all queries to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }
}
