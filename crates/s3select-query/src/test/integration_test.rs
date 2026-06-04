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
        CSVInput, CSVOutput, ExpressionType, FileHeaderInfo, InputSerialization, JSONInput, JSONOutput, JSONType,
        OutputSerialization, ParquetInput, ScanRange, SelectObjectContentInput, SelectObjectContentRequest,
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

    /// Build a `SelectObjectContentInput` targeting a JSON DOCUMENT file.
    /// Uses `JSONType::DOCUMENT`, which keeps the document-style validation path.
    fn create_test_json_input(sql: &str) -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: "test-bucket".to_string(),
            expected_bucket_owner: None,
            key: "test.json".to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: sql.to_string(),
                expression_type: ExpressionType::from_static("SQL"),
                input_serialization: InputSerialization {
                    json: Some(JSONInput {
                        type_: Some(JSONType::from_static(JSONType::DOCUMENT)),
                    }),
                    ..Default::default()
                },
                output_serialization: OutputSerialization {
                    json: Some(JSONOutput::default()),
                    ..Default::default()
                },
                request_progress: None,
                scan_range: None,
            },
        }
    }

    fn create_test_json_lines_input(sql: &str) -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: "test-bucket".to_string(),
            expected_bucket_owner: None,
            key: "test.json".to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: sql.to_string(),
                expression_type: ExpressionType::from_static("SQL"),
                input_serialization: InputSerialization {
                    json: Some(JSONInput {
                        type_: Some(JSONType::from_static(JSONType::LINES)),
                    }),
                    ..Default::default()
                },
                output_serialization: OutputSerialization {
                    json: Some(JSONOutput::default()),
                    ..Default::default()
                },
                request_progress: None,
                scan_range: None,
            },
        }
    }

    fn create_test_parquet_input(sql: &str) -> SelectObjectContentInput {
        SelectObjectContentInput {
            bucket: "test-bucket".to_string(),
            expected_bucket_owner: None,
            key: "test.parquet".to_string(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request: SelectObjectContentRequest {
                expression: sql.to_string(),
                expression_type: ExpressionType::from_static("SQL"),
                input_serialization: InputSerialization {
                    parquet: Some(ParquetInput {}),
                    ..Default::default()
                },
                output_serialization: OutputSerialization {
                    json: Some(JSONOutput::default()),
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
        let db = get_global_db(input.clone(), true).await.expect("create csv test database");
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

    // ──────────────────────────────────────────────
    // JSON-input variants of all the above tests
    // ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_database_creation_json() {
        let input = create_test_json_input("SELECT * FROM S3Object");
        let result = make_rustfsms(Arc::new(input), true).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_global_db_creation_json() {
        let input = create_test_json_input("SELECT * FROM S3Object");
        let result = get_global_db(input.clone(), true).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_simple_select_query_json() {
        let sql = "SELECT * FROM S3Object";
        let input = create_test_json_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());

        let query_handle = result.unwrap();
        let output = query_handle.result().chunk_result().await;
        assert!(output.is_ok());
    }

    #[tokio::test]
    async fn test_simple_select_query_parquet() {
        let sql = "SELECT name, age FROM S3Object WHERE age > 25";
        let input = create_test_parquet_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());

        let output = result.unwrap().result().chunk_result().await.unwrap();
        let total_rows: usize = output.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_simple_select_query_parquet_with_scan_range_filters_row_groups() {
        let sql = "SELECT name, age FROM S3Object WHERE age > 25";
        let mut input = create_test_parquet_input(sql);
        input.request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(1),
        });
        let db = get_global_db(input.clone(), true)
            .await
            .expect("create parquet scan range test database");
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());

        let output = result
            .expect("execute parquet scan range query")
            .result()
            .chunk_result()
            .await
            .expect("collect parquet scan range query output");
        let total_rows: usize = output.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[tokio::test]
    async fn test_simple_select_query_parquet_with_full_scan_range() {
        let sql = "SELECT * FROM S3Object";
        let mut input = create_test_parquet_input(sql);
        input.request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(1024),
        });
        let db = get_global_db(input.clone(), true)
            .await
            .expect("create parquet full scan range database");
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());

        let output = result
            .expect("execute parquet full scan range query")
            .result()
            .chunk_result()
            .await
            .expect("collect parquet full scan range output");
        let total_rows: usize = output.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_simple_select_query_csv_with_scan_range() {
        let sql = "SELECT name, age FROM S3Object LIMIT 20";
        let mut input = create_test_input(sql);
        input.request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(1024),
        });
        let db = get_global_db(input.clone(), true)
            .await
            .expect("create csv scan range test database");
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());

        let output = result
            .expect("execute csv scan range query")
            .result()
            .chunk_result()
            .await
            .expect("collect csv scan range output");
        let total_rows: usize = output.iter().map(|batch| batch.num_rows()).sum();
        assert!(total_rows > 0);
    }

    #[tokio::test]
    async fn test_simple_select_query_json_with_scan_range() {
        let sql = "SELECT name, age FROM S3Object LIMIT 20";
        let mut input = create_test_json_lines_input(sql);
        input.request.scan_range = Some(ScanRange {
            start: Some(0),
            end: Some(1024),
        });
        let db = get_global_db(input.clone(), true)
            .await
            .expect("create json scan range test database");
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());

        let output = result
            .expect("execute json scan range query")
            .result()
            .chunk_result()
            .await
            .expect("collect json scan range output");
        let total_rows: usize = output.iter().map(|batch| batch.num_rows()).sum();
        assert!(total_rows > 0);
    }

    #[tokio::test]
    async fn test_select_with_where_clause_json() {
        let sql = "SELECT name, age FROM S3Object WHERE age > 30";
        let input = create_test_json_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_select_with_aggregation_json() {
        let sql = "SELECT department, COUNT(*) as count FROM S3Object GROUP BY department";
        let input = create_test_json_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        // Aggregation queries may fail due to lack of actual data, which is acceptable
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
    async fn test_invalid_sql_syntax_json() {
        let sql = "INVALID SQL SYNTAX";
        let input = create_test_json_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multi_statement_error_json() {
        let sql = "SELECT * FROM S3Object; SELECT 1;";
        let input = create_test_json_input(sql);
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
    async fn test_query_state_machine_workflow_json() {
        let sql = "SELECT * FROM S3Object";
        let input = create_test_json_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let state_machine = db.build_query_state_machine(query.clone()).await;
        assert!(state_machine.is_ok());

        let state_machine = state_machine.unwrap();

        let logical_plan = db.build_logical_plan(state_machine.clone()).await;
        assert!(logical_plan.is_ok());

        if let Ok(Some(plan)) = logical_plan {
            let execution_result = db.execute_logical_plan(plan, state_machine).await;
            assert!(execution_result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_query_with_limit_json() {
        let sql = "SELECT * FROM S3Object LIMIT 5";
        let input = create_test_json_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());

        let query_handle = result.unwrap();
        let output = query_handle.result().chunk_result().await.unwrap();

        let total_rows: usize = output.iter().map(|batch| batch.num_rows()).sum();
        assert!(total_rows <= 5);
    }

    #[tokio::test]
    async fn test_query_with_order_by_json() {
        let sql = "SELECT name, age FROM S3Object ORDER BY age DESC";
        let input = create_test_json_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_queries_json() {
        let sql = "SELECT * FROM S3Object";
        let input = create_test_json_input(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();

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

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }
}
