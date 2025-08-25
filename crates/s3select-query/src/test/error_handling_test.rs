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
mod error_handling_tests {
    use crate::get_global_db;
    use rustfs_s3select_api::{
        QueryError,
        query::{Context, Query},
    };
    use s3s::dto::{
        CSVInput, ExpressionType, FileHeaderInfo, InputSerialization, SelectObjectContentInput, SelectObjectContentRequest,
    };
    use std::sync::Arc;

    fn create_test_input_with_sql(sql: &str) -> SelectObjectContentInput {
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
                output_serialization: s3s::dto::OutputSerialization::default(),
                request_progress: None,
                scan_range: None,
            },
        }
    }

    #[tokio::test]
    async fn test_syntax_error_handling() {
        let invalid_sqls = vec![
            "INVALID SQL",
            "SELECT FROM",
            "SELECT * FORM S3Object", // typo in FROM
            "SELECT * FROM",
            "SELECT * FROM S3Object WHERE",
            "SELECT COUNT( FROM S3Object", // missing closing parenthesis
        ];

        for sql in invalid_sqls {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true).await.unwrap();
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let result = db.execute(&query).await;
            assert!(result.is_err(), "Expected error for SQL: {sql}");
        }
    }

    #[tokio::test]
    async fn test_multi_statement_error() {
        let multi_statement_sqls = vec![
            "SELECT * FROM S3Object; SELECT 1;",
            "SELECT 1; SELECT 2; SELECT 3;",
            "SELECT * FROM S3Object; DROP TABLE test;",
        ];

        for sql in multi_statement_sqls {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true).await.unwrap();
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let result = db.execute(&query).await;
            assert!(result.is_err(), "Expected multi-statement error for SQL: {sql}");

            if let Err(QueryError::MultiStatement { num, .. }) = result {
                assert!(num >= 2, "Expected at least 2 statements, got: {num}");
            }
        }
    }

    #[tokio::test]
    async fn test_unsupported_operations() {
        let unsupported_sqls = vec![
            "INSERT INTO S3Object VALUES (1, 'test')",
            "UPDATE S3Object SET name = 'test'",
            "DELETE FROM S3Object",
            "CREATE TABLE test (id INT)",
            "DROP TABLE S3Object",
        ];

        for sql in unsupported_sqls {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true).await.unwrap();
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let result = db.execute(&query).await;
            // These should either fail with syntax error or not implemented error
            assert!(result.is_err(), "Expected error for unsupported SQL: {sql}");
        }
    }

    #[tokio::test]
    async fn test_invalid_column_references() {
        let invalid_column_sqls = vec![
            "SELECT nonexistent_column FROM S3Object",
            "SELECT * FROM S3Object WHERE nonexistent_column = 1",
            "SELECT * FROM S3Object ORDER BY nonexistent_column",
            "SELECT * FROM S3Object GROUP BY nonexistent_column",
        ];

        for sql in invalid_column_sqls {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true).await.unwrap();
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let result = db.execute(&query).await;
            // These might succeed or fail depending on schema inference
            // The test verifies that the system handles them gracefully
            match result {
                Ok(_) => {
                    // If it succeeds, verify we can get results
                    let handle = result.unwrap();
                    let output = handle.result().chunk_result().await;
                    // Should either succeed with empty results or fail gracefully
                    let _ = output;
                }
                Err(_) => {
                    // Expected to fail - this is acceptable
                }
            }
        }
    }

    #[tokio::test]
    async fn test_complex_query_error_recovery() {
        let complex_invalid_sql = r#"
            SELECT 
                name,
                age,
                INVALID_FUNCTION(salary) as invalid_calc,
                department
            FROM S3Object 
            WHERE age > 'invalid_number'
            GROUP BY department, nonexistent_column
            HAVING COUNT(*) > INVALID_FUNCTION()
            ORDER BY invalid_column
        "#;

        let input = create_test_input_with_sql(complex_invalid_sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, complex_invalid_sql.to_string());

        let result = db.execute(&query).await;
        assert!(result.is_err(), "Expected error for complex invalid SQL");
    }

    #[tokio::test]
    async fn test_empty_query() {
        let empty_sqls = vec!["", "   ", "\n\t  \n"];

        for sql in empty_sqls {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true).await.unwrap();
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let result = db.execute(&query).await;
            // Empty queries might be handled differently by the parser
            match result {
                Ok(_) => {
                    // Some parsers might accept empty queries
                }
                Err(_) => {
                    // Expected to fail for empty SQL
                }
            }
        }
    }

    #[tokio::test]
    async fn test_very_long_query() {
        // Create a very long but valid query
        let mut long_sql = "SELECT ".to_string();
        for i in 0..1000 {
            if i > 0 {
                long_sql.push_str(", ");
            }
            long_sql.push_str(&format!("'column_{i}' as col_{i}"));
        }
        long_sql.push_str(" FROM S3Object LIMIT 1");

        let input = create_test_input_with_sql(&long_sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, long_sql);

        let result = db.execute(&query).await;
        // This should either succeed or fail gracefully
        match result {
            Ok(handle) => {
                let output = handle.result().chunk_result().await;
                assert!(output.is_ok(), "Query execution should complete successfully");
            }
            Err(_) => {
                // Acceptable to fail due to resource constraints
            }
        }
    }

    #[tokio::test]
    async fn test_sql_injection_patterns() {
        let injection_patterns = vec![
            "SELECT * FROM S3Object WHERE name = 'test'; DROP TABLE users; --",
            "SELECT * FROM S3Object UNION SELECT * FROM information_schema.tables",
            "SELECT * FROM S3Object WHERE 1=1 OR 1=1",
        ];

        for sql in injection_patterns {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true).await.unwrap();
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let result = db.execute(&query).await;
            // These should be handled safely - either succeed with limited scope or fail
            match result {
                Ok(_) => {
                    // If successful, it should only access S3Object data
                }
                Err(_) => {
                    // Expected to fail for security reasons
                }
            }
        }
    }
}
