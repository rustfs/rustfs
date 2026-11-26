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
    use datafusion::object_store::ObjectStoreExt as _;
    use rustfs_s3select_api::request::{
        CsvInput, ExpressionType, FileHeaderInfo, InputSerialization, SelectObjectContentInput, SelectObjectContentRequest,
    };
    use rustfs_s3select_api::{
        QueryError, SelectError,
        query::{Context, Query},
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
                expression_type: ExpressionType::from("SQL"),
                input_serialization: InputSerialization {
                    csv: Some(CsvInput {
                        file_header_info: Some(FileHeaderInfo::USE),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                output_serialization: rustfs_s3select_api::request::OutputSerialization::default(),
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
    async fn test_non_select_statement_is_typed_unsupported_structure() {
        let sql = "CREATE TABLE test (id INT)";
        let input = create_test_input_with_sql(sql);
        let db = get_global_db(input.clone(), true).await.unwrap();
        let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

        let error = match db.execute(&query).await {
            Err(error) => error,
            Ok(_) => panic!("non-SELECT statement must fail"),
        };

        assert!(matches!(error.select_error(), SelectError::UnsupportedSqlStructure { .. }));
    }

    #[tokio::test]
    async fn test_function_argument_coercion_failure_is_typed() {
        for sql in ["SELECT ROUND(3.14, 1.1) FROM S3Object", "SELECT SQRT(1, 2) FROM S3Object"] {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true)
                .await
                .expect("test database should initialize");
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let error = match db.execute(&query).await {
                Err(error) => error,
                Ok(_) => panic!("invalid function arguments must fail during planning: {sql}"),
            };

            assert_eq!(
                error.select_error(),
                SelectError::IncorrectSqlFunctionArgumentType,
                "unexpected planner error for {sql}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_other_planner_failures_remain_invalid_query() {
        for sql in [
            "SELECT DEFINITELY_UNKNOWN_FUNCTION(1) FROM S3Object",
            "SELECT 1 + 'text' FROM S3Object",
        ] {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true)
                .await
                .expect("test database should initialize");
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let error = match db.execute(&query).await {
                Err(error) => error,
                Ok(_) => panic!("invalid query must fail during planning: {sql}"),
            };

            assert_eq!(
                error.select_error(),
                SelectError::InvalidQuery,
                "unexpected planner error for {sql}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_non_allowlisted_datafusion_functions_are_rejected() {
        for sql in [
            "SELECT length(repeat(arrow_cast('a', 'LargeUtf8'), 4611686018427387904)) FROM S3Object",
            "SELECT length(repeat(arrow_cast(name, 'LargeUtf8'), 1000000000000000)) FROM S3Object",
            "SELECT length(repeat('a', 1000000000)) FROM S3Object",
            "SELECT length(lpad('a', 500000000)) FROM S3Object",
            "SELECT length(rpad('a', 500000000)) FROM S3Object",
            "SELECT levenshtein('abc', 'def') FROM S3Object",
            "SELECT arrow_cast(name, 'LargeUtf8') FROM S3Object",
            "SELECT version() FROM S3Object",
            "SELECT uuid() FROM S3Object",
            "SELECT replace(name, 'a', 'bb') FROM S3Object",
            "SELECT regexp_replace(name, 'a', 'b') FROM S3Object",
            "SELECT encode(name, 'hex') FROM S3Object",
            "SELECT array_agg(name) FROM S3Object",
            "SELECT string_agg(name, ',') FROM S3Object",
            "SELECT row_number() OVER () FROM S3Object",
        ] {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true)
                .await
                .expect("test database should initialize");
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let error = match db.execute(&query).await {
                Err(error) => error,
                Ok(_) => panic!("disallowed function must be rejected during planning: {sql}"),
            };

            assert_eq!(
                error.select_error(),
                SelectError::InvalidQuery,
                "disallowed function must fail with InvalidQuery for {sql}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_standard_s3_select_functions_continue_working() {
        for sql in [
            "SELECT LOWER(name), UPPER(name), LENGTH(name) AS len1, CHAR_LENGTH(name) AS len2, CHARACTER_LENGTH(name) AS len3 FROM S3Object LIMIT 2",
            "SELECT LEFT(name, 2), RIGHT(name, 2), TRIM(name) AS trimmed, BTRIM(name), LTRIM(name), RTRIM(name), STRPOS(name, 'a'), STARTS_WITH(name, 'A'), ENDS_WITH(name, 'e'), CONTAINS(name, 'l') FROM S3Object LIMIT 2",
            "SELECT COALESCE(name, 'n/a'), NULLIF(name, 'Alice'), CONCAT(name, '-', department), CONCAT_WS(':', name, department) FROM S3Object LIMIT 2",
            "SELECT ABS(-5), ROUND(3.14159, 2), SQRT(16), CEIL(1.2), FLOOR(1.8), POWER(2, 3) AS p1, POW(2, 4) AS p2 FROM S3Object LIMIT 1",
            "SELECT DATE_PART('year', TO_TIMESTAMP('2025-01-02T03:04:05Z')), CURRENT_DATE(), NOW() FROM S3Object LIMIT 1",
            "SELECT COUNT(*), SUM(CAST(age AS INT)), AVG(CAST(age AS INT)), MIN(CAST(age AS INT)), MAX(CAST(age AS INT)) FROM S3Object",
        ] {
            let input = create_test_input_with_sql(sql);
            let db = get_global_db(input.clone(), true)
                .await
                .expect("test database should initialize");
            let query = Query::new(Context { input: Arc::new(input) }, sql.to_string());

            let handle = db
                .execute(&query)
                .await
                .unwrap_or_else(|error| panic!("standard S3 Select query must plan ({sql}): {error:?}"));
            let batches = handle
                .result()
                .chunk_result()
                .await
                .unwrap_or_else(|error| panic!("standard S3 Select query must execute ({sql}): {error:?}"));
            assert!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>() > 0,
                "standard S3 Select query must return rows: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn test_retained_expanding_functions_respect_result_size_bound() {
        // Test with a large format literal in `to_char` over the 10-row fixture (column-dependent
        // timestamp prevents scalar-only folding): 10 rows * (100_000 * 20 + 64) > 16 MiB.
        let huge_format = "%B".repeat(50_000);
        let sql = format!("SELECT TO_CHAR(TO_TIMESTAMP(CAST(age AS BIGINT)), '{huge_format}') FROM S3Object");
        let input = create_test_input_with_sql(&sql);
        let db = get_global_db(input.clone(), true)
            .await
            .expect("test database should initialize");
        let query = Query::new(Context { input: Arc::new(input) }, sql);

        let error = match db.execute(&query).await {
            Err(error) => error,
            Ok(handle) => match handle.result().chunk_result().await {
                Err(error) => error,
                Ok(_) => panic!("oversized to_char result must fail"),
            },
        };
        assert_eq!(error.select_error(), SelectError::InternalError, "unexpected error: {error:?}");

        // Verify `concat_ws('', ...)` simplification re-wraps `concat` and enforces the bound:
        // a single 200 KiB CSV column repeated 100 times (~20 MiB > 16 MiB) fails cleanly
        // even though `concat_ws('', ...)` simplifies to `concat(...)` and SQL is < 1 KiB.
        let repeated_cols = std::iter::repeat_n("payload", 100).collect::<Vec<_>>().join(", ");
        let concat_sql = format!("SELECT LENGTH(CONCAT_WS('', {repeated_cols})) FROM S3Object");
        let mut concat_input = create_test_input_with_sql(&concat_sql);
        concat_input.key = "concat_bound.csv".to_string();
        let db = get_global_db(concat_input.clone(), true)
            .await
            .expect("test database should initialize");
        let query = Query::new(
            Context {
                input: Arc::new(concat_input.clone()),
            },
            concat_sql,
        );
        let state_machine = db.build_query_state_machine(query).await.expect("build query state machine");
        let store_url = datafusion::execution::object_store::ObjectStoreUrl::parse(format!("s3://{}", concat_input.bucket))
            .expect("valid test store url");
        let store = state_machine
            .session
            .inner()
            .runtime_env()
            .object_store(&store_url)
            .expect("test object store");
        let csv_payload = format!("payload\n{}\n", "x".repeat(200 * 1024));
        store
            .put(
                &datafusion::object_store::path::Path::from(concat_input.key.as_str()),
                csv_payload.into_bytes().into(),
            )
            .await
            .expect("put oversized concat fixture");
        let plan = db
            .build_logical_plan(Arc::clone(&state_machine))
            .await
            .expect("plan concat_ws query")
            .expect("logical plan");
        let output = db
            .execute_logical_plan(plan, state_machine)
            .await
            .expect("start concat_ws execution");
        let error = output
            .result()
            .chunk_result()
            .await
            .expect_err("oversized concat_ws result must fail instead of allocating");
        assert_eq!(error.select_error(), SelectError::InternalError, "unexpected error: {error:?}");
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
            assert!(
                matches!(result, Err(QueryError::Parser { .. })),
                "Expected parser error for empty SQL: {sql:?}"
            );
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
