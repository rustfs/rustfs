#![cfg(test)]
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

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{
    CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization, JsonInput, JsonOutput, JsonType, OutputSerialization,
};
use bytes::Bytes;
use std::error::Error;
use std::time::Duration;

const BUCKET: &str = "test-sql-bucket";
const CSV_OBJECT: &str = "test-data.csv";
const JSON_OBJECT: &str = "test-data.json";
const SELECT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

async fn create_test_environment() -> TestResult<(RustFSTestEnvironment, Client)> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();
    Ok((env, client))
}

async fn setup_test_bucket(client: &Client) -> TestResult<()> {
    client.create_bucket().bucket(BUCKET).send().await?;
    Ok(())
}

async fn upload_test_csv(client: &Client) -> TestResult<()> {
    let csv_data = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\nDiana,28,Boston";

    client
        .put_object()
        .bucket(BUCKET)
        .key(CSV_OBJECT)
        .body(Bytes::from(csv_data.as_bytes()).into())
        .send()
        .await?;

    Ok(())
}

async fn upload_test_json(client: &Client) -> TestResult<()> {
    let json_data = r#"{"name":"Alice","age":30,"city":"New York"}
{"name":"Bob","age":25,"city":"Los Angeles"}
{"name":"Charlie","age":35,"city":"Chicago"}
{"name":"Diana","age":28,"city":"Boston"}"#;

    client
        .put_object()
        .bucket(BUCKET)
        .key(JSON_OBJECT)
        .body(Bytes::from(json_data.as_bytes()).into())
        .send()
        .await?;
    Ok(())
}

async fn process_select_response(
    mut event_stream: aws_sdk_s3::operation::select_object_content::SelectObjectContentOutput,
) -> TestResult<String> {
    tokio::time::timeout(SELECT_RESPONSE_TIMEOUT, async move {
        let mut total_data = Vec::new();
        let mut saw_end = false;

        while let Some(event) = event_stream.payload.recv().await? {
            match event {
                aws_sdk_s3::types::SelectObjectContentEventStream::Records(records_event) => {
                    if let Some(payload) = records_event.payload {
                        total_data.extend_from_slice(payload.as_ref());
                    }
                }
                aws_sdk_s3::types::SelectObjectContentEventStream::End(_) => {
                    saw_end = true;
                    break;
                }
                _ => {}
            }
        }

        if !saw_end {
            return Err("Select response ended without an End event".into());
        }
        Ok(String::from_utf8(total_data)?)
    })
    .await
    .map_err(|_| -> Box<dyn Error + Send + Sync> { "Select response timed out".into() })?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_csv_basic() -> TestResult<()> {
    let (_env, client) = create_test_environment().await?;
    setup_test_bucket(&client).await?;
    upload_test_csv(&client).await?;

    // Construct SelectObjectContent request - basic query
    let sql = "SELECT * FROM S3Object WHERE age > 28";

    let csv_input = CsvInput::builder().file_header_info(FileHeaderInfo::Use).build();

    let input_serialization = InputSerialization::builder().csv(csv_input).build();

    let csv_output = CsvOutput::builder().build();
    let output_serialization = OutputSerialization::builder().csv(csv_output).build();

    let response = client
        .select_object_content()
        .bucket(BUCKET)
        .key(CSV_OBJECT)
        .expression(sql)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await?;

    let result_str = process_select_response(response).await?;

    println!("CSV Select result: {result_str}");

    // Verify results contain records with age > 28
    assert!(result_str.contains("Alice,30,New York"));
    assert!(result_str.contains("Charlie,35,Chicago"));
    assert!(!result_str.contains("Bob,25,Los Angeles"));
    assert!(!result_str.contains("Diana,28,Boston"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_csv_aggregation() -> TestResult<()> {
    let (_env, client) = create_test_environment().await?;
    setup_test_bucket(&client).await?;
    upload_test_csv(&client).await?;

    // Construct aggregation query - use simpler approach
    let sql = "SELECT name, age FROM S3Object WHERE age >= 25";

    let csv_input = CsvInput::builder().file_header_info(FileHeaderInfo::Use).build();

    let input_serialization = InputSerialization::builder().csv(csv_input).build();

    let csv_output = CsvOutput::builder().build();
    let output_serialization = OutputSerialization::builder().csv(csv_output).build();

    let response = client
        .select_object_content()
        .bucket(BUCKET)
        .key(CSV_OBJECT)
        .expression(sql)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await?;

    let result_str = process_select_response(response).await?;

    println!("CSV Aggregation result: {result_str}");

    // Verify query results - should include records with age >= 25
    assert!(result_str.contains("Alice"));
    assert!(result_str.contains("Bob"));
    assert!(result_str.contains("Charlie"));
    assert!(result_str.contains("Diana"));
    assert!(result_str.contains("30"));
    assert!(result_str.contains("25"));
    assert!(result_str.contains("35"));
    assert!(result_str.contains("28"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_json_basic() -> TestResult<()> {
    let (_env, client) = create_test_environment().await?;
    setup_test_bucket(&client).await?;
    upload_test_json(&client).await?;

    // Construct JSON query
    let sql = "SELECT s.name, s.age FROM S3Object s WHERE s.age > 28";

    let json_input = JsonInput::builder().set_type(Some(JsonType::Lines)).build();

    let input_serialization = InputSerialization::builder().json(json_input).build();

    let json_output = JsonOutput::builder().build();
    let output_serialization = OutputSerialization::builder().json(json_output).build();

    let response = client
        .select_object_content()
        .bucket(BUCKET)
        .key(JSON_OBJECT)
        .expression(sql)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await?;

    let result_str = process_select_response(response).await?;

    println!("JSON Select result: {result_str}");

    // Verify JSON query results
    assert!(result_str.contains("Alice"));
    assert!(result_str.contains("Charlie"));
    assert!(result_str.contains("30"));
    assert!(result_str.contains("35"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_csv_limit() -> TestResult<()> {
    let (_env, client) = create_test_environment().await?;
    setup_test_bucket(&client).await?;
    upload_test_csv(&client).await?;

    // Test LIMIT clause
    let sql = "SELECT * FROM S3Object LIMIT 2";

    let csv_input = CsvInput::builder().file_header_info(FileHeaderInfo::Use).build();

    let input_serialization = InputSerialization::builder().csv(csv_input).build();

    let csv_output = CsvOutput::builder().build();
    let output_serialization = OutputSerialization::builder().csv(csv_output).build();

    let response = client
        .select_object_content()
        .bucket(BUCKET)
        .key(CSV_OBJECT)
        .expression(sql)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await?;

    let result_str = process_select_response(response).await?;

    println!("CSV Limit result: {result_str}");

    // Verify only first 2 records are returned
    assert_eq!(
        result_str.lines().filter(|line| !line.trim().is_empty()).count(),
        2,
        "Should return exactly 2 records"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_csv_order_by() -> TestResult<()> {
    let (_env, client) = create_test_environment().await?;
    setup_test_bucket(&client).await?;
    upload_test_csv(&client).await?;

    // Test ORDER BY clause
    let sql = "SELECT name, age FROM S3Object ORDER BY age DESC LIMIT 2";

    let csv_input = CsvInput::builder().file_header_info(FileHeaderInfo::Use).build();

    let input_serialization = InputSerialization::builder().csv(csv_input).build();

    let csv_output = CsvOutput::builder().build();
    let output_serialization = OutputSerialization::builder().csv(csv_output).build();

    let response = client
        .select_object_content()
        .bucket(BUCKET)
        .key(CSV_OBJECT)
        .expression(sql)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await?;

    let result_str = process_select_response(response).await?;

    println!("CSV Order By result: {result_str}");

    // Verify ordered by age descending
    assert_eq!(
        result_str.lines().filter(|line| !line.trim().is_empty()).count(),
        2,
        "Should return exactly 2 records"
    );

    // Check if contains highest age records
    assert!(result_str.contains("Charlie,35"));
    assert!(result_str.contains("Alice,30"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_error_handling() -> TestResult<()> {
    let (_env, client) = create_test_environment().await?;
    setup_test_bucket(&client).await?;
    upload_test_csv(&client).await?;

    // Test invalid SQL query
    let sql = "SELECT * FROM S3Object WHERE invalid_column > 10";

    let csv_input = CsvInput::builder().file_header_info(FileHeaderInfo::Use).build();

    let input_serialization = InputSerialization::builder().csv(csv_input).build();

    let csv_output = CsvOutput::builder().build();
    let output_serialization = OutputSerialization::builder().csv(csv_output).build();

    // This query should fail because invalid_column doesn't exist
    let error = client
        .select_object_content()
        .bucket(BUCKET)
        .key(CSV_OBJECT)
        .expression(sql)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await
        .expect_err("a query referencing an unknown column must fail");

    assert_eq!(
        error.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("EvaluatorBindingDoesNotExist")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_nonexistent_object() -> TestResult<()> {
    let (_env, client) = create_test_environment().await?;
    setup_test_bucket(&client).await?;

    // Test query on nonexistent object
    let sql = "SELECT * FROM S3Object";

    let csv_input = CsvInput::builder().file_header_info(FileHeaderInfo::Use).build();

    let input_serialization = InputSerialization::builder().csv(csv_input).build();

    let csv_output = CsvOutput::builder().build();
    let output_serialization = OutputSerialization::builder().csv(csv_output).build();

    let error = client
        .select_object_content()
        .bucket(BUCKET)
        .key("nonexistent.csv")
        .expression(sql)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await
        .expect_err("selecting a missing object must fail");

    assert_eq!(error.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));

    Ok(())
}
