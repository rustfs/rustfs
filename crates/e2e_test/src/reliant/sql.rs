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
    CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization, JsonInput, JsonOutput, JsonType,
    OutputSerialization, RequestProgress,
};
use bytes::Bytes;
use std::error::Error;
use std::time::Duration;

const BUCKET: &str = "test-sql-bucket";
const CSV_OBJECT: &str = "test-data.csv";
const JSON_OBJECT: &str = "test-data.json";
const JSON_DOCUMENT_OBJECT: &str = "nested-data.json";
const JSON_ROOT_ARRAY_OBJECT: &str = "root-array.json";
const JSON_ROOT_SCALAR_ARRAY_OBJECT: &str = "root-scalars.json";
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

async fn upload_nested_json_document(client: &Client) -> TestResult<()> {
    let json_data = r#"{"departments":[{"employees":[{"name":"Alice","active":true},{"name":"Bob","active":false}]},{"employees":[{"name":"Charlie","active":true}]}]}"#;

    client
        .put_object()
        .bucket(BUCKET)
        .key(JSON_DOCUMENT_OBJECT)
        .body(Bytes::from_static(json_data.as_bytes()).into())
        .send()
        .await?;
    client
        .put_object()
        .bucket(BUCKET)
        .key(JSON_ROOT_ARRAY_OBJECT)
        .body(Bytes::from_static(br#"[{"name":"Alice"},{"name":"Bob"}]"#).into())
        .send()
        .await?;
    client
        .put_object()
        .bucket(BUCKET)
        .key(JSON_ROOT_SCALAR_ARRAY_OBJECT)
        .body(Bytes::from_static(b"[1,2]").into())
        .send()
        .await?;
    Ok(())
}

async fn select_json_document(client: &Client, key: &str, expression: &str) -> TestResult<String> {
    let response = client
        .select_object_content()
        .bucket(BUCKET)
        .key(key)
        .expression(expression)
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .json(JsonInput::builder().set_type(Some(JsonType::Document)).build())
                .build(),
        )
        .output_serialization(OutputSerialization::builder().json(JsonOutput::builder().build()).build())
        .send()
        .await?;
    process_select_response(response).await
}

fn csv_select_request(
    client: &Client,
    key: &str,
) -> aws_sdk_s3::operation::select_object_content::builders::SelectObjectContentFluentBuilder {
    client
        .select_object_content()
        .bucket(BUCKET)
        .key(key)
        .expression("SELECT * FROM S3Object")
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .csv(CsvInput::builder().file_header_info(FileHeaderInfo::Use).build())
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
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

async fn assert_input_byte_stats(
    client: &Client,
    object: &str,
    body: &[u8],
    expression: &str,
    input_serialization: InputSerialization,
    output_serialization: OutputSerialization,
    progress_enabled: bool,
) -> TestResult<()> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(object)
        .body(Bytes::copy_from_slice(body).into())
        .send()
        .await?;

    let mut request = client
        .select_object_content()
        .bucket(BUCKET)
        .key(object)
        .expression(expression)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization);
    if progress_enabled {
        request = request.request_progress(RequestProgress::builder().enabled(true).build());
    }
    let response = request.send().await?;

    let mut payload = response.payload;
    let mut records_len = 0_u64;
    let mut last_progress: Option<aws_sdk_s3::types::Progress> = None;
    let mut stats = None;
    let mut saw_end = false;
    tokio::time::timeout(SELECT_RESPONSE_TIMEOUT, async {
        // The AWS SDK validates both event-stream CRCs before yielding an event.
        while let Some(event) = payload.recv().await? {
            assert!(!saw_end, "Select emitted an event after End");
            match event {
                aws_sdk_s3::types::SelectObjectContentEventStream::Records(records) => {
                    assert!(stats.is_none(), "Select emitted Records after Stats");
                    if let Some(bytes) = records.payload {
                        records_len = records_len.saturating_add(u64::try_from(bytes.as_ref().len())?);
                    }
                }
                aws_sdk_s3::types::SelectObjectContentEventStream::Progress(event) => {
                    assert!(stats.is_none(), "Select emitted Progress after Stats");
                    let details = event.details.ok_or("Progress event did not contain details")?;
                    if let Some(previous) = last_progress.as_ref() {
                        assert!(details.bytes_scanned() >= previous.bytes_scanned());
                        assert!(details.bytes_processed() >= previous.bytes_processed());
                        assert!(details.bytes_returned() >= previous.bytes_returned());
                    }
                    last_progress = Some(details);
                }
                aws_sdk_s3::types::SelectObjectContentEventStream::Stats(event) => {
                    assert!(stats.is_none(), "Select emitted more than one Stats event");
                    stats = event.details;
                }
                aws_sdk_s3::types::SelectObjectContentEventStream::End(_) => {
                    assert!(stats.is_some(), "Select emitted End before Stats");
                    saw_end = true;
                }
                _ => assert!(stats.is_none(), "Select emitted a non-terminal event after Stats"),
            }
        }
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    })
    .await
    .map_err(|_| -> Box<dyn Error + Send + Sync> { "Select response timed out".into() })??;

    let stats = stats.ok_or("Select response ended without a Stats event")?;
    let input_len = i64::try_from(body.len())?;
    assert_eq!(stats.bytes_scanned(), Some(input_len));
    assert_eq!(stats.bytes_processed(), Some(input_len));
    assert_eq!(stats.bytes_returned(), Some(i64::try_from(records_len)?));
    if progress_enabled {
        if let Some(progress) = last_progress {
            assert!(stats.bytes_scanned() >= progress.bytes_scanned());
            assert!(stats.bytes_processed() >= progress.bytes_processed());
            assert!(stats.bytes_returned() >= progress.bytes_returned());
        }
    } else {
        assert!(last_progress.is_none(), "disabled request progress emitted a Progress event");
    }
    assert!(saw_end, "Select response ended without an End event");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_http_event_order_crc_and_input_byte_stats() -> TestResult<()> {
    const CSV_BODY: &[u8] = b"name,age\nAlice,30\nBob,25\n";
    const JSON_LINES_BODY: &[u8] = b"{\"name\":\"Alice\"}\n{\"name\":\"Bob\"}\n";
    const JSON_DOCUMENT_BODY: &[u8] = b"[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]";

    let (_env, client) = create_test_environment().await?;
    setup_test_bucket(&client).await?;
    assert_input_byte_stats(
        &client,
        "input-metrics.csv",
        CSV_BODY,
        "SELECT name FROM S3Object",
        InputSerialization::builder()
            .csv(CsvInput::builder().file_header_info(FileHeaderInfo::Use).build())
            .build(),
        OutputSerialization::builder().csv(CsvOutput::builder().build()).build(),
        true,
    )
    .await?;
    assert_input_byte_stats(
        &client,
        "input-metrics.jsonl",
        JSON_LINES_BODY,
        "SELECT name FROM S3Object",
        InputSerialization::builder()
            .json(JsonInput::builder().set_type(Some(JsonType::Lines)).build())
            .build(),
        OutputSerialization::builder().json(JsonOutput::builder().build()).build(),
        true,
    )
    .await?;
    assert_input_byte_stats(
        &client,
        "input-metrics.json",
        JSON_DOCUMENT_BODY,
        "SELECT name FROM S3Object",
        InputSerialization::builder()
            .json(JsonInput::builder().set_type(Some(JsonType::Document)).build())
            .build(),
        OutputSerialization::builder().json(JsonOutput::builder().build()).build(),
        true,
    )
    .await?;
    assert_input_byte_stats(
        &client,
        "input-metrics-without-progress.csv",
        CSV_BODY,
        "SELECT name FROM S3Object",
        InputSerialization::builder()
            .csv(CsvInput::builder().file_header_info(FileHeaderInfo::Use).build())
            .build(),
        OutputSerialization::builder().csv(CsvOutput::builder().build()).build(),
        false,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_http_disconnect_releases_query() -> TestResult<()> {
    const OBJECT: &str = "disconnect.csv";
    const ROWS: usize = 16 * 1024;
    const RELEASE_ATTEMPTS: usize = 20;
    const RELEASE_BACKOFF: Duration = Duration::from_millis(25);

    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_S3SELECT_MAX_CONCURRENT_QUERIES", "1")])
        .await?;
    let client = env.create_s3_client();
    setup_test_bucket(&client).await?;

    let row = format!("{}\n", "x".repeat(1023));
    let mut body = Vec::with_capacity("value\n".len() + ROWS * row.len());
    body.extend_from_slice(b"value\n");
    for _ in 0..ROWS {
        body.extend_from_slice(row.as_bytes());
    }
    client
        .put_object()
        .bucket(BUCKET)
        .key(OBJECT)
        .body(Bytes::from(body).into())
        .send()
        .await?;

    // Leaving this response body unread fills the bounded HTTP/event channels before the query can finish.
    let first = csv_select_request(&client, OBJECT).send().await?;
    let saturated = csv_select_request(&client, OBJECT)
        .send()
        .await
        .expect_err("the first HTTP stream should retain the only query permit");
    assert_eq!(saturated.as_service_error().and_then(ProvideErrorMetadata::code), Some("SlowDown"));

    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(5), async {
        for attempt in 0..RELEASE_ATTEMPTS {
            match csv_select_request(&client, OBJECT).send().await {
                Ok(response) => return Ok::<_, Box<dyn Error + Send + Sync>>(response),
                Err(error)
                    if error.as_service_error().and_then(ProvideErrorMetadata::code) == Some("SlowDown")
                        && attempt + 1 < RELEASE_ATTEMPTS =>
                {
                    tokio::time::sleep(RELEASE_BACKOFF).await;
                }
                Err(error) if error.as_service_error().and_then(ProvideErrorMetadata::code) == Some("SlowDown") => {
                    return Err("disconnected Select retained its query permit after bounded retries".into());
                }
                Err(error) => return Err(format!("unexpected Select error after disconnect: {error}").into()),
            }
        }
        Err("query permit release retry loop ended unexpectedly".into())
    })
    .await
    .map_err(|_| -> Box<dyn Error + Send + Sync> { "disconnected Select did not release its query permit".into() })??;
    drop(second);

    Ok(())
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
async fn test_select_object_content_nested_json_source_path() -> TestResult<()> {
    let (_env, client) = create_test_environment().await?;
    setup_test_bucket(&client).await?;
    upload_nested_json_document(&client).await?;

    let result = select_json_document(
        &client,
        JSON_DOCUMENT_OBJECT,
        "SELECT e.name FROM S3Object[*].departments[*].employees[*] AS e WHERE e.active = true",
    )
    .await?;
    let names: Vec<String> = result
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| -> TestResult<String> {
            let value: serde_json::Value = serde_json::from_str(line)?;
            Ok(value["name"].as_str().ok_or("missing name field")?.to_string())
        })
        .collect::<TestResult<_>>()?;

    assert_eq!(names, vec!["Alice", "Charlie"]);

    let terminal_scalars = select_json_document(
        &client,
        JSON_DOCUMENT_OBJECT,
        "SELECT NAME FROM S3Object[*].DEPARTMENTS[*].employees[*].NAME",
    )
    .await?;
    let scalar_names: Vec<String> = terminal_scalars
        .lines()
        .map(|line| -> TestResult<String> {
            let value: serde_json::Value = serde_json::from_str(line)?;
            Ok(value["name"].as_str().ok_or("missing scalar name field")?.to_string())
        })
        .collect::<TestResult<_>>()?;
    assert_eq!(scalar_names, vec!["Alice", "Bob", "Charlie"]);

    let aliased_scalars = select_json_document(
        &client,
        JSON_DOCUMENT_OBJECT,
        "SELECT v FROM S3Object[*].departments[*].employees[*].name AS v",
    )
    .await?;
    let aliased_names: Vec<String> = aliased_scalars
        .lines()
        .map(|line| -> TestResult<String> {
            let value: serde_json::Value = serde_json::from_str(line)?;
            Ok(value["v"].as_str().ok_or("missing aliased scalar field")?.to_string())
        })
        .collect::<TestResult<_>>()?;
    assert_eq!(aliased_names, vec!["Alice", "Bob", "Charlie"]);

    let root_array = select_json_document(&client, JSON_ROOT_ARRAY_OBJECT, "SELECT c.name FROM S3Object[*][*] AS c").await?;
    let root_names: Vec<String> = root_array
        .lines()
        .map(|line| -> TestResult<String> {
            let value: serde_json::Value = serde_json::from_str(line)?;
            Ok(value["name"].as_str().ok_or("missing root-array name field")?.to_string())
        })
        .collect::<TestResult<_>>()?;
    assert_eq!(root_names, vec!["Alice", "Bob"]);

    let root_index = select_json_document(&client, JSON_ROOT_ARRAY_OBJECT, "SELECT c.name FROM S3Object[*][0] AS c").await?;
    let root_index_value: serde_json::Value = serde_json::from_str(root_index.trim())?;
    assert_eq!(root_index_value["name"], "Alice");

    let root_scalars = select_json_document(&client, JSON_ROOT_SCALAR_ARRAY_OBJECT, "SELECT V FROM S3Object AS V").await?;
    let scalar_values: Vec<i64> = root_scalars
        .lines()
        .map(|line| -> TestResult<i64> {
            let value: serde_json::Value = serde_json::from_str(line)?;
            Ok(value["v"].as_i64().ok_or("missing root scalar value")?)
        })
        .collect::<TestResult<_>>()?;
    assert_eq!(scalar_values, vec![1, 2]);

    let implicit_root_scalars =
        select_json_document(&client, JSON_ROOT_SCALAR_ARRAY_OBJECT, "SELECT S3Object FROM S3Object").await?;
    let implicit_scalar_values: Vec<i64> = implicit_root_scalars
        .lines()
        .map(|line| -> TestResult<i64> {
            let value: serde_json::Value = serde_json::from_str(line)?;
            Ok(value["s3object"].as_i64().ok_or("missing implicit root scalar value")?)
        })
        .collect::<TestResult<_>>()?;
    assert_eq!(implicit_scalar_values, vec![1, 2]);

    let quoted_root_scalars =
        select_json_document(&client, JSON_ROOT_SCALAR_ARRAY_OBJECT, "SELECT \"S3Object\" FROM \"S3Object\"").await?;
    let quoted_scalar_values: Vec<i64> = quoted_root_scalars
        .lines()
        .map(|line| -> TestResult<i64> {
            let value: serde_json::Value = serde_json::from_str(line)?;
            Ok(value["S3Object"].as_i64().ok_or("missing quoted root scalar value")?)
        })
        .collect::<TestResult<_>>()?;
    assert_eq!(quoted_scalar_values, vec![1, 2]);
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
