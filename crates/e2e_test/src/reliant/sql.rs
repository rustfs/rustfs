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

use anyhow::{Context as _, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization, JsonInput, JsonOutput,
    JsonType, OutputSerialization, VersioningConfiguration,
};
use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use serial_test::serial;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_util::task::AbortOnDropHandle;
use uuid::Uuid;

const ENDPOINT: &str = "http://localhost:9000";
const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "test-sql-bucket";
const CSV_OBJECT: &str = "test-data.csv";
const JSON_OBJECT: &str = "test-data.json";
const SNAPSHOT_HEADER: &[u8] = b"generation\n";
const OLD_GENERATION_POISON: &[u8] = b"OLD_GENERATION_POISON_1629";
const NEW_GENERATION_POISON: &[u8] = b"NEW_GENERATION_POISON_1629";
const SNAPSHOT_ROW_COUNT: usize = 400_000;
const SNAPSHOT_RACE_ROUNDS: usize = 3;
const SNAPSHOT_ACTIVE_RESPONSE_BYTES: usize = 1024 * 1024;
const SNAPSHOT_OPERATION_TIMEOUT: Duration = Duration::from_secs(90);
const SNAPSHOT_UPLOAD_CHUNK_SIZE: usize = 64 * 1024;

struct SignalingUploadBody {
    remaining: Bytes,
    started: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Body for SignalingUploadBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        if !self.remaining.is_empty() {
            let chunk_len = self.remaining.len().min(SNAPSHOT_UPLOAD_CHUNK_SIZE);
            let chunk = self.remaining.split_to(chunk_len);
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        self.remaining.is_empty() && self.started.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining.len() as u64)
    }
}

async fn create_aws_s3_client() -> Result<Client> {
    let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "static"))
        .endpoint_url(ENDPOINT)
        .load()
        .await;

    let client = Client::from_conf(
        aws_sdk_s3::Config::from(&shared_config)
            .to_builder()
            .force_path_style(true) // Important for S3-compatible services
            .build(),
    );

    Ok(client)
}

async fn setup_bucket(client: &Client, bucket: &str) -> Result<()> {
    match client.create_bucket().bucket(bucket).send().await {
        Ok(_) => {}
        Err(e) => {
            let error_str = e.to_string();
            if !error_str.contains("BucketAlreadyOwnedByYou") && !error_str.contains("BucketAlreadyExists") {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

async fn setup_test_bucket(client: &Client) -> Result<()> {
    setup_bucket(client, BUCKET).await
}

async fn upload_test_csv(client: &Client) -> Result<()> {
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

async fn upload_test_json(client: &Client) -> Result<()> {
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
    event_stream: aws_sdk_s3::operation::select_object_content::SelectObjectContentOutput,
) -> Result<String> {
    Ok(String::from_utf8(collect_select_response(event_stream, Vec::new()).await?)?)
}

async fn collect_select_response(
    mut event_stream: aws_sdk_s3::operation::select_object_content::SelectObjectContentOutput,
    mut total_data: Vec<u8>,
) -> Result<Vec<u8>> {
    let mut saw_end = false;

    while let Some(event) = event_stream.payload.recv().await? {
        match event {
            aws_sdk_s3::types::SelectObjectContentEventStream::Records(records_event) => {
                if let Some(payload) = records_event.payload {
                    let data = payload.into_inner();
                    total_data.extend_from_slice(&data);
                }
            }
            aws_sdk_s3::types::SelectObjectContentEventStream::End(_) => {
                saw_end = true;
                break;
            }
            _ => {
                // Handle other event types (Stats, Progress, Cont, etc.)
            }
        }
    }
    anyhow::ensure!(saw_end, "SelectObjectContent stream ended without an End event");

    Ok(total_data)
}

fn snapshot_csv(poison: &[u8], row_count: usize) -> Bytes {
    let mut body = Vec::with_capacity(SNAPSHOT_HEADER.len() + row_count * (poison.len() + 1));
    body.extend_from_slice(SNAPSHOT_HEADER);
    for _ in 0..row_count {
        body.extend_from_slice(poison);
        body.push(b'\n');
    }
    Bytes::from(body)
}

async fn put_snapshot_generation(client: &Client, bucket: &str, key: &str, body: Bytes) -> Result<Option<String>> {
    put_snapshot_generation_with_body_start_signal(client, bucket, key, body, None).await
}

async fn put_snapshot_generation_with_body_start_signal(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Bytes,
    started: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<Option<String>> {
    let body = match started {
        Some(started) => ByteStream::from_body_1_x(SignalingUploadBody {
            remaining: body,
            started: Some(started),
        }),
        None => ByteStream::from(body),
    };
    let output = tokio::time::timeout(SNAPSHOT_OPERATION_TIMEOUT, client.put_object().bucket(bucket).key(key).body(body).send())
        .await
        .with_context(|| format!("timed out putting snapshot generation for {bucket}/{key}"))?
        .with_context(|| format!("put snapshot generation for {bucket}/{key}"))?;
    Ok(output.version_id)
}

async fn start_snapshot_select(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<aws_sdk_s3::operation::select_object_content::SelectObjectContentOutput> {
    client
        .select_object_content()
        .bucket(bucket)
        .key(key)
        .expression("SELECT * FROM S3Object")
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .csv(CsvInput::builder().file_header_info(FileHeaderInfo::Use).build())
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .csv(CsvOutput::builder().record_delimiter("\n").field_delimiter(",").build())
                .build(),
        )
        .send()
        .await
        .with_context(|| format!("start SelectObjectContent for {bucket}/{key}"))
}

async fn enable_bucket_versioning(client: &Client, bucket: &str) -> Result<()> {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .with_context(|| format!("enable versioning for {bucket}"))?;
    Ok(())
}

fn assert_complete_snapshot_output(round: usize, output: &[u8], expected_body: &[u8]) -> Result<()> {
    let expected_output = &expected_body[SNAPSHOT_HEADER.len()..];
    anyhow::ensure!(
        output == expected_output,
        "Select overwrite race round {round} did not return the generation captured before overwrite: bytes={}, old_poison={}, new_poison={}",
        output.len(),
        output
            .windows(OLD_GENERATION_POISON.len())
            .any(|window| window == OLD_GENERATION_POISON),
        output
            .windows(NEW_GENERATION_POISON.len())
            .any(|window| window == NEW_GENERATION_POISON),
    );
    Ok(())
}

async fn select_snapshot_during_pending_overwrite(
    client: &Client,
    bucket: &str,
    key: &str,
    new_body: Bytes,
) -> Result<(Vec<u8>, Option<String>)> {
    let mut response = tokio::time::timeout(SNAPSHOT_OPERATION_TIMEOUT, start_snapshot_select(client, bucket, key))
        .await
        .context("timed out starting SelectObjectContent overwrite probe")??;
    let overwrite_client = client.clone();
    let overwrite_bucket = bucket.to_string();
    let overwrite_key = key.to_string();
    let (overwrite_started_tx, mut overwrite_started_rx) = tokio::sync::oneshot::channel();
    let mut overwrite = AbortOnDropHandle::new(tokio::spawn(async move {
        put_snapshot_generation_with_body_start_signal(
            &overwrite_client,
            &overwrite_bucket,
            &overwrite_key,
            new_body,
            Some(overwrite_started_tx),
        )
        .await
    }));
    let mut overwrite_started = false;
    let mut records = Vec::new();

    tokio::time::timeout(SNAPSHOT_OPERATION_TIMEOUT, async {
        while records.len() < SNAPSHOT_ACTIVE_RESPONSE_BYTES || !overwrite_started {
            tokio::select! {
                biased;
                overwrite_result = &mut overwrite => {
                    overwrite_result.context("overwrite task failed")??;
                    anyhow::bail!(
                        "overwrite completed before Select established an active snapshot after {} record bytes",
                        records.len()
                    );
                }
                start_result = &mut overwrite_started_rx, if !overwrite_started => {
                    start_result.context("overwrite task exited before its request body started streaming")?;
                    overwrite_started = true;
                }
                event = response.payload.recv() => {
                    let Some(event) = event? else {
                        anyhow::bail!("Select response ended before the overwrite reached the snapshot lock");
                    };
                    match event {
                        aws_sdk_s3::types::SelectObjectContentEventStream::Records(record_event) => {
                            if let Some(payload) = record_event.payload {
                                records.extend_from_slice(payload.as_ref());
                            }
                        }
                        aws_sdk_s3::types::SelectObjectContentEventStream::End(_) => {
                            anyhow::bail!("Select response reached End before the overwrite was observed pending");
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("timed out waiting for an active Select snapshot")??;

    let mut overwrite_version = None;
    let output = tokio::time::timeout(SNAPSHOT_OPERATION_TIMEOUT, async {
        loop {
            tokio::select! {
                biased;
                overwrite_result = &mut overwrite, if overwrite_version.is_none() => {
                    overwrite_version = Some(overwrite_result.context("overwrite task failed")??);
                }
                event = response.payload.recv() => {
                    let Some(event) = event? else {
                        anyhow::bail!("Select response stream ended without an End event");
                    };
                    match event {
                        aws_sdk_s3::types::SelectObjectContentEventStream::Records(record_event) => {
                            if let Some(payload) = record_event.payload {
                                records.extend_from_slice(payload.as_ref());
                            }
                        }
                        aws_sdk_s3::types::SelectObjectContentEventStream::End(_) => break,
                        _ => {}
                    }
                }
            }
        }
        Ok::<Vec<u8>, anyhow::Error>(records)
    })
    .await
    .context("timed out consuming the established Select snapshot")??;
    let overwrite_version = match overwrite_version {
        Some(overwrite_version) => overwrite_version,
        None => tokio::time::timeout(SNAPSHOT_OPERATION_TIMEOUT, &mut overwrite)
            .await
            .context("overwrite remained blocked after the Select response completed")?
            .context("overwrite task failed")??,
    };
    Ok((output, overwrite_version))
}

async fn assert_disconnect_releases_snapshot(
    client: &Client,
    bucket: &str,
    key: &str,
    old_body: Bytes,
    new_body: Bytes,
    versioned: bool,
) -> Result<()> {
    let initial_version = put_snapshot_generation(client, bucket, key, old_body).await?;
    anyhow::ensure!(
        !versioned || initial_version.as_deref().is_some_and(|version| !version.is_empty()),
        "versioned disconnect fixture PUT omitted version_id"
    );

    let mut response = tokio::time::timeout(SNAPSHOT_OPERATION_TIMEOUT, start_snapshot_select(client, bucket, key))
        .await
        .context("timed out starting SelectObjectContent disconnect probe")??;

    let overwrite_client = client.clone();
    let overwrite_bucket = bucket.to_string();
    let overwrite_key = key.to_string();
    let (overwrite_started_tx, mut overwrite_started_rx) = tokio::sync::oneshot::channel();
    let mut overwrite = AbortOnDropHandle::new(tokio::spawn(async move {
        put_snapshot_generation_with_body_start_signal(
            &overwrite_client,
            &overwrite_bucket,
            &overwrite_key,
            new_body,
            Some(overwrite_started_tx),
        )
        .await
    }));
    let mut overwrite_started = false;
    let mut received_records = 0usize;
    tokio::time::timeout(SNAPSHOT_OPERATION_TIMEOUT, async {
        while received_records < SNAPSHOT_ACTIVE_RESPONSE_BYTES || !overwrite_started {
            tokio::select! {
                biased;
                overwrite_result = &mut overwrite => {
                    overwrite_result.context("overwrite task failed")??;
                    anyhow::bail!(
                        "overwrite completed while the Select response still held an active snapshot after {received_records} record bytes"
                    );
                }
                start_result = &mut overwrite_started_rx, if !overwrite_started => {
                    start_result.context("disconnect overwrite task exited before its request body started streaming")?;
                    overwrite_started = true;
                }
                event = response.payload.recv() => {
                    let Some(event) = event? else {
                        anyhow::bail!("Select response ended before the disconnect probe reached its record threshold");
                    };
                    match event {
                        aws_sdk_s3::types::SelectObjectContentEventStream::Records(records) => {
                            if let Some(payload) = records.payload {
                                received_records = received_records.saturating_add(payload.as_ref().len());
                            }
                        }
                        aws_sdk_s3::types::SelectObjectContentEventStream::End(_) => {
                            anyhow::bail!("Select response reached End before the disconnect probe could close it");
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("timed out waiting for an active Select response")??;

    drop(response);
    let overwrite_version = tokio::time::timeout(SNAPSHOT_OPERATION_TIMEOUT, &mut overwrite)
        .await
        .context("overwrite remained blocked after the Select client stream was dropped")?
        .context("overwrite task failed")??;
    if versioned {
        anyhow::ensure!(
            overwrite_version.as_deref().is_some_and(|version| !version.is_empty()),
            "versioned disconnect overwrite omitted version_id"
        );
        anyhow::ensure!(
            initial_version != overwrite_version,
            "versioned disconnect overwrite must create a new generation"
        );
    }
    Ok(())
}

async fn cleanup_snapshot_object(client: &Client, bucket: &str, key: &str, versioned: bool) -> Result<()> {
    if versioned {
        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await
            .with_context(|| format!("list snapshot test versions for {bucket}/{key}"))?;
        for version in versions.versions() {
            if version.key() == Some(key)
                && let Some(version_id) = version.version_id()
            {
                client
                    .delete_object()
                    .bucket(bucket)
                    .key(key)
                    .version_id(version_id)
                    .send()
                    .await
                    .with_context(|| format!("delete snapshot test version {version_id} for {bucket}/{key}"))?;
            }
        }
    } else {
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("delete snapshot test object {bucket}/{key}"))?;
    }
    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("delete snapshot test bucket {bucket}"))?;
    Ok(())
}

async fn run_snapshot_overwrite_http_test(versioned: bool) -> Result<()> {
    let client = create_aws_s3_client().await?;
    let bucket_kind = if versioned { "versioned" } else { "unversioned" };
    let bucket = format!("select-snapshot-{bucket_kind}-{}", Uuid::new_v4().simple());
    let key = "snapshot-race.csv";
    let test_result = async {
        setup_bucket(&client, &bucket).await?;
        if versioned {
            enable_bucket_versioning(&client, &bucket).await?;
        }

        let old_body = snapshot_csv(OLD_GENERATION_POISON, SNAPSHOT_ROW_COUNT);
        let new_body = snapshot_csv(NEW_GENERATION_POISON, SNAPSHOT_ROW_COUNT);
        let disconnect_overwrite = snapshot_csv(NEW_GENERATION_POISON, 1);
        let mut current_body = old_body.clone();
        let mut latest_version = put_snapshot_generation(&client, &bucket, key, current_body.clone()).await?;
        anyhow::ensure!(
            !versioned || latest_version.as_deref().is_some_and(|version| !version.is_empty()),
            "versioned snapshot fixture PUT omitted version_id"
        );

        for round in 0..SNAPSHOT_RACE_ROUNDS {
            let next_body = if round % 2 == 0 { new_body.clone() } else { old_body.clone() };
            let (output, overwrite_version) = select_snapshot_during_pending_overwrite(&client, &bucket, key, next_body.clone())
                .await
                .with_context(|| format!("Select overwrite race round {round}"))?;

            assert_complete_snapshot_output(round, &output, &current_body)?;
            if versioned {
                anyhow::ensure!(
                    overwrite_version.as_deref().is_some_and(|version| !version.is_empty()),
                    "versioned overwrite round {round} omitted version_id"
                );
                anyhow::ensure!(
                    latest_version != overwrite_version,
                    "versioned overwrite round {round} must create a new generation"
                );
                latest_version = overwrite_version;
            }
            current_body = next_body;
        }

        assert_disconnect_releases_snapshot(&client, &bucket, key, old_body, disconnect_overwrite, versioned).await
    }
    .await;
    let cleanup_result =
        tokio::time::timeout(SNAPSHOT_OPERATION_TIMEOUT, cleanup_snapshot_object(&client, &bucket, key, versioned))
            .await
            .context("timed out cleaning up Select overwrite test")
            .and_then(|result| result);
    match (test_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(test_error), Ok(())) => Err(test_error),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error.context("cleanup after Select overwrite test")),
        (Err(test_error), Err(cleanup_error)) => {
            Err(test_error.context(format!("cleanup after Select overwrite test also failed: {cleanup_error:#}")))
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_select_object_content_csv_basic() -> Result<()> {
    let client = create_aws_s3_client().await?;
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
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_select_object_content_csv_aggregation() -> Result<()> {
    let client = create_aws_s3_client().await?;
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
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_select_object_content_json_basic() -> Result<()> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;
    upload_test_json(&client).await?;

    // Construct JSON query
    let sql = "SELECT s.name, s.age FROM S3Object s WHERE s.age > 28";

    let json_input = JsonInput::builder().set_type(Some(JsonType::Document)).build();

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
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_select_object_content_csv_limit() -> Result<()> {
    let client = create_aws_s3_client().await?;
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
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_select_object_content_csv_order_by() -> Result<()> {
    let client = create_aws_s3_client().await?;
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
    assert!(
        result_str.lines().filter(|line| !line.trim().is_empty()).count() >= 2,
        "Should return at least 2 records"
    );

    // Check if contains highest age records
    assert!(result_str.contains("Charlie,35"));
    assert!(result_str.contains("Alice,30"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_select_object_content_error_handling() -> Result<()> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;
    upload_test_csv(&client).await?;

    // Test invalid SQL query
    let sql = "SELECT * FROM S3Object WHERE invalid_column > 10";

    let csv_input = CsvInput::builder().file_header_info(FileHeaderInfo::Use).build();

    let input_serialization = InputSerialization::builder().csv(csv_input).build();

    let csv_output = CsvOutput::builder().build();
    let output_serialization = OutputSerialization::builder().csv(csv_output).build();

    // This query should fail because invalid_column doesn't exist
    let result = client
        .select_object_content()
        .bucket(BUCKET)
        .key(CSV_OBJECT)
        .expression(sql)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await;

    // Verify query fails (expected behavior)
    assert!(result.is_err(), "Query with invalid column should fail");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_select_object_content_nonexistent_object() -> Result<()> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    // Test query on nonexistent object
    let sql = "SELECT * FROM S3Object";

    let csv_input = CsvInput::builder().file_header_info(FileHeaderInfo::Use).build();

    let input_serialization = InputSerialization::builder().csv(csv_input).build();

    let csv_output = CsvOutput::builder().build();
    let output_serialization = OutputSerialization::builder().csv(csv_output).build();

    let result = client
        .select_object_content()
        .bucket(BUCKET)
        .key("nonexistent.csv")
        .expression(sql)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await;

    // Verify query fails (expected behavior)
    assert!(result.is_err(), "Query on nonexistent object should fail");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_select_object_content_overwrite_snapshot_unversioned_http() -> Result<()> {
    run_snapshot_overwrite_http_test(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_select_object_content_overwrite_snapshot_versioned_http() -> Result<()> {
    // SelectObjectContent has no version_id request member; versioned reads therefore race the latest generation.
    run_snapshot_overwrite_http_test(true).await
}
