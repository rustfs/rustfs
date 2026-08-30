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
use async_compression::tokio::write::BzEncoder;
use aws_sdk_s3::{
    Client,
    error::ProvideErrorMetadata,
    operation::select_object_content::{SelectObjectContentOutput, builders::SelectObjectContentFluentBuilder},
    types::{
        CompressionType, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization, JsonInput, JsonOutput,
        JsonType, OutputSerialization, SelectObjectContentEventStream,
    },
};
use aws_smithy_types::event_stream::RawMessage;
use bytes::Bytes;
use flate2::{Compression, write::GzEncoder};
use std::{error::Error, io::Cursor, time::Duration};
use tokio::io::AsyncWriteExt;

const BUCKET: &str = "s3-select-compression";
const SELECT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

async fn create_test_environment(extra_env: &[(&str, &str)]) -> TestResult<(RustFSTestEnvironment, Client)> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], extra_env).await?;
    let client = env.create_s3_client();
    client.create_bucket().bucket(BUCKET).send().await?;
    Ok((env, client))
}

async fn put_object(client: &Client, key: &str, body: &[u8]) -> TestResult<()> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::copy_from_slice(body).into())
        .send()
        .await?;
    Ok(())
}

fn gzip(input: &[u8]) -> TestResult<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    std::io::Write::write_all(&mut encoder, input)?;
    Ok(encoder.finish()?)
}

async fn bzip2(input: &[u8]) -> TestResult<Vec<u8>> {
    let mut encoder = BzEncoder::new(Cursor::new(Vec::new()));
    encoder.write_all(input).await?;
    encoder.shutdown().await?;
    Ok(encoder.into_inner().into_inner())
}

fn csv_select_request(
    client: &Client,
    key: &str,
    compression: CompressionType,
    expression: &str,
) -> SelectObjectContentFluentBuilder {
    client
        .select_object_content()
        .bucket(BUCKET)
        .key(key)
        .expression(expression)
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .compression_type(compression)
                .csv(CsvInput::builder().file_header_info(FileHeaderInfo::Use).build())
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
}

fn json_select_request(
    client: &Client,
    key: &str,
    compression: CompressionType,
    json_type: JsonType,
) -> SelectObjectContentFluentBuilder {
    client
        .select_object_content()
        .bucket(BUCKET)
        .key(key)
        .expression("SELECT name FROM S3Object")
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .compression_type(compression)
                .json(JsonInput::builder().set_type(Some(json_type)).build())
                .build(),
        )
        .output_serialization(OutputSerialization::builder().json(JsonOutput::builder().build()).build())
}

async fn collect_success(
    mut response: SelectObjectContentOutput,
    compressed_bytes: usize,
    processed_bytes: usize,
) -> TestResult<Vec<u8>> {
    tokio::time::timeout(SELECT_RESPONSE_TIMEOUT, async move {
        let mut records = Vec::new();
        let mut stats = None;
        let mut saw_end = false;

        while let Some(event) = response.payload.recv().await? {
            assert!(!saw_end, "Select emitted an event after End");
            match event {
                SelectObjectContentEventStream::Records(event) => {
                    assert!(stats.is_none(), "Select emitted Records after Stats");
                    if let Some(payload) = event.payload {
                        records.extend_from_slice(payload.as_ref());
                    }
                }
                SelectObjectContentEventStream::Stats(event) => {
                    assert!(stats.is_none(), "Select emitted more than one Stats event");
                    stats = event.details;
                }
                SelectObjectContentEventStream::End(_) => {
                    assert!(stats.is_some(), "Select emitted End before Stats");
                    saw_end = true;
                }
                _ => assert!(stats.is_none(), "Select emitted a non-terminal event after Stats"),
            }
        }

        let stats = stats.ok_or("Select response ended without a Stats event")?;
        assert_eq!(stats.bytes_scanned(), Some(i64::try_from(compressed_bytes)?));
        assert_eq!(stats.bytes_processed(), Some(i64::try_from(processed_bytes)?));
        assert_eq!(stats.bytes_returned(), Some(i64::try_from(records.len())?));
        assert!(saw_end, "Select response ended without an End event");
        Ok::<_, Box<dyn Error + Send + Sync>>(records)
    })
    .await
    .map_err(|_| -> Box<dyn Error + Send + Sync> { "Select response timed out".into() })?
}

async fn assert_truncated_stream_failure(mut response: SelectObjectContentOutput) -> TestResult<()> {
    tokio::time::timeout(SELECT_RESPONSE_TIMEOUT, async move {
        loop {
            match response.payload.recv().await {
                Err(error) => {
                    // S3 Select request-level errors use `error` frames, which this SDK version exposes as raw response errors.
                    if let Some(code) = error.code() {
                        assert_eq!(code, "TruncatedInput", "unexpected modeled event-stream error: {error:?}");
                    } else if let aws_sdk_s3::error::SdkError::ResponseError(context) = &error
                        && let RawMessage::Decoded(message) = context.raw()
                    {
                        let header = |name: &str| {
                            message
                                .headers()
                                .iter()
                                .find(|header| header.name().as_str() == name)
                                .and_then(|header| header.value().as_string().ok())
                                .map(|value| value.as_str())
                        };
                        assert_eq!(header(":message-type"), Some("error"));
                        assert_eq!(header(":error-code"), Some("TruncatedInput"));
                    } else {
                        panic!("unexpected event-stream error: {error:?}");
                    }
                    return Ok(());
                }
                Ok(Some(SelectObjectContentEventStream::Stats(_))) | Ok(Some(SelectObjectContentEventStream::End(_))) => {
                    return Err("truncated compressed input reached a success terminal event".into());
                }
                Ok(Some(_)) => {}
                Ok(None) => return Err("truncated compressed input ended without an error event".into()),
            }
        }
    })
    .await
    .map_err(|_| -> Box<dyn Error + Send + Sync> { "truncated Select response timed out".into() })?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_compressed_csv_and_json() -> TestResult<()> {
    const CSV: &[u8] = b"name,age\nAlice,30\nBob,25\n";
    const JSON_LINES: &[u8] = b"{\"name\":\"Alice\"}\n{\"name\":\"Bob\"}\n";
    const JSON_DOCUMENT: &[u8] = br#"[{"name":"Alice"},{"name":"Bob"}]"#;

    let (_env, client) = create_test_environment(&[]).await?;

    let gzip_csv = gzip(CSV)?;
    put_object(&client, "records.csv.gz", &gzip_csv).await?;
    let gzip_csv_records = collect_success(
        csv_select_request(&client, "records.csv.gz", CompressionType::Gzip, "SELECT * FROM S3Object")
            .send()
            .await?,
        gzip_csv.len(),
        CSV.len(),
    )
    .await?;
    assert_eq!(gzip_csv_records, b"Alice,30\nBob,25\n");

    let bzip_csv = bzip2(CSV).await?;
    put_object(&client, "records.csv.bz2", &bzip_csv).await?;
    let bzip_csv_records = collect_success(
        csv_select_request(&client, "records.csv.bz2", CompressionType::Bzip2, "SELECT * FROM S3Object")
            .send()
            .await?,
        bzip_csv.len(),
        CSV.len(),
    )
    .await?;
    assert_eq!(bzip_csv_records, gzip_csv_records);

    let gzip_json_lines = gzip(JSON_LINES)?;
    put_object(&client, "json-lines", &gzip_json_lines).await?;
    let gzip_json_records = collect_success(
        json_select_request(&client, "json-lines", CompressionType::Gzip, JsonType::Lines)
            .send()
            .await?,
        gzip_json_lines.len(),
        JSON_LINES.len(),
    )
    .await?;
    assert_eq!(gzip_json_records, JSON_LINES);

    let bzip_json_lines = bzip2(JSON_LINES).await?;
    put_object(&client, "records.jsonl.bz2", &bzip_json_lines).await?;
    let bzip_json_records = collect_success(
        json_select_request(&client, "records.jsonl.bz2", CompressionType::Bzip2, JsonType::Lines)
            .send()
            .await?,
        bzip_json_lines.len(),
        JSON_LINES.len(),
    )
    .await?;
    assert_eq!(bzip_json_records, gzip_json_records);

    let gzip_json_document = gzip(JSON_DOCUMENT)?;
    put_object(&client, "document.json.gz", &gzip_json_document).await?;
    let document_records = collect_success(
        json_select_request(&client, "document.json.gz", CompressionType::Gzip, JsonType::Document)
            .send()
            .await?,
        gzip_json_document.len(),
        JSON_DOCUMENT.len(),
    )
    .await?;
    assert_eq!(document_records, JSON_LINES);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_invalid_compressed_stream_fails() -> TestResult<()> {
    const CSV: &[u8] = b"name\nAlice\n";

    let (_env, client) = create_test_environment(&[]).await?;

    put_object(&client, "invalid.csv.gz", CSV).await?;
    let invalid = csv_select_request(&client, "invalid.csv.gz", CompressionType::Gzip, "SELECT * FROM S3Object")
        .send()
        .await
        .expect_err("invalid GZIP header must fail before streaming");
    assert_eq!(
        invalid.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("InvalidCompressionFormat")
    );

    put_object(&client, "empty.csv.gz", b"").await?;
    let empty = csv_select_request(&client, "empty.csv.gz", CompressionType::Gzip, "SELECT * FROM S3Object")
        .send()
        .await
        .expect_err("empty GZIP input must fail as truncated");
    assert_eq!(empty.as_service_error().and_then(ProvideErrorMetadata::code), Some("TruncatedInput"));

    let mut truncated = bzip2(CSV).await?;
    truncated.pop();
    put_object(&client, "truncated.csv.bz2", &truncated).await?;
    let truncated = csv_select_request(&client, "truncated.csv.bz2", CompressionType::Bzip2, "SELECT * FROM S3Object")
        .send()
        .await?;
    assert_truncated_stream_failure(truncated).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_object_content_compressed_disconnect_releases_query() -> TestResult<()> {
    const OBJECT: &str = "disconnect.csv.gz";
    const ROWS: usize = 16 * 1024;
    const RELEASE_ATTEMPTS: usize = 20;
    const RELEASE_BACKOFF: Duration = Duration::from_millis(25);

    let (_env, client) = create_test_environment(&[("RUSTFS_S3SELECT_MAX_CONCURRENT_QUERIES", "1")]).await?;
    let row = format!("{}\n", "x".repeat(1023));
    let mut body = Vec::with_capacity("value\n".len() + ROWS * row.len());
    body.extend_from_slice(b"value\n");
    for _ in 0..ROWS {
        body.extend_from_slice(row.as_bytes());
    }
    let compressed = gzip(&body)?;
    put_object(&client, OBJECT, &compressed).await?;

    let first = csv_select_request(&client, OBJECT, CompressionType::Gzip, "SELECT * FROM S3Object")
        .send()
        .await?;
    let saturated = csv_select_request(&client, OBJECT, CompressionType::Gzip, "SELECT * FROM S3Object")
        .send()
        .await
        .expect_err("the unread compressed response should retain the only query permit");
    assert_eq!(saturated.as_service_error().and_then(ProvideErrorMetadata::code), Some("SlowDown"));

    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(5), async {
        for attempt in 0..RELEASE_ATTEMPTS {
            match csv_select_request(&client, OBJECT, CompressionType::Gzip, "SELECT * FROM S3Object")
                .send()
                .await
            {
                Ok(response) => return Ok::<_, Box<dyn Error + Send + Sync>>(response),
                Err(error)
                    if error.as_service_error().and_then(ProvideErrorMetadata::code) == Some("SlowDown")
                        && attempt + 1 < RELEASE_ATTEMPTS =>
                {
                    tokio::time::sleep(RELEASE_BACKOFF).await;
                }
                Err(error) if error.as_service_error().and_then(ProvideErrorMetadata::code) == Some("SlowDown") => {
                    return Err("disconnected compressed Select retained its query permit".into());
                }
                Err(error) => return Err(format!("unexpected Select error after disconnect: {error}").into()),
            }
        }
        Err("query permit release retry loop ended unexpectedly".into())
    })
    .await
    .map_err(|_| -> Box<dyn Error + Send + Sync> { "compressed Select did not release its query permit".into() })??;
    drop(second);

    Ok(())
}
