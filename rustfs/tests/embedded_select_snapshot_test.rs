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

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization, OutputSerialization,
    VersioningConfiguration,
};
use aws_sdk_s3::{Client, Config};
use bytes::Bytes;
use rustfs::embedded::{RustFSServerBuilder, find_available_port};
use rustfs_ecstore::api::set_disk::test_util::{PutObjectCommitBarrier, PutObjectCommitPause};
use std::time::Duration;

mod common;

const OBJECT: &str = "snapshot-race.csv";
const CSV_HEADER: &[u8] = b"generation\n";
const OLD_GENERATION: &[u8] = b"OLD_GENERATION_POISON_1629";
const NEW_GENERATION: &[u8] = b"NEW_GENERATION_POISON_1629";
const ROW_COUNT: usize = 200_000;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(90);

fn s3_client(endpoint: &str, access_key: &str, secret_key: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "select-snapshot-test");
    let config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

fn snapshot_csv(generation: &[u8]) -> Bytes {
    let mut body = Vec::with_capacity(CSV_HEADER.len() + ROW_COUNT * (generation.len() + 1));
    body.extend_from_slice(CSV_HEADER);
    for _ in 0..ROW_COUNT {
        body.extend_from_slice(generation);
        body.push(b'\n');
    }
    Bytes::from(body)
}

async fn put_generation(client: &Client, bucket: &str, body: Bytes) -> Option<String> {
    tokio::time::timeout(
        OPERATION_TIMEOUT,
        client
            .put_object()
            .bucket(bucket)
            .key(OBJECT)
            .body(ByteStream::from(body))
            .send(),
    )
    .await
    .expect("snapshot PUT should finish before the test timeout")
    .expect("snapshot PUT should succeed")
    .version_id
}

async fn start_select(client: &Client, bucket: &str) -> aws_sdk_s3::operation::select_object_content::SelectObjectContentOutput {
    tokio::time::timeout(
        OPERATION_TIMEOUT,
        client
            .select_object_content()
            .bucket(bucket)
            .key(OBJECT)
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
            .send(),
    )
    .await
    .expect("SelectObjectContent should start before the test timeout")
    .expect("SelectObjectContent should start")
}

async fn collect_select(mut response: aws_sdk_s3::operation::select_object_content::SelectObjectContentOutput) -> Vec<u8> {
    tokio::time::timeout(OPERATION_TIMEOUT, async {
        let mut output = Vec::new();
        let mut saw_end = false;
        while let Some(event) = response.payload.recv().await.expect("Select event should decode") {
            match event {
                aws_sdk_s3::types::SelectObjectContentEventStream::Records(records) => {
                    if let Some(payload) = records.payload {
                        output.extend_from_slice(payload.as_ref());
                    }
                }
                aws_sdk_s3::types::SelectObjectContentEventStream::End(_) => {
                    saw_end = true;
                    break;
                }
                _ => {}
            }
        }
        assert!(saw_end, "Select response should contain an End event");
        output
    })
    .await
    .expect("Select response should finish before the test timeout")
}

fn assert_version_advanced(previous: &Option<String>, current: &Option<String>, versioned: bool) {
    if !versioned {
        return;
    }
    assert!(
        previous.as_deref().is_some_and(|version| !version.is_empty()),
        "versioned fixture PUT should return a version ID"
    );
    assert!(
        current.as_deref().is_some_and(|version| !version.is_empty()),
        "versioned overwrite should return a version ID"
    );
    assert_ne!(previous, current, "versioned overwrite should create a new generation");
}

async fn pending_overwrite_keeps_select_on_one_generation(
    client: &Client,
    bucket: &str,
    expected_body: &Bytes,
    replacement_body: Bytes,
) -> Option<String> {
    let barrier = PutObjectCommitBarrier::install(bucket, OBJECT, PutObjectCommitPause::BeforeNamespace);
    let overwrite_client = client.clone();
    let overwrite_bucket = bucket.to_string();
    let overwrite = tokio::spawn(async move { put_generation(&overwrite_client, &overwrite_bucket, replacement_body).await });

    barrier.wait_until_paused().await;
    let response = start_select(client, bucket).await;
    barrier.release();

    let output = collect_select(response).await;
    assert_eq!(
        output.as_slice(),
        &expected_body[CSV_HEADER.len()..],
        "Select should return only the generation captured before overwrite"
    );

    tokio::time::timeout(OPERATION_TIMEOUT, overwrite)
        .await
        .expect("overwrite should finish after Select releases its snapshot")
        .expect("overwrite task should not panic")
}

async fn run_snapshot_case(client: &Client, versioned: bool) {
    let bucket = if versioned {
        "select-snapshot-versioned"
    } else {
        "select-snapshot-unversioned"
    };
    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create snapshot test bucket");
    if versioned {
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
            .expect("enable snapshot test bucket versioning");
    }

    let old_body = snapshot_csv(OLD_GENERATION);
    let new_body = snapshot_csv(NEW_GENERATION);
    let initial_version = put_generation(client, bucket, old_body.clone()).await;
    let overwrite_version = pending_overwrite_keeps_select_on_one_generation(client, bucket, &old_body, new_body.clone()).await;
    assert_version_advanced(&initial_version, &overwrite_version, versioned);
}

#[test]
fn select_snapshot_is_stable_during_http_overwrite() {
    common::run_embedded_test(|| async {
        let port = match find_available_port() {
            Ok(port) => port,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("find free port for Select snapshot test: {error}"),
        };
        let server = RustFSServerBuilder::new()
            .address(format!("127.0.0.1:{port}"))
            .access_key("select-snapshot-access")
            .secret_key("select-snapshot-secret")
            .build()
            .await
            .expect("start embedded RustFS for Select snapshot test");
        assert!(
            server.endpoint().ends_with(&format!(":{port}")),
            "embedded server should bind the requested port"
        );
        let client = s3_client(&server.endpoint(), server.access_key(), server.secret_key());

        run_snapshot_case(&client, false).await;
        run_snapshot_case(&client, true).await;

        server.shutdown().await;
    });
}
