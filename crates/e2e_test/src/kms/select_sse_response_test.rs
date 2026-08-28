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

//! SelectObjectContent SSE response-header compatibility (backlog#1625).

use super::common::{LocalKMSTestEnvironment, sse_customer_key_md5_base64, start_kms};
use crate::common::signed_s3_request_with_headers;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use base64_simd::STANDARD as BASE64;
use http::{HeaderMap, Method};
use std::error::Error;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const CSV_BODY: &[u8] = b"name\nalice\n";
const SELECT_BODY: &str = r#"<SelectObjectContentRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Expression>SELECT * FROM S3Object</Expression>
<ExpressionType>SQL</ExpressionType>
<InputSerialization><CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization>
</SelectObjectContentRequest>"#;
const KMS_CONTEXT: &str = "eyJ0ZW5hbnQiOiJzMy1zZWxlY3QifQ==";
const SSE_ALGORITHM: &str = "x-amz-server-side-encryption";
const SSE_KMS_KEY_ID: &str = "x-amz-server-side-encryption-aws-kms-key-id";
const SSE_KMS_CONTEXT: &str = "x-amz-server-side-encryption-context";
const SSE_C_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";
const SSE_C_KEY_MD5: &str = "x-amz-server-side-encryption-customer-key-md5";
const LOG_FLUSH_SENTINEL: &str = "select-sse-log-flush-sentinel.csv";

async fn raw_select(
    env: &crate::common::RustFSTestEnvironment,
    bucket: &str,
    object: &str,
    request_headers: &HeaderMap,
) -> TestResult<reqwest::Response> {
    let url = format!("{}/{bucket}/{object}?select&select-type=2", env.url);
    signed_s3_request_with_headers(
        Method::POST,
        &url,
        Some(SELECT_BODY.to_string()),
        Some("application/xml"),
        &env.access_key,
        &env.secret_key,
        request_headers,
    )
    .await
}

async fn assert_success_headers(response: reqwest::Response, expected: &[(&str, &str)], absent: &[&str]) -> TestResult {
    if response.status() != reqwest::StatusCode::OK {
        let status = response.status();
        let url = response.url().clone();
        let body = response.text().await?;
        panic!("Select request to {url} failed with {status}: {body}");
    }
    for (name, value) in expected {
        assert_eq!(response.headers().get(*name).and_then(|header| header.to_str().ok()), Some(*value));
    }
    for name in absent {
        assert!(response.headers().get(*name).is_none(), "successful Select response must omit {name}");
    }
    let body = response.bytes().await?;
    assert!(
        body.windows(b"alice".len()).any(|window| window == b"alice"),
        "successful Select response must contain a Records event with the selected row"
    );
    assert!(
        body.windows(b"End".len()).any(|window| window == b"End"),
        "successful Select response must contain the terminal End event"
    );
    Ok(())
}

async fn assert_pre_stream_failure(response: reqwest::Response) -> TestResult {
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = response.text().await?;
    assert!(body.contains("<Error>"), "pre-stream failure must return an S3 XML error: {body}");
    assert!(
        body.contains("<Code>InvalidRequest</Code>"),
        "invalid SSE-C parameters must preserve the S3 error code: {body}"
    );
    Ok(())
}

fn put_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    object: &str,
) -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder {
    client
        .put_object()
        .bucket(bucket)
        .key(object)
        .body(ByteStream::from_static(CSV_BODY))
}

#[tokio::test]
async fn select_projects_encryption_headers_and_rejects_invalid_sse_c_before_streaming() -> TestResult {
    let mut kms = LocalKMSTestEnvironment::new().await?;
    let log_path = format!("{}/server.log", kms.base_env.temp_dir);
    kms.base_env.capture_log_path = Some(log_path.clone());
    kms.base_env
        .start_rustfs_server_with_env(Vec::new(), &[("RUST_LOG", "s3s=debug,rustfs=info")])
        .await?;
    let key_id = kms.configure_local_kms().await?;
    start_kms(&kms.base_env.url, &kms.base_env.access_key, &kms.base_env.secret_key).await?;

    let client = kms.base_env.create_s3_client();
    let bucket = format!("select-sse-{}", Uuid::new_v4().simple());
    client.create_bucket().bucket(&bucket).send().await?;

    put_object(&client, &bucket, "plain.csv").send().await?;
    put_object(&client, &bucket, "sse-s3.csv")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;
    put_object(&client, &bucket, "sse-kms.csv")
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(&key_id)
        .ssekms_encryption_context(KMS_CONTEXT)
        .send()
        .await?;

    let customer_key = "01234567890123456789012345678901";
    let customer_key_b64 = BASE64.encode_to_string(customer_key);
    let customer_key_md5 = sse_customer_key_md5_base64(customer_key);
    put_object(&client, &bucket, "sse-c.csv")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key_b64)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;

    assert_success_headers(
        raw_select(&kms.base_env, &bucket, "plain.csv", &HeaderMap::new()).await?,
        &[],
        &[
            SSE_ALGORITHM,
            SSE_KMS_KEY_ID,
            SSE_KMS_CONTEXT,
            SSE_C_ALGORITHM,
            SSE_C_KEY,
            SSE_C_KEY_MD5,
        ],
    )
    .await?;
    assert_success_headers(
        raw_select(&kms.base_env, &bucket, "sse-s3.csv", &HeaderMap::new()).await?,
        &[(SSE_ALGORITHM, "AES256")],
        &[SSE_KMS_KEY_ID, SSE_KMS_CONTEXT, SSE_C_ALGORITHM, SSE_C_KEY, SSE_C_KEY_MD5],
    )
    .await?;
    assert_success_headers(
        raw_select(&kms.base_env, &bucket, "sse-kms.csv", &HeaderMap::new()).await?,
        &[
            (SSE_ALGORITHM, "aws:kms"),
            (SSE_KMS_KEY_ID, &key_id),
            (SSE_KMS_CONTEXT, KMS_CONTEXT),
        ],
        &[SSE_C_ALGORITHM, SSE_C_KEY, SSE_C_KEY_MD5],
    )
    .await?;

    let mut sse_c_headers = HeaderMap::new();
    sse_c_headers.insert(SSE_C_ALGORITHM, "AES256".parse()?);
    sse_c_headers.insert(SSE_C_KEY, customer_key_b64.parse()?);
    sse_c_headers.insert(SSE_C_KEY_MD5, customer_key_md5.parse()?);
    assert_success_headers(
        raw_select(&kms.base_env, &bucket, "sse-c.csv", &sse_c_headers).await?,
        &[(SSE_C_ALGORITHM, "AES256"), (SSE_C_KEY_MD5, &customer_key_md5)],
        &[SSE_ALGORITHM, SSE_KMS_KEY_ID, SSE_KMS_CONTEXT, SSE_C_KEY],
    )
    .await?;

    assert_pre_stream_failure(raw_select(&kms.base_env, &bucket, "sse-c.csv", &HeaderMap::new()).await?).await?;

    let mut missing_algorithm_headers = HeaderMap::new();
    missing_algorithm_headers.insert(SSE_C_KEY, customer_key_b64.parse()?);
    missing_algorithm_headers.insert(SSE_C_KEY_MD5, customer_key_md5.parse()?);
    assert_pre_stream_failure(raw_select(&kms.base_env, &bucket, "sse-c.csv", &missing_algorithm_headers).await?).await?;

    let mut wrong_algorithm_headers = sse_c_headers.clone();
    wrong_algorithm_headers.insert(SSE_C_ALGORITHM, "AES128".parse()?);
    assert_pre_stream_failure(raw_select(&kms.base_env, &bucket, "sse-c.csv", &wrong_algorithm_headers).await?).await?;

    let wrong_md5 = sse_customer_key_md5_base64("99999999999999999999999999999999");
    let mut wrong_md5_headers = sse_c_headers.clone();
    wrong_md5_headers.insert(SSE_C_KEY_MD5, wrong_md5.parse()?);
    assert_pre_stream_failure(raw_select(&kms.base_env, &bucket, "sse-c.csv", &wrong_md5_headers).await?).await?;

    let wrong_key = "99999999999999999999999999999999";
    let wrong_key_b64 = BASE64.encode_to_string(wrong_key);
    let mut wrong_key_headers = HeaderMap::new();
    wrong_key_headers.insert(SSE_C_ALGORITHM, "AES256".parse()?);
    wrong_key_headers.insert(SSE_C_KEY, wrong_key_b64.parse()?);
    wrong_key_headers.insert(SSE_C_KEY_MD5, wrong_md5.parse()?);
    assert_pre_stream_failure(raw_select(&kms.base_env, &bucket, "sse-c.csv", &wrong_key_headers).await?).await?;

    put_object(&client, &bucket, LOG_FLUSH_SENTINEL).send().await?;
    assert_success_headers(
        raw_select(&kms.base_env, &bucket, LOG_FLUSH_SENTINEL, &HeaderMap::new()).await?,
        &[],
        &[
            SSE_ALGORITHM,
            SSE_KMS_KEY_ID,
            SSE_KMS_CONTEXT,
            SSE_C_ALGORITHM,
            SSE_C_KEY,
            SSE_C_KEY_MD5,
        ],
    )
    .await?;
    let mut logs = String::new();
    for _ in 0..100 {
        logs = tokio::fs::read_to_string(&log_path).await?;
        if logs.contains(LOG_FLUSH_SENTINEL) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(logs.contains(LOG_FLUSH_SENTINEL), "timed out waiting for the log sink to flush");
    for secret in [customer_key, customer_key_b64.as_str(), wrong_key, wrong_key_b64.as_str()] {
        assert!(!logs.contains(secret), "Select request logging leaked SSE-C customer key material");
    }

    Ok(())
}
