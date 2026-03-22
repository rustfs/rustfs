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

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use http::header::{CONTENT_TYPE, HOST};
use reqwest::StatusCode;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serial_test::serial;
use std::error::Error;

async fn signed_request(
    method: http::Method,
    url: &str,
    access_key: &str,
    secret_key: &str,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let mut request = http::Request::builder().method(method.clone()).uri(uri);
    request = request.header(HOST, authority);
    request = request.header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }

    let content_len = body.as_ref().map(|body| body.len() as i64).unwrap_or_default();
    let signed = sign_v4(request.body(Body::empty())?, content_len, access_key, secret_key, "", "us-east-1");

    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())?;
    let client = local_http_client();
    let mut request_builder = client.request(reqwest_method, url);
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    if let Some(body) = body {
        request_builder = request_builder.body(body);
    }

    Ok(request_builder.send().await?)
}

async fn set_replication_target(
    source_env: &RustFSTestEnvironment,
    source_bucket: &str,
    target_env: &RustFSTestEnvironment,
    target_bucket: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let body = serde_json::json!({
        "endpoint": target_env.address,
        "credentials": {
            "accessKey": target_env.access_key,
            "secretKey": target_env.secret_key
        },
        "targetbucket": target_bucket,
        "secure": false,
        "type": "replication"
    });
    let url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}",
        source_env.url,
        urlencoding::encode(source_bucket)
    );
    let response = signed_request(
        http::Method::PUT,
        &url,
        &source_env.access_key,
        &source_env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("set remote target failed: {status} {body}").into());
    }

    let body = response.bytes().await?;
    let arn: String = serde_json::from_slice(&body)?;
    Ok(arn)
}

async fn put_bucket_replication(
    env: &RustFSTestEnvironment,
    bucket: &str,
    target_arn: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let body = format!(
        r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role></Role>
  <Rule>
    <ID>rule-1</ID>
    <Priority>1</Priority>
    <Status>Enabled</Status>
    <DeleteMarkerReplication>
      <Status>Enabled</Status>
    </DeleteMarkerReplication>
    <ExistingObjectReplication>
      <Status>Enabled</Status>
    </ExistingObjectReplication>
    <Destination>
      <Bucket>{target_arn}</Bucket>
    </Destination>
  </Rule>
</ReplicationConfiguration>"#
    );
    let url = format!("{}/{bucket}?replication", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.into_bytes()),
        Some("application/xml"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("put bucket replication failed: {status} {body}").into());
    }

    Ok(())
}

async fn enable_bucket_versioning(env: &RustFSTestEnvironment, bucket: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = env.create_s3_client();
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

async fn run_replication_check(
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?replication-check", env.url);
    signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn build_replication_pair(
    enable_target_versioning: bool,
) -> Result<(RustFSTestEnvironment, RustFSTestEnvironment, String), Box<dyn Error + Send + Sync>> {
    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-src";
    let target_bucket = "replication-check-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    if enable_target_versioning {
        enable_bucket_versioning(&target_env, target_bucket).await?;
    }

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    Ok((source_env, target_env, source_bucket.to_string()))
}

#[tokio::test]
#[serial]
async fn test_replication_check_succeeds_with_remote_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (_source_env, _target_env, source_bucket) = build_replication_pair(true).await?;
    let response = run_replication_check(&_source_env, &source_bucket).await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await?.is_empty());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_rejects_unversioned_target_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-src";
    let target_bucket = "replication-check-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;

    let err = set_replication_target(&source_env, source_bucket, &target_env, target_bucket)
        .await
        .expect_err("unversioned target bucket should be rejected during remote target setup");
    assert!(err.to_string().contains("not versioned"), "unexpected set remote target error: {err}");

    Ok(())
}
