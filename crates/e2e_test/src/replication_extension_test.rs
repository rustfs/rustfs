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

async fn send_set_replication_target_request(
    source_env: &RustFSTestEnvironment,
    source_bucket: &str,
    update: bool,
    body: serde_json::Value,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}",
        source_env.url,
        urlencoding::encode(source_bucket)
    );
    if update {
        url.push_str("&update=true");
    }
    signed_request(
        http::Method::PUT,
        &url,
        &source_env.access_key,
        &source_env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await
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

async fn remove_replication_target(
    env: &RustFSTestEnvironment,
    bucket: &str,
    arn: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!(
        "{}/rustfs/admin/v3/remove-remote-target?bucket={}&arn={}",
        env.url,
        urlencoding::encode(bucket),
        urlencoding::encode(arn)
    );
    signed_request(http::Method::DELETE, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn remove_replication_target_request(
    env: &RustFSTestEnvironment,
    bucket: Option<&str>,
    arn: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!("{}/rustfs/admin/v3/remove-remote-target", env.url);
    let mut separator = '?';

    if let Some(bucket) = bucket {
        url.push(separator);
        separator = '&';
        url.push_str("bucket=");
        url.push_str(&urlencoding::encode(bucket));
    }

    if let Some(arn) = arn {
        url.push(separator);
        url.push_str("arn=");
        url.push_str(&urlencoding::encode(arn));
    }

    signed_request(http::Method::DELETE, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn list_replication_targets_request(
    env: &RustFSTestEnvironment,
    bucket: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!("{}/rustfs/admin/v3/list-remote-targets", env.url);
    if let Some(bucket) = bucket {
        url.push_str("?bucket=");
        url.push_str(&urlencoding::encode(bucket));
    }
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
async fn test_replication_check_rejects_target_without_object_lock()
-> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-lock-src";
    let target_bucket = "replication-check-lock-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client
        .create_bucket()
        .bucket(source_bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let response = run_replication_check(&source_env, source_bucket).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("object lock"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_rejects_unversioned_source_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-unversioned-src";
    let target_bucket = "replication-check-unversioned-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&target_env, target_bucket).await?;

    let err = set_replication_target(&source_env, source_bucket, &target_env, target_bucket)
        .await
        .expect_err("unversioned source bucket should be rejected during remote target setup");
    let err = err.to_string();

    assert!(err.contains("400 Bad Request"), "unexpected set remote target error: {err}");
    assert!(err.contains("InvalidRequest"), "unexpected set remote target error: {err}");
    assert!(err.to_ascii_lowercase().contains("not versioned"), "unexpected set remote target error: {err}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_replication_check_rejects_unversioned_source_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-check-source-unversioned";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;

    let response = run_replication_check(&env, bucket).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("versioning"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_replication_check_rejects_missing_replication_config() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-check-missing-config";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&env, bucket).await?;

    let response = run_replication_check(&env, bucket).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body.contains("ReplicationConfigurationNotFoundError"),
        "unexpected response: {body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_replication_check_rejects_invalid_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = run_replication_check(&env, "replication-check-no-such-bucket").await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchBucket"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_rejects_same_bucket_on_same_deployment() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-check-same-target";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&env, bucket).await?;

    let body = serde_json::json!({
        "endpoint": env.address,
        "credentials": {
            "accessKey": env.access_key,
            "secretKey": env.secret_key
        },
        "targetbucket": bucket,
        "secure": false,
        "type": "replication"
    });
    let url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}",
        env.url,
        urlencoding::encode(bucket)
    );
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("IncorrectEndpoint"), "unexpected response: {body}");

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

#[tokio::test]
#[serial]
async fn test_set_remote_target_update_requires_arn() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-update-needs-arn-src";
    let target_bucket = "replication-update-needs-arn-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let response = send_set_replication_target_request(
        &source_env,
        source_bucket,
        true,
        serde_json::json!({
            "endpoint": target_env.address,
            "credentials": {
                "accessKey": target_env.access_key,
                "secretKey": target_env.secret_key
            },
            "targetbucket": target_bucket,
            "secure": false,
            "type": "replication"
        }),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("arn is empty"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_update_rejects_missing_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-update-missing-target-src";
    let target_bucket = "replication-update-missing-target-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let response = send_set_replication_target_request(
        &source_env,
        source_bucket,
        true,
        serde_json::json!({
            "endpoint": target_env.address,
            "credentials": {
                "accessKey": target_env.access_key,
                "secretKey": target_env.secret_key
            },
            "targetbucket": target_bucket,
            "secure": false,
            "type": "replication",
            "arn": "arn:aws:s3:us-east-1:123456789012:replication::missing-target"
        }),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("target not found"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_rejects_invalid_target_url() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-invalid-target-url-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;

    let response = send_set_replication_target_request(
        &source_env,
        bucket,
        false,
        serde_json::json!({
            "endpoint": "://invalid-target-url",
            "credentials": {
                "accessKey": "replication",
                "secretKey": "replication"
            },
            "targetbucket": "target-bucket",
            "secure": false,
            "type": "replication"
        }),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("invalid target url"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_list_remote_targets_rejects_empty_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = list_replication_targets_request(&env, Some("")).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("bucket is required"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_list_remote_targets_rejects_invalid_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = list_replication_targets_request(&env, Some("missing-replication-target-bucket")).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchBucket"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_remove_remote_target_rejects_missing_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let bucket = "replication-remove-missing-target";
    let target_bucket = "replication-remove-missing-target-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let arn = set_replication_target(&source_env, bucket, &target_env, target_bucket).await?;

    let first_remove = remove_replication_target(&source_env, bucket, &arn).await?;
    assert_eq!(first_remove.status(), StatusCode::NO_CONTENT);

    let response = remove_replication_target(&source_env, bucket, &arn).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("not found"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_remove_remote_target_rejects_missing_arn() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-remove-missing-arn";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&env, bucket).await?;

    let response = remove_replication_target_request(&env, Some(bucket), None).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("arn is required"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_remove_remote_target_rejects_invalid_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = remove_replication_target_request(
        &env,
        Some("missing-replication-remove-bucket"),
        Some("arn:aws:s3:us-east-1:123456789012:replication::missing"),
    )
    .await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchBucket"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_remove_remote_target_rejects_target_used_by_replication() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (source_env, _target_env, source_bucket) = build_replication_pair(true).await?;
    let targets_url = format!(
        "{}/rustfs/admin/v3/list-remote-targets?bucket={}",
        source_env.url,
        urlencoding::encode(&source_bucket)
    );
    let targets_response = signed_request(
        http::Method::GET,
        &targets_url,
        &source_env.access_key,
        &source_env.secret_key,
        None,
        None,
    )
    .await?;
    assert_eq!(targets_response.status(), StatusCode::OK);
    let targets: Vec<serde_json::Value> = targets_response.json().await?;
    let arn = targets
        .first()
        .and_then(|target| target.get("arn"))
        .and_then(|arn| arn.as_str())
        .ok_or("replication target arn missing")?
        .to_string();

    let response = remove_replication_target(&source_env, &source_bucket, &arn).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("removal disallowed"), "unexpected response: {body}");

    Ok(())
}
