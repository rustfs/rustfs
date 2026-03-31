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

//! Regression coverage for anonymous access on multipart control APIs.

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use async_compression::tokio::write::{BzEncoder, XzEncoder};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
};
use base64::Engine;
use chrono::{Duration as ChronoDuration, Utc};
use flate2::{Compression, write::GzEncoder};
use http::HeaderValue;
use http::header::{CONTENT_TYPE, HOST};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serial_test::serial;
use std::collections::HashMap;
use std::error::Error;
use std::io::Cursor;
use std::io::Write;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

fn encode_post_policy(conditions: Vec<serde_json::Value>) -> String {
    let expiration = (Utc::now() + ChronoDuration::hours(1))
        .format("%Y-%m-%dT%H:%M:%S.000Z")
        .to_string();
    let policy = serde_json::json!({
        "expiration": expiration,
        "conditions": conditions,
    });

    base64::engine::general_purpose::STANDARD.encode(policy.to_string())
}

fn sse_customer_key_md5_base64(key: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0)
}

async fn make_tar(files: &[(&str, &[u8])], dirs: &[&str]) -> Vec<u8> {
    let buf = Cursor::new(Vec::new());
    let mut builder = tokio_tar::Builder::new(buf);

    for &dir in dirs {
        let mut header = tokio_tar::Header::new_gnu();
        header.set_entry_type(tokio_tar::EntryType::Directory);
        header.set_size(0);
        header.set_mode(0o755);
        header.set_cksum();
        builder
            .append_data(&mut header, dir, Cursor::new(&[] as &[u8]))
            .await
            .expect("directory entry should be appended");
    }

    for &(name, data) in files {
        let mut header = tokio_tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, Cursor::new(data))
            .await
            .expect("file entry should be appended");
    }

    builder.into_inner().await.expect("tar builder should finalize").into_inner()
}

fn build_pax_record(key: &str, value: &str) -> Vec<u8> {
    let payload = format!("{key}={value}\n");
    let mut len = payload.len() + 3;
    loop {
        let record = format!("{len} {payload}");
        if record.len() == len {
            return record.into_bytes();
        }
        len = record.len();
    }
}

async fn make_tar_with_pax_entry(path: &str, data: &[u8], mtime: Option<u64>, pax: &HashMap<&str, String>) -> Vec<u8> {
    let buf = Cursor::new(Vec::new());
    let mut builder = tokio_tar::Builder::new(buf);

    if !pax.is_empty() {
        let mut pax_payload = Vec::new();
        for (key, value) in pax {
            pax_payload.extend(build_pax_record(key, value));
        }

        let mut pax_header = tokio_tar::Header::new_gnu();
        pax_header.set_entry_type(tokio_tar::EntryType::XHeader);
        pax_header.set_size(pax_payload.len() as u64);
        pax_header.set_mode(0o644);
        pax_header.set_cksum();
        builder
            .append_data(&mut pax_header, "PaxHeaders.X/entry", Cursor::new(pax_payload))
            .await
            .expect("pax header entry should be appended");
    }

    let mut header = tokio_tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    header.set_mode(0o644);
    if let Some(mtime) = mtime {
        header.set_mtime(mtime);
    }
    header.set_cksum();
    builder
        .append_data(&mut header, path, Cursor::new(data))
        .await
        .expect("file entry should be appended");

    builder.into_inner().await.expect("tar builder should finalize").into_inner()
}

fn gzip_bytes(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).expect("gzip encoder should accept input");
    encoder.finish().expect("gzip encoder should finish")
}

fn zstd_bytes(data: &[u8]) -> Vec<u8> {
    let mut encoder = zstd::Encoder::new(Vec::new(), 0).expect("zstd encoder should initialize");
    encoder.write_all(data).expect("zstd encoder should accept input");
    encoder.finish().expect("zstd encoder should finish")
}

async fn bzip2_bytes(data: &[u8]) -> Vec<u8> {
    let cursor = Cursor::new(Vec::new());
    let mut encoder = BzEncoder::new(cursor);
    encoder.write_all(data).await.expect("bzip2 encoder should accept input");
    encoder.shutdown().await.expect("bzip2 encoder should finish");
    encoder.into_inner().into_inner()
}

async fn xz_bytes(data: &[u8]) -> Vec<u8> {
    let cursor = Cursor::new(Vec::new());
    let mut encoder = XzEncoder::new(cursor);
    encoder.write_all(data).await.expect("xz encoder should accept input");
    encoder.shutdown().await.expect("xz encoder should finish");
    encoder.into_inner().into_inner()
}

fn assert_s3_error_code<T, E>(result: Result<T, SdkError<E>>, code: &str)
where
    T: std::fmt::Debug,
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let err = result.expect_err("request should fail");
    match err {
        SdkError::ServiceError(service_err) => {
            let s3_err = service_err.into_err();
            assert_eq!(s3_err.meta().code(), Some(code), "unexpected S3 error: {s3_err:?}");
        }
        other_err => panic!("Expected service error {code}, got: {other_err:?}"),
    }
}

async fn signed_raw_request(
    method: http::Method,
    url: &str,
    access_key: &str,
    secret_key: &str,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
    extra_headers: &[(&str, &str)],
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let mut request = http::Request::builder().method(method.clone()).uri(uri);
    request = request.header(HOST, authority);
    request = request.header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }
    for (name, value) in extra_headers {
        request = request.header(*name, *value);
    }

    let content_len = body.as_ref().map(|value| value.len() as i64).unwrap_or_default();
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

async fn allow_anonymous_put_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let policy_json = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAnonymousPutObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:PutObject"],
                "Resource": [format!("arn:aws:s3:::{}/*", bucket)]
            }
        ]
    })
    .to_string();

    client.put_bucket_policy().bucket(bucket).policy(policy_json).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_multipart_control_apis_require_auth() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-multipart-auth";
    let key = "multipart-target";
    let source_key = "copy-source";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    admin_client
        .put_object()
        .bucket(bucket)
        .key(source_key)
        .body(ByteStream::from_static(b"copy-source-data"))
        .send()
        .await?;

    let http = local_http_client();
    let base = format!("{}/{}/{}", env.url, bucket, key);
    let upload_id = "dummy-upload-id";

    let abort_resp = http.delete(format!("{base}?uploadId={upload_id}")).send().await?;
    assert_eq!(
        abort_resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "anonymous AbortMultipartUpload should be rejected"
    );

    let list_parts_resp = http.get(format!("{base}?uploadId={upload_id}")).send().await?;
    assert_eq!(
        list_parts_resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "anonymous ListParts should be rejected"
    );

    let complete_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>"dummy-etag"</ETag>
  </Part>
</CompleteMultipartUpload>"#;
    let complete_resp = http
        .post(format!("{base}?uploadId={upload_id}"))
        .header(reqwest::header::CONTENT_TYPE, "application/xml")
        .body(complete_body)
        .send()
        .await?;
    assert_eq!(
        complete_resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "anonymous CompleteMultipartUpload should be rejected"
    );

    let copy_source = format!("/{bucket}/{source_key}");
    let upload_part_copy_resp = http
        .put(format!("{base}?uploadId={upload_id}&partNumber=1"))
        .header("x-amz-copy-source", copy_source)
        .send()
        .await?;
    assert_eq!(
        upload_part_copy_resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "anonymous UploadPartCopy should be rejected"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_requires_auth() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-auth";
    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let post_form = reqwest::multipart::Form::new().text("key", "post-object.txt").part(
        "file",
        reqwest::multipart::Part::bytes(b"post-object-body".to_vec())
            .file_name("post.txt")
            .mime_str("text/plain")?,
    );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    assert_eq!(
        post_resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "anonymous PostObject should be rejected"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_honors_success_action_status() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy";
    let object_key = "post-policy-object.txt";
    let expected_body = b"anonymous-post-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("success_action_status", "201")
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "PostObject should honor success_action_status=201 when upload is allowed"
    );
    assert!(
        response_body.contains("<PostResponse>"),
        "201 response should contain PostResponse XML, got: {response_body}"
    );
    assert!(
        response_body.contains(&format!("<Bucket>{bucket}</Bucket>")),
        "201 response should include bucket in XML, got: {response_body}"
    );
    assert!(
        response_body.contains(&format!("<Key>{object_key}</Key>")),
        "201 response should include object key in XML, got: {response_body}"
    );
    assert!(
        response_body.contains("<ETag>"),
        "201 response should include ETag in XML, got: {response_body}"
    );

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice(), "uploaded object body should match");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_honors_success_action_redirect() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-redirect";
    let object_key = "post-redirect-object.txt";
    let expected_body = b"anonymous-post-redirect-body".to_vec();
    let redirect_target = "https://example.com/upload/callback?origin=test";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("success_action_redirect", redirect_target.to_string())
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let post_resp = http
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    assert_eq!(
        post_resp.status(),
        reqwest::StatusCode::SEE_OTHER,
        "PostObject should return redirect status when success_action_redirect is set"
    );

    let location = post_resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or("missing redirect location header")?;
    assert!(
        location.starts_with(redirect_target),
        "redirect location should start with requested target, got: {location}"
    );
    assert!(
        location.contains("bucket="),
        "redirect location should include bucket query parameter, got: {location}"
    );
    assert!(
        location.contains("key="),
        "redirect location should include key query parameter, got: {location}"
    );
    assert!(
        location.to_ascii_lowercase().contains("etag="),
        "redirect location should include etag query parameter, got: {location}"
    );

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice(), "uploaded object body should match");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_defaults_to_no_content() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-default-status";
    let object_key = "post-default-object.txt";
    let expected_body = b"anonymous-post-default-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let post_form = reqwest::multipart::Form::new().text("key", object_key.to_string()).part(
        "file",
        reqwest::multipart::Part::bytes(expected_body.clone())
            .file_name("upload.txt")
            .mime_str("text/plain")?,
    );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(
        status,
        reqwest::StatusCode::NO_CONTENT,
        "PostObject should default to 204 when no success_action_status is provided"
    );
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice(), "uploaded object body should match");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_kms() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-kms";
    let object_key = "post-sse-kms-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("x-amz-server-side-encryption", "aws:kms")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-sse-kms-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(
        status,
        reqwest::StatusCode::NOT_IMPLEMENTED,
        "PostObject should reject SSE-KMS form uploads"
    );
    assert!(
        response_body.contains("<Code>NotImplemented</Code>"),
        "response should contain NotImplemented code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_kms_with_key_id_outside_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-kms-keyid";
    let object_key = "post-sse-kms-keyid-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-server-side-encryption": "aws:kms" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-server-side-encryption", "aws:kms")
        .text("x-amz-server-side-encryption-aws-kms-key-id", "test-key")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-sse-kms-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(
        status,
        reqwest::StatusCode::NOT_IMPLEMENTED,
        "SSE-KMS key id should not fail policy validation before runtime rejection"
    );
    assert!(
        response_body.contains("<Code>NotImplemented</Code>"),
        "response should contain NotImplemented code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_kms_with_context_outside_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-kms-context";
    let object_key = "post-sse-kms-context-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-server-side-encryption": "aws:kms" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-server-side-encryption", "aws:kms")
        .text("x-amz-server-side-encryption-context", "e30=")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-sse-kms-context-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(
        status,
        reqwest::StatusCode::NOT_IMPLEMENTED,
        "SSE-KMS context should not fail policy validation before runtime rejection"
    );
    assert!(
        response_body.contains("<Code>NotImplemented</Code>"),
        "response should contain NotImplemented code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_kms_key_id_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-kms-keyid-mismatch";
    let object_key = "post-sse-kms-keyid-mismatch-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-server-side-encryption": "aws:kms" }),
        serde_json::json!({ "x-amz-server-side-encryption-aws-kms-key-id": "expected-key" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-server-side-encryption", "aws:kms")
        .text("x-amz-server-side-encryption-aws-kms-key-id", "other-key")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-sse-kms-keyid-mismatch-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("aws-kms-key-id"),
        "response should mention the conflicting kms key id field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_kms_context_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-kms-context-mismatch";
    let object_key = "post-sse-kms-context-mismatch-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-server-side-encryption": "aws:kms" }),
        serde_json::json!({ "x-amz-server-side-encryption-context": "e30=" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-server-side-encryption", "aws:kms")
        .text("x-amz-server-side-encryption-context", "eyJrIjoiYiJ9")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-sse-kms-context-mismatch-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("server-side-encryption-context"),
        "response should mention the conflicting kms context field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_kms_with_bucket_key_enabled_outside_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-kms-bucket-key";
    let object_key = "post-sse-kms-bucket-key-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-server-side-encryption": "aws:kms" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-server-side-encryption", "aws:kms")
        .text("x-amz-server-side-encryption-bucket-key-enabled", "true")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-sse-kms-bucket-key-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(
        status,
        reqwest::StatusCode::NOT_IMPLEMENTED,
        "SSE-KMS bucket-key-enabled should not fail policy validation before runtime rejection"
    );
    assert!(
        response_body.contains("<Code>NotImplemented</Code>"),
        "response should contain NotImplemented code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_kms_bucket_key_enabled_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-kms-bucket-key-mismatch";
    let object_key = "post-sse-kms-bucket-key-mismatch-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-server-side-encryption": "aws:kms" }),
        serde_json::json!({ "x-amz-server-side-encryption-bucket-key-enabled": "false" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-server-side-encryption", "aws:kms")
        .text("x-amz-server-side-encryption-bucket-key-enabled", "true")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-sse-kms-bucket-key-mismatch-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("bucket-key-enabled"),
        "response should mention the conflicting bucket-key-enabled field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-s3";
    let object_key = "post-sse-s3-object.txt";
    let expected_body = b"post-sse-s3-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-server-side-encryption": "AES256" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-server-side-encryption", "AES256")
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.server_side_encryption().map(|value| value.as_str()), Some("AES256"));

    let uploaded = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = uploaded.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_uses_bucket_default_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-default-sse-s3";
    let object_key = "post-default-sse-s3-object.txt";
    let expected_body = b"post-default-sse-s3-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let encryption_config = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::Aes256)
                        .build()
                        .expect("default encryption rule should build"),
                )
                .build(),
        )
        .build()
        .expect("bucket encryption config should build");

    admin_client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    assert_eq!(post_resp.status(), reqwest::StatusCode::NO_CONTENT);

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.server_side_encryption().map(|value| value.as_str()), Some("AES256"));

    let uploaded = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = uploaded.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_uses_bucket_default_sse_kms() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-default-sse-kms";
    let object_key = "post-default-sse-kms-object.txt";
    let expected_body = b"post-default-sse-kms-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let encryption_config = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::AwsKms)
                        .kms_master_key_id("test-key")
                        .build()
                        .expect("default encryption rule should build"),
                )
                .build(),
        )
        .build()
        .expect("bucket encryption config should build");

    admin_client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    assert_eq!(post_resp.status(), reqwest::StatusCode::NO_CONTENT);

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.server_side_encryption().map(|value| value.as_str()), Some("aws:kms"));

    let uploaded = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = uploaded.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_s3_policy_mismatch() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-s3-reject";
    let object_key = "post-sse-s3-reject-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-server-side-encryption": "AES256" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-server-side-encryption", "aws:kms")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-sse-s3-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_s3_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-s3-missing";
    let object_key = "post-sse-s3-missing-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-server-side-encryption", "AES256")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-sse-s3-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(
        response_body.contains("<Code>AccessDenied</Code>"),
        "response should contain AccessDenied code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_storage_class_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-storage-class";
    let object_key = "post-storage-class-object.txt";
    let expected_body = b"post-storage-class-body".to_vec();
    let storage_class = "STANDARD_IA";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-storage-class": storage_class }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-storage-class", storage_class)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    assert_eq!(post_resp.status(), reqwest::StatusCode::NO_CONTENT);

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.storage_class().map(|value| value.as_str()), Some(storage_class));

    let uploaded = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = uploaded.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_storage_class_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-storage-class-missing";
    let object_key = "post-storage-class-missing-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-storage-class", "STANDARD_IA")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-storage-class-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(
        response_body.contains("<Code>AccessDenied</Code>"),
        "response should contain AccessDenied code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_storage_class_policy_mismatch() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-storage-class-mismatch";
    let object_key = "post-storage-class-mismatch-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-storage-class": "STANDARD_IA" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-storage-class", "ONEZONE_IA")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-storage-class-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("storage-class"),
        "response should mention storage class mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_invalid_storage_class_value() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-storage-class-invalid";
    let object_key = "post-storage-class-invalid-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-storage-class": "INVALID" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-storage-class", "INVALID")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-storage-class-invalid".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidStorageClass</Code>"),
        "response should contain InvalidStorageClass code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_checksum_algorithm_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-checksum-missing";
    let object_key = "post-checksum-missing-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key)
        .text("policy", policy)
        .text("x-amz-checksum-algorithm", "SHA256")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-checksum-missing".to_vec())
                .file_name("checksum.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(
        response_body.contains("<Code>AccessDenied</Code>"),
        "response should contain AccessDenied code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("x-amz-checksum-algorithm"),
        "response should mention x-amz-checksum-algorithm, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_checksum_algorithm_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-checksum-mismatch";
    let object_key = "post-checksum-mismatch-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-checksum-algorithm": "SHA256" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key)
        .text("policy", policy)
        .text("x-amz-checksum-algorithm", "CRC32")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-checksum-mismatch".to_vec())
                .file_name("checksum.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("x-amz-checksum-algorithm"),
        "response should mention x-amz-checksum-algorithm mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_checksum_auxiliary_fields_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let admin_client = env.create_s3_client();

    for (bucket, field_name, field_value) in [
        ("anon-post-checksum-crc32-missing", "x-amz-checksum-crc32", "AAAAAA=="),
        ("anon-post-checksum-crc32c-missing", "x-amz-checksum-crc32c", "AAAAAA=="),
        ("anon-post-checksum-sha1-missing", "x-amz-checksum-sha1", "ZmFrZXNoYTE="),
        ("anon-post-checksum-sha256-missing", "x-amz-checksum-sha256", "ZmFrZXNoYTI1Ng=="),
        ("anon-post-checksum-mode-missing", "x-amz-checksum-mode", "ENABLED"),
    ] {
        let object_key = format!("uploads/{field_name}.txt");

        admin_client.create_bucket().bucket(bucket).send().await?;
        allow_anonymous_put_object(&admin_client, bucket).await?;

        let policy = encode_post_policy(vec![
            serde_json::json!({ "bucket": bucket }),
            serde_json::json!({ "key": object_key }),
            serde_json::json!(["content-length-range", 0, 1024]),
        ]);

        let post_form = reqwest::multipart::Form::new()
            .text("key", object_key.clone())
            .text("policy", policy)
            .text(field_name, field_value)
            .part(
                "file",
                reqwest::multipart::Part::bytes(format!("post-{field_name}").into_bytes())
                    .file_name("checksum.txt")
                    .mime_str("text/plain")?,
            );

        let post_resp = local_http_client()
            .post(format!("{}/{}", env.url, bucket))
            .multipart(post_form)
            .send()
            .await?;

        let status = post_resp.status();
        let response_body = post_resp.text().await?;
        let response_body_lower = response_body.to_ascii_lowercase();

        assert_eq!(status, reqwest::StatusCode::FORBIDDEN, "unexpected status for {field_name}");
        assert!(
            response_body.contains("<Code>AccessDenied</Code>"),
            "response should contain AccessDenied for {field_name}, got: {response_body}"
        );
        assert!(
            response_body_lower.contains(field_name),
            "response should mention {field_name}, got: {response_body}"
        );
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_allows_sse_c_fields_outside_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-c-ignore";
    let object_key = "sse-c-object.txt";
    let expected_body = b"anonymous-post-sse-c".to_vec();
    let customer_key = "01234567890123456789012345678901";
    let customer_key_b64 = base64::engine::general_purpose::STANDARD.encode(customer_key);
    let customer_key_md5 = sse_customer_key_md5_base64(customer_key);

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key)
        .text("policy", policy)
        .text("x-amz-server-side-encryption-customer-algorithm", "AES256")
        .text("x-amz-server-side-encryption-customer-key", customer_key_b64.clone())
        .text("x-amz-server-side-encryption-customer-key-md5", customer_key_md5.clone())
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("sse-c.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    assert_eq!(
        post_resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "SSE-C form fields should be accepted outside policy conditions"
    );

    let head_resp = admin_client
        .head_object()
        .bucket(bucket)
        .key(object_key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(customer_key_b64)
        .sse_customer_key_md5(customer_key_md5.clone())
        .send()
        .await?;
    assert_eq!(head_resp.sse_customer_algorithm(), Some("AES256"));

    let get_resp = admin_client
        .get_object()
        .bucket(bucket)
        .key(object_key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(base64::engine::general_purpose::STANDARD.encode(customer_key))
        .sse_customer_key_md5(customer_key_md5)
        .send()
        .await?;
    let actual_body = get_resp.body.collect().await?.into_bytes().to_vec();
    assert_eq!(actual_body, expected_body);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sse_c_exact_policy_mismatch() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-sse-c-mismatch";
    let object_key = "sse-c-mismatch-object.txt";
    let policy_key = "01234567890123456789012345678901";
    let request_key = "abcdefghijklmnopqrstuvwxyzABCDEF";
    let policy_key_b64 = base64::engine::general_purpose::STANDARD.encode(policy_key);
    let request_key_b64 = base64::engine::general_purpose::STANDARD.encode(request_key);

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-server-side-encryption-customer-algorithm": "AES256" }),
        serde_json::json!({ "x-amz-server-side-encryption-customer-key": policy_key_b64 }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key)
        .text("policy", policy)
        .text("x-amz-server-side-encryption-customer-algorithm", "AES256")
        .text("x-amz-server-side-encryption-customer-key", request_key_b64)
        .text("x-amz-server-side-encryption-customer-key-md5", sse_customer_key_md5_base64(request_key))
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"sse-c-policy-mismatch".to_vec())
                .file_name("sse-c.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_duplicate_key_form_values() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-duplicate-key";
    let object_key = "duplicate-key-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("key", "other-object.txt")
        .text("policy", policy)
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"duplicate-key".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_invalid_success_action_status() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-invalid-status";
    let object_key = "post-invalid-status-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("success_action_status", "202")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-invalid-status-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "PostObject should reject unsupported success_action_status values"
    );
    assert!(
        response_body.contains("<Code>MalformedPOSTRequest</Code>"),
        "response should contain MalformedPOSTRequest code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_invalid_success_action_redirect()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-invalid-redirect";
    let object_key = "post-invalid-redirect-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("success_action_redirect", "://invalid-url")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-invalid-redirect-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "PostObject should reject malformed success_action_redirect values"
    );
    assert!(
        response_body.contains("<Code>MalformedPOSTRequest</Code>"),
        "response should contain MalformedPOSTRequest code, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_form_fields_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-fields";
    let object_key = "post-policy-field-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("success_action_status", "201")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(
        response_body.contains("<Code>AccessDenied</Code>"),
        "response should contain AccessDenied code, got: {response_body}"
    );
    assert!(
        response_body.contains("success_action_status"),
        "response should mention the missing field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_form_fields_covered_by_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-covered";
    let object_key = "post-policy-covered-object.txt";
    let expected_body = b"post-policy-covered-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["starts-with", "$success_action_status", ""]),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("success_action_status", "201")
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::CREATED);
    assert!(
        response_body.contains("<PostResponse>"),
        "201 response should contain PostResponse XML, got: {response_body}"
    );

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_starts_with_policy_mismatch() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-starts-with";
    let object_key = "unexpected/upload.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!(["starts-with", "$key", "uploads/"]),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("starts-with"),
        "response should mention the starts-with condition, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_content_length_range_violation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-length";
    let object_key = "uploads/content-length-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 5]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"payload-too-large".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>EntityTooLarge</Code>"),
        "response should contain EntityTooLarge code, got: {response_body}"
    );
    assert!(
        response_body.contains("maximum allowed object size"),
        "response should mention the size limit, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_success_action_status_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-status-mismatch";
    let object_key = "uploads/status-mismatch-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "success_action_status": "201" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("success_action_status", "204")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("success_action_status"),
        "response should mention the conflicting status field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_success_action_status_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-status-accept";
    let object_key = "uploads/success-action-status-accept.txt";
    let expected_body = b"post-policy-success-action-status-accept".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "success_action_status": "201" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("success_action_status", "201")
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::CREATED);
    assert!(
        response_body.contains("<PostResponse>"),
        "201 response should contain PostResponse XML, got: {response_body}"
    );

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_success_action_redirect_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-redirect-mismatch";
    let object_key = "uploads/redirect-mismatch-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "success_action_redirect": "https://example.com/success" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("success_action_redirect", "https://example.com/other")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("success_action_redirect"),
        "response should mention the conflicting redirect field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_success_action_redirect_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-redirect-accept";
    let object_key = "uploads/success-action-redirect-accept.txt";
    let expected_body = b"post-policy-success-action-redirect-accept".to_vec();
    let redirect_target = "https://example.com/upload/success";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "success_action_redirect": redirect_target }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("success_action_redirect", redirect_target.to_string())
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let post_resp = http
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    assert_eq!(post_resp.status(), reqwest::StatusCode::SEE_OTHER);

    let location = post_resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or("missing redirect location header")?;
    assert!(
        location.starts_with(redirect_target),
        "redirect location should start with requested target, got: {location}"
    );

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_success_action_redirect_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-success-redirect-missing";
    let object_key = "uploads/success-redirect-missing.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("success_action_redirect", "https://example.com/success")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-success-redirect-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("success_action_redirect"),
        "response should mention success_action_redirect, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_metadata_field_covered_by_starts_with()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-meta-accept";
    let object_key = "uploads/meta-object.txt";
    let metadata_value = "alpha-demo";
    let expected_body = b"post-policy-meta-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["starts-with", "$x-amz-meta-project", "alpha-"]),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-meta-project", metadata_value)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    let metadata = head.metadata().expect("head_object should expose uploaded metadata");
    assert_eq!(metadata.get("project").map(String::as_str), Some(metadata_value));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_content_type_field_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-type-accept";
    let object_key = "uploads/content-type-accept.txt";
    let content_type = "text/plain";
    let expected_body = b"post-policy-content-type-accept".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Content-Type": content_type }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Type", content_type)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str(content_type)?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.content_type(), Some(content_type));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_content_type_field_covered_by_starts_with()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-type-accept";
    let object_key = "uploads/content-type-object.txt";
    let content_type = "image/png";
    let expected_body = b"post-policy-content-type-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["starts-with", "$Content-Type", "image/"]),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Type", content_type)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str(content_type)?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.content_type(), Some(content_type));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_content_disposition_field_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-disposition-accept";
    let object_key = "uploads/disposition-object.txt";
    let content_disposition = "attachment; filename=\"upload.txt\"";
    let expected_body = b"post-policy-disposition-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Content-Disposition": content_disposition }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Disposition", content_disposition)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.content_disposition(), Some(content_disposition));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_content_disposition_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-disposition-reject";
    let object_key = "uploads/content-disposition-reject.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Content-Disposition": "attachment; filename=\"payload.bin\"" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Disposition", "inline")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-content-disposition-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("content-disposition"),
        "response should mention content-disposition mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_cache_control_field_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-cache-control-accept";
    let object_key = "uploads/cache-control-object.txt";
    let cache_control = "max-age=60";
    let expected_body = b"post-policy-cache-control-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Cache-Control": cache_control }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Cache-Control", cache_control)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.cache_control(), Some(cache_control));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_cache_control_policy_mismatch() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-cache-control-reject";
    let object_key = "uploads/cache-control-reject.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Cache-Control": "max-age=60" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Cache-Control", "max-age=120")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-cache-control-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("cache-control"),
        "response should mention cache-control mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_cache_control_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-cache-control-missing";
    let object_key = "uploads/cache-control-missing.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Cache-Control", "max-age=60")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-cache-control-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("cache-control"),
        "response should mention cache-control, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_content_language_field_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-language-accept";
    let object_key = "uploads/content-language-object.txt";
    let content_language = "en-US";
    let expected_body = b"post-policy-content-language-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Content-Language": content_language }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Language", content_language)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.content_language(), Some(content_language));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_content_language_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-language-reject";
    let object_key = "uploads/content-language-reject.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Content-Language": "en-US" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Language", "fr-FR")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-content-language-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("content-language"),
        "response should mention content-language mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_content_language_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-language-missing";
    let object_key = "uploads/content-language-missing.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Language", "en-US")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-content-language-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("content-language"),
        "response should mention content-language, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_content_encoding_field_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-encoding-accept";
    let object_key = "uploads/content-encoding-object.txt";
    let content_encoding = "gzip";
    let expected_body = b"post-policy-content-encoding-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Content-Encoding": content_encoding }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Encoding", content_encoding)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.content_encoding(), Some(content_encoding));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_content_encoding_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-encoding-reject";
    let object_key = "uploads/content-encoding-reject.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Content-Encoding": "gzip" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Encoding", "br")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-content-encoding-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("content-encoding"),
        "response should mention content-encoding mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_content_encoding_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-encoding-missing";
    let object_key = "uploads/content-encoding-missing.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Encoding", "gzip")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-content-encoding-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("content-encoding"),
        "response should mention content-encoding, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_website_redirect_location_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-website-redirect-accept";
    let object_key = "uploads/website-redirect-object.txt";
    let website_redirect_location = "/docs/landing.html";
    let expected_body = b"post-policy-website-redirect-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-website-redirect-location": website_redirect_location }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-website-redirect-location", website_redirect_location)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.website_redirect_location(), Some(website_redirect_location));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_website_redirect_location_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-website-redirect-missing";
    let object_key = "uploads/website-redirect-missing.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-website-redirect-location", "/docs/landing.html")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-website-redirect-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("x-amz-website-redirect-location"),
        "response should mention x-amz-website-redirect-location, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_website_redirect_location_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-website-redirect-reject";
    let object_key = "uploads/website-redirect-reject-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-website-redirect-location": "/docs/landing.html" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-website-redirect-location", "/docs/other.html")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"website-redirect-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("x-amz-website-redirect-location"),
        "response should mention x-amz-website-redirect-location mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_expires_field_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-expires-accept";
    let object_key = "uploads/expires-object.txt";
    let expires = "Wed, 21 Oct 2037 07:28:00 GMT";
    let expected_body = b"post-policy-expires-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Expires": expires }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Expires", expires)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(head.expires_string(), Some(expires));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_expires_field_policy_mismatch() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-expires-reject";
    let object_key = "uploads/expires-reject-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Expires": "Wed, 21 Oct 2037 07:28:00 GMT" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Expires", "Wed, 21 Oct 2037 08:28:00 GMT")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-expires-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("expires"),
        "response should mention Expires mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_expires_field_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-expires-missing";
    let object_key = "uploads/expires-missing-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Expires", "Wed, 21 Oct 2037 07:28:00 GMT")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-expires-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("expires"),
        "response should mention Expires, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_object_lock_retention_fields() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-object-lock-retention";
    let object_key = "uploads/object-lock-retention.txt";
    let retain_until = "2037-10-21T07:28:00Z";
    let expected_body = b"post-policy-object-lock-retention-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-object-lock-mode": "GOVERNANCE" }),
        serde_json::json!({ "x-amz-object-lock-retain-until-date": retain_until }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-object-lock-mode", "GOVERNANCE")
        .text("x-amz-object-lock-retain-until-date", retain_until)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let retention = admin_client
        .get_object_retention()
        .bucket(bucket)
        .key(object_key)
        .send()
        .await?;
    let retention = retention.retention().expect("retention should be present");
    assert_eq!(retention.mode().map(|value| value.as_str()), Some("GOVERNANCE"));
    let retain_until_out = retention
        .retain_until_date()
        .expect("retain_until_date should be present")
        .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)?;
    assert_eq!(retain_until_out, retain_until);

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_object_lock_retention_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-object-lock-retention-reject";
    let object_key = "uploads/object-lock-retention-reject.txt";

    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-object-lock-mode": "GOVERNANCE" }),
        serde_json::json!({ "x-amz-object-lock-retain-until-date": "2037-10-21T07:28:00Z" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-object-lock-mode", "GOVERNANCE")
        .text("x-amz-object-lock-retain-until-date", "2037-10-21T08:28:00Z")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-object-lock-retention-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("x-amz-object-lock-retain-until-date"),
        "response should mention x-amz-object-lock-retain-until-date mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_object_lock_mode_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-object-lock-mode-reject";
    let object_key = "uploads/object-lock-mode-reject.txt";

    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-object-lock-mode": "GOVERNANCE" }),
        serde_json::json!({ "x-amz-object-lock-retain-until-date": "2037-10-21T07:28:00Z" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-object-lock-mode", "COMPLIANCE")
        .text("x-amz-object-lock-retain-until-date", "2037-10-21T07:28:00Z")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-object-lock-mode-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("x-amz-object-lock-mode"),
        "response should mention x-amz-object-lock-mode mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_object_lock_retention_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-object-lock-retention-missing";
    let object_key = "uploads/object-lock-retention-missing.txt";

    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-object-lock-mode", "GOVERNANCE")
        .text("x-amz-object-lock-retain-until-date", "2037-10-21T07:28:00Z")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-object-lock-retention-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("x-amz-object-lock-mode")
            || response_body_lower.contains("x-amz-object-lock-retain-until-date"),
        "response should mention object lock retention fields, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_object_lock_legal_hold_field() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-object-lock-legal-hold";
    let object_key = "uploads/object-lock-legal-hold.txt";
    let expected_body = b"post-policy-object-lock-legal-hold-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-object-lock-legal-hold": "ON" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-object-lock-legal-hold", "ON")
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let legal_hold = admin_client
        .get_object_legal_hold()
        .bucket(bucket)
        .key(object_key)
        .send()
        .await?;
    assert_eq!(
        legal_hold
            .legal_hold()
            .and_then(|value| value.status())
            .map(|value| value.as_str()),
        Some("ON")
    );

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_object_lock_legal_hold_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-object-lock-legal-hold-reject";
    let object_key = "uploads/object-lock-legal-hold-reject.txt";

    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-object-lock-legal-hold": "ON" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-object-lock-legal-hold", "OFF")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-object-lock-legal-hold-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("x-amz-object-lock-legal-hold"),
        "response should mention x-amz-object-lock-legal-hold mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_object_lock_legal_hold_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-object-lock-legal-hold-missing";
    let object_key = "uploads/object-lock-legal-hold-missing.txt";

    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-object-lock-legal-hold", "ON")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-object-lock-legal-hold-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("x-amz-object-lock-legal-hold"),
        "response should mention x-amz-object-lock-legal-hold, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_tagging_field_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-tagging-accept";
    let object_key = "uploads/tagging-object.txt";
    let tagging = "project=alpha&env=test";
    let expected_body = b"post-policy-tagging-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-tagging": tagging }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-tagging", tagging)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let tagging_output = admin_client
        .get_object_tagging()
        .bucket(bucket)
        .key(object_key)
        .send()
        .await?;
    let tag_set = tagging_output.tag_set();
    assert_eq!(tag_set.len(), 2);
    assert!(tag_set.iter().any(|tag| tag.key() == "project" && tag.value() == "alpha"));
    assert!(tag_set.iter().any(|tag| tag.key() == "env" && tag.value() == "test"));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_tagging_field_policy_mismatch() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-tagging-reject";
    let object_key = "uploads/tagging-reject-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-tagging": "project=alpha&env=test" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-tagging", "project=alpha&env=prod")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-tagging-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("InvalidPolicyDocument"));
    assert!(
        response_body_lower.contains("x-amz-tagging"),
        "response should mention x-amz-tagging mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_tagging_field_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-tagging-missing";
    let object_key = "uploads/tagging-missing-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-tagging", "project=alpha&env=test")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-tagging-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("x-amz-tagging"),
        "response should mention x-amz-tagging, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_metadata_field_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-meta-reject";
    let object_key = "uploads/meta-reject-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-meta-project", "alpha-demo")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(
        response_body.contains("<Code>AccessDenied</Code>"),
        "response should contain AccessDenied code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("x-amz-meta-project"),
        "response should mention the missing metadata field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_metadata_field_exact_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-meta-exact-mismatch";
    let object_key = "uploads/meta-exact-mismatch-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-meta-project": "alpha-demo" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-meta-project", "beta-demo")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("x-amz-meta-project"),
        "response should mention the conflicting metadata field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_accepts_metadata_field_exact_policy_match()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-meta-exact-accept";
    let object_key = "uploads/meta-exact-accept-object.txt";
    let metadata_value = "alpha-demo";
    let expected_body = b"post-policy-meta-exact-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-meta-project": metadata_value }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-meta-project", metadata_value)
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    let metadata = head.metadata().expect("head_object should expose uploaded metadata");
    assert_eq!(metadata.get("project").map(String::as_str), Some(metadata_value));

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_allows_x_ignore_fields_outside_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-ignore";
    let object_key = "post-policy-ignore-object.txt";
    let expected_body = b"post-policy-ignore-body".to_vec();

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-ignore-trace-id", "trace-123")
        .part(
            "file",
            reqwest::multipart::Part::bytes(expected_body.clone())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
    assert!(response_body.is_empty(), "204 response should not contain a body, got: {response_body}");

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), expected_body.as_slice());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_metadata_field_missing_from_policy_conditions_for_new_key()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-meta-name-missing";
    let object_key = "uploads/meta-name-missing.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-meta-name", "demo-name")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-meta-name-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("<Code>AccessDenied</Code>"));
    assert!(
        response_body_lower.contains("x-amz-meta-name"),
        "response should mention x-amz-meta-name, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_metadata_uuid_exact_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-meta-uuid-mismatch";
    let object_key = "uploads/meta-uuid-mismatch.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-meta-uuid": "14365123651274" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-meta-uuid", "151274")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-meta-uuid-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("<Code>InvalidPolicyDocument</Code>"));
    assert!(
        response_body_lower.contains("x-amz-meta-uuid"),
        "response should mention x-amz-meta-uuid mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sigv4_algorithm_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-sigv4-algorithm-mismatch";
    let object_key = "uploads/sigv4-algorithm-mismatch.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-algorithm": "AWS4-HMAC-SHA256" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-algorithm", "incorrect")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-sigv4-algorithm-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("<Code>InvalidPolicyDocument</Code>"));
    assert!(
        response_body_lower.contains("x-amz-algorithm"),
        "response should mention x-amz-algorithm mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sigv4_credential_policy_mismatch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-sigv4-credential-mismatch";
    let object_key = "uploads/sigv4-credential-mismatch.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-credential": "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-credential", "incorrect")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-sigv4-credential-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("<Code>InvalidPolicyDocument</Code>"));
    assert!(
        response_body_lower.contains("x-amz-credential"),
        "response should mention x-amz-credential mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_sigv4_date_policy_mismatch() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-sigv4-date-mismatch";
    let object_key = "uploads/sigv4-date-mismatch.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "x-amz-date": "20160727T000000Z" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("x-amz-date", "20160728T000000Z")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-sigv4-date-mismatch".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("<Code>InvalidPolicyDocument</Code>"));
    assert!(
        response_body_lower.contains("x-amz-date"),
        "response should mention x-amz-date mismatch, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_mismatched_bucket_form_field() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-bucket-mismatch";
    let object_key = "post-policy-bucket-mismatch-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("bucket", "different-bucket")
        .text("key", object_key.to_string())
        .text("policy", policy)
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body.contains("different-bucket"),
        "response should mention the conflicting bucket field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_multiple_bucket_values() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-many-bucket-values";
    let object_key = "uploads/many-bucket-values.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("bucket", bucket.to_string())
        .text("bucket", "anotherbucket")
        .text("key", object_key.to_string())
        .text("policy", policy)
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-many-bucket-values".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(response_body.contains("<Code>InvalidPolicyDocument</Code>"));
    assert!(
        response_body.contains("anotherbucket") || response_body.contains("multiple values"),
        "response should mention duplicated bucket values, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_extra_content_disposition_field()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-extra-disposition";
    let object_key = "post-policy-extra-disposition-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Disposition", "attachment; filename=\"payload.bin\"")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-body".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(
        response_body.contains("<Code>AccessDenied</Code>"),
        "response should contain AccessDenied code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("content-disposition"),
        "response should mention the extra field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_content_type_policy_mismatch() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-type";
    let object_key = "post-policy-content-type-object.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!({ "Content-Type": "image/jpeg" }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Type", "application/octet-stream")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-body".to_vec())
                .file_name("upload.txt")
                .mime_str("application/octet-stream")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(
        response_body.contains("<Code>InvalidPolicyDocument</Code>"),
        "response should contain InvalidPolicyDocument code, got: {response_body}"
    );
    assert!(
        response_body_lower.contains("content-type"),
        "response should mention the conflicting field, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_post_object_rejects_content_type_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-content-type-missing";
    let object_key = "uploads/content-type-missing.txt";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(vec![
        serde_json::json!({ "bucket": bucket }),
        serde_json::json!({ "key": object_key }),
        serde_json::json!(["content-length-range", 0, 1024]),
    ]);

    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text("Content-Type", "text/plain")
        .part(
            "file",
            reqwest::multipart::Part::bytes(b"post-policy-content-type-missing".to_vec())
                .file_name("upload.txt")
                .mime_str("text/plain")?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;
    let response_body_lower = response_body.to_ascii_lowercase();

    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
    assert!(response_body.contains("AccessDenied"));
    assert!(
        response_body_lower.contains("content-type"),
        "response should mention content-type, got: {response_body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_expands_tar_entries_with_prefix_headers()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-upload";
    let archive_key = "batch.tar";
    let extracted_prefix = "imports/run-01";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("alpha.txt", b"alpha-body"), ("nested/beta.txt", b"beta-body")], &["ignored/"]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
            req.headers_mut().insert("x-amz-meta-acme-snowball-ignore-dirs", "true");
        })
        .send()
        .await?;

    let alpha = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/alpha.txt"))
        .send()
        .await?;
    let alpha_body = alpha.body.collect().await?.into_bytes();
    assert_eq!(alpha_body.as_ref(), b"alpha-body");

    let beta = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/nested/beta.txt"))
        .send()
        .await?;
    let beta_body = beta.body.collect().await?.into_bytes();
    assert_eq!(beta_body.as_ref(), b"beta-body");

    let ignored_dir = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/ignored/"))
        .send()
        .await
        .expect_err("directory marker should be skipped when ignore-dirs is enabled");
    match ignored_dir {
        SdkError::ServiceError(service_err) => {
            let s3_err = service_err.into_err();
            assert!(
                s3_err.is_no_such_key() || s3_err.meta().code() == Some("NoSuchVersion"),
                "Error should be NoSuchKey or NoSuchVersion, got: {s3_err:?}"
            );
        }
        other_err => panic!("Expected ServiceError with missing-object code, got: {other_err:?}"),
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_preserves_request_metadata_on_extracted_objects()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-request-metadata";
    let archive_key = "metadata.tar";
    let extracted_prefix = "imports/metadata";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .cache_control("max-age=60")
        .tagging("project=archive&env=test")
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let head = admin_client
        .head_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/alpha.txt"))
        .send()
        .await?;
    assert_eq!(head.cache_control(), Some("max-age=60"));

    let tagging = admin_client
        .get_object_tagging()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/alpha.txt"))
        .send()
        .await?;

    let mut tags = tagging
        .tag_set()
        .iter()
        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
        .collect::<Vec<_>>();
    tags.sort();
    assert_eq!(
        tags,
        vec![
            ("env".to_string(), "test".to_string()),
            ("project".to_string(), "archive".to_string())
        ]
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_preserves_sse_s3_and_redirect() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-sse-s3-redirect";
    let archive_key = "encrypted-metadata.tar";
    let extracted_prefix = "imports/encrypted";
    let redirect_location = "/docs/extracted.html";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .website_redirect_location(redirect_location)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let head = admin_client
        .head_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/alpha.txt"))
        .send()
        .await?;

    assert_eq!(head.server_side_encryption().map(|value| value.as_str()), Some("AES256"));
    assert_eq!(head.website_redirect_location(), Some(redirect_location));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_preserves_storage_class() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-storage-class";
    let archive_key = "storage-class.tar";
    let extracted_prefix = "imports/storage-class";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .storage_class(aws_sdk_s3::types::StorageClass::StandardIa)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let head = admin_client
        .head_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/alpha.txt"))
        .send()
        .await?;

    assert_eq!(head.storage_class().map(|value| value.as_str()), Some("STANDARD_IA"));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_rejects_invalid_storage_class() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-storage-class-invalid";
    let archive_key = "storage-class-invalid.tar";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;

    let result = admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(|req| {
            req.headers_mut()
                .insert("x-amz-meta-snowball-auto-extract", HeaderValue::from_static("true"));
            req.headers_mut()
                .insert("x-amz-storage-class", HeaderValue::from_static("INVALID"));
        })
        .send()
        .await;

    assert_s3_error_code(result, "InvalidStorageClass");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_rejects_write_offset_bytes_header() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "put-write-offset-reject";
    let key = "write-offset-object";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let result = admin_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"write-offset-body"))
        .customize()
        .mutate_request(|req| {
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_static("0"));
        })
        .send()
        .await;

    assert_s3_error_code(result, "NotImplemented");

    let head_after_reject = admin_client.head_object().bucket(bucket).key(key).send().await;
    match head_after_reject.expect_err("rejected request should not create the object") {
        SdkError::ServiceError(service_err) => {
            let s3_err = service_err.into_err();
            assert!(
                s3_err.meta().code() == Some("NoSuchKey") || s3_err.meta().code() == Some("NotFound"),
                "expected the rejected write to leave no object behind, got: {s3_err:?}"
            );
        }
        other_err => panic!("expected missing object error after rejected write, got: {other_err:?}"),
    }

    admin_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"regular-put-body"))
        .send()
        .await?;

    admin_client.head_object().bucket(bucket).key(key).send().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_raw_signed_put_object_write_offset_bytes_returns_minio_compatible_error_body()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "put-write-offset-raw";
    let key = "write-offset-raw-object";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let response = signed_raw_request(
        http::Method::PUT,
        &format!("{}/{bucket}/{key}", env.url),
        &env.access_key,
        &env.secret_key,
        Some(b"write-offset-body".to_vec()),
        None,
        &[("x-amz-write-offset-bytes", "0")],
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, reqwest::StatusCode::NOT_IMPLEMENTED);
    assert!(body.contains("<Code>NotImplemented</Code>"), "unexpected response body: {body}");
    assert!(
        body.contains("A header you provided implies functionality that is not implemented"),
        "unexpected response body: {body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_anonymous_put_object_write_offset_bytes_returns_minio_compatible_error_body()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "put-write-offset-anon";
    let key = "write-offset-anon-object";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let response = local_http_client()
        .put(format!("{}/{bucket}/{key}", env.url))
        .header("x-amz-write-offset-bytes", "0")
        .body("write-offset-body")
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, reqwest::StatusCode::NOT_IMPLEMENTED);
    assert!(body.contains("<Code>NotImplemented</Code>"), "unexpected response body: {body}");
    assert!(
        body.contains("A header you provided implies functionality that is not implemented"),
        "unexpected response body: {body}"
    );

    let head_after_reject = admin_client.head_object().bucket(bucket).key(key).send().await;
    match head_after_reject.expect_err("rejected anonymous request should not create the object") {
        SdkError::ServiceError(service_err) => {
            let s3_err = service_err.into_err();
            assert!(
                s3_err.meta().code() == Some("NoSuchKey") || s3_err.meta().code() == Some("NotFound"),
                "expected the rejected write to leave no object behind, got: {s3_err:?}"
            );
        }
        other_err => panic!("expected missing object error after rejected anonymous write, got: {other_err:?}"),
    }

    let ok_response = local_http_client()
        .put(format!("{}/{bucket}/{key}", env.url))
        .body("anonymous-plain-put-body")
        .send()
        .await?;
    assert_eq!(ok_response.status(), reqwest::StatusCode::OK);

    let stored = admin_client.get_object().bucket(bucket).key(key).send().await?;
    let stored_body = stored.body.collect().await?.into_bytes();
    assert_eq!(stored_body.as_ref(), b"anonymous-plain-put-body");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_uses_bucket_default_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-default-sse-s3";
    let archive_key = "default-encryption.tar";
    let extracted_prefix = "imports/default-encryption";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let encryption_config = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::Aes256)
                        .build()
                        .expect("default encryption rule should build"),
                )
                .build(),
        )
        .build()
        .expect("bucket encryption config should build");

    admin_client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    let tar_bytes = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let head = admin_client
        .head_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/alpha.txt"))
        .send()
        .await?;

    assert_eq!(head.server_side_encryption().map(|value| value.as_str()), Some("AES256"));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_rejects_bucket_default_sse_kms() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-default-sse-kms";
    let archive_key = "default-encryption-kms.tar";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let encryption_config = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::AwsKms)
                        .kms_master_key_id("test-key")
                        .build()
                        .expect("default encryption rule should build"),
                )
                .build(),
        )
        .build()
        .expect("bucket encryption config should build");

    admin_client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    let tar_bytes = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;

    let result = admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(|req| {
            req.headers_mut()
                .insert("x-amz-meta-snowball-auto-extract", HeaderValue::from_static("true"));
        })
        .send()
        .await;

    assert_s3_error_code(result, "NotImplemented");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_preserves_sse_c() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "extract-sse-c";
    let archive_key = "bundle.tar";
    let extracted_key = "nested/file.txt";
    let expected_body = b"extract-sse-c-body".to_vec();
    let customer_key = "01234567890123456789012345678901";
    let customer_key_b64 = base64::engine::general_purpose::STANDARD.encode(customer_key);
    let customer_key_md5 = sse_customer_key_md5_base64(customer_key);

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;

    let archive = make_tar(&[(extracted_key, expected_body.as_slice())], &[]).await;

    client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(archive))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(customer_key_b64.clone())
        .sse_customer_key_md5(customer_key_md5.clone())
        .customize()
        .mutate_request(|req| {
            req.headers_mut()
                .insert("x-amz-meta-snowball-auto-extract", HeaderValue::from_static("true"));
            req.headers_mut()
                .insert("x-amz-meta-rustfs-snowball-prefix", HeaderValue::from_static("extract-root"));
        })
        .send()
        .await?;

    let extracted = client
        .head_object()
        .bucket(bucket)
        .key("extract-root/nested/file.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(customer_key_b64.clone())
        .sse_customer_key_md5(customer_key_md5.clone())
        .send()
        .await?;
    assert_eq!(extracted.sse_customer_algorithm(), Some("AES256"));

    let fetched = client
        .get_object()
        .bucket(bucket)
        .key("extract-root/nested/file.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(customer_key_b64)
        .sse_customer_key_md5(customer_key_md5)
        .send()
        .await?;
    let actual_body = fetched.body.collect().await?.into_bytes().to_vec();
    assert_eq!(actual_body, expected_body);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_preserves_object_lock_legal_hold() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-object-lock-hold";
    let archive_key = "legal-hold.tar";
    let extracted_prefix = "imports/legal-hold";

    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;

    let tar_bytes = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .object_lock_legal_hold_status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let legal_hold = admin_client
        .get_object_legal_hold()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/alpha.txt"))
        .send()
        .await?;

    assert_eq!(
        legal_hold
            .legal_hold()
            .and_then(|value| value.status())
            .map(|value| value.as_str()),
        Some("ON")
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_preserves_object_lock_retention() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-object-lock-retention";
    let archive_key = "retention.tar";
    let extracted_prefix = "imports/retention";
    let retain_until = aws_sdk_s3::primitives::DateTime::from_secs(2_143_623_680);
    let retain_until_expected = retain_until.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)?;

    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;

    let tar_bytes = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
        .object_lock_retain_until_date(retain_until)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let retention = admin_client
        .get_object_retention()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/alpha.txt"))
        .send()
        .await?;
    let retention = retention.retention().expect("retention should be present");

    assert_eq!(retention.mode().map(|value| value.as_str()), Some("GOVERNANCE"));
    assert_eq!(
        retention
            .retain_until_date()
            .expect("retain_until_date should be present")
            .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)?,
        retain_until_expected
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_returns_archive_etag() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-etag";
    let archive_key = "bundle.tar";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;

    let archive = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;
    let expected_etag = format!("\"{:x}\"", md5::compute(&archive));

    let response = client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(archive))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await?;

    assert_eq!(response.e_tag(), Some(expected_etag.as_str()));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_preserves_entry_mtime() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-mtime";
    let archive_key = "bundle.tar";
    let extracted_key = "mtime/file.txt";
    let modified_at_secs = 1_704_000_123_u64;

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;

    let archive = make_tar_with_pax_entry(extracted_key, b"mtime-body", Some(modified_at_secs), &HashMap::new()).await;

    client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(archive))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await?;

    let head = client.head_object().bucket(bucket).key(extracted_key).send().await?;
    assert_eq!(head.last_modified().expect("last_modified should exist").secs(), modified_at_secs as i64);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_preserves_pax_metadata_and_version_id()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-pax";
    let archive_key = "bundle.tar";
    let extracted_key = "pax/alpha.txt";
    let expected_version_id = Uuid::new_v4().to_string();

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;

    let mut pax = HashMap::new();
    pax.insert("minio.metadata.project", "alpha-demo".to_string());
    pax.insert("minio.metadata.x-amz-meta-owner", "ops".to_string());
    pax.insert("minio.versionId", expected_version_id.clone());
    let archive = make_tar_with_pax_entry(extracted_key, b"pax-body", None, &pax).await;

    client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(archive))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await?;

    let head = client.head_object().bucket(bucket).key(extracted_key).send().await?;
    let metadata = head.metadata().expect("head_object should expose metadata");
    assert_eq!(metadata.get("project").map(String::as_str), Some("alpha-demo"));
    assert_eq!(metadata.get("owner").map(String::as_str), Some("ops"));
    assert_eq!(head.version_id(), Some(expected_version_id.as_str()));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_accepts_compat_header() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-compat";
    let archive_key = "compat.tar";
    let extracted_prefix = "imports/compat";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("gamma.txt", b"gamma-body")], &[]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let gamma = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/gamma.txt"))
        .send()
        .await?;
    let gamma_body = gamma.body.collect().await?.into_bytes();
    assert_eq!(gamma_body.as_ref(), b"gamma-body");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_preserves_directory_markers_by_default()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-dirs";
    let archive_key = "dirs.tar";
    let extracted_prefix = "imports/tree";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("nested/file.txt", b"file-body")], &["empty/", "nested/"]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let empty_dir = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/empty/"))
        .send()
        .await?;
    let empty_dir_body = empty_dir.body.collect().await?.into_bytes();
    assert!(empty_dir_body.is_empty(), "directory marker object should be empty");

    let nested_dir = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/nested/"))
        .send()
        .await?;
    let nested_dir_body = nested_dir.body.collect().await?.into_bytes();
    assert!(nested_dir_body.is_empty(), "nested directory marker object should be empty");

    let nested_file = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/nested/file.txt"))
        .send()
        .await?;
    let nested_file_body = nested_file.body.collect().await?.into_bytes();
    assert_eq!(nested_file_body.as_ref(), b"file-body");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_expands_tar_gz_archive() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-tar-gz";
    let archive_key = "bundle.tar.gz";
    let extracted_prefix = "imports/gzip";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("delta.txt", b"delta-body"), ("nested/epsilon.txt", b"epsilon-body")], &[]).await;
    let tar_gz_bytes = gzip_bytes(&tar_bytes);

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_gz_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let delta = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/delta.txt"))
        .send()
        .await?;
    let delta_body = delta.body.collect().await?.into_bytes();
    assert_eq!(delta_body.as_ref(), b"delta-body");

    let epsilon = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/nested/epsilon.txt"))
        .send()
        .await?;
    let epsilon_body = epsilon.body.collect().await?.into_bytes();
    assert_eq!(epsilon_body.as_ref(), b"epsilon-body");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_expands_tgz_archive() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-tgz";
    let archive_key = "bundle.tgz";
    let extracted_prefix = "imports/tgz";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("phi.txt", b"phi-body"), ("nested/psi.txt", b"psi-body")], &[]).await;
    let tgz_bytes = gzip_bytes(&tar_bytes);

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tgz_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let phi = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/phi.txt"))
        .send()
        .await?;
    let phi_body = phi.body.collect().await?.into_bytes();
    assert_eq!(phi_body.as_ref(), b"phi-body");

    let psi = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/nested/psi.txt"))
        .send()
        .await?;
    let psi_body = psi.body.collect().await?.into_bytes();
    assert_eq!(psi_body.as_ref(), b"psi-body");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_expands_tbz2_archive() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-tbz2";
    let archive_key = "bundle.tbz2";
    let extracted_prefix = "imports/tbz2";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("rho.txt", b"rho-body"), ("nested/tau.txt", b"tau-body")], &[]).await;
    let tbz2_bytes = bzip2_bytes(&tar_bytes).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tbz2_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let rho = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/rho.txt"))
        .send()
        .await?;
    let rho_body = rho.body.collect().await?.into_bytes();
    assert_eq!(rho_body.as_ref(), b"rho-body");

    let tau = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/nested/tau.txt"))
        .send()
        .await?;
    let tau_body = tau.body.collect().await?.into_bytes();
    assert_eq!(tau_body.as_ref(), b"tau-body");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_expands_txz_archive() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-txz";
    let archive_key = "bundle.txz";
    let extracted_prefix = "imports/txz";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("chi.txt", b"chi-body"), ("nested/upsilon.txt", b"upsilon-body")], &[]).await;
    let txz_bytes = xz_bytes(&tar_bytes).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(txz_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let chi = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/chi.txt"))
        .send()
        .await?;
    let chi_body = chi.body.collect().await?.into_bytes();
    assert_eq!(chi_body.as_ref(), b"chi-body");

    let upsilon = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/nested/upsilon.txt"))
        .send()
        .await?;
    let upsilon_body = upsilon.body.collect().await?.into_bytes();
    assert_eq!(upsilon_body.as_ref(), b"upsilon-body");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_skips_invalid_entry_when_ignore_errors_enabled()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-ignore-errors";
    let archive_key = "bundle.tar";
    let extracted_prefix = "imports/ignore-errors";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));

    let mut valid_header = tokio_tar::Header::new_gnu();
    valid_header.set_size(b"valid-body".len() as u64);
    valid_header.set_mode(0o644);
    valid_header.set_cksum();
    builder
        .append_data(&mut valid_header, "valid.txt", Cursor::new(b"valid-body".as_slice()))
        .await
        .expect("valid tar entry should be appended");

    let long_name = format!("{}.txt", "a".repeat(1100));
    let mut invalid_header = tokio_tar::Header::new_gnu();
    invalid_header.set_size(b"ignored-body".len() as u64);
    invalid_header.set_mode(0o644);
    invalid_header.set_cksum();
    builder
        .append_data(&mut invalid_header, long_name, Cursor::new(b"ignored-body".as_slice()))
        .await
        .expect("long-name tar entry should be appended");

    let tar_bytes = builder.into_inner().await.expect("tar builder should finalize").into_inner();

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
            req.headers_mut().insert("x-amz-meta-acme-snowball-ignore-errors", "true");
        })
        .send()
        .await?;

    let valid = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/valid.txt"))
        .send()
        .await?;
    let valid_body = valid.body.collect().await?.into_bytes();
    assert_eq!(valid_body.as_ref(), b"valid-body");

    let listed = admin_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(format!("{extracted_prefix}/"))
        .send()
        .await?;
    let keys: Vec<_> = listed.contents().iter().filter_map(|entry| entry.key()).collect();
    assert_eq!(keys, vec![format!("{extracted_prefix}/valid.txt")]);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_normalizes_prefix_header_value() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-prefix-normalize";
    let archive_key = "bundle.tar";
    let extracted_prefix = " /batch/incoming/ ";
    let normalized_prefix = "batch/incoming";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("theta.txt", b"theta-body")], &[]).await;

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let theta = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{normalized_prefix}/theta.txt"))
        .send()
        .await?;
    let theta_body = theta.body.collect().await?.into_bytes();
    assert_eq!(theta_body.as_ref(), b"theta-body");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_expands_tzst_archive() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-tzst";
    let archive_key = "bundle.tzst";
    let extracted_prefix = "imports/tzst";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("omega.txt", b"omega-body"), ("nested/sigma.txt", b"sigma-body")], &[]).await;
    let tzst_bytes = zstd_bytes(&tar_bytes);

    admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tzst_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            req.headers_mut().insert("x-amz-meta-acme-snowball-prefix", extracted_prefix);
        })
        .send()
        .await?;

    let omega = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/omega.txt"))
        .send()
        .await?;
    let omega_body = omega.body.collect().await?.into_bytes();
    assert_eq!(omega_body.as_ref(), b"omega-body");

    let sigma = admin_client
        .get_object()
        .bucket(bucket)
        .key(format!("{extracted_prefix}/nested/sigma.txt"))
        .send()
        .await?;
    let sigma_body = sigma.body.collect().await?.into_bytes();
    assert_eq!(sigma_body.as_ref(), b"sigma-body");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_rejects_missing_archive_extension() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-no-ext";
    let archive_key = "bundle";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let tar_bytes = make_tar(&[("plain.txt", b"plain-body")], &[]).await;

    let result = admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from(tar_bytes))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await;

    assert_s3_error_code(result, "InvalidArgument");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_signed_put_object_extract_rejects_invalid_tar_gz_payload() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-bad-gzip";
    let archive_key = "broken.tar.gz";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;

    let result = admin_client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .body(ByteStream::from_static(b"not-a-gzip-stream"))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await;

    assert_s3_error_code(result, "InvalidArgument");

    Ok(())
}
