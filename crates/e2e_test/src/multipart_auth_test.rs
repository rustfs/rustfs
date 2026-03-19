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

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::primitives::ByteStream;
use base64::Engine;
use chrono::{Duration as ChronoDuration, Utc};
use serial_test::serial;
use std::io::Cursor;

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

    let http = reqwest::Client::new();
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

    let post_resp = reqwest::Client::new()
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

    let post_resp = reqwest::Client::new()
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

    let post_resp = reqwest::Client::new()
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

    let post_resp = reqwest::Client::new()
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

    let post_resp = reqwest::Client::new()
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

    let post_resp = reqwest::Client::new()
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

    let post_resp = reqwest::Client::new()
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

    let post_resp = reqwest::Client::new()
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

    let post_resp = reqwest::Client::new()
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
