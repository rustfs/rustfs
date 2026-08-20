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
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
};
use base64::Engine;
use chrono::{Duration as ChronoDuration, Utc};
use flate2::{Compression, write::GzEncoder};
use http::HeaderValue;
use http::header::{CONTENT_TYPE, HOST};
use md5::{Digest as Md5Digest, Md5};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
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
    let mut hasher = Md5::new();
    hasher.update(key.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

fn md5_hex(input: impl AsRef<[u8]>) -> String {
    let mut hasher = Md5::new();
    hasher.update(input.as_ref());
    hex::encode(hasher.finalize())
}

async fn create_restricted_user(
    env: &RustFSTestEnvironment,
    username: &str,
    secret_key: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-user?accessKey={username}", env.url);
    let body = serde_json::json!({
        "secretKey": secret_key,
        "status": "enabled"
    })
    .to_string();
    crate::common::awscurl_put(&url, &body, &env.access_key, &env.secret_key).await?;
    Ok(())
}

fn restricted_user_client(env: &RustFSTestEnvironment, username: &str, secret_key: &str) -> aws_sdk_s3::Client {
    let credentials = aws_sdk_s3::config::Credentials::new(username, secret_key, None, None, "snowball-pax-auth-test");
    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .endpoint_url(&env.url)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

/// Env var consumed by the local SSE-S3 DEK provider when KMS is not configured.
///
/// Since rustfs#3564 the server fails closed on managed SSE (SSE-S3 or
/// bucket-default encryption) unless KMS is configured or this master key is
/// provided, so tests exercising managed SSE on a bare server must seed it.
const LOCAL_SSE_MASTER_KEY_ENV: &str = "RUSTFS_SSE_S3_MASTER_KEY";

fn local_sse_master_key_value() -> String {
    base64::engine::general_purpose::STANDARD.encode([0x42u8; 32])
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

        // Pax extension entries must carry a POSIX ustar header — this is what real
        // tar writers emit, and the server-side reader rejects an XHeader typeflag on
        // GNU-format headers ("extension typeflag is not permitted on an unrecognized
        // header").
        let mut pax_header = tokio_tar::Header::new_ustar();
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

/// One rejected POST Object upload driven end-to-end (backlog#1838): starts a
/// fresh server, allows anonymous PutObject on `bucket`, posts an anonymous
/// POST Object form whose policy carries `policy_conditions` and whose form
/// carries `form_fields` on top of the mandatory key+policy fields, then
/// asserts the expected status, error code, and lowercase-body mention.
/// `case` prefixes every assertion message so a failing table row is
/// identifiable at a glance.
#[allow(clippy::too_many_arguments)]
async fn run_post_object_policy_case(
    bucket: &str,
    object_key: &str,
    policy_conditions: Vec<serde_json::Value>,
    form_fields: &[(&str, &str)],
    file_body: &[u8],
    expected_status: reqwest::StatusCode,
    expected_code: &str,
    expected_mention: &str,
    case: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(policy_conditions);

    let mut post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy);
    for (name, value) in form_fields {
        post_form = post_form.text(name.to_string(), value.to_string());
    }
    let post_form = post_form.part(
        "file",
        reqwest::multipart::Part::bytes(file_body.to_vec())
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

    assert_eq!(status, expected_status, "[{case}] unexpected status, body: {response_body}");
    assert!(
        response_body.contains(expected_code),
        "[{case}] response should contain {expected_code}, got: {response_body}"
    );
    assert!(
        response_body_lower.contains(expected_mention),
        "[{case}] response should mention {expected_mention}, got: {response_body}"
    );

    Ok(())
}

/// One accepted POST Object upload driven end-to-end (backlog#1838): starts a
/// fresh server, allows anonymous PutObject on `bucket`, posts an anonymous
/// POST Object form whose policy carries `policy_conditions` and whose form
/// carries `form_field` on top of the mandatory key+policy fields, then asserts
/// 204 with an empty body, that `read_stored` observes the submitted value on
/// the stored object, and that the object body round-tripped unchanged.
/// `case` prefixes every assertion message so a failing table row is
/// identifiable at a glance.
#[allow(clippy::too_many_arguments)]
async fn run_post_object_accept_case(
    bucket: &str,
    object_key: &str,
    policy_conditions: Vec<serde_json::Value>,
    form_field: (&str, &str),
    file_mime: &str,
    file_body: &[u8],
    read_stored: fn(&HeadObjectOutput) -> Option<&str>,
    case: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    allow_anonymous_put_object(&admin_client, bucket).await?;

    let policy = encode_post_policy(policy_conditions);

    let (field_name, field_value) = form_field;
    let post_form = reqwest::multipart::Form::new()
        .text("key", object_key.to_string())
        .text("policy", policy)
        .text(field_name.to_string(), field_value.to_string())
        .part(
            "file",
            reqwest::multipart::Part::bytes(file_body.to_vec())
                .file_name("upload.txt")
                .mime_str(file_mime)?,
        );

    let post_resp = local_http_client()
        .post(format!("{}/{}", env.url, bucket))
        .multipart(post_form)
        .send()
        .await?;

    let status = post_resp.status();
    let response_body = post_resp.text().await?;

    assert_eq!(status, reqwest::StatusCode::NO_CONTENT, "[{case}] unexpected status");
    assert!(
        response_body.is_empty(),
        "[{case}] 204 response should not contain a body, got: {response_body}"
    );

    let head = admin_client.head_object().bucket(bucket).key(object_key).send().await?;
    assert_eq!(read_stored(&head), Some(field_value), "[{case}] stored {field_name} mismatch");

    let get_out = admin_client.get_object().bucket(bucket).key(object_key).send().await?;
    let uploaded = get_out.body.collect().await?.into_bytes();
    assert_eq!(uploaded.as_ref(), file_body, "[{case}] uploaded body mismatch");

    Ok(())
}

/// Table-driven fold of the nine `*_missing_from_policy_conditions` POST
/// Object tests (backlog#1838 PR1). Every row keeps its original test's exact
/// bucket, key, form field, file body, and expected error strings; the shared
/// shape is: policy pins bucket + key + content-length-range only, the form
/// smuggles one extra field the policy never declared, and the upload must be
/// rejected with 403 AccessDenied naming the offending field.
#[tokio::test]
async fn test_anonymous_post_object_rejects_fields_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    // (case, bucket, object_key, form field, file body, expected code, expected mention)
    type Case = (
        &'static str,
        &'static str,
        &'static str,
        (&'static str, &'static str),
        &'static [u8],
        &'static str,
        &'static str,
    );
    let cases: &[Case] = &[
        (
            "cache-control",
            "anon-post-policy-cache-control-missing",
            "uploads/cache-control-missing.txt",
            ("Cache-Control", "max-age=60"),
            b"post-policy-cache-control-missing",
            "AccessDenied",
            "cache-control",
        ),
        (
            "content-language",
            "anon-post-policy-content-language-missing",
            "uploads/content-language-missing.txt",
            ("Content-Language", "en-US"),
            b"post-policy-content-language-missing",
            "AccessDenied",
            "content-language",
        ),
        (
            "content-encoding",
            "anon-post-policy-content-encoding-missing",
            "uploads/content-encoding-missing.txt",
            ("Content-Encoding", "gzip"),
            b"post-policy-content-encoding-missing",
            "AccessDenied",
            "content-encoding",
        ),
        (
            "website-redirect-location",
            "anon-post-policy-website-redirect-missing",
            "uploads/website-redirect-missing.txt",
            ("x-amz-website-redirect-location", "/docs/landing.html"),
            b"post-policy-website-redirect-missing",
            "AccessDenied",
            "x-amz-website-redirect-location",
        ),
        (
            "expires",
            "anon-post-policy-expires-missing",
            "uploads/expires-missing-object.txt",
            ("Expires", "Wed, 21 Oct 2037 07:28:00 GMT"),
            b"post-policy-expires-missing",
            "AccessDenied",
            "expires",
        ),
        (
            "tagging",
            "anon-post-policy-tagging-missing",
            "uploads/tagging-missing-object.txt",
            ("x-amz-tagging", "project=alpha&env=test"),
            b"post-policy-tagging-missing",
            "AccessDenied",
            "x-amz-tagging",
        ),
        (
            "metadata",
            "anon-post-policy-meta-reject",
            "uploads/meta-reject-object.txt",
            ("x-amz-meta-project", "alpha-demo"),
            b"post-policy-body",
            "<Code>AccessDenied</Code>",
            "x-amz-meta-project",
        ),
        (
            "metadata-new-key",
            "anon-post-policy-meta-name-missing",
            "uploads/meta-name-missing.txt",
            ("x-amz-meta-name", "demo-name"),
            b"post-policy-meta-name-missing",
            "<Code>AccessDenied</Code>",
            "x-amz-meta-name",
        ),
        (
            "content-type",
            "anon-post-policy-content-type-missing",
            "uploads/content-type-missing.txt",
            ("Content-Type", "text/plain"),
            b"post-policy-content-type-missing",
            "AccessDenied",
            "content-type",
        ),
    ];

    for (case, bucket, object_key, form_field, file_body, expected_code, expected_mention) in cases {
        run_post_object_policy_case(
            bucket,
            object_key,
            vec![
                serde_json::json!({ "bucket": bucket }),
                serde_json::json!({ "key": object_key }),
                serde_json::json!(["content-length-range", 0, 1024]),
            ],
            &[*form_field],
            file_body,
            reqwest::StatusCode::FORBIDDEN,
            expected_code,
            expected_mention,
            case,
        )
        .await?;
    }

    Ok(())
}

/// Table-driven fold of the seven `*_policy_mismatch` POST Object tests
/// (backlog#1838 PR2). Every row keeps its original test's exact bucket, key,
/// policy value, mismatched form value, file body, and expected error strings;
/// the shared shape is: the policy pins the field to one exact value, the form
/// sends a different one, and the upload must be rejected with 400
/// InvalidPolicyDocument naming the field.
#[tokio::test]
async fn test_anonymous_post_object_rejects_exact_condition_policy_mismatches()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    // (case, bucket, object_key, field, policy value, mismatched form value, file body, expected code, expected mention)
    type Case = (
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static [u8],
        &'static str,
        &'static str,
    );
    let cases: &[Case] = &[
        (
            "content-disposition",
            "anon-post-policy-content-disposition-reject",
            "uploads/content-disposition-reject.txt",
            "Content-Disposition",
            "attachment; filename=\"payload.bin\"",
            "inline",
            b"post-policy-content-disposition-mismatch",
            "InvalidPolicyDocument",
            "content-disposition",
        ),
        (
            "content-language",
            "anon-post-policy-content-language-reject",
            "uploads/content-language-reject.txt",
            "Content-Language",
            "en-US",
            "fr-FR",
            b"post-policy-content-language-mismatch",
            "InvalidPolicyDocument",
            "content-language",
        ),
        (
            "content-encoding",
            "anon-post-policy-content-encoding-reject",
            "uploads/content-encoding-reject.txt",
            "Content-Encoding",
            "gzip",
            "br",
            b"post-policy-content-encoding-mismatch",
            "InvalidPolicyDocument",
            "content-encoding",
        ),
        (
            "website-redirect-location",
            "anon-post-policy-website-redirect-reject",
            "uploads/website-redirect-reject-object.txt",
            "x-amz-website-redirect-location",
            "/docs/landing.html",
            "/docs/other.html",
            b"website-redirect-mismatch",
            "InvalidPolicyDocument",
            "x-amz-website-redirect-location",
        ),
        (
            "metadata-uuid-exact",
            "anon-post-policy-meta-uuid-mismatch",
            "uploads/meta-uuid-mismatch.txt",
            "x-amz-meta-uuid",
            "14365123651274",
            "151274",
            b"post-policy-meta-uuid-mismatch",
            "<Code>InvalidPolicyDocument</Code>",
            "x-amz-meta-uuid",
        ),
        (
            "sigv4-algorithm",
            "anon-post-policy-sigv4-algorithm-mismatch",
            "uploads/sigv4-algorithm-mismatch.txt",
            "x-amz-algorithm",
            "AWS4-HMAC-SHA256",
            "incorrect",
            b"post-policy-sigv4-algorithm-mismatch",
            "<Code>InvalidPolicyDocument</Code>",
            "x-amz-algorithm",
        ),
        (
            "sigv4-credential",
            "anon-post-policy-sigv4-credential-mismatch",
            "uploads/sigv4-credential-mismatch.txt",
            "x-amz-credential",
            "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request",
            "incorrect",
            b"post-policy-sigv4-credential-mismatch",
            "<Code>InvalidPolicyDocument</Code>",
            "x-amz-credential",
        ),
        (
            "cache-control",
            "anon-post-policy-cache-control-reject",
            "uploads/cache-control-reject.txt",
            "Cache-Control",
            "max-age=60",
            "max-age=120",
            b"post-policy-cache-control-mismatch",
            "InvalidPolicyDocument",
            "cache-control",
        ),
        (
            "expires",
            "anon-post-policy-expires-reject",
            "uploads/expires-reject-object.txt",
            "Expires",
            "Wed, 21 Oct 2037 07:28:00 GMT",
            "Wed, 21 Oct 2037 08:28:00 GMT",
            b"post-policy-expires-mismatch",
            "InvalidPolicyDocument",
            "expires",
        ),
        (
            "tagging",
            "anon-post-policy-tagging-reject",
            "uploads/tagging-reject-object.txt",
            "x-amz-tagging",
            "project=alpha&env=test",
            "project=alpha&env=prod",
            b"post-policy-tagging-mismatch",
            "InvalidPolicyDocument",
            "x-amz-tagging",
        ),
        (
            "storage-class",
            "anon-post-storage-class-mismatch",
            "post-storage-class-mismatch-object.txt",
            "x-amz-storage-class",
            "STANDARD_IA",
            "ONEZONE_IA",
            b"post-storage-class-mismatch",
            "<Code>InvalidPolicyDocument</Code>",
            "storage-class",
        ),
        (
            "content-type",
            "anon-post-policy-content-type",
            "post-policy-content-type-object.txt",
            "Content-Type",
            "image/jpeg",
            "application/octet-stream",
            b"post-policy-body",
            "<Code>InvalidPolicyDocument</Code>",
            "content-type",
        ),
        (
            "success-action-status",
            "anon-post-policy-status-mismatch",
            "uploads/status-mismatch-object.txt",
            "success_action_status",
            "201",
            "204",
            b"post-policy-body",
            "<Code>InvalidPolicyDocument</Code>",
            "success_action_status",
        ),
        (
            "metadata-field-exact",
            "anon-post-policy-meta-exact-mismatch",
            "uploads/meta-exact-mismatch-object.txt",
            "x-amz-meta-project",
            "alpha-demo",
            "beta-demo",
            b"post-policy-body",
            "<Code>InvalidPolicyDocument</Code>",
            "x-amz-meta-project",
        ),
    ];

    for (case, bucket, object_key, field, policy_value, form_value, file_body, expected_code, expected_mention) in cases {
        let mut pinned_condition = serde_json::Map::new();
        pinned_condition.insert((*field).to_string(), serde_json::Value::String((*policy_value).to_string()));

        run_post_object_policy_case(
            bucket,
            object_key,
            vec![
                serde_json::json!({ "bucket": bucket }),
                serde_json::json!({ "key": object_key }),
                serde_json::Value::Object(pinned_condition),
                serde_json::json!(["content-length-range", 0, 1024]),
            ],
            &[(*field, *form_value)],
            file_body,
            reqwest::StatusCode::BAD_REQUEST,
            expected_code,
            expected_mention,
            case,
        )
        .await?;
    }

    Ok(())
}

/// Table-driven fold of the two object-lock `*_policy_mismatch` tests
/// (backlog#1838 PR3): the policy pins both object-lock fields, the form sends
/// one of them with a different value, and the upload must be rejected with
/// 400 InvalidPolicyDocument naming the mismatched field.
#[tokio::test]
async fn test_anonymous_post_object_rejects_object_lock_policy_mismatches() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();

    // (case, bucket, object_key, form mode, form retain-until, file body, expected mention)
    type Case = (
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static [u8],
        &'static str,
    );
    let cases: &[Case] = &[
        (
            "retention",
            "anon-post-policy-object-lock-retention-reject",
            "uploads/object-lock-retention-reject.txt",
            "GOVERNANCE",
            "2037-10-21T08:28:00Z",
            b"post-policy-object-lock-retention-mismatch",
            "x-amz-object-lock-retain-until-date",
        ),
        (
            "mode",
            "anon-post-policy-object-lock-mode-reject",
            "uploads/object-lock-mode-reject.txt",
            "COMPLIANCE",
            "2037-10-21T07:28:00Z",
            b"post-policy-object-lock-mode-mismatch",
            "x-amz-object-lock-mode",
        ),
    ];

    for (case, bucket, object_key, form_mode, form_retain, file_body, expected_mention) in cases {
        run_post_object_policy_case(
            bucket,
            object_key,
            vec![
                serde_json::json!({ "bucket": bucket }),
                serde_json::json!({ "key": object_key }),
                serde_json::json!({ "x-amz-object-lock-mode": "GOVERNANCE" }),
                serde_json::json!({ "x-amz-object-lock-retain-until-date": "2037-10-21T07:28:00Z" }),
                serde_json::json!(["content-length-range", 0, 1024]),
            ],
            &[
                ("x-amz-object-lock-mode", *form_mode),
                ("x-amz-object-lock-retain-until-date", *form_retain),
            ],
            file_body,
            reqwest::StatusCode::BAD_REQUEST,
            "InvalidPolicyDocument",
            expected_mention,
            case,
        )
        .await?;
    }

    Ok(())
}

/// Table-driven fold of the three SSE-KMS `*_policy_mismatch` tests
/// (backlog#1838 PR3): the policy pins the SSE mode and one KMS parameter to
/// exact values, the form sends a different parameter value, and the upload
/// must be rejected with 400 InvalidPolicyDocument naming the parameter.
#[tokio::test]
async fn test_anonymous_post_object_rejects_sse_kms_policy_mismatches() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    // (case, bucket, object_key, kms field, policy value, mismatched form value, file body, expected mention)
    type Case = (
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static [u8],
        &'static str,
    );
    let cases: &[Case] = &[
        (
            "key-id",
            "anon-post-sse-kms-keyid-mismatch",
            "post-sse-kms-keyid-mismatch-object.txt",
            "x-amz-server-side-encryption-aws-kms-key-id",
            "expected-key",
            "other-key",
            b"post-sse-kms-keyid-mismatch-body",
            "aws-kms-key-id",
        ),
        (
            "context",
            "anon-post-sse-kms-context-mismatch",
            "post-sse-kms-context-mismatch-object.txt",
            "x-amz-server-side-encryption-context",
            "e30=",
            "eyJrIjoiYiJ9",
            b"post-sse-kms-context-mismatch-body",
            "server-side-encryption-context",
        ),
        (
            "bucket-key-enabled",
            "anon-post-sse-kms-bucket-key-mismatch",
            "post-sse-kms-bucket-key-mismatch-object.txt",
            "x-amz-server-side-encryption-bucket-key-enabled",
            "false",
            "true",
            b"post-sse-kms-bucket-key-mismatch-body",
            "bucket-key-enabled",
        ),
    ];

    for (case, bucket, object_key, field, policy_value, form_value, file_body, expected_mention) in cases {
        let mut pinned_condition = serde_json::Map::new();
        pinned_condition.insert((*field).to_string(), serde_json::Value::String((*policy_value).to_string()));

        run_post_object_policy_case(
            bucket,
            object_key,
            vec![
                serde_json::json!({ "bucket": bucket }),
                serde_json::json!({ "key": object_key }),
                serde_json::json!({ "x-amz-server-side-encryption": "aws:kms" }),
                serde_json::Value::Object(pinned_condition),
                serde_json::json!(["content-length-range", 0, 1024]),
            ],
            &[("x-amz-server-side-encryption", "aws:kms"), (*field, *form_value)],
            file_body,
            reqwest::StatusCode::BAD_REQUEST,
            "<Code>InvalidPolicyDocument</Code>",
            expected_mention,
            case,
        )
        .await?;
    }

    Ok(())
}

/// Table-driven fold of the three SSE-KMS `*_outside_policy_conditions` tests
/// (backlog#1838 PR3): the policy pins only the SSE mode, the form smuggles
/// one extra KMS parameter the policy never declared, and the request must
/// sail past policy validation and be rejected at runtime with 501
/// NotImplemented (SSE-KMS POST uploads are not implemented), not with a
/// policy error.
#[tokio::test]
async fn test_anonymous_post_object_rejects_sse_kms_params_outside_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    // (case, bucket, object_key, extra kms form field, file body)
    type Case = (&'static str, &'static str, &'static str, (&'static str, &'static str), &'static [u8]);
    let cases: &[Case] = &[
        (
            "key-id",
            "anon-post-sse-kms-keyid",
            "post-sse-kms-keyid-object.txt",
            ("x-amz-server-side-encryption-aws-kms-key-id", "test-key"),
            b"post-sse-kms-body",
        ),
        (
            "context",
            "anon-post-sse-kms-context",
            "post-sse-kms-context-object.txt",
            ("x-amz-server-side-encryption-context", "e30="),
            b"post-sse-kms-context-body",
        ),
        (
            "bucket-key-enabled",
            "anon-post-sse-kms-bucket-key",
            "post-sse-kms-bucket-key-object.txt",
            ("x-amz-server-side-encryption-bucket-key-enabled", "true"),
            b"post-sse-kms-bucket-key-body",
        ),
    ];

    for (case, bucket, object_key, kms_field, file_body) in cases {
        run_post_object_policy_case(
            bucket,
            object_key,
            vec![
                serde_json::json!({ "bucket": bucket }),
                serde_json::json!({ "key": object_key }),
                serde_json::json!({ "x-amz-server-side-encryption": "aws:kms" }),
                serde_json::json!(["content-length-range", 0, 1024]),
            ],
            &[("x-amz-server-side-encryption", "aws:kms"), *kms_field],
            file_body,
            reqwest::StatusCode::NOT_IMPLEMENTED,
            "<Code>NotImplemented</Code>",
            "notimplemented",
            case,
        )
        .await?;
    }

    Ok(())
}

#[tokio::test]
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
async fn test_anonymous_post_object_accepts_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    let master_key = local_sse_master_key_value();
    env.start_rustfs_server_with_env(vec![], &[(LOCAL_SSE_MASTER_KEY_ENV, master_key.as_str())])
        .await?;

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
async fn test_anonymous_post_object_uses_bucket_default_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    let master_key = local_sse_master_key_value();
    env.start_rustfs_server_with_env(vec![], &[(LOCAL_SSE_MASTER_KEY_ENV, master_key.as_str())])
        .await?;

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
async fn test_anonymous_post_object_uses_bucket_default_sse_kms() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    let master_key = local_sse_master_key_value();
    env.start_rustfs_server_with_env(vec![], &[(LOCAL_SSE_MASTER_KEY_ENV, master_key.as_str())])
        .await?;

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
async fn test_anonymous_post_object_accepts_sse_s3_missing_from_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    // MinIO-compatible POST-policy validation (s3s-project/s3s#608) exempts the
    // x-amz-server-side-encryption* form fields from the "every form field must
    // appear in the policy conditions" rule, so an SSE-S3 field that the policy
    // does not mention is accepted and encryption is applied. When the policy
    // does cover the field, a value mismatch is still rejected — see
    // test_anonymous_post_object_rejects_sse_s3_policy_mismatch.
    let mut env = RustFSTestEnvironment::new().await?;
    let master_key = local_sse_master_key_value();
    env.start_rustfs_server_with_env(vec![], &[(LOCAL_SSE_MASTER_KEY_ENV, master_key.as_str())])
        .await?;

    let bucket = "anon-post-sse-s3-missing";
    let object_key = "post-sse-s3-missing-object.txt";
    let expected_body = b"post-sse-s3-missing".to_vec();

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

/// Table-driven fold of the eleven accepted POST Object form-field tests
/// (backlog#1838 PR4). Every row keeps its original test's exact bucket, key,
/// form field, submitted value, policy condition, file MIME type, and file
/// body; the shared shape is: the policy covers the field (exact condition or
/// `starts-with` prefix), the form submits it, the upload returns 204 with an
/// empty body, and the stored object echoes the submitted value back.
#[tokio::test]
async fn test_anonymous_post_object_accepts_fields_covered_by_policy_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    // (case, bucket, object_key, field, submitted value, `starts-with` prefix
    // (`None` pins the field to an exact policy condition), file part MIME type,
    // file body, stored-value accessor)
    type Case = (
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        Option<&'static str>,
        &'static str,
        &'static [u8],
        fn(&HeadObjectOutput) -> Option<&str>,
    );
    let cases: &[Case] = &[
        (
            "storage-class",
            "anon-post-storage-class",
            "post-storage-class-object.txt",
            "x-amz-storage-class",
            "REDUCED_REDUNDANCY",
            None,
            "text/plain",
            b"post-storage-class-body",
            |head: &HeadObjectOutput| head.storage_class().map(|value| value.as_str()),
        ),
        (
            "metadata-starts-with",
            "anon-post-policy-meta-accept",
            "uploads/meta-object.txt",
            "x-amz-meta-project",
            "alpha-demo",
            Some("alpha-"),
            "text/plain",
            b"post-policy-meta-body",
            |head: &HeadObjectOutput| head.metadata().and_then(|meta| meta.get("project")).map(String::as_str),
        ),
        (
            "content-type",
            "anon-post-policy-content-type-accept",
            "uploads/content-type-accept.txt",
            "Content-Type",
            "text/plain",
            None,
            "text/plain",
            b"post-policy-content-type-accept",
            |head: &HeadObjectOutput| head.content_type(),
        ),
        (
            "content-type-starts-with",
            "anon-post-policy-content-type-accept",
            "uploads/content-type-object.txt",
            "Content-Type",
            "image/png",
            Some("image/"),
            "image/png",
            b"post-policy-content-type-body",
            |head: &HeadObjectOutput| head.content_type(),
        ),
        (
            "content-disposition",
            "anon-post-policy-disposition-accept",
            "uploads/disposition-object.txt",
            "Content-Disposition",
            "attachment; filename=\"upload.txt\"",
            None,
            "text/plain",
            b"post-policy-disposition-body",
            |head: &HeadObjectOutput| head.content_disposition(),
        ),
        (
            "cache-control",
            "anon-post-policy-cache-control-accept",
            "uploads/cache-control-object.txt",
            "Cache-Control",
            "max-age=60",
            None,
            "text/plain",
            b"post-policy-cache-control-body",
            |head: &HeadObjectOutput| head.cache_control(),
        ),
        (
            "content-language",
            "anon-post-policy-content-language-accept",
            "uploads/content-language-object.txt",
            "Content-Language",
            "en-US",
            None,
            "text/plain",
            b"post-policy-content-language-body",
            |head: &HeadObjectOutput| head.content_language(),
        ),
        (
            "content-encoding",
            "anon-post-policy-content-encoding-accept",
            "uploads/content-encoding-object.txt",
            "Content-Encoding",
            "gzip",
            None,
            "text/plain",
            b"post-policy-content-encoding-body",
            |head: &HeadObjectOutput| head.content_encoding(),
        ),
        (
            "website-redirect-location",
            "anon-post-policy-website-redirect-accept",
            "uploads/website-redirect-object.txt",
            "x-amz-website-redirect-location",
            "/docs/landing.html",
            None,
            "text/plain",
            b"post-policy-website-redirect-body",
            |head: &HeadObjectOutput| head.website_redirect_location(),
        ),
        (
            "expires",
            "anon-post-policy-expires-accept",
            "uploads/expires-object.txt",
            "Expires",
            "Wed, 21 Oct 2037 07:28:00 GMT",
            None,
            "text/plain",
            b"post-policy-expires-body",
            |head: &HeadObjectOutput| head.expires_string(),
        ),
        (
            "metadata-exact",
            "anon-post-policy-meta-exact-accept",
            "uploads/meta-exact-accept-object.txt",
            "x-amz-meta-project",
            "alpha-demo",
            None,
            "text/plain",
            b"post-policy-meta-exact-body",
            |head: &HeadObjectOutput| head.metadata().and_then(|meta| meta.get("project")).map(String::as_str),
        ),
    ];

    for (case, bucket, object_key, field, value, starts_with_prefix, file_mime, file_body, read_stored) in cases {
        let condition = match starts_with_prefix {
            Some(prefix) => serde_json::json!(["starts-with", format!("${field}"), prefix]),
            None => {
                let mut exact = serde_json::Map::new();
                exact.insert((*field).to_string(), serde_json::Value::String((*value).to_string()));
                serde_json::Value::Object(exact)
            }
        };

        run_post_object_accept_case(
            bucket,
            object_key,
            vec![
                serde_json::json!({ "bucket": bucket }),
                serde_json::json!({ "key": object_key }),
                condition,
                serde_json::json!(["content-length-range", 0, 1024]),
            ],
            (field, value),
            file_mime,
            file_body,
            *read_stored,
            case,
        )
        .await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_anonymous_post_object_rejects_object_lock_retention_without_permission()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-object-lock-retention";
    let object_key = "uploads/object-lock-retention.txt";
    let retain_until = "2037-10-21T07:28:00Z";
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
            reqwest::multipart::Part::bytes(b"post-policy-object-lock-retention-body".to_vec())
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
    assert!(response_body.contains("AccessDenied"));

    Ok(())
}

#[tokio::test]
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
async fn test_anonymous_post_object_rejects_object_lock_legal_hold_without_permission()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-post-policy-object-lock-legal-hold";
    let object_key = "uploads/object-lock-legal-hold.txt";
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
            reqwest::multipart::Part::bytes(b"post-policy-object-lock-legal-hold-body".to_vec())
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
    assert!(response_body.contains("AccessDenied"));

    Ok(())
}

#[tokio::test]
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
async fn test_signed_put_object_extract_preserves_sse_s3_and_redirect() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    let sse_master_key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_SSE_S3_MASTER_KEY", sse_master_key.as_str())])
        .await?;

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
        .storage_class(aws_sdk_s3::types::StorageClass::ReducedRedundancy)
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

    assert_eq!(head.storage_class().map(|value| value.as_str()), Some("REDUCED_REDUNDANCY"));

    Ok(())
}

#[tokio::test]
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
async fn test_signed_put_object_extract_uses_bucket_default_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    let sse_master_key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_SSE_S3_MASTER_KEY", sse_master_key.as_str())])
        .await?;

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
async fn test_signed_put_object_extract_pax_retention_overrides_request_retention()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-pax-retention-precedence";
    let archive_key = "retention.tar";
    let extracted_key = "alpha.txt";
    let request_retain_until = aws_sdk_s3::primitives::DateTime::from_secs(2_114_380_800);
    let pax_retain_until = "2040-01-01T00:00:00Z";

    let client = env.create_s3_client();
    client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;

    let pax = HashMap::from([
        ("minio.metadata.x-amz-object-lock-mode", "COMPLIANCE".to_string()),
        ("minio.metadata.x-amz-object-lock-retain-until-date", pax_retain_until.to_string()),
    ]);
    let archive = make_tar_with_pax_entry(extracted_key, b"alpha-body", None, &pax).await;

    client
        .put_object()
        .bucket(bucket)
        .key(archive_key)
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
        .object_lock_retain_until_date(request_retain_until)
        .body(ByteStream::from(archive))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await?;

    let retention = client
        .get_object_retention()
        .bucket(bucket)
        .key(extracted_key)
        .send()
        .await?
        .retention()
        .expect("retention should be present")
        .clone();
    assert_eq!(retention.mode().map(|value| value.as_str()), Some("COMPLIANCE"));
    assert_eq!(
        retention
            .retain_until_date()
            .expect("retain_until_date should be present")
            .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)?,
        pax_retain_until
    );

    Ok(())
}

#[tokio::test]
async fn test_signed_put_object_extract_returns_archive_etag() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-etag";
    let archive_key = "bundle.tar";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;

    let archive = make_tar(&[("alpha.txt", b"alpha-body")], &[]).await;
    let expected_etag = format!("\"{}\"", md5_hex(&archive));

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
async fn test_signed_put_object_extract_authorizes_each_pax_privilege_and_retention_conditions()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    if !crate::common::awscurl_available() {
        return Ok(());
    }

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "signed-extract-pax-auth";
    let put_only_user = "snowball-put-only";
    let put_only_secret = "snowball-put-only-secret";
    let conditional_user = "snowball-retention-condition";
    let conditional_secret = "snowball-retention-condition-secret";
    let wrong_action_user = "snowball-wrong-action";
    let wrong_action_secret = "snowball-wrong-action-secret";
    let version_condition_user = "snowball-version-condition";
    let version_condition_secret = "snowball-version-condition-secret";
    let pax_context_user = "snowball-pax-context";
    let pax_context_secret = "snowball-pax-context-secret";
    let conditional_version_id = Uuid::new_v4().to_string();
    let admin_client = env.create_s3_client();
    admin_client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    create_restricted_user(&env, put_only_user, put_only_secret).await?;
    create_restricted_user(&env, conditional_user, conditional_secret).await?;
    create_restricted_user(&env, wrong_action_user, wrong_action_secret).await?;
    create_restricted_user(&env, version_condition_user, version_condition_secret).await?;
    create_restricted_user(&env, pax_context_user, pax_context_secret).await?;

    let object_resource = format!("arn:aws:s3:::{bucket}/*");
    let context_archive_resources = [
        format!("arn:aws:s3:::{bucket}/tag-context.tar"),
        format!("arn:aws:s3:::{bucket}/lock-context.tar"),
    ];
    let tag_entry_resource = format!("arn:aws:s3:::{bucket}/tag-context-entry.txt");
    let lock_entry_resource = format!("arn:aws:s3:::{bucket}/lock-context-entry.txt");
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PutOnly",
                "Effect": "Allow",
                "Principal": { "AWS": [put_only_user] },
                "Action": ["s3:PutObject"],
                "Resource": [object_resource.clone()]
            },
            {
                "Sid": "RetentionWithLimit",
                "Effect": "Allow",
                "Principal": { "AWS": [conditional_user] },
                "Action": ["s3:PutObject", "s3:PutObjectRetention"],
                "Resource": [object_resource.clone()]
            },
            {
                "Sid": "DenyRetentionBeyondCutoff",
                "Effect": "Deny",
                "Principal": { "AWS": [conditional_user] },
                "Action": ["s3:PutObject"],
                "Resource": [object_resource.clone()],
                "Condition": {
                    "DateGreaterThan": {
                        "s3:object-lock-retain-until-date": "2030-01-01T00:00:00Z"
                    }
                }
            },
            {
                "Sid": "WrongAdditionalAction",
                "Effect": "Allow",
                "Principal": { "AWS": [wrong_action_user] },
                "Action": ["s3:PutObject", "s3:PutObjectLegalHold"],
                "Resource": [object_resource.clone()]
            },
            {
                "Sid": "VersionConditionPut",
                "Effect": "Allow",
                "Principal": { "AWS": [version_condition_user] },
                "Action": ["s3:PutObject"],
                "Resource": [object_resource.clone()]
            },
            {
                "Sid": "VersionConditionReplicate",
                "Effect": "Allow",
                "Principal": { "AWS": [version_condition_user] },
                "Action": ["s3:ReplicateObject"],
                "Resource": [object_resource],
                "Condition": {
                    "StringEquals": {
                        "s3:VersionId": conditional_version_id.clone()
                    }
                }
            },
            {
                "Sid": "PaxContextArchives",
                "Effect": "Allow",
                "Principal": { "AWS": [pax_context_user] },
                "Action": ["s3:PutObject", "s3:PutObjectRetention", "s3:PutObjectTagging"],
                "Resource": context_archive_resources
            },
            {
                "Sid": "PaxTagContextPut",
                "Effect": "Allow",
                "Principal": { "AWS": [pax_context_user] },
                "Action": ["s3:PutObject"],
                "Resource": [tag_entry_resource.clone()],
                "Condition": {
                    "StringEquals": {
                        "s3:RequestObjectTag/classification": "public"
                    }
                }
            },
            {
                "Sid": "PaxTagContextAction",
                "Effect": "Allow",
                "Principal": { "AWS": [pax_context_user] },
                "Action": ["s3:PutObjectTagging"],
                "Resource": [tag_entry_resource]
            },
            {
                "Sid": "PaxLockContextPut",
                "Effect": "Allow",
                "Principal": { "AWS": [pax_context_user] },
                "Action": ["s3:PutObject"],
                "Resource": [lock_entry_resource.clone()],
                "Condition": {
                    "StringEquals": {
                        "s3:object-lock-mode": "COMPLIANCE"
                    }
                }
            },
            {
                "Sid": "PaxLockContextAction",
                "Effect": "Allow",
                "Principal": { "AWS": [pax_context_user] },
                "Action": ["s3:PutObjectRetention"],
                "Resource": [lock_entry_resource]
            }
        ]
    })
    .to_string();
    admin_client.put_bucket_policy().bucket(bucket).policy(policy).send().await?;

    let put_only_client = restricted_user_client(&env, put_only_user, put_only_secret);
    let conditional_client = restricted_user_client(&env, conditional_user, conditional_secret);
    let wrong_action_client = restricted_user_client(&env, wrong_action_user, wrong_action_secret);
    let cases = [
        (
            "legal-hold.tar",
            put_only_client,
            HashMap::from([("minio.metadata.x-amz-object-lock-legal-hold", "ON".to_string())]),
        ),
        (
            "retention-condition.tar",
            conditional_client,
            HashMap::from([
                ("minio.metadata.x-amz-object-lock-mode", "COMPLIANCE".to_string()),
                ("minio.metadata.x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z".to_string()),
            ]),
        ),
        (
            "version-id.tar",
            wrong_action_client,
            HashMap::from([("minio.versionId", Uuid::new_v4().to_string())]),
        ),
    ];

    for (archive_key, client, pax) in cases {
        let archive = make_tar_with_pax_entry("entry.txt", b"must-not-write", None, &pax).await;
        let err = client
            .put_object()
            .bucket(bucket)
            .key(archive_key)
            .body(ByteStream::from(archive))
            .customize()
            .mutate_request(|req| {
                req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
            })
            .send()
            .await
            .expect_err("missing, conditional, or wrong PAX privilege must be rejected");
        assert_eq!(
            err.as_service_error().and_then(|error| error.meta().code()),
            Some("AccessDenied"),
            "{archive_key}"
        );
    }

    let version_condition_client = restricted_user_client(&env, version_condition_user, version_condition_secret);
    let mismatching_version_pax = HashMap::from([("minio.versionId", Uuid::new_v4().to_string())]);
    let archive = make_tar_with_pax_entry("version-mismatch-entry.txt", b"must-not-write", None, &mismatching_version_pax).await;
    let err = version_condition_client
        .put_object()
        .bucket(bucket)
        .key("version-mismatch.tar")
        .body(ByteStream::from(archive))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await
        .expect_err("a mismatching PAX version ID must fail the replication condition");
    assert_eq!(err.as_service_error().and_then(|error| error.meta().code()), Some("AccessDenied"));
    let err = admin_client
        .head_object()
        .bucket(bucket)
        .key("version-mismatch-entry.txt")
        .send()
        .await
        .expect_err("a denied PAX entry must not be written");
    assert!(matches!(
        err.as_service_error().and_then(|error| error.meta().code()),
        Some("NoSuchKey" | "NotFound")
    ));

    let matching_version_pax = HashMap::from([("minio.versionId", conditional_version_id)]);
    let archive = make_tar_with_pax_entry("condition-entry.txt", b"condition-body", None, &matching_version_pax).await;
    version_condition_client
        .put_object()
        .bucket(bucket)
        .key("version-condition.tar")
        .body(ByteStream::from(archive))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await?;
    let stored = admin_client
        .get_object()
        .bucket(bucket)
        .key("condition-entry.txt")
        .send()
        .await?;
    assert_eq!(stored.body.collect().await?.into_bytes().as_ref(), b"condition-body");

    let pax_context_client = restricted_user_client(&env, pax_context_user, pax_context_secret);
    let tag_pax = HashMap::from([("minio.metadata.x-amz-tagging", "classification=public".to_string())]);
    let archive = make_tar_with_pax_entry("tag-context-entry.txt", b"tag-context-body", None, &tag_pax).await;
    pax_context_client
        .put_object()
        .bucket(bucket)
        .key("tag-context.tar")
        .tagging("classification=restricted")
        .body(ByteStream::from(archive))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await?;
    let tags = admin_client
        .get_object_tagging()
        .bucket(bucket)
        .key("tag-context-entry.txt")
        .send()
        .await?;
    assert!(
        tags.tag_set()
            .iter()
            .any(|tag| tag.key() == "classification" && tag.value() == "public")
    );

    let pax_retain_until = "2040-01-01T00:00:00Z";
    let lock_pax = HashMap::from([
        ("minio.metadata.x-amz-object-lock-mode", "COMPLIANCE".to_string()),
        ("minio.metadata.x-amz-object-lock-retain-until-date", pax_retain_until.to_string()),
    ]);
    let archive = make_tar_with_pax_entry("lock-context-entry.txt", b"lock-context-body", None, &lock_pax).await;
    pax_context_client
        .put_object()
        .bucket(bucket)
        .key("lock-context.tar")
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
        .object_lock_retain_until_date(aws_sdk_s3::primitives::DateTime::from_secs(2_114_380_800))
        .body(ByteStream::from(archive))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
        })
        .send()
        .await?;
    let retention = admin_client
        .get_object_retention()
        .bucket(bucket)
        .key("lock-context-entry.txt")
        .send()
        .await?
        .retention()
        .expect("PAX retention should be present")
        .clone();
    assert_eq!(retention.mode().map(|mode| mode.as_str()), Some("COMPLIANCE"));
    assert_eq!(
        retention
            .retain_until_date()
            .expect("PAX retain-until should be present")
            .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)?,
        pax_retain_until
    );

    Ok(())
}

#[tokio::test]
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
