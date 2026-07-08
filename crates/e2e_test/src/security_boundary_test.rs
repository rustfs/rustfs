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

//! Security boundary tests for RustFS
//!
//! These tests verify that RustFS properly enforces security-sensitive
//! controls by issuing real requests against a running server and asserting
//! the concrete outcome of each control:
//! - DoS protection (oversized tagging payloads, excessive multipart parts)
//! - SSRF prevention (internal/private endpoints rejected for tiering)
//! - Race condition handling (concurrent writes converge without corruption)

use crate::common::{RustFSTestEnvironment, awscurl_available, awscurl_put, init_logging};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Tag, Tagging};
use serial_test::serial;
use std::error::Error;
use tracing::info;

/// Oversized tagging payloads must be rejected by the per-object tag limit.
///
/// RustFS caps object tagging at 10 tags (`InvalidTag`). This protects the
/// server against unbounded tagging XML bodies. We transmit a payload that is
/// far beyond that limit and assert the server rejects it with the specific
/// error, rather than accepting an arbitrarily large control-plane body.
#[tokio::test]
#[serial]
async fn test_large_xml_body_rejection() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();

    let bucket_name = format!("security-large-xml-{}", uuid::Uuid::new_v4());
    client.create_bucket().bucket(&bucket_name).send().await?;

    let object_key = "oversized-tagging-target";
    client
        .put_object()
        .bucket(&bucket_name)
        .key(object_key)
        .body(ByteStream::from_static(b"payload"))
        .send()
        .await?;

    // Build a tag set that is well past the enforced 10-tag limit. This is a
    // genuinely oversized tagging XML body that must be rejected, not a single
    // in-limit tag.
    let mut tag_builder = Tagging::builder();
    for i in 0..500 {
        tag_builder = tag_builder.tag_set(Tag::builder().key(format!("key-{i}")).value(format!("value-{i}")).build()?);
    }
    let oversized_tagging = tag_builder.build()?;

    let result = client
        .put_object_tagging()
        .bucket(&bucket_name)
        .key(object_key)
        .tagging(oversized_tagging)
        .send()
        .await;

    // Cleanup (best effort) before assertions so a failed assertion still
    // leaves no residue.
    let _ = client.delete_object().bucket(&bucket_name).key(object_key).send().await;
    let _ = client.delete_bucket().bucket(&bucket_name).send().await;

    assert!(result.is_err(), "Server must reject an oversized tagging payload, but it was accepted");
    let err = result.err().expect("checked is_err above");
    let code = err.as_service_error().and_then(|e| e.code());
    assert_eq!(
        code,
        Some("InvalidTag"),
        "Oversized tagging should be rejected with InvalidTag, got code {code:?}, err: {err:?}"
    );

    env.stop_server();
    Ok(())
}

/// Excessive multipart parts must be rejected.
#[tokio::test]
#[serial]
async fn test_excessive_multipart_parts() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();

    let bucket_name = format!("security-multipart-{}", uuid::Uuid::new_v4());
    client.create_bucket().bucket(&bucket_name).send().await?;

    let create_result = client
        .create_multipart_upload()
        .bucket(&bucket_name)
        .key("test-large")
        .send()
        .await?;

    let upload_id = create_result.upload_id().expect("upload_id should be present").to_string();

    // Try to complete with too many parts (should be rejected).
    let mut parts = Vec::new();
    for i in 1..=10001 {
        parts.push(CompletedPart::builder().part_number(i).e_tag(format!("etag-{i}")).build());
    }

    let result = client
        .complete_multipart_upload()
        .bucket(&bucket_name)
        .key("test-large")
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
        .send()
        .await;

    // Cleanup.
    let _ = client
        .abort_multipart_upload()
        .bucket(&bucket_name)
        .key("test-large")
        .upload_id(&upload_id)
        .send()
        .await;
    let _ = client.delete_bucket().bucket(&bucket_name).send().await;

    assert!(result.is_err(), "Server should reject excessive multipart parts");

    env.stop_server();
    Ok(())
}

/// Concurrent writes to the same object must converge without corruption.
///
/// We fan out concurrent PUTs of distinct contents and assert every write is
/// accepted, then assert the final object is exactly one of the written values
/// (last-writer-wins, no torn/garbage state) and that it is absent after a
/// subsequent delete.
#[tokio::test]
#[serial]
async fn test_concurrent_object_operations() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    let client = env.create_s3_client();

    let bucket_name = format!("security-concurrent-{}", uuid::Uuid::new_v4());
    client.create_bucket().bucket(&bucket_name).send().await?;

    let key = "concurrent-test";
    let writer_count = 10;
    let expected_contents: Vec<String> = (0..writer_count).map(|i| format!("content-{i}")).collect();

    // Fan out concurrent PUTs of distinct contents.
    let mut handles = Vec::new();
    for content in expected_contents.iter().cloned() {
        let client_clone = client.clone();
        let bucket_clone = bucket_name.clone();
        handles.push(tokio::spawn(async move {
            client_clone
                .put_object()
                .bucket(&bucket_clone)
                .key(key)
                .body(ByteStream::from(content.into_bytes()))
                .send()
                .await
                .is_ok()
        }));
    }

    // Every concurrent write must be accepted by the server.
    for handle in handles {
        let put_ok = handle.await?;
        assert!(
            put_ok,
            "A concurrent PUT was rejected; server must accept concurrent writes to the same key"
        );
    }

    // The final object must be exactly one of the written contents (no torn or
    // corrupted state from interleaved writes).
    let get = client.get_object().bucket(&bucket_name).key(key).send().await?;
    let body = get.body.collect().await?.into_bytes();
    let final_content = String::from_utf8(body.to_vec())?;
    assert!(
        expected_contents.contains(&final_content),
        "Final object content {final_content:?} is not one of the concurrently written values"
    );

    // After deletion, the object must be absent.
    client.delete_object().bucket(&bucket_name).key(key).send().await?;
    let get_after_delete = client.get_object().bucket(&bucket_name).key(key).send().await;
    assert!(get_after_delete.is_err(), "Object must be absent after delete");
    let code = get_after_delete
        .err()
        .and_then(|e| e.as_service_error().and_then(|se| se.code()).map(str::to_string));
    assert_eq!(
        code.as_deref(),
        Some("NoSuchKey"),
        "GET after delete should return NoSuchKey, got {code:?}"
    );

    let _ = client.delete_bucket().bucket(&bucket_name).send().await;

    env.stop_server();
    Ok(())
}

/// Internal/private endpoints must be rejected as remote tier backends (SSRF).
///
/// This issues a real admin AddTier call (`PUT /rustfs/admin/v3/tier`) for each
/// internal/private endpoint and asserts the server rejects it (non-2xx, so the
/// signed request helper returns an error). An internal endpoint must never be
/// accepted as a tier backend. The rejection may originate from explicit
/// SSRF/internal-address filtering or from the backend connectivity/credential
/// validation performed during AddTier; either way the security-relevant
/// outcome — the internal endpoint is not accepted — is asserted here.
///
/// The admin API is exercised via signed `awscurl` requests, matching the
/// pattern used by the other admin-API E2E tests in this crate; the test is
/// skipped when `awscurl` is not installed.
#[tokio::test]
#[serial]
async fn test_tiering_url_validation() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();
    if !awscurl_available() {
        info!("Skipping tiering URL validation test because awscurl is not available");
        return Ok(());
    }

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let tier_url = format!("{}/rustfs/admin/v3/tier", env.url);
    let internal_endpoints = [
        "http://127.0.0.1:8080",
        "http://localhost:8080",
        "http://169.254.169.254", // cloud instance metadata endpoint
        "http://[::1]:8080",
    ];

    for endpoint in internal_endpoints {
        // AddTier expects an uppercase tier name and a backend configuration.
        let body = serde_json::json!({
            "type": "s3",
            "s3": {
                "name": "SSRFTEST",
                "endpoint": endpoint,
                "accessKey": "ssrf-probe",
                "secretKey": "ssrf-probe",
                "bucket": "ssrf-probe-bucket",
                "prefix": "",
                "region": "us-east-1",
                "storageClass": ""
            }
        })
        .to_string();

        let result = awscurl_put(&tier_url, &body, &env.access_key, &env.secret_key).await;
        assert!(
            result.is_err(),
            "AddTier must reject internal endpoint {endpoint}, but it was accepted: {result:?}"
        );
    }

    env.stop_server();
    Ok(())
}
