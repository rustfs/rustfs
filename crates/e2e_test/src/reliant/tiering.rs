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

//! Hermetic ILM transition main-path end-to-end test (backlog#1148 ilm-7).
//!
//! Two embedded `rustfs` servers, each with independent credentials, port and
//! data directory:
//!
//! * `cold` — a second RustFS server wired as a [`TierType::RustFS`] remote tier.
//! * `hot` — the source server that transitions objects to `cold`.
//!
//! There are no containers, no external S3 backend and no `awscurl`: the
//! `AddTier` admin call is signed in-process with `rustfs_signer`, exactly like
//! the other admin-API e2e suites in this crate. The RustFS warm backend has no
//! loopback/SSRF restriction (that guard is replication-only), so `hot` can tier
//! to `cold` over `http://127.0.0.1:<port>`.
//!
//! A single test drives the full transition main path and pins the chain
//! required by ilm-7:
//!   1. `AddTier(RustFS)` on `hot` targeting `cold` — the real connectivity /
//!      in-use probe runs (no `force`), so this also proves the tier is reachable.
//!   2. A `Transition Days=0` rule installed before a multipart PUT transitions
//!      the object immediately (the completion path enqueues it; the 1s scanner
//!      cycle is only a backstop).
//!   3. `HEAD` reports `x-amz-storage-class == <tier name>` and no `x-amz-restore`.
//!   4. `GET` streams byte-identical data back through the warm backend, and the
//!      content-type and user metadata survive the round trip (rustfs#2246).
//!   5. Range `GET` within a part and across the part boundary read the correct
//!      bytes from the tier (backlog#807).
//!   6. The remote object is present in the cold-tier bucket after transition.
//!   7. `DeleteObject` on `hot` drives free-version cleanup: the cold-tier copy
//!      eventually disappears and the hot object is gone (no local residue).

use crate::common::{RustFSTestEnvironment, local_http_client};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, CompletedMultipartUpload, CompletedPart, ExpirationStatus, LifecycleRule, LifecycleRuleFilter,
    Transition, TransitionStorageClass,
};
use http::Method;
use http::header::HOST;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use std::time::{Duration as StdDuration, Instant};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const TIER_NAME: &str = "COLDTIER";
const TIER_BUCKET: &str = "ilm7-cold-tier";
const TIER_PREFIX: &str = "tiered";
const SOURCE_BUCKET: &str = "ilm7-hot";
const OBJECT_KEY: &str = "tier/report.bin";
const CONTENT_TYPE: &str = "application/x-ilm7";
const USER_META_KEY: &str = "ilm7-origin";
const USER_META_VAL: &str = "hermetic-transition";

/// 5 MiB — the S3 minimum size for a non-final multipart part; the object's only
/// internal part boundary sits at this offset.
const PART0_SIZE: usize = 5 * 1024 * 1024;
/// 1 MiB tail so the completed object is genuinely multipart (two parts).
const PART1_SIZE: usize = 1024 * 1024;
const OBJECT_SIZE: usize = PART0_SIZE + PART1_SIZE;

/// Deterministic, position-dependent payload: adjacent offsets differ, so a
/// misaligned range read is caught.
fn payload() -> Vec<u8> {
    (0..OBJECT_SIZE).map(|i| (i % 251) as u8).collect()
}

/// Sign and send an admin request in-process (no `awscurl`).
///
/// Mirrors the shared admin-API e2e pattern: the SigV4 signature is computed
/// over `UNSIGNED_PAYLOAD`, so the JSON body rides on the wire without being
/// pre-hashed. Returns the response status and body text.
async fn signed_admin_request(
    base_url: &str,
    method: Method,
    path: &str,
    body: Option<&str>,
    access_key: &str,
    secret_key: &str,
) -> Result<(reqwest::StatusCode, String), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{base_url}{path}");
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let body_bytes = body.map(|b| b.as_bytes().to_vec()).unwrap_or_default();

    let request = http::Request::builder()
        .method(method.clone())
        .uri(uri)
        .header(HOST, authority)
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    let signed = sign_v4(request.body(Body::empty())?, 0, access_key, secret_key, "", "us-east-1");

    let client = local_http_client();
    let mut request_builder = client.request(method, url.as_str());
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    if !body_bytes.is_empty() {
        request_builder = request_builder.body(body_bytes);
    }
    let response = request_builder.send().await?;
    let status = response.status();
    let text = response.text().await?;
    Ok((status, text))
}

/// Wire `hot` -> `cold` as a `TierType::RustFS` remote tier via `AddTier`.
///
/// No `force`, so the server runs the real in-use / connectivity probe against
/// `cold` (which requires the tier bucket to already exist there).
async fn add_rustfs_tier(hot: &RustFSTestEnvironment, cold: &RustFSTestEnvironment) -> TestResult {
    let body = serde_json::json!({
        "type": "rustfs",
        "rustfs": {
            "name": TIER_NAME,
            "endpoint": cold.url.as_str(),
            "accessKey": cold.access_key.as_str(),
            "secretKey": cold.secret_key.as_str(),
            "bucket": TIER_BUCKET,
            "prefix": TIER_PREFIX,
            "region": "us-east-1",
            "storageClass": ""
        }
    })
    .to_string();

    let (status, resp) = signed_admin_request(
        &hot.url,
        Method::PUT,
        "/rustfs/admin/v3/tier",
        Some(&body),
        &hot.access_key,
        &hot.secret_key,
    )
    .await?;
    if !status.is_success() {
        return Err(format!("AddTier(RustFS) failed: status={status}, body={resp}").into());
    }
    Ok(())
}

/// A current-version `Transition Days=0` rule scoped to the object's prefix.
fn transition_rule() -> Result<LifecycleRule, Box<dyn std::error::Error + Send + Sync>> {
    Ok(LifecycleRule::builder()
        .id("ilm7-transition")
        .filter(LifecycleRuleFilter::builder().prefix("tier/").build())
        .transitions(
            Transition::builder()
                .days(0)
                .storage_class(TransitionStorageClass::from(TIER_NAME))
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?)
}

/// Upload `data` as a two-part multipart object with a content-type and one
/// user-metadata entry.
async fn put_multipart_object(client: &Client, bucket: &str, key: &str, data: &[u8]) -> TestResult {
    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .content_type(CONTENT_TYPE)
        .metadata(USER_META_KEY, USER_META_VAL)
        .send()
        .await?;
    let upload_id = create
        .upload_id()
        .ok_or("CreateMultipartUpload returned no upload id")?
        .to_string();

    let mut completed = Vec::new();
    for (idx, chunk) in [&data[..PART0_SIZE], &data[PART0_SIZE..]].into_iter().enumerate() {
        let part_number = (idx + 1) as i32;
        let uploaded = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(chunk.to_vec()))
            .send()
            .await?;
        completed.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().unwrap_or_default())
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
        .send()
        .await?;
    Ok(())
}

/// Number of objects currently stored in the cold-tier bucket.
async fn cold_tier_object_count(cold_client: &Client) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let resp = cold_client.list_objects_v2().bucket(TIER_BUCKET).send().await?;
    Ok(resp.contents().len())
}

/// Poll `HEAD` until the object's storage class is the tier name (transition
/// complete), or fail after `deadline`.
async fn wait_for_transition(client: &Client, bucket: &str, key: &str, deadline: StdDuration) -> TestResult {
    let start = Instant::now();
    loop {
        let head = client.head_object().bucket(bucket).key(key).send().await?;
        if head.storage_class().map(|sc| sc.as_str()) == Some(TIER_NAME) {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "object {bucket}/{key} was not transitioned to {TIER_NAME} within {}s (storage_class={:?})",
                deadline.as_secs(),
                head.storage_class()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(500)).await;
    }
}

/// Poll until the cold-tier bucket is empty (remote free-version cleanup done),
/// or fail after `deadline`.
async fn wait_for_cold_tier_empty(cold_client: &Client, deadline: StdDuration) -> TestResult {
    let start = Instant::now();
    loop {
        let count = cold_tier_object_count(cold_client).await?;
        if count == 0 {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "cold-tier bucket still holds {count} object(s) {}s after DeleteObject; \
                 free-version remote cleanup did not converge",
                deadline.as_secs()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(500)).await;
    }
}

/// GET `bytes=start-end` (inclusive) and assert it equals `data[start..=end]`.
async fn assert_range(client: &Client, start: usize, end: usize, data: &[u8]) -> TestResult {
    let range = format!("bytes={start}-{end}");
    let resp = client
        .get_object()
        .bucket(SOURCE_BUCKET)
        .key(OBJECT_KEY)
        .range(&range)
        .send()
        .await?;
    let got = resp.body.collect().await?.into_bytes();
    let expected = &data[start..=end];
    assert_eq!(got.len(), expected.len(), "range {range}: length mismatch");
    assert_eq!(got.as_ref(), expected, "range {range}: bytes mismatch reading from the tier");
    Ok(())
}

/// Full ilm-7 hermetic transition main path across two embedded RustFS servers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_hermetic_transition_main_path() -> TestResult {
    // Cold-tier server (independent credentials). It is a passive tier target,
    // so it needs no lifecycle/scanner configuration.
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "coldtieradmin".to_string();
    cold.secret_key = "coldtiersecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    // Hot/source server. A 1s scanner cycle is a backstop; transition is
    // primarily driven immediately by the multipart completion path.
    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_CYCLE", "1")])
        .await?;
    let hot_client = hot.create_s3_client();

    // Wire the RustFS remote tier (real connectivity probe, no force).
    add_rustfs_tier(&hot, &cold).await?;

    // Source bucket + Days=0 transition rule installed BEFORE the write so the
    // completion path enqueues the transition immediately.
    hot_client.create_bucket().bucket(SOURCE_BUCKET).send().await?;
    let lifecycle = BucketLifecycleConfiguration::builder().rules(transition_rule()?).build()?;
    hot_client
        .put_bucket_lifecycle_configuration()
        .bucket(SOURCE_BUCKET)
        .lifecycle_configuration(lifecycle)
        .send()
        .await?;

    let data = payload();
    put_multipart_object(&hot_client, SOURCE_BUCKET, OBJECT_KEY, &data).await?;

    // 1) Transition completes: HEAD reports the tier name and no restore state.
    wait_for_transition(&hot_client, SOURCE_BUCKET, OBJECT_KEY, StdDuration::from_secs(90)).await?;
    let head = hot_client.head_object().bucket(SOURCE_BUCKET).key(OBJECT_KEY).send().await?;
    assert_eq!(
        head.storage_class().map(|sc| sc.as_str()),
        Some(TIER_NAME),
        "transitioned object must report the tier name as its storage class"
    );
    assert!(
        head.restore().is_none(),
        "a freshly transitioned object must not advertise x-amz-restore, got {:?}",
        head.restore()
    );

    // 2) The remote object now lives in the cold-tier bucket.
    assert!(
        cold_tier_object_count(&cold_client).await? >= 1,
        "cold-tier bucket must hold the transitioned object"
    );

    // 3) GET streams identical bytes back through the warm backend; content-type
    //    and user metadata survive the transition round trip (rustfs#2246).
    let get = hot_client.get_object().bucket(SOURCE_BUCKET).key(OBJECT_KEY).send().await?;
    assert_eq!(get.content_type(), Some(CONTENT_TYPE), "content-type must survive transition");
    assert_eq!(
        get.metadata().and_then(|m| m.get(USER_META_KEY)).map(String::as_str),
        Some(USER_META_VAL),
        "user metadata must survive transition"
    );
    let body = get.body.collect().await?.into_bytes();
    assert_eq!(body.len(), data.len(), "full transitioned GET length mismatch");
    assert_eq!(body.as_ref(), data.as_slice(), "full transitioned GET must be byte-identical");

    // 4) Range GET within a single part and across the part boundary
    //    (backlog#807): both must read the correct bytes from the tier.
    assert_range(&hot_client, 1000, 1099, &data).await?;
    assert_range(&hot_client, PART0_SIZE - 5, PART0_SIZE + 4, &data).await?;

    // 5) DeleteObject drives free-version cleanup. The local object is gone
    //    immediately; the remote copy is removed asynchronously.
    hot_client
        .delete_object()
        .bucket(SOURCE_BUCKET)
        .key(OBJECT_KEY)
        .send()
        .await?;

    let get_after = hot_client.get_object().bucket(SOURCE_BUCKET).key(OBJECT_KEY).send().await;
    let err = get_after.expect_err("hot object must be gone immediately after delete");
    assert_eq!(
        err.as_service_error().and_then(|e| e.code()),
        Some("NoSuchKey"),
        "hot GET after delete must be NoSuchKey, got {err:?}"
    );

    wait_for_cold_tier_empty(&cold_client, StdDuration::from_secs(90)).await?;

    Ok(())
}
