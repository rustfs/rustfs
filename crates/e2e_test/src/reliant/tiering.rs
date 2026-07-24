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
    BucketLifecycleConfiguration, BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, ExpirationStatus,
    LifecycleRule, LifecycleRuleFilter, NoncurrentVersionTransition, Transition, TransitionStorageClass, VersioningConfiguration,
};
use http::Method;
use http::header::HOST;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serde::Deserialize;
use std::time::{Duration as StdDuration, Instant};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const TIER_NAME: &str = "COLDTIER";
const TIER_BUCKET: &str = "ilm7-cold-tier";
const TIER_PREFIX: &str = "tiered";
const SOURCE_BUCKET: &str = "ilm7-hot";
const MANUAL_DUE_BUCKET: &str = "ilm7-manual-due";
const MANUAL_DRY_RUN_BUCKET: &str = "ilm7-manual-dry-run";
const MANUAL_NOT_DUE_BUCKET: &str = "ilm7-manual-not-due";
const MANUAL_QUEUE_PRESSURE_BUCKET: &str = "ilm7-manual-queue-pressure";
const MANUAL_QUEUE_PRESSURE_PREFIX: &str = "manual-queue-pressure/";
const OBJECT_KEY: &str = "tier/鲁A12345/report.bin";
const MANUAL_DUE_KEY: &str = "manual-due/report.bin";
const MANUAL_DRY_RUN_KEY: &str = "manual-dry-run/report.bin";
const MANUAL_NOT_DUE_KEY: &str = "manual-not-due/report.bin";
const CONTENT_TYPE: &str = "application/x-ilm7";
const USER_META_KEY: &str = "ilm7-origin";
const USER_META_VAL: &str = "hermetic-transition";
const HDR_SOURCE_REPLICATION_REQUEST: &str = "x-rustfs-source-replication-request";
const HDR_SOURCE_MTIME: &str = "x-rustfs-source-mtime";

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
    transition_rule_for("ilm7-transition", "tier/", 0)
}

fn transition_rule_for(id: &str, prefix: &str, days: i32) -> Result<LifecycleRule, Box<dyn std::error::Error + Send + Sync>> {
    Ok(LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .transitions(
            Transition::builder()
                .days(days)
                .storage_class(TransitionStorageClass::from(TIER_NAME))
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?)
}

async fn put_lifecycle_transition_rule(client: &Client, bucket: &str, id: &str, prefix: &str, days: i32) -> TestResult {
    let lifecycle = BucketLifecycleConfiguration::builder()
        .rules(transition_rule_for(id, prefix, days)?)
        .build()?;
    client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(lifecycle)
        .send()
        .await?;
    Ok(())
}

async fn put_lifecycle_noncurrent_transition_rule(client: &Client, bucket: &str, id: &str, prefix: &str) -> TestResult {
    let rule = LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .noncurrent_version_transitions(
            NoncurrentVersionTransition::builder()
                .noncurrent_days(0)
                .storage_class(TransitionStorageClass::from(TIER_NAME))
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?;
    let lifecycle = BucketLifecycleConfiguration::builder().rules(rule).build()?;
    client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(lifecycle)
        .send()
        .await?;
    Ok(())
}

async fn enable_bucket_versioning(client: &Client, bucket: &str) -> TestResult {
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

async fn put_single_part_object(client: &Client, bucket: &str, key: &str, body: &[u8]) -> TestResult {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await?;
    Ok(())
}

async fn put_backdated_single_part_object(
    client: &Client,
    bucket: &str,
    key: &str,
    body: &'static [u8],
    mtime: OffsetDateTime,
) -> TestResult {
    let mtime_rfc3339 = mtime.format(&Rfc3339)?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert(HDR_SOURCE_REPLICATION_REQUEST, "true");
            req.headers_mut().insert(HDR_SOURCE_MTIME, mtime_rfc3339.clone());
        })
        .send()
        .await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct ManualTransitionRunResponse {
    state: String,
    mode: String,
    job_id: Option<String>,
    status_endpoint: Option<String>,
    report: ManualTransitionRunReport,
}

#[derive(Debug, Deserialize)]
struct ManualTransitionRunReport {
    bucket: String,
    prefix: String,
    tier: Option<String>,
    dry_run: bool,
    lifecycle_config_found: bool,
    scanned: u64,
    eligible: u64,
    enqueued: u64,
    dry_run_eligible: u64,
    skipped_not_transition: u64,
    skipped_tier: u64,
    skipped_delete_marker: u64,
    skipped_directory: u64,
    skipped_replication: u64,
    skipped_already_in_flight: u64,
    skipped_queue_full: u64,
    skipped_queue_closed: u64,
    skipped_queue_timeout: u64,
    truncated_by_limit: bool,
    truncated_by_duration: bool,
}

async fn manual_transition_run(
    hot: &RustFSTestEnvironment,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
) -> Result<ManualTransitionRunResponse, Box<dyn std::error::Error + Send + Sync>> {
    manual_transition_run_with_max_objects(hot, bucket, prefix, dry_run, 10).await
}

async fn manual_transition_run_with_max_objects(
    hot: &RustFSTestEnvironment,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
    max_objects: u64,
) -> Result<ManualTransitionRunResponse, Box<dyn std::error::Error + Send + Sync>> {
    let bucket = urlencoding::encode(bucket);
    let prefix = urlencoding::encode(prefix);
    let tier = urlencoding::encode(TIER_NAME);
    let path = format!(
        "/rustfs/admin/v3/ilm/transition/run?bucket={bucket}&prefix={prefix}&tier={tier}&dryRun={dry_run}&maxObjects={max_objects}"
    );
    let (status, body) = signed_admin_request(&hot.url, Method::POST, &path, None, &hot.access_key, &hot.secret_key).await?;
    if !status.is_success() {
        return Err(format!("manual transition run failed: status={status}, body={body}").into());
    }
    Ok(serde_json::from_str(&body)?)
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

async fn assert_not_transitioned(client: &Client, bucket: &str, key: &str) -> TestResult {
    let head = client.head_object().bucket(bucket).key(key).send().await?;
    assert!(
        head.storage_class().is_none(),
        "{bucket}/{key} must remain in the hot tier, got storage_class={:?}",
        head.storage_class()
    );
    Ok(())
}

async fn assert_remains_not_transitioned(client: &Client, bucket: &str, key: &str, duration: StdDuration) -> TestResult {
    let deadline = Instant::now() + duration;
    loop {
        assert_not_transitioned(client, bucket, key).await?;
        if Instant::now() >= deadline {
            return Ok(());
        }
        tokio::time::sleep(StdDuration::from_millis(250)).await;
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_run_black_box_semantics() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualcoldtieradmin".to_string();
    cold.secret_key = "manualcoldtiersecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;
    let due_mtime = OffsetDateTime::now_utc() - time::Duration::hours(25);

    hot_client.create_bucket().bucket(MANUAL_DUE_BUCKET).send().await?;
    put_lifecycle_transition_rule(&hot_client, MANUAL_DUE_BUCKET, "manual-due", "manual-due/", 0).await?;
    put_backdated_single_part_object(&hot_client, MANUAL_DUE_BUCKET, MANUAL_DUE_KEY, b"manual due object", due_mtime).await?;

    let due = manual_transition_run(&hot, MANUAL_DUE_BUCKET, "manual-due/", false).await?;
    assert_eq!(due.mode, "enqueue_only");
    assert!(due.job_id.is_none());
    assert!(due.status_endpoint.is_none());
    assert_eq!(due.state, "completed");
    assert_eq!(due.report.bucket, MANUAL_DUE_BUCKET);
    assert_eq!(due.report.prefix, "manual-due/");
    assert_eq!(due.report.tier.as_deref(), Some(TIER_NAME));
    assert!(!due.report.dry_run);
    assert!(due.report.lifecycle_config_found);
    assert_eq!(due.report.scanned, 1, "due report: {:#?}", due.report);
    assert_eq!(due.report.eligible, 1, "due report: {:#?}", due.report);
    assert_eq!(
        due.report.enqueued + due.report.skipped_already_in_flight,
        1,
        "due report: {:#?}",
        due.report
    );
    assert_eq!(due.report.skipped_tier, 0);
    assert_eq!(due.report.skipped_delete_marker, 0);
    assert_eq!(due.report.skipped_directory, 0);
    assert_eq!(due.report.skipped_replication, 0);
    assert!(!due.report.truncated_by_limit);
    assert!(!due.report.truncated_by_duration);
    wait_for_transition(&hot_client, MANUAL_DUE_BUCKET, MANUAL_DUE_KEY, StdDuration::from_secs(90)).await?;
    let remote_count_after_due = cold_tier_object_count(&cold_client).await?;

    hot_client.create_bucket().bucket(MANUAL_DRY_RUN_BUCKET).send().await?;
    enable_bucket_versioning(&hot_client, MANUAL_DRY_RUN_BUCKET).await?;
    put_lifecycle_noncurrent_transition_rule(&hot_client, MANUAL_DRY_RUN_BUCKET, "manual-dry-run", "manual-dry-run/").await?;
    put_single_part_object(&hot_client, MANUAL_DRY_RUN_BUCKET, MANUAL_DRY_RUN_KEY, b"manual dry-run object v1").await?;
    put_single_part_object(&hot_client, MANUAL_DRY_RUN_BUCKET, MANUAL_DRY_RUN_KEY, b"manual dry-run object v2").await?;

    let before_dry_run_remote_count = cold_tier_object_count(&cold_client).await?;
    assert_eq!(
        before_dry_run_remote_count, remote_count_after_due,
        "dry-run setup must not enqueue transition work before the manual dry-run"
    );
    let dry = manual_transition_run(&hot, MANUAL_DRY_RUN_BUCKET, "manual-dry-run/", true).await?;
    assert_eq!(dry.state, "completed");
    assert_eq!(dry.report.bucket, MANUAL_DRY_RUN_BUCKET);
    assert_eq!(dry.report.prefix, "manual-dry-run/");
    assert_eq!(dry.report.tier.as_deref(), Some(TIER_NAME));
    assert!(dry.report.dry_run);
    assert_eq!(dry.report.scanned, 2, "dry-run report: {:#?}", dry.report);
    assert_eq!(dry.report.eligible, 1, "dry-run report: {:#?}", dry.report);
    assert_eq!(dry.report.dry_run_eligible, 1, "dry-run report: {:#?}", dry.report);
    assert_eq!(dry.report.enqueued, 0, "dry-run report: {:#?}", dry.report);
    assert_eq!(dry.report.skipped_not_transition, 1, "dry-run report: {:#?}", dry.report);
    assert!(!dry.report.truncated_by_duration);
    assert_eq!(
        cold_tier_object_count(&cold_client).await?,
        before_dry_run_remote_count,
        "dry-run must not create a remote tier object"
    );
    assert_not_transitioned(&hot_client, MANUAL_DRY_RUN_BUCKET, MANUAL_DRY_RUN_KEY).await?;

    hot_client.create_bucket().bucket(MANUAL_NOT_DUE_BUCKET).send().await?;
    put_lifecycle_transition_rule(&hot_client, MANUAL_NOT_DUE_BUCKET, "manual-not-due", "manual-not-due/", 1).await?;
    put_single_part_object(&hot_client, MANUAL_NOT_DUE_BUCKET, MANUAL_NOT_DUE_KEY, b"manual not-yet-due object").await?;

    let not_due = manual_transition_run(&hot, MANUAL_NOT_DUE_BUCKET, "manual-not-due/", false).await?;
    assert_eq!(not_due.state, "completed");
    assert_eq!(not_due.report.bucket, MANUAL_NOT_DUE_BUCKET);
    assert_eq!(not_due.report.prefix, "manual-not-due/");
    assert_eq!(not_due.report.tier.as_deref(), Some(TIER_NAME));
    assert!(!not_due.report.dry_run);
    assert_eq!(not_due.report.scanned, 1, "not-due report: {:#?}", not_due.report);
    assert_eq!(not_due.report.eligible, 0, "not-due report: {:#?}", not_due.report);
    assert_eq!(not_due.report.enqueued, 0, "not-due report: {:#?}", not_due.report);
    assert_eq!(not_due.report.skipped_not_transition, 1, "not-due report: {:#?}", not_due.report);
    assert_eq!(not_due.report.skipped_queue_full, 0);
    assert_eq!(not_due.report.skipped_queue_closed, 0);
    assert_eq!(not_due.report.skipped_queue_timeout, 0);
    assert!(!not_due.report.truncated_by_duration);
    assert_remains_not_transitioned(&hot_client, MANUAL_NOT_DUE_BUCKET, MANUAL_NOT_DUE_KEY, StdDuration::from_secs(2)).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_run_queue_pressure_partial() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualpressurecoldtieradmin".to_string();
    cold.secret_key = "manualpressurecoldtiersecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_SCANNER_ENABLED", "false"),
            ("RUSTFS_SCANNER_CYCLE", "3600"),
            ("RUSTFS_MAX_TRANSITION_WORKERS", "1"),
            ("RUSTFS_TRANSITION_QUEUE_CAPACITY", "1"),
        ],
    )
    .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    hot_client.create_bucket().bucket(MANUAL_QUEUE_PRESSURE_BUCKET).send().await?;
    for idx in 0..20 {
        let key = format!("{MANUAL_QUEUE_PRESSURE_PREFIX}obj-{idx:02}");
        let body = vec![0x5a; 1024 * 1024];
        put_single_part_object(&hot_client, MANUAL_QUEUE_PRESSURE_BUCKET, &key, &body).await?;
    }

    put_lifecycle_transition_rule(
        &hot_client,
        MANUAL_QUEUE_PRESSURE_BUCKET,
        "manual-queue-pressure",
        MANUAL_QUEUE_PRESSURE_PREFIX,
        0,
    )
    .await?;

    let result =
        manual_transition_run_with_max_objects(&hot, MANUAL_QUEUE_PRESSURE_BUCKET, MANUAL_QUEUE_PRESSURE_PREFIX, false, 20)
            .await?;
    assert_eq!(result.state, "partial");
    assert!(
        result.report.skipped_queue_full > 0,
        "queue-pressure partial should record queue full skips: {:#?}",
        result.report
    );
    assert!(
        !result.report.truncated_by_duration,
        "queue-pressure partial should be pressure-driven: {:#?}",
        result.report
    );

    let remaining = cold_tier_object_count(&cold_client).await?;
    assert!(
        remaining < 20,
        "queue-pressure partial should skip at least one enqueue, remote count should be <20, got {remaining}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_run_async_not_implemented() -> TestResult {
    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;

    let mode_async =
        manual_transition_run_with_query(&hot, "/rustfs/admin/v3/ilm/transition/run?bucket=manual-async-test&mode=async").await?;
    let (mode_async_status, mode_async_body) = mode_async;
    assert_eq!(
        mode_async_status,
        reqwest::StatusCode::NOT_IMPLEMENTED,
        "mode=async must remain unimplemented"
    );
    assert!(
        mode_async_body.contains("NotImplemented"),
        "mode=async body must advertise NotImplemented, body: {mode_async_body}"
    );

    let async_bool =
        manual_transition_run_with_query(&hot, "/rustfs/admin/v3/ilm/transition/run?bucket=manual-async-test&async=true").await?;
    let (async_bool_status, async_bool_body) = async_bool;
    assert_eq!(
        async_bool_status,
        reqwest::StatusCode::NOT_IMPLEMENTED,
        "async=true must remain unimplemented"
    );
    assert!(
        async_bool_body.contains("NotImplemented"),
        "async=true body must advertise NotImplemented, body: {async_bool_body}"
    );

    hot.stop_server();
    Ok(())
}

async fn manual_transition_run_with_query(
    hot: &RustFSTestEnvironment,
    path: &str,
) -> Result<(reqwest::StatusCode, String), Box<dyn std::error::Error + Send + Sync>> {
    signed_admin_request(&hot.url, Method::POST, path, None, &hot.access_key, &hot.secret_key).await
}
