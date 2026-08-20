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
const MANUAL_ASYNC_STATUS_BUCKET: &str = "ilm7-manual-async-status";
const MANUAL_CONTINUATION_BUCKET: &str = "ilm7-manual-continuation";
const MANUAL_ASYNC_LIMIT_BUCKET: &str = "ilm7-manual-async-limit";
const MANUAL_ASYNC_CONFLICT_BUCKET: &str = "ilm7-manual-async-conflict";
const MANUAL_ASYNC_PARALLEL_BUCKET_A: &str = "ilm7-manual-async-parallel-a";
const MANUAL_ASYNC_PARALLEL_BUCKET_B: &str = "ilm7-manual-async-parallel-b";
const MANUAL_TIER_FAILURE_BUCKET: &str = "ilm7-manual-tier-failure";
const MANUAL_WORKER_FAILURE_BUCKET: &str = "ilm7-manual-worker-failure";
const MANUAL_ACTIVE_CANCEL_BUCKET: &str = "ilm7-manual-active-cancel";
const MANUAL_RESTART_CANCEL_BUCKET: &str = "ilm7-manual-restart-cancel";
const MANUAL_QUEUE_PRESSURE_PREFIX: &str = "manual-queue-pressure/";
const MANUAL_CONTINUATION_PREFIX: &str = "manual-continuation/";
const MANUAL_ASYNC_LIMIT_PREFIX: &str = "manual-async-limit/";
const MANUAL_ASYNC_CONFLICT_PREFIX: &str = "manual-async-conflict/";
const MANUAL_ASYNC_CONFLICT_NESTED_PREFIX: &str = "manual-async-conflict/nested/";
const MANUAL_ASYNC_PARALLEL_PREFIX: &str = "manual-async-parallel/";
const MANUAL_TIER_FAILURE_PREFIX: &str = "manual-tier-failure/";
const MANUAL_WORKER_FAILURE_PREFIX: &str = "manual-worker-failure/";
const MANUAL_ACTIVE_CANCEL_PREFIX: &str = "manual-active-cancel/";
const MANUAL_RESTART_CANCEL_PREFIX: &str = "manual-restart-cancel/";
const MANUAL_ASYNC_CONFLICT_OBJECTS: usize = 512;
const MANUAL_ASYNC_PARALLEL_OBJECTS: usize = 64;
const MANUAL_ACTIVE_CANCEL_OBJECTS: usize = 512;
const MANUAL_RESTART_CANCEL_OBJECTS: usize = 512;
const MANUAL_ACTIVE_CANCEL_RUNNING_TIMEOUT: StdDuration = StdDuration::from_secs(15);
const MANUAL_TRANSITION_CANCEL_BARRIER_ENV: &str = "RUSTFS_E2E_MANUAL_TRANSITION_CANCEL_BARRIER";
const MANUAL_ASYNC_CONFLICT_TERMINAL_TIMEOUT: StdDuration = StdDuration::from_secs(90);
const MANUAL_RESTART_RECOVERY_TIMEOUT: StdDuration = StdDuration::from_secs(80);
const OBJECT_KEY: &str = "tier/鲁A12345/report.bin";
const MANUAL_DUE_KEY: &str = "manual-due/report.bin";
const MANUAL_DRY_RUN_KEY: &str = "manual-dry-run/report.bin";
const MANUAL_NOT_DUE_KEY: &str = "manual-not-due/report.bin";
const MANUAL_ASYNC_STATUS_KEY: &str = "manual-async-status/report.bin";
const MANUAL_TIER_FAILURE_KEY: &str = "manual-tier-failure/report.bin";
const MANUAL_WORKER_FAILURE_KEY: &str = "manual-worker-failure/report.bin";
const CONTENT_TYPE: &str = "application/x-ilm7";
const USER_META_KEY: &str = "ilm7-origin";
const USER_META_VAL: &str = "hermetic-transition";
const HDR_SOURCE_REPLICATION_REQUEST: &str = "x-rustfs-source-replication-request";
const HDR_SOURCE_MTIME: &str = "x-rustfs-source-mtime";
const TIER_MUTATION_RECOVERY_CHANGED: &str = "Remote tier mutation recovery changed before publish";

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

    let verify_path = format!("/rustfs/admin/v3/tier/{TIER_NAME}");
    let deadline = Instant::now() + StdDuration::from_secs(30);
    let mut recovery_changed = false;
    loop {
        if recovery_changed {
            let (status, _) =
                signed_admin_request(&hot.url, Method::GET, &verify_path, None, &hot.access_key, &hot.secret_key).await?;
            if status.is_success() {
                return Ok(());
            }
        }
        let (status, resp) = signed_admin_request(
            &hot.url,
            Method::PUT,
            "/rustfs/admin/v3/tier",
            Some(&body),
            &hot.access_key,
            &hot.secret_key,
        )
        .await?;
        if status.is_success() {
            return Ok(());
        }
        if resp.contains(TIER_MUTATION_RECOVERY_CHANGED) {
            recovery_changed = true;
        } else if !recovery_changed || !resp.contains("TierNameAlreadyExist") {
            return Err(format!("AddTier(RustFS) failed: status={status}, body={resp}").into());
        }
        if Instant::now() >= deadline {
            return Err(format!("AddTier(RustFS) failed: status={status}, body={resp}").into());
        }
        tokio::time::sleep(StdDuration::from_millis(100)).await;
    }
}

async fn remove_rustfs_tier_force(hot: &RustFSTestEnvironment) -> TestResult {
    let path = format!("/rustfs/admin/v3/tier/{TIER_NAME}?force=true");
    let deadline = Instant::now() + StdDuration::from_secs(30);
    loop {
        let (status, resp) =
            signed_admin_request(&hot.url, Method::DELETE, &path, None, &hot.access_key, &hot.secret_key).await?;
        if status.is_success() {
            return Ok(());
        }
        if (!resp.contains("TierNameBackendInUse") && !resp.contains(TIER_MUTATION_RECOVERY_CHANGED))
            || Instant::now() >= deadline
        {
            return Err(format!("RemoveTier(RustFS) failed: status={status}, body={resp}").into());
        }
        // Tier mutation cleanup and startup recovery are asynchronous.
        tokio::time::sleep(StdDuration::from_millis(100)).await;
    }
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

async fn put_single_part_object(client: &Client, bucket: &str, key: &str, body: &'static [u8]) -> TestResult {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
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
    cancel_endpoint: Option<String>,
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
    #[serde(default)]
    transition_completed: u64,
    #[serde(default)]
    transition_failed: u64,
    #[serde(default)]
    tier_failure: u64,
    #[serde(default)]
    cancelled: bool,
    truncated_by_limit: bool,
    truncated_by_duration: bool,
    continuation_token: Option<String>,
}

fn assert_completed_or_in_flight_partial(state: &str, report: &ManualTransitionRunReport, context: &str) {
    match state {
        "completed" => {}
        "partial" if report.skipped_already_in_flight > 0 => {}
        _ => panic!("{context}: state={state}, report={report:#?}"),
    }
}

#[derive(Debug, Deserialize)]
struct ManualTransitionQueueSnapshot {
    queue_capacity: u64,
    queued: u64,
    active: u64,
    workers: u64,
    queue_full: u64,
    queue_send_timeout: u64,
    compensation_pending: u64,
    compensation_running: u64,
}

#[derive(Debug, Deserialize)]
struct ScannerStatusResponse {
    metrics: ScannerStatusMetrics,
}

#[derive(Debug, Deserialize)]
struct ScannerStatusMetrics {
    lifecycle_transition: ScannerTransitionQueueState,
}

#[derive(Debug, Deserialize)]
struct ScannerTransitionQueueState {
    current_queued: u64,
    current_active: u64,
    failed: u64,
}

#[derive(Debug, Deserialize)]
struct ManualTransitionJobStatusResponse {
    job_id: String,
    status_endpoint: String,
    cancel_endpoint: String,
    status: String,
    cancel_requested: bool,
    failure_reason: Option<String>,
    report: ManualTransitionRunReport,
    queue_snapshot: ManualTransitionQueueSnapshot,
}

#[derive(Debug, Deserialize)]
struct ManualTransitionJobConflictResponse {
    state: String,
    mode: String,
    active_job_id: String,
    status_endpoint: String,
    cancel_endpoint: String,
    scope_key: String,
}

async fn manual_transition_run(
    hot: &RustFSTestEnvironment,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
) -> Result<ManualTransitionRunResponse, Box<dyn std::error::Error + Send + Sync>> {
    manual_transition_run_with_max(hot, bucket, prefix, dry_run, 10).await
}

async fn manual_transition_run_with_max(
    hot: &RustFSTestEnvironment,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
    max_objects: u64,
) -> Result<ManualTransitionRunResponse, Box<dyn std::error::Error + Send + Sync>> {
    manual_transition_run_with_max_and_continuation(hot, bucket, prefix, dry_run, max_objects, None).await
}

async fn manual_transition_run_with_max_and_continuation(
    hot: &RustFSTestEnvironment,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
    max_objects: u64,
    continuation_token: Option<&str>,
) -> Result<ManualTransitionRunResponse, Box<dyn std::error::Error + Send + Sync>> {
    let bucket = urlencoding::encode(bucket);
    let prefix = urlencoding::encode(prefix);
    let tier = urlencoding::encode(TIER_NAME);
    let mut path = format!(
        "/rustfs/admin/v3/ilm/transition/run?bucket={bucket}&prefix={prefix}&tier={tier}&dryRun={dry_run}&maxObjects={max_objects}"
    );
    if let Some(token) = continuation_token {
        path.push_str("&continuationToken=");
        path.push_str(&urlencoding::encode(token));
    }
    let (status, body) = signed_admin_request(&hot.url, Method::POST, &path, None, &hot.access_key, &hot.secret_key).await?;
    if !status.is_success() {
        return Err(format!("manual transition run failed: status={status}, body={body}").into());
    }
    Ok(serde_json::from_str(&body)?)
}

async fn manual_transition_async_run(
    hot: &RustFSTestEnvironment,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
    max_objects: u64,
) -> Result<ManualTransitionRunResponse, Box<dyn std::error::Error + Send + Sync>> {
    let (status, body) = manual_transition_async_run_raw(hot, bucket, prefix, dry_run, max_objects).await?;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "async manual transition response: {body}");
    Ok(serde_json::from_str(&body)?)
}

async fn manual_transition_async_run_raw(
    hot: &RustFSTestEnvironment,
    bucket: &str,
    prefix: &str,
    dry_run: bool,
    max_objects: u64,
) -> Result<(reqwest::StatusCode, String), Box<dyn std::error::Error + Send + Sync>> {
    let bucket = urlencoding::encode(bucket);
    let prefix = urlencoding::encode(prefix);
    let tier = urlencoding::encode(TIER_NAME);
    let path = format!(
        "/rustfs/admin/v3/ilm/transition/run?bucket={bucket}&prefix={prefix}&tier={tier}&dryRun={dry_run}&maxObjects={max_objects}&mode=async"
    );
    signed_admin_request(&hot.url, Method::POST, &path, None, &hot.access_key, &hot.secret_key).await
}

async fn manual_transition_job_status(
    hot: &RustFSTestEnvironment,
    status_endpoint: &str,
) -> Result<ManualTransitionJobStatusResponse, Box<dyn std::error::Error + Send + Sync>> {
    let (status, body) = manual_transition_job_status_raw(hot, status_endpoint).await?;
    assert_eq!(status, reqwest::StatusCode::OK, "manual transition job status response: {body}");
    assert_manual_transition_job_response_contract(&body, "manual transition job status response")?;
    Ok(serde_json::from_str(&body)?)
}

async fn manual_transition_job_status_raw(
    hot: &RustFSTestEnvironment,
    status_endpoint: &str,
) -> Result<(reqwest::StatusCode, String), Box<dyn std::error::Error + Send + Sync>> {
    signed_admin_request(&hot.url, Method::GET, status_endpoint, None, &hot.access_key, &hot.secret_key).await
}

async fn manual_transition_job_cancel(
    hot: &RustFSTestEnvironment,
    status_endpoint: &str,
) -> Result<ManualTransitionJobStatusResponse, Box<dyn std::error::Error + Send + Sync>> {
    let (status, body) =
        signed_admin_request(&hot.url, Method::DELETE, status_endpoint, None, &hot.access_key, &hot.secret_key).await?;
    assert_eq!(status, reqwest::StatusCode::OK, "manual transition job cancel response: {body}");
    assert_manual_transition_job_response_contract(&body, "manual transition job cancel response")?;
    Ok(serde_json::from_str(&body)?)
}

fn assert_manual_transition_job_response_contract(
    body: &str,
    context: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let value: serde_json::Value = serde_json::from_str(body)?;
    assert_eq!(
        value.get("mode").and_then(serde_json::Value::as_str),
        Some("durable_job"),
        "{context} must keep the durable job mode: {body}"
    );
    assert!(
        value.get("job_id").and_then(serde_json::Value::as_str).is_some(),
        "{context} must include job_id: {body}"
    );
    assert!(
        value.get("status_endpoint").and_then(serde_json::Value::as_str).is_some(),
        "{context} must include status_endpoint: {body}"
    );
    assert!(
        value.get("cancel_endpoint").and_then(serde_json::Value::as_str).is_some(),
        "{context} must include cancel_endpoint: {body}"
    );
    for incompatible in [
        "scope_key",
        "next_marker",
        "next_version_idmarker",
        "marker",
        "version_marker",
        "versionMarker",
    ] {
        assert!(
            value.get(incompatible).is_none(),
            "{context} must not expose incompatible field {incompatible}: {body}"
        );
    }
    Ok(())
}

async fn wait_for_manual_transition_job_terminal(
    hot: &RustFSTestEnvironment,
    status_endpoint: &str,
    deadline: StdDuration,
) -> Result<ManualTransitionJobStatusResponse, Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    loop {
        let status = manual_transition_job_status(hot, status_endpoint).await?;
        if matches!(status.status.as_str(), "completed" | "partial" | "cancelled" | "failed" | "unknown") {
            return Ok(status);
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "manual transition job at {status_endpoint} did not reach a terminal state within {}s; last={status:#?}",
                deadline.as_secs()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(250)).await;
    }
}

async fn wait_for_manual_transition_job_running(
    hot: &RustFSTestEnvironment,
    status_endpoint: &str,
    deadline: StdDuration,
) -> Result<ManualTransitionJobStatusResponse, Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    loop {
        let status = manual_transition_job_status(hot, status_endpoint).await?;
        if status.status == "running" {
            return Ok(status);
        }
        if matches!(status.status.as_str(), "completed" | "partial" | "cancelled" | "failed" | "unknown") {
            return Err(format!(
                "manual transition job reached terminal state before it became observable as running: {status:#?}"
            )
            .into());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "manual transition job at {status_endpoint} did not become running within {}s; last={status:#?}",
                deadline.as_secs()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }
}

async fn scanner_transition_queue_state(
    hot: &RustFSTestEnvironment,
) -> Result<ScannerTransitionQueueState, Box<dyn std::error::Error + Send + Sync>> {
    let path = "/rustfs/admin/v3/scanner/status";
    let (status, body) = signed_admin_request(&hot.url, Method::GET, path, None, &hot.access_key, &hot.secret_key).await?;
    if !status.is_success() {
        return Err(format!("scanner status failed: status={status}, body={body}").into());
    }
    Ok(serde_json::from_str::<ScannerStatusResponse>(&body)?
        .metrics
        .lifecycle_transition)
}

async fn wait_for_transition_failure_and_idle(
    hot: &RustFSTestEnvironment,
    failed_before: u64,
    deadline: StdDuration,
) -> TestResult {
    let start = Instant::now();
    loop {
        let state = scanner_transition_queue_state(hot).await?;
        if state.failed > failed_before && state.current_queued == 0 && state.current_active == 0 {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "transition queue did not report a new failure and become idle within {}s; failed_before={failed_before}, last={state:#?}",
                deadline.as_secs()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(100)).await;
    }
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
    assert_completed_or_in_flight_partial(&due.state, &due.report, "due manual transition run");
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
    assert_eq!(due.report.tier_failure, 0);
    assert!(!due.report.cancelled);
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
    assert_eq!(dry.report.tier_failure, 0);
    assert!(!dry.report.cancelled);
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
    assert_eq!(not_due.report.tier_failure, 0);
    assert!(!not_due.report.cancelled);
    assert_eq!(not_due.report.skipped_queue_full, 0);
    assert_eq!(not_due.report.skipped_queue_closed, 0);
    assert_eq!(not_due.report.skipped_queue_timeout, 0);
    assert!(!not_due.report.truncated_by_duration);
    assert_remains_not_transitioned(&hot_client, MANUAL_NOT_DUE_BUCKET, MANUAL_NOT_DUE_KEY, StdDuration::from_secs(2)).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_async_job_status_polling() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualasynccoldtieradmin".to_string();
    cold.secret_key = "manualasynccoldtiersecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    hot_client.create_bucket().bucket(MANUAL_ASYNC_STATUS_BUCKET).send().await?;
    put_lifecycle_transition_rule(&hot_client, MANUAL_ASYNC_STATUS_BUCKET, "manual-async-status", "manual-async-status/", 1)
        .await?;
    put_single_part_object(
        &hot_client,
        MANUAL_ASYNC_STATUS_BUCKET,
        MANUAL_ASYNC_STATUS_KEY,
        b"manual async status not-yet-due dry-run object",
    )
    .await?;

    let before_dry_run_remote_count = cold_tier_object_count(&cold_client).await?;
    let accepted = manual_transition_async_run(&hot, MANUAL_ASYNC_STATUS_BUCKET, "manual-async-status/", true, 10).await?;
    assert_eq!(accepted.state, "accepted");
    assert_eq!(accepted.mode, "durable_job");
    assert_eq!(accepted.report.bucket, MANUAL_ASYNC_STATUS_BUCKET);
    assert_eq!(accepted.report.prefix, "manual-async-status/");
    assert!(accepted.report.dry_run);
    assert_eq!(accepted.report.scanned, 0);
    assert_eq!(accepted.report.eligible, 0);
    assert_eq!(accepted.report.transition_completed, 0);
    assert_eq!(accepted.report.transition_failed, 0);
    let job_id = accepted.job_id.as_deref().ok_or("async response must include job_id")?;
    let status_endpoint = accepted
        .status_endpoint
        .as_deref()
        .ok_or("async response must include status_endpoint")?;
    let cancel_endpoint = accepted
        .cancel_endpoint
        .as_deref()
        .ok_or("async response must include cancel_endpoint")?;
    assert_eq!(cancel_endpoint, status_endpoint);
    assert!(
        status_endpoint.ends_with(job_id),
        "status endpoint must embed job id: endpoint={status_endpoint}, job_id={job_id}"
    );
    assert_eq!(
        accepted.cancel_endpoint.as_deref(),
        Some(status_endpoint),
        "async run must return the cancel endpoint used by rc cancel"
    );

    let terminal = wait_for_manual_transition_job_terminal(&hot, status_endpoint, StdDuration::from_secs(30)).await?;
    assert_eq!(terminal.job_id, job_id);
    assert_eq!(terminal.status_endpoint, status_endpoint);
    assert_eq!(terminal.cancel_endpoint, status_endpoint);
    assert_eq!(terminal.status, "completed", "terminal job response: {terminal:#?}");
    assert!(!terminal.cancel_requested);
    assert_eq!(terminal.failure_reason, None);
    assert_eq!(terminal.report.bucket, MANUAL_ASYNC_STATUS_BUCKET);
    assert_eq!(terminal.report.prefix, "manual-async-status/");
    assert_eq!(terminal.report.tier.as_deref(), Some(TIER_NAME));
    assert!(terminal.report.dry_run);
    assert!(terminal.report.lifecycle_config_found);
    assert_eq!(terminal.report.scanned, 1, "terminal job response: {terminal:#?}");
    assert_eq!(terminal.report.eligible, 0, "terminal job response: {terminal:#?}");
    assert_eq!(terminal.report.dry_run_eligible, 0, "terminal job response: {terminal:#?}");
    assert_eq!(terminal.report.enqueued, 0, "terminal job response: {terminal:#?}");
    assert_eq!(terminal.report.skipped_not_transition, 1, "terminal job response: {terminal:#?}");
    assert_eq!(terminal.report.skipped_queue_full, 0);
    assert_eq!(terminal.report.skipped_queue_closed, 0);
    assert_eq!(terminal.report.skipped_queue_timeout, 0);
    assert_eq!(terminal.report.transition_completed, 0);
    assert_eq!(terminal.report.transition_failed, 0);
    assert_eq!(terminal.report.tier_failure, 0);
    assert!(!terminal.report.cancelled);
    assert!(!terminal.report.truncated_by_limit);
    assert!(!terminal.report.truncated_by_duration);

    let after_cancel = manual_transition_job_cancel(&hot, status_endpoint).await?;
    assert_eq!(after_cancel.status, "completed");
    assert_eq!(after_cancel.status_endpoint, status_endpoint);
    assert_eq!(after_cancel.cancel_endpoint, status_endpoint);
    assert!(!after_cancel.cancel_requested);
    assert_eq!(
        cold_tier_object_count(&cold_client).await?,
        before_dry_run_remote_count,
        "not-yet-due async dry-run job must not write to cold tier"
    );
    assert_not_transitioned(&hot_client, MANUAL_ASYNC_STATUS_BUCKET, MANUAL_ASYNC_STATUS_KEY).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_async_limit_reports_terminal_partial() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualasynclimitcoldtieradmin".to_string();
    cold.secret_key = "manualasynclimitcoldtiersecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    hot_client.create_bucket().bucket(MANUAL_ASYNC_LIMIT_BUCKET).send().await?;
    for idx in 0..2 {
        let key = format!("{MANUAL_ASYNC_LIMIT_PREFIX}obj-{idx:02}");
        put_single_part_object(&hot_client, MANUAL_ASYNC_LIMIT_BUCKET, &key, b"async limit payload").await?;
    }
    put_lifecycle_transition_rule(&hot_client, MANUAL_ASYNC_LIMIT_BUCKET, "manual-async-limit", MANUAL_ASYNC_LIMIT_PREFIX, 1)
        .await?;

    let before_remote_count = cold_tier_object_count(&cold_client).await?;
    let accepted = manual_transition_async_run(&hot, MANUAL_ASYNC_LIMIT_BUCKET, MANUAL_ASYNC_LIMIT_PREFIX, false, 1).await?;
    assert_eq!(accepted.state, "accepted");
    assert_eq!(accepted.mode, "durable_job");
    assert_eq!(accepted.report.bucket, MANUAL_ASYNC_LIMIT_BUCKET);
    assert_eq!(accepted.report.prefix, MANUAL_ASYNC_LIMIT_PREFIX);
    assert_eq!(accepted.report.scanned, 0);
    assert_eq!(accepted.report.transition_completed, 0);
    assert_eq!(accepted.report.transition_failed, 0);
    let job_id = accepted.job_id.as_deref().ok_or("async response must include job_id")?;
    let status_endpoint = accepted
        .status_endpoint
        .as_deref()
        .ok_or("async response must include status_endpoint")?;
    assert_eq!(
        accepted.cancel_endpoint.as_deref(),
        Some(status_endpoint),
        "async partial run must return the cancel endpoint used by rc cancel"
    );

    let terminal = wait_for_manual_transition_job_terminal(&hot, status_endpoint, StdDuration::from_secs(30)).await?;
    assert_eq!(terminal.job_id, job_id);
    assert_eq!(terminal.status_endpoint, status_endpoint);
    assert_eq!(terminal.cancel_endpoint, status_endpoint);
    assert_eq!(terminal.status, "partial", "terminal limit job response: {terminal:#?}");
    assert!(!terminal.cancel_requested);
    assert_eq!(terminal.failure_reason, None);
    assert_eq!(terminal.report.bucket, MANUAL_ASYNC_LIMIT_BUCKET);
    assert_eq!(terminal.report.prefix, MANUAL_ASYNC_LIMIT_PREFIX);
    assert_eq!(terminal.report.tier.as_deref(), Some(TIER_NAME));
    assert!(!terminal.report.dry_run);
    assert_eq!(terminal.report.scanned, 1, "terminal limit job response: {terminal:#?}");
    assert_eq!(terminal.report.eligible, 0, "terminal limit job response: {terminal:#?}");
    assert_eq!(terminal.report.enqueued, 0, "terminal limit job response: {terminal:#?}");
    assert_eq!(terminal.report.skipped_not_transition, 1, "terminal limit job response: {terminal:#?}");
    assert_eq!(terminal.report.skipped_queue_full, 0);
    assert_eq!(terminal.report.skipped_queue_closed, 0);
    assert_eq!(terminal.report.skipped_queue_timeout, 0);
    assert_eq!(terminal.report.transition_completed, 0);
    assert_eq!(terminal.report.transition_failed, 0);
    assert_eq!(terminal.report.tier_failure, 0);
    assert!(!terminal.report.cancelled);
    assert!(terminal.report.truncated_by_limit);
    assert!(!terminal.report.truncated_by_duration);
    assert!(terminal.queue_snapshot.queue_capacity >= terminal.queue_snapshot.queued);
    assert_eq!(terminal.queue_snapshot.queued, 0);
    assert!(terminal.queue_snapshot.workers >= terminal.queue_snapshot.active);
    assert_eq!(terminal.queue_snapshot.active, 0);
    assert_eq!(terminal.queue_snapshot.queue_full, 0);
    assert_eq!(terminal.queue_snapshot.queue_send_timeout, 0);
    assert_eq!(terminal.queue_snapshot.compensation_pending, 0);
    assert_eq!(terminal.queue_snapshot.compensation_running, 0);
    let continuation = terminal
        .report
        .continuation_token
        .as_deref()
        .ok_or("terminal partial async job must return an opaque continuation token")?;
    assert!(
        !continuation.contains(MANUAL_ASYNC_LIMIT_PREFIX),
        "async continuation token must not expose the raw object prefix: {continuation}"
    );

    let after_cancel = manual_transition_job_cancel(&hot, status_endpoint).await?;
    assert_eq!(after_cancel.status, "partial");
    assert_eq!(after_cancel.status_endpoint, status_endpoint);
    assert_eq!(after_cancel.cancel_endpoint, status_endpoint);
    assert!(!after_cancel.cancel_requested);
    assert_eq!(after_cancel.report.bucket, terminal.report.bucket);
    assert_eq!(after_cancel.report.prefix, terminal.report.prefix);
    assert_eq!(after_cancel.report.tier, terminal.report.tier);
    assert_eq!(after_cancel.report.scanned, terminal.report.scanned);
    assert_eq!(after_cancel.report.skipped_not_transition, terminal.report.skipped_not_transition);
    assert_eq!(after_cancel.report.transition_completed, terminal.report.transition_completed);
    assert_eq!(after_cancel.report.transition_failed, terminal.report.transition_failed);
    assert_eq!(after_cancel.report.tier_failure, terminal.report.tier_failure);
    assert_eq!(after_cancel.report.cancelled, terminal.report.cancelled);
    assert_eq!(after_cancel.report.truncated_by_limit, terminal.report.truncated_by_limit);
    assert_eq!(after_cancel.report.continuation_token, terminal.report.continuation_token);

    let second_cancel = manual_transition_job_cancel(&hot, status_endpoint).await?;
    assert_eq!(second_cancel.status, "partial");
    assert_eq!(second_cancel.status_endpoint, status_endpoint);
    assert_eq!(second_cancel.cancel_endpoint, status_endpoint);
    assert!(!second_cancel.cancel_requested);
    assert_eq!(second_cancel.report.scanned, terminal.report.scanned);
    assert_eq!(second_cancel.report.transition_completed, terminal.report.transition_completed);
    assert_eq!(second_cancel.report.transition_failed, terminal.report.transition_failed);
    assert_eq!(second_cancel.report.tier_failure, terminal.report.tier_failure);
    assert_eq!(second_cancel.report.cancelled, terminal.report.cancelled);
    assert_eq!(second_cancel.report.truncated_by_limit, terminal.report.truncated_by_limit);
    assert_eq!(second_cancel.report.continuation_token, terminal.report.continuation_token);

    let status_after_cancel = manual_transition_job_status(&hot, status_endpoint).await?;
    assert_eq!(status_after_cancel.job_id, job_id);
    assert_eq!(status_after_cancel.status, second_cancel.status);
    assert_eq!(status_after_cancel.status_endpoint, status_endpoint);
    assert_eq!(status_after_cancel.cancel_endpoint, status_endpoint);
    assert_eq!(status_after_cancel.cancel_requested, second_cancel.cancel_requested);
    assert_eq!(status_after_cancel.failure_reason, second_cancel.failure_reason);
    assert_eq!(status_after_cancel.report.bucket, terminal.report.bucket);
    assert_eq!(status_after_cancel.report.prefix, terminal.report.prefix);
    assert_eq!(status_after_cancel.report.tier, terminal.report.tier);
    assert_eq!(status_after_cancel.report.scanned, terminal.report.scanned);
    assert_eq!(status_after_cancel.report.skipped_not_transition, terminal.report.skipped_not_transition);
    assert_eq!(status_after_cancel.report.transition_completed, terminal.report.transition_completed);
    assert_eq!(status_after_cancel.report.transition_failed, terminal.report.transition_failed);
    assert_eq!(status_after_cancel.report.tier_failure, terminal.report.tier_failure);
    assert_eq!(status_after_cancel.report.cancelled, terminal.report.cancelled);
    assert_eq!(status_after_cancel.report.truncated_by_limit, terminal.report.truncated_by_limit);
    assert_eq!(status_after_cancel.report.continuation_token, terminal.report.continuation_token);

    let resumed = manual_transition_run_with_max_and_continuation(
        &hot,
        MANUAL_ASYNC_LIMIT_BUCKET,
        MANUAL_ASYNC_LIMIT_PREFIX,
        false,
        10,
        Some(continuation),
    )
    .await?;
    assert_eq!(resumed.state, "completed", "async limit continuation resume: {resumed:#?}");
    assert_eq!(resumed.mode, "enqueue_only");
    assert_eq!(resumed.report.bucket, MANUAL_ASYNC_LIMIT_BUCKET);
    assert_eq!(resumed.report.prefix, MANUAL_ASYNC_LIMIT_PREFIX);
    assert_eq!(resumed.report.tier.as_deref(), Some(TIER_NAME));
    assert!(!resumed.report.dry_run);
    assert_eq!(resumed.report.scanned, 1, "async limit continuation resume: {resumed:#?}");
    assert_eq!(resumed.report.eligible, 0, "async limit continuation resume: {resumed:#?}");
    assert_eq!(resumed.report.skipped_not_transition, 1, "async limit continuation resume: {resumed:#?}");
    assert_eq!(resumed.report.transition_completed, 0);
    assert_eq!(resumed.report.transition_failed, 0);
    assert_eq!(resumed.report.tier_failure, 0);
    assert!(!resumed.report.cancelled);
    assert!(!resumed.report.truncated_by_limit);
    assert!(resumed.report.continuation_token.is_none());
    assert_eq!(cold_tier_object_count(&cold_client).await?, before_remote_count);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_async_scope_conflicts_report_active_job() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualasyncconflictcoldtieradmin".to_string();
    cold.secret_key = "manualasyncconflictcoldtiersecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_SCANNER_ENABLED", "false"),
            ("RUSTFS_SCANNER_CYCLE", "3600"),
            (MANUAL_TRANSITION_CANCEL_BARRIER_ENV, "1"),
        ],
    )
    .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    hot_client.create_bucket().bucket(MANUAL_ASYNC_CONFLICT_BUCKET).send().await?;
    for idx in 0..MANUAL_ASYNC_CONFLICT_OBJECTS {
        let key = format!("{MANUAL_ASYNC_CONFLICT_NESTED_PREFIX}obj-{idx:02}");
        put_single_part_object(&hot_client, MANUAL_ASYNC_CONFLICT_BUCKET, &key, b"async conflict payload").await?;
    }
    put_lifecycle_transition_rule(
        &hot_client,
        MANUAL_ASYNC_CONFLICT_BUCKET,
        "manual-async-conflict",
        MANUAL_ASYNC_CONFLICT_PREFIX,
        0,
    )
    .await?;

    let before_remote_count = cold_tier_object_count(&cold_client).await?;
    let accepted = manual_transition_async_run(
        &hot,
        MANUAL_ASYNC_CONFLICT_BUCKET,
        MANUAL_ASYNC_CONFLICT_PREFIX,
        false,
        MANUAL_ASYNC_CONFLICT_OBJECTS as u64,
    )
    .await?;
    let job_id = accepted
        .job_id
        .as_deref()
        .ok_or("accepted async response must include job_id")?;
    let status_endpoint = accepted
        .status_endpoint
        .as_deref()
        .ok_or("accepted async response must include status_endpoint")?;
    let cancel_endpoint = accepted
        .cancel_endpoint
        .as_deref()
        .ok_or("accepted async response must include cancel_endpoint")?;
    assert_eq!(cancel_endpoint, status_endpoint);

    assert_eq!(accepted.state, "accepted");
    assert_eq!(accepted.mode, "durable_job");
    assert_eq!(accepted.report.bucket, MANUAL_ASYNC_CONFLICT_BUCKET);
    assert_eq!(accepted.report.prefix, MANUAL_ASYNC_CONFLICT_PREFIX);
    let active = wait_for_manual_transition_job_running(&hot, status_endpoint, MANUAL_ACTIVE_CANCEL_RUNNING_TIMEOUT).await?;
    assert_eq!(active.job_id, job_id);

    let (conflict_status, conflict_body) = manual_transition_async_run_raw(
        &hot,
        MANUAL_ASYNC_CONFLICT_BUCKET,
        MANUAL_ASYNC_CONFLICT_NESTED_PREFIX,
        false,
        MANUAL_ASYNC_CONFLICT_OBJECTS as u64,
    )
    .await?;
    assert_eq!(
        conflict_status,
        reqwest::StatusCode::CONFLICT,
        "active async run must reject nested prefix {}: {conflict_body}",
        MANUAL_ASYNC_CONFLICT_NESTED_PREFIX
    );
    let conflict: ManualTransitionJobConflictResponse = serde_json::from_str(&conflict_body)?;
    assert_eq!(conflict.state, "conflict");
    assert_eq!(conflict.mode, "durable_job");
    assert_eq!(conflict.active_job_id, job_id);
    assert_eq!(conflict.status_endpoint, status_endpoint);
    assert_eq!(conflict.cancel_endpoint, status_endpoint);
    assert!(!conflict.scope_key.is_empty());

    manual_transition_job_cancel(&hot, cancel_endpoint).await?;

    let terminal = wait_for_manual_transition_job_terminal(&hot, status_endpoint, MANUAL_ASYNC_CONFLICT_TERMINAL_TIMEOUT).await?;
    assert_eq!(terminal.job_id, job_id);
    assert_eq!(terminal.status, "cancelled", "terminal conflict winner response: {terminal:#?}");
    assert!(!terminal.report.dry_run);
    assert_eq!(terminal.report.bucket, MANUAL_ASYNC_CONFLICT_BUCKET);
    assert_eq!(terminal.report.prefix, accepted.report.prefix);
    assert!(terminal.report.cancelled, "terminal conflict winner response: {terminal:#?}");
    assert_eq!(terminal.report.scanned, 0, "terminal conflict winner response: {terminal:#?}");
    assert_eq!(terminal.report.enqueued, 0, "terminal conflict winner response: {terminal:#?}");
    assert_eq!(
        terminal.report.transition_completed, 0,
        "terminal conflict winner response: {terminal:#?}"
    );
    let after_remote_count = cold_tier_object_count(&cold_client).await?;
    assert!(after_remote_count >= before_remote_count);
    assert!(after_remote_count <= before_remote_count + MANUAL_ASYNC_CONFLICT_OBJECTS);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_async_different_buckets_admit_concurrently() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualasyncparallelcoldadmin".to_string();
    cold.secret_key = "manualasyncparallelcoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    for bucket in [MANUAL_ASYNC_PARALLEL_BUCKET_A, MANUAL_ASYNC_PARALLEL_BUCKET_B] {
        hot_client.create_bucket().bucket(bucket).send().await?;
        put_lifecycle_transition_rule(&hot_client, bucket, "manual-async-parallel", MANUAL_ASYNC_PARALLEL_PREFIX, 1).await?;
        for idx in 0..MANUAL_ASYNC_PARALLEL_OBJECTS {
            let key = format!("{MANUAL_ASYNC_PARALLEL_PREFIX}obj-{idx:02}");
            put_single_part_object(&hot_client, bucket, &key, b"async parallel bucket payload").await?;
        }
    }

    let before_remote_count = cold_tier_object_count(&cold_client).await?;

    let (first, second) = tokio::join!(
        manual_transition_async_run_raw(
            &hot,
            MANUAL_ASYNC_PARALLEL_BUCKET_A,
            MANUAL_ASYNC_PARALLEL_PREFIX,
            true,
            MANUAL_ASYNC_PARALLEL_OBJECTS as u64
        ),
        manual_transition_async_run_raw(
            &hot,
            MANUAL_ASYNC_PARALLEL_BUCKET_B,
            MANUAL_ASYNC_PARALLEL_PREFIX,
            true,
            MANUAL_ASYNC_PARALLEL_OBJECTS as u64
        )
    );
    let first = first?;
    let second = second?;
    assert_eq!(
        first.0,
        reqwest::StatusCode::ACCEPTED,
        "different-bucket async run A must not conflict with run B: {}",
        first.1
    );
    assert_eq!(
        second.0,
        reqwest::StatusCode::ACCEPTED,
        "different-bucket async run B must not conflict with run A: {}",
        second.1
    );

    let first: ManualTransitionRunResponse = serde_json::from_str(&first.1)?;
    let second: ManualTransitionRunResponse = serde_json::from_str(&second.1)?;
    assert_eq!(first.state, "accepted");
    assert_eq!(first.mode, "durable_job");
    assert_eq!(first.report.bucket, MANUAL_ASYNC_PARALLEL_BUCKET_A);
    assert_eq!(first.report.prefix, MANUAL_ASYNC_PARALLEL_PREFIX);
    assert!(first.report.dry_run);
    assert_eq!(second.state, "accepted");
    assert_eq!(second.mode, "durable_job");
    assert_eq!(second.report.bucket, MANUAL_ASYNC_PARALLEL_BUCKET_B);
    assert_eq!(second.report.prefix, MANUAL_ASYNC_PARALLEL_PREFIX);
    assert!(second.report.dry_run);

    let first_job_id = first.job_id.as_deref().ok_or("accepted async run A must include job_id")?;
    let first_status_endpoint = first
        .status_endpoint
        .as_deref()
        .ok_or("accepted async run A must include status_endpoint")?;
    let second_job_id = second.job_id.as_deref().ok_or("accepted async run B must include job_id")?;
    let second_status_endpoint = second
        .status_endpoint
        .as_deref()
        .ok_or("accepted async run B must include status_endpoint")?;
    assert_ne!(first_job_id, second_job_id);
    assert_ne!(first_status_endpoint, second_status_endpoint);
    assert_eq!(first.cancel_endpoint.as_deref(), Some(first_status_endpoint));
    assert_eq!(second.cancel_endpoint.as_deref(), Some(second_status_endpoint));

    let (first_terminal, second_terminal) = tokio::join!(
        wait_for_manual_transition_job_terminal(&hot, first_status_endpoint, StdDuration::from_secs(30)),
        wait_for_manual_transition_job_terminal(&hot, second_status_endpoint, StdDuration::from_secs(30))
    );
    let first_terminal = first_terminal?;
    let second_terminal = second_terminal?;
    assert_eq!(first_terminal.job_id, first_job_id);
    assert_eq!(first_terminal.status, "completed", "terminal parallel run A: {first_terminal:#?}");
    assert_eq!(first_terminal.report.bucket, MANUAL_ASYNC_PARALLEL_BUCKET_A);
    assert_eq!(first_terminal.report.prefix, MANUAL_ASYNC_PARALLEL_PREFIX);
    assert_eq!(
        first_terminal.report.scanned, MANUAL_ASYNC_PARALLEL_OBJECTS as u64,
        "terminal parallel run A: {first_terminal:#?}"
    );
    assert!(!first_terminal.report.cancelled);
    assert_eq!(first_terminal.report.transition_failed, 0);

    assert_eq!(second_terminal.job_id, second_job_id);
    assert_eq!(second_terminal.status, "completed", "terminal parallel run B: {second_terminal:#?}");
    assert_eq!(second_terminal.report.bucket, MANUAL_ASYNC_PARALLEL_BUCKET_B);
    assert_eq!(second_terminal.report.prefix, MANUAL_ASYNC_PARALLEL_PREFIX);
    assert_eq!(
        second_terminal.report.scanned, MANUAL_ASYNC_PARALLEL_OBJECTS as u64,
        "terminal parallel run B: {second_terminal:#?}"
    );
    assert!(!second_terminal.report.cancelled);
    assert_eq!(second_terminal.report.transition_failed, 0);
    assert_eq!(
        cold_tier_object_count(&cold_client).await?,
        before_remote_count,
        "parallel dry-run jobs must not create additional remote tier objects"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_async_tier_failure_reports_terminal_partial() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualtierfailurecoldadmin".to_string();
    cold.secret_key = "manualtierfailurecoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    hot_client.create_bucket().bucket(MANUAL_TIER_FAILURE_BUCKET).send().await?;
    put_lifecycle_transition_rule(
        &hot_client,
        MANUAL_TIER_FAILURE_BUCKET,
        "manual-tier-failure",
        MANUAL_TIER_FAILURE_PREFIX,
        0,
    )
    .await?;
    remove_rustfs_tier_force(&hot).await?;

    let due_mtime = OffsetDateTime::now_utc() - time::Duration::hours(25);
    put_backdated_single_part_object(
        &hot_client,
        MANUAL_TIER_FAILURE_BUCKET,
        MANUAL_TIER_FAILURE_KEY,
        b"manual tier failure object",
        due_mtime,
    )
    .await?;
    let before_remote_count = cold_tier_object_count(&cold_client).await?;
    let accepted = manual_transition_async_run(&hot, MANUAL_TIER_FAILURE_BUCKET, MANUAL_TIER_FAILURE_PREFIX, false, 10).await?;
    assert_eq!(accepted.state, "accepted");
    assert_eq!(accepted.mode, "durable_job");
    assert_eq!(accepted.report.transition_completed, 0);
    assert_eq!(accepted.report.transition_failed, 0);
    let job_id = accepted.job_id.as_deref().ok_or("async response must include job_id")?;
    let status_endpoint = accepted
        .status_endpoint
        .as_deref()
        .ok_or("async response must include status_endpoint")?;
    let cancel_endpoint = accepted
        .cancel_endpoint
        .as_deref()
        .ok_or("async response must include cancel_endpoint")?;
    assert_eq!(cancel_endpoint, status_endpoint);

    let terminal = wait_for_manual_transition_job_terminal(&hot, status_endpoint, StdDuration::from_secs(30)).await?;
    assert_eq!(terminal.job_id, job_id);
    assert_eq!(terminal.status, "partial", "terminal tier failure job response: {terminal:#?}");
    assert!(!terminal.cancel_requested);
    assert_eq!(terminal.failure_reason, None);
    assert_eq!(terminal.report.bucket, MANUAL_TIER_FAILURE_BUCKET);
    assert_eq!(terminal.report.prefix, MANUAL_TIER_FAILURE_PREFIX);
    assert_eq!(terminal.report.tier.as_deref(), Some(TIER_NAME));
    assert!(!terminal.report.dry_run);
    assert!(terminal.report.lifecycle_config_found);
    assert_eq!(terminal.report.scanned, 1, "terminal tier failure job response: {terminal:#?}");
    assert_eq!(terminal.report.eligible, 0, "terminal tier failure job response: {terminal:#?}");
    assert_eq!(terminal.report.enqueued, 0, "terminal tier failure job response: {terminal:#?}");
    assert_eq!(
        terminal.report.transition_completed, 0,
        "terminal tier failure job response: {terminal:#?}"
    );
    assert_eq!(terminal.report.transition_failed, 0, "terminal tier failure job response: {terminal:#?}");
    assert_eq!(terminal.report.tier_failure, 1, "terminal tier failure job response: {terminal:#?}");
    assert_eq!(terminal.report.skipped_queue_full, 0);
    assert_eq!(terminal.report.skipped_queue_closed, 0);
    assert_eq!(terminal.report.skipped_queue_timeout, 0);
    assert!(!terminal.report.cancelled);
    assert!(!terminal.report.truncated_by_limit);
    assert!(!terminal.report.truncated_by_duration);
    assert_eq!(
        cold_tier_object_count(&cold_client).await?,
        before_remote_count,
        "tier failure must not create a remote object"
    );
    assert_remains_not_transitioned(
        &hot_client,
        MANUAL_TIER_FAILURE_BUCKET,
        MANUAL_TIER_FAILURE_KEY,
        StdDuration::from_secs(2),
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_async_worker_failure_reports_terminal_partial() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualworkerfailurecoldadmin".to_string();
    cold.secret_key = "manualworkerfailurecoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;
    cold.stop_server();

    hot_client.create_bucket().bucket(MANUAL_WORKER_FAILURE_BUCKET).send().await?;
    let due_mtime = OffsetDateTime::now_utc() - time::Duration::hours(25);
    put_backdated_single_part_object(
        &hot_client,
        MANUAL_WORKER_FAILURE_BUCKET,
        MANUAL_WORKER_FAILURE_KEY,
        b"manual worker failure object",
        due_mtime,
    )
    .await?;
    let transition_failures_before = scanner_transition_queue_state(&hot).await?.failed;
    put_lifecycle_transition_rule(
        &hot_client,
        MANUAL_WORKER_FAILURE_BUCKET,
        "manual-worker-failure",
        MANUAL_WORKER_FAILURE_PREFIX,
        0,
    )
    .await?;
    wait_for_transition_failure_and_idle(&hot, transition_failures_before, StdDuration::from_secs(30)).await?;

    let accepted =
        manual_transition_async_run(&hot, MANUAL_WORKER_FAILURE_BUCKET, MANUAL_WORKER_FAILURE_PREFIX, false, 10).await?;
    assert_eq!(accepted.state, "accepted");
    assert_eq!(accepted.mode, "durable_job");
    assert_eq!(accepted.report.transition_completed, 0);
    assert_eq!(accepted.report.transition_failed, 0);
    let job_id = accepted.job_id.as_deref().ok_or("async response must include job_id")?;
    let status_endpoint = accepted
        .status_endpoint
        .as_deref()
        .ok_or("async response must include status_endpoint")?;
    let cancel_endpoint = accepted
        .cancel_endpoint
        .as_deref()
        .ok_or("async response must include cancel_endpoint")?;
    assert_eq!(cancel_endpoint, status_endpoint);

    let terminal = wait_for_manual_transition_job_terminal(&hot, status_endpoint, StdDuration::from_secs(30)).await?;
    assert_eq!(terminal.job_id, job_id);
    assert_eq!(terminal.status, "partial", "terminal worker failure job response: {terminal:#?}");
    assert!(!terminal.cancel_requested);
    assert_eq!(terminal.failure_reason, None);
    assert_eq!(terminal.report.bucket, MANUAL_WORKER_FAILURE_BUCKET);
    assert_eq!(terminal.report.prefix, MANUAL_WORKER_FAILURE_PREFIX);
    assert_eq!(terminal.report.tier.as_deref(), Some(TIER_NAME));
    assert!(!terminal.report.dry_run);
    assert!(terminal.report.lifecycle_config_found);
    assert_eq!(terminal.report.scanned, 1, "terminal worker failure job response: {terminal:#?}");
    assert_eq!(terminal.report.eligible, 1, "terminal worker failure job response: {terminal:#?}");
    assert_eq!(terminal.report.enqueued, 1, "terminal worker failure job response: {terminal:#?}");
    assert_eq!(
        terminal.report.transition_completed, 0,
        "terminal worker failure job response: {terminal:#?}"
    );
    assert_eq!(
        terminal.report.transition_failed, 1,
        "terminal worker failure job response: {terminal:#?}"
    );
    assert_eq!(terminal.report.tier_failure, 1, "terminal worker failure job response: {terminal:#?}");
    assert_eq!(terminal.report.skipped_queue_full, 0);
    assert_eq!(terminal.report.skipped_queue_closed, 0);
    assert_eq!(terminal.report.skipped_queue_timeout, 0);
    assert!(!terminal.report.cancelled);
    assert!(!terminal.report.truncated_by_limit);
    assert!(!terminal.report.truncated_by_duration);
    assert_remains_not_transitioned(
        &hot_client,
        MANUAL_WORKER_FAILURE_BUCKET,
        MANUAL_WORKER_FAILURE_KEY,
        StdDuration::from_secs(2),
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_async_active_cancel_reports_terminal_cancelled() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualactivecancelcoldadmin".to_string();
    cold.secret_key = "manualactivecancelcoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_SCANNER_ENABLED", "false"),
            ("RUSTFS_SCANNER_CYCLE", "3600"),
            (MANUAL_TRANSITION_CANCEL_BARRIER_ENV, "1"),
        ],
    )
    .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    hot_client.create_bucket().bucket(MANUAL_ACTIVE_CANCEL_BUCKET).send().await?;
    put_lifecycle_transition_rule(
        &hot_client,
        MANUAL_ACTIVE_CANCEL_BUCKET,
        "manual-active-cancel",
        MANUAL_ACTIVE_CANCEL_PREFIX,
        1,
    )
    .await?;
    for idx in 0..MANUAL_ACTIVE_CANCEL_OBJECTS {
        let key = format!("{MANUAL_ACTIVE_CANCEL_PREFIX}obj-{idx:03}");
        put_single_part_object(&hot_client, MANUAL_ACTIVE_CANCEL_BUCKET, &key, b"manual active cancel payload").await?;
    }

    let before_remote_count = cold_tier_object_count(&cold_client).await?;
    let accepted =
        manual_transition_async_run(&hot, MANUAL_ACTIVE_CANCEL_BUCKET, MANUAL_ACTIVE_CANCEL_PREFIX, true, 10_000).await?;
    assert_eq!(accepted.state, "accepted");
    assert_eq!(accepted.mode, "durable_job");
    assert_eq!(accepted.report.bucket, MANUAL_ACTIVE_CANCEL_BUCKET);
    assert_eq!(accepted.report.prefix, MANUAL_ACTIVE_CANCEL_PREFIX);
    assert!(accepted.report.dry_run);
    assert_eq!(accepted.report.scanned, 0);
    assert_eq!(accepted.report.enqueued, 0);
    assert_eq!(accepted.report.transition_completed, 0);
    assert_eq!(accepted.report.transition_failed, 0);
    let job_id = accepted.job_id.as_deref().ok_or("async response must include job_id")?;
    let status_endpoint = accepted
        .status_endpoint
        .as_deref()
        .ok_or("async response must include status_endpoint")?;
    assert_eq!(
        accepted.cancel_endpoint.as_deref(),
        Some(status_endpoint),
        "async active-cancel run must return the cancel endpoint used by rc cancel"
    );

    let active = wait_for_manual_transition_job_running(&hot, status_endpoint, MANUAL_ACTIVE_CANCEL_RUNNING_TIMEOUT).await?;
    assert_eq!(active.job_id, job_id);
    assert_eq!(active.status_endpoint, status_endpoint);
    assert_eq!(active.cancel_endpoint, status_endpoint);
    assert!(!active.cancel_requested);

    let cancel_response = manual_transition_job_cancel(&hot, status_endpoint).await?;
    assert_eq!(cancel_response.job_id, job_id);
    assert_eq!(cancel_response.status_endpoint, status_endpoint);
    assert_eq!(cancel_response.cancel_endpoint, status_endpoint);
    assert_eq!(cancel_response.status, "running", "active cancel response: {cancel_response:#?}");
    assert!(cancel_response.cancel_requested, "active cancel response: {cancel_response:#?}");

    let terminal = wait_for_manual_transition_job_terminal(&hot, status_endpoint, StdDuration::from_secs(30)).await?;
    assert_eq!(terminal.job_id, job_id);
    assert_eq!(terminal.status_endpoint, status_endpoint);
    assert_eq!(terminal.cancel_endpoint, status_endpoint);
    assert_eq!(terminal.status, "cancelled", "terminal active cancel response: {terminal:#?}");
    assert!(terminal.cancel_requested);
    assert_eq!(terminal.failure_reason, None);
    assert_eq!(terminal.report.bucket, MANUAL_ACTIVE_CANCEL_BUCKET);
    assert_eq!(terminal.report.prefix, MANUAL_ACTIVE_CANCEL_PREFIX);
    assert_eq!(terminal.report.tier.as_deref(), Some(TIER_NAME));
    assert!(terminal.report.dry_run);
    assert!(terminal.report.lifecycle_config_found);
    assert!(terminal.report.cancelled, "terminal active cancel response: {terminal:#?}");
    assert_eq!(terminal.report.enqueued, 0, "terminal active cancel response: {terminal:#?}");
    assert_eq!(terminal.report.transition_completed, 0);
    assert_eq!(terminal.report.transition_failed, 0);
    assert_eq!(terminal.report.tier_failure, 0);
    assert!(!terminal.report.truncated_by_limit);
    assert!(!terminal.report.truncated_by_duration);
    assert_eq!(
        cold_tier_object_count(&cold_client).await?,
        before_remote_count,
        "active dry-run cancel must not create a remote tier object"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_async_cancel_after_process_restart_recovers_terminal() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualrestartcancelcoldadmin".to_string();
    cold.secret_key = "manualrestartcancelcoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let restart_env = [
        ("RUSTFS_SCANNER_ENABLED", "false"),
        ("RUSTFS_SCANNER_CYCLE", "3600"),
        ("RUSTFS_MAX_TRANSITION_WORKERS", "1"),
        ("RUSTFS_TRANSITION_QUEUE_CAPACITY", "512"),
    ];
    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &restart_env).await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    hot_client.create_bucket().bucket(MANUAL_RESTART_CANCEL_BUCKET).send().await?;
    put_lifecycle_transition_rule(
        &hot_client,
        MANUAL_RESTART_CANCEL_BUCKET,
        "manual-restart-cancel",
        MANUAL_RESTART_CANCEL_PREFIX,
        1,
    )
    .await?;
    for idx in 0..MANUAL_RESTART_CANCEL_OBJECTS {
        let key = format!("{MANUAL_RESTART_CANCEL_PREFIX}obj-{idx:03}");
        put_single_part_object(&hot_client, MANUAL_RESTART_CANCEL_BUCKET, &key, b"manual restart cancel payload").await?;
    }

    cold.stop_server();
    let accepted =
        manual_transition_async_run(&hot, MANUAL_RESTART_CANCEL_BUCKET, MANUAL_RESTART_CANCEL_PREFIX, false, 10_000).await?;
    let job_id = accepted.job_id.as_deref().ok_or("async response must include job_id")?;
    let status_endpoint = accepted
        .status_endpoint
        .as_deref()
        .ok_or("async response must include status_endpoint")?;
    assert_eq!(accepted.cancel_endpoint.as_deref(), Some(status_endpoint));

    hot.restart_server_preserving_data(vec![], &restart_env).await?;

    let restarted = manual_transition_job_status(&hot, status_endpoint).await?;
    assert_eq!(restarted.job_id, job_id);
    assert_eq!(restarted.status_endpoint, status_endpoint);
    assert_eq!(restarted.cancel_endpoint, status_endpoint);

    let terminal = match restarted.status.as_str() {
        "running" => {
            let cancel_after_restart = manual_transition_job_cancel(&hot, status_endpoint).await?;
            assert_eq!(cancel_after_restart.job_id, job_id);
            assert_eq!(cancel_after_restart.status_endpoint, status_endpoint);
            assert_eq!(cancel_after_restart.cancel_endpoint, status_endpoint);
            assert_eq!(
                cancel_after_restart.status, "running",
                "cancel after restart response: {cancel_after_restart:#?}"
            );
            assert!(
                cancel_after_restart.cancel_requested,
                "cancel after restart must durably mark a still-running job: {cancel_after_restart:#?}"
            );
            wait_for_manual_transition_job_terminal(&hot, status_endpoint, MANUAL_RESTART_RECOVERY_TIMEOUT).await?
        }
        "unknown" | "cancelled" | "completed" | "partial" => {
            let cancel_after_restart = manual_transition_job_cancel(&hot, status_endpoint).await?;
            assert_eq!(cancel_after_restart.job_id, job_id);
            assert_eq!(cancel_after_restart.status_endpoint, status_endpoint);
            assert_eq!(cancel_after_restart.cancel_endpoint, status_endpoint);
            assert_eq!(cancel_after_restart.status, restarted.status);
            assert_eq!(cancel_after_restart.report.bucket, MANUAL_RESTART_CANCEL_BUCKET);
            assert_eq!(cancel_after_restart.report.prefix, MANUAL_RESTART_CANCEL_PREFIX);
            assert!(!cancel_after_restart.report.dry_run);
            cancel_after_restart
        }
        _ => {
            return Err(format!("unexpected manual transition job status after restart: {restarted:#?}").into());
        }
    };

    assert_eq!(terminal.job_id, job_id);
    assert_eq!(terminal.status_endpoint, status_endpoint);
    assert_eq!(terminal.cancel_endpoint, status_endpoint);
    assert!(
        matches!(terminal.status.as_str(), "unknown" | "cancelled" | "completed" | "partial"),
        "post-restart manual transition job must remain readable through the durable endpoint: {terminal:#?}"
    );
    match terminal.status.as_str() {
        "unknown" => {
            assert!(
                terminal
                    .failure_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("unknown after restart") || reason.contains("owner loss")),
                "unknown terminal status must explain the restart/owner-loss boundary: {terminal:#?}"
            );
        }
        "cancelled" => {
            assert!(
                terminal.cancel_requested,
                "cancelled terminal response must retain the cancel request: {terminal:#?}"
            );
            assert!(
                terminal.report.cancelled,
                "cancelled terminal response must report cancellation: {terminal:#?}"
            );
            assert_eq!(terminal.failure_reason, None);
        }
        "partial" => {
            assert!(
                terminal.report.transition_failed > 0 || terminal.report.tier_failure > 0,
                "partial terminal response must report a worker or tier failure after cold-tier stop: {terminal:#?}"
            );
        }
        "completed" => {
            assert_eq!(terminal.failure_reason, None);
        }
        _ => unreachable!("terminal status was validated above"),
    }
    assert_eq!(terminal.report.bucket, MANUAL_RESTART_CANCEL_BUCKET);
    assert_eq!(terminal.report.prefix, MANUAL_RESTART_CANCEL_PREFIX);
    assert!(!terminal.report.dry_run);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_job_status_cancel_reject_unknown_jobs() -> TestResult {
    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;

    for method in [Method::GET, Method::DELETE] {
        let (status, body) = signed_admin_request(
            &hot.url,
            method.clone(),
            "/rustfs/admin/v3/ilm/transition/jobs/not-a-uuid",
            None,
            &hot.access_key,
            &hot.secret_key,
        )
        .await?;
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{method} invalid job id response: {body}");
        assert!(body.contains("InvalidArgument"), "{method} invalid job id body: {body}");

        let missing_endpoint = "/rustfs/admin/v3/ilm/transition/jobs/11111111-1111-4111-8111-111111111111";
        let (status, body) =
            signed_admin_request(&hot.url, method.clone(), missing_endpoint, None, &hot.access_key, &hot.secret_key).await?;
        assert_eq!(status, reqwest::StatusCode::NOT_FOUND, "{method} missing job response: {body}");
        assert!(body.contains("NoSuchKey"), "{method} missing job body: {body}");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_run_contract_no_status_cancel_fields() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualcontractcoldtieradmin".to_string();
    cold.secret_key = "manualcontractcoldtiersecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    hot_client.create_bucket().bucket("ilm7-manual-contract").send().await?;

    let (status, body) = signed_admin_request(
        &hot.url,
        Method::POST,
        "/rustfs/admin/v3/ilm/transition/run?bucket=ilm7-manual-contract",
        None,
        &hot.access_key,
        &hot.secret_key,
    )
    .await?;
    assert_eq!(status, reqwest::StatusCode::OK, "manual transition contract call should still be OK");
    let response: serde_json::Value = serde_json::from_str(&body)?;

    assert!(response.get("state").is_some(), "response should include state field");
    assert!(response.get("job_id").is_none() || response.get("job_id").is_some_and(|v| v.is_null()));
    assert!(response.get("status").is_none() || response.get("status").is_some_and(|v| v.is_null()));
    assert!(response.get("status_endpoint").is_none() || response.get("status_endpoint").is_some_and(|v| v.is_null()));
    assert!(response.get("cancel").is_none() || response.get("cancel").is_some_and(|v| v.is_null()));
    assert!(response.get("cancel_endpoint").is_none() || response.get("cancel_endpoint").is_some_and(|v| v.is_null()));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_manual_transition_run_continuation_token_resumes_without_raw_markers() -> TestResult {
    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "manualcontinuationcoldtieradmin".to_string();
    cold.secret_key = "manualcontinuationcoldtiersecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestEnvironment::new().await?;
    hot.start_rustfs_server_with_env(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;
    let hot_client = hot.create_s3_client();
    add_rustfs_tier(&hot, &cold).await?;

    hot_client.create_bucket().bucket(MANUAL_CONTINUATION_BUCKET).send().await?;
    for idx in 0..2 {
        let key = format!("{MANUAL_CONTINUATION_PREFIX}obj-{idx:02}");
        put_single_part_object(&hot_client, MANUAL_CONTINUATION_BUCKET, &key, b"manual continuation payload").await?;
    }
    put_lifecycle_transition_rule(
        &hot_client,
        MANUAL_CONTINUATION_BUCKET,
        "manual-continuation",
        MANUAL_CONTINUATION_PREFIX,
        0,
    )
    .await?;

    let first = manual_transition_run_with_max(&hot, MANUAL_CONTINUATION_BUCKET, MANUAL_CONTINUATION_PREFIX, true, 1).await?;
    assert_eq!(first.state, "partial", "first continuation page: {first:#?}");
    assert_eq!(first.mode, "enqueue_only");
    assert_eq!(first.report.bucket, MANUAL_CONTINUATION_BUCKET);
    assert_eq!(first.report.prefix, MANUAL_CONTINUATION_PREFIX);
    assert!(first.report.dry_run);
    assert_eq!(first.report.scanned, 1, "first continuation page: {first:#?}");
    assert_eq!(first.report.eligible, 1, "first continuation page: {first:#?}");
    assert_eq!(first.report.dry_run_eligible, 1, "first continuation page: {first:#?}");
    assert_eq!(first.report.tier_failure, 0);
    assert!(!first.report.cancelled);
    assert!(first.report.truncated_by_limit);
    let continuation = first
        .report
        .continuation_token
        .as_deref()
        .ok_or("partial manual transition run must return an opaque continuation token")?;
    assert!(
        !continuation.contains(MANUAL_CONTINUATION_PREFIX),
        "continuation token must not expose the raw object prefix: {continuation}"
    );

    hot.restart_server_preserving_data(vec![], &[("RUSTFS_SCANNER_ENABLED", "false"), ("RUSTFS_SCANNER_CYCLE", "3600")])
        .await?;

    let second = manual_transition_run_with_max_and_continuation(
        &hot,
        MANUAL_CONTINUATION_BUCKET,
        MANUAL_CONTINUATION_PREFIX,
        true,
        10,
        Some(continuation),
    )
    .await?;
    assert_eq!(second.state, "completed", "second continuation page: {second:#?}");
    assert_eq!(second.report.scanned, 1, "second continuation page: {second:#?}");
    assert_eq!(second.report.eligible, 1, "second continuation page: {second:#?}");
    assert_eq!(second.report.dry_run_eligible, 1, "second continuation page: {second:#?}");
    assert_eq!(second.report.tier_failure, 0);
    assert!(!second.report.cancelled);
    assert!(!second.report.truncated_by_limit);
    assert!(second.report.continuation_token.is_none());

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
        put_single_part_object(&hot_client, MANUAL_QUEUE_PRESSURE_BUCKET, &key, b"queue-pressure payload").await?;
    }
    put_lifecycle_transition_rule(
        &hot_client,
        MANUAL_QUEUE_PRESSURE_BUCKET,
        "manual-queue-pressure",
        MANUAL_QUEUE_PRESSURE_PREFIX,
        0,
    )
    .await?;

    let response =
        manual_transition_run_with_max(&hot, MANUAL_QUEUE_PRESSURE_BUCKET, MANUAL_QUEUE_PRESSURE_PREFIX, false, 20).await?;

    assert_eq!(response.state, "partial");
    assert_eq!(response.report.bucket, MANUAL_QUEUE_PRESSURE_BUCKET);
    assert_eq!(response.report.prefix, MANUAL_QUEUE_PRESSURE_PREFIX);
    assert!(
        response.report.skipped_queue_full > 0 || response.report.skipped_already_in_flight > 0,
        "expected queue-pressure path to skip at least one object: {:#?}",
        response.report
    );
    assert_eq!(response.report.tier_failure, 0);
    assert!(!response.report.cancelled);
    assert!(!response.report.truncated_by_duration);
    assert!(response.report.enqueued < 20, "partial run should not enqueue all items in this setup");

    let remote_count = cold_tier_object_count(&cold_client).await?;
    assert!(
        remote_count < 20,
        "queue pressure must leave at least one object unqueued, remote_count={remote_count}"
    );
    Ok(())
}
