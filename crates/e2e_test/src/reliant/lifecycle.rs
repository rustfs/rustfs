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

//! Self-managed lifecycle *expiry* end-to-end tests (backlog#1148 ilm-3).
//!
//! Each test spawns its own `rustfs` binary on a random port with an isolated
//! temp dir via [`RustFSTestEnvironment`], so there is no dependency on a
//! pre-started `localhost:9000` server and no `#[ignore]`. Expiry is driven to
//! completion in seconds using two independent, per-test time-control tools:
//!
//! * **`mod_time` back-dating** (see [`put_object_with_backdated_mtime`]): write
//!   an object whose stored `mod_time` is already in the past, so a `Days`-based
//!   rule is due immediately without accelerating the day length. Used by
//!   [`test_lifecycle_expiry_backdated_mtime`].
//! * **`RUSTFS_ILM_DEBUG_DAY_SECS`** (ilm-5): compress one lifecycle "day" to a
//!   couple of seconds so a `Days=1` rule fires shortly after a normal PUT. Used
//!   by [`test_lifecycle_versioned_current_version_expiry_creates_delete_marker`].
//!
//! In every case the scanner must actually *run* for expiry to apply, so each
//! server is started with `RUSTFS_SCANNER_CYCLE=1` (1-second scan cycle) and a
//! small `RUSTFS_ILM_PROCESS_TIME` rounding boundary. Tests poll for the
//! terminal state instead of sleeping a fixed wall-clock interval.

use crate::common::RustFSTestEnvironment;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, BucketVersioningStatus, ExpirationStatus, LifecycleExpiration, LifecycleRule,
    LifecycleRuleFilter, VersioningConfiguration,
};
use std::time::Duration as StdDuration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Internal replication headers that back-date a written object's `mod_time`.
///
/// These are the RustFS side of the MinIO-compatible replication protocol
/// (`crates/utils/src/http/header_compat.rs`; consumed in
/// `rustfs/src/storage/options.rs::put_opts_from_headers`). Sending
/// `x-rustfs-source-replication-request: true` routes the PUT through the
/// replica branch and, when `x-rustfs-source-mtime` (RFC3339) is present, forces
/// the stored `mod_time` to that value.
const HDR_SOURCE_REPLICATION_REQUEST: &str = "x-rustfs-source-replication-request";
const HDR_SOURCE_MTIME: &str = "x-rustfs-source-mtime";

/// PUT an object whose stored `mod_time` is back-dated to `mtime`.
///
/// # Side effects / caveats
///
/// This uses the internal source-replication backdoor, so the write sets
/// `ObjectOptions::replication_request = true` and takes the replica code path.
/// That flag by itself does **not** set the object's replication *status* to
/// `Pending`/`Failed` — only an `x-amz-bucket-replication-status: replica`
/// header would set `REPLICA` status, and the test bucket has no replication
/// configuration — so lifecycle expiry is **not** gated
/// (`crates/lifecycle/src/evaluator.rs::replication_status_blocks_lifecycle`
/// only blocks on `Pending`/`Failed`). The passing
/// [`test_lifecycle_expiry_backdated_mtime`] is the end-to-end proof that a
/// back-dated object is still expired by the scanner.
async fn put_object_with_backdated_mtime(
    client: &Client,
    bucket: &str,
    key: &str,
    body: &[u8],
    mtime: OffsetDateTime,
) -> TestResult {
    let mtime_rfc3339 = mtime.format(&Rfc3339)?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.to_vec()))
        .customize()
        .mutate_request(move |req| {
            req.headers_mut().insert(HDR_SOURCE_REPLICATION_REQUEST, "true");
            req.headers_mut().insert(HDR_SOURCE_MTIME, mtime_rfc3339.clone());
        })
        .send()
        .await?;
    Ok(())
}

/// Returns `true` once `GET bucket/key` fails with `NoSuchKey`, `false` while it
/// still succeeds. Any other error is surfaced.
async fn object_is_gone(client: &Client, bucket: &str, key: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match client.get_object().bucket(bucket).key(key).send().await {
        Ok(_) => Ok(false),
        Err(e) => {
            if let Some(service_error) = e.as_service_error() {
                if service_error.is_no_such_key() {
                    return Ok(true);
                }
                return Err(format!("expected NoSuchKey, got: {e:?}").into());
            }
            Err(format!("expected a service error, got: {e:?}").into())
        }
    }
}

/// Poll until `GET bucket/key` returns `NoSuchKey`, or fail after `deadline`.
///
/// The scanner cycle is 1s under test, so expiry normally lands within a few
/// seconds; the deadline is a generous safety net, not the expected wall-clock.
async fn wait_for_object_expired(client: &Client, bucket: &str, key: &str, deadline: StdDuration) -> TestResult {
    let start = std::time::Instant::now();
    loop {
        if object_is_gone(client, bucket, key).await? {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "object {bucket}/{key} was not expired by the lifecycle scanner within {}s",
                deadline.as_secs()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(500)).await;
    }
}

/// Build a prefix-scoped `Days`-based expiration rule.
fn expiration_rule(id: &str, prefix: &str, days: i32) -> Result<LifecycleRule, Box<dyn std::error::Error + Send + Sync>> {
    let rule = LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .expiration(LifecycleExpiration::builder().days(days).build())
        .status(ExpirationStatus::Enabled)
        .build()?;
    Ok(rule)
}

async fn put_expiration_config(client: &Client, bucket: &str, rule: LifecycleRule) -> TestResult {
    let lifecycle = BucketLifecycleConfiguration::builder().rules(rule).build()?;
    client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(lifecycle)
        .send()
        .await?;
    Ok(())
}

/// Env applied to every server in this module: run the scanner every second and
/// shrink the ILM processing/rounding boundary so `Days`-based deadlines are not
/// rounded up to the next real day.
fn fast_lifecycle_env() -> Vec<(&'static str, &'static str)> {
    vec![("RUSTFS_SCANNER_CYCLE", "1"), ("RUSTFS_ILM_PROCESS_TIME", "1")]
}

/// `Days=1` expiry driven purely by `mod_time` back-dating (no day-length
/// acceleration). Proves:
/// * a matching-prefix object whose `mod_time` is 25h old is expired (the rule's
///   1-day deadline is already in the past);
/// * the `put_object_with_backdated_mtime` backdoor does not trip the
///   replication gate that would otherwise block expiry;
/// * a non-matching-prefix object with the **same** back-dated `mod_time`
///   survives, isolating the prefix filter (not recency) as the cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lifecycle_expiry_backdated_mtime() -> TestResult {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &fast_lifecycle_env()).await?;

    let client = env.create_s3_client();
    let bucket = "ilm3-backdated";
    client.create_bucket().bucket(bucket).send().await?;

    let matched_key = "expire/object.txt";
    let survivor_key = "keep/object.txt";
    // 25h in the past: older than the 1-day rule with a normal 86400s day length.
    let backdated = OffsetDateTime::now_utc() - time::Duration::hours(25);

    put_object_with_backdated_mtime(&client, bucket, matched_key, b"expire me", backdated).await?;
    put_object_with_backdated_mtime(&client, bucket, survivor_key, b"keep me", backdated).await?;

    // Both objects exist before the lifecycle rule is installed.
    assert!(!object_is_gone(&client, bucket, matched_key).await?);
    assert!(!object_is_gone(&client, bucket, survivor_key).await?);

    put_expiration_config(&client, bucket, expiration_rule("expire-backdated", "expire/", 1)?).await?;

    // Matching, back-dated object is expired by the scanner.
    wait_for_object_expired(&client, bucket, matched_key, StdDuration::from_secs(90)).await?;

    // Negative control: identical back-dating, non-matching prefix -> survives.
    assert!(
        !object_is_gone(&client, bucket, survivor_key).await?,
        "non-matching-prefix object must not be expired by a prefix-scoped rule"
    );

    Ok(())
}

/// `Days=1` current-version expiry on a **versioned** bucket, accelerated with
/// `RUSTFS_ILM_DEBUG_DAY_SECS`. Proves that current-version expiry inserts a
/// delete marker (rather than permanently removing the object): after expiry a
/// plain `GET` returns `NoSuchKey`, `ListObjectVersions` shows a latest delete
/// marker, and the original data version is retained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lifecycle_versioned_current_version_expiry_creates_delete_marker() -> TestResult {
    let mut env = RustFSTestEnvironment::new().await?;
    // One lifecycle "day" == 2s, plus the fast scanner/rounding env.
    let mut extra_env = fast_lifecycle_env();
    extra_env.push(("RUSTFS_ILM_DEBUG_DAY_SECS", "2"));
    env.start_rustfs_server_with_env(vec![], &extra_env).await?;

    let client = env.create_s3_client();
    let bucket = "ilm3-versioned";
    client.create_bucket().bucket(bucket).send().await?;
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

    let key = "versioned/object.txt";
    let put = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"versioned payload"))
        .send()
        .await?;
    let data_version_id = put
        .version_id()
        .map(str::to_string)
        .expect("versioned PUT returns a version id");

    put_expiration_config(&client, bucket, expiration_rule("expire-versioned", "versioned/", 1)?).await?;

    // Current version expiry adds a delete marker; a plain GET now 404s.
    wait_for_object_expired(&client, bucket, key, StdDuration::from_secs(90)).await?;

    // ListObjectVersions: original data version retained, latest is a delete marker.
    let versions = client.list_object_versions().bucket(bucket).prefix(key).send().await?;

    let data_versions = versions.versions();
    assert!(
        data_versions.iter().any(|v| v.version_id() == Some(data_version_id.as_str())),
        "original data version {data_version_id} must be retained, got: {data_versions:?}"
    );

    let delete_markers = versions.delete_markers();
    assert!(
        delete_markers.iter().any(|m| m.is_latest() == Some(true)),
        "expiry on a versioned bucket must create a latest delete marker, got: {delete_markers:?}"
    );

    // The retained data version is still directly readable by version id.
    let by_version = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .version_id(&data_version_id)
        .send()
        .await?;
    assert_eq!(by_version.body.collect().await?.into_bytes().as_ref(), b"versioned payload");

    Ok(())
}

/// `Days=0` (immediate) expiry on an unversioned bucket. This is the simplest
/// expiry path and needs no time control beyond the 1s scanner cycle. Proves the
/// matching-prefix object is deleted and a non-matching object survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lifecycle_zero_day_expiry() -> TestResult {
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &fast_lifecycle_env()).await?;

    let client = env.create_s3_client();
    let bucket = "ilm3-zero-day";
    client.create_bucket().bucket(bucket).send().await?;

    let matched_key = "zero-days/object.txt";
    let survivor_key = "retain/object.txt";
    client
        .put_object()
        .bucket(bucket)
        .key(matched_key)
        .body(ByteStream::from_static(b"expire immediately"))
        .send()
        .await?;
    client
        .put_object()
        .bucket(bucket)
        .key(survivor_key)
        .body(ByteStream::from_static(b"retain me"))
        .send()
        .await?;

    put_expiration_config(&client, bucket, expiration_rule("expire-zero-days", "zero-days/", 0)?).await?;

    wait_for_object_expired(&client, bucket, matched_key, StdDuration::from_secs(90)).await?;

    assert!(
        !object_is_gone(&client, bucket, survivor_key).await?,
        "non-matching-prefix object must survive a prefix-scoped zero-day rule"
    );

    Ok(())
}
