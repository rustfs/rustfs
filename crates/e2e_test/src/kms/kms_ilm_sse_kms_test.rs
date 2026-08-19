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

//! ILM on SSE-KMS buckets while per-key SSE authorization is enforced (backlog#1582).
//!
//! Per-key KMS authorization (`RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true`) scopes the
//! SSE-KMS data path to the requesting principal's `kms:GenerateDataKey` /
//! `kms:Decrypt` grants. Internal callers — the lifecycle scanner's expiry deletes
//! and the tier transition worker's reads — carry no request principal, and
//! `authorize_sse_kms_key` (rustfs/src/storage/sse.rs) exempts a `None` principal
//! so background maintenance keeps working on encrypted buckets.
//!
//! These tests pin that exemption end to end. If enforcement ever starts applying
//! to the scanner's internal operations, expiry stops happening on SSE-KMS buckets
//! and [`ilm_expiration_on_sse_kms_bucket_under_enforcement`] times out; if it
//! starts applying to the transition worker or the read-through path,
//! [`ilm_transition_on_sse_kms_bucket_under_enforcement_reads_back`] fails at the
//! transition wait or the plaintext round-trip.
//!
//! The replication half of the same acceptance item lives in
//! `crates/e2e_test/src/replication_extension_test.rs`
//! (`test_bucket_replication_sse_kms_failure_contract`); ILM had no coverage
//! before this file.
//!
//! Deployment constraint pinned by the transition test's setup: the RustFS warm
//! backend forwards the object's stored `x-amz-server-side-encryption*` metadata
//! as raw headers on the tier data PUT (`build_transition_put_options` +
//! `api_put_object.rs` header mapping), so a RustFS tier target must itself have
//! KMS enabled and hold the named key or it rejects every transition upload with
//! 400 InvalidRequest. That rejection is independent of the enforcement switch;
//! the cold server here therefore runs its own Local KMS with the same key id.

use super::common::{LocalKMSTestEnvironment, create_key_with_specific_id};
use crate::common::{RustFSTestEnvironment, admin_request, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, RestoreRequest,
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule, Transition,
    TransitionStorageClass,
};
use serde::Deserialize;
use std::time::{Duration as StdDuration, Instant};
use tracing::info;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const SSE_KEY: &str = "kms-ilm-sse-key";
const PAYLOAD: &[u8] = b"kms ilm sse payload: survives enforcement, expires and transitions on schedule";

const EXPIRY_BUCKET: &str = "kms-ilm-expiry";
const EXPIRE_KEY: &str = "expire/object.bin";
const SURVIVOR_KEY: &str = "keep/object.bin";

const TIER_NAME: &str = "KMSCOLD";
const TIER_BUCKET: &str = "kms-ilm-cold-tier";
const TIER_PREFIX: &str = "tiered";
const TRANSITION_BUCKET: &str = "kms-ilm-transition";
const TRANSITION_KEY: &str = "tier/object.bin";

/// Generous CI safety net; with a 1s scanner cycle and 2s lifecycle days the
/// terminal state normally lands within a few seconds.
const ILM_DEADLINE: StdDuration = StdDuration::from_secs(90);

/// Start a Local-KMS server with per-key SSE authorization enforced and the
/// lifecycle clock accelerated.
///
/// KMS wiring matches `kms_authorization_negative_matrix_test.rs` (local backend,
/// `--kms-default-key-id`, insecure dev defaults). The lifecycle env matches
/// `reliant/lifecycle.rs::fast_lifecycle_env` plus `RUSTFS_ILM_DEBUG_DAY_SECS=2`,
/// so a `Days=1` rule is due about two seconds after the write.
async fn start_enforcing_ilm_server(env: &mut LocalKMSTestEnvironment) -> TestResult {
    create_key_with_specific_id(&env.kms_keys_dir, SSE_KEY).await?;

    let key_dir = env.kms_keys_dir.clone();
    let args = vec![
        "--kms-enable",
        "--kms-backend",
        "local",
        "--kms-key-dir",
        key_dir.as_str(),
        "--kms-default-key-id",
        SSE_KEY,
    ];

    let envs = [
        ("RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS", "true"),
        ("RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY", "true"),
        ("RUSTFS_SCANNER_CYCLE", "1"),
        ("RUSTFS_ILM_PROCESS_TIME", "1"),
        ("RUSTFS_ILM_DEBUG_DAY_SECS", "2"),
    ];

    env.base_env.start_rustfs_server_with_env(args, &envs).await?;
    Ok(())
}

/// Set the bucket's default encryption to SSE-KMS under [`SSE_KEY`], so plain
/// PUTs (and internal rewrites) are encrypted without per-request SSE headers.
async fn set_bucket_default_sse_kms(client: &Client, bucket: &str) -> TestResult {
    let encryption_config = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::AwsKms)
                        .kms_master_key_id(SSE_KEY)
                        .build()?,
                )
                .build(),
        )
        .build()?;
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;
    Ok(())
}

/// Assert via `HeadObject` that the stored object is SSE-KMS encrypted under
/// [`SSE_KEY`]. Without this, a bucket-default misconfiguration would let the
/// tests pass on an unencrypted object and prove nothing about KMS.
async fn assert_head_sse_kms(client: &Client, bucket: &str, key: &str) -> TestResult {
    let head = client.head_object().bucket(bucket).key(key).send().await?;
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms),
        "{bucket}/{key} must be SSE-KMS encrypted via the bucket default"
    );
    assert_eq!(
        head.ssekms_key_id(),
        Some(SSE_KEY),
        "{bucket}/{key} must be wrapped under the configured KMS key"
    );
    Ok(())
}

/// Returns `true` once `GET bucket/key` fails with `NoSuchKey`, `false` while it
/// still succeeds. Any other error is surfaced. (Copied from
/// `reliant/lifecycle.rs`; that helper is private to the reliant module.)
async fn object_is_gone(client: &Client, bucket: &str, key: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match client.get_object().bucket(bucket).key(key).send().await {
        Ok(output) => {
            output.body.collect().await?;
            Ok(false)
        }
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
async fn wait_for_object_expired(client: &Client, bucket: &str, key: &str, deadline: StdDuration) -> TestResult {
    let start = Instant::now();
    loop {
        if object_is_gone(client, bucket, key).await? {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "object {bucket}/{key} was not expired by the lifecycle scanner within {}s; \
                 SSE key-policy enforcement may have started blocking the scanner's internal deletes",
                deadline.as_secs()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(500)).await;
    }
}

/// Install a prefix-scoped `Days`-based expiration rule.
async fn put_expiration_rule(client: &Client, bucket: &str, id: &str, prefix: &str, days: i32) -> TestResult {
    let rule = LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .expiration(LifecycleExpiration::builder().days(days).build())
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

/// Install a prefix-scoped `Days`-based transition rule targeting [`TIER_NAME`].
async fn put_transition_rule(client: &Client, bucket: &str, id: &str, prefix: &str, days: i32) -> TestResult {
    let rule = LifecycleRule::builder()
        .id(id)
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .transitions(
            Transition::builder()
                .days(days)
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

/// Start a plain Local-KMS server (no enforcement, no lifecycle acceleration)
/// holding [`SSE_KEY`], to serve as the cold tier target.
///
/// The RustFS warm backend forwards the stored SSE-KMS headers on the tier data
/// PUT, so the target re-applies managed SSE-KMS under the named key and must
/// be able to resolve it; without KMS it answers 400 InvalidRequest and the
/// transition can never complete. Enforcement stays off here: the tier writes
/// arrive under `cold`'s root credentials, and one enforcing side is enough to
/// pin the exemption.
async fn start_cold_tier_kms_server(env: &mut LocalKMSTestEnvironment) -> TestResult {
    create_key_with_specific_id(&env.kms_keys_dir, SSE_KEY).await?;

    let key_dir = env.kms_keys_dir.clone();
    let args = vec![
        "--kms-enable",
        "--kms-backend",
        "local",
        "--kms-key-dir",
        key_dir.as_str(),
        "--kms-default-key-id",
        SSE_KEY,
    ];

    env.base_env
        .start_rustfs_server_with_env(args, &[("RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS", "true")])
        .await?;
    Ok(())
}

/// The subset of the manual transition run report these tests assert on.
///
/// Unknown fields are ignored, so this stays compatible with report growth; the
/// full shape is pinned by `reliant/tiering.rs`.
#[derive(Debug, Deserialize)]
struct ManualTransitionRunReport {
    #[serde(default)]
    scanned: u64,
    #[serde(default)]
    enqueued: u64,
    #[serde(default)]
    skipped_already_in_flight: u64,
    #[serde(default)]
    skipped_tier: u64,
}

#[derive(Debug, Deserialize)]
struct ManualTransitionRunResponse {
    state: String,
    report: ManualTransitionRunReport,
}

/// One synchronous (enqueue-only) manual transition run over `bucket/prefix`,
/// via the same admin endpoint `reliant/tiering.rs` drives.
async fn manual_transition_run(
    hot: &RustFSTestEnvironment,
    bucket: &str,
    prefix: &str,
) -> Result<ManualTransitionRunResponse, Box<dyn std::error::Error + Send + Sync>> {
    let bucket = urlencoding::encode(bucket);
    let prefix = urlencoding::encode(prefix);
    let tier = urlencoding::encode(TIER_NAME);
    let path =
        format!("/rustfs/admin/v3/ilm/transition/run?bucket={bucket}&prefix={prefix}&tier={tier}&dryRun=false&maxObjects=10");
    let (status, body) = admin_request(&hot.url, http::Method::POST, &path, None, &hot.access_key, &hot.secret_key).await?;
    if !status.is_success() {
        return Err(format!("manual transition run failed: status={status}, body={body}").into());
    }
    Ok(serde_json::from_str(&body)?)
}

/// Drive manual transition runs until one reports the object as processed.
///
/// The `Days=1` rule becomes due about two seconds after the write
/// (`RUSTFS_ILM_DEBUG_DAY_SECS=2`), so early runs may legitimately report the
/// object as not yet eligible; the loop keeps running the endpoint until it
/// either enqueues the transition, sees it already in flight (the 1s scanner
/// backstop got there first), or finds it already on the tier.
async fn run_manual_transition_until_processed(
    hot: &RustFSTestEnvironment,
    bucket: &str,
    prefix: &str,
    deadline: StdDuration,
) -> TestResult {
    let start = Instant::now();
    loop {
        let run = manual_transition_run(hot, bucket, prefix).await?;
        assert_eq!(run.report.scanned, 1, "manual transition run must scan the object: {run:#?}");
        if run.report.enqueued + run.report.skipped_already_in_flight + run.report.skipped_tier >= 1 {
            info!(state = %run.state, report = ?run.report, "manual transition run processed the SSE-KMS object");
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "manual transition runs never processed {bucket}/{prefix} within {}s; last report: {run:#?}",
                deadline.as_secs()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(500)).await;
    }
}

/// Wire `hot` -> `cold` as a `TierType::RustFS` remote tier via `AddTier`.
///
/// No `force`, so the server runs the real connectivity probe against `cold`
/// (the tier bucket must already exist there). Mirrors
/// `reliant/tiering.rs::add_rustfs_tier`, which is private to that module.
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

    let (status, resp) = admin_request(
        &hot.url,
        http::Method::PUT,
        "/rustfs/admin/v3/tier",
        Some(body),
        &hot.access_key,
        &hot.secret_key,
    )
    .await?;
    if !status.is_success() {
        return Err(format!("AddTier(RustFS) failed: status={status}, body={resp}").into());
    }
    Ok(())
}

/// Poll `HEAD` until the object's storage class is the tier name (transition
/// complete), or fail after `deadline`. (From `reliant/tiering.rs`.)
async fn wait_for_transition(client: &Client, bucket: &str, key: &str, deadline: StdDuration) -> TestResult {
    let start = Instant::now();
    loop {
        let head = client.head_object().bucket(bucket).key(key).send().await?;
        if head.storage_class().map(|sc| sc.as_str()) == Some(TIER_NAME) {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "object {bucket}/{key} was not transitioned to {TIER_NAME} within {}s (storage_class={:?}); \
                 SSE key-policy enforcement may have started blocking the transition worker's internal reads",
                deadline.as_secs(),
                head.storage_class()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(500)).await;
    }
}

/// Poll `HEAD` until `x-amz-restore` reports a finished restore
/// (`ongoing-request="false"`), or fail after `deadline`.
async fn wait_for_restore_complete(client: &Client, bucket: &str, key: &str, deadline: StdDuration) -> TestResult {
    let start = Instant::now();
    loop {
        let head = client.head_object().bucket(bucket).key(key).send().await?;
        if head.restore().is_some_and(|r| r.contains("ongoing-request=\"false\"")) {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(format!(
                "object {bucket}/{key} restore did not complete within {}s (restore={:?}); \
                 SSE key-policy enforcement may have started blocking the restore copy-back's internal reads",
                deadline.as_secs(),
                head.restore()
            )
            .into());
        }
        tokio::time::sleep(StdDuration::from_millis(500)).await;
    }
}

/// ILM expiration keeps working on an SSE-KMS bucket while per-key SSE
/// authorization is enforced.
///
/// The lifecycle scanner deletes expired objects with an internal (no-principal)
/// identity that holds no `kms` grant. If enforcement ever starts applying to
/// those internal deletes (or to the scanner's metadata reads) on encrypted
/// buckets, expiry stops happening and this test times out.
///
/// A survivor object under a non-matching prefix isolates the rule's prefix
/// filter as the cause of the deletion and proves the encrypted bucket stays
/// readable end to end after the scanner has run.
#[tokio::test]
async fn ilm_expiration_on_sse_kms_bucket_under_enforcement() -> TestResult {
    init_logging();

    let mut env = LocalKMSTestEnvironment::new().await?;
    start_enforcing_ilm_server(&mut env).await?;
    env.base_env.create_test_bucket(EXPIRY_BUCKET).await?;

    let client = env.base_env.create_s3_client();
    set_bucket_default_sse_kms(&client, EXPIRY_BUCKET).await?;

    for key in [EXPIRE_KEY, SURVIVOR_KEY] {
        client
            .put_object()
            .bucket(EXPIRY_BUCKET)
            .key(key)
            .body(ByteStream::from_static(PAYLOAD))
            .send()
            .await?;
        assert_head_sse_kms(&client, EXPIRY_BUCKET, key).await?;
    }
    info!("both objects stored SSE-KMS encrypted under enforcement");

    put_expiration_rule(&client, EXPIRY_BUCKET, "kms-ilm-expire", "expire/", 1).await?;

    // The regression this pins: the scanner's internal delete must stay exempt
    // from per-key SSE authorization, so the encrypted object actually expires.
    wait_for_object_expired(&client, EXPIRY_BUCKET, EXPIRE_KEY, ILM_DEADLINE).await?;
    info!("SSE-KMS object expired by the lifecycle scanner under enforcement");

    // Negative control: same bucket, same encryption, non-matching prefix. It
    // must survive the scanner and still decrypt for the requesting principal.
    assert!(
        !object_is_gone(&client, EXPIRY_BUCKET, SURVIVOR_KEY).await?,
        "non-matching-prefix object must not be expired by a prefix-scoped rule"
    );
    let survivor = client.get_object().bucket(EXPIRY_BUCKET).key(SURVIVOR_KEY).send().await?;
    assert_eq!(
        survivor.body.collect().await?.into_bytes().as_ref(),
        PAYLOAD,
        "surviving SSE-KMS object must still decrypt after the scanner has run"
    );

    Ok(())
}

/// ILM transition to a remote tier keeps working on an SSE-KMS bucket while
/// per-key SSE authorization is enforced, and the transitioned object reads
/// back as plaintext.
///
/// The transition worker moves the stored (encrypted) bytes to the cold tier
/// with an internal (no-principal) identity; the read-through `GET` then
/// decrypts the envelope for the requesting principal. If enforcement ever
/// starts applying to the worker's internal reads, the transition wait times
/// out; if the stored envelope is mishandled across the tier round trip, the
/// plaintext comparison fails.
///
/// The transition is driven through the manual transition-run admin endpoint
/// (the mechanism `reliant/tiering.rs` established), so the test does not
/// depend on scanner scheduling; the 1s scanner cycle stays on as a backstop.
#[tokio::test]
async fn ilm_transition_on_sse_kms_bucket_under_enforcement_reads_back() -> TestResult {
    init_logging();

    // Cold-tier server: independent credentials, its own Local KMS holding the
    // same key id (see the module docs for why the tier target needs KMS).
    // Started first; each server's startup cleanup only matches its own unique
    // address and temp dir, so the two instances coexist.
    let mut cold = LocalKMSTestEnvironment::new().await?;
    cold.base_env.access_key = "kmscoldtieradmin".to_string();
    cold.base_env.secret_key = "kmscoldtiersecret".to_string();
    start_cold_tier_kms_server(&mut cold).await?;
    let cold_client = cold.base_env.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    // Hot server: Local KMS + enforcement + accelerated lifecycle clock.
    let mut env = LocalKMSTestEnvironment::new().await?;
    start_enforcing_ilm_server(&mut env).await?;
    let hot_client = env.base_env.create_s3_client();

    add_rustfs_tier(&env.base_env, &cold.base_env).await?;

    env.base_env.create_test_bucket(TRANSITION_BUCKET).await?;
    set_bucket_default_sse_kms(&hot_client, TRANSITION_BUCKET).await?;

    hot_client
        .put_object()
        .bucket(TRANSITION_BUCKET)
        .key(TRANSITION_KEY)
        .body(ByteStream::from_static(PAYLOAD))
        .send()
        .await?;
    assert_head_sse_kms(&hot_client, TRANSITION_BUCKET, TRANSITION_KEY).await?;
    info!("object stored SSE-KMS encrypted under enforcement");

    // Days=1 is due ~2s after the write with RUSTFS_ILM_DEBUG_DAY_SECS=2.
    put_transition_rule(&hot_client, TRANSITION_BUCKET, "kms-ilm-transition", "tier/", 1).await?;

    // Drive the transition deterministically via the manual run endpoint, then
    // wait for HEAD to report the tier as the object's storage class.
    run_manual_transition_until_processed(&env.base_env, TRANSITION_BUCKET, "tier/", ILM_DEADLINE).await?;
    wait_for_transition(&hot_client, TRANSITION_BUCKET, TRANSITION_KEY, ILM_DEADLINE).await?;
    info!("SSE-KMS object transitioned to the remote tier under enforcement");

    let head = hot_client
        .head_object()
        .bucket(TRANSITION_BUCKET)
        .key(TRANSITION_KEY)
        .send()
        .await?;
    assert!(
        head.restore().is_none(),
        "a freshly transitioned object must not advertise x-amz-restore, got {:?}",
        head.restore()
    );

    // The remote copy exists on the cold tier. The payload the tier holds is the
    // hot server's stored ciphertext, wrapped once more under the cold server's
    // own managed SSE-KMS layer (the forwarded headers re-request encryption).
    let remote = cold_client.list_objects_v2().bucket(TIER_BUCKET).send().await?;
    assert!(!remote.contents().is_empty(), "cold-tier bucket must hold the transitioned object's data");

    // Read-through GET under enforcement must succeed (not AccessDenied) and
    // keep advertising SSE-KMS. Its BODY is deliberately not compared here:
    // the transitioned read path skips managed-SSE decryption — a product gap
    // unrelated to enforcement — so a direct GET streams the stored ciphertext
    // (`new_getobjectreader` in crates/ecstore/src/client/object_api_utils.rs
    // hardcodes `is_encrypted = false` and never applies the
    // `ReadTransform::Encrypted` wrapping the hot-read path builds in
    // crates/ecstore/src/object_api/readers.rs). Plaintext recovery is pinned
    // through restore semantics below; when the read-through gap is fixed, a
    // byte assertion can be added here too.
    let read_through = hot_client
        .get_object()
        .bucket(TRANSITION_BUCKET)
        .key(TRANSITION_KEY)
        .send()
        .await?;
    assert_eq!(
        read_through.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms),
        "transitioned object must still report SSE-KMS on read-through"
    );
    let read_through_body = read_through.body.collect().await?.into_bytes();
    assert_eq!(
        read_through_body.len(),
        PAYLOAD.len(),
        "read-through GET must stream the object's full logical size under enforcement"
    );

    // RestoreObject copies the ciphertext back from the tier under the original
    // envelope metadata; the restored copy is then served by the normal
    // decrypting read path. The copy-back runs with an internal (no-principal)
    // identity, so this also pins the exemption on the restore path. Days=300
    // because RUSTFS_ILM_DEBUG_DAY_SECS=2 accelerates the restored copy's
    // expiry as well (300 accelerated days == 600s of validity).
    hot_client
        .restore_object()
        .bucket(TRANSITION_BUCKET)
        .key(TRANSITION_KEY)
        .restore_request(RestoreRequest::builder().days(300).build())
        .send()
        .await?;
    wait_for_restore_complete(&hot_client, TRANSITION_BUCKET, TRANSITION_KEY, ILM_DEADLINE).await?;
    info!("SSE-KMS object restored from the remote tier under enforcement");

    // The KMS-relevant half: the restored envelope decrypts back to the exact
    // plaintext for the requesting principal.
    let restored = hot_client
        .get_object()
        .bucket(TRANSITION_BUCKET)
        .key(TRANSITION_KEY)
        .send()
        .await?;
    assert_eq!(
        restored.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms),
        "restored object must still report SSE-KMS"
    );
    let body = restored.body.collect().await?.into_bytes();
    assert_eq!(body.as_ref(), PAYLOAD, "restored SSE-KMS object must round-trip byte-identical plaintext");

    Ok(())
}
