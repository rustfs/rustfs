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

use crate::common::{
    RustFSTestClusterEnvironment, RustFSTestEnvironment, admin_request, init_logging, replication_fast_env, rustfs_binary_path,
};
use crate::fake_s3_target::{FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3Target};
use crate::replication_extension_test::{
    LOOPBACK_REPLICATION_TARGET_ENV, ReplicationTargetOptions, put_bucket_replication, set_replication_target_with_options,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, DefaultRetention,
    ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, ObjectLockConfiguration, ObjectLockEnabled,
    ObjectLockRetentionMode, ObjectLockRule, PublicAccessBlockConfiguration, ServerSideEncryption, ServerSideEncryptionByDefault,
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule, Tag, Tagging, VersioningConfiguration,
};
use http::{Method, StatusCode};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

const SOURCE_BINARY_ENV: &str = "RUSTFS_UPGRADE_SOURCE_BINARY";
const SSE_MASTER_KEY_ENV: &str = "RUSTFS_SSE_S3_MASTER_KEY";
const SSE_MASTER_KEY: &str = "QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI=";
const PLAIN_BUCKET: &str = "upgrade-plain-data";
const VERSIONED_BUCKET: &str = "upgrade-versioned-data";
const MIXED_BUCKET: &str = "upgrade-mixed-version-data";
const MIXED_NODE_COUNT: usize = 4;
const MULTIPART_WORKERS: usize = 16;
const MULTIPART_UPLOADS_PER_WORKER: usize = 16;
// Peers keep a restarted node's drive in Suspect/Returning for roughly
// probe_interval (2s) x success_threshold (3) after it comes back; 30s
// comfortably covers that window plus CI scheduling jitter.
const LISTING_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

// Bucket-configuration upgrade/rollback scenarios (rustfs#7172, #7183, #7089).
const CONFIG_PLAIN_BUCKET: &str = "upgrade-config-plain";
const CONFIG_ENCRYPTED_BUCKET: &str = "upgrade-config-encrypted";
const CONFIG_REPLICATED_BUCKET: &str = "upgrade-config-replicated";
const CONFIG_LOCKED_BUCKET: &str = "upgrade-config-locked";
const CONFIG_REPLICA_BUCKET: &str = "upgrade-config-replica";
const ROLLBACK_BUCKET: &str = "rollback-config-data";
const ROLLBACK_REPLICA_BUCKET: &str = "rollback-config-replica";
const BUCKET_QUOTA_BYTES: u64 = 64 * 1024 * 1024;
const LIFECYCLE_RULE_ID: &str = "upgrade-expire-logs";
const LIFECYCLE_PREFIX: &str = "logs/";
const LIFECYCLE_DAYS: i32 = 30;
const BUCKET_TAG_KEY: &str = "owner";
const BUCKET_TAG_VALUE: &str = "upgrade-compatibility";
const OBJECT_LOCK_DAYS: i32 = 1;
// `set-bucket-quota` answers 503 until the scanner has made the bucket's usage
// authoritative; the quota test uses the same 30s budget.
const QUOTA_READINESS_TIMEOUT: Duration = Duration::from_secs(30);
// Quota admission fails closed while a freshly started server has neither
// authoritative usage nor a persisted degraded baseline for the bucket
// (rustfs#5716), so a write to a quota-enabled bucket is retryable-503 for that
// window. It is a restart property, not an upgrade property — the same window
// opens on the very first start — so the write assertions ride it out instead
// of treating it as an upgrade failure.
const QUOTA_ADMISSION_WARMUP_TIMEOUT: Duration = Duration::from_secs(90);

fn source_binary() -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let path = std::env::var_os(SOURCE_BINARY_ENV)
        .map(PathBuf::from)
        .ok_or("RUSTFS_UPGRADE_SOURCE_BINARY must point to the pinned previous release binary")?;
    if !path.is_file() {
        return Err(format!("upgrade source binary does not exist: {}", path.display()).into());
    }
    Ok(path)
}

async fn enable_versioning(client: &Client, bucket: &str) -> TestResult {
    let configuration = VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .build();
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(configuration)
        .send()
        .await?;
    Ok(())
}

async fn read_object(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(Option<ServerSideEncryption>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let mut request = client.get_object().bucket(bucket).key(key);
    if let Some(version_id) = version_id {
        request = request.version_id(version_id);
    }
    let response = request.send().await?;
    let encryption = response.server_side_encryption().cloned();
    let body = response.body.collect().await?.into_bytes().to_vec();
    Ok((encryption, body))
}

async fn write_multipart(client: &Client, bucket: &str, key: &str, parts: &[Vec<u8>]) -> TestResult {
    let created = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = created.upload_id().ok_or("CreateMultipartUpload omitted upload ID")?;
    let mut completed_parts = Vec::with_capacity(parts.len());

    for (index, part) in parts.iter().enumerate() {
        let part_number = i32::try_from(index + 1)?;
        let uploaded = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(part.clone()))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().ok_or("UploadPart omitted ETag")?)
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;
    Ok(())
}

fn configure_cluster_logs(cluster: &mut RustFSTestClusterEnvironment) -> TestResult {
    let Some(log_dir) = std::env::var_os("RUSTFS_E2E_LOG_DIR") else {
        return Ok(());
    };
    std::fs::create_dir_all(&log_dir)?;
    for node_idx in 0..cluster.nodes.len() {
        let path = Path::new(&log_dir).join(format!("mixed-upgrade-node-{node_idx}.log"));
        cluster.set_node_capture_log_path(node_idx, path.to_string_lossy().into_owned())?;
    }
    Ok(())
}

async fn write_multipart_load(clients: &[Client], phase: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut tasks = JoinSet::new();
    for worker in 0..MULTIPART_WORKERS {
        let client = clients[worker % clients.len()].clone();
        let phase = phase.to_string();
        tasks.spawn(async move {
            let mut keys = Vec::with_capacity(MULTIPART_UPLOADS_PER_WORKER);
            for upload in 0..MULTIPART_UPLOADS_PER_WORKER {
                let key = format!("{phase}/multipart/{worker:02}/{upload:02}");
                let part = vec![u8::try_from(worker)?; 64 * 1024];
                write_multipart(&client, MIXED_BUCKET, &key, &[part]).await?;
                keys.push(key);
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(keys)
        });
    }

    let mut keys = Vec::with_capacity(MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER);
    while let Some(result) = tasks.join_next().await {
        keys.extend(result??);
    }
    Ok(keys)
}

/// Assert that `client` eventually lists exactly `expected` objects under
/// `{phase}/`, polling until [`LISTING_CONVERGENCE_TIMEOUT`].
///
/// A single-snapshot assertion here is racy by construction: each phase both
/// writes and lists within seconds of a node restart. While a peer still holds
/// the restarted node's drive in Suspect/Returning, strict-quorum listing
/// consults only the remaining three drives and drops any object that was
/// itself legally written at write quorum (3/4 drives) during an earlier
/// node's identical post-restart window — its xl.meta is then visible on only
/// two of the three consulted drives, below the required object quorum of
/// three. GET still succeeds for such objects; only the listing under-counts
/// until drive health converges. A genuine upgrade data-loss regression still
/// fails after the deadline.
async fn wait_for_phase_listing(client: &Client, phase: &str, expected: usize, context: &str) -> TestResult {
    let deadline = Instant::now() + LISTING_CONVERGENCE_TIMEOUT;
    loop {
        let listed = client
            .list_objects_v2()
            .bucket(MIXED_BUCKET)
            .prefix(format!("{phase}/"))
            .send()
            .await?;
        let count = listed.contents().len();
        if count == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{context}: listing under {phase}/ returned {count} of {expected} objects even after {}s of post-restart convergence",
                LISTING_CONVERGENCE_TIMEOUT.as_secs()
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn exercise_mixed_cluster(
    cluster: &RustFSTestClusterEnvironment,
    phase: &str,
    current_node: usize,
    previous_node: usize,
) -> TestResult {
    let clients = cluster.create_all_clients()?;
    let current_client = &clients[current_node];
    let previous_client = &clients[previous_node];

    let current_key = format!("{phase}/written-by-current");
    let current_body = format!("{phase}: current RustFS build").into_bytes();
    current_client
        .put_object()
        .bucket(MIXED_BUCKET)
        .key(&current_key)
        .body(ByteStream::from(current_body.clone()))
        .send()
        .await?;
    assert_eq!(read_object(previous_client, MIXED_BUCKET, &current_key, None).await?.1, current_body);

    let previous_key = format!("{phase}/written-by-previous");
    let previous_body = format!("{phase}: previous RustFS release").into_bytes();
    previous_client
        .put_object()
        .bucket(MIXED_BUCKET)
        .key(&previous_key)
        .body(ByteStream::from(previous_body.clone()))
        .send()
        .await?;
    assert_eq!(read_object(current_client, MIXED_BUCKET, &previous_key, None).await?.1, previous_body);

    let multipart_keys = write_multipart_load(&clients, phase).await?;
    let expected_count = multipart_keys.len() + 2;
    for (label, client) in [("current", current_client), ("previous", previous_client)] {
        wait_for_phase_listing(
            client,
            phase,
            expected_count,
            &format!("the {label} RustFS version must stream the complete mixed-version listing"),
        )
        .await?;
    }

    let last_multipart_key = format!("{phase}/multipart/{:02}/{:02}", MULTIPART_WORKERS - 1, MULTIPART_UPLOADS_PER_WORKER - 1);
    assert_eq!(
        read_object(previous_client, MIXED_BUCKET, &last_multipart_key, None).await?.1,
        vec![u8::try_from(MULTIPART_WORKERS - 1)?; 64 * 1024]
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn direct_upgrade_from_rc2_preserves_object_contracts() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let mut env = RustFSTestEnvironment::new().await?;
    let server_env = [(SSE_MASTER_KEY_ENV, SSE_MASTER_KEY)];
    env.start_rustfs_server_from_binary(&previous_binary, vec![], &server_env)
        .await?;

    let old_client = env.create_s3_client();
    env.create_test_bucket(PLAIN_BUCKET).await?;
    env.create_test_bucket(VERSIONED_BUCKET).await?;
    enable_versioning(&old_client, VERSIONED_BUCKET).await?;

    let plain_key = "plain-object";
    let plain_bytes = b"written by the previous RustFS release";
    old_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(plain_key)
        .body(ByteStream::from_static(plain_bytes))
        .send()
        .await?;

    let encrypted_key = "sse-s3-object";
    let encrypted_bytes = b"encrypted by the previous RustFS release";
    old_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(encrypted_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(encrypted_bytes))
        .send()
        .await?;

    let multipart_key = "multipart-object";
    let multipart_parts = vec![vec![b'a'; 5 * 1024 * 1024], b"final multipart bytes".to_vec()];
    let multipart_bytes = multipart_parts.concat();
    write_multipart(&old_client, PLAIN_BUCKET, multipart_key, &multipart_parts).await?;

    let versioned_key = "versioned-object";
    let version1_bytes = b"version one from the previous release";
    let version1 = old_client
        .put_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(version1_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("first versioned PUT omitted version ID")?
        .to_string();
    let version2_bytes = b"version two from the previous release";
    let version2 = old_client
        .put_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(version2_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("second versioned PUT omitted version ID")?
        .to_string();
    let deleted = old_client
        .delete_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .send()
        .await?;
    assert_eq!(deleted.delete_marker(), Some(true));
    let delete_marker = deleted
        .version_id()
        .ok_or("versioned DELETE omitted delete marker version ID")?
        .to_string();

    env.restart_server_preserving_data(vec![], &server_env).await?;
    let current_client = env.create_s3_client();

    assert_eq!(read_object(&current_client, PLAIN_BUCKET, plain_key, None).await?.1, plain_bytes);

    let (encryption, upgraded_encrypted_bytes) = read_object(&current_client, PLAIN_BUCKET, encrypted_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(upgraded_encrypted_bytes, encrypted_bytes);

    assert_eq!(read_object(&current_client, PLAIN_BUCKET, multipart_key, None).await?.1, multipart_bytes);

    assert_eq!(
        read_object(&current_client, VERSIONED_BUCKET, versioned_key, Some(&version1))
            .await?
            .1,
        version1_bytes
    );
    assert_eq!(
        read_object(&current_client, VERSIONED_BUCKET, versioned_key, Some(&version2))
            .await?
            .1,
        version2_bytes
    );

    let current_read = current_client
        .get_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .send()
        .await
        .expect_err("the previous release's delete marker must remain current after upgrade");
    assert_eq!(current_read.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(current_read.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));

    let listed = current_client
        .list_object_versions()
        .bucket(VERSIONED_BUCKET)
        .prefix(versioned_key)
        .send()
        .await?;
    assert_eq!(listed.versions().len(), 2);
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(version1.as_str()))
    );
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(version2.as_str()))
    );
    assert_eq!(listed.delete_markers().len(), 1);
    assert_eq!(listed.delete_markers()[0].version_id(), Some(delete_marker.as_str()));
    assert_eq!(listed.delete_markers()[0].is_latest(), Some(true));

    let post_upgrade_key = "written-after-upgrade";
    let post_upgrade_bytes = b"written by the current RustFS build";
    current_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(post_upgrade_key)
        .body(ByteStream::from_static(post_upgrade_bytes))
        .send()
        .await?;
    assert_eq!(
        read_object(&current_client, PLAIN_BUCKET, post_upgrade_key, None).await?.1,
        post_upgrade_bytes
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn rolling_upgrade_from_rc2_preserves_mixed_version_contracts() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let current_binary = rustfs_binary_path();
    let mut cluster = RustFSTestClusterEnvironment::new(MIXED_NODE_COUNT).await?;
    cluster.set_env("RUST_LOG", "rustfs=warn,rustfs_notify=warn");
    configure_cluster_logs(&mut cluster)?;
    cluster.start_with_binary(&previous_binary).await?;
    cluster.create_test_bucket(MIXED_BUCKET).await?;

    cluster.stop_node(0)?;
    cluster.start_node_from_binary(0, &current_binary).await?;
    exercise_mixed_cluster(&cluster, "one-current-node", 0, 1).await?;

    for node_idx in [1, 2] {
        cluster.stop_node(node_idx)?;
        cluster.start_node_from_binary(node_idx, &current_binary).await?;
    }
    exercise_mixed_cluster(&cluster, "one-previous-node", 0, 3).await?;

    cluster.stop_node(3)?;
    cluster.start_node_from_binary(3, &current_binary).await?;

    for (node_idx, client) in cluster.create_all_clients()?.iter().enumerate() {
        for phase in ["one-current-node", "one-previous-node"] {
            wait_for_phase_listing(
                client,
                phase,
                MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER + 2,
                &format!("node {node_idx}: the homogeneous current cluster must preserve every object"),
            )
            .await?;
        }
    }

    Ok(())
}

/// Child-process environment shared by both bucket-configuration scenarios.
///
/// The replication target is an in-process fake bound to `127.0.0.1`, which
/// `set-remote-target` rejects as an SSRF risk without the loopback opt-in, and
/// the proxy bypass keeps a developer's `HTTP_PROXY` from intercepting the
/// server's outbound health check.
fn bucket_config_server_env() -> Vec<(&'static str, &'static str)> {
    let mut env = vec![
        (SSE_MASTER_KEY_ENV, SSE_MASTER_KEY),
        ("NO_PROXY", "127.0.0.1,localhost"),
        ("HTTP_PROXY", ""),
        ("HTTPS_PROXY", ""),
        // Shorten the scanner cycle so the bucket's usage becomes authoritative
        // in seconds; both `set-bucket-quota` and quota admission block on it.
        ("RUSTFS_SCANNER_CYCLE", "1"),
        ("RUSTFS_SCANNER_START_DELAY_SECS", "0"),
    ];
    env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env.extend(replication_fast_env());
    env
}

/// Restart `env` in place on the same data directory using an explicit binary.
///
/// [`RustFSTestEnvironment::restart_server_preserving_data`] always relaunches
/// the workspace build, which is the upgrade direction only. The rollback
/// scenario needs the reverse: stop the current build and bring the pinned
/// previous release up on the metadata that build just wrote.
async fn restart_from_binary(env: &mut RustFSTestEnvironment, binary: &Path, server_env: &[(&str, &str)]) -> TestResult {
    env.stop_server();
    env.start_rustfs_server_from_binary(binary, vec![], server_env).await
}

async fn set_bucket_quota(env: &RustFSTestEnvironment, bucket: &str, quota_bytes: u64) -> TestResult {
    let path = format!("/rustfs/admin/v3/quota/{bucket}");
    let body = serde_json::json!({ "quota": quota_bytes, "quota_type": "HARD" }).to_string();
    let deadline = Instant::now() + QUOTA_READINESS_TIMEOUT;
    loop {
        let (status, response) =
            admin_request(&env.url, Method::PUT, &path, Some(body.clone()), &env.access_key, &env.secret_key).await?;
        if status.is_success() {
            return Ok(());
        }
        if status != StatusCode::SERVICE_UNAVAILABLE || Instant::now() >= deadline {
            return Err(format!("setting the quota of {bucket} failed: {status} {response}").into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// PUT into a quota-enabled bucket, riding out the post-start quota-admission
/// warm-up described on [`QUOTA_ADMISSION_WARMUP_TIMEOUT`].
///
/// Only `ServiceUnavailable` is retried: any other failure, and a warm-up that
/// never ends, is a genuine regression and surfaces as an error.
async fn put_object_through_quota_warmup(client: &Client, bucket: &str, key: &str, body: &'static [u8]) -> TestResult {
    let deadline = Instant::now() + QUOTA_ADMISSION_WARMUP_TIMEOUT;
    loop {
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await;
        let error = match result {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };
        let retryable = error.as_service_error().and_then(ProvideErrorMetadata::code) == Some("ServiceUnavailable");
        if !retryable || Instant::now() >= deadline {
            return Err(format!("PUT {bucket}/{key} failed after the quota warm-up window: {error}").into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn get_bucket_quota(env: &RustFSTestEnvironment, bucket: &str) -> Result<Option<u64>, BoxError> {
    let path = format!("/rustfs/admin/v3/quota/{bucket}");
    let (status, response) = admin_request(&env.url, Method::GET, &path, None, &env.access_key, &env.secret_key).await?;
    if status != StatusCode::OK {
        return Err(format!("reading the quota of {bucket} failed: {status} {response}").into());
    }
    let quota: serde_json::Value = serde_json::from_str(&response)?;
    Ok(quota.get("quota").and_then(serde_json::Value::as_u64))
}

/// `GET /rustfs/admin/v3/list-remote-targets?bucket=...`.
///
/// Returns an error for any non-200, because rustfs#7172 made this endpoint
/// fail closed on a `bucket-targets.json` blob the running build cannot parse.
/// An upgrade that misreads a blob written by the previous release therefore
/// shows up here as an error, and a silently dropped target shows up as an
/// empty list — the caller must distinguish the two.
async fn list_remote_targets(env: &RustFSTestEnvironment, bucket: &str) -> Result<Vec<serde_json::Value>, BoxError> {
    let path = format!("/rustfs/admin/v3/list-remote-targets?bucket={}", urlencoding::encode(bucket));
    let (status, response) = admin_request(&env.url, Method::GET, &path, None, &env.access_key, &env.secret_key).await?;
    if status != StatusCode::OK {
        return Err(format!("list-remote-targets for {bucket} failed: {status} {response}").into());
    }
    Ok(serde_json::from_str(&response)?)
}

/// Assert that `bucket` still carries exactly the replication target `arn`.
async fn assert_remote_target_preserved(env: &RustFSTestEnvironment, bucket: &str, arn: &str, context: &str) -> TestResult {
    let targets = list_remote_targets(env, bucket).await?;
    assert_eq!(
        targets.len(),
        1,
        "{context}: list-remote-targets must still report the single configured target, got {targets:?}"
    );
    assert_eq!(
        targets[0].get("arn").and_then(serde_json::Value::as_str),
        Some(arn),
        "{context}: the target ARN changed across the restart: {targets:?}"
    );
    Ok(())
}

/// Configure a replication target on `bucket` pointing at the in-process fake,
/// then attach an enabled replication rule for it. Returns the target ARN.
async fn configure_replication(
    env: &RustFSTestEnvironment,
    bucket: &str,
    target: &FakeS3Target,
    target_bucket: &str,
) -> Result<String, BoxError> {
    let arn = set_replication_target_with_options(
        env,
        bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(env, bucket, &arn).await?;
    Ok(arn)
}

async fn put_default_sse_s3_encryption(client: &Client, bucket: &str) -> TestResult {
    let configuration = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::Aes256)
                        .build()?,
                )
                .build(),
        )
        .build()?;
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(configuration)
        .send()
        .await?;
    Ok(())
}

async fn assert_default_sse_s3_encryption(client: &Client, bucket: &str, context: &str) -> TestResult {
    let response = client.get_bucket_encryption().bucket(bucket).send().await?;
    let rules = response
        .server_side_encryption_configuration()
        .ok_or("GetBucketEncryption omitted the configuration")?
        .rules();
    assert_eq!(rules.len(), 1, "{context}: expected exactly one encryption rule, got {rules:?}");
    assert_eq!(
        rules[0]
            .apply_server_side_encryption_by_default()
            .map(ServerSideEncryptionByDefault::sse_algorithm),
        Some(&ServerSideEncryption::Aes256),
        "{context}: the default encryption algorithm changed"
    );
    Ok(())
}

async fn put_bucket_tag(client: &Client, bucket: &str) -> TestResult {
    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key(BUCKET_TAG_KEY).value(BUCKET_TAG_VALUE).build()?)
        .build()?;
    client.put_bucket_tagging().bucket(bucket).tagging(tagging).send().await?;
    Ok(())
}

async fn assert_bucket_tag(client: &Client, bucket: &str, context: &str) -> TestResult {
    let tags = client.get_bucket_tagging().bucket(bucket).send().await?;
    let tag_set = tags.tag_set();
    assert_eq!(tag_set.len(), 1, "{context}: expected exactly one bucket tag, got {tag_set:?}");
    assert_eq!(tag_set[0].key(), BUCKET_TAG_KEY, "{context}: bucket tag key changed");
    assert_eq!(tag_set[0].value(), BUCKET_TAG_VALUE, "{context}: bucket tag value changed");
    Ok(())
}

async fn assert_versioning_enabled(client: &Client, bucket: &str, context: &str) -> TestResult {
    let versioning = client.get_bucket_versioning().bucket(bucket).send().await?;
    assert_eq!(
        versioning.status(),
        Some(&BucketVersioningStatus::Enabled),
        "{context}: versioning is no longer Enabled on {bucket}"
    );
    Ok(())
}

fn bucket_policy_document(bucket: &str) -> serde_json::Value {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "UpgradePublicRead",
            "Effect": "Allow",
            "Principal": { "AWS": ["*"] },
            "Action": ["s3:GetObject"],
            "Resource": [format!("arn:aws:s3:::{bucket}/public/*")]
        }]
    })
}

/// `GET .../on-demand-migration/{bucket}/status`.
///
/// The migration module defaults on from rustfs#7089, so a bucket that never
/// configured a source must still answer `configured: false` rather than
/// engaging the migration path.
async fn assert_migration_not_configured(env: &RustFSTestEnvironment, bucket: &str) -> TestResult {
    let path = format!("/rustfs/admin/v3/on-demand-migration/{bucket}/status");
    let (status, response) = admin_request(&env.url, Method::GET, &path, None, &env.access_key, &env.secret_key).await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "the migration status endpoint must answer for an unconfigured bucket: {status} {response}"
    );
    let body: serde_json::Value = serde_json::from_str(&response)?;
    assert_eq!(
        body.get("configured"),
        Some(&serde_json::Value::Bool(false)),
        "a bucket upgraded from the previous release must not look migration-configured: {body}"
    );
    Ok(())
}

/// A GET for a key that was never written must be a plain `NoSuchKey`.
///
/// With the migration module on by default this is the cheap proof that an
/// unconfigured bucket never consults a source: any migration engagement would
/// surface as a different status or error code here.
async fn assert_missing_key_is_no_such_key(client: &Client, bucket: &str, key: &str) -> TestResult {
    let error = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err("a key that was never written must not be readable");
    assert_eq!(
        error.raw_response().map(|response| response.status().as_u16()),
        Some(404),
        "a missing key must stay a 404 on a bucket with no migration configuration"
    );
    assert_eq!(
        error.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("NoSuchKey"),
        "a missing key must stay NoSuchKey on a bucket with no migration configuration"
    );
    Ok(())
}

/// Bucket configuration written by the pinned previous release must survive an
/// upgrade to the current build unchanged, and must keep working.
///
/// This pins the three on-disk surfaces the on-demand-migration series moved:
///
/// * `BucketMetadata` grew two msgpack keys (encoded map length 44 -> 46), so
///   every configuration read below decodes a 44-key blob on 46-key code.
/// * rustfs#7172 made an unreadable `bucket-targets.json` / encryption /
///   public-access-block / quota blob "present but unreadable" instead of
///   silently defaulting, and made `list-remote-targets` fail closed on it. A
///   replication target configured by the old release must therefore still be
///   *listed*, not dropped and not an error.
/// * rustfs#7183 made the object write path refuse a PUT when the bucket's
///   encryption configuration cannot be read, so a misparsed SSE config would
///   turn every PUT to that bucket into a 500.
///
/// Not covered on purpose: on-demand-migration configuration itself, which the
/// previous release has no public API for — the reverse direction is asserted
/// instead (an upgraded bucket reports `configured: false`).
#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn direct_upgrade_from_previous_release_preserves_bucket_configuration() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;

    // In-process: the fake target outlives both server processes, so the
    // replication target stays reachable across the upgrade.
    let replication_target = FakeS3Target::start().await?;
    replication_target.create_bucket(CONFIG_REPLICA_BUCKET);

    let mut env = RustFSTestEnvironment::new().await?;
    let server_env = bucket_config_server_env();
    env.start_rustfs_server_from_binary(&previous_binary, vec![], &server_env)
        .await?;
    let old_client = env.create_s3_client();

    env.create_test_bucket(CONFIG_PLAIN_BUCKET).await?;
    env.create_test_bucket(CONFIG_ENCRYPTED_BUCKET).await?;
    env.create_test_bucket(CONFIG_REPLICATED_BUCKET).await?;
    old_client
        .create_bucket()
        .bucket(CONFIG_LOCKED_BUCKET)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;

    // Plain bucket: policy, tags, lifecycle, quota.
    let policy = bucket_policy_document(CONFIG_PLAIN_BUCKET);
    old_client
        .put_bucket_policy()
        .bucket(CONFIG_PLAIN_BUCKET)
        .policy(policy.to_string())
        .send()
        .await?;
    put_bucket_tag(&old_client, CONFIG_PLAIN_BUCKET).await?;
    old_client
        .put_bucket_lifecycle_configuration()
        .bucket(CONFIG_PLAIN_BUCKET)
        .lifecycle_configuration(
            BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id(LIFECYCLE_RULE_ID)
                        .status(ExpirationStatus::Enabled)
                        .filter(LifecycleRuleFilter::builder().prefix(LIFECYCLE_PREFIX).build())
                        .expiration(LifecycleExpiration::builder().days(LIFECYCLE_DAYS).build())
                        .build()?,
                )
                .build()?,
        )
        .send()
        .await?;
    set_bucket_quota(&env, CONFIG_PLAIN_BUCKET, BUCKET_QUOTA_BYTES).await?;

    // Encrypted bucket: SSE-S3 default encryption plus a fully restrictive
    // public access block, both of which rustfs#7172 now fails closed on.
    put_default_sse_s3_encryption(&old_client, CONFIG_ENCRYPTED_BUCKET).await?;
    old_client
        .put_public_access_block()
        .bucket(CONFIG_ENCRYPTED_BUCKET)
        .public_access_block_configuration(
            PublicAccessBlockConfiguration::builder()
                .block_public_acls(true)
                .ignore_public_acls(true)
                .block_public_policy(true)
                .restrict_public_buckets(true)
                .build(),
        )
        .send()
        .await?;

    // Replicated bucket: versioning, a validated remote target, a rule.
    enable_versioning(&old_client, CONFIG_REPLICATED_BUCKET).await?;
    let target_arn = configure_replication(&env, CONFIG_REPLICATED_BUCKET, &replication_target, CONFIG_REPLICA_BUCKET).await?;
    assert_remote_target_preserved(&env, CONFIG_REPLICATED_BUCKET, &target_arn, "before the upgrade").await?;

    // Object-lock bucket: a default GOVERNANCE retention on a fresh bucket.
    old_client
        .put_object_lock_configuration()
        .bucket(CONFIG_LOCKED_BUCKET)
        .object_lock_configuration(
            ObjectLockConfiguration::builder()
                .object_lock_enabled(ObjectLockEnabled::Enabled)
                .rule(
                    ObjectLockRule::builder()
                        .default_retention(
                            DefaultRetention::builder()
                                .mode(ObjectLockRetentionMode::Governance)
                                .days(OBJECT_LOCK_DAYS)
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;

    let plain_key = "plain/written-by-previous";
    let plain_bytes = b"plain object written by the previous RustFS release";
    put_object_through_quota_warmup(&old_client, CONFIG_PLAIN_BUCKET, plain_key, plain_bytes).await?;

    let encrypted_key = "encrypted/written-by-previous";
    let encrypted_bytes = b"default-encrypted object written by the previous RustFS release";
    old_client
        .put_object()
        .bucket(CONFIG_ENCRYPTED_BUCKET)
        .key(encrypted_key)
        .body(ByteStream::from_static(encrypted_bytes))
        .send()
        .await?;
    assert_eq!(
        read_object(&old_client, CONFIG_ENCRYPTED_BUCKET, encrypted_key, None)
            .await?
            .0,
        Some(ServerSideEncryption::Aes256),
        "the previous release must apply the bucket default encryption it just accepted"
    );

    // The multipart object lives in the default-encrypted bucket so the
    // upgraded build has to reassemble parts *and* re-derive the object key.
    let multipart_key = "encrypted/multipart-written-by-previous";
    let multipart_parts = vec![vec![b'm'; 5 * 1024 * 1024], b"final multipart bytes".to_vec()];
    let multipart_bytes = multipart_parts.concat();
    write_multipart(&old_client, CONFIG_ENCRYPTED_BUCKET, multipart_key, &multipart_parts).await?;

    let versioned_key = "versioned/written-by-previous";
    let versioned_bytes = b"versioned object written by the previous RustFS release";
    let versioned_id = old_client
        .put_object()
        .bucket(CONFIG_REPLICATED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(versioned_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("versioned PUT omitted version ID")?
        .to_string();

    env.restart_server_preserving_data(vec![], &server_env).await?;
    let new_client = env.create_s3_client();

    // Every configuration must read back unchanged on the upgraded build.
    let upgraded_policy = new_client.get_bucket_policy().bucket(CONFIG_PLAIN_BUCKET).send().await?;
    let upgraded_policy: serde_json::Value =
        serde_json::from_str(upgraded_policy.policy().ok_or("GetBucketPolicy omitted the document")?)?;
    assert_eq!(upgraded_policy, policy, "the bucket policy changed across the upgrade");
    assert_bucket_tag(&new_client, CONFIG_PLAIN_BUCKET, "after the upgrade").await?;

    let lifecycle = new_client
        .get_bucket_lifecycle_configuration()
        .bucket(CONFIG_PLAIN_BUCKET)
        .send()
        .await?;
    let rules = lifecycle.rules();
    assert_eq!(rules.len(), 1, "the lifecycle rule count changed across the upgrade: {rules:?}");
    assert_eq!(rules[0].id(), Some(LIFECYCLE_RULE_ID));
    assert_eq!(rules[0].status(), &ExpirationStatus::Enabled);
    assert_eq!(
        rules[0].expiration().and_then(LifecycleExpiration::days),
        Some(LIFECYCLE_DAYS),
        "the lifecycle expiration changed across the upgrade"
    );

    assert_eq!(
        get_bucket_quota(&env, CONFIG_PLAIN_BUCKET).await?,
        Some(BUCKET_QUOTA_BYTES),
        "the bucket quota changed across the upgrade"
    );

    assert_default_sse_s3_encryption(&new_client, CONFIG_ENCRYPTED_BUCKET, "after the upgrade").await?;
    let public_access_block = new_client
        .get_public_access_block()
        .bucket(CONFIG_ENCRYPTED_BUCKET)
        .send()
        .await?;
    let public_access_block = public_access_block
        .public_access_block_configuration()
        .ok_or("GetPublicAccessBlock omitted the configuration")?;
    assert_eq!(public_access_block.block_public_acls(), Some(true));
    assert_eq!(public_access_block.ignore_public_acls(), Some(true));
    assert_eq!(public_access_block.block_public_policy(), Some(true));
    assert_eq!(public_access_block.restrict_public_buckets(), Some(true));

    assert_versioning_enabled(&new_client, CONFIG_REPLICATED_BUCKET, "after the upgrade").await?;
    // rustfs#7172: neither an empty list nor an error is acceptable here.
    assert_remote_target_preserved(&env, CONFIG_REPLICATED_BUCKET, &target_arn, "after the upgrade").await?;
    let replication = new_client
        .get_bucket_replication()
        .bucket(CONFIG_REPLICATED_BUCKET)
        .send()
        .await?;
    let replication_rules = replication
        .replication_configuration()
        .ok_or("GetBucketReplication omitted the configuration")?
        .rules();
    assert_eq!(
        replication_rules.len(),
        1,
        "the replication rule count changed across the upgrade: {replication_rules:?}"
    );
    assert_eq!(
        replication_rules[0].destination().map(|destination| destination.bucket()),
        Some(target_arn.as_str()),
        "the replication rule no longer points at the configured target"
    );

    let object_lock = new_client
        .get_object_lock_configuration()
        .bucket(CONFIG_LOCKED_BUCKET)
        .send()
        .await?;
    let object_lock = object_lock
        .object_lock_configuration()
        .ok_or("GetObjectLockConfiguration omitted the configuration")?;
    assert_eq!(object_lock.object_lock_enabled(), Some(&ObjectLockEnabled::Enabled));
    let retention = object_lock
        .rule()
        .and_then(ObjectLockRule::default_retention)
        .ok_or("the object lock configuration lost its default retention")?;
    assert_eq!(retention.mode(), Some(&ObjectLockRetentionMode::Governance));
    assert_eq!(retention.days(), Some(OBJECT_LOCK_DAYS));

    // rustfs#7183: a PUT into the default-encrypted bucket must still succeed
    // and still come back encrypted.
    let post_upgrade_encrypted_key = "encrypted/written-after-upgrade";
    let post_upgrade_encrypted_bytes = b"default-encrypted object written by the current RustFS build";
    new_client
        .put_object()
        .bucket(CONFIG_ENCRYPTED_BUCKET)
        .key(post_upgrade_encrypted_key)
        .body(ByteStream::from_static(post_upgrade_encrypted_bytes))
        .send()
        .await?;
    let (encryption, body) = read_object(&new_client, CONFIG_ENCRYPTED_BUCKET, post_upgrade_encrypted_key, None).await?;
    assert_eq!(
        encryption,
        Some(ServerSideEncryption::Aes256),
        "a PUT after the upgrade lost the bucket default encryption"
    );
    assert_eq!(body, post_upgrade_encrypted_bytes);

    let post_upgrade_plain_key = "plain/written-after-upgrade";
    let post_upgrade_plain_bytes = b"plain object written by the current RustFS build";
    put_object_through_quota_warmup(&new_client, CONFIG_PLAIN_BUCKET, post_upgrade_plain_key, post_upgrade_plain_bytes).await?;
    let (encryption, body) = read_object(&new_client, CONFIG_PLAIN_BUCKET, post_upgrade_plain_key, None).await?;
    assert_eq!(encryption, None, "a bucket without default encryption must not encrypt a PUT");
    assert_eq!(body, post_upgrade_plain_bytes);

    // Every object written by the previous release reads back byte-identical.
    assert_eq!(read_object(&new_client, CONFIG_PLAIN_BUCKET, plain_key, None).await?.1, plain_bytes);
    let (encryption, body) = read_object(&new_client, CONFIG_ENCRYPTED_BUCKET, encrypted_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(body, encrypted_bytes);
    let (encryption, body) = read_object(&new_client, CONFIG_ENCRYPTED_BUCKET, multipart_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(body, multipart_bytes, "the multipart object did not survive the upgrade");
    assert_eq!(
        read_object(&new_client, CONFIG_REPLICATED_BUCKET, versioned_key, Some(&versioned_id))
            .await?
            .1,
        versioned_bytes
    );

    // rustfs#7089: the migration module is on by default, but a bucket that
    // never configured a source behaves exactly as before.
    assert_migration_not_configured(&env, CONFIG_PLAIN_BUCKET).await?;
    assert_missing_key_is_no_such_key(&new_client, CONFIG_PLAIN_BUCKET, "plain/never-written").await?;

    replication_target.shutdown().await;
    Ok(())
}

/// Rolling back to the pinned previous release must still read the bucket
/// metadata the current build wrote.
///
/// This is the other half of the `BucketMetadata` 44 -> 46 key change: the
/// current build writes a 46-key msgpack map with `OnDemandMigrationConfigJSON`
/// and `OnDemandMigrationConfigUpdatedAt`, and the previous release's decoder
/// has to skip those two unknown keys instead of failing the whole blob. If it
/// did not, every configuration read below would come back empty or error and
/// the rollback would silently discard the bucket's configuration.
#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn rollback_to_previous_release_reads_current_bucket_metadata() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;

    let replication_target = FakeS3Target::start().await?;
    replication_target.create_bucket(ROLLBACK_REPLICA_BUCKET);

    let mut env = RustFSTestEnvironment::new().await?;
    let server_env = bucket_config_server_env();
    env.start_rustfs_server_with_env(vec![], &server_env).await?;
    let new_client = env.create_s3_client();

    env.create_test_bucket(ROLLBACK_BUCKET).await?;
    enable_versioning(&new_client, ROLLBACK_BUCKET).await?;
    put_default_sse_s3_encryption(&new_client, ROLLBACK_BUCKET).await?;
    put_bucket_tag(&new_client, ROLLBACK_BUCKET).await?;
    let target_arn = configure_replication(&env, ROLLBACK_BUCKET, &replication_target, ROLLBACK_REPLICA_BUCKET).await?;
    assert_remote_target_preserved(&env, ROLLBACK_BUCKET, &target_arn, "before the rollback").await?;

    let single_key = "rollback/single";
    let single_bytes = b"single-part object written by the current RustFS build";
    let single_version = new_client
        .put_object()
        .bucket(ROLLBACK_BUCKET)
        .key(single_key)
        .body(ByteStream::from_static(single_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("versioned PUT omitted version ID")?
        .to_string();

    let multipart_key = "rollback/multipart";
    let multipart_parts = vec![vec![b'r'; 5 * 1024 * 1024], b"final rollback bytes".to_vec()];
    let multipart_bytes = multipart_parts.concat();
    write_multipart(&new_client, ROLLBACK_BUCKET, multipart_key, &multipart_parts).await?;

    restart_from_binary(&mut env, &previous_binary, &server_env).await?;
    let old_client = env.create_s3_client();

    assert_versioning_enabled(&old_client, ROLLBACK_BUCKET, "after the rollback").await?;
    assert_default_sse_s3_encryption(&old_client, ROLLBACK_BUCKET, "after the rollback").await?;
    assert_bucket_tag(&old_client, ROLLBACK_BUCKET, "after the rollback").await?;
    assert_remote_target_preserved(&env, ROLLBACK_BUCKET, &target_arn, "after the rollback").await?;

    let (encryption, body) = read_object(&old_client, ROLLBACK_BUCKET, single_key, Some(&single_version)).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(body, single_bytes);
    let (encryption, body) = read_object(&old_client, ROLLBACK_BUCKET, multipart_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(body, multipart_bytes, "the multipart object did not survive the rollback");

    // A PUT on the rolled-back release must still honour the encryption
    // configuration it decoded out of the current build's metadata blob.
    let post_rollback_key = "rollback/written-after-rollback";
    let post_rollback_bytes = b"object written by the previous RustFS release after the rollback";
    old_client
        .put_object()
        .bucket(ROLLBACK_BUCKET)
        .key(post_rollback_key)
        .body(ByteStream::from_static(post_rollback_bytes))
        .send()
        .await?;
    let (encryption, body) = read_object(&old_client, ROLLBACK_BUCKET, post_rollback_key, None).await?;
    assert_eq!(
        encryption,
        Some(ServerSideEncryption::Aes256),
        "the rolled-back release lost the bucket default encryption"
    );
    assert_eq!(body, post_rollback_bytes);

    replication_target.shutdown().await;
    Ok(())
}
