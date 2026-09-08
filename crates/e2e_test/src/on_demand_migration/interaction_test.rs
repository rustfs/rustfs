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

//! How on-demand migration composes with the rest of the bucket surface
//! (rustfs/backlog#2158): default encryption, Object Lock, quota,
//! notifications, replication, versioning and delete markers, the disable
//! switch, and the admin view.
//!
//! A pulled object goes through the internal put path, so it must be
//! indistinguishable from a client PUT. Each case pins both the resulting
//! local object and what the source was asked for.

use super::common::{
    ALLOW_LOOPBACK_SOURCE_ENV, AdminResponse, BackfillOp, BackfillRequest, BoxError, ODM_MODULE_SWITCH_ENV, ODM_SERVER_ENV,
    OdmEnvOptions, OdmSourceSpec, OdmTestEnv, SeedObject, start_configured_env, start_configured_env_with, start_source_rustfs,
};
use crate::common::{RustFSTestEnvironment, replication_fast_env, signed_request};
use crate::fake_s3_target::{BucketMode, FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3Target, Operation};
use crate::object_lock::common::put_object_lock_configuration;
use crate::replication_extension_test::{
    ReplicationTargetOptions, enable_bucket_versioning, set_replication_target_with_options,
};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{
    BucketVersioningStatus, Event, FilterRule, FilterRuleName, NotificationConfiguration, NotificationConfigurationFilter,
    ObjectAttributes, ObjectLockRetentionMode, QueueConfiguration, S3KeyFilter, ServerSideEncryption,
    ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule, Tag, Tagging,
    VersioningConfiguration,
};
use bytes::Bytes;
use local_ip_address::local_ip;
use rustfs_utils::egress::ENV_OUTBOUND_ALLOW_ORIGINS;
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

type TestResult = Result<(), BoxError>;

const SOURCE_BUCKET: &str = "odm-interaction-source";
const ODM_RESPONSE_HEADER: &str = "x-rustfs-on-demand-migration";
/// `userIdentity.principalId` every write-back event carries.
const ODM_PRINCIPAL_ID: &str = "rustfs-on-demand-migration";
const SETTLE: Duration = Duration::from_secs(120);

fn payload(len: usize) -> Bytes {
    (0..len).map(|index| (index % 251) as u8).collect::<Vec<u8>>().into()
}

async fn admin(
    env: &RustFSTestEnvironment,
    method: http::Method,
    path: &str,
    body: Option<Value>,
) -> Result<AdminResponse, BoxError> {
    let url = format!("{}{path}", env.url);
    let body = body.map(|value| serde_json::to_vec(&value)).transpose()?;
    let content_type = body.is_some().then_some("application/json");
    let response = signed_request(method, &url, &env.access_key, &env.secret_key, body, content_type).await?;
    Ok(AdminResponse {
        status: response.status().as_u16(),
        body: response.text().await?,
    })
}

async fn enable_versioning(env: &OdmTestEnv, bucket: &str) -> TestResult {
    env.client
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

/// Case 12: a bucket that encrypts by default stores the pulled object
/// encrypted, and it reads back as plaintext afterwards without touching the
/// source again.
#[tokio::test]
async fn test_odm_pulled_object_uses_bucket_default_encryption() -> TestResult {
    let bucket = "odm-interaction-sse";
    let env = start_configured_env_with(
        OdmEnvOptions {
            local_kms: true,
            ..OdmEnvOptions::default()
        },
        bucket,
        SOURCE_BUCKET,
        |_| {},
    )
    .await?;
    env.client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(
            ServerSideEncryptionConfiguration::builder()
                .rules(
                    ServerSideEncryptionRule::builder()
                        .apply_server_side_encryption_by_default(
                            ServerSideEncryptionByDefault::builder()
                                .sse_algorithm(ServerSideEncryption::Aes256)
                                .build()?,
                        )
                        .build(),
                )
                .build()?,
        )
        .send()
        .await?;

    let key = "sse/report.bin";
    let body = payload(128 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    let first = env.raw_get(bucket, key).await?;
    assert_eq!(first.status, 200, "{}", String::from_utf8_lossy(&first.body));
    assert_eq!(first.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(first.body, body);
    assert!(env.wait_local_listed(bucket, key, SETTLE).await?, "the pull must store the object");

    let second = env.raw_get(bucket, key).await?;
    assert_eq!(second.status, 200, "{}", String::from_utf8_lossy(&second.body));
    assert_eq!(second.header(ODM_RESPONSE_HEADER), None, "the second read is local");
    assert_eq!(
        second.header("x-amz-server-side-encryption"),
        Some("AES256"),
        "the write-back honours the bucket default encryption"
    );
    assert_eq!(second.body, body, "the encrypted copy reads back as the source bytes");
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        1,
        "the encrypted local copy serves the second read"
    );
    Ok(())
}

/// Case 13: a pulled object inherits the bucket's default Object Lock
/// retention, so it cannot be deleted while the retention holds.
#[tokio::test]
async fn test_odm_pulled_object_inherits_object_lock_retention() -> TestResult {
    let bucket = "odm-interaction-object-lock";
    let env = OdmTestEnv::start().await?;
    env.source.create_bucket_with_mode(SOURCE_BUCKET, BucketMode::Unversioned);
    env.client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    put_object_lock_configuration(&env.client, bucket, ObjectLockRetentionMode::Compliance, Some(1), None).await?;
    let spec = env.fake_source_spec(SOURCE_BUCKET);
    env.configure_and_wait(bucket, &spec).await?;

    let key = "locked/record.bin";
    let body = payload(32 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    let pulled = env.raw_get(bucket, key).await?;
    assert_eq!(pulled.status, 200, "{}", String::from_utf8_lossy(&pulled.body));
    assert_eq!(pulled.body, body);
    assert!(env.wait_local_listed(bucket, key, SETTLE).await?, "the pull must store the object");

    let head = env.client.head_object().bucket(bucket).key(key).send().await?;
    assert_eq!(
        head.object_lock_mode().map(|mode| mode.as_str()),
        Some("COMPLIANCE"),
        "the default retention mode is applied to the pulled object"
    );
    assert!(head.object_lock_retain_until_date().is_some(), "a retain-until date is set");

    let version_id = head.version_id().ok_or("an Object Lock bucket is versioned")?.to_string();
    let error = env
        .client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .version_id(&version_id)
        .send()
        .await
        .expect_err("a COMPLIANCE-retained version cannot be deleted");
    assert_eq!(error.code(), Some("AccessDenied"), "{error:?}");
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        1,
        "the rejected delete never consults the source"
    );
    Ok(())
}

/// Case 14: the write-back obeys the bucket quota. The client is still
/// served from the source, but nothing is stored and the failure is counted.
#[tokio::test]
async fn test_odm_write_back_respects_the_bucket_quota() -> TestResult {
    let bucket = "odm-interaction-quota";
    let env = start_configured_env_with(
        OdmEnvOptions {
            env: vec![("RUSTFS_SCANNER_CYCLE", "1"), ("RUSTFS_SCANNER_START_DELAY_SECS", "0")],
            ..OdmEnvOptions::default()
        },
        bucket,
        SOURCE_BUCKET,
        |_| {},
    )
    .await?;

    // Fill the bucket past the quota it is about to get, so the write-back's
    // admission check has to reject it.
    let filler = payload(2 * 1024 * 1024);
    env.client
        .put_object()
        .bucket(bucket)
        .key("quota/filler.bin")
        .body(aws_sdk_s3::primitives::ByteStream::from(filler.clone()))
        .send()
        .await?;
    wait_for_bucket_usage(&env, bucket, filler.len() as u64).await?;
    set_bucket_quota(&env, bucket, 1024 * 1024).await?;

    let key = "quota/oversized.bin";
    let body = payload(2 * 1024 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    let response = env.raw_get(bucket, key).await?;
    assert_eq!(response.status, 200, "{}", String::from_utf8_lossy(&response.body));
    assert_eq!(response.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(response.body, body, "a full bucket still serves the client from the source");

    env.wait_for_status_counter(bucket, "/counters/pull_failures_total/quota", 1, SETTLE)
        .await?;
    env.assert_local_absent(bucket, key).await;
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        1,
        "the rejected write-back is not retried against the source"
    );
    Ok(())
}

/// The quota route answers 503 until the durable-quota capability is
/// confirmed on the fresh single-node deployment, so the write is retried.
async fn set_bucket_quota(env: &OdmTestEnv, bucket: &str, quota_bytes: u64) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let response = admin(
            &env.rustfs,
            http::Method::PUT,
            &format!("/rustfs/admin/v3/quota/{bucket}"),
            Some(serde_json::json!({ "quota": quota_bytes, "quota_type": "HARD" })),
        )
        .await?;
        if response.status < 300 {
            return Ok(());
        }
        if response.status != 503 || Instant::now() >= deadline {
            return Err(format!("set quota for {bucket}: {} {}", response.status, response.body).into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_bucket_usage(env: &OdmTestEnv, bucket: &str, at_least: u64) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let response = admin(&env.rustfs, http::Method::GET, &format!("/rustfs/admin/v3/quota-stats/{bucket}"), None).await?;
        if response.status == 200 {
            let usage = serde_json::from_str::<Value>(&response.body)?
                .get("current_usage")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if usage >= at_least {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("bucket usage for {bucket} did not reach {at_least} bytes: {}", response.body).into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Case 15: a pull emits an ordinary creation event attributed to the
/// migration principal, and `emit_events=false` silences it.
#[tokio::test]
async fn test_odm_pull_emits_object_created_events_unless_disabled() -> TestResult {
    let emitting = "odm-interaction-events";
    let silent = "odm-interaction-events-off";
    // The collector binds first: the outbound guard rejects a webhook
    // endpoint on a private address unless its origin is allowed at startup.
    let (endpoint, mut events) = spawn_event_collector().await?;
    let allowed_origin = reqwest::Url::parse(&endpoint)?.origin().ascii_serialization();
    let env = start_configured_env_with(
        OdmEnvOptions {
            env: vec![(ENV_OUTBOUND_ALLOW_ORIGINS, allowed_origin.as_str())],
            ..OdmEnvOptions::default()
        },
        emitting,
        SOURCE_BUCKET,
        |_| {},
    )
    .await?;
    let mut silent_spec = env.fake_source_spec(SOURCE_BUCKET);
    silent_spec.policy.emit_events = false;
    env.configure_and_wait(silent, &silent_spec).await?;

    let target = "odm-events";
    let switches = admin(
        &env.rustfs,
        http::Method::PUT,
        "/rustfs/admin/v3/module-switches",
        Some(serde_json::json!({ "notify_enabled": true, "audit_enabled": false })),
    )
    .await?;
    assert_eq!(switches.status, 200, "{}", switches.body);
    let queue_dir = format!("{}/notify-queue-{target}", env.rustfs.temp_dir);
    tokio::fs::create_dir_all(&queue_dir).await?;
    let configured = admin(
        &env.rustfs,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/target/notify_webhook/{target}"),
        Some(serde_json::json!({
            "key_values": [
                { "key": "endpoint", "value": endpoint },
                { "key": "queue_dir", "value": queue_dir },
            ]
        })),
    )
    .await?;
    assert_eq!(configured.status, 200, "{}", configured.body);
    wait_for_target_online(&env.rustfs, target).await?;
    for bucket in [emitting, silent] {
        put_notification_config(&env, bucket, target).await?;
    }

    // Control: an ordinary client PUT must produce an event, so a missing
    // one below is about the write-back and not about the pipeline.
    let control_key = "events/control.bin";
    env.client
        .put_object()
        .bucket(emitting)
        .key(control_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(payload(1024)))
        .send()
        .await?;
    let control = wait_for_event(&mut events, emitting, control_key, Duration::from_secs(60))
        .await
        .ok_or("the notification pipeline delivered no event for a plain PUT")?;
    assert_eq!(
        control.pointer("/eventName").and_then(Value::as_str),
        Some("s3:ObjectCreated:Put"),
        "{control}"
    );

    let emitting_key = "events/pulled.bin";
    let silent_key = "events/quiet.bin";
    let body = payload(16 * 1024);
    env.seed_source(
        SOURCE_BUCKET,
        &[
            SeedObject::new(emitting_key, body.clone()),
            SeedObject::new(silent_key, body.clone()),
        ],
    );

    for (bucket, key) in [(emitting, emitting_key), (silent, silent_key)] {
        let response = env.raw_get(bucket, key).await?;
        assert_eq!(response.status, 200, "{bucket}: {}", String::from_utf8_lossy(&response.body));
        assert!(env.wait_local_listed(bucket, key, SETTLE).await?, "{bucket}/{key} must be stored");
        assert_eq!(env.source.count_requests(Operation::GetObject, key), 1, "{bucket}/{key}");
    }

    let record = wait_for_event(&mut events, emitting, emitting_key, Duration::from_secs(60))
        .await
        .ok_or("no creation event for the pulled object")?;
    assert_eq!(
        record.pointer("/eventName").and_then(Value::as_str),
        Some("s3:ObjectCreated:Put"),
        "{record}"
    );
    assert_eq!(
        record.pointer("/userIdentity/principalId").and_then(Value::as_str),
        Some(ODM_PRINCIPAL_ID),
        "{record}"
    );

    // The silent bucket's object landed before the event above was observed,
    // so a missing event here is a decision, not a race.
    assert!(
        wait_for_event(&mut events, silent, silent_key, Duration::from_secs(5))
            .await
            .is_none(),
        "emit_events=false must not publish a creation event"
    );
    Ok(())
}

async fn put_notification_config(env: &OdmTestEnv, bucket: &str, target: &str) -> TestResult {
    let queue = QueueConfiguration::builder()
        .id(format!("{bucket}-rule"))
        .queue_arn(format!("arn:rustfs:sqs:us-east-1:{target}:webhook"))
        .events(Event::from("s3:ObjectCreated:*"))
        .filter(
            NotificationConfigurationFilter::builder()
                .key(
                    S3KeyFilter::builder()
                        .filter_rules(FilterRule::builder().name(FilterRuleName::Prefix).value("events/").build())
                        .build(),
                )
                .build(),
        )
        .build()?;
    env.client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(NotificationConfiguration::builder().queue_configurations(queue).build())
        .send()
        .await?;
    Ok(())
}

async fn wait_for_target_online(env: &RustFSTestEnvironment, target: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = admin(env, http::Method::GET, "/rustfs/admin/v3/target/list", None).await?;
        if response.status == 200 {
            let body: Value = serde_json::from_str(&response.body)?;
            let online = body["notification_endpoints"].as_array().is_some_and(|endpoints| {
                endpoints.iter().any(|endpoint| {
                    endpoint["account_id"].as_str() == Some(target) && endpoint["status"].as_str() == Some("online")
                })
            });
            if online {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("webhook target {target} did not come online: {}", response.body).into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Minimal HTTP receiver: answers everything 200 (so the target's
/// reachability probe reports online) and forwards parsed POST bodies.
async fn spawn_event_collector() -> Result<(String, mpsc::UnboundedReceiver<Value>), BoxError> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let port = listener.local_addr()?.port();
    let endpoint = format!("http://{}/events", std::net::SocketAddr::new(local_ip()?, port));
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0_u8; 4096];
                let mut content_length = 0usize;
                let mut header_end = None;
                while header_end.is_none() {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => buffer.extend_from_slice(&chunk[..read]),
                    }
                    header_end = buffer.windows(4).position(|window| window == b"\r\n\r\n");
                }
                let header_end = header_end.expect("loop exits only with a header end");
                let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
                for line in headers.split("\r\n").skip(1) {
                    if let Some((name, value)) = line.split_once(':')
                        && name.trim().eq_ignore_ascii_case("content-length")
                    {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                }
                let body_offset = header_end + 4;
                while buffer.len() - body_offset < content_length {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => buffer.extend_from_slice(&chunk[..read]),
                    }
                }
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await;
                let _ = stream.shutdown().await;
                if let Ok(value) = serde_json::from_slice::<Value>(&buffer[body_offset..body_offset + content_length]) {
                    let _ = tx.send(value);
                }
            });
        }
    });
    Ok((endpoint, rx))
}

/// The first delivered record for `bucket`/`key`, or `None` on timeout.
async fn wait_for_event(
    events: &mut mpsc::UnboundedReceiver<Value>,
    bucket: &str,
    key: &str,
    timeout: Duration,
) -> Option<Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.checked_duration_since(Instant::now())?;
        let envelope = tokio::time::timeout(remaining, events.recv()).await.ok()??;
        for record in envelope["Records"].as_array().into_iter().flatten() {
            // S3 event notifications URL-encode the object key.
            let record_key = record.pointer("/s3/object/key").and_then(Value::as_str).map(|raw| {
                urlencoding::decode(raw)
                    .map(|decoded| decoded.into_owned())
                    .unwrap_or_else(|_| raw.to_string())
            });
            if record.pointer("/s3/bucket/name").and_then(Value::as_str) == Some(bucket) && record_key.as_deref() == Some(key) {
                return Some(record.clone());
            }
        }
    }
}

/// Case 16: a pulled object enters the replication pipeline like any other
/// write, and a configuration whose source is one of the bucket's own
/// replication targets is rejected.
#[tokio::test]
async fn test_odm_pulled_object_replicates_and_target_as_source_is_rejected() -> TestResult {
    let bucket = "odm-interaction-replication";
    let replica_bucket = "odm-replica";
    let fast_env = replication_fast_env();
    let env = start_configured_env_with(
        OdmEnvOptions {
            env: fast_env.clone(),
            ..OdmEnvOptions::default()
        },
        bucket,
        SOURCE_BUCKET,
        |_| {},
    )
    .await?;
    let replica = FakeS3Target::start().await?;
    replica.create_bucket(replica_bucket);

    enable_versioning(&env, bucket).await?;
    let arn = set_remote_target(&env.rustfs, bucket, &replica.address(), replica_bucket).await?;
    put_bucket_replication(&env.rustfs, bucket, &arn).await?;

    let key = "replicated/asset.bin";
    let body = payload(64 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, body.clone())]);

    let response = env.raw_get(bucket, key).await?;
    assert_eq!(response.status, 200, "{}", String::from_utf8_lossy(&response.body));
    assert_eq!(response.body, body);
    assert!(env.wait_local_listed(bucket, key, SETTLE).await?, "the pull must store the object");

    let deadline = Instant::now() + SETTLE;
    while !replica.has_object(replica_bucket, key) {
        assert!(Instant::now() < deadline, "the pulled object was never replicated to the target");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        1,
        "replication reads the local copy, never the migration source"
    );

    let looping = OdmSourceSpec::for_fake_source(&replica, replica_bucket);
    let rejected = env.configure_source(bucket, &looping).await?;
    assert_eq!(
        rejected.status, 400,
        "a bucket may not migrate from its own replication target: {}",
        rejected.body
    );
    Box::pin(assert_odm_multipart_replicates_to_rustfs(&env, bucket)).await?;
    Ok(())
}

async fn assert_odm_multipart_replicates_to_rustfs(env: &OdmTestEnv, bucket: &str) -> TestResult {
    const PART_SIZE: usize = 5 * 1024 * 1024;
    let replica = start_source_rustfs().await?;
    let replica_bucket = "odm-real-replica";
    replica.create_test_bucket(replica_bucket).await?;
    enable_bucket_versioning(&replica, replica_bucket).await?;
    let arn = set_replication_target_with_options(
        &env.rustfs,
        bucket,
        ReplicationTargetOptions {
            endpoint: &replica.address,
            access_key: &replica.access_key,
            secret_key: &replica.secret_key,
            target_bucket: replica_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&env.rustfs, bucket, &arn).await?;
    let mut spec = env.fake_source_spec(SOURCE_BUCKET);
    // Below the 16 MiB inline default the pull is one tee'd PUT with a single
    // part; force the passthrough + background multipart write-back instead.
    spec.policy.inline_max_bytes = 4096;
    spec.policy.multipart_part_size_bytes = PART_SIZE as u64;
    spec.policy.preserve_etag = true;
    env.configure_and_wait(bucket, &spec).await?;

    let key = "replicated/preserved-md5-multipart.bin";
    let body = payload(PART_SIZE + 4096);
    let source_put = env
        .source_client()
        .put_object()
        .bucket(SOURCE_BUCKET)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from(body.clone()))
        .send()
        .await?;
    let etag = source_put.e_tag().ok_or("source PUT omitted its ETag")?.trim_matches('"');
    assert_eq!(etag.len(), 32, "the source must retain a single-PUT MD5 ETag");
    assert!(etag.bytes().all(|byte| byte.is_ascii_hexdigit()));
    let pulled = env.raw_get(bucket, key).await?;
    assert_eq!(pulled.status, 200, "{}", String::from_utf8_lossy(&pulled.body));
    assert_eq!(pulled.body, body);
    assert!(env.wait_local_listed(bucket, key, SETTLE).await?, "the multipart pull must persist");

    let deadline = Instant::now() + SETTLE;
    let source_head = loop {
        let head = env.client.head_object().bucket(bucket).key(key).send().await?;
        match head.replication_status().map(|status| status.as_str()) {
            Some("COMPLETED") => break head,
            Some("FAILED") => return Err("the ODM multipart copy failed replication to RustFS".into()),
            _ => {
                assert!(Instant::now() < deadline, "the ODM multipart copy never completed replication to RustFS");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    };
    let version = source_head
        .version_id()
        .ok_or("the versioned ODM copy omitted its version id")?;
    assert_ne!(version, "null");
    let replica_client = replica.create_s3_client();
    for (client, object_bucket) in [(&env.client, bucket), (&replica_client, replica_bucket)] {
        let attributes = client
            .get_object_attributes()
            .bucket(object_bucket)
            .key(key)
            .version_id(version)
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::ObjectParts)
            .send()
            .await?;
        assert_eq!(attributes.e_tag().map(|value| value.trim_matches('"')), Some(etag));
        let parts = attributes
            .object_parts()
            .ok_or("the local copy and replica must both expose two parts")?;
        assert_eq!(parts.total_parts_count(), Some(2));
        assert_eq!(
            parts
                .parts()
                .iter()
                .map(|part| (part.part_number(), part.size()))
                .collect::<Vec<_>>(),
            [(Some(1), Some(PART_SIZE as i64)), (Some(2), Some(4096))]
        );
    }
    // REPLICA status surfaces on HEAD, like the other inbound-replica checks.
    let replica_head = replica_client
        .head_object()
        .bucket(replica_bucket)
        .key(key)
        .version_id(version)
        .send()
        .await?;
    assert_eq!(replica_head.replication_status().map(|status| status.as_str()), Some("REPLICA"));
    let replica_get = replica_client
        .get_object()
        .bucket(replica_bucket)
        .key(key)
        .version_id(version)
        .send()
        .await?;
    assert_eq!(replica_get.version_id(), Some(version));
    assert_eq!(replica_get.body.collect().await?.into_bytes(), body);
    let boundary = replica_client
        .get_object()
        .bucket(replica_bucket)
        .key(key)
        .version_id(version)
        .range(format!("bytes={}-{}", PART_SIZE - 32, PART_SIZE + 31))
        .send()
        .await?;
    assert_eq!(boundary.body.collect().await?.into_bytes(), body.slice(PART_SIZE - 32..PART_SIZE + 32));
    assert_eq!(
        env.source.count_requests(Operation::GetObject, key),
        2,
        "one passthrough GET plus one background pull; replication and local reads must not fetch the migration source again"
    );
    Ok(())
}

async fn set_remote_target(
    env: &RustFSTestEnvironment,
    bucket: &str,
    endpoint: &str,
    target_bucket: &str,
) -> Result<String, BoxError> {
    let response = admin(
        env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/set-remote-target?bucket={}", urlencoding::encode(bucket)),
        Some(serde_json::json!({
            "endpoint": endpoint,
            "credentials": { "accessKey": FAKE_ACCESS_KEY, "secretKey": FAKE_SECRET_KEY },
            "targetbucket": target_bucket,
            "secure": false,
            "skipTlsVerify": false,
            "type": "replication"
        })),
    )
    .await?;
    if response.status != 200 {
        return Err(format!("set remote target: {} {}", response.status, response.body).into());
    }
    Ok(serde_json::from_str(&response.body)?)
}

async fn put_bucket_replication(env: &RustFSTestEnvironment, bucket: &str, arn: &str) -> TestResult {
    let body = format!(
        r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role></Role>
  <Rule>
    <ID>odm-rule</ID>
    <Priority>1</Priority>
    <Status>Enabled</Status>
    <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
    <ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>
    <Destination><Bucket>{arn}</Bucket></Destination>
  </Rule>
</ReplicationConfiguration>"#
    );
    let url = format!("{}/{bucket}?replication", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.into_bytes()),
        Some("application/xml"),
    )
    .await?;
    if response.status() != 200 {
        let status = response.status();
        return Err(format!("put bucket replication: {status} {}", response.text().await.unwrap_or_default()).into());
    }
    Ok(())
}

/// Case 17: a local delete marker is the authoritative answer in a versioned
/// bucket, while an unversioned delete leaves nothing behind and the key is
/// migrated again.
#[tokio::test]
async fn test_odm_delete_marker_shadows_the_source_but_a_plain_delete_does_not() -> TestResult {
    let versioned = "odm-interaction-delete-marker";
    let unversioned = "odm-interaction-plain-delete";
    let env = start_configured_env(versioned, SOURCE_BUCKET, |_| {}).await?;
    let spec = env.fake_source_spec(SOURCE_BUCKET);
    env.configure_and_wait(unversioned, &spec).await?;
    enable_versioning(&env, versioned).await?;

    let key = "deleted/doc.bin";
    let source_body = payload(8 * 1024);
    let local_body = payload(4 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(key, source_body.clone())]);

    for bucket in [versioned, unversioned] {
        env.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(local_body.clone()))
            .send()
            .await?;
        env.client.delete_object().bucket(bucket).key(key).send().await?;
    }

    let shadowed = env.raw_get(versioned, key).await?;
    assert_eq!(shadowed.status, 404, "{}", String::from_utf8_lossy(&shadowed.body));
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, key),
        0,
        "a local delete marker answers without the source"
    );

    let migrated = env.raw_get(unversioned, key).await?;
    assert_eq!(migrated.status, 200, "{}", String::from_utf8_lossy(&migrated.body));
    assert_eq!(migrated.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(migrated.body, source_body, "an unversioned delete leaves the source authoritative");
    assert_eq!(env.source.count_requests(Operation::HeadObject, key), 1);
    assert_eq!(env.source.count_requests(Operation::GetObject, key), 1);
    Ok(())
}

/// Case 18: deleting the configuration stops all source traffic without
/// touching what was already migrated, and reinstalling it resumes.
#[tokio::test]
async fn test_odm_disable_keeps_pulled_objects_and_stops_source_traffic() -> TestResult {
    let bucket = "odm-interaction-disable";
    let env = start_configured_env(bucket, SOURCE_BUCKET, |_| {}).await?;
    let pulled_key = "disable/pulled.bin";
    let untouched_key = "disable/untouched.bin";
    let body = payload(32 * 1024);
    env.seed_source(
        SOURCE_BUCKET,
        &[
            SeedObject::new(pulled_key, body.clone()),
            SeedObject::new(untouched_key, body.clone()),
        ],
    );

    let pulled = env.raw_get(bucket, pulled_key).await?;
    assert_eq!(pulled.status, 200, "{}", String::from_utf8_lossy(&pulled.body));
    assert!(env.wait_local_listed(bucket, pulled_key, SETTLE).await?);

    let disabled = env.disable(bucket).await?;
    assert_eq!(disabled.status, 204, "{}", disabled.body);

    let still_readable = env.raw_get(bucket, pulled_key).await?;
    assert_eq!(still_readable.status, 200, "{}", String::from_utf8_lossy(&still_readable.body));
    assert_eq!(still_readable.body, body, "a migrated object survives the disable");
    assert_eq!(still_readable.header(ODM_RESPONSE_HEADER), None);
    assert_eq!(env.source.count_requests(Operation::GetObject, pulled_key), 1);

    let missing = env.raw_get(bucket, untouched_key).await?;
    assert_eq!(missing.status, 404, "{}", String::from_utf8_lossy(&missing.body));
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, untouched_key),
        0,
        "a disabled bucket never reaches the source"
    );

    let spec = env.fake_source_spec(SOURCE_BUCKET);
    env.configure_and_wait(bucket, &spec).await?;
    let resumed = env.raw_get(bucket, untouched_key).await?;
    assert_eq!(resumed.status, 200, "{}", String::from_utf8_lossy(&resumed.body));
    assert_eq!(resumed.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(resumed.body, body);
    assert_eq!(env.source.count_requests(Operation::GetObject, untouched_key), 1);
    Ok(())
}

/// The process switch preserves configured buckets and unfinished jobs while
/// restoring local-only S3 behavior, including after an ordinary metadata write.
#[tokio::test]
async fn test_odm_global_disable_preserves_data_config_and_backfill_across_restarts() -> TestResult {
    let bucket = "odm-global-disable";
    let mut env = start_configured_env(bucket, SOURCE_BUCKET, |spec| spec.policy.list_through = true).await?;
    let pulled_key = "migrated/pulled.bin";
    let remote_key = "remote/untouched.bin";
    let pending_key = "backfill/pending.bin";
    let local_key = "local/kept.bin";
    let source_body = Bytes::from_static(b"source payload");
    let local_body = Bytes::from_static(b"client payload");
    env.seed_source(
        SOURCE_BUCKET,
        &[
            SeedObject::new(pulled_key, source_body.clone()),
            SeedObject::new(remote_key, source_body.clone()),
            SeedObject::new(pending_key, source_body.clone()),
        ],
    );
    env.client
        .put_object()
        .bucket(bucket)
        .key(local_key)
        .body(local_body.clone().into())
        .send()
        .await?;
    let pulled = env.raw_get(bucket, pulled_key).await?;
    assert_eq!(pulled.status, 200);
    assert_eq!(pulled.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(pulled.body, source_body);
    let stored = env.raw_get(bucket, pulled_key).await?;
    assert_eq!(stored.status, 200);
    assert_eq!(stored.header(ODM_RESPONSE_HEADER), None, "the inline pull has committed locally");
    assert_eq!(stored.body, source_body);
    let config = env.get_config(bucket).await?;
    assert_eq!(config.status, 200, "{}", config.body);
    let config = config.json()?;

    // Hold every attempt until the process has exited, so retries cannot commit
    // the only backfill object before the crash. The start checkpoint exists.
    let mut pending_get = env.source.hold_get_object(SOURCE_BUCKET, pending_key);
    let started = env
        .start_backfill(
            bucket,
            BackfillRequest {
                prefix: Some("backfill/".to_string()),
                ..BackfillRequest::default()
            },
        )
        .await?;
    assert_eq!(started.status, 200, "{}", started.body);
    let job_id = started.json()?["job"]["job_id"].as_str().ok_or("missing job ID")?.to_string();
    tokio::time::timeout(Duration::from_secs(10), pending_get.wait_until_entered())
        .await
        .expect("backfill never reached the held source GET")?;
    let process = env.rustfs.process.as_mut().ok_or("missing RustFS process before crash")?;
    assert!(process.try_wait()?.is_none(), "RustFS exited before the controlled crash");
    process.kill()?;
    let stopped = process.wait()?;
    assert!(!stopped.success(), "the interrupted process must exit after being killed");
    drop(env.rustfs.process.take());
    drop(pending_get);
    env.source.take_requests();
    env.rustfs
        .restart_server_preserving_data(vec![], &[(ODM_MODULE_SWITCH_ENV, "false"), (ALLOW_LOOPBACK_SOURCE_ENV, "true")])
        .await?;

    let off_config = env.get_config(bucket).await?;
    assert_eq!(off_config.status, 200, "{}", off_config.body);
    assert_eq!(off_config.json()?, config, "the saved configuration and timestamp survive disabling");
    let status = env.status_json(bucket).await?;
    assert_eq!(status["configured"], true, "{status}");
    assert_eq!(status["enabled"], true, "the bucket remains configured as enabled: {status}");
    assert_eq!(status["module_enabled"], false, "{status}");
    assert_eq!(status["counters"], Value::Null, "no bucket runtime is installed: {status}");
    let checkpoint = env.backfill_job(bucket).await?.ok_or("disabled module lost the checkpoint")?;
    assert_eq!(checkpoint["job_id"], job_id);
    assert_eq!(checkpoint["state"], "running", "the interrupted job is retained: {checkpoint}");

    for (key, body) in [(local_key, &local_body), (pulled_key, &source_body)] {
        let get = env.raw_get(bucket, key).await?;
        assert_eq!(get.status, 200);
        assert_eq!(&get.body, body);
        assert_eq!(get.header(ODM_RESPONSE_HEADER), None);
        let head = env.client.head_object().bucket(bucket).key(key).send().await?;
        assert_eq!(head.content_length(), Some(i64::try_from(body.len())?));
    }
    for key in [remote_key, pending_key] {
        let get = env.raw_get(bucket, key).await?;
        assert_eq!(get.status, 404, "disabled source GET {key}: {}", String::from_utf8_lossy(&get.body));
        let head = env.client.head_object().bucket(bucket).key(key).send().await;
        let err = head.expect_err("a source-only object must remain absent locally");
        assert_eq!(err.raw_response().map(|response| response.status().as_u16()), Some(404));
    }

    let replacement = Bytes::from_static(b"written while the module is off");
    for key in [local_key, "local/deleted.bin"] {
        env.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(replacement.clone().into())
            .send()
            .await?;
    }
    env.client
        .delete_object()
        .bucket(bucket)
        .key("local/deleted.bin")
        .send()
        .await?;
    assert_eq!(env.raw_get(bucket, "local/deleted.bin").await?.status, 404);
    assert_eq!(env.raw_get(bucket, local_key).await?.body, replacement);

    // Both wire protocols must finish their local pages even though the saved
    // configuration still requests list-through.
    for use_v2 in [false, true] {
        let mut cursor = None;
        let mut listed = Vec::new();
        for page_number in 0..2 {
            let (keys, truncated, next) = if use_v2 {
                let page = env
                    .client
                    .list_objects_v2()
                    .bucket(bucket)
                    .max_keys(1)
                    .set_continuation_token(cursor)
                    .send()
                    .await?;
                (
                    page.contents()
                        .iter()
                        .map(|object| object.key().expect("listed key").to_string())
                        .collect::<Vec<_>>(),
                    page.is_truncated(),
                    page.next_continuation_token().map(str::to_string),
                )
            } else {
                let page = env
                    .client
                    .list_objects()
                    .bucket(bucket)
                    .max_keys(1)
                    .set_marker(cursor)
                    .send()
                    .await?;
                // V1 may omit NextMarker without a delimiter; clients then
                // continue from the last returned key.
                let next = page.next_marker().or_else(|| {
                    if page.is_truncated() == Some(true) {
                        page.contents().last().and_then(|object| object.key())
                    } else {
                        None
                    }
                });
                (
                    page.contents()
                        .iter()
                        .map(|object| object.key().expect("listed key").to_string())
                        .collect::<Vec<_>>(),
                    page.is_truncated(),
                    next.map(str::to_string),
                )
            };
            assert_eq!(keys.len(), 1, "one local key per page, V2={use_v2}");
            assert_eq!(truncated, Some(page_number == 0), "local pagination must terminate, V2={use_v2}");
            if page_number == 0 {
                assert!(next.as_ref().is_some_and(|value| !value.is_empty()), "missing local cursor, V2={use_v2}");
            }
            cursor = next;
            listed.extend(keys);
        }
        assert_eq!(listed, [local_key, pulled_key], "source-only keys must stay absent, V2={use_v2}");
    }

    let spec = env.fake_source_spec(SOURCE_BUCKET);
    for response in [
        env.configure_source(bucket, &spec).await?,
        env.validate_source(bucket, &spec).await?,
        env.backfill(bucket, BackfillOp::Start(BackfillRequest::default())).await?,
    ] {
        assert_eq!(response.status, 400, "{}", response.body);
        assert!(response.body.contains("OnDemandMigrationDisabled"), "{}", response.body);
    }
    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("module").value("disabled").build()?)
        .build()?;
    env.client
        .put_bucket_tagging()
        .bucket(bucket)
        .tagging(tagging.clone())
        .send()
        .await?;
    assert_eq!(env.get_config(bucket).await?.json()?, config, "an unrelated metadata write preserves ODM");
    assert_eq!(
        env.backfill_job(bucket).await?,
        Some(checkpoint),
        "no recovery or checkpoint update while disabled"
    );
    assert!(
        env.source.requests().is_empty(),
        "disabled startup and all requests must leave the source untouched"
    );

    env.rustfs.restart_server_preserving_data(vec![], ODM_SERVER_ENV).await?;
    env.wait_until_source_consulted(bucket).await?;
    assert_eq!(
        env.get_config(bucket).await?.json()?,
        config,
        "reenabling uses the persisted configuration"
    );
    let tags = env.client.get_bucket_tagging().bucket(bucket).send().await?;
    assert_eq!(tags.tag_set(), tagging.tag_set(), "the ordinary metadata write also persists");
    let resumed = env.raw_get(bucket, remote_key).await?;
    assert_eq!(resumed.status, 200);
    assert_eq!(resumed.header(ODM_RESPONSE_HEADER), Some("source"));
    assert_eq!(resumed.body, source_body, "stored credentials still authenticate without reconfiguration");
    let completed = env
        .wait_for_backfill(bucket, SETTLE, |job| job["state"] == "completed")
        .await?;
    assert_eq!(completed["job_id"], job_id, "the interrupted job resumes without a new start");
    assert_eq!(completed["failed"], 0, "{completed}");
    for (key, body) in [
        (local_key, &replacement),
        (pulled_key, &source_body),
        (pending_key, &source_body),
    ] {
        let get = env.raw_get(bucket, key).await?;
        assert_eq!(get.status, 200);
        assert_eq!(&get.body, body);
        assert_eq!(get.header(ODM_RESPONSE_HEADER), None, "{key} remains stored locally");
    }
    Ok(())
}

/// Case 19: the admin surface an operator sees — the configuration read back
/// without its secret, and a status document whose counters match the source
/// journal exactly.
#[tokio::test]
async fn test_odm_admin_config_is_redacted_and_status_counts_match_the_source() -> TestResult {
    let bucket = "odm-interaction-admin";
    let env = start_configured_env(bucket, SOURCE_BUCKET, |_| {}).await?;
    let hit_key = "admin/present.bin";
    let miss_key = "admin/absent.bin";
    let body = payload(16 * 1024);
    env.seed_source(SOURCE_BUCKET, &[SeedObject::new(hit_key, body.clone())]);

    let config = env.get_config(bucket).await?;
    assert_eq!(config.status, 200, "{}", config.body);
    let config = config.json()?;
    assert_eq!(
        config
            .pointer("/config/source/credentials/secret_key")
            .and_then(Value::as_str),
        Some("REDACTED"),
        "{config}"
    );
    assert_eq!(
        config
            .pointer("/config/source/credentials/access_key")
            .and_then(Value::as_str),
        Some(FAKE_ACCESS_KEY),
        "the access key stays readable: {config}"
    );
    assert!(
        !config.to_string().contains(FAKE_SECRET_KEY),
        "the secret must not appear anywhere in the response"
    );

    let hit = env.raw_get(bucket, hit_key).await?;
    assert_eq!(hit.status, 200, "{}", String::from_utf8_lossy(&hit.body));
    for _ in 0..2 {
        let miss = env.raw_get(bucket, miss_key).await?;
        assert_eq!(miss.status, 404, "{}", String::from_utf8_lossy(&miss.body));
    }
    assert!(env.wait_local_listed(bucket, hit_key, SETTLE).await?);

    let status = env.status_json(bucket).await?;
    assert_eq!(status.pointer("/configured").and_then(Value::as_bool), Some(true), "{status}");
    assert_eq!(status.pointer("/enabled").and_then(Value::as_bool), Some(true), "{status}");
    assert_eq!(status.pointer("/module_enabled").and_then(Value::as_bool), Some(true), "{status}");
    assert_eq!(status.pointer("/provider").and_then(Value::as_str), Some("s3"), "{status}");
    assert_eq!(
        status
            .pointer("/counters/requests_total/get/source_hit")
            .and_then(Value::as_u64),
        Some(1),
        "one source hit, matching the one source GET: {status}"
    );
    assert_eq!(
        status
            .pointer("/counters/requests_total/get/source_miss")
            .and_then(Value::as_u64),
        Some(1),
        "only the first miss reached the source: {status}"
    );
    assert_eq!(
        status
            .pointer("/counters/requests_total/get/negative_cached")
            .and_then(Value::as_u64),
        Some(1),
        "the second miss stopped at the negative cache: {status}"
    );
    assert_eq!(
        status
            .pointer("/counters/pulled_objects_total/inline")
            .and_then(Value::as_u64),
        Some(1),
        "{status}"
    );
    assert_eq!(
        status.pointer("/counters/pulled_bytes_total").and_then(Value::as_u64),
        Some(body.len() as u64),
        "{status}"
    );
    assert_eq!(env.source.count_requests(Operation::GetObject, hit_key), 1);
    assert_eq!(
        env.source.count_requests(Operation::HeadObject, miss_key),
        1,
        "the status counters and the source journal agree"
    );
    Ok(())
}
