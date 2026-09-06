// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::harness::{
    DistCluster, DistLayout, TestResult, enable_versioning, put_bucket_replication, put_object, retrying_put, set_bucket_quota,
    set_remote_target, unique_bucket, wait_for_ready, wait_for_replicated_bytes, wait_until,
};
use crate::common::{FAST_DATA_USAGE_SCANNER_ENV, init_logging};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use http::Method;
use std::time::Duration;

async fn wait_for_replication_status(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected: &[&str],
    timeout: Duration,
) -> TestResult {
    wait_until(
        timeout,
        || async {
            let head = client.head_object().bucket(bucket).key(key).send().await?;
            Ok(head
                .replication_status()
                .is_some_and(|status| expected.contains(&status.as_str())))
        },
        &format!("replication status for {bucket}/{key} in {expected:?}"),
    )
    .await
}

#[tokio::test]
async fn four_node_bucket_replication_converges_to_peer_cluster() -> TestResult {
    init_logging();
    let (source, mut target) = DistCluster::start_replication_pair().await?;
    let source_bucket = unique_bucket("replsrc");
    let target_bucket = unique_bucket("repldst");
    source.create_bucket(&source_bucket).await?;
    target.create_bucket(&target_bucket).await?;

    let source_client = source.client(0)?;
    let target_client = target.client(0)?;
    enable_versioning(&source_client, &source_bucket).await?;
    enable_versioning(&target_client, &target_bucket).await?;

    let arn = set_remote_target(&source.cluster, &source_bucket, &target.cluster, &target_bucket).await?;
    put_bucket_replication(&source.cluster, &source_bucket, &arn).await?;

    let key = "replicated/metadata-and-tags.bin";
    let body = b"distributed-bucket-replication".to_vec();
    source_client
        .put_object()
        .bucket(&source_bucket)
        .key(key)
        .metadata("origin", "four-node-source")
        .tagging("suite=distributed&shape=metadata")
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    wait_for_replicated_bytes(&target_client, &target_bucket, key, &body, Duration::from_secs(45)).await?;
    wait_for_replication_status(&source_client, &source_bucket, key, &["COMPLETED"], Duration::from_secs(30)).await?;

    let peer_read = target.client(3)?;
    wait_for_replicated_bytes(&peer_read, &target_bucket, key, &body, Duration::from_secs(15)).await?;
    let replica_head = peer_read.head_object().bucket(&target_bucket).key(key).send().await?;
    assert_eq!(
        replica_head
            .metadata()
            .and_then(|metadata| metadata.get("origin"))
            .map(String::as_str),
        Some("four-node-source")
    );
    assert_eq!(replica_head.replication_status().map(|status| status.as_str()), Some("REPLICA"));
    let replica_tags = peer_read.get_object_tagging().bucket(&target_bucket).key(key).send().await?;
    let tags: std::collections::BTreeMap<_, _> = replica_tags.tag_set().iter().map(|tag| (tag.key(), tag.value())).collect();
    assert_eq!(tags.get("suite"), Some(&"distributed"));
    assert_eq!(tags.get("shape"), Some(&"metadata"));

    target.cluster.stop();
    let outage_key = "replicated/queued-during-target-outage.bin";
    let outage_body = b"retry-after-target-restart".to_vec();
    put_object(&source_client, &source_bucket, outage_key, outage_body.clone()).await?;
    wait_for_replication_status(
        &source_client,
        &source_bucket,
        outage_key,
        &["PENDING", "FAILED"],
        Duration::from_secs(30),
    )
    .await?;

    target.cluster.start().await?;
    wait_for_ready(&target.cluster).await?;
    wait_for_replicated_bytes(&target.client(2)?, &target_bucket, outage_key, &outage_body, Duration::from_secs(90)).await?;
    wait_for_replication_status(&source_client, &source_bucket, outage_key, &["COMPLETED"], Duration::from_secs(45)).await?;
    Ok(())
}

#[tokio::test]
async fn four_node_four_drive_hard_quota_rejects_over_limit_put() -> TestResult {
    init_logging();
    let dist = DistCluster::start_with_env(DistLayout::FourByFour, FAST_DATA_USAGE_SCANNER_ENV).await?;
    let bucket = unique_bucket("quota");
    dist.create_bucket(&bucket).await?;
    set_bucket_quota(&dist.cluster, &bucket, 8 * 1024).await?;

    let client = dist.client(1)?;
    retrying_put(&client, &bucket, "small.bin", vec![0u8; 1024], Duration::from_secs(30)).await?;
    wait_until(
        Duration::from_secs(30),
        || async {
            let (status, body) = super::harness::cluster_admin(
                &dist.cluster,
                Method::GET,
                &format!("/rustfs/admin/v3/quota-stats/{bucket}"),
                None,
            )
            .await?;
            if !status.is_success() {
                return Ok(false);
            }
            let stats: serde_json::Value =
                serde_json::from_str(&body).map_err(|error| format!("quota stats returned invalid JSON: {error}: {body}"))?;
            let usage = stats
                .get("current_usage")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("quota stats omitted current_usage: {stats}"))?;
            Ok(usage >= 1024)
        },
        "quota stats observe small object",
    )
    .await?;

    let oversized_key = "too-big.bin";
    let error = client
        .put_object()
        .bucket(&bucket)
        .key(oversized_key)
        .body(vec![0u8; 16 * 1024].into())
        .send()
        .await
        .expect_err("hard quota must reject the oversized PUT");
    let service_error = error
        .as_service_error()
        .ok_or("quota rejection was not an S3 service error")?;
    assert_eq!(
        error.raw_response().map(|response| response.status().as_u16()),
        Some(400),
        "quota rejection must be HTTP 400: {error:?}"
    );
    assert_eq!(service_error.code(), Some("InvalidRequest"), "unexpected quota error: {error:?}");
    assert!(
        service_error
            .message()
            .is_some_and(|message| message.starts_with("Bucket quota exceeded")),
        "PUT must fail specifically at quota admission: {error:?}"
    );

    let missing = client
        .head_object()
        .bucket(&bucket)
        .key(oversized_key)
        .send()
        .await
        .expect_err("an object rejected by quota must not become visible");
    assert_eq!(
        missing.raw_response().map(|response| response.status().as_u16()),
        Some(404),
        "quota-rejected object returned an unexpected HEAD result: {missing:?}"
    );

    let listed = client.list_objects_v2().bucket(&bucket).send().await?;
    assert!(
        listed.contents().iter().all(|object| object.key() != Some(oversized_key)),
        "quota-rejected key leaked into ListObjectsV2"
    );
    Ok(())
}
