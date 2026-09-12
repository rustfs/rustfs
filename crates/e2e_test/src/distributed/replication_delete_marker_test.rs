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

//! Functional REP-105 (rustfs/backlog#2195 item 4): a delete marker created
//! on a multi-node source cluster must replicate to the bucket-replication
//! target. Objects converged in seconds while delete markers did not arrive
//! within 180 s on the shared 3-node functional environment; the single-node
//! e2e never saw it.

use super::harness::{
    DistCluster, DistLayout, TestResult, enable_versioning, put_bucket_replication, put_object, set_remote_target, unique_bucket,
    wait_for_replicated_bytes, wait_until,
};
use crate::common::{FAST_DATA_USAGE_SCANNER_ENV, RustFSTestEnvironment, init_logging, replication_fast_env, signed_request};
use crate::replication_extension_test::LOOPBACK_REPLICATION_TARGET_ENV;
use aws_sdk_s3::Client;
use http::{Method, StatusCode};
use std::time::Duration;

async fn target_has_delete_marker(client: &Client, bucket: &str, key: &str) -> TestResult<bool> {
    let versions = client.list_object_versions().bucket(bucket).prefix(key).send().await?;
    Ok(versions.delete_markers().iter().any(|marker| marker.key() == Some(key)))
}

async fn delete_marker_replicates(
    source: &DistCluster,
    source_bucket: &str,
    target_client: &Client,
    target_bucket: &str,
) -> TestResult {
    let key = "delete-marker/object.bin";
    let body = b"delete marker replication payload".to_vec();
    // Write through one node, delete through another: behind a load
    // balancer consecutive requests land on different nodes.
    put_object(&source.client(1)?, source_bucket, key, body.clone()).await?;
    wait_for_replicated_bytes(target_client, target_bucket, key, &body, Duration::from_secs(60)).await?;

    let delete = source
        .client(2)?
        .delete_object()
        .bucket(source_bucket)
        .key(key)
        .send()
        .await?;
    assert_eq!(
        delete.delete_marker(),
        Some(true),
        "a versioned DELETE without versionId must create a marker"
    );

    wait_until(
        Duration::from_secs(90),
        || async { target_has_delete_marker(target_client, target_bucket, key).await },
        "delete marker replicated to the target bucket",
    )
    .await
}

#[tokio::test]
async fn four_node_bucket_replication_replicates_delete_marker_to_peer_cluster() -> TestResult {
    init_logging();
    let (source, target) = DistCluster::start_replication_pair().await?;
    let source_bucket = unique_bucket("dm-src");
    let target_bucket = unique_bucket("dm-dst");
    source.create_bucket(&source_bucket).await?;
    target.create_bucket(&target_bucket).await?;
    enable_versioning(&source.client(0)?, &source_bucket).await?;
    enable_versioning(&target.client(0)?, &target_bucket).await?;

    let arn = set_remote_target(&source.cluster, &source_bucket, &target.cluster, &target_bucket).await?;
    put_bucket_replication(&source.cluster, &source_bucket, &arn).await?;

    delete_marker_replicates(&source, &source_bucket, &target.client(0)?, &target_bucket).await
}

/// The functional environment replicates from a 3-node site to a single-node
/// target; keep that shape as its own case.
#[tokio::test]
async fn four_node_bucket_replication_replicates_delete_marker_to_single_node_target() -> TestResult {
    init_logging();
    let mut extra: Vec<(&str, &str)> = replication_fast_env();
    extra.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    extra.extend_from_slice(FAST_DATA_USAGE_SCANNER_ENV);
    let source = DistCluster::start_with_env(DistLayout::FourNodeFourDisk, &extra).await?;
    let mut target = RustFSTestEnvironment::new().await?;
    target.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = unique_bucket("dm-src");
    let target_bucket = unique_bucket("dm-dst");
    source.create_bucket(&source_bucket).await?;
    let target_client = target.create_s3_client();
    target_client.create_bucket().bucket(&target_bucket).send().await?;
    enable_versioning(&source.client(0)?, &source_bucket).await?;
    enable_versioning(&target_client, &target_bucket).await?;

    let body = serde_json::json!({
        "endpoint": target.address,
        "credentials": { "accessKey": target.access_key, "secretKey": target.secret_key },
        "targetbucket": target_bucket,
        "secure": false,
        "type": "replication"
    });
    let url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}",
        source.cluster.nodes[0].url,
        urlencoding::encode(&source_bucket)
    );
    let response = signed_request(
        Method::PUT,
        &url,
        &source.cluster.access_key,
        &source.cluster.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("set remote target failed: {status} {body}").into());
    }
    let arn: String = serde_json::from_slice(&response.bytes().await?)?;
    put_bucket_replication(&source.cluster, &source_bucket, &arn).await?;

    delete_marker_replicates(&source, &source_bucket, &target_client, &target_bucket).await
}
