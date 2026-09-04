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
    DistCluster, DistLayout, TestResult, assert_inventory, decommission_started_or_fenced, list_pools_json, put_inventory,
    rebalance_started_or_fenced, unique_bucket, wait_for_decommission_complete, wait_for_rebalance_idle,
};
use crate::common::init_logging;
use std::time::Duration;

#[tokio::test]
async fn two_pool_restart_preserves_objects_then_rebalance_attempt() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::TwoPoolFourDrive).await?;
    let bucket = unique_bucket("expand");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(0)?;
    let inventory = put_inventory(&client, &bucket, 12, 32 * 1024).await?;
    assert_inventory(&client, &bucket, &inventory).await?;

    dist.cluster.stop();
    dist.cluster.start().await?;

    let after_restart = dist.client(0)?;
    assert_inventory(&after_restart, &bucket, &inventory).await?;
    let peer = dist.client(1)?;
    assert_inventory(&peer, &bucket, &inventory).await?;

    if rebalance_started_or_fenced(&dist.cluster).await? {
        wait_for_rebalance_idle(&dist.cluster, Duration::from_secs(90)).await?;
    }
    assert_inventory(&peer, &bucket, &inventory).await?;
    Ok(())
}

#[tokio::test]
async fn two_pool_decommission_attempt_does_not_lose_objects() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::TwoPoolFourDrive).await?;
    let bucket = unique_bucket("decom");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(1)?;
    let inventory = put_inventory(&client, &bucket, 16, 48 * 1024).await?;

    let pools_before = list_pools_json(&dist.cluster).await?;
    let pool_count = pools_before
        .as_array()
        .map(Vec::len)
        .or_else(|| pools_before.get("pools").and_then(serde_json::Value::as_array).map(Vec::len))
        .unwrap_or(2);
    assert!(pool_count >= 2, "expected two pools before decommission: {pools_before}");

    if decommission_started_or_fenced(&dist.cluster, 0).await? {
        wait_for_decommission_complete(&dist.cluster, 0, Duration::from_secs(180)).await?;
    }

    let after = dist.client(1)?;
    assert_inventory(&after, &bucket, &inventory).await?;
    Ok(())
}

#[tokio::test]
async fn localhost_append_pool_restart_is_refused_by_pool_meta_recovery() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::TwoPoolFourDrive).await?;
    dist.cluster.stop();
    dist.cluster.append_single_node_pool().await?;
    dist.cluster.append_single_node_pool().await?;
    let err = dist
        .cluster
        .start()
        .await
        .expect_err("appending pools and restarting currently lacks a fresh-bootstrap proof on localhost DistErasure");
    let message = err.to_string();
    assert!(
        message.contains("process exited")
            || message.contains("failed to become ready")
            || message.contains("pool metadata recovery"),
        "expand restart must fail closed on the current pool-meta bootstrap gate, got: {message}"
    );
    Ok(())
}
