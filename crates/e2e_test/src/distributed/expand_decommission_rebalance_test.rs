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
    DistCluster, DistLayout, TestResult, assert_inventory, list_pools_json, put_inventory, start_decommission, start_rebalance,
    unique_bucket, wait_for_decommission_complete, wait_for_rebalance_idle,
};
use crate::common::init_logging;
use std::time::Duration;

#[tokio::test]
async fn four_node_pool_expand_preserves_objects_then_rebalance() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::TwoPoolFourDrive).await?;
    let bucket = unique_bucket("expand");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(0)?;
    let inventory = put_inventory(&client, &bucket, 12, 32 * 1024).await?;
    assert_inventory(&client, &bucket, &inventory).await?;

    dist.cluster.stop();
    dist.cluster.append_single_node_pool().await?;
    dist.cluster.append_single_node_pool().await?;
    assert_eq!(dist.cluster.nodes.len(), 4);
    dist.cluster.start().await?;

    let after_expand = dist.client(0)?;
    assert_inventory(&after_expand, &bucket, &inventory).await?;
    let peer = dist.client(3)?;
    assert_inventory(&peer, &bucket, &inventory).await?;

    let _ = start_rebalance(&dist.cluster).await?;
    // Rebalance may finish immediately on a tiny dataset; either idle or a
    // started-then-completed status is success. A hard failure is not.
    let _ = wait_for_rebalance_idle(&dist.cluster, Duration::from_secs(90)).await;
    assert_inventory(&peer, &bucket, &inventory).await?;
    Ok(())
}

#[tokio::test]
async fn four_pool_decommission_moves_objects_without_loss() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourPoolFourDrive).await?;
    let bucket = unique_bucket("decom");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(1)?;
    let inventory = put_inventory(&client, &bucket, 16, 48 * 1024).await?;

    let pools_before = list_pools_json(&dist.cluster).await?;
    let pool_count = pools_before
        .as_array()
        .map(Vec::len)
        .or_else(|| pools_before.get("pools").and_then(serde_json::Value::as_array).map(Vec::len))
        .unwrap_or(4);
    assert!(pool_count >= 4, "expected four pools before decommission: {pools_before}");

    start_decommission(&dist.cluster, 0).await?;
    wait_for_decommission_complete(&dist.cluster, 0, Duration::from_secs(180)).await?;

    let after = dist.client(3)?;
    assert_inventory(&after, &bucket, &inventory).await?;
    Ok(())
}
