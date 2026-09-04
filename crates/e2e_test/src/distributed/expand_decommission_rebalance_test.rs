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
    DECOMMISSION_POOL_ID, DistCluster, DistLayout, TestResult, assert_inventory, list_pools_json, put_inventory,
    put_inventory_retrying, start_decommission, start_rebalance, unique_bucket, wait_for_decommission_active,
    wait_for_decommission_complete, wait_for_rebalance_active, wait_for_rebalance_complete,
};
use crate::common::init_logging;
use std::time::Duration;

#[tokio::test]
async fn four_node_pool_expand_preserves_objects_then_rebalance() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::SingleNodeFourDrive).await?;
    let bucket = unique_bucket("expand");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(0)?;
    let inventory = put_inventory(&client, &bucket, 64, 256 * 1024).await?;
    assert_inventory(&client, &bucket, &inventory).await?;

    for expected_nodes in 2..=4 {
        let new_node = dist.append_pool_and_restart().await?;
        assert_eq!(new_node + 1, expected_nodes);
        assert_inventory(&dist.client(new_node)?, &bucket, &inventory).await?;
    }
    assert_eq!(dist.cluster.nodes.len(), 4);

    // Prove that the expanded pool map is durable, and clear any recovery
    // latch raised while the newly-added pool replicas converged.
    dist.restart_current_binary_gracefully().await?;

    let after_expand = dist.client(0)?;
    assert_inventory(&after_expand, &bucket, &inventory).await?;
    let peer = dist.client(3)?;
    assert_inventory(&peer, &bucket, &inventory).await?;

    let rebalance_id = start_rebalance(&dist.cluster).await?;
    wait_for_rebalance_active(&dist.cluster, &rebalance_id, Duration::from_secs(30)).await?;
    wait_for_rebalance_complete(&dist.cluster, &rebalance_id, Duration::from_secs(180)).await?;
    assert_inventory(&peer, &bucket, &inventory).await?;
    Ok(())
}

#[tokio::test]
async fn four_pool_decommission_moves_objects_without_loss() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::SingleNodeFourDrive).await?;
    let bucket = unique_bucket("decom");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(0)?;
    let inventory = put_inventory_retrying(&client, &bucket, 96, 128 * 1024, Duration::from_secs(30)).await?;
    dist.expand_to_four_pools().await?;

    let pools_before = list_pools_json(&dist.cluster).await?;
    let pool_count = pools_before
        .as_array()
        .map(Vec::len)
        .or_else(|| pools_before.get("pools").and_then(serde_json::Value::as_array).map(Vec::len))
        .ok_or_else(|| format!("pool list omitted an array: {pools_before}"))?;
    assert_eq!(pool_count, 4, "expected exactly four pools before decommission: {pools_before}");

    start_decommission(&dist.cluster, DECOMMISSION_POOL_ID).await?;
    wait_for_decommission_active(&dist.cluster, DECOMMISSION_POOL_ID, Duration::from_secs(30)).await?;
    wait_for_decommission_complete(&dist.cluster, DECOMMISSION_POOL_ID, Duration::from_secs(180)).await?;

    let after = dist.client(2)?;
    assert_inventory(&after, &bucket, &inventory).await?;
    Ok(())
}
