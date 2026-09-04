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
    DECOMMISSION_POOL_ID, DistCluster, DistLayout, TestResult, assert_inventory, put_inventory_retrying, retrying_get_equals,
    retrying_put, start_decommission, start_rebalance, unique_bucket, wait_for_decommission_active,
    wait_for_decommission_complete, wait_for_rebalance_active, wait_for_rebalance_complete,
};
use crate::common::init_logging;
use std::time::Duration;

#[tokio::test]
async fn s3_put_get_list_succeed_during_decommission_and_rebalance() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::SingleNodeFourDrive).await?;
    let bucket = unique_bucket("s3move");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(0)?;
    let inventory = put_inventory_retrying(&client, &bucket, 96, 256 * 1024, Duration::from_secs(30)).await?;
    dist.expand_to_four_pools().await?;

    start_decommission(&dist.cluster, DECOMMISSION_POOL_ID).await?;
    wait_for_decommission_active(&dist.cluster, DECOMMISSION_POOL_ID, Duration::from_secs(30)).await?;
    let live = dist.client(2)?;
    retrying_put(
        &live,
        &bucket,
        "during-decommission.bin",
        b"written-while-decommissioning".to_vec(),
        Duration::from_secs(30),
    )
    .await?;
    retrying_get_equals(
        &live,
        &bucket,
        "during-decommission.bin",
        b"written-while-decommissioning",
        Duration::from_secs(30),
    )
    .await?;
    let listed = live.list_objects_v2().bucket(&bucket).send().await?;
    assert!(
        listed
            .contents()
            .iter()
            .any(|object| object.key() == Some("during-decommission.bin")),
        "list during decommission missed the newly written key"
    );

    wait_for_decommission_complete(&dist.cluster, DECOMMISSION_POOL_ID, Duration::from_secs(180)).await?;
    assert_inventory(&live, &bucket, &inventory).await?;

    let rebalance_id = start_rebalance(&dist.cluster).await?;
    wait_for_rebalance_active(&dist.cluster, &rebalance_id, Duration::from_secs(30)).await?;
    retrying_put(
        &live,
        &bucket,
        "during-rebalance.bin",
        b"written-while-rebalancing".to_vec(),
        Duration::from_secs(30),
    )
    .await?;
    retrying_get_equals(
        &live,
        &bucket,
        "during-rebalance.bin",
        b"written-while-rebalancing",
        Duration::from_secs(30),
    )
    .await?;
    wait_for_rebalance_complete(&dist.cluster, &rebalance_id, Duration::from_secs(180)).await?;
    assert_inventory(&dist.client(1)?, &bucket, &inventory).await?;
    Ok(())
}
