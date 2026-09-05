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
    DECOMMISSION_POOL_ID, DistCluster, DistLayout, TestResult, assert_inventory, decommission_running_with_progress,
    decommission_status_json, put_inventory_retrying, rebalance_running_with_progress, rebalance_status_json,
    retrying_get_equals, retrying_put, start_decommission, start_rebalance, unique_bucket, wait_for_decommission_complete,
    wait_for_decommission_running_with_progress, wait_for_rebalance_complete, wait_for_rebalance_running_with_progress,
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
    wait_for_decommission_running_with_progress(&dist.cluster, DECOMMISSION_POOL_ID, Duration::from_secs(30)).await?;
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
    let status = decommission_status_json(&dist.cluster).await?;
    if !decommission_running_with_progress(&status, DECOMMISSION_POOL_ID)? {
        return Err(format!("decommission did not remain active across the S3 operations: {status}").into());
    }

    wait_for_decommission_complete(&dist.cluster, DECOMMISSION_POOL_ID, Duration::from_secs(180)).await?;
    assert_inventory(&live, &bucket, &inventory).await?;

    let rebalance_id = start_rebalance(&dist.cluster).await?;
    wait_for_rebalance_running_with_progress(&dist.cluster, &rebalance_id, Duration::from_secs(30)).await?;
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
    let status = rebalance_status_json(&dist.cluster).await?;
    if !rebalance_running_with_progress(&status, &rebalance_id)? {
        return Err(format!("rebalance did not remain active across the S3 operations: {status}").into());
    }
    wait_for_rebalance_complete(&dist.cluster, &rebalance_id, Duration::from_secs(180)).await?;
    assert_inventory(&dist.client(1)?, &bucket, &inventory).await?;
    Ok(())
}
