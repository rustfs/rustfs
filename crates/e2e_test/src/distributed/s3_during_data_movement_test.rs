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
    DistCluster, TestResult, assert_inventory, decommission_started_or_fenced, put_inventory_retrying,
    rebalance_started_or_fenced, retrying_get_equals, retrying_put, unique_bucket, wait_for_decommission_complete,
};
use crate::common::init_logging;
use std::time::Duration;

#[tokio::test]
async fn s3_put_get_list_succeed_during_decommission_and_rebalance() -> TestResult {
    init_logging();
    let dist = DistCluster::start_four_pool_via_expand().await?;
    let bucket = unique_bucket("s3move");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(0)?;
    let inventory = put_inventory_retrying(&client, &bucket, 8, 16 * 1024, Duration::from_secs(30)).await?;

    let decommission_started = decommission_started_or_fenced(&dist.cluster, 0).await?;
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

    if decommission_started {
        wait_for_decommission_complete(&dist.cluster, 0, Duration::from_secs(180)).await?;
    }
    assert_inventory(&live, &bucket, &inventory).await?;

    let _ = rebalance_started_or_fenced(&dist.cluster).await?;
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
    assert_inventory(&dist.client(1)?, &bucket, &inventory).await?;
    Ok(())
}
