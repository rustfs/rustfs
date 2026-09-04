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
    DistCluster, DistLayout, TestResult, assert_inventory, decommission_started_or_fenced, put_inventory_retrying, sha256_hex,
    unique_bucket, wait_for_decommission_complete,
};
use crate::common::init_logging;
use std::time::Duration;

#[tokio::test]
async fn decommission_does_not_alter_object_sha256_across_pools() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::TwoPoolFourDrive).await?;
    let bucket = unique_bucket("integrity");
    dist.create_bucket(&bucket).await?;
    let client = dist.client(0)?;
    let inventory = put_inventory_retrying(&client, &bucket, 20, 64 * 1024, Duration::from_secs(30)).await?;
    let before: Vec<(String, String)> = inventory.iter().map(|(key, body)| (key.clone(), sha256_hex(body))).collect();

    if decommission_started_or_fenced(&dist.cluster, 0).await? {
        wait_for_decommission_complete(&dist.cluster, 0, Duration::from_secs(180)).await?;
    }

    let after_client = dist.client(1)?;
    assert_inventory(&after_client, &bucket, &inventory).await?;
    for (key, expected_hash) in before {
        let got = after_client.get_object().bucket(&bucket).key(&key).send().await?;
        let body = got.body.collect().await?.into_bytes();
        assert_eq!(sha256_hex(body.as_ref()), expected_hash, "checksum changed for {key} after decommission");
    }
    Ok(())
}
