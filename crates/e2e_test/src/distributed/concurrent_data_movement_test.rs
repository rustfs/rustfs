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
    DistCluster, DistLayout, TestResult, assert_inventory, payload_for, put_inventory, retrying_get_equals, retrying_put,
    start_decommission, unique_bucket, wait_for_decommission_complete,
};
use crate::common::init_logging;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;

#[tokio::test]
async fn concurrent_puts_during_decommission_do_not_lose_baseline_or_new_objects() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourPoolFourDrive).await?;
    let bucket = unique_bucket("concdecom");
    dist.create_bucket(&bucket).await?;
    let baseline_client = dist.client(0)?;
    let inventory = put_inventory(&baseline_client, &bucket, 10, 24 * 1024).await?;

    start_decommission(&dist.cluster, 0).await?;

    let clients = Arc::new(dist.clients()?);
    let barrier = Arc::new(Barrier::new(16));
    let mut handles = Vec::new();
    for idx in 0..16 {
        let clients = clients.clone();
        let barrier = barrier.clone();
        let bucket = bucket.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let client = &clients[idx % clients.len()];
            let key = format!("live/{idx:02}.bin");
            let body = payload_for(&key, 8 * 1024);
            retrying_put(client, &bucket, &key, body.clone(), Duration::from_secs(45)).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((key, body))
        }));
    }

    let mut live_objects = Vec::new();
    for handle in handles {
        live_objects.push(handle.await??);
    }

    wait_for_decommission_complete(&dist.cluster, 0, Duration::from_secs(180)).await?;

    let checker = dist.client(3)?;
    assert_inventory(&checker, &bucket, &inventory).await?;
    for (key, body) in live_objects {
        retrying_get_equals(&checker, &bucket, &key, &body, Duration::from_secs(30)).await?;
    }
    Ok(())
}
