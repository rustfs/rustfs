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

use super::harness::{DistCluster, DistLayout, TestResult, assert_object_bytes, payload_for, put_object, unique_bucket};
use crate::common::init_logging;
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::test]
async fn four_node_high_concurrency_puts_are_readable_from_every_node() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("conc");
    dist.create_bucket(&bucket).await?;
    let clients = Arc::new(dist.clients()?);
    let barrier = Arc::new(Barrier::new(32));

    let mut handles = Vec::new();
    for idx in 0..32 {
        let clients = clients.clone();
        let barrier = barrier.clone();
        let bucket = bucket.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let client = &clients[idx % clients.len()];
            let key = format!("c/{idx:02}.bin");
            let body = payload_for(&key, 16 * 1024);
            put_object(client, &bucket, &key, body.clone()).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((key, body))
        }));
    }

    let mut inventory = Vec::new();
    for handle in handles {
        inventory.push(handle.await??);
    }

    for (node_idx, client) in clients.iter().enumerate() {
        for (key, body) in &inventory {
            assert_object_bytes(client, &bucket, key, body)
                .await
                .map_err(|error| format!("node {node_idx} failed to read {key}: {error}"))?;
        }
    }
    Ok(())
}
