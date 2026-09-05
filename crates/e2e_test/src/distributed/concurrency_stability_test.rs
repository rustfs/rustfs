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
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::test]
async fn four_node_high_concurrency_mixed_workload_is_consistent_on_every_node() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("conc");
    dist.create_bucket(&bucket).await?;
    let clients = Arc::new(dist.clients()?);
    let worker_count = 24;
    let rounds = 4;
    let barrier = Arc::new(Barrier::new(worker_count));

    let mut handles = Vec::new();
    for idx in 0..worker_count {
        let clients = clients.clone();
        let barrier = barrier.clone();
        let bucket = bucket.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let writer = &clients[idx % clients.len()];
            let reader = &clients[(idx + 1) % clients.len()];
            let copier = &clients[(idx + 2) % clients.len()];
            let mut retained = Vec::with_capacity(rounds);
            for round in 0..rounds {
                let key = format!("source/worker-{idx:02}-round-{round}.bin");
                let copy_key = format!("retained/worker-{idx:02}-round-{round}.bin");
                let body = payload_for(&key, 64 * 1024);
                put_object(writer, &bucket, &key, body.clone()).await?;

                let head = reader.head_object().bucket(&bucket).key(&key).send().await?;
                if head.content_length() != Some(body.len() as i64) {
                    return Err(format!("HEAD returned the wrong size for {key}: {head:?}").into());
                }
                assert_object_bytes(reader, &bucket, &key, &body).await?;

                copier
                    .copy_object()
                    .bucket(&bucket)
                    .key(&copy_key)
                    .copy_source(format!("{bucket}/{key}"))
                    .send()
                    .await?;
                assert_object_bytes(writer, &bucket, &copy_key, &body).await?;

                writer.delete_object().bucket(&bucket).key(&key).send().await?;
                let missing = reader
                    .head_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .expect_err("deleted source key must not remain visible");
                if missing.raw_response().map(|response| response.status().as_u16()) != Some(404) {
                    return Err(format!("deleted source {key} returned an unexpected result: {missing:?}").into());
                }
                retained.push((copy_key, body));
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(retained)
        }));
    }

    let mut inventory = Vec::new();
    for handle in handles {
        inventory.extend(handle.await??);
    }

    let expected_keys: BTreeSet<_> = inventory.iter().map(|(key, _)| key.as_str()).collect();
    for (node_idx, client) in clients.iter().enumerate() {
        let listed = client.list_objects_v2().bucket(&bucket).prefix("retained/").send().await?;
        let listed_keys: BTreeSet<_> = listed.contents().iter().filter_map(|object| object.key()).collect();
        assert_eq!(listed_keys, expected_keys, "node {node_idx} returned a divergent retained-key listing");
        for (key, body) in &inventory {
            assert_object_bytes(client, &bucket, key, body)
                .await
                .map_err(|error| format!("node {node_idx} failed to read {key}: {error}"))?;
        }
    }
    Ok(())
}
