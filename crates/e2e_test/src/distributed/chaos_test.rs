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
    DistCluster, DistLayout, TestResult, assert_object_bytes, bring_drive_online, put_object, retrying_get_equals,
    take_drive_offline, unique_bucket, wait_for_ready,
};
use crate::common::init_logging;
use crate::fault_proxy::FaultMode;
use std::time::Duration;

#[tokio::test]
async fn kill_and_restart_node_preserves_objects() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("killnode");
    dist.create_bucket(&bucket).await?;
    let body = vec![0x11u8; 128 * 1024];
    put_object(&dist.client(0)?, &bucket, "keep.bin", body.clone()).await?;

    dist.cluster.stop_node(3)?;
    retrying_get_equals(&dist.client(0)?, &bucket, "keep.bin", &body, Duration::from_secs(20)).await?;

    dist.cluster.start_node(3).await?;
    wait_for_ready(&dist.cluster).await?;
    assert_object_bytes(&dist.client(3)?, &bucket, "keep.bin", &body).await?;
    Ok(())
}

#[tokio::test]
async fn full_cluster_restart_preserves_objects() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("pwr");
    dist.create_bucket(&bucket).await?;
    let body = vec![0x44u8; 64 * 1024];
    put_object(&dist.client(1)?, &bucket, "survive.bin", body.clone()).await?;

    dist.cluster.stop();
    dist.cluster.start().await?;
    wait_for_ready(&dist.cluster).await?;
    for node_idx in 0..dist.cluster.nodes.len() {
        assert_object_bytes(&dist.client(node_idx)?, &bucket, "survive.bin", &body).await?;
    }
    Ok(())
}

#[tokio::test]
async fn offline_drive_then_replace_keeps_object_readable() -> TestResult {
    init_logging();
    let dist = DistCluster::start(DistLayout::FourByFour).await?;
    let bucket = unique_bucket("baddrive");
    dist.create_bucket(&bucket).await?;
    let body = vec![0x22u8; 96 * 1024];
    put_object(&dist.client(1)?, &bucket, "durable.bin", body.clone()).await?;

    take_drive_offline(&dist.cluster, 0, 0)?;
    retrying_get_equals(&dist.client(2)?, &bucket, "durable.bin", &body, Duration::from_secs(20)).await?;
    bring_drive_online(&dist.cluster, 0, 0)?;
    retrying_get_equals(&dist.client(3)?, &bucket, "durable.bin", &body, Duration::from_secs(20)).await?;
    Ok(())
}

#[tokio::test]
async fn volume_proxy_blackhole_then_restore_keeps_s3_available() -> TestResult {
    init_logging();
    // 4 nodes × 1 drive (4-disk DistErasure). Proxying a 16-disk 4×4 set
    // prevents first-disk format: proxied drives look like missing peers, so
    // `should_init_erasure_disks` is false and the first disk waits out.
    let mut cluster =
        crate::common::RustFSTestClusterEnvironment::with_topology(crate::common::ClusterTopology::single_pool(4)).await?;
    let proxy = cluster.start_volume_proxy_for_node(1).await?;
    cluster.start().await?;
    cluster.create_test_bucket("chaos-net").await?;
    let client = cluster.create_s3_client(0)?;
    let body = vec![0x33u8; 32 * 1024];
    put_object(&client, "chaos-net", "via-proxy.bin", body.clone()).await?;

    proxy.set_mode(FaultMode::Blackhole);
    retrying_get_equals(
        &cluster.create_s3_client(2)?,
        "chaos-net",
        "via-proxy.bin",
        &body,
        Duration::from_secs(20),
    )
    .await?;

    proxy.set_mode(FaultMode::Pass);
    assert_object_bytes(&cluster.create_s3_client(3)?, "chaos-net", "via-proxy.bin", &body).await?;
    proxy.shutdown().await;
    Ok(())
}
