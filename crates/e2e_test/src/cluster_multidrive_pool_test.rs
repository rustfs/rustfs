// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Smoke tests for the multi-drive / multi-pool cluster harness.
//!
//! These belong to the nightly 4-node cluster lane (they spin up real RustFS
//! processes, so they are excluded from the merge-gate `e2e-full` profile in
//! `.config/nextest.toml`). They assert that:
//!
//! * `drivesPerNode > 1` produces a bootable single-pool cluster whose
//!   `RUSTFS_VOLUMES` string enumerates every drive, and that a PUT/GET
//!   round-trips through the multi-drive erasure layout.
//! * A two-pool topology (one node per pool, `drivesPerNode` drives each) boots
//!   with the ellipses `RUSTFS_VOLUMES` form and round-trips a PUT/GET.
//!
//! Readiness is established by the harness's `start()` handshake (TCP reachability
//! plus an S3 `ListBuckets` poll) — there are no fixed sleeps.
//!
//! The volume-proxy smoke below also proves that the socket-level fault proxy
//! can be installed before startup without changing the client-facing node URL.
//! A full lock-plane partition matrix and 5GiB large-object budget remain
//! tracked separately.

use crate::common::{ClusterTopology, RustFSTestClusterEnvironment};
use crate::fault_proxy::FaultMode;
use aws_sdk_s3::Client;
use std::time::Duration;
use tokio::time::{Instant, timeout};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const BUCKET: &str = "cluster-multidrive-pool-bucket";

/// PUT an object then GET it back and assert the bytes round-trip.
async fn put_get_roundtrip(cluster: &RustFSTestClusterEnvironment, key: &str, payload: &[u8]) -> TestResult {
    let writer = cluster.create_s3_client(0)?;
    writer
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(bytes::Bytes::copy_from_slice(payload).into())
        .send()
        .await?;

    // Read back from the last node to exercise the cross-node/cross-pool path.
    let reader = cluster.create_s3_client(cluster.nodes.len() - 1)?;
    let got = reader.get_object().bucket(BUCKET).key(key).send().await?;
    let body = got.body.collect().await?.into_bytes();
    assert_eq!(body.as_ref(), payload, "round-tripped object bytes must match");
    Ok(())
}

/// 4 nodes x 2 drives, single pool: the multi-drive layout boots and round-trips.
#[tokio::test]
async fn cluster_multidrive_single_pool_smoke() -> TestResult {
    crate::common::init_logging();

    let mut cluster = RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(4, 2)).await?;

    // The single-pool multi-drive layout must list every (node, drive) endpoint
    // explicitly (8 endpoints, no ellipses) so the server keeps one legacy pool.
    let volumes = cluster.rustfs_volumes_arg();
    assert_eq!(volumes.split(' ').count(), 8, "expected 8 explicit endpoints, got: {volumes}");
    assert!(!volumes.contains('{'), "single-pool layout must not use ellipses: {volumes}");

    cluster.start().await?;
    cluster.create_test_bucket(BUCKET).await?;

    let payload = vec![0xA5u8; 512 * 1024];
    put_get_roundtrip(&cluster, "multidrive/object", &payload).await?;
    Ok(())
}

/// 4 nodes x 4 drives, single pool: exercise the maximum local erasure layout
/// supported by the cluster harness. This remains in the nightly lane because
/// it starts four real server processes and sixteen data directories.
#[tokio::test]
async fn cluster_four_node_four_drive_single_pool_smoke() -> TestResult {
    crate::common::init_logging();

    let mut cluster = RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(4, 4)).await?;

    let volumes = cluster.rustfs_volumes_arg();
    assert_eq!(volumes.split(' ').count(), 16, "expected 16 explicit endpoints, got: {volumes}");
    assert!(!volumes.contains('{'), "single-pool layout must not use ellipses: {volumes}");
    assert!(cluster.nodes.iter().all(|node| node.data_dirs.len() == 4));

    cluster.start().await?;
    cluster.create_test_bucket(BUCKET).await?;

    let payload = vec![0x3Cu8; 1024 * 1024];
    put_get_roundtrip(&cluster, "multidrive-4/object", &payload).await?;
    Ok(())
}

/// Two single-node pools, 2 drives each: the multi-pool layout boots and
/// round-trips. Every pool is a distinct erasure pool (`pool_idx` 0 and 1).
#[tokio::test]
async fn cluster_two_pool_smoke() -> TestResult {
    crate::common::init_logging();

    let mut cluster =
        RustFSTestClusterEnvironment::with_topology(ClusterTopology::per_node_pools(2, vec![vec![0], vec![1]])).await?;

    // The two-pool layout must emit one ellipses argument per pool.
    let volumes = cluster.rustfs_volumes_arg();
    let args: Vec<&str> = volumes.split(' ').collect();
    assert_eq!(args.len(), 2, "expected two pool arguments, got: {volumes}");
    assert!(
        args.iter().all(|a| a.contains("/drive{0...1}")),
        "each pool arg must use the drive ellipses form: {volumes}"
    );
    assert_eq!(cluster.nodes[0].pool_idx, 0);
    assert_eq!(cluster.nodes[1].pool_idx, 1);

    cluster.start().await?;
    cluster.create_test_bucket(BUCKET).await?;

    let payload = vec![0x5Au8; 256 * 1024];
    put_get_roundtrip(&cluster, "twopool/object", &payload).await?;
    Ok(())
}

/// A real cluster smoke for the volume FaultProxy wiring. The proxy target is
/// not listening yet when it is created; cluster startup must still converge
/// once the target node starts, and peer disk/RPC traffic must traverse it.
#[tokio::test]
async fn cluster_volume_fault_proxy_pass_smoke() -> TestResult {
    crate::common::init_logging();

    let mut cluster = RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(2, 2)).await?;
    let proxy = cluster.start_volume_proxy_for_node(0).await?;
    let proxied = proxy.local_addr().to_string();
    assert!(cluster.rustfs_volumes_arg().contains(&proxied));

    let result: TestResult = async {
        cluster.start().await?;
        cluster.create_test_bucket(BUCKET).await?;
        let payload = vec![0x6Du8; 256 * 1024];
        put_get_roundtrip(&cluster, "volume-proxy/object", &payload).await?;

        // Node 1 owns only two disks; a successful three-vote write must
        // authenticate at least one disk mutation through node 0's proxy.
        let client = cluster.create_s3_client(1)?;
        let client = Client::from_conf(
            client
                .config()
                .to_builder()
                .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
                .build(),
        );
        let forwarded_before = proxy.forwarded_bytes();
        client
            .put_object()
            .bucket(BUCKET)
            .key("volume-proxy/authenticated")
            .body(bytes::Bytes::copy_from_slice(&payload).into())
            .send()
            .await?;
        assert!(
            proxy.forwarded_bytes() > forwarded_before,
            "successful peer-disk publication must traverse the proxy"
        );

        let dropped_before = proxy.dropped_bytes();
        proxy.set_mode(FaultMode::Blackhole);
        assert_eq!(proxy.mode(), FaultMode::Blackhole);
        let error = timeout(
            Duration::from_secs(30),
            client
                .put_object()
                .bucket(BUCKET)
                .key("volume-proxy/rejected")
                .body(bytes::Bytes::copy_from_slice(&payload).into())
                .send(),
        )
        .await?
        .expect_err("two local disks cannot commit a write while the peer is blackholed");
        assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(503));
        assert_eq!(error.as_service_error().and_then(|error| error.meta().code()), Some("ServiceUnavailable"));
        assert!(
            proxy.dropped_bytes() > dropped_before,
            "the injected blackhole must discard actual peer traffic"
        );

        proxy.set_mode(FaultMode::Pass);
        assert_eq!(proxy.mode(), FaultMode::Pass);
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let response = http.get(format!("{}/health/ready", cluster.nodes[1].url)).send().await?;
            let status = response.status();
            let body: serde_json::Value = response.json().await?;
            // Reachable peers can still have Returning disk handles that reject
            // writes. Wait for this node's live writer inventory to be Online.
            if status.as_u16() == 200
                && body["details"]["storage"]["source"] == "local_runtime"
                && body["details"]["storage"]["writeQuorum"] == true
                && body["details"]["storage"]["unavailableDrives"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
                && body["details"]["lock"]["ready"] == true
            {
                break;
            }
            assert!(Instant::now() < deadline, "peer write quorum did not recover: HTTP {status}, {body}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        client
            .put_object()
            .bucket(BUCKET)
            .key("volume-proxy/recovered")
            .body(bytes::Bytes::copy_from_slice(&payload).into())
            .send()
            .await?;
        for key in ["volume-proxy/object", "volume-proxy/authenticated", "volume-proxy/recovered"] {
            let got = client.get_object().bucket(BUCKET).key(key).send().await?;
            let body = got.body.collect().await?.into_bytes();
            assert_eq!(body.as_ref(), payload, "proxy recovery must preserve the full object body for {key}");
        }
        let rejected = client
            .head_object()
            .bucket(BUCKET)
            .key("volume-proxy/rejected")
            .send()
            .await
            .expect_err("a rejected proxied write must never become visible after recovery");
        assert_eq!(rejected.raw_response().map(|response| response.status().as_u16()), Some(404));
        Ok(())
    }
    .await;

    proxy.shutdown().await;
    result
}
