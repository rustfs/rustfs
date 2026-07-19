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
//! Out of scope for this block (tracked separately): network fault injection
//! (toxiproxy / socket proxy) and 5GiB large-object budgets.

use crate::common::{ClusterTopology, RustFSTestClusterEnvironment};
use serial_test::serial;

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
#[serial]
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

/// Two single-node pools, 2 drives each: the multi-pool layout boots and
/// round-trips. Every pool is a distinct erasure pool (`pool_idx` 0 and 1).
#[tokio::test]
#[serial]
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
