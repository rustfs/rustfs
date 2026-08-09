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

//! Black-box replacement recovery across a 3-node, 4-drive-per-node set.

use crate::chaos::{VersionShardCensus, census_object_version_on_disk, replacement_resume_state_for_target};
use crate::common::{ClusterTopology, RustFSTestClusterEnvironment, init_logging};
use aws_sdk_s3::primitives::ByteStream;
use serial_test::serial;
use std::error::Error;
use std::path::PathBuf;
use tokio::time::{Duration, Instant, interval};
use tracing::info;

fn payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u64).wrapping_mul(2_654_435_761).wrapping_add(u64::from(seed)) as u8)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn automatic_replacement_rebuilds_a_rebooted_disk_in_a_three_by_four_set() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();
    info!("Issue #1791: automatic replacement rebuilds a rebooted drive in a 3x4 set without an admin heal request");

    let mut cluster = RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(3, 4)).await?;
    cluster.set_env("RUSTFS_HEAL_INTERVAL_SECS", "10");
    // The local harness creates every drive under one temporary filesystem. The
    // debug-only readiness override keeps that topology limitation out of the
    // production admission contract while still exercising the real scanner,
    // intent, format and erasure-heal paths.
    cluster.set_env("RUSTFS_TEST_AUTO_REPLACEMENT_READINESS_BYPASS", "1");
    cluster.start().await?;

    let client = cluster.create_s3_client(0)?;
    let bucket = "replacement-auto-3x4";
    client.create_bucket().bucket(bucket).send().await?;

    let objects = (0..8)
        .map(|index| (format!("replacement/object-{index:02}.bin"), payload(512 * 1024, index as u8)))
        .collect::<Vec<_>>();
    for (key, body) in &objects {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await?;
    }

    let target_node = 1;
    let target_drive = 2;
    let target_disk = PathBuf::from(&cluster.nodes[target_node].data_dirs[target_drive]);
    let expected = objects
        .iter()
        .map(|(key, _)| {
            let census = census_object_version_on_disk(&target_disk, bucket, key, None)?;
            if !census.is_complete() {
                return Err(format!("target disk has incomplete baseline census for {key}: {census:?}").into());
            }
            Ok::<(String, VersionShardCensus), Box<dyn Error + Send + Sync>>((key.clone(), census))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Swap the configured path while the node is alive. Its mount lease keeps
    // the old instance fenced until reboot; the empty path is not allowed to
    // become a replacement target during this runtime-detach window.
    let detached_disk = target_disk.with_extension("detached");
    std::fs::rename(&target_disk, &detached_disk)?;
    std::fs::create_dir_all(&target_disk)?;
    assert!(
        !target_disk.join(".rustfs.sys").join("format.json").exists(),
        "the runtime replacement path must remain blank before reboot"
    );

    // Restart only the affected node. There is intentionally no admin heal
    // request in this test: the automatic scanner must create a durable intent
    // and reconstruct every selected physical shard itself.
    cluster.stop_node(target_node)?;
    cluster.start_node(target_node).await?;

    let survivors = cluster
        .nodes
        .iter()
        .flat_map(|node| node.data_dirs.iter())
        .map(PathBuf::from)
        .filter(|disk| disk != &target_disk)
        .collect::<Vec<_>>();
    let scanner_deadline = Instant::now() + Duration::from_secs(90);
    let mut scanner_poll = interval(Duration::from_millis(50));
    loop {
        if let Some(state) = replacement_resume_state_for_target(&survivors, &target_disk)? {
            assert!(
                matches!(state["replacement_phase"].as_str(), Some("intent") | Some("rebuilding")),
                "scanner must persist an active replacement intent before the rebuild completes: {state}"
            );
            assert!(
                state["replacement_generation"].as_str().is_some(),
                "automatic replacement intent must have a durable generation: {state}"
            );
            break;
        }
        if Instant::now() >= scanner_deadline {
            return Err("automatic scanner did not persist a replacement intent within 90s".into());
        }
        scanner_poll.tick().await;
    }

    let rebuild_deadline = Instant::now() + Duration::from_secs(180);
    let mut rebuild_poll = interval(Duration::from_millis(100));
    loop {
        let complete = expected.iter().all(|(key, manifest)| {
            census_object_version_on_disk(&target_disk, bucket, key, None)
                .map(|census| census.matches_manifest(manifest))
                .unwrap_or(false)
        });
        if complete {
            return Ok(());
        }
        if Instant::now() >= rebuild_deadline {
            let incomplete = expected
                .iter()
                .filter_map(|(key, manifest)| {
                    census_object_version_on_disk(&target_disk, bucket, key, None)
                        .ok()
                        .filter(|census| !census.matches_manifest(manifest))
                        .map(|census| format!("{key}: {census:?}"))
                })
                .collect::<Vec<_>>();
            return Err(format!("automatic replacement left incomplete physical shards: {incomplete:?}").into());
        }
        rebuild_poll.tick().await;
    }
}
