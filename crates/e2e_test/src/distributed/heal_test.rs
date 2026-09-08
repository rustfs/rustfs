// Copyright 2026 RustFS Team
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

use super::harness::{DistCluster, DistLayout, TestResult, assert_inventory, payload_for, put_object, unique_bucket, wait_until};
use crate::chaos::{VersionShardCensus, census_object_version_on_disk, signed_admin_post};
use crate::common::init_logging;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

const EC84_NODE_COUNT: usize = 3;
const EC84_DRIVES_PER_NODE: usize = 4;
const EC84_DATA_BLOCKS: usize = 8;
const EC84_PARITY_BLOCKS: usize = 4;

#[derive(Clone)]
struct ExpectedShard {
    key: String,
    body: Vec<u8>,
    baseline: VersionShardCensus,
}

fn assert_ec84_geometry(census: &VersionShardCensus, key: &str) -> TestResult {
    if census.data_blocks != Some(EC84_DATA_BLOCKS) || census.parity_blocks != Some(EC84_PARITY_BLOCKS) {
        return Err(format!("object {key} did not use EC8+4 geometry: {census:?}").into());
    }
    let erasure_index = census
        .erasure_index
        .ok_or_else(|| format!("object {key} did not record an erasure index: {census:?}"))?;
    if !(1..=EC84_DATA_BLOCKS + EC84_PARITY_BLOCKS).contains(&erasure_index) {
        return Err(format!("object {key} has out-of-range erasure index {erasure_index}: {census:?}").into());
    }
    if !census.is_complete() || census.expected_part_numbers.is_empty() {
        return Err(format!("object {key} does not have complete physical shard evidence: {census:?}").into());
    }
    Ok(())
}

fn assert_replaced_drive_empty(drive: &Path, bucket: &str, keys: &[String]) -> TestResult {
    for key in keys {
        let census = census_object_version_on_disk(drive, bucket, key, None)?;
        if census.has_xl_meta {
            return Err(format!("replacement drive unexpectedly retained {bucket}/{key}: {census:?}").into());
        }
    }
    Ok(())
}

async fn put_large_inventory(client: &Client, bucket: &str) -> TestResult<Vec<ExpectedShard>> {
    let mut expected = Vec::new();
    for index in 0..4 {
        let key = format!("ec84/prefix-{}/object-{index:04}.bin", index % 2);
        let body = payload_for(&key, 10 * 1024 * 1024);
        put_object(client, bucket, &key, body.clone()).await?;
        expected.push(ExpectedShard {
            key,
            body,
            baseline: VersionShardCensus {
                version_id: None,
                has_xl_meta: false,
                data_dir: None,
                erasure_index: None,
                data_blocks: None,
                parity_blocks: None,
                expected_part_numbers: Default::default(),
                present_part_fingerprints: Default::default(),
                inline_data_fingerprint: None,
            },
        });
    }
    Ok(expected)
}

#[tokio::test]
async fn three_node_four_drive_ec8_4_root_heal_rebuilds_replaced_drive_after_restart() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start_with_env(
        DistLayout::ThreeByFourEc84,
        &[
            ("RUSTFS_STORAGE_CLASS_STANDARD", "EC:4"),
            ("RUSTFS_HEAL_ENABLED", "true"),
            ("RUSTFS_HEAL_AUTO_HEAL_ENABLE", "false"),
            ("RUSTFS_HEAL_MRF_ENABLE", "false"),
            ("RUSTFS_SCANNER_ENABLED", "false"),
        ],
    )
    .await?;
    assert_eq!(dist.cluster.nodes.len(), EC84_NODE_COUNT);
    assert_eq!(dist.cluster.topology.drives_per_node, EC84_DRIVES_PER_NODE);

    let bucket = unique_bucket("healec84");
    dist.create_bucket(&bucket).await?;
    let writer = dist.client(0)?;
    let mut expected = put_large_inventory(&writer, &bucket).await?;
    let replaced_node = 1;
    let replaced_drive_index = 2;
    let replaced_drive = PathBuf::from(&dist.cluster.nodes[replaced_node].data_dirs[replaced_drive_index]);

    for item in &mut expected {
        item.baseline = census_object_version_on_disk(&replaced_drive, &bucket, &item.key, None)?;
        assert_ec84_geometry(&item.baseline, &item.key)?;
    }

    let format_path = replaced_drive.join(".rustfs.sys").join("format.json");
    let format_json = std::fs::read(&format_path)?;
    dist.cluster.stop_node_gracefully(replaced_node).await?;
    let retired_drive = PathBuf::from(format!("{}.retired", replaced_drive.display()));
    std::fs::rename(&replaced_drive, &retired_drive)?;
    std::fs::create_dir_all(format_path.parent().ok_or("replacement format path has no parent")?)?;
    std::fs::write(&format_path, format_json)?;
    assert_replaced_drive_empty(
        &replaced_drive,
        &bucket,
        &expected.iter().map(|item| item.key.clone()).collect::<Vec<_>>(),
    )?;

    let outage_key = "ec84/written-while-node-restarting.bin";
    let outage_body = payload_for(outage_key, 10 * 1024 * 1024);
    writer
        .put_object()
        .bucket(&bucket)
        .key(outage_key)
        .body(ByteStream::from(outage_body.clone()))
        .send()
        .await?;

    dist.cluster.start_node(replaced_node).await?;
    let heal_body =
        r#"{"recursive":true,"dryRun":false,"remove":false,"recreate":true,"scanMode":2,"updateParity":false,"nolock":false}"#;
    let heal_url = format!("{}/rustfs/admin/v3/heal/{bucket}?forceStart=true", dist.cluster.nodes[0].url);
    signed_admin_post(&heal_url, Some(heal_body), &dist.cluster.access_key, &dist.cluster.secret_key).await?;

    wait_until(
        Duration::from_secs(120),
        || async {
            for item in &expected {
                let current = census_object_version_on_disk(&replaced_drive, &bucket, &item.key, None)?;
                if !current.matches_manifest(&item.baseline) {
                    return Ok(false);
                }
            }
            let outage = census_object_version_on_disk(&replaced_drive, &bucket, outage_key, None)?;
            Ok(outage.is_complete()
                && outage.data_blocks == Some(EC84_DATA_BLOCKS)
                && outage.parity_blocks == Some(EC84_PARITY_BLOCKS))
        },
        "EC8+4 replacement drive rebuilt baseline and outage shards",
    )
    .await?;

    let inventory = expected
        .iter()
        .map(|item| (item.key.clone(), item.body.clone()))
        .chain(std::iter::once((outage_key.to_string(), outage_body.clone())))
        .collect::<BTreeMap<_, _>>();
    let expected_keys = inventory.keys().cloned().collect::<HashSet<_>>();
    for node_index in 0..dist.cluster.nodes.len() {
        let client = dist.client(node_index)?;
        assert_inventory(&client, &bucket, &inventory).await?;
        let listing = client.list_objects_v2().bucket(&bucket).send().await?;
        let observed = listing
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_owned))
            .collect::<HashSet<_>>();
        assert_eq!(observed, expected_keys, "node {node_index} listing diverged after EC8+4 heal");
    }

    Ok(())
}
