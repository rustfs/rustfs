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

use super::harness::{
    DistCluster, DistLayout, TestResult, assert_inventory, get_object_bytes, payload_for, put_object, sha256_hex, unique_bucket,
    wait_until,
};
use crate::chaos::{VersionShardCensus, census_object_version_on_disk, signed_admin_post};
use crate::common::init_logging;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

const EC84_NODE_COUNT: usize = 3;
const EC84_DRIVES_PER_NODE: usize = 4;
const EC84_DATA_BLOCKS: usize = 8;
const EC84_PARITY_BLOCKS: usize = 4;
const EC84_TARGET_DRIVE_RESTART_CASE: &str = "ec84-target-drive-restart";
const EC84_TARGET_DRIVE_RESTART_ORACLE: &str = "ec84-target-drive-restart.json";

#[derive(Clone)]
struct ExpectedShard {
    key: String,
    body: Vec<u8>,
    baseline: VersionShardCensus,
}

struct ScannerHealEvidenceContext {
    directory: PathBuf,
    run: Value,
}

fn file_sha256(path: &Path) -> TestResult<String> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect())
}

fn compiled_test_identity() -> Value {
    serde_json::json!({
        "source_revision": env!("RUSTFS_E2E_BUILD_COMMIT"),
        "dirty": env!("RUSTFS_E2E_BUILD_DIRTY") != "false",
        "lock_blob": env!("RUSTFS_E2E_BUILD_LOCK"),
        "features": env!("RUSTFS_E2E_BUILD_FEATURES"),
        "target": env!("RUSTFS_E2E_BUILD_TARGET"),
        "profile": env!("RUSTFS_E2E_BUILD_PROFILE"),
        "rustflags_hex": env!("RUSTFS_E2E_BUILD_RUSTFLAGS_HEX"),
    })
}

fn string_field<'a>(value: &'a Value, path: &str) -> TestResult<&'a str> {
    let mut current = value;
    for segment in path.split('.') {
        current = current
            .get(segment)
            .ok_or_else(|| format!("scanner/heal run receipt missing {path}"))?;
    }
    current
        .as_str()
        .filter(|text| !text.is_empty())
        .ok_or_else(|| format!("scanner/heal run receipt has invalid {path}").into())
}

fn scanner_heal_evidence_context() -> TestResult<Option<ScannerHealEvidenceContext>> {
    let Some(directory) = std::env::var_os("RUSTFS_SCANNER_HEAL_RUN_DIR") else {
        return Ok(None);
    };
    let directory = PathBuf::from(directory);
    let receipt = directory.join("run.json");
    if receipt.metadata()?.len() > 1024 * 1024 {
        return Err("oversized scanner/heal execution receipt".into());
    }
    let run: Value = serde_json::from_slice(&std::fs::read(receipt)?)?;
    let built = compiled_test_identity();
    for key in ["source_revision", "dirty", "lock_blob", "features"] {
        if built[key] != run["test_build"][key] {
            return Err(format!("compiled test identity differs for {key}").into());
        }
    }
    let binary_path = PathBuf::from(string_field(&run, "binary.path")?);
    if file_sha256(&binary_path)? != string_field(&run, "binary.sha256")? {
        return Err("server binary must match the run receipt".into());
    }
    if file_sha256(&std::env::current_exe()?)? != string_field(&run, "test_binary.sha256")? {
        return Err("test executable must match the run receipt".into());
    }
    if directory.join(EC84_TARGET_DRIVE_RESTART_ORACLE).exists() {
        return Err("scanner/heal oracle already exists; create a new execution receipt".into());
    }
    Ok(Some(ScannerHealEvidenceContext { directory, run }))
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

async fn write_scanner_heal_evidence(
    context: ScannerHealEvidenceContext,
    dist: &DistCluster,
    bucket: &str,
    expected: &[ExpectedShard],
    outage_key: &str,
    outage_body: &[u8],
    replaced_drive: &Path,
    pid_before: u32,
    pid_after: u32,
    node_listings: Vec<Vec<String>>,
) -> TestResult {
    let verifier = dist.client(0)?;
    let mut objects = Vec::new();
    for item in expected {
        let actual = get_object_bytes(&verifier, bucket, &item.key).await?;
        let physical = census_object_version_on_disk(replaced_drive, bucket, &item.key, None)?;
        objects.push(serde_json::json!({
            "key": item.key,
            "version_id": null,
            "expected_bytes": item.body.len(),
            "actual_bytes": actual.len(),
            "expected_sha256": sha256_hex(&item.body),
            "actual_sha256": sha256_hex(&actual),
            "expected_physical": item.baseline,
            "physical": physical,
        }));
    }
    let actual = get_object_bytes(&verifier, bucket, outage_key).await?;
    let physical = census_object_version_on_disk(replaced_drive, bucket, outage_key, None)?;
    objects.push(serde_json::json!({
        "key": outage_key,
        "version_id": null,
        "expected_bytes": outage_body.len(),
        "actual_bytes": actual.len(),
        "expected_sha256": sha256_hex(outage_body),
        "actual_sha256": sha256_hex(&actual),
        "expected_physical": null,
        "physical": physical,
    }));

    let evidence = serde_json::json!({
        "schema": 1,
        "case": EC84_TARGET_DRIVE_RESTART_CASE,
        "evidence": "process-restart",
        "run_id": string_field(&context.run, "run_id")?,
        "source_revision": string_field(&context.run, "source_revision")?,
        "test_build": compiled_test_identity(),
        "binary_sha256": string_field(&context.run, "binary.sha256")?,
        "test_binary_sha256": string_field(&context.run, "test_binary.sha256")?,
        "topology": {"nodes": EC84_NODE_COUNT, "drives_per_node": EC84_DRIVES_PER_NODE},
        "pid_before": pid_before,
        "pid_after": pid_after,
        "unclean_shutdown_marker": false,
        "objects": objects,
        "node_listings": node_listings,
    });
    let data = serde_json::to_vec(&evidence)?;
    if data.len() > 1024 * 1024 {
        return Err("scanner/heal oracle exceeds the 1 MiB artifact budget".into());
    }
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(context.directory.join(EC84_TARGET_DRIVE_RESTART_ORACLE))?;
    output.write_all(&data)?;
    output.sync_all()?;
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
    let evidence_context = scanner_heal_evidence_context()?;
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
    let target_pid_before = dist.cluster.nodes[replaced_node]
        .process
        .as_ref()
        .ok_or("target process is absent before graceful restart")?
        .id();
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
    let target_pid_after = dist.cluster.nodes[replaced_node]
        .process
        .as_ref()
        .ok_or("target process is absent after restart")?
        .id();
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
    let mut node_listings = Vec::new();
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
        let mut observed = observed.into_iter().collect::<Vec<_>>();
        observed.sort();
        node_listings.push(observed);
    }
    if let Some(context) = evidence_context {
        write_scanner_heal_evidence(
            context,
            &dist,
            &bucket,
            &expected,
            outage_key,
            &outage_body,
            &replaced_drive,
            target_pid_before,
            target_pid_after,
            node_listings,
        )
        .await?;
    }

    Ok(())
}
