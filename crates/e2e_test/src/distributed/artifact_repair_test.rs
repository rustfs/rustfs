// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::artifact_seal_test::{ArtifactAdmin, assert_exact_version, confirm_seal, create_artifact_attempt};
use super::harness::{
    DistCluster, DistLayout, TestResult, enable_versioning, payload_for, unique_bucket, wait_for_ready, wait_until,
};
use crate::chaos::{VersionShardCensus, census_object_version_on_disk, signed_admin_post, start_root_heal_when_control_ready};
use crate::common::init_logging;
use http::StatusCode;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;

const REPAIR_SEAL_ROUTE: &str = "/rustfs/admin/v3/plugin-artifacts/seal-repairs";
const HEAL_BODY: &str =
    r#"{"recursive":true,"dryRun":false,"remove":false,"recreate":true,"scanMode":2,"updateParity":false,"nolock":false}"#;
const HEAL_BUDGET: Duration = Duration::from_secs(120);

struct RepairObject {
    key: String,
    version: String,
    body: Vec<u8>,
    manifests: Vec<VersionShardCensus>,
}

fn roots(dist: &DistCluster) -> Vec<PathBuf> {
    dist.cluster
        .nodes
        .iter()
        .map(|node| PathBuf::from(&node.data_dirs[0]))
        .collect()
}

impl RepairObject {
    async fn put(dist: &DistCluster, bucket: &str, key: String) -> TestResult<Self> {
        let body = payload_for(&key, 2 * 1024 * 1024);
        let output = timeout(
            Duration::from_secs(30),
            dist.client(1)?
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(body.clone().into())
                .send(),
        )
        .await??;
        let version = output.version_id().ok_or("managed PUT omitted version ID")?.to_owned();
        let manifests = roots(dist)
            .iter()
            .map(|disk| census_object_version_on_disk(disk, bucket, &key, Some(&version)))
            .collect::<TestResult<Vec<_>>>()?;
        for manifest in &manifests {
            assert!(manifest.is_complete(), "repair needs a complete baseline: {manifest:?}");
            assert!(!manifest.expected_part_numbers.is_empty(), "repair must use actual shard files");
        }
        Ok(Self {
            key,
            version,
            body,
            manifests,
        })
    }

    fn part(&self, disk: &Path, bucket: &str, node: usize) -> TestResult<PathBuf> {
        let data_dir = self.manifests[node]
            .data_dir
            .as_ref()
            .ok_or("external shard omitted data directory")?;
        Ok(disk.join(bucket).join(&self.key).join(data_dir).join("part.1"))
    }

    async fn assert_repaired(&self, dist: &DistCluster, bucket: &str) -> TestResult {
        wait_until(
            HEAL_BUDGET,
            || async {
                for (disk, baseline) in roots(dist).iter().zip(&self.manifests) {
                    if !census_object_version_on_disk(disk, bucket, &self.key, Some(&self.version))?.matches_manifest(baseline) {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
            "every configured receiver restored the exact version and original shard fingerprints",
        )
        .await?;
        for node in 0..dist.cluster.nodes.len() {
            assert_exact_version(&dist.client(node)?, bucket, &self.key, &self.version, &self.body).await?;
        }
        Ok(())
    }
}

async fn start_heal(dist: &DistCluster, bucket: &str, prefix: &str, remove: bool) -> TestResult<String> {
    let url = format!("{}/rustfs/admin/v3/heal/{bucket}/{prefix}?forceStart=true", dist.cluster.nodes[0].url);
    let body = if remove {
        HEAL_BODY.replace("\"remove\":false", "\"remove\":true")
    } else {
        HEAL_BODY.to_owned()
    };
    let response = timeout(
        HEAL_BUDGET,
        start_root_heal_when_control_ready(&url, &body, &dist.cluster.access_key, &dist.cluster.secret_key),
    )
    .await??;
    let response: Value = serde_json::from_str(&response)?;
    Ok(response["clientToken"]
        .as_str()
        .ok_or("heal omitted its task token")?
        .to_owned())
}

async fn finish_heal(dist: &DistCluster, token: &str, rejected_key: Option<&str>) -> TestResult {
    let url = format!("{}/rustfs/admin/v3/heal/?clientToken={token}", dist.cluster.nodes[0].url);
    wait_until(
        HEAL_BUDGET,
        || async {
            let response = signed_admin_post(&url, None, &dist.cluster.access_key, &dist.cluster.secret_key).await?;
            let status: Value = serde_json::from_str(&response)?;
            if status["summary"] == "running" {
                return Ok(false);
            }
            if let Some(key) = rejected_key {
                assert_eq!(status["summary"], "stopped", "closed repair must report failure: {status}");
                assert!(
                    status["outcome"]["counters"]["failed"]
                        .as_u64()
                        .is_some_and(|count| count > 0)
                );
                assert!(
                    status["outcome"]["objects"].as_array().is_some_and(|objects| objects
                        .iter()
                        .any(|object| { object["identity"]["object"] == key && object["disposition"]["state"] == "failed" })),
                    "heal must actually attempt the rejected object: {status}"
                );
            } else {
                assert_eq!(status["summary"], "finished", "heal did not complete: {status}");
            }
            Ok(true)
        },
        "exact admin heal task reached its expected terminal outcome",
    )
    .await
}

async fn repair_seal(dist: &DistCluster, user: &ArtifactAdmin, target: &Value, confirmed: bool) -> TestResult {
    wait_until(
        Duration::from_secs(45),
        || async {
            let request = json!({"version": 1, "target": target}).to_string();
            let (status, body) = user.request(dist, 0, REPAIR_SEAL_ROUTE, request).await?;
            if !confirmed {
                assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "offline receiver repair seal: {body}");
                assert!(body.contains("<Code>ServiceUnavailable</Code>"));
                assert!(body.contains("artifact operation unconfirmed"));
                return Ok(true);
            }
            if status == StatusCode::SERVICE_UNAVAILABLE
                && body.contains("<Code>ServiceUnavailable</Code>")
                && body.contains("<Message>artifact operation unconfirmed; retry the same identity</Message>")
            {
                return Ok(false);
            }
            assert_eq!(status, StatusCode::OK, "repair seal failed: {body}");
            assert_eq!(
                serde_json::from_str::<Value>(&body)?,
                json!({"version": 1, "target": target, "attempt_repairs_sealed": true})
            );
            Ok(true)
        },
        "repair seal confirms every configured receiver",
    )
    .await
}

#[tokio::test]
async fn four_node_artifact_shard_and_minority_metadata_repair_after_put_seal() -> TestResult {
    init_logging();
    // Sub-quorum metadata recovery supports genuine legacy shards only.
    let mut dist =
        DistCluster::start_with_env(DistLayout::FourNodeFourDisk, &[("RUSTFS_SHARD_INTEGRITY_WRITE", "false")]).await?;
    let (target, user) = create_artifact_attempt(&dist).await?;
    let bucket = target["bucket"].as_str().ok_or("target omitted bucket")?;
    let prefix = target["attempt_prefix"].as_str().ok_or("target omitted prefix")?;
    let shard = RepairObject::put(&dist, bucket, format!("{prefix}shard.bin")).await?;
    let metadata = RepairObject::put(&dist, bucket, format!("{prefix}metadata.bin")).await?;
    let later = dist
        .client(2)?
        .put_object()
        .bucket(bucket)
        .key(&shard.key)
        .body(b"newer-version".to_vec().into())
        .send()
        .await?;
    let later_version = later.version_id().ok_or("second PUT omitted version ID")?.to_owned();
    assert_ne!(shard.version, later_version);
    confirm_seal(&dist, &user, &json!({"version": 1, "target": target}).to_string(), &target).await?;

    dist.cluster.stop();
    let disks = roots(&dist);
    std::fs::remove_file(shard.part(&disks[3], bucket, 3)?)?;
    for disk in &disks[1..] {
        std::fs::remove_file(disk.join(bucket).join(&metadata.key).join("xl.meta"))?;
    }
    assert!(!census_object_version_on_disk(&disks[3], bucket, &shard.key, Some(&shard.version))?.is_complete());
    assert!(disks[0].join(bucket).join(&metadata.key).join("xl.meta").is_file());
    assert!(
        disks[1..]
            .iter()
            .all(|disk| !disk.join(bucket).join(&metadata.key).join("xl.meta").exists())
    );
    dist.cluster.start().await?;
    wait_for_ready(&dist.cluster).await?;
    let token = start_heal(&dist, bucket, prefix, false).await?;
    shard.assert_repaired(&dist, bucket).await?;
    metadata.assert_repaired(&dist, bucket).await?;
    finish_heal(&dist, &token, None).await?;
    assert_exact_version(&dist.client(3)?, bucket, &shard.key, &later_version, b"newer-version").await?;
    let listed = dist
        .client(0)?
        .list_object_versions()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await?;
    assert_eq!(listed.versions().len(), 3, "repair must not publish replacement versions");
    assert!(listed.delete_markers().is_empty());
    Ok(())
}

#[tokio::test]
async fn four_node_artifact_repair_seal_denies_shard_and_metadata_repair_after_restart() -> TestResult {
    init_logging();
    let mut dist =
        DistCluster::start_with_env(DistLayout::FourNodeFourDisk, &[("RUSTFS_SHARD_INTEGRITY_WRITE", "false")]).await?;
    let (target, user) = create_artifact_attempt(&dist).await?;
    let bucket = target["bucket"].as_str().ok_or("target omitted bucket")?;
    let prefix = target["attempt_prefix"].as_str().ok_or("target omitted prefix")?;
    let shard = RepairObject::put(&dist, bucket, format!("{prefix}shard.bin")).await?;
    let metadata = RepairObject::put(&dist, bucket, format!("{prefix}metadata.bin")).await?;
    confirm_seal(&dist, &user, &json!({"version": 1, "target": target}).to_string(), &target).await?;
    repair_seal(&dist, &user, &target, true).await?;
    let ordinary_bucket = unique_bucket("repair-control");
    dist.create_bucket(&ordinary_bucket).await?;
    enable_versioning(&dist.client(0)?, &ordinary_bucket).await?;
    let ordinary = RepairObject::put(&dist, &ordinary_bucket, "ordinary.bin".to_owned()).await?;
    let disks = roots(&dist);
    let part = shard.part(&disks[3], bucket, 3)?;
    std::fs::remove_file(&part)?;
    for disk in &disks[1..] {
        std::fs::remove_file(disk.join(bucket).join(&metadata.key).join("xl.meta"))?;
    }
    for restarted in [false, true] {
        if restarted {
            dist.restart_with_current_binary().await?;
        }
        // The same receivers must still execute an ordinary bucket repair.
        std::fs::remove_file(ordinary.part(&disks[3], &ordinary_bucket, 3)?)?;
        let control = start_heal(&dist, &ordinary_bucket, &ordinary.key, false).await?;
        ordinary.assert_repaired(&dist, &ordinary_bucket).await?;
        finish_heal(&dist, &control, None).await?;
        for object in [&shard, &metadata] {
            let token = start_heal(&dist, bucket, &object.key, false).await?;
            finish_heal(&dist, &token, Some(&object.key)).await?;
        }
        assert!(!part.exists(), "sealed repair recreated the missing shard");
        assert!(
            disks[1..]
                .iter()
                .all(|disk| !disk.join(bucket).join(&metadata.key).join("xl.meta").exists())
        );
        assert_exact_version(&dist.client(2)?, bucket, &shard.key, &shard.version, &shard.body).await?;
    }
    Ok(())
}

#[tokio::test]
async fn four_node_artifact_repair_seal_requires_every_receiver_on_exact_retry() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourNodeFourDisk).await?;
    let (target, user) = create_artifact_attempt(&dist).await?;
    let request = json!({"version": 1, "target": target}).to_string();
    confirm_seal(&dist, &user, &request, &target).await?;
    // Repeat after global confirmation: its persisted receipt cannot replace a
    // fresh acknowledgement from an offline configured receiver.
    for _ in 0..2 {
        dist.cluster.stop_node(3)?;
        repair_seal(&dist, &user, &target, false).await?;
        dist.cluster.start_node(3).await?;
        wait_for_ready(&dist.cluster).await?;
        repair_seal(&dist, &user, &target, true).await?;
    }
    Ok(())
}

#[tokio::test]
async fn four_node_artifact_dangling_repair_preserves_surviving_metadata() -> TestResult {
    init_logging();
    let mut dist =
        DistCluster::start_with_env(DistLayout::FourNodeFourDisk, &[("RUSTFS_SHARD_INTEGRITY_WRITE", "false")]).await?;
    let (target, user) = create_artifact_attempt(&dist).await?;
    let bucket = target["bucket"].as_str().ok_or("target omitted bucket")?;
    let prefix = target["attempt_prefix"].as_str().ok_or("target omitted prefix")?;
    let object = RepairObject::put(&dist, bucket, format!("{prefix}dangling.bin")).await?;
    confirm_seal(&dist, &user, &json!({"version": 1, "target": target}).to_string(), &target).await?;
    dist.cluster.stop();
    let disks = roots(&dist);
    let survivor = disks[0].join(bucket).join(&object.key).join("xl.meta");
    let before = std::fs::read(&survivor)?;
    for (node, disk) in disks.iter().enumerate() {
        std::fs::remove_file(object.part(disk, bucket, node)?)?;
        if node != 0 {
            std::fs::remove_file(disk.join(bucket).join(&object.key).join("xl.meta"))?;
        }
    }
    dist.cluster.start().await?;
    wait_for_ready(&dist.cluster).await?;
    let token = start_heal(&dist, bucket, &object.key, true).await?;
    finish_heal(&dist, &token, Some(&object.key)).await?;
    assert_eq!(
        std::fs::read(survivor)?,
        before,
        "distributed repair must not authorize managed dangling deletion"
    );
    Ok(())
}
