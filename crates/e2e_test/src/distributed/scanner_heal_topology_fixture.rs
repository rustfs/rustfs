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

//! Fixture for the `scanner_heal_topology_` protection matrix (backlog#2873).
//!
//! The fixture seeds a deterministic inventory, records every version with its
//! requested and listed version id, byte length and SHA-256, and re-verifies
//! that manifest through every node before, during and after pool expansion,
//! rebalance and decommission. Physical placement is read from `xl.meta` on the
//! pool drives so a passing run proves which pool owns each version, not only
//! that a quorum could serve a GET. Fault windows are reached through the
//! `e2e-test-hooks` file barriers; a window counts as exercised only when the
//! server wrote a reached observation.

use super::harness::{
    DistCluster, DistLayout, TestResult, cluster_admin, cluster_admin_ok, decommission_status_json, get_object_bytes,
    payload_for, put_object, s3_probe_client, s3_probe_error_code, sha256_hex, unique_bucket, wait_until,
};
use crate::chaos::{VersionShardCensus, census_object_version_on_disk};
use crate::common::{FAST_DATA_USAGE_SCANNER_ENV, RustFSTestClusterEnvironment};
use crate::object_lock::common::{put_object_with_legal_hold, put_object_with_retention};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, ObjectLockLegalHoldStatus, ObjectLockRetentionMode,
    VersioningConfiguration,
};
use chrono::{DateTime, Utc};
use http::{Method, StatusCode};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

pub(crate) const MOVEMENT_BARRIER_DIR_ENV: &str = "RUSTFS_E2E_DATA_MOVEMENT_BARRIER_DIR";
pub(crate) const STAGE_BEFORE_TARGET_COMMIT: &str = "before_target_commit";
pub(crate) const STAGE_BEFORE_SOURCE_CLEANUP: &str = "before_source_cleanup";
pub(crate) const STAGE_AFTER_RESPONSIBILITY_PERSISTED: &str = "after_responsibility_persisted";
/// Optional directory that keeps node logs, barrier observations and the
/// per-case evidence JSON after the test. `RUSTFS_E2E_LOG_DIR` is the fallback;
/// with neither set everything lives in a temp dir that is removed on drop.
pub(crate) const EVIDENCE_DIR_ENV: &str = "RUSTFS_SCANNER_HEAL_TOPOLOGY_EVIDENCE_DIR";
pub(crate) const DURABILITY_EVENT: &str = "disk_local_durability_mode";
/// The existing expand/decommission cases wait 180 s for 96 objects of
/// 128-256 KiB. This inventory adds 1 MiB externals, multipart and version
/// history (about 45 MiB), so the terminal wait is 240 s. The e2e-distributed
/// profile terminates a test after 6 x 120 s, which bounds every case here.
pub(crate) const MOVEMENT_COMPLETE_TIMEOUT: Duration = Duration::from_secs(240);
pub(crate) const MOVEMENT_ACTIVE_TIMEOUT: Duration = Duration::from_secs(30);
/// A status probe that outlives this is treated as "still running": a held
/// barrier can block the admin status request until the barrier itself times out.
const STATUS_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const SEED_POOL: usize = 0;

const TOPOLOGY_ENV: &[(&str, &str)] = &[
    ("RUSTFS_DURABILITY_MODE", "strict"),
    ("RUSTFS_NEW_BUCKET_DURABILITY_MODE", "strict"),
    ("RUSTFS_SCANNER_ENABLED", "true"),
    ("RUSTFS_HEAL_ENABLED", "true"),
    ("RUSTFS_HEAL_AUTO_HEAL_ENABLE", "true"),
    ("RUSTFS_HEAL_MRF_ENABLE", "true"),
];

const EMPTY_COUNT: usize = 8;
const INLINE_COUNT: usize = 24;
const INLINE_SIZE: usize = 1024;
const EXTERNAL_COUNT: usize = 16;
const EXTERNAL_SIZE: usize = 1024 * 1024;
const MULTIPART_COUNT: usize = 3;
const MULTIPART_PART_SIZES: [usize; 2] = [5 * 1024 * 1024, 1024 * 1024];
const SPECIAL_KEYS: [&str; 10] = [
    "special/sp ace.txt",
    "special/plus+sign.txt",
    "special/amp&eq=.txt",
    "special/pct%25.txt",
    "special/quote'apos.txt",
    "special/semi;colon,comma.txt",
    "special/tilde~bang!.txt",
    "special/ünïcödé-键.bin",
    "special/emoji-😀.bin",
    "special/paren(1)[2]{3}.txt",
];
const NESTED_PAIRS: [(&str, &str); 4] = [
    ("nested/a", "nested/a/child"),
    ("nested/b", "nested/b/child/deeper"),
    ("nested/c.bin", "nested/c.bin/child.bin"),
    ("nested/dir", "nested/dir/sub/leaf"),
];
const VERSIONED_KEYS: usize = 8;
const VERSIONED_NULL_SIZE: usize = 2048;
const VERSIONED_HISTORY_SIZES: [usize; 3] = [1024, 32 * 1024, 300 * 1024];
const LOCKED_SIZE: usize = 8 * 1024;

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct RetentionRecord {
    pub mode: String,
    pub retain_until_unix: i64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ManifestEntry {
    pub bucket: String,
    pub key: String,
    pub shape: &'static str,
    /// Version id returned by the write response (`None` when the response
    /// carried none, which is the pre-versioning null case).
    pub put_version_id: Option<String>,
    /// Version id readers must request; `None` means the bucket is unversioned.
    pub version_id: Option<String>,
    pub delete_marker: bool,
    pub length: u64,
    pub sha256: String,
    pub legal_hold: bool,
    pub retention: Option<RetentionRecord>,
}

impl ManifestEntry {
    fn body(
        bucket: &str,
        key: &str,
        shape: &'static str,
        version_id: Option<String>,
        put_version_id: Option<String>,
        body: &[u8],
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            shape,
            put_version_id,
            version_id,
            delete_marker: false,
            length: body.len() as u64,
            sha256: sha256_hex(body),
            legal_hold: false,
            retention: None,
        }
    }

    pub(crate) fn listed_version_id(&self) -> String {
        normalize_version_id(self.version_id.as_deref())
    }

    fn version_request(&self) -> Option<&str> {
        self.version_id.as_deref()
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct TopologyManifest {
    pub plain_bucket: String,
    pub versioned_bucket: String,
    pub locked_bucket: String,
    pub entries: Vec<ManifestEntry>,
    /// Physical census of the `.bucket-incarnation` sidecar object per bucket,
    /// keyed by `pool/drive`, recorded while the seed pool was the only pool.
    pub incarnation_sidecars: BTreeMap<String, BTreeMap<String, VersionShardCensus>>,
}

impl TopologyManifest {
    pub(crate) fn buckets(&self) -> [&str; 3] {
        [&self.plain_bucket, &self.versioned_bucket, &self.locked_bucket]
    }

    pub(crate) fn plain_entries(&self) -> impl Iterator<Item = &ManifestEntry> {
        self.entries.iter().filter(|entry| entry.bucket == self.plain_bucket)
    }

    pub(crate) fn plain_key(&self, shape: &str, index: usize) -> TestResult<String> {
        self.plain_entries()
            .filter(|entry| entry.shape == shape)
            .nth(index)
            .map(|entry| entry.key.clone())
            .ok_or_else(|| format!("manifest has no {shape} entry #{index}").into())
    }
}

pub(crate) fn normalize_version_id(version_id: Option<&str>) -> String {
    match version_id {
        None | Some("") | Some("null") | Some("00000000-0000-0000-0000-000000000000") => "null".to_string(),
        Some(version_id) => version_id.to_string(),
    }
}

/// Fault cases inject at server-side barriers that exist only in a server
/// built with `e2e-test-hooks`. Fail before starting a cluster instead of
/// waiting for a barrier that can never be reached.
pub(crate) fn require_test_hooks() {
    crate::common::rustfs_binary_path_with_features(Some("e2e-test-hooks"));
}

/// One cluster run plus its barrier directories and node log capture.
pub(crate) struct TopologyRun {
    pub dist: DistCluster,
    artifacts: PathBuf,
    keep_artifacts: bool,
    pub movement_barrier: FileBarrier,
}

impl TopologyRun {
    pub(crate) async fn start(label: &str) -> TestResult<Self> {
        let run_name = format!("scanner-heal-topology-{label}-{}", Uuid::new_v4().simple());
        // CI uploads RUSTFS_E2E_LOG_DIR; keep node logs, barrier observations
        // and evidence there so a failed lane can be diagnosed.
        let kept_root = std::env::var_os(EVIDENCE_DIR_ENV).or_else(|| std::env::var_os("RUSTFS_E2E_LOG_DIR"));
        let (artifacts, keep_artifacts) = match kept_root {
            Some(directory) => (PathBuf::from(directory).join(&run_name), true),
            None => (std::env::temp_dir().join(run_name), false),
        };
        let movement_dir = artifacts.join("movement-barrier");
        let log_dir = artifacts.join("node-logs");
        for directory in [&movement_dir, &log_dir] {
            std::fs::create_dir_all(directory)?;
        }
        if !movement_dir.is_absolute() {
            return Err("barrier directories must be absolute".into());
        }
        let movement_dir_value = movement_dir.to_string_lossy().into_owned();
        let mut env: Vec<(&str, &str)> = TOPOLOGY_ENV.to_vec();
        env.extend_from_slice(FAST_DATA_USAGE_SCANNER_ENV);
        env.push((MOVEMENT_BARRIER_DIR_ENV, movement_dir_value.as_str()));
        let mut dist = DistCluster::new_stopped_with_env(DistLayout::SingleNodeFourDrive, &env).await?;
        dist.capture_logs_under(&log_dir)?;
        dist.cluster.start().await?;
        Ok(Self {
            dist,
            artifacts,
            keep_artifacts,
            movement_barrier: FileBarrier::new(movement_dir),
        })
    }

    pub(crate) fn node_log_paths(&self) -> Vec<PathBuf> {
        self.dist
            .cluster
            .node_capture_log_paths
            .iter()
            .flatten()
            .map(PathBuf::from)
            .collect()
    }

    /// Persist the case evidence when `RUSTFS_SCANNER_HEAL_TOPOLOGY_EVIDENCE_DIR`
    /// is set; otherwise the evidence lives only in the test output.
    pub(crate) fn write_evidence(&self, case: &str, evidence: &Value) -> TestResult<Option<PathBuf>> {
        if !self.keep_artifacts {
            return Ok(None);
        }
        let path = self.artifacts.join(format!("{case}.json"));
        std::fs::write(&path, serde_json::to_vec_pretty(evidence)?)?;
        Ok(Some(path))
    }

    /// Hard-kill one node (SIGKILL); returns the killed pid.
    pub(crate) fn kill_node(&mut self, node_idx: usize) -> TestResult<u32> {
        let pid = self.dist.cluster.nodes[node_idx]
            .process
            .as_ref()
            .ok_or_else(|| format!("node {node_idx} is not running before the kill"))?
            .id();
        self.dist.cluster.stop_node(node_idx)?;
        Ok(pid)
    }

    /// Bring a killed node back on the same directories; returns the new pid.
    pub(crate) async fn restart_killed_node(&mut self, node_idx: usize) -> TestResult<u32> {
        self.dist.cluster.start_node(node_idx).await?;
        super::harness::wait_for_ready(&self.dist.cluster).await?;
        self.dist.cluster.nodes[node_idx]
            .process
            .as_ref()
            .map(|process| process.id())
            .ok_or_else(|| format!("node {node_idx} is not running after the restart").into())
    }
}

impl Drop for TopologyRun {
    fn drop(&mut self) {
        self.dist.cluster.stop();
        if !self.keep_artifacts
            && let Err(error) = std::fs::remove_dir_all(&self.artifacts)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("failed to remove topology artifacts {}: {error}", self.artifacts.display());
        }
    }
}

/// The data movement a fault window belongs to.
#[derive(Clone, Copy)]
pub(crate) enum MovementOperation<'a> {
    Decommission(usize),
    Rebalance(&'a str),
}

/// Driver for the file-based data-movement barrier (`arm.json`, `reached.*`,
/// `release.<nonce>`).
pub(crate) struct FileBarrier {
    dir: PathBuf,
    nonce: String,
}

impl FileBarrier {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            nonce: Uuid::new_v4().to_string(),
        }
    }

    pub(crate) fn nonce(&self) -> &str {
        &self.nonce
    }

    fn write_atomic(&self, name: &str, body: &[u8]) -> TestResult {
        let tmp = self.dir.join(format!(".{name}.{}.tmp", Uuid::new_v4().simple()));
        std::fs::write(&tmp, body)?;
        std::fs::rename(&tmp, self.dir.join(name))?;
        Ok(())
    }

    /// Arm the data-movement barrier for one object at one stage.
    pub(crate) fn arm_movement(&self, operation: &str, bucket: &str, object: &str, stage: &str) -> TestResult {
        let arm = serde_json::json!({
            "nonce": self.nonce,
            "bucket": bucket,
            "object": object,
            "stage": stage,
            "operation": operation,
        });
        self.write_atomic("arm.json", &serde_json::to_vec(&arm)?)
    }

    pub(crate) fn disarm(&self) -> TestResult {
        match std::fs::remove_file(self.dir.join("arm.json")) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn release_movement_with_io_error(&self) -> TestResult {
        self.write_atomic(&format!("release.{}", self.nonce), b"io_error")
    }

    pub(crate) fn reached_observations(&self) -> TestResult<Vec<Value>> {
        let prefix = format!("reached.{}.", self.nonce);
        let mut observations = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with(&prefix) && name.ends_with(".json") {
                observations.push(serde_json::from_slice(&std::fs::read(entry.path())?)?);
            }
        }
        Ok(observations)
    }

    /// Wait until the mover reaches the armed window while the operation is
    /// still running. A window can sit deep in the object order, so the wait
    /// shares the movement budget; an operation that ends without entering the
    /// window fails at once instead of being counted as a fault test.
    pub(crate) async fn wait_reached_during(
        &self,
        cluster: &RustFSTestClusterEnvironment,
        operation: MovementOperation<'_>,
        timeout: Duration,
    ) -> TestResult<Vec<Value>> {
        let deadline = Instant::now() + timeout;
        loop {
            let observations = self.reached_observations()?;
            if !observations.is_empty() {
                return Ok(observations);
            }
            // The status request can wait on the held mover, so a slow probe is
            // read as "still running" and never keeps this loop from noticing
            // that the window was reached.
            match operation {
                MovementOperation::Decommission(pool_id) => {
                    if let Ok(status) = tokio::time::timeout(STATUS_PROBE_TIMEOUT, decommission_status_json(cluster)).await {
                        let pool = decommission_pool_entry(&status?, pool_id)?;
                        if ["complete", "failed", "canceled"]
                            .iter()
                            .any(|flag| decommission_flag(&pool, flag))
                        {
                            return Err(format!(
                                "decommission of pool {pool_id} ended without entering barrier {}; the fault was not injected: {pool}",
                                self.nonce
                            )
                            .into());
                        }
                    }
                }
                MovementOperation::Rebalance(rebalance_id) => {
                    if let Ok(status) =
                        tokio::time::timeout(STATUS_PROBE_TIMEOUT, super::harness::rebalance_status_json(cluster)).await
                    {
                        let status = status?;
                        let ended = super::harness::rebalance_complete(&status, rebalance_id).unwrap_or(false)
                            || rebalance_stopped(&status, rebalance_id).unwrap_or(false)
                            || status
                                .get("pools")
                                .and_then(Value::as_array)
                                .is_some_and(|pools| pools.iter().any(|pool| pool_status_text(pool) == "failed"));
                        if ended {
                            return Err(format!(
                                "rebalance {rebalance_id} ended without entering barrier {}; the fault was not injected: {status}",
                                self.nonce
                            )
                            .into());
                        }
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "data movement barrier {} was not reached within {timeout:?} while the operation was still running; the fault was not injected",
                    self.nonce
                )
                .into());
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    /// Wait until the server records that it reached the armed window.
    pub(crate) async fn wait_reached(&self, timeout: Duration) -> TestResult<Vec<Value>> {
        let deadline = Instant::now() + timeout;
        loop {
            let observations = self.reached_observations()?;
            if !observations.is_empty() {
                return Ok(observations);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "data movement barrier {} was not reached within {timeout:?}; the fault was not injected",
                    self.nonce
                )
                .into());
            }
            sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn put_tracked(client: &Client, bucket: &str, key: &str, body: Vec<u8>) -> TestResult<Option<String>> {
    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await?;
    Ok(response.version_id().map(str::to_owned))
}

async fn put_multipart(client: &Client, bucket: &str, key: &str) -> TestResult<Vec<u8>> {
    let upload = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = upload.upload_id().ok_or("multipart upload omitted upload ID")?;
    let mut body = Vec::new();
    let mut completed = CompletedMultipartUpload::builder();
    for (index, size) in MULTIPART_PART_SIZES.iter().enumerate() {
        let part_number = index as i32 + 1;
        let part = payload_for(&format!("{key}#part{part_number}"), *size);
        let uploaded = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(part.clone()))
            .send()
            .await?;
        completed = completed.parts(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().ok_or("multipart part omitted ETag")?)
                .build(),
        );
        body.extend_from_slice(&part);
    }
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed.build())
        .send()
        .await?;
    Ok(body)
}

fn retention_deadline() -> TestResult<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(Utc::now().timestamp() + 24 * 3600, 0).ok_or_else(|| "retention deadline out of range".into())
}

/// Seed the deterministic inventory into the seed pool and record it.
pub(crate) async fn seed_manifest(run: &TopologyRun) -> TestResult<TopologyManifest> {
    let client = run.dist.client(0)?;
    let plain = unique_bucket("sht-plain");
    let versioned = unique_bucket("sht-ver");
    let locked = unique_bucket("sht-lock");
    run.dist.create_bucket(&plain).await?;
    run.dist.create_bucket(&versioned).await?;
    client
        .create_bucket()
        .bucket(&locked)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;

    let mut entries = Vec::new();
    for index in 0..EMPTY_COUNT {
        let key = format!("empty/{index:02}");
        put_object(&client, &plain, &key, Vec::new()).await?;
        entries.push(ManifestEntry::body(&plain, &key, "empty", None, None, &[]));
    }
    for index in 0..INLINE_COUNT {
        let key = format!("inline/{index:03}.bin");
        let body = payload_for(&key, INLINE_SIZE);
        put_object(&client, &plain, &key, body.clone()).await?;
        entries.push(ManifestEntry::body(&plain, &key, "inline", None, None, &body));
    }
    for index in 0..EXTERNAL_COUNT {
        let key = format!("external/{index:03}.bin");
        let body = payload_for(&key, EXTERNAL_SIZE);
        put_object(&client, &plain, &key, body.clone()).await?;
        entries.push(ManifestEntry::body(&plain, &key, "external", None, None, &body));
    }
    for index in 0..MULTIPART_COUNT {
        let key = format!("multipart/{index}.bin");
        let body = put_multipart(&client, &plain, &key).await?;
        entries.push(ManifestEntry::body(&plain, &key, "multipart", None, None, &body));
    }
    for key in SPECIAL_KEYS {
        let body = payload_for(key, 4096);
        put_object(&client, &plain, key, body.clone()).await?;
        entries.push(ManifestEntry::body(&plain, key, "special", None, None, &body));
    }
    for (parent, child) in NESTED_PAIRS {
        for key in [parent, child] {
            let body = payload_for(key, 2048);
            put_object(&client, &plain, key, body.clone()).await?;
            entries.push(ManifestEntry::body(&plain, key, "nested", None, None, &body));
        }
    }

    // Explicit null versions must exist before versioning is enabled.
    let versioned_keys: Vec<String> = (0..VERSIONED_KEYS).map(|index| format!("versioned/{index:02}.bin")).collect();
    for key in &versioned_keys {
        let body = payload_for(&format!("{key}@null"), VERSIONED_NULL_SIZE);
        let put_version_id = put_tracked(&client, &versioned, key, body.clone()).await?;
        entries.push(ManifestEntry::body(
            &versioned,
            key,
            "null-version",
            Some("null".to_string()),
            put_version_id,
            &body,
        ));
    }
    client
        .put_bucket_versioning()
        .bucket(&versioned)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    for key in &versioned_keys {
        for (history, size) in VERSIONED_HISTORY_SIZES.iter().enumerate() {
            let body = payload_for(&format!("{key}@v{history}"), *size);
            let put_version_id = put_tracked(&client, &versioned, key, body.clone()).await?;
            let version_id = put_version_id
                .clone()
                .filter(|id| normalize_version_id(Some(id)) != "null")
                .ok_or_else(|| format!("versioned PUT of {key} history {history} omitted a real version id"))?;
            entries.push(ManifestEntry::body(&versioned, key, "history", Some(version_id), put_version_id, &body));
        }
        let deleted = client.delete_object().bucket(&versioned).key(key).send().await?;
        if deleted.delete_marker() != Some(true) {
            return Err(format!("DELETE of {key} on the versioned bucket did not create a delete marker: {deleted:?}").into());
        }
        let marker = deleted
            .version_id()
            .map(str::to_owned)
            .ok_or_else(|| format!("delete marker for {key} omitted a version id"))?;
        entries.push(ManifestEntry {
            bucket: versioned.clone(),
            key: key.clone(),
            shape: "delete-marker",
            put_version_id: Some(marker.clone()),
            version_id: Some(marker),
            delete_marker: true,
            length: 0,
            sha256: String::new(),
            legal_hold: false,
            retention: None,
        });
    }

    for index in 0..2 {
        let key = format!("hold/{index}.bin");
        let body = payload_for(&key, LOCKED_SIZE);
        let version_id = put_object_with_legal_hold(&client, &locked, &key, &body, ObjectLockLegalHoldStatus::On).await?;
        let mut entry = ManifestEntry::body(&locked, &key, "legal-hold", Some(version_id.clone()), Some(version_id), &body);
        entry.legal_hold = true;
        entries.push(entry);
    }
    let retain_until = retention_deadline()?;
    for index in 0..2 {
        let key = format!("retain/{index}.bin");
        let body = payload_for(&key, LOCKED_SIZE);
        let version_id =
            put_object_with_retention(&client, &locked, &key, &body, ObjectLockRetentionMode::Compliance, retain_until).await?;
        let mut entry = ManifestEntry::body(&locked, &key, "compliance", Some(version_id.clone()), Some(version_id), &body);
        entry.retention = Some(RetentionRecord {
            mode: "COMPLIANCE".to_string(),
            retain_until_unix: retain_until.timestamp(),
        });
        entries.push(entry);
    }

    let mut manifest = TopologyManifest {
        plain_bucket: plain,
        versioned_bucket: versioned,
        locked_bucket: locked,
        entries,
        incarnation_sidecars: BTreeMap::new(),
    };
    let buckets: Vec<String> = manifest.buckets().iter().map(|bucket| bucket.to_string()).collect();
    for bucket in buckets {
        let census = incarnation_sidecar_census(&run.dist, &bucket)?;
        if census.is_empty() {
            return Err(format!("bucket {bucket} has no physical incarnation sidecar on the seed pool").into());
        }
        manifest.incarnation_sidecars.insert(bucket, census);
    }
    Ok(manifest)
}

/// Physical census of the bucket incarnation sidecar on every pool drive.
pub(crate) fn incarnation_sidecar_census(dist: &DistCluster, bucket: &str) -> TestResult<BTreeMap<String, VersionShardCensus>> {
    let object = format!("buckets/{bucket}/.bucket-incarnation");
    let mut census = BTreeMap::new();
    for node in &dist.cluster.nodes {
        for (drive_idx, drive) in node.data_dirs.iter().enumerate() {
            let observed = census_object_version_on_disk(Path::new(drive), ".rustfs.sys", &object, None)?;
            if observed.has_xl_meta {
                census.insert(format!("{}/{drive_idx}", node.pool_idx), observed);
            }
        }
    }
    Ok(census)
}

#[derive(Clone, Debug)]
struct ListedVersion {
    key: String,
    version_id: String,
    is_latest: bool,
    delete_marker: bool,
    size: Option<i64>,
}

async fn list_all_versions(client: &Client, bucket: &str) -> TestResult<Vec<ListedVersion>> {
    let mut listed = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut version_id_marker: Option<String> = None;
    loop {
        let mut request = client.list_object_versions().bucket(bucket).max_keys(1000);
        if let Some(marker) = &key_marker {
            request = request.key_marker(marker);
        }
        if let Some(marker) = &version_id_marker {
            request = request.version_id_marker(marker);
        }
        let page = request.send().await?;
        for version in page.versions() {
            listed.push(ListedVersion {
                key: version.key().unwrap_or_default().to_string(),
                version_id: normalize_version_id(version.version_id()),
                is_latest: version.is_latest() == Some(true),
                delete_marker: false,
                size: version.size(),
            });
        }
        for marker in page.delete_markers() {
            listed.push(ListedVersion {
                key: marker.key().unwrap_or_default().to_string(),
                version_id: normalize_version_id(marker.version_id()),
                is_latest: marker.is_latest() == Some(true),
                delete_marker: true,
                size: None,
            });
        }
        if page.is_truncated() == Some(true) {
            key_marker = page.next_key_marker().map(str::to_owned);
            version_id_marker = page.next_version_id_marker().map(str::to_owned);
            if key_marker.is_none() {
                return Err(format!("ListObjectVersions on {bucket} was truncated without a key marker").into());
            }
            continue;
        }
        return Ok(listed);
    }
}

async fn list_all_keys(client: &Client, bucket: &str) -> TestResult<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    let mut token: Option<String> = None;
    loop {
        let mut request = client.list_objects_v2().bucket(bucket).max_keys(1000);
        if let Some(token) = &token {
            request = request.continuation_token(token);
        }
        let page = request.send().await?;
        keys.extend(page.contents().iter().filter_map(|object| object.key().map(str::to_owned)));
        match page.next_continuation_token() {
            Some(next) if page.is_truncated() == Some(true) => token = Some(next.to_string()),
            _ => return Ok(keys),
        }
    }
}

fn get_error_code(error: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>) -> Option<&str> {
    error.as_service_error().and_then(ProvideErrorMetadata::code)
}

async fn verify_entry_bytes(client: &Client, entry: &ManifestEntry, label: &str) -> TestResult {
    let mut request = client.get_object().bucket(&entry.bucket).key(&entry.key);
    if let Some(version_id) = entry.version_request() {
        request = request.version_id(version_id);
    }
    let output = request.send().await.map_err(|error| {
        format!(
            "{label}: GET {}/{} version {:?} failed: {error} (code {:?})",
            entry.bucket,
            entry.key,
            entry.version_id,
            get_error_code(&error)
        )
    })?;
    let body = output.body.collect().await?.into_bytes();
    if body.len() as u64 != entry.length || sha256_hex(&body) != entry.sha256 {
        return Err(format!(
            "{label}: content mismatch for {}/{} version {:?}: expected {} bytes sha256={} got {} bytes sha256={}",
            entry.bucket,
            entry.key,
            entry.version_id,
            entry.length,
            entry.sha256,
            body.len(),
            sha256_hex(&body)
        )
        .into());
    }
    Ok(())
}

async fn verify_marker_current(client: &Client, entry: &ManifestEntry, label: &str) -> TestResult {
    match client.get_object().bucket(&entry.bucket).key(&entry.key).send().await {
        Ok(_) => Err(format!(
            "{label}: {}/{} is covered by delete marker {:?} but the current GET succeeded",
            entry.bucket, entry.key, entry.version_id
        )
        .into()),
        Err(error) if matches!(get_error_code(&error), Some("NoSuchKey" | "NotFound")) => Ok(()),
        Err(error) => Err(format!(
            "{label}: current GET of marker-covered {}/{} failed unexpectedly with code {:?}: {error}",
            entry.bucket,
            entry.key,
            get_error_code(&error)
        )
        .into()),
    }
}

async fn verify_lock_state(client: &Client, entry: &ManifestEntry, label: &str) -> TestResult {
    let version_id = entry
        .version_id
        .as_deref()
        .ok_or_else(|| format!("{label}: locked entry {} has no version id", entry.key))?;
    if entry.legal_hold {
        let hold = client
            .get_object_legal_hold()
            .bucket(&entry.bucket)
            .key(&entry.key)
            .version_id(version_id)
            .send()
            .await?;
        if hold.legal_hold().and_then(|hold| hold.status()) != Some(&ObjectLockLegalHoldStatus::On) {
            return Err(format!("{label}: legal hold lost on {}/{} {version_id}: {hold:?}", entry.bucket, entry.key).into());
        }
    }
    if let Some(expected) = &entry.retention {
        let retention = client
            .get_object_retention()
            .bucket(&entry.bucket)
            .key(&entry.key)
            .version_id(version_id)
            .send()
            .await?;
        let actual_mode = retention
            .retention()
            .and_then(|retention| retention.mode())
            .map(|mode| mode.as_str().to_string());
        let actual_until = retention
            .retention()
            .and_then(|retention| retention.retain_until_date())
            .map(|date| date.secs());
        if actual_mode.as_deref() != Some(expected.mode.as_str()) || actual_until != Some(expected.retain_until_unix) {
            return Err(format!(
                "{label}: retention changed on {}/{} {version_id}: expected {expected:?} got mode {actual_mode:?} until {actual_until:?}",
                entry.bucket, entry.key
            )
            .into());
        }
    }
    // WORM must still hold for the exact version after movement.
    match client
        .delete_object()
        .bucket(&entry.bucket)
        .key(&entry.key)
        .version_id(version_id)
        .send()
        .await
    {
        Ok(_) => Err(format!("{label}: locked version {}/{} {version_id} was deleted", entry.bucket, entry.key).into()),
        Err(error) if error.as_service_error().and_then(ProvideErrorMetadata::code) == Some("AccessDenied") => Ok(()),
        Err(error) => Err(format!(
            "{label}: locked delete of {}/{} {version_id} failed with an unexpected error: {error}",
            entry.bucket, entry.key
        )
        .into()),
    }
}

/// Verify every manifest version through one client: exact version listing
/// per bucket, byte-for-byte bodies, delete-marker coverage, lock state, and
/// the plain-bucket key listing.
pub(crate) async fn verify_manifest(client: &Client, manifest: &TopologyManifest, label: &str) -> TestResult {
    for bucket in manifest.buckets() {
        let listed = list_all_versions(client, bucket)
            .await
            .map_err(|error| format!("{label}: list versions of {bucket}: {error}"))?;
        let expected: BTreeSet<(String, String, bool)> = manifest
            .entries
            .iter()
            .filter(|entry| entry.bucket == bucket)
            .map(|entry| (entry.key.clone(), entry.listed_version_id(), entry.delete_marker))
            .collect();
        let actual: BTreeSet<(String, String, bool)> = listed
            .iter()
            .map(|version| (version.key.clone(), version.version_id.clone(), version.delete_marker))
            .collect();
        if expected != actual {
            let missing: Vec<_> = expected.difference(&actual).collect();
            let extra: Vec<_> = actual.difference(&expected).collect();
            return Err(format!("{label}: version listing of {bucket} diverged; missing={missing:?} extra={extra:?}").into());
        }
        for version in &listed {
            let entry = manifest
                .entries
                .iter()
                .find(|entry| {
                    entry.bucket == bucket && entry.key == version.key && entry.listed_version_id() == version.version_id
                })
                .ok_or_else(|| format!("{label}: listed version without manifest entry: {version:?}"))?;
            if !version.delete_marker && version.size.is_some_and(|size| size != entry.length as i64) {
                return Err(format!(
                    "{label}: listing size for {bucket}/{} {} is {:?}, manifest says {}",
                    version.key, version.version_id, version.size, entry.length
                )
                .into());
            }
            if bucket == manifest.versioned_bucket && version.is_latest != version.delete_marker {
                return Err(format!("{label}: versioned key {} latest flag diverged: {version:?}", version.key).into());
            }
        }
    }
    for entry in &manifest.entries {
        if entry.delete_marker {
            verify_marker_current(client, entry, label).await?;
        } else {
            verify_entry_bytes(client, entry, label).await?;
        }
        if entry.legal_hold || entry.retention.is_some() {
            verify_lock_state(client, entry, label).await?;
        }
    }
    let plain_keys: BTreeSet<String> = manifest.plain_entries().map(|entry| entry.key.clone()).collect();
    let listed_keys = list_all_keys(client, &manifest.plain_bucket).await?;
    if plain_keys != listed_keys {
        let missing: Vec<_> = plain_keys.difference(&listed_keys).collect();
        let extra: Vec<_> = listed_keys.difference(&plain_keys).collect();
        return Err(format!("{label}: plain listing diverged; missing={missing:?} extra={extra:?}").into());
    }
    Ok(())
}

/// Verify the manifest through every node of the cluster.
pub(crate) async fn verify_from_every_node(dist: &DistCluster, manifest: &TopologyManifest, label: &str) -> TestResult {
    for node_idx in 0..dist.cluster.nodes.len() {
        let client = dist.client(node_idx)?;
        verify_manifest(&client, manifest, &format!("{label} via node {node_idx}")).await?;
    }
    Ok(())
}

fn xl_meta_has_version(path: &Path, version_id: Option<&str>) -> TestResult<bool> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let Some(version_id) = version_id else {
        return Ok(true);
    };
    let target = if normalize_version_id(Some(version_id)) == "null" {
        Uuid::nil()
    } else {
        Uuid::parse_str(version_id)?
    };
    // Null versions are stored with the nil UUID, which `find_version` also
    // uses for `None`; a decode failure counts as "not held" and surfaces as a
    // placement mismatch in the caller.
    Ok(rustfs_filemeta::FileMeta::load(&bytes)?.find_version(Some(target)).is_ok())
}

/// Pools whose drives hold `xl.meta` for the version (physical placement).
pub(crate) fn pools_holding(
    dist: &DistCluster,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> TestResult<BTreeSet<usize>> {
    let mut pools = BTreeSet::new();
    for node in &dist.cluster.nodes {
        for drive in &node.data_dirs {
            let path = Path::new(drive).join(bucket).join(key).join("xl.meta");
            if xl_meta_has_version(&path, version_id)? {
                pools.insert(node.pool_idx);
            }
        }
    }
    Ok(pools)
}

/// Require every manifest version to live in exactly one pool from `allowed`.
pub(crate) fn assert_single_pool_placement(
    dist: &DistCluster,
    manifest: &TopologyManifest,
    allowed: &BTreeSet<usize>,
    label: &str,
) -> TestResult<BTreeMap<String, usize>> {
    let mut placement = BTreeMap::new();
    for entry in &manifest.entries {
        let pools = pools_holding(dist, &entry.bucket, &entry.key, entry.version_request())?;
        if pools.len() != 1 || !pools.is_subset(allowed) {
            return Err(format!(
                "{label}: {}/{} version {:?} is held by pools {pools:?}; expected exactly one of {allowed:?}",
                entry.bucket, entry.key, entry.version_id
            )
            .into());
        }
        placement.insert(
            format!("{}/{}@{}", entry.bucket, entry.key, entry.listed_version_id()),
            pools.into_iter().next().unwrap_or(usize::MAX),
        );
    }
    Ok(placement)
}

/// Physical shard census of every unversioned plain entry on the seed pool.
pub(crate) fn capture_seed_pool_census(
    dist: &DistCluster,
    manifest: &TopologyManifest,
) -> TestResult<BTreeMap<String, Vec<VersionShardCensus>>> {
    let node = &dist.cluster.nodes[SEED_POOL];
    let mut census = BTreeMap::new();
    for entry in manifest.plain_entries() {
        let mut drives = Vec::new();
        for drive in &node.data_dirs {
            drives.push(census_object_version_on_disk(Path::new(drive), &entry.bucket, &entry.key, None)?);
        }
        if !drives.iter().any(|drive| drive.has_xl_meta) {
            return Err(format!("seed pool holds no shard of {}/{}", entry.bucket, entry.key).into());
        }
        census.insert(entry.key.clone(), drives);
    }
    Ok(census)
}

/// Expansion and restart must not move or rewrite seed-pool shards.
pub(crate) fn assert_seed_pool_census_unchanged(
    dist: &DistCluster,
    manifest: &TopologyManifest,
    baseline: &BTreeMap<String, Vec<VersionShardCensus>>,
    label: &str,
) -> TestResult {
    let current = capture_seed_pool_census(dist, manifest)?;
    for (key, drives) in baseline {
        let now = current
            .get(key)
            .ok_or_else(|| format!("{label}: {key} vanished from the seed pool census"))?;
        for (drive_idx, (before, after)) in drives.iter().zip(now).enumerate() {
            if before.has_xl_meta != after.has_xl_meta
                || before.erasure_distribution != after.erasure_distribution
                || before.erasure_index != after.erasure_index
                || before.data_dir != after.data_dir
                || before.present_part_fingerprints != after.present_part_fingerprints
                || before.inline_data_fingerprint != after.inline_data_fingerprint
            {
                return Err(format!(
                    "{label}: seed pool drive {drive_idx} shard of {key} changed: before {before:?} after {after:?}"
                )
                .into());
            }
        }
    }
    for entry in manifest.plain_entries() {
        let pools = pools_holding(dist, &entry.bucket, &entry.key, None)?;
        if pools != BTreeSet::from([SEED_POOL]) {
            return Err(format!(
                "{label}: {} is held by pools {pools:?} although only the seed pool should own it",
                entry.key
            )
            .into());
        }
    }
    Ok(())
}

/// Admin read-back of the bucket durability override seeded at creation.
pub(crate) async fn assert_bucket_durability_strict(
    cluster: &RustFSTestClusterEnvironment,
    buckets: &[&str],
) -> TestResult<Vec<Value>> {
    let mut responses = Vec::new();
    for bucket in buckets {
        let body = cluster_admin_ok(cluster, Method::GET, &format!("/rustfs/admin/v3/bucket-durability/{bucket}"), None).await?;
        let response: Value = serde_json::from_str(&body)?;
        if response.get("mode").and_then(Value::as_str) != Some("strict") {
            return Err(format!("bucket {bucket} does not report an effective strict durability override: {body}").into());
        }
        responses.push(response);
    }
    Ok(responses)
}

fn log_field<'a>(line: &'a Value, name: &str) -> Option<&'a str> {
    line.get("fields")
        .and_then(|fields| fields.get(name))
        .or_else(|| line.get(name))
        .and_then(Value::as_str)
}

/// Read the resolved process-wide durability mode from the captured node logs.
/// Returns the number of logs that reported the event; every reported mode must
/// be strict and the seed node must have reported one.
pub(crate) fn assert_process_durability_strict(log_paths: &[PathBuf]) -> TestResult<usize> {
    if log_paths.is_empty() {
        return Err("no node logs were captured; the process durability mode cannot be confirmed".into());
    }
    let mut reported = 0;
    for (index, path) in log_paths.iter().enumerate() {
        let text = std::fs::read_to_string(path)?;
        let mut seen = false;
        for line in text.lines() {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                if line.contains(DURABILITY_EVENT) {
                    return Err(format!("{}: durability event is not JSON: {line}", path.display()).into());
                }
                continue;
            };
            if log_field(&value, "event") != Some(DURABILITY_EVENT) {
                continue;
            }
            seen = true;
            if log_field(&value, "mode") != Some("strict") {
                return Err(format!("{}: process durability mode is not strict: {line}", path.display()).into());
            }
        }
        if seen {
            reported += 1;
        } else if index == SEED_POOL {
            return Err(format!("{}: the seed node never reported {DURABILITY_EVENT}", path.display()).into());
        }
    }
    Ok(reported)
}

/// Start a (possibly queued) decommission and fail closed unless acknowledged.
pub(crate) async fn start_decommission_targets(cluster: &RustFSTestClusterEnvironment, targets: &str) -> TestResult<String> {
    let path = format!("/rustfs/admin/v3/pools/decommission?pool={}&by-id=true", urlencoding::encode(targets));
    super::harness::post_decommission_start(cluster, &path, MOVEMENT_COMPLETE_TIMEOUT).await
}

/// Decommission start that must be rejected deterministically (4xx).
pub(crate) async fn expect_decommission_rejected(
    cluster: &RustFSTestClusterEnvironment,
    targets: &str,
) -> TestResult<(StatusCode, String)> {
    let path = format!("/rustfs/admin/v3/pools/decommission?pool={}&by-id=true", urlencoding::encode(targets));
    let (status, response) = cluster_admin(cluster, Method::POST, &path, None).await?;
    if !status.is_client_error() {
        return Err(format!("POST {path} was not rejected: {status} {response}").into());
    }
    Ok((status, response))
}

pub(crate) async fn cancel_decommission(cluster: &RustFSTestClusterEnvironment, pool_id: usize) -> TestResult<String> {
    cluster_admin_ok(
        cluster,
        Method::POST,
        &format!("/rustfs/admin/v3/pools/cancel?pool={pool_id}&by-id=true"),
        None,
    )
    .await
}

pub(crate) async fn clear_decommission(cluster: &RustFSTestClusterEnvironment, pool_id: usize) -> TestResult<String> {
    cluster_admin_ok(
        cluster,
        Method::POST,
        &format!("/rustfs/admin/v3/pools/clear?pool={pool_id}&by-id=true"),
        None,
    )
    .await
}

pub(crate) async fn stop_rebalance(cluster: &RustFSTestClusterEnvironment) -> TestResult<String> {
    cluster_admin_ok(cluster, Method::POST, "/rustfs/admin/v3/rebalance/stop", None).await
}

pub(crate) fn decommission_pool_entry(status: &Value, pool_id: usize) -> TestResult<Value> {
    if let Some(pools) = status.get("pools").and_then(Value::as_array) {
        return pools
            .iter()
            .find(|pool| pool.get("id").and_then(Value::as_u64) == Some(pool_id as u64))
            .cloned()
            .ok_or_else(|| format!("pool {pool_id} missing from decommission status: {status}").into());
    }
    if status.get("id").and_then(Value::as_u64) == Some(pool_id as u64) {
        return Ok(status.clone());
    }
    Err(format!("pool {pool_id} missing from decommission status: {status}").into())
}

pub(crate) fn decommission_flag(pool: &Value, flag: &str) -> bool {
    pool.get("decommissionInfo")
        .and_then(|info| info.get(flag))
        .and_then(Value::as_bool)
        == Some(true)
}

pub(crate) async fn wait_for_decommission_flag(
    cluster: &RustFSTestClusterEnvironment,
    pool_id: usize,
    flag: &str,
    timeout: Duration,
) -> TestResult<Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = decommission_status_json(cluster).await?;
        let pool = decommission_pool_entry(&status, pool_id)?;
        if decommission_flag(&pool, flag) {
            return Ok(pool);
        }
        if Instant::now() >= deadline {
            return Err(
                format!("pool {pool_id} did not report decommission {flag} within {timeout:?}; last status: {status}").into(),
            );
        }
        sleep(Duration::from_millis(500)).await;
    }
}

fn pool_status_text(pool: &Value) -> String {
    pool.get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
}

/// A stopped rebalance: the expected run id, no pool still started or
/// stopping, at least one pool stopped, and no peer propagation failure.
pub(crate) fn rebalance_stopped(status: &Value, expected_id: &str) -> TestResult<bool> {
    let id = status
        .get("id")
        .and_then(Value::as_str)
        .ok_or("rebalance status omitted id")?;
    if id != expected_id {
        return Err(format!("rebalance status id changed: expected {expected_id}, got {id}").into());
    }
    let pools = status
        .get("pools")
        .and_then(Value::as_array)
        .filter(|pools| !pools.is_empty())
        .ok_or("rebalance status omitted non-empty pools")?;
    let mut any_stopped = false;
    for pool in pools {
        let text = pool_status_text(pool);
        if pool.get("stopping").and_then(Value::as_bool) == Some(true) || text == "started" {
            return Ok(false);
        }
        if text == "failed" {
            return Err(format!("rebalance failed while stopping: {status}").into());
        }
        if text == "stopped" {
            any_stopped = true;
        }
    }
    let propagation_pending = status.get("stopPropagation").is_some_and(|propagation| {
        propagation.get("pendingTerminalReload").and_then(Value::as_bool) == Some(true)
            || propagation
                .get("failedPeers")
                .and_then(Value::as_array)
                .is_some_and(|peers| !peers.is_empty())
    });
    if propagation_pending {
        return Ok(false);
    }
    Ok(any_stopped)
}

pub(crate) async fn pool_count(cluster: &RustFSTestClusterEnvironment) -> TestResult<usize> {
    let pools = super::harness::list_pools_json(cluster).await?;
    pools
        .as_array()
        .map(Vec::len)
        .or_else(|| pools.get("pools").and_then(Value::as_array).map(Vec::len))
        .ok_or_else(|| format!("pool list omitted an array: {pools}").into())
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct LiveOp {
    pub op: &'static str,
    pub key: String,
    pub sha256: Option<String>,
    pub length: u64,
    pub version_id: Option<String>,
    /// `acked` has a success response; `uncertain` timed out or lost the
    /// connection and must not be assumed to have succeeded or failed.
    pub outcome: &'static str,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) enum LiveExpectation {
    Present(String),
    Absent,
    Uncertain(Vec<String>),
}

fn is_availability_code(code: Option<&str>) -> bool {
    matches!(code, Some("SlowDown" | "ServiceUnavailable"))
}

async fn retrying_get_matches_sha(client: &Client, bucket: &str, key: &str, expected_sha: &str, timeout: Duration) -> TestResult {
    wait_until(
        timeout,
        || async {
            match get_object_bytes(client, bucket, key).await {
                Ok(got) if sha256_hex(&got) == expected_sha => Ok(true),
                Ok(got) => Err(format!(
                    "object {bucket}/{key} bytes mismatch: expected sha256={expected_sha} got sha256={}",
                    sha256_hex(&got)
                )
                .into()),
                Err(error) => {
                    let code = s3_probe_error_code(error.as_ref());
                    if is_availability_code(code) {
                        Ok(false)
                    } else {
                        Err(format!("GET {bucket}/{key} failed with code {code:?}: {error}").into())
                    }
                }
            }
        },
        &format!("get {bucket}/{key} during data movement"),
    )
    .await
}

/// PUT/GET/DELETE mix against one bucket while data movement runs. Baseline
/// keys (from `baseline_bucket`) are read and compared byte-for-byte; a wrong body or a non-availability
/// service error fails the workload immediately.
/// Shape of the concurrent PUT/GET/DELETE workload run beside data movement.
pub(crate) struct LiveWorkload {
    pub clients: Vec<Client>,
    pub bucket: String,
    pub prefix: String,
    pub writers: usize,
    pub rounds: usize,
    pub baseline_bucket: String,
    pub baseline: Vec<(String, String)>,
    pub pause: Duration,
}

pub(crate) async fn run_live_workload(workload: LiveWorkload) -> TestResult<Vec<LiveOp>> {
    let LiveWorkload {
        clients,
        bucket,
        prefix,
        writers,
        rounds,
        baseline_bucket,
        baseline,
        pause,
    } = workload;
    let mut handles = Vec::new();
    for writer in 0..writers {
        let client = s3_probe_client(&clients[writer % clients.len()]);
        let bucket = bucket.clone();
        let prefix = prefix.clone();
        let baseline = baseline.clone();
        let baseline_bucket = baseline_bucket.clone();
        handles.push(tokio::spawn(async move {
            let work = async {
                let mut ops = Vec::new();
                for round in 0..rounds {
                    let key = format!("{prefix}/w{writer:02}/r{round:02}.bin");
                    let body = payload_for(&key, 16 * 1024);
                    let sha = sha256_hex(&body);
                    let deadline = Instant::now() + Duration::from_secs(30);
                    loop {
                        match client
                            .put_object()
                            .bucket(&bucket)
                            .key(&key)
                            .body(ByteStream::from(body.clone()))
                            .send()
                            .await
                        {
                            Ok(output) => {
                                ops.push(LiveOp {
                                    op: "put",
                                    key: key.clone(),
                                    sha256: Some(sha.clone()),
                                    length: body.len() as u64,
                                    version_id: output.version_id().map(str::to_owned),
                                    outcome: "acked",
                                    detail: None,
                                });
                                break;
                            }
                            Err(error) => {
                                let code = error
                                    .as_service_error()
                                    .and_then(ProvideErrorMetadata::code)
                                    .map(str::to_owned);
                                if is_availability_code(code.as_deref()) && Instant::now() < deadline {
                                    sleep(Duration::from_millis(200)).await;
                                    continue;
                                }
                                if code.is_none() {
                                    ops.push(LiveOp {
                                        op: "put",
                                        key: key.clone(),
                                        sha256: Some(sha.clone()),
                                        length: body.len() as u64,
                                        version_id: None,
                                        outcome: "uncertain",
                                        detail: Some(error.to_string()),
                                    });
                                    break;
                                }
                                return Err(format!("live PUT {key} failed with {code:?}: {error}").into());
                            }
                        }
                    }
                    if let Some((baseline_key, baseline_sha)) = baseline.get((writer + round) % baseline.len().max(1)) {
                        retrying_get_matches_sha(&client, &baseline_bucket, baseline_key, baseline_sha, Duration::from_secs(30))
                            .await
                            .map_err(|error| format!("live GET of baseline {baseline_key} failed: {error}"))?;
                    }
                    if round % 3 == 2 {
                        let victim = format!("{prefix}/w{writer:02}/r{:02}.bin", round - 1);
                        match client.delete_object().bucket(&bucket).key(&victim).send().await {
                            Ok(output) => ops.push(LiveOp {
                                op: "delete",
                                key: victim,
                                sha256: None,
                                length: 0,
                                version_id: output.version_id().map(str::to_owned),
                                outcome: "acked",
                                detail: None,
                            }),
                            Err(error) => {
                                let code = error
                                    .as_service_error()
                                    .and_then(ProvideErrorMetadata::code)
                                    .map(str::to_owned);
                                if code.is_some() && !is_availability_code(code.as_deref()) {
                                    return Err(format!("live DELETE {victim} failed with {code:?}: {error}").into());
                                }
                                ops.push(LiveOp {
                                    op: "delete",
                                    key: victim,
                                    sha256: None,
                                    length: 0,
                                    version_id: None,
                                    outcome: "uncertain",
                                    detail: Some(error.to_string()),
                                });
                            }
                        }
                    }
                    sleep(pause).await;
                }
                Ok::<Vec<LiveOp>, Box<dyn std::error::Error + Send + Sync>>(ops)
            };
            work.await
        }));
    }
    let mut ops = Vec::new();
    for handle in handles {
        ops.extend(handle.await??);
    }
    Ok(ops)
}

/// Fold the op log into the state every node must now report.
pub(crate) fn expected_live_state(ops: &[LiveOp]) -> BTreeMap<String, LiveExpectation> {
    let mut state: BTreeMap<String, LiveExpectation> = BTreeMap::new();
    for op in ops {
        let previous = state.remove(&op.key);
        let next = match (op.op, op.outcome) {
            ("put", "acked") => LiveExpectation::Present(op.sha256.clone().unwrap_or_default()),
            ("delete", "acked") => LiveExpectation::Absent,
            _ => {
                let mut candidates = match previous {
                    Some(LiveExpectation::Present(existing)) => vec![existing],
                    Some(LiveExpectation::Uncertain(existing)) => existing,
                    Some(LiveExpectation::Absent) | None => Vec::new(),
                };
                if op.op == "put" {
                    candidates.push(op.sha256.clone().unwrap_or_default());
                }
                LiveExpectation::Uncertain(candidates)
            }
        };
        state.insert(op.key.clone(), next);
    }
    state
}

pub(crate) async fn verify_live_state(
    client: &Client,
    bucket: &str,
    expected: &BTreeMap<String, LiveExpectation>,
    label: &str,
) -> TestResult {
    for (key, expectation) in expected {
        let result = client.get_object().bucket(bucket).key(key).send().await;
        match (expectation, result) {
            (LiveExpectation::Present(sha), Ok(output)) => {
                let body = output.body.collect().await?.into_bytes();
                if &sha256_hex(&body) != sha {
                    return Err(format!("{label}: acked live object {key} returned different bytes").into());
                }
            }
            (LiveExpectation::Present(_), Err(error)) => {
                return Err(format!("{label}: acked live object {key} is unreadable: {error}").into());
            }
            (LiveExpectation::Absent, Ok(_)) => {
                return Err(format!("{label}: deleted live object {key} is still readable").into());
            }
            (LiveExpectation::Absent, Err(error)) if matches!(get_error_code(&error), Some("NoSuchKey" | "NotFound")) => {}
            (LiveExpectation::Absent, Err(error)) => {
                return Err(format!("{label}: deleted live object {key} failed unexpectedly: {error}").into());
            }
            (LiveExpectation::Uncertain(candidates), Ok(output)) => {
                let body = output.body.collect().await?.into_bytes();
                if !candidates.contains(&sha256_hex(&body)) {
                    return Err(format!("{label}: uncertain live object {key} holds bytes that were never written").into());
                }
            }
            (LiveExpectation::Uncertain(_), Err(error)) if matches!(get_error_code(&error), Some("NoSuchKey" | "NotFound")) => {}
            (LiveExpectation::Uncertain(_), Err(error)) => {
                return Err(format!("{label}: uncertain live object {key} failed unexpectedly: {error}").into());
            }
        }
    }
    Ok(())
}

/// Write `count` fresh objects and return their keys with physical pools.
pub(crate) async fn put_probe_objects(
    dist: &DistCluster,
    bucket: &str,
    prefix: &str,
    count: usize,
) -> TestResult<Vec<(String, Vec<u8>)>> {
    let client = dist.client(dist.cluster.nodes.len() - 1)?;
    let mut written = Vec::new();
    for index in 0..count {
        let key = format!("{prefix}/{index:02}.bin");
        let body = payload_for(&key, 64 * 1024);
        super::harness::retrying_put(&client, bucket, &key, body.clone(), Duration::from_secs(30)).await?;
        written.push((key, body));
    }
    Ok(written)
}

/// Every probe object must be readable with exact bytes and live in exactly
/// one pool drawn from `allowed`.
pub(crate) async fn assert_probe_objects(
    dist: &DistCluster,
    bucket: &str,
    written: &[(String, Vec<u8>)],
    allowed: &BTreeSet<usize>,
    label: &str,
) -> TestResult<BTreeMap<String, usize>> {
    let client = dist.client(0)?;
    let mut placement = BTreeMap::new();
    for (key, body) in written {
        let got = get_object_bytes(&client, bucket, key).await?;
        if &got != body {
            return Err(format!("{label}: probe object {key} returned different bytes").into());
        }
        let pools = pools_holding(dist, bucket, key, None)?;
        if pools.len() != 1 || !pools.is_subset(allowed) {
            return Err(
                format!("{label}: probe object {key} is held by pools {pools:?}; expected exactly one of {allowed:?}").into(),
            );
        }
        placement.insert(key.clone(), pools.into_iter().next().unwrap_or(usize::MAX));
    }
    Ok(placement)
}

/// Readiness plus a round-trip PUT/GET through every node.
pub(crate) async fn assert_every_node_serves(dist: &DistCluster, bucket: &str, label: &str) -> TestResult {
    super::harness::wait_for_ready(&dist.cluster).await?;
    for node_idx in 0..dist.cluster.nodes.len() {
        let client = dist.client(node_idx)?;
        let key = format!("liveness/{label}/node-{node_idx}.bin");
        let body = payload_for(&key, 1024);
        super::harness::retrying_put(&client, bucket, &key, body.clone(), Duration::from_secs(30)).await?;
        super::harness::retrying_get_equals(&client, bucket, &key, &body, Duration::from_secs(30)).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_ids_normalize_null_spellings() {
        for spelling in [None, Some(""), Some("null"), Some("00000000-0000-0000-0000-000000000000")] {
            assert_eq!(normalize_version_id(spelling), "null");
        }
        assert_eq!(normalize_version_id(Some("abc")), "abc");
    }

    #[test]
    fn live_state_folds_acks_and_uncertain_outcomes() {
        let ops = vec![
            LiveOp {
                op: "put",
                key: "a".into(),
                sha256: Some("1".into()),
                length: 1,
                version_id: None,
                outcome: "acked",
                detail: None,
            },
            LiveOp {
                op: "put",
                key: "a".into(),
                sha256: Some("2".into()),
                length: 1,
                version_id: None,
                outcome: "uncertain",
                detail: None,
            },
            LiveOp {
                op: "put",
                key: "b".into(),
                sha256: Some("3".into()),
                length: 1,
                version_id: None,
                outcome: "acked",
                detail: None,
            },
            LiveOp {
                op: "delete",
                key: "b".into(),
                sha256: None,
                length: 0,
                version_id: None,
                outcome: "acked",
                detail: None,
            },
        ];
        let state = expected_live_state(&ops);
        assert!(
            matches!(&state["a"], LiveExpectation::Uncertain(candidates) if candidates == &vec!["1".to_string(), "2".to_string()])
        );
        assert!(matches!(&state["b"], LiveExpectation::Absent));
    }

    #[test]
    fn rebalance_stopped_requires_terminal_pools_without_pending_propagation() {
        let stopped = serde_json::json!({
            "id": "run-1",
            "pools": [
                { "id": 0, "status": "Stopped", "stopping": false },
                { "id": 1, "status": "None", "stopping": false }
            ]
        });
        assert!(rebalance_stopped(&stopped, "run-1").unwrap());
        let stopping = serde_json::json!({ "id": "run-1", "pools": [{ "id": 0, "status": "Started", "stopping": true }] });
        assert!(!rebalance_stopped(&stopping, "run-1").unwrap());
        let pending = serde_json::json!({
            "id": "run-1",
            "pools": [{ "id": 0, "status": "Stopped", "stopping": false }],
            "stopPropagation": { "pendingTerminalReload": true }
        });
        assert!(!rebalance_stopped(&pending, "run-1").unwrap());
        assert!(rebalance_stopped(&stopped, "run-2").is_err());
    }

    #[test]
    fn durability_log_check_requires_strict_events() {
        let dir = std::env::temp_dir().join(format!("sht-durability-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let strict = dir.join("node-0.log");
        std::fs::write(
            &strict,
            "{\"fields\":{\"event\":\"disk_local_durability_mode\",\"mode\":\"strict\"}}\nnot json\n",
        )
        .unwrap();
        assert_eq!(assert_process_durability_strict(std::slice::from_ref(&strict)).unwrap(), 1);
        let relaxed = dir.join("node-1.log");
        std::fs::write(&relaxed, "{\"fields\":{\"event\":\"disk_local_durability_mode\",\"mode\":\"relaxed\"}}\n").unwrap();
        assert!(assert_process_durability_strict(&[strict, relaxed]).is_err());
        let silent = dir.join("node-2.log");
        std::fs::write(&silent, "{\"fields\":{\"event\":\"other\"}}\n").unwrap();
        assert!(assert_process_durability_strict(&[silent]).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
