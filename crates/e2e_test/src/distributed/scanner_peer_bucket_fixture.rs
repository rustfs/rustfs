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

//! Real multi-set fixture for peer-owned dirty-usage certification.
//! The file barrier only observes or pauses production work. Every dirty
//! generation in these cases originates from an acknowledged S3 operation.

use super::harness::{DistCluster, DistLayout, TestResult, enable_versioning, payload_for, sha256_hex, wait_until};
use super::scanner_heal_topology_fixture::{assert_bucket_durability_strict, assert_process_durability_strict};
use crate::chaos::census_object_version_on_disk;
use crate::common::admin_request;
use crate::rpc_fault_proxy::{RpcFaultProxy, ScopedAckQueue, ScopedAckScope};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use http::Method;
use rustfs_data_usage::DataUsageInfo;
use rustfs_ecstore::api::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use rustfs_ecstore::api::erasure::{ReedSolomonEncoder, calc_shard_size};
use rustfs_ecstore::api::rpc::PeerRestClient;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

const BARRIER_DIR_ENV: &str = "RUSTFS_E2E_SCANNER_BARRIER_DIR";
const EVIDENCE_DIR_ENV: &str = "RUSTFS_SCANNER_PEER_BUCKET_EVIDENCE_DIR";
pub(super) const CYCLE_TIMEOUT: Duration = Duration::from_secs(180);
const OBSERVATION_LIMIT: usize = 512;
const OBSERVATION_BYTES_LIMIT: u64 = 1024 * 1024;

#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct ObjectReceipt {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub length: u64,
    pub sha256: String,
    pub ingress_node: usize,
    pub set: usize,
}

pub(super) struct PeerBucketRun {
    pub dist: DistCluster,
    pub buckets: [String; 3],
    pub objects: Vec<ObjectReceipt>,
    pub barrier: ScannerBarrier,
    pub topology: Value,
    pub artifacts: PathBuf,
    keep_artifacts: bool,
    drive_sets: BTreeMap<String, usize>,
    proxies: Vec<RpcFaultProxy>,
}

impl PeerBucketRun {
    pub async fn start(label: &str) -> TestResult<Self> {
        Self::start_inner(label, false, false).await
    }

    pub async fn start_with_ack_faults(label: &str) -> TestResult<Self> {
        Self::start_inner(label, true, false).await
    }

    pub async fn start_for_crash(label: &str) -> TestResult<Self> {
        Self::start_inner(label, true, true).await
    }

    #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
    pub async fn replace_followers_with_old_binary(&mut self, old_binary: &Path, expected_old_sha256: &str) -> TestResult<Value> {
        if expected_old_sha256.len() != 64 || !expected_old_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("mixed-version fixture requires the qualified old executable digest".into());
        }
        let old_binary = old_binary.canonicalize()?;
        let (_, old_sha256) = mixed_executable_hash(&old_binary)?;
        if old_sha256 != expected_old_sha256 {
            return Err("old executable differs from the qualified mixed-version artifact".into());
        }
        let current = self.executable_identity()?;
        let new_binary = PathBuf::from(current["path"].as_str().ok_or("current executable omitted its path")?);
        let new_sha256 = current["sha256"].as_str().ok_or("current executable omitted its digest")?;
        if old_sha256 == new_sha256 {
            return Err("mixed-version fixture requires distinct old and new executables".into());
        }
        self.quiet_baseline().await?;
        let leader = self.leader().await?;
        let retained = self.dirty_snapshot(leader).await?;
        let mut initial_processes = Vec::new();
        for node in 0..4 {
            initial_processes.push(self.process_executable_identity(node, &new_binary, new_sha256)?);
        }
        let drives_before = (0..4).map(|node| self.drive_identity(node)).collect::<TestResult<Vec<_>>>()?;
        let mut replacements = Vec::new();
        use std::io::{Read as _, Seek as _, SeekFrom};
        use std::os::unix::fs::MetadataExt as _;
        for node in (0..4).filter(|node| *node != leader) {
            let log_path = PathBuf::from(
                self.dist.cluster.node_capture_log_paths[node]
                    .as_deref()
                    .ok_or("mixed-version node log is absent")?,
            );
            let previous_peer = self.dirty_snapshot(node).await?;
            let log_before = std::fs::symlink_metadata(&log_path)?;
            if !log_before.is_file() || log_before.file_type().is_symlink() {
                return Err("mixed-version node log is not a regular captured file".into());
            }
            let log_offset = log_before.len();
            self.dist.cluster.stop_node_gracefully(node).await?;
            self.dist.cluster.start_node_from_binary(node, &old_binary).await?;
            let process = self.process_executable_identity(node, &old_binary, expected_old_sha256)?;
            if self.leader().await? != leader
                || u64::from(self.pid(leader)?) != retained["pid"].as_u64().ok_or("leader omitted PID")?
            {
                return Err("mixed-version replacement lost the retained new coordinator".into());
            }
            let current_leader = self.dirty_snapshot(leader).await?;
            if current_leader["owner_id"] != retained["owner_id"] || current_leader["instance_id"] != retained["instance_id"] {
                return Err("mixed-version replacement changed the retained coordinator instance".into());
            }
            let expected_drives = std::mem::take(&mut self.drive_sets);
            self.verify_topology().await?;
            if self.drive_sets != expected_drives || self.drive_identity(node)? != drives_before[node] {
                return Err("mixed-version replacement changed drive placement or format identity".into());
            }
            let mut log = std::fs::File::open(&log_path)?;
            let log_after = log.metadata()?;
            if log_after.dev() != log_before.dev() || log_after.ino() != log_before.ino() || log_after.len() < log_offset {
                return Err("mixed-version node log was replaced or shortened".into());
            }
            log.seek(SeekFrom::Start(log_offset))?;
            let mut startup = Vec::new();
            log.by_ref().take(OBSERVATION_BYTES_LIMIT).read_to_end(&mut startup)?;
            let current_log = std::fs::symlink_metadata(&log_path)?;
            if !current_log.is_file()
                || current_log.file_type().is_symlink()
                || current_log.dev() != log_before.dev()
                || current_log.ino() != log_before.ino()
                || current_log.len() < log_offset
            {
                return Err("mixed-version node log changed during startup prefix inspection".into());
            }
            let last_line = startup
                .iter()
                .rposition(|byte| *byte == b'\n')
                .ok_or("replacement startup prefix has no complete log line")?;
            startup.truncate(last_line + 1);
            let startup_log = self.artifacts.join(format!("mixed-node-{node}-startup.jsonl"));
            std::fs::write(&startup_log, &startup)?;
            if assert_process_durability_strict(&[startup_log])? != 1 {
                return Err("replacement process did not report strict durability in its bounded startup prefix".into());
            }
            let replacement_peer = self.mixed_replacement_snapshot(node, &previous_peer, &process).await?;
            replacements.push(
                json!({"node": node, "process": process, "previous_peer": previous_peer, "peer": replacement_peer,
                "retained_leader": current_leader, "startup_log": {"path": log_path, "dev": log_before.dev(),
                    "inode": log_before.ino(), "offset": log_offset, "prefix_bytes": startup.len(),
                    "prefix_sha256": sha256_hex(&startup), "prefix_limit": OBSERVATION_BYTES_LIMIT}}),
            );
        }
        self.quiet_baseline().await?;
        let processes = self.mixed_process_identities(leader, &old_binary, expected_old_sha256)?;
        if processes[leader] != initial_processes[leader]
            || replacements.iter().any(|replacement| {
                replacement["node"]
                    .as_u64()
                    .and_then(|node| usize::try_from(node).ok())
                    .is_none_or(|node| processes.get(node) != Some(&replacement["process"]))
            })
        {
            return Err("mixed-version setup replaced a previously measured process".into());
        }
        let mut peers = Vec::new();
        for node in 0..4 {
            peers.push(self.dirty_snapshot(node).await?);
        }
        let drives_after = (0..4).map(|node| self.drive_identity(node)).collect::<TestResult<Vec<_>>>()?;
        if drives_before != drives_after {
            return Err("mixed-version setup changed a retained drive identity".into());
        }
        let fleet = json!({"leader": leader, "leader_pid": self.pid(leader)?, "retained": retained,
            "old_sha256": expected_old_sha256, "new_executable": current, "old_binary": old_binary,
            "initial_processes": initial_processes, "processes": processes, "peers": peers,
            "replacements": replacements, "drives_before": drives_before, "drives_after": drives_after});
        self.assert_mixed_roles(&fleet).await?;
        self.write_evidence("mixed-version-fleet", &fleet)?;
        Ok(fleet)
    }

    #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
    async fn mixed_replacement_snapshot(&self, node: usize, previous: &Value, process: &Value) -> TestResult<Value> {
        let mut attempts = 1;
        let snapshot = match self.dirty_snapshot(node).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
                let mut epoch_changed = false;
                for _ in 0..8 {
                    let Some(current) = cause else { break };
                    if let Some(status) = current.downcast_ref::<tonic::Status>() {
                        epoch_changed = status.code() == tonic::Code::Unavailable && status.message() == "RPC boot epoch changed";
                        break;
                    }
                    cause = current.source();
                }
                if !epoch_changed {
                    return Err(error);
                }
                self.write_evidence(
                    &format!("mixed-node-{node}-rpc-rejection"),
                    &json!({"node": node, "pid": self.pid(node)?, "attempt": 1,
                        "code": "Unavailable", "message": "RPC boot epoch changed"}),
                )?;
                // The authenticated rejection refreshes this peer's boot proof;
                // one new read must then validate normally on the replacement.
                attempts = 2;
                self.dirty_snapshot(node).await?
            }
        };
        if snapshot["pid"] != process["pid"]
            || snapshot["pid"] == previous["pid"]
            || snapshot["owner_id"] != previous["owner_id"]
            || snapshot["instance_id"] == previous["instance_id"]
            || snapshot["host"] != previous["host"]
        {
            return Err("mixed-version replacement did not bind a new process on the unchanged owner and endpoint".into());
        }
        self.write_evidence(
            &format!("mixed-node-{node}-rpc-handshake"),
            &json!({"attempts": attempts, "previous": previous,
            "process": process, "snapshot": snapshot}),
        )?;
        Ok(snapshot)
    }

    #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
    pub fn mixed_process_identities(&mut self, leader: usize, old_binary: &Path, old_sha256: &str) -> TestResult<Vec<Value>> {
        let current = self.executable_identity()?;
        let new_binary = PathBuf::from(current["path"].as_str().ok_or("current executable omitted its path")?);
        let new_sha256 = current["sha256"].as_str().ok_or("current executable omitted its digest")?;
        let mut processes = Vec::new();
        for node in 0..4 {
            let (path, digest) = if node == leader {
                (new_binary.as_path(), new_sha256)
            } else {
                (old_binary, old_sha256)
            };
            processes.push(self.process_executable_identity(node, path, digest)?);
        }
        Ok(processes)
    }

    #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
    pub async fn assert_mixed_roles(&self, fleet: &Value) -> TestResult<Value> {
        let leader = usize::try_from(fleet["leader"].as_u64().ok_or("mixed fleet omitted coordinator")?)?;
        if self.leader().await? != leader || fleet["leader_pid"].as_u64() != Some(u64::from(self.pid(leader)?)) {
            return Err("mixed-version window lost the actual new coordinator".into());
        }
        let processes = fleet["processes"]
            .as_array()
            .filter(|entries| entries.len() == 4)
            .ok_or("mixed fleet omitted four processes")?;
        let peers = fleet["peers"]
            .as_array()
            .filter(|entries| entries.len() == 4)
            .ok_or("mixed fleet omitted four peer identities")?;
        let mut snapshots = Vec::new();
        for node in 0..4 {
            let pid = self.pid(node)?;
            if processes[node]["pid"].as_u64() != Some(u64::from(pid))
                || processes[node]["start_time_ticks"].as_u64() != Some(proc_start_time(pid)?)
            {
                return Err("mixed-version window replaced a measured process".into());
            }
            let peer = self.dirty_snapshot(node).await?;
            if peer["owner_id"] != peers[node]["owner_id"] || peer["instance_id"] != peers[node]["instance_id"] {
                return Err("mixed-version window changed an authenticated peer identity".into());
            }
            snapshots.push(peer);
        }
        if snapshots[leader]["instance_id"] != fleet["retained"]["instance_id"]
            || snapshots[leader]["owner_id"] != fleet["retained"]["owner_id"]
        {
            return Err("mixed-version setup did not retain the original new coordinator".into());
        }
        Ok(json!({"leader": leader, "leader_pid": self.pid(leader)?, "peers": snapshots}))
    }

    #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
    fn process_executable_identity(&mut self, node: usize, expected_path: &Path, expected_sha256: &str) -> TestResult<Value> {
        let child = self.dist.cluster.nodes[node]
            .process
            .as_mut()
            .ok_or("mixed-version child is absent")?;
        if child.try_wait()?.is_some() {
            return Err("mixed-version child exited before executable inspection".into());
        }
        let pid = child.id();
        let start_time = proc_start_time(pid)?;
        let executable = PathBuf::from(format!("/proc/{pid}/exe"));
        let path = std::fs::read_link(&executable)?;
        if path != expected_path.canonicalize()? {
            return Err(format!("process {pid} does not execute its expected mixed-version binary").into());
        }
        let (size, digest) = mixed_executable_hash(&executable)?;
        if digest != expected_sha256 || proc_start_time(pid)? != start_time || child.try_wait()?.is_some() {
            return Err(format!("process {pid} changed or failed its actual executable digest").into());
        }
        Ok(json!({"node": node, "pid": pid, "start_time_ticks": start_time, "path": path,
            "size": size, "sha256": digest, "expected_sha256": expected_sha256}))
    }

    async fn start_inner(label: &str, proxy_acks: bool, read_peer_snapshots: bool) -> TestResult<Self> {
        crate::common::rustfs_binary_path_with_features(Some("e2e-test-hooks"));
        let nonce = Uuid::new_v4().simple().to_string();
        let kept_root = std::env::var_os(EVIDENCE_DIR_ENV).or_else(|| std::env::var_os("RUSTFS_E2E_LOG_DIR"));
        let keep_artifacts = kept_root.is_some();
        let artifacts = kept_root
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("scanner-peer-bucket-{label}-{nonce}"));
        let barrier_dir = artifacts.join("scanner-barrier");
        std::fs::create_dir_all(&barrier_dir)?;
        if !barrier_dir.is_absolute() {
            return Err("scanner barrier directory must be absolute".into());
        }
        let barrier_value = barrier_dir.to_string_lossy().into_owned();
        // An explicit cycle or start-delay value disables adaptive dirty
        // wakeups. The fixture uses real mutations on the default cadence.
        let env = [
            ("RUSTFS_SCANNER_ENABLED", "true"),
            ("RUSTFS_SCANNER_BITROT_CYCLE_SECS", "off"),
            ("RUSTFS_ERASURE_SET_DRIVE_COUNT", "4"),
            ("RUSTFS_STORAGE_CLASS_STANDARD", "EC:2"),
            ("RUSTFS_DURABILITY_MODE", "strict"),
            ("RUSTFS_NEW_BUCKET_DURABILITY_MODE", "strict"),
            ("RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE", "false"),
            (BARRIER_DIR_ENV, barrier_value.as_str()),
        ];
        let mut dist = DistCluster::new_stopped_with_env(DistLayout::FourByFour, &env).await?;
        if read_peer_snapshots {
            // Match the existing internode RPC tests without relaxing any
            // authentication or replay policy. These clients only read state.
            const RPC_SECRET: &str = "rustfs-internode-signature-e2e-secret";
            let _ = rustfs_credentials::set_global_rpc_secret(RPC_SECRET.to_string());
            if rustfs_credentials::try_get_rpc_token()? != RPC_SECRET {
                return Err("crash fixture could not establish its authenticated snapshot reader".into());
            }
            dist.cluster.set_env("RUSTFS_RPC_SECRET", RPC_SECRET);
        }
        let mut proxies = Vec::new();
        if proxy_acks {
            // Keep endpoint locality's shared port while separating the proxy
            // and direct server listeners by loopback address family.
            for node in &mut dist.cluster.nodes {
                let proxy_address = node.address.parse::<SocketAddr>()?;
                let direct_address = SocketAddr::from((Ipv6Addr::LOCALHOST, proxy_address.port()));
                node.address = direct_address.to_string();
                node.url = format!("http://{direct_address}");
                proxies.push(RpcFaultProxy::start_on(proxy_address, direct_address).await?);
            }
            dist.cluster.set_env("NO_PROXY", "localhost,127.0.0.1,::1");
            dist.cluster.set_env("no_proxy", "localhost,127.0.0.1,::1");
        }
        let peer_addresses = (0..4)
            .map(|node| {
                proxies
                    .get(node)
                    .map_or_else(|| dist.cluster.nodes[node].address.clone(), |proxy| proxy.local_addr().to_string())
            })
            .collect::<Vec<_>>();
        // Each set contains one drive from each process. The harness defaults
        // to node-major order, which would put all four drives of a set on one node.
        let volumes = (0..4)
            .flat_map(|drive| {
                let peer_addresses = &peer_addresses;
                dist.cluster
                    .nodes
                    .iter()
                    .enumerate()
                    .map(move |(index, node)| format!("http://{}{}", peer_addresses[index], node.data_dirs[drive]))
            })
            .collect::<Vec<_>>()
            .join(" ");
        dist.cluster.set_env("RUSTFS_VOLUMES", volumes.clone());
        dist.capture_logs_under(&artifacts.join("node-logs"))?;
        dist.cluster.start().await?;
        let buckets = [format!("cold-a-{nonce}"), format!("cold-b-{nonce}"), format!("hot-{nonce}")];
        let mut run = Self {
            dist,
            buckets,
            objects: Vec::new(),
            barrier: ScannerBarrier {
                directory: barrier_dir,
                nonce,
            },
            topology: Value::Null,
            artifacts,
            keep_artifacts,
            drive_sets: BTreeMap::new(),
            proxies,
        };
        if proxy_acks {
            let nodes = (0..4)
                .map(|node| {
                    Ok(json!({
                        "node": node,
                        "proxy_address": run.proxies[node].local_addr().to_string(),
                        "direct_address": run.dist.cluster.nodes[node].address,
                        "direct_url": run.dist.cluster.nodes[node].url,
                        "pid": run.pid(node)?,
                    }))
                })
                .collect::<TestResult<Vec<_>>>()?;
            run.write_evidence("proxy-routing", &json!({ "nodes": nodes, "volumes": volumes }))?;
        }
        run.verify_topology().await?;
        run.seed().await?;
        let bucket_names = run.buckets.iter().map(String::as_str).collect::<Vec<_>>();
        assert_bucket_durability_strict(&run.dist.cluster, &bucket_names).await?;
        let logs = run
            .dist
            .cluster
            .node_capture_log_paths
            .iter()
            .flatten()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        let reported = assert_process_durability_strict(&logs)?;
        if reported != 4 {
            return Err(format!("strict process durability was observed on {reported}/4 nodes").into());
        }
        Ok(run)
    }

    async fn verify_topology(&mut self) -> TestResult {
        let storage = self.admin_json(0, "/rustfs/admin/v3/storageinfo").await?;
        let disks = storage["info"]["disks"].as_array().ok_or("storageinfo omitted disks")?;
        if disks.len() != 16 {
            return Err(format!("expected sixteen actual disks, got {}", disks.len()).into());
        }
        let mut set_nodes = BTreeMap::<usize, BTreeSet<usize>>::new();
        let mut set_disks = BTreeMap::<usize, BTreeSet<u64>>::new();
        let peer_addresses = (0..4).map(|node| self.peer_address(node)).collect::<Vec<_>>();
        for disk in disks {
            if disk["pool_index"].as_u64() != Some(0)
                || disk["state"].as_str().is_none_or(|state| !state.eq_ignore_ascii_case("ok"))
                || disk["runtimeState"]
                    .as_str()
                    .is_none_or(|state| !state.eq_ignore_ascii_case("online"))
            {
                return Err(format!("disk is outside the healthy single-pool topology: {disk}").into());
            }
            let set = usize::try_from(disk["set_index"].as_u64().ok_or("disk omitted set_index")?)?;
            let disk_index = disk["disk_index"].as_u64().ok_or("disk omitted disk_index")?;
            let endpoint = disk["endpoint"].as_str().ok_or("disk omitted endpoint")?;
            let matches = self
                .dist
                .cluster
                .nodes
                .iter()
                .enumerate()
                .flat_map(|(node_index, node)| {
                    let peer_addresses = &peer_addresses;
                    node.data_dirs.iter().filter_map(move |path| {
                        (endpoint.trim_end_matches('/') == format!("http://{}{}", peer_addresses[node_index], path))
                            .then_some((node_index, path))
                    })
                })
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                return Err(format!("storage endpoint does not identify one fixture drive: {endpoint}").into());
            }
            let (node, path) = matches[0];
            if self.drive_sets.insert(path.clone(), set).is_some()
                || !set_nodes.entry(set).or_default().insert(node)
                || !set_disks.entry(set).or_default().insert(disk_index)
            {
                return Err(format!("duplicate drive, node, or disk index in set {set}").into());
            }
        }
        let expected = BTreeSet::from([0, 1, 2, 3]);
        if set_nodes.keys().copied().collect::<BTreeSet<_>>() != expected
            || set_nodes.values().any(|nodes| nodes != &expected)
            || set_disks.values().any(|disks| disks != &BTreeSet::from([0, 1, 2, 3]))
        {
            return Err(format!("actual erasure sets do not each span four nodes: {set_nodes:?}, {set_disks:?}").into());
        }
        self.topology = storage;
        Ok(())
    }

    async fn seed(&mut self) -> TestResult {
        let client = self.dist.client(0)?;
        for bucket in self.buckets.clone() {
            client.create_bucket().bucket(&bucket).send().await?;
            enable_versioning(&client, &bucket).await?;
            let mut covered = BTreeSet::new();
            for index in 0..128 {
                let receipt = self.put(0, &bucket, &format!("seed-{index:03}"), 71).await?;
                covered.insert(receipt.set);
                if covered == BTreeSet::from([0, 1, 2, 3]) {
                    break;
                }
            }
            if covered != BTreeSet::from([0, 1, 2, 3]) {
                return Err(format!("bounded real PUT inventory did not cover four sets for {bucket}: {covered:?}").into());
            }
        }
        self.verify_objects().await
    }

    pub async fn put(&mut self, node: usize, bucket: &str, key: &str, length: usize) -> TestResult<ObjectReceipt> {
        let receipt = write_object(&self.dist.client(node)?, &self.drive_inventory()?, node, bucket, key, length).await?;
        self.objects.push(receipt.clone());
        Ok(receipt)
    }

    fn drive_inventory(&self) -> TestResult<Vec<Vec<(String, usize)>>> {
        self.dist
            .cluster
            .nodes
            .iter()
            .map(|node| {
                node.data_dirs
                    .iter()
                    .map(|drive| {
                        Ok((
                            drive.clone(),
                            *self
                                .drive_sets
                                .get(drive)
                                .ok_or("fixture drive missing from actual topology")?,
                        ))
                    })
                    .collect()
            })
            .collect()
    }

    pub fn start_hot_writer(&self, node: usize, bucket: &str) -> TestResult<HotWriter> {
        self.start_alternating_hot_writer(&[node], bucket)
    }

    pub fn start_alternating_hot_writer(&self, nodes: &[usize], bucket: &str) -> TestResult<HotWriter> {
        if nodes.is_empty()
            || nodes.len() > 2
            || nodes.iter().any(|node| *node >= 4)
            || nodes.iter().collect::<BTreeSet<_>>().len() != nodes.len()
        {
            return Err("hot writer requires one or two distinct fixture ingress nodes".into());
        }
        let clients = nodes
            .iter()
            .map(|node| self.dist.client(*node).map(|client| (*node, client)))
            .collect::<TestResult<Vec<_>>>()?;
        let drives = self.drive_inventory()?;
        let bucket = bucket.to_string();
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let (progress, completed) = tokio::sync::watch::channel(0);
        let task = tokio::spawn(async move {
            let mut receipts = Vec::new();
            for index in 0..256 {
                if *stopped.borrow() {
                    return Ok(receipts);
                }
                let (node, client) = &clients[index % clients.len()];
                receipts.push(write_object(client, &drives, *node, &bucket, &format!("hot-stream-{index:03}"), 83).await?);
                let _ = progress.send(receipts.len());
                tokio::select! {
                    _ = stopped.changed() => {},
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {},
                }
            }
            Err("bounded hot writer exhausted 256 actual PUTs before certification completed".into())
        });
        Ok(HotWriter {
            stop,
            completed,
            task: Some(task),
        })
    }

    pub fn peer_address(&self, node: usize) -> String {
        self.proxies
            .get(node)
            .map_or_else(|| self.dist.cluster.nodes[node].address.clone(), |proxy| proxy.local_addr().to_string())
    }

    pub fn scoped_ack_queue(
        &self,
        node: usize,
        owner: &Value,
        buckets: BTreeSet<String>,
        capacity: usize,
    ) -> TestResult<ScopedAckQueue> {
        let proxy = self.proxies.get(node).ok_or("fixture did not enable the scoped ACK proxy")?;
        Ok(proxy.scoped_ack_queue(
            ScopedAckScope {
                nonce: self.barrier.nonce.clone(),
                owner_id: owner["owner_id"]
                    .as_str()
                    .ok_or("scoped ACK receiver omitted owner")?
                    .to_string(),
                instance_id: owner["instance_id"]
                    .as_str()
                    .ok_or("scoped ACK receiver omitted instance")?
                    .to_string(),
                buckets,
            },
            capacity,
        )?)
    }

    pub async fn verify_objects(&self) -> TestResult {
        for client in self.dist.clients()? {
            for bucket in &self.buckets {
                let listed = client.list_object_versions().bucket(bucket).send().await?;
                if listed.is_truncated() == Some(true) || !listed.delete_markers().is_empty() {
                    return Err("fixture version listing is truncated or contains an unexpected delete marker".into());
                }
                let expected = self
                    .objects
                    .iter()
                    .filter(|receipt| receipt.bucket == *bucket)
                    .map(|receipt| (receipt.key.as_str(), receipt.version_id.as_str(), receipt.length as i64))
                    .collect::<BTreeSet<_>>();
                let actual = listed
                    .versions()
                    .iter()
                    .map(|version| {
                        Ok((
                            version.key().ok_or("version listing omitted key")?,
                            version.version_id().ok_or("version listing omitted version ID")?,
                            version.size().ok_or("version listing omitted size")?,
                        ))
                    })
                    .collect::<TestResult<BTreeSet<_>>>()?;
                if actual != expected {
                    return Err(format!("version listing differs from acknowledged S3 receipts for {bucket}").into());
                }
            }
            for receipt in &self.objects {
                let output = client
                    .get_object()
                    .bucket(&receipt.bucket)
                    .key(&receipt.key)
                    .version_id(&receipt.version_id)
                    .send()
                    .await?;
                if output.version_id() != Some(receipt.version_id.as_str()) {
                    return Err(format!("GET returned a different version for {}", receipt.key).into());
                }
                let body = output.body.collect().await?.into_bytes();
                if body.len() as u64 != receipt.length || sha256_hex(&body) != receipt.sha256 {
                    return Err(format!("GET bytes differ from acknowledged S3 receipt for {}", receipt.key).into());
                }
            }
        }
        Ok(())
    }

    pub async fn admin_json(&self, node: usize, path: &str) -> TestResult<Value> {
        let (status, body) = admin_request(
            &self.dist.cluster.nodes[node].url,
            Method::GET,
            path,
            None,
            &self.dist.cluster.access_key,
            &self.dist.cluster.secret_key,
        )
        .await?;
        if !status.is_success() {
            return Err(format!("node {node} GET {path} returned {status}: {body}").into());
        }
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn leader(&self) -> TestResult<usize> {
        let mut leaders = Vec::new();
        for node in 0..self.dist.cluster.nodes.len() {
            let status = self.admin_json(node, "/rustfs/admin/v3/scanner/status").await?;
            if status["metrics"]["leader_lock_held_by_this_process"].as_bool() == Some(true) {
                leaders.push(node);
            }
        }
        if leaders.len() != 1 {
            return Err(format!("expected one observed scanner coordinator, got {leaders:?}").into());
        }
        Ok(leaders[0])
    }

    pub async fn assert_default_cadence(&self) -> TestResult<Vec<Value>> {
        let mut configs = Vec::new();
        for node in 0..self.dist.cluster.nodes.len() {
            let status = self.admin_json(node, "/rustfs/admin/v3/scanner/status").await?;
            let config = &status["runtime_config"];
            if config["cycle_interval_seconds"]["source"].as_str() != Some("default")
                || config["start_delay_seconds"]["source"].as_str() != Some("default")
                || !config["start_delay_seconds"]["value"].is_null()
            {
                return Err(
                    format!("node {node} must use the default scanner cadence without a start-delay override: {config}").into(),
                );
            }
            configs.push(config.clone());
        }
        Ok(configs)
    }

    pub fn pid(&self, node: usize) -> TestResult<u32> {
        self.dist.cluster.nodes[node]
            .process
            .as_ref()
            .map(std::process::Child::id)
            .ok_or_else(|| format!("node {node} is not running").into())
    }

    pub async fn dirty_snapshot(&self, node: usize) -> TestResult<Value> {
        let address = self.peer_address(node).parse::<SocketAddr>()?;
        let client = PeerRestClient::new(
            rustfs_utils::net::XHost {
                name: address.ip().to_string(),
                port: address.port(),
                is_port_set: true,
            },
            format!("http://{address}"),
        );
        // The production client verifies the challenge-bound response proof,
        // protocol, owner, incarnation, ordering and generation bounds.
        let snapshot = client.scanner_dirty_usage_snapshot().await?;
        Ok(json!({
            "node": node, "pid": self.pid(node)?, "host": address.to_string(),
            "owner_id": snapshot.owner_id, "instance_id": snapshot.instance_id,
            "generation": snapshot.generation, "pending_bucket_count": snapshot.pending_bucket_count,
            "protocol_version": snapshot.protocol_version, "complete": snapshot.complete,
            "buckets": snapshot.buckets.into_iter().map(|(bucket, entry)| json!({
                "bucket": bucket, "bucket_incarnation": entry.bucket_incarnation, "generation": entry.generation,
            })).collect::<Vec<_>>(),
        }))
    }

    pub fn drive_identity(&self, node: usize) -> TestResult<Value> {
        self.dist.cluster.nodes[node]
            .data_dirs
            .iter()
            .map(|drive| {
                let path = Path::new(drive).join(RUSTFS_META_BUCKET).join("format.json");
                let bytes = read_bounded_file(&path)?;
                Ok(json!({"drive": drive, "set": self.drive_sets[drive], "format_sha256": sha256_hex(&bytes),
                    "format": serde_json::from_slice::<Value>(&bytes)?}))
            })
            .collect::<TestResult<Vec<_>>>()
            .map(Value::Array)
    }

    pub fn executable_identity(&self) -> TestResult<Value> {
        use sha2::{Digest, Sha256};
        use std::io::Read;

        let path = crate::common::rustfs_binary_path_with_features(Some("e2e-test-hooks"));
        let mut file = std::fs::File::open(&path)?;
        let size = file.metadata()?.len();
        let mut digest = Sha256::new();
        let mut buffer = [0; 64 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
        Ok(json!({"path": path, "size": size,
            "sha256": hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower)}))
    }

    pub fn kill_at(&mut self, node: usize, gate: &Value) -> TestResult<Value> {
        if !cfg!(unix) {
            return Err("scanner crash windows require Unix SIGKILL semantics".into());
        }
        let pid = self.pid(node)?;
        if gate["armed"] != true || gate["pid"].as_u64() != Some(u64::from(pid)) {
            return Err("crash target is not the actual process held at the requested barrier".into());
        }
        let drives = self.drive_identity(node)?;
        let process = self.dist.cluster.nodes[node]
            .process
            .as_mut()
            .ok_or("crash target process disappeared")?;
        if process.try_wait()?.is_some() {
            return Err("crash target exited before SIGKILL".into());
        }
        process.kill()?;
        let status = process.wait()?;
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if status.signal() != Some(9) {
                return Err(format!("scanner crash did not terminate with SIGKILL: {status}").into());
            }
        }
        self.dist.cluster.nodes[node].process = None;
        Ok(
            json!({"node": node, "pid": pid, "signal": 9, "wait_status": status.to_string(), "gate": gate,
            "drives": drives, "direct_address": self.dist.cluster.nodes[node].address, "peer_address": self.peer_address(node)}),
        )
    }

    pub async fn restart_after_crash(&mut self, crash: &Value, old_snapshot: &Value) -> TestResult<Value> {
        let node = usize::try_from(crash["node"].as_u64().ok_or("crash receipt omitted node")?)?;
        if self.drive_identity(node)? != crash["drives"] {
            return Err("crashed node drive paths or format identity changed before restart".into());
        }
        self.dist.cluster.start_node(node).await?;
        super::harness::wait_for_ready(&self.dist.cluster).await?;
        let mut snapshot_attempts = 1;
        let snapshot = match self.dirty_snapshot(node).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
                let mut epoch_changed = false;
                for _ in 0..8 {
                    let Some(current) = cause else { break };
                    if let Some(status) = current.downcast_ref::<tonic::Status>() {
                        epoch_changed = status.code() == tonic::Code::Unavailable && status.message() == "RPC boot epoch changed";
                        break;
                    }
                    cause = current.source();
                }
                if !epoch_changed {
                    return Err(error);
                }
                self.write_evidence(
                    "restart-rpc-rejection",
                    &json!({"node": node, "pid": self.pid(node)?, "attempt": 1,
                        "code": "Unavailable", "message": "RPC boot epoch changed"}),
                )?;
                // The rejected read refreshes the authenticated peer epoch.
                // A fresh client must then validate one new snapshot normally.
                snapshot_attempts = 2;
                self.dirty_snapshot(node).await?
            }
        };
        if snapshot["pid"] == crash["pid"]
            || snapshot["instance_id"] == old_snapshot["instance_id"]
            || snapshot["owner_id"] != old_snapshot["owner_id"]
            || self.drive_identity(node)? != crash["drives"]
            || self.dist.cluster.nodes[node].address != crash["direct_address"]
            || self.peer_address(node) != crash["peer_address"]
        {
            return Err("restart did not replace the process instance on unchanged owner, endpoints and drives".into());
        }
        let names = self.buckets.iter().map(String::as_str).collect::<Vec<_>>();
        assert_bucket_durability_strict(&self.dist.cluster, &names).await?;
        self.assert_default_cadence().await?;
        self.write_evidence("restart-rpc-handshake", &json!({"attempts": snapshot_attempts, "snapshot": snapshot}))?;
        Ok(snapshot)
    }

    /// Decode only the bounded single-stripe root used by this fixture. Every
    /// present replica must agree; this never elects a convenient old revision.
    pub fn authoritative_root(&self) -> TestResult<Value> {
        let key = format!("{BUCKET_META_PREFIX}/{}", rustfs_data_usage::DATA_USAGE_OBJECT_NAME);
        let mut common = None;
        let mut shards = BTreeMap::new();
        let mut locations = Vec::new();
        let mut nodes = BTreeSet::new();
        let mut sets = BTreeSet::new();
        for (node, process) in self.dist.cluster.nodes.iter().enumerate() {
            for drive in &process.data_dirs {
                let object = Path::new(drive).join(RUSTFS_META_BUCKET).join(&key);
                let path = object.join("xl.meta");
                if !path.try_exists()? {
                    continue;
                }
                let metadata = read_bounded_file(&path)?;
                let info =
                    rustfs_filemeta::FileMeta::load(&metadata)?.into_fileinfo(RUSTFS_META_BUCKET, &key, "", true, false, true)?;
                info.validate_for_metadata_read()?;
                let size = usize::try_from(info.size)?;
                if info.deleted
                    || info.uses_legacy_checksum
                    || size == 0
                    || size > 1024 * 1024
                    || size > info.erasure.block_size
                    || info.parts.len() != 1
                    || info.parts[0].size != size
                    || info.erasure.data_blocks != 2
                    || info.erasure.parity_blocks != 2
                    || !(1..=4).contains(&info.erasure.index)
                    || info.erasure.distribution.iter().copied().collect::<BTreeSet<_>>() != BTreeSet::from([1, 2, 3, 4])
                {
                    return Err(
                        format!("authoritative root is not the fixture's bounded modern EC:2 single stripe: {path:?}").into(),
                    );
                }
                let version = info
                    .get_etag()
                    .filter(|value| !value.is_empty())
                    .ok_or("root metadata omitted ETag")?;
                let revision = json!({"root_version": version, "size": size, "version_id": info.version_id,
                    "mod_time": info.mod_time, "data_dir": info.data_dir, "distribution": info.erasure.distribution,
                    "block_size": info.erasure.block_size, "algorithm": info.erasure.algorithm});
                if common.as_ref().is_some_and(|previous| previous != &revision) {
                    return Err("authoritative root metadata disagrees across physical replicas".into());
                }
                common = Some(revision);
                let framed = if info.inline_data() {
                    info.data.as_ref().ok_or("inline root omitted shard bytes")?.to_vec()
                } else {
                    let part = object
                        .join(info.data_dir.ok_or("external root omitted data directory")?.to_string())
                        .join(format!("part.{}", info.parts[0].number));
                    read_bounded_file(&part)?
                };
                let checksum = info.erasure.get_checksum_info(info.parts[0].number);
                let hash_size = checksum.algorithm.size();
                let shard_size = calc_shard_size(size, info.erasure.data_blocks);
                if hash_size == 0
                    || framed.len() != hash_size + shard_size
                    || checksum.algorithm.hash_encode(&framed[hash_size..]).as_ref() != &framed[..hash_size]
                {
                    return Err(format!("authoritative root shard failed its actual bitrot framing: {path:?}").into());
                }
                if shards.insert(info.erasure.index, framed[hash_size..].to_vec()).is_some() || !nodes.insert(node) {
                    return Err("authoritative root repeated an erasure index or node".into());
                }
                sets.insert(self.drive_sets[drive]);
                locations.push(json!({"node": node, "drive": drive, "index": info.erasure.index,
                    "xl_meta_sha256": sha256_hex(&metadata), "xl_meta_hex": hex_simd::encode_to_string(&metadata, hex_simd::AsciiCase::Lower),
                    "shard_sha256": sha256_hex(&framed), "shard_frame_hex": hex_simd::encode_to_string(&framed, hex_simd::AsciiCase::Lower)}));
            }
        }
        if nodes != BTreeSet::from([0, 1, 2, 3]) || shards.len() != 4 || sets.len() != 1 {
            return Err("authoritative root lacks four agreeing replicas in one actual erasure set".into());
        }
        let mut root = common.ok_or("authoritative root metadata is absent")?;
        let size = usize::try_from(root["size"].as_u64().ok_or("root size is absent")?)?;
        let codec = ReedSolomonEncoder::new(2, 2)?;
        let mut data = None;
        let mut read_quorums = Vec::new();
        for first in 1..=4 {
            for second in first + 1..=4 {
                let mut decoded = (1..=4)
                    .map(|index| (index == first || index == second).then(|| shards[&index].clone()))
                    .collect::<Vec<_>>();
                codec.reconstruct_data(&mut decoded)?;
                let mut payload = decoded[0].as_ref().ok_or("root data shard 1 was not reconstructed")?.clone();
                payload.extend_from_slice(decoded[1].as_ref().ok_or("root data shard 2 was not reconstructed")?);
                payload.truncate(size);
                if data.as_ref().is_some_and(|previous| previous != &payload) {
                    return Err("authoritative root payload differs between actual erasure read quorums".into());
                }
                data = Some(payload);
                read_quorums.push([first, second]);
            }
        }
        let data = data.ok_or("authoritative root has no decoded read quorum")?;
        let usage: DataUsageInfo = serde_json::from_slice(&data)?;
        if !usage.is_complete_bucket_usage_snapshot() || usage.usage_snapshot_observational {
            return Err("physical authoritative root is not a complete authoritative usage snapshot".into());
        }
        root["root_sha256"] = json!(sha256_hex(&data));
        root["payload_hex"] = json!(hex_simd::encode_to_string(&data, hex_simd::AsciiCase::Lower));
        root["usage"] = serde_json::to_value(usage)?;
        root["shards"] = json!(locations);
        root["read_quorums"] = json!(read_quorums);
        Ok(root)
    }

    pub fn usage_matches(&self, usage: &DataUsageInfo, buckets: &[&str]) -> bool {
        buckets.iter().all(|bucket| {
            let objects = self
                .objects
                .iter()
                .filter(|receipt| receipt.bucket == *bucket)
                .collect::<Vec<_>>();
            usage.buckets_usage.get(*bucket).is_some_and(|actual| {
                actual.objects_count == objects.iter().map(|receipt| &receipt.key).collect::<BTreeSet<_>>().len() as u64
                    && actual.versions_count == objects.len() as u64
                    && actual.delete_markers_count == 0
                    && actual.size == objects.iter().map(|receipt| receipt.length).sum::<u64>()
            })
        })
    }

    pub async fn quiet_baseline(&self) -> TestResult<DataUsageInfo> {
        let names = self.buckets.iter().map(String::as_str).collect::<Vec<_>>();
        wait_until(
            CYCLE_TIMEOUT,
            || async {
                let Ok(leader) = self.leader().await else { return Ok(false) };
                let usage: DataUsageInfo =
                    serde_json::from_value(self.admin_json(leader, "/rustfs/admin/v3/datausageinfo").await?)?;
                Ok(usage.is_complete_bucket_usage_snapshot()
                    && usage.usage_snapshot_converged == Some(true)
                    && usage.usage_snapshot_pending_buckets.is_empty()
                    && usage.scanner_epoch.is_some()
                    && usage.scanner_cycle.is_some()
                    && self.usage_matches(&usage, &names))
            },
            "quiet authoritative peer-bucket baseline",
        )
        .await?;
        let leader = self.leader().await?;
        Ok(serde_json::from_value(self.admin_json(leader, "/rustfs/admin/v3/datausageinfo").await?)?)
    }

    pub fn write_evidence(&self, name: &str, evidence: &Value) -> TestResult {
        std::fs::write(self.artifacts.join(format!("{name}.json")), serde_json::to_vec_pretty(evidence)?)?;
        Ok(())
    }
}

#[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
fn mixed_executable_hash(path: &Path) -> TestResult<(u64, String)> {
    use sha2::{Digest, Sha256};
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let before = file.metadata()?;
    if !before.is_file() || before.len() == 0 {
        return Err("mixed-version executable is not a nonempty regular file".into());
    }
    let mut digest = Sha256::new();
    let mut count = 0_u64;
    let mut buffer = [0; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        count = count.checked_add(u64::try_from(read)?).ok_or("executable size overflow")?;
        digest.update(&buffer[..read]);
    }
    if count != before.len() || file.metadata()?.len() != before.len() {
        return Err("mixed-version executable changed while being hashed".into());
    }
    Ok((count, hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower)))
}

#[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
fn proc_start_time(pid: u32) -> TestResult<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (prefix, _) = stat.split_once('(').ok_or("process stat omitted its command")?;
    if prefix.trim().parse::<u32>()? != pid {
        return Err("process stat returned another PID".into());
    }
    let (_, tail) = stat.rsplit_once(')').ok_or("process stat has an unterminated command")?;
    let fields = tail.split_ascii_whitespace().collect::<Vec<_>>();
    if matches!(fields.first().copied(), None | Some("Z" | "X" | "x")) {
        return Err("measured process is not live".into());
    }
    fields
        .get(19)
        .ok_or("process stat omitted start time")?
        .parse::<u64>()
        .map_err(Into::into)
}

fn read_bounded_file(path: &Path) -> TestResult<Vec<u8>> {
    use std::io::Read;

    let file = std::fs::File::open(path)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() > OBSERVATION_BYTES_LIMIT {
        return Err(format!("fixture evidence file exceeds its regular-file bound: {path:?}").into());
    }
    let mut bytes = Vec::new();
    file.take(OBSERVATION_BYTES_LIMIT + 1).read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len())? > OBSERVATION_BYTES_LIMIT {
        return Err(format!("fixture evidence file grew past its byte bound: {path:?}").into());
    }
    Ok(bytes)
}

async fn write_object(
    client: &Client,
    drives: &[Vec<(String, usize)>],
    node: usize,
    bucket: &str,
    key: &str,
    length: usize,
) -> TestResult<ObjectReceipt> {
    let body = payload_for(key, length);
    let output = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    let version = output.version_id().ok_or("versioned fixture PUT returned no version ID")?;
    if Uuid::parse_str(version)?.is_nil() {
        return Err("versioned fixture PUT returned a nil version ID".into());
    }
    let mut object_sets = BTreeSet::new();
    for process in drives {
        let mut complete = 0;
        for (drive, set) in process {
            let census = census_object_version_on_disk(Path::new(drive), bucket, key, Some(version))?;
            if census.has_xl_meta {
                if !census.is_complete() || (census.data_blocks, census.parity_blocks) != (Some(2), Some(2)) {
                    return Err(format!("acknowledged PUT has an incomplete EC:2 shard on {drive}: {census:?}").into());
                }
                complete += 1;
                object_sets.insert(*set);
            }
        }
        if complete != 1 {
            return Err(format!("acknowledged PUT must have one complete shard per node, got {complete}").into());
        }
    }
    if object_sets.len() != 1 {
        return Err(format!("object shards crossed erasure sets: {object_sets:?}").into());
    }
    Ok(ObjectReceipt {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: version.to_string(),
        length: u64::try_from(length)?,
        sha256: sha256_hex(&body),
        ingress_node: node,
        set: *object_sets.first().ok_or("object has no physical set")?,
    })
}

pub(super) struct HotWriter {
    stop: tokio::sync::watch::Sender<bool>,
    completed: tokio::sync::watch::Receiver<usize>,
    task: Option<tokio::task::JoinHandle<TestResult<Vec<ObjectReceipt>>>>,
}

impl HotWriter {
    pub fn completed(&self) -> usize {
        *self.completed.borrow()
    }

    pub async fn wait_for_completed(&mut self, minimum: usize) -> TestResult {
        tokio::time::timeout(Duration::from_secs(30), async {
            while self.completed() < minimum {
                self.completed
                    .changed()
                    .await
                    .map_err(|_| "hot writer stopped before the required S3 receipt")?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;
        Ok(())
    }

    pub async fn finish(mut self) -> TestResult<Vec<ObjectReceipt>> {
        let _ = self.stop.send(true);
        self.task.take().ok_or("hot writer already joined")?.await?
    }
}

impl Drop for HotWriter {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for PeerBucketRun {
    fn drop(&mut self) {
        self.dist.cluster.stop();
        if !self.keep_artifacts {
            let _ = std::fs::remove_dir_all(&self.artifacts);
        }
    }
}

pub(super) struct ScannerBarrier {
    directory: PathBuf,
    nonce: String,
}

impl ScannerBarrier {
    pub fn start_session(&self, buckets: &[String]) -> TestResult {
        self.write_json("session.json", &json!({"nonce": self.nonce, "buckets": buckets}))
    }

    pub fn stop_session(&self) -> TestResult {
        self.disarm()?;
        std::fs::remove_file(self.directory.join("session.json"))?;
        Ok(())
    }

    pub fn arm(&self, stage: &str, pid: u32, identity: Option<(u64, u64)>) -> TestResult {
        let release = self.directory.join(format!("release.{}.{stage}.{pid}", self.nonce));
        if release.exists() {
            std::fs::remove_file(release)?;
        }
        let mut arm = json!({"nonce": self.nonce, "stage": stage, "pid": pid});
        if let Some((epoch, cycle)) = identity {
            arm["epoch"] = json!(epoch);
            arm["cycle"] = json!(cycle);
        }
        self.write_json("arm.json", &arm)
    }

    pub fn release(&self, stage: &str, pid: u32) -> TestResult {
        std::fs::write(self.directory.join(format!("release.{}.{stage}.{pid}", self.nonce)), b"release")?;
        Ok(())
    }

    pub fn arm_receiver(&self, stage: &str, pid: u32, peer: &Value) -> TestResult {
        self.arm(stage, pid, None)?;
        self.write_json(
            "arm.json",
            &json!({"nonce": self.nonce, "stage": stage, "pid": pid,
            "owner_id": peer["owner_id"], "instance_id": peer["instance_id"]}),
        )
    }

    pub fn disarm(&self) -> TestResult {
        match std::fs::remove_file(self.directory.join("arm.json")) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn write_json(&self, name: &str, value: &Value) -> TestResult {
        let temporary = self.directory.join(format!("{name}.tmp"));
        std::fs::write(&temporary, serde_json::to_vec(value)?)?;
        std::fs::rename(temporary, self.directory.join(name))?;
        Ok(())
    }

    pub fn observations(&self, reached: bool) -> TestResult<Vec<Value>> {
        let prefix = if reached { "reached." } else { "observed." };
        let mut observations = Vec::new();
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with(&format!("{prefix}{}.", self.nonce)) || !name.ends_with(".json") {
                continue;
            }
            if !entry.file_type()?.is_file() || entry.metadata()?.len() > OBSERVATION_BYTES_LIMIT {
                return Err("scanner observation is not a bounded regular file".into());
            }
            let value: Value = serde_json::from_slice(&std::fs::read(entry.path())?)?;
            if value["nonce"].as_str() != Some(self.nonce.as_str()) {
                return Err("scanner observation nonce differs from the fixture session".into());
            }
            observations.push(value);
            if observations.len() > OBSERVATION_LIMIT {
                return Err("scanner observation count exceeded the bounded fixture limit".into());
            }
        }
        observations.sort_by_key(|value| (value["pid"].as_u64(), value["sequence"].as_u64()));
        Ok(observations)
    }

    pub async fn wait_stage(&self, stage: &str, pid: u32, identity: Option<(u64, u64)>, reached: bool) -> TestResult<Value> {
        self.wait_stage_at_or_after(stage, pid, identity, reached, 0).await
    }

    pub async fn wait_stage_after(
        &self,
        stage: &str,
        pid: u32,
        identity: Option<(u64, u64)>,
        reached: bool,
        after_sequence: u64,
    ) -> TestResult<Value> {
        self.wait_stage_at_or_after(stage, pid, identity, reached, after_sequence.saturating_add(1))
            .await
    }

    async fn wait_stage_at_or_after(
        &self,
        stage: &str,
        pid: u32,
        identity: Option<(u64, u64)>,
        reached: bool,
        minimum_sequence: u64,
    ) -> TestResult<Value> {
        let select = || -> TestResult<Option<Value>> {
            Ok(self.observations(reached)?.into_iter().find(|value| {
                value["stage"].as_str() == Some(stage)
                    && value["pid"].as_u64() == Some(u64::from(pid))
                    && (!reached || value["armed"].as_bool() == Some(true))
                    && value["sequence"]
                        .as_u64()
                        .is_some_and(|sequence| sequence >= minimum_sequence)
                    && identity.is_none_or(|(epoch, cycle)| {
                        value["detail"]["epoch"].as_u64() == Some(epoch) && value["detail"]["cycle"].as_u64() == Some(cycle)
                    })
            }))
        };
        wait_until(
            CYCLE_TIMEOUT,
            || async { Ok(select()?.is_some()) },
            &format!("scanner stage {stage} on pid {pid}"),
        )
        .await?;
        select()?.ok_or_else(|| format!("scanner stage {stage} disappeared after observation").into())
    }
}
