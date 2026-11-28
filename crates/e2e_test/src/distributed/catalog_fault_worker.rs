// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Private worker transport and evidence for real four-process Catalog tests.

mod owned_linux_block_devices;

use crate::common::RustFSTestClusterEnvironment;
use crate::rpc_fault_proxy::RpcFaultProxy;
use owned_linux_block_devices::OwnedLinuxBlockDevices;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io::{self, Read};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone, Copy)]
pub(crate) enum RecoveryStorage {
    Directory,
    OwnedLoopFiles,
}

pub(crate) async fn recovery_namespace_child(test_name: &str) -> TestResult<bool> {
    owned_linux_block_devices::namespace_child(test_name).await
}

#[derive(Debug)]
struct CaseFailures {
    original: Box<dyn std::error::Error + Send + Sync>,
    additional: Box<dyn std::error::Error + Send + Sync>,
}
impl std::fmt::Display for CaseFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; additional failure: {}", self.original, self.additional)
    }
}
impl std::error::Error for CaseFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.original.as_ref())
    }
}
fn combine_results(original: TestResult, additional: TestResult) -> TestResult {
    match (original, additional) {
        (Err(original), Err(additional)) => Err(Box::new(CaseFailures { original, additional })),
        (Err(error), _) | (_, Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

pub(crate) const WORKER_TEST: &str = "startup_entrypoint::tests::catalog_worker_native_lifecycle";
const MAX_RESPONSE: usize = 4 * 1024 * 1024;
const MAX_INVENTORY_BYTES: u64 = 512 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(125);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SourceIdentity {
    head: String,
    sha256: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkerReceipt {
    version: u32,
    workspace_root: PathBuf,
    source: SourceIdentity,
    worker_path: PathBuf,
    worker_sha256: String,
    worker_test: String,
    launcher_path: PathBuf,
    launcher_sha256: String,
    build_manifest_path: PathBuf,
    build_manifest_sha256: String,
    profile: String,
    features: Vec<String>,
    run_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BuildManifest {
    version: u32,
    kind: String,
    workspace_root: PathBuf,
    source: SourceIdentity,
    worker_path: PathBuf,
    worker_sha256: String,
    worker_test: String,
    requested_features: Vec<String>,
    features: Vec<String>,
    profile: String,
    rustc: String,
}

fn check(condition: bool, message: impl Into<String>) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message.into()))
    }
}

pub(crate) fn field<'a>(value: &'a Value, key: &str) -> TestResult<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::other(format!("missing string field {key}: {value}")).into())
}

fn hash_file(path: &Path) -> io::Result<String> {
    let mut input = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut bytes = [0_u8; 64 * 1024];
    loop {
        let n = input.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        digest.update(&bytes[..n]);
    }
    Ok(hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower))
}

pub(crate) fn sha256(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower)
}

fn private_path(path: &Path, directory: bool) -> io::Result<()> {
    let meta = std::fs::symlink_metadata(path)?;
    check(!meta.file_type().is_symlink(), "private path is a symlink")?;
    check(if directory { meta.is_dir() } else { meta.is_file() }, "private path has wrong type")?;
    check(
        meta.permissions().mode() & 0o777 == if directory { 0o700 } else { 0o600 },
        "private path permissions must be 0700/0600",
    )
}

impl WorkerReceipt {
    fn load() -> TestResult<Self> {
        let path =
            PathBuf::from(std::env::var_os("RUSTFS_CATALOG_WORKER_RECEIPT").ok_or("RUSTFS_CATALOG_WORKER_RECEIPT is required")?);
        private_path(&path, false)?;
        private_path(path.parent().ok_or("receipt has no parent")?, true)?;
        let receipt: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        receipt.verify()?;
        let launcher = PathBuf::from(
            std::env::var_os("RUSTFS_CATALOG_WORKER_LAUNCHER").ok_or("RUSTFS_CATALOG_WORKER_LAUNCHER is required")?,
        );
        check(
            launcher.canonicalize()? == receipt.launcher_path,
            "launcher environment differs from receipt",
        )?;
        Ok(receipt)
    }

    async fn verify_async(&self) -> TestResult {
        let receipt = self.clone();
        tokio::task::spawn_blocking(move || receipt.verify()).await?
    }

    fn verify(&self) -> TestResult {
        check(
            self.version == 1 && self.worker_test == WORKER_TEST,
            "wrong receipt version or worker test",
        )?;
        check(matches!(self.profile.as_str(), "debug" | "release"), "unsupported worker profile")?;
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize()?;
        check(self.workspace_root == workspace, "receipt workspace mismatch")?;
        let head = std::process::Command::new("git")
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(&workspace)
            .output()?;
        check(head.status.success(), "cannot determine workspace HEAD")?;
        check(String::from_utf8(head.stdout)?.trim() == self.source.head, "receipt HEAD mismatch")?;
        for (path, digest) in [
            (&self.worker_path, &self.worker_sha256),
            (&self.launcher_path, &self.launcher_sha256),
            (&self.build_manifest_path, &self.build_manifest_sha256),
        ] {
            check(
                path.is_absolute() && path.canonicalize()? == *path,
                "receipt paths must be canonical absolute paths",
            )?;
            check(hash_file(path)? == *digest, format!("artifact digest mismatch: {}", path.display()))?;
        }
        check(
            std::fs::metadata(&self.launcher_path)?.permissions().mode() & 0o111 != 0,
            "launcher is not executable",
        )?;
        let manifest: BuildManifest = serde_json::from_slice(&std::fs::read(&self.build_manifest_path)?)?;
        check(
            manifest.version == 1 && manifest.kind == "rustfs-catalog-libtest",
            "invalid worker build manifest",
        )?;
        check(
            manifest.workspace_root == self.workspace_root
                && manifest.source == self.source
                && manifest.worker_path == self.worker_path
                && manifest.worker_sha256 == self.worker_sha256
                && manifest.worker_test == self.worker_test
                && manifest.profile == self.profile
                && manifest.features == self.features,
            "receipt and build manifest differ",
        )?;
        check(
            !manifest.rustc.is_empty() && manifest.requested_features.iter().all(|f| manifest.features.contains(f)),
            "invalid build feature/toolchain evidence",
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NodeIdentity {
    pub node_id: String,
    pub boot_id: String,
    pub pid: u32,
}

/// Keep secrets in memory; evidence contains only explicitly selected public settings.
#[derive(PartialEq, Eq)]
struct DeploymentConfiguration {
    common: BTreeMap<OsString, OsString>,
}

impl DeploymentConfiguration {
    fn capture(cluster: &RustFSTestClusterEnvironment, ipc_root: &Path, nonces: &[String]) -> TestResult<Self> {
        check(
            cluster.nodes.len() == nonces.len() && cluster.node_extra_env.len() == nonces.len(),
            "node configuration identity count differs",
        )?;
        // Match the inherited environment and explicit defaults used by spawn_node.
        let mut defaults = std::env::vars_os().collect::<BTreeMap<_, _>>();
        for (key, value) in [
            ("RUSTFS_VOLUMES", cluster.rustfs_volumes_arg()),
            ("RUSTFS_ACCESS_KEY", cluster.access_key.clone()),
            ("RUSTFS_SECRET_KEY", cluster.secret_key.clone()),
            ("RUSTFS_CONSOLE_ENABLE", "false".into()),
            ("RUST_LOG", "rustfs=info,rustfs_notify=debug".into()),
        ] {
            defaults.insert(key.into(), value.into());
        }
        let mut common = defaults.clone();
        for (key, value) in &cluster.extra_env {
            common.insert(key.into(), value.into());
        }
        for (node, nonce) in nonces.iter().enumerate() {
            let local = BTreeMap::from([
                (OsString::from("RUSTFS_ADDRESS"), OsString::from(&cluster.nodes[node].address)),
                (
                    OsString::from("RUSTFS_CATALOG_WORKER_SOCKET"),
                    ipc_root.join(format!("{node}.sock")).into_os_string(),
                ),
                (OsString::from("RUSTFS_CATALOG_WORKER_NODE_ID"), OsString::from(format!("node-{node}"))),
                (OsString::from("RUSTFS_CATALOG_WORKER_NONCE"), OsString::from(nonce)),
            ]);
            let mut actual = defaults.clone();
            actual.insert("RUSTFS_ADDRESS".into(), cluster.nodes[node].address.clone().into());
            for (key, value) in cluster.extra_env.iter().chain(&cluster.node_extra_env[node]) {
                actual.insert(key.into(), value.into());
            }
            Self::verify_node(&common, actual, &local)?;
        }
        Ok(Self { common })
    }

    fn verify_node(
        common: &BTreeMap<OsString, OsString>,
        mut actual: BTreeMap<OsString, OsString>,
        local: &BTreeMap<OsString, OsString>,
    ) -> TestResult {
        for (key, expected) in local {
            check(
                actual.get(key) == Some(expected),
                format!("node-local configuration differs: {}", key.to_string_lossy()),
            )?;
            if let Some(value) = common.get(key) {
                actual.insert(key.clone(), value.clone());
            } else {
                actual.remove(key);
            }
        }
        check(actual == *common, "node compatibility settings or build receipt differ")?;
        Ok(())
    }

    fn verify_unchanged(&self, current: &Self) -> TestResult {
        check(self == current, "deployment configuration changed before node restart")?;
        Ok(())
    }

    fn evidence(&self) -> TestResult<Value> {
        let mut settings = BTreeMap::new();
        for key in [
            "RUSTFS_VOLUMES",
            "RUSTFS_REGION",
            "RUSTFS_CONSOLE_ENABLE",
            "RUSTFS_STORAGE_CLASS_STANDARD",
            "RUSTFS_TABLE_CATALOG_BACKING",
            "TOKIO_WORKER_THREADS",
            "RAYON_NUM_THREADS",
        ] {
            let value = self.common.get(OsStr::new(key));
            settings.insert(
                key,
                value
                    .map(|v| v.to_str().ok_or("non-UTF8 public deployment setting"))
                    .transpose()?,
            );
        }
        Ok(json!({"version":1,"non_sensitive_settings":settings,
            "non_sensitive_settings_sha256":sha256(&serde_json::to_vec(&settings)?),
            "all_other_environment_compared_in_memory":true}))
    }
}

fn validate_response(response: &Value, request_id: &str, identity: &NodeIdentity) -> TestResult {
    check(
        response["version"] == 1 && response["request_id"] == request_id,
        "worker response version/request mismatch",
    )?;
    check(
        response["node_id"] == identity.node_id && response["boot_id"] == identity.boot_id && response["pid"] == identity.pid,
        "worker response process identity changed",
    )?;
    check(
        matches!(response["status"].as_str(), Some("ok" | "error")),
        "invalid worker response status",
    )?;
    Ok(())
}

pub(crate) struct Evidence {
    pub root: PathBuf,
    sequence: AtomicU64,
    started: Instant,
}

impl Evidence {
    fn new(root: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
        Ok(Self {
            root,
            sequence: AtomicU64::new(0),
            started: Instant::now(),
        })
    }

    pub(crate) fn record(&self, label: &str, value: &Value) -> TestResult {
        check(
            label.bytes().all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c)),
            "unsafe evidence label",
        )?;
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let path = self.root.join(format!("{sequence:06}-{label}.json"));
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        let value = json!({"sequence":sequence,"elapsed_ms":self.started.elapsed().as_millis(),"label":label,"value":value});
        serde_json::to_writer_pretty(&mut output, &value)?;
        std::io::Write::write_all(&mut output, b"\n")?;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct WorkerClient {
    path: PathBuf,
    nonce: String,
    pub identity: NodeIdentity,
    evidence: Arc<Evidence>,
}

pub(crate) struct PendingWorkerResponse {
    stream: UnixStream,
    pub request_id: String,
    action: Value,
    identity: NodeIdentity,
    evidence: Arc<Evidence>,
}

impl PendingWorkerResponse {
    pub(crate) async fn discard(mut self) -> TestResult {
        self.stream.shutdown().await?;
        drop(self.stream);
        self.evidence.record(
            "response-discarded",
            &json!({"node":self.identity,"request_id":self.request_id,"action":self.action}),
        )?;
        Ok(())
    }
}

impl WorkerClient {
    async fn exchange(&self, request_id: &str, action: Value) -> TestResult<Value> {
        let request = json!({"version":1,"nonce":self.nonce,"request_id":request_id,"timeout_ms":120000,"action":action});
        let future = async {
            let mut stream = UnixStream::connect(&self.path).await?;
            let mut bytes = serde_json::to_vec(&request)?;
            bytes.push(b'\n');
            stream.write_all(&bytes).await?;
            let mut reader = BufReader::new(stream);
            let mut response = Vec::new();
            loop {
                let available = reader.fill_buf().await?;
                check(!available.is_empty(), "worker closed before response newline")?;
                let count = available.iter().position(|b| *b == b'\n').map_or(available.len(), |n| n + 1);
                check(response.len() + count <= MAX_RESPONSE, "worker response exceeds evidence budget")?;
                response.extend_from_slice(&available[..count]);
                reader.consume(count);
                if response.last() == Some(&b'\n') {
                    break;
                }
            }
            Ok::<Value, Box<dyn std::error::Error + Send + Sync>>(serde_json::from_slice(&response)?)
        };
        let result = tokio::time::timeout(REQUEST_TIMEOUT, future)
            .await
            .map_err(io::Error::other)
            .and_then(|value| value.map_err(io::Error::other));
        match &result {
            Ok(response) => self.evidence.record(
                "worker-response",
                &json!({"node":self.identity.node_id,"request_id":request_id,"action":action,"response":response}),
            )?,
            Err(error) => self.evidence.record(
                "worker-transport-error",
                &json!({"node":self.identity.node_id,"request_id":request_id,"action":action,"error":error.to_string()}),
            )?,
        }
        Ok(result?)
    }

    pub(crate) async fn response(&self, action: Value) -> TestResult<Value> {
        let request_id = Uuid::new_v4().to_string();
        let response = self.exchange(&request_id, action).await?;
        validate_response(&response, &request_id, &self.identity)?;
        Ok(response)
    }

    pub(crate) async fn call(&self, action: Value) -> TestResult<Value> {
        let response = self.response(action).await?;
        check(response["status"] == "ok", format!("worker operation failed: {}", response["error"]))?;
        Ok(response["result"].clone())
    }

    /// Keep the response channel open until the caller proves the selected server boundary.
    pub(crate) async fn send_pending_response(&self, action: Value) -> TestResult<PendingWorkerResponse> {
        let request_id = Uuid::new_v4().to_string();
        let mut stream = UnixStream::connect(&self.path).await?;
        let mut bytes = serde_json::to_vec(
            &json!({"version":1,"nonce":self.nonce,"request_id":request_id,"timeout_ms":120000,"action":action}),
        )?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        stream.flush().await?;
        self.evidence.record(
            "request-sent-awaiting-response",
            &json!({"node":self.identity,"request_id":request_id,"action":action}),
        )?;
        Ok(PendingWorkerResponse {
            stream,
            request_id,
            action,
            identity: self.identity.clone(),
            evidence: self.evidence.clone(),
        })
    }
}

pub(crate) fn verify_manifest(value: &Value) -> TestResult<Vec<u8>> {
    let bytes = base64_simd::STANDARD.decode_to_vec(field(value, "bytes_base64")?)?;
    check(value["length"].as_u64() == Some(u64::try_from(bytes.len())?), "manifest length mismatch")?;
    check(field(value, "sha256")? == sha256(&bytes), "manifest digest mismatch")?;
    check(
        !field(value, "bucket")?.is_empty() && !field(value, "key")?.is_empty(),
        "empty manifest location",
    )?;
    Ok(bytes)
}

pub(crate) fn inventory(root: &Path) -> TestResult<Value> {
    let root = root.canonicalize()?;
    let mut entries = Vec::new();
    let mut size = 0_u64;
    for entry in walkdir::WalkDir::new(&root).follow_links(false).max_depth(64) {
        let entry = entry?;
        check(!entry.file_type().is_symlink(), "disk inventory encountered a symlink")?;
        if entry.file_type().is_file() {
            check(entries.len() < 10000, "disk inventory file budget exceeded")?;
            let metadata = entry.metadata()?;
            size = size.checked_add(metadata.len()).ok_or("disk inventory size overflow")?;
            check(size <= MAX_INVENTORY_BYTES, "disk inventory byte budget exceeded")?;
            entries.push(json!({"path":entry.path().strip_prefix(&root)?,"length":metadata.len(),"sha256":hash_file(entry.path())?,"device":metadata.dev(),"inode":metadata.ino()}));
        }
    }
    entries.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    Ok(json!({"root":root,"bytes":size,"files":entries}))
}

pub(crate) struct Session {
    pub identity: Value,
    pub ids: Vec<String>,
    pub initial: Value,
}

pub(crate) struct CatalogCluster {
    pub cluster: RustFSTestClusterEnvironment,
    pub clients: Vec<WorkerClient>,
    pub proxies: Vec<RpcFaultProxy>,
    pub evidence: Arc<Evidence>,
    receipt: WorkerReceipt,
    deployment_configuration: DeploymentConfiguration,
    ipc_root: PathBuf,
    nonces: Vec<String>,
    finished: bool,
    session_identities: Mutex<Vec<Value>>,
    recovery_storage: Option<OwnedLinuxBlockDevices>,
}

impl CatalogCluster {
    pub(crate) async fn start(label: &str) -> TestResult<Self> {
        Self::start_with_storage(label, RecoveryStorage::Directory).await
    }

    pub(crate) async fn start_with_storage(label: &str, storage: RecoveryStorage) -> TestResult<Self> {
        let receipt = tokio::task::spawn_blocking(WorkerReceipt::load).await??;
        check(label.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'), "unsafe case label")?;
        let artifact_root = std::env::var_os("RUSTFS_CATALOG_FAULT_ARTIFACT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| receipt.workspace_root.join("target/catalog-fault-artifacts"));
        check(artifact_root.is_absolute(), "artifact directory must be absolute")?;
        let evidence = Arc::new(Evidence::new(
            artifact_root
                .join(receipt.run_id.to_string())
                .join(format!("{label}-{}", Uuid::new_v4())),
        )?);
        evidence.record("receipt", &serde_json::to_value(&receipt)?)?;
        let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
        cluster.retain_data()?;
        cluster.temp_dir = Path::new(&cluster.temp_dir).canonicalize()?.to_string_lossy().into_owned();
        for node in &mut cluster.nodes {
            node.data_dirs = node
                .data_dirs
                .iter()
                .map(|path| std::fs::canonicalize(path).map(|path| path.to_string_lossy().into_owned()))
                .collect::<io::Result<Vec<_>>>()?;
            node.data_dir = node.data_dirs[0].clone();
        }
        std::fs::set_permissions(&cluster.temp_dir, std::fs::Permissions::from_mode(0o700))?;
        cluster.access_key = Uuid::new_v4().simple().to_string();
        cluster.secret_key = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        for (key, value) in [
            ("RUSTFS_STORAGE_CLASS_STANDARD", "EC:2"),
            ("RUSTFS_TABLE_CATALOG_BACKING", "object"),
            ("RUSTFS_CONSOLE_ENABLE", "false"),
            ("TOKIO_WORKER_THREADS", "2"),
            ("RAYON_NUM_THREADS", "2"),
            ("HTTP_PROXY", ""),
            ("HTTPS_PROXY", ""),
            ("ALL_PROXY", ""),
            ("http_proxy", ""),
            ("https_proxy", ""),
            ("all_proxy", ""),
            ("NO_PROXY", "127.0.0.1,localhost,::1"),
            ("no_proxy", "127.0.0.1,localhost,::1"),
        ] {
            cluster.set_env(key, value);
        }
        let rpc_secret = Uuid::new_v4().simple().to_string();
        cluster.set_env("RUSTFS_RPC_SECRET", rpc_secret.clone());
        let ipc_root = Path::new("/tmp").join(format!("rfc-{}", Uuid::new_v4().simple()));
        std::fs::create_dir(&ipc_root)?;
        std::fs::set_permissions(&ipc_root, std::fs::Permissions::from_mode(0o700))?;
        let mut proxies = Vec::new();
        let mut nonces = Vec::new();
        for node in 0..4 {
            let nonce = Uuid::new_v4().to_string();
            cluster.set_node_env(
                node,
                "RUSTFS_CATALOG_WORKER_SOCKET",
                ipc_root.join(format!("{node}.sock")).to_string_lossy().to_string(),
            )?;
            cluster.set_node_env(node, "RUSTFS_CATALOG_WORKER_NODE_ID", format!("node-{node}"))?;
            cluster.set_node_env(node, "RUSTFS_CATALOG_WORKER_NONCE", nonce.clone())?;
            cluster
                .set_node_capture_log_path(node, evidence.root.join(format!("node-{node}.log")).to_string_lossy().to_string())?;
            proxies.push(cluster.start_rpc_proxy_for_node(node).await?);
            nonces.push(nonce);
        }
        let mut private = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(ipc_root.join("private.json"))?;
        serde_json::to_writer(
            &mut private,
            &json!({"access_key":cluster.access_key,"secret_key":cluster.secret_key,"rpc_secret":rpc_secret,"nonces":nonces}),
        )?;
        std::io::Write::write_all(&mut private, b"\n")?;
        let deployment_configuration = DeploymentConfiguration::capture(&cluster, &ipc_root, &nonces)?;
        evidence.record("deployment-preflight", &deployment_configuration.evidence()?)?;
        evidence.record("starting", &json!({"cluster_root":cluster.temp_dir,"ipc_root":ipc_root,"nodes":cluster.nodes.iter().map(|n| json!({"address":n.address,"data_dirs":n.data_dirs})).collect::<Vec<_>>(),"physical_device_independence":false}))?;
        let recovery_storage = match storage {
            RecoveryStorage::Directory => None,
            RecoveryStorage::OwnedLoopFiles => Some(
                OwnedLinuxBlockDevices::new(
                    PathBuf::from(&cluster.temp_dir),
                    cluster.nodes.iter().map(|node| PathBuf::from(&node.data_dirs[0])).collect(),
                    evidence.clone(),
                )
                .await?,
            ),
        };
        let mut instance = Self {
            cluster,
            clients: Vec::new(),
            proxies,
            evidence,
            receipt,
            deployment_configuration,
            ipc_root,
            nonces,
            finished: false,
            session_identities: Mutex::new(Vec::new()),
            recovery_storage,
        };
        let startup = async {
            if let Some(owner) = &instance.recovery_storage {
                owner.provision().await?;
            }
            instance.cluster.start_with_binary(&instance.receipt.launcher_path).await?;
            for node in 0..4 {
                let client = instance.connect(node).await?;
                instance.clients.push(client);
            }
            instance.capture("started").await
        }
        .await;
        if let Err(error) = startup {
            let recorded = instance.evidence.record("start-failed", &json!({"error":error.to_string()}));
            let cleanup = instance.cleanup_storage().await;
            let outcome = combine_results(combine_results(Err(error), recorded), cleanup);
            instance.finished = true;
            outcome?;
        }
        Ok(instance)
    }

    async fn connect(&self, node: usize) -> TestResult<WorkerClient> {
        let pid = self.cluster.nodes[node]
            .process
            .as_ref()
            .ok_or("node has no owned process")?
            .id();
        let mut client = WorkerClient {
            path: self.ipc_root.join(format!("{node}.sock")),
            nonce: self.nonces[node].clone(),
            identity: NodeIdentity {
                node_id: format!("node-{node}"),
                boot_id: String::new(),
                pid,
            },
            evidence: self.evidence.clone(),
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        while !client.path.exists() {
            check(Instant::now() < deadline, "worker IPC readiness deadline exceeded")?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let metadata = std::fs::symlink_metadata(&client.path)?;
        check(
            metadata.file_type().is_socket() && metadata.permissions().mode() & 0o777 == 0o600,
            "worker socket type or permissions mismatch",
        )?;
        let request_id = Uuid::new_v4().to_string();
        let response = client.exchange(&request_id, json!({"op":"ping"})).await?;
        client.identity.boot_id = field(&response, "boot_id")?.to_owned();
        Uuid::parse_str(&client.identity.boot_id)?;
        validate_response(&response, &request_id, &client.identity)?;
        check(response["status"] == "ok", "worker ping failed")?;
        let result = &response["result"];
        check(
            result["store_context_matches"] == true,
            "worker uses a different store from ServerContextSlot",
        )?;
        let pools = result["instance_endpoints"]
            .as_array()
            .ok_or("missing actual endpoint topology")?;
        check(
            pools.len() == 1 && pools[0]["set_count"] == 1 && pools[0]["drives_per_set"] == 4,
            "worker is not using the four-disk EC set",
        )?;
        let endpoints = pools[0]["endpoints"].as_array().ok_or("missing actual storage endpoints")?;
        check(
            endpoints.len() == 4 && endpoints.iter().filter(|e| e["is_local"] == true).count() == 1,
            "worker has an unexpected local/remote endpoint count",
        )?;
        self.evidence.record(
            "node-deployment",
            &json!({"node":client.identity,"build":self.receipt,
                "configuration":self.deployment_configuration.evidence()?,
                "node_local":{"address":self.cluster.nodes[node].address,"data_dirs":self.cluster.nodes[node].data_dirs,
                    "socket":client.path,"nonce_matches_expected":true},
                "verification":"receipt-verified launcher execve and owned-child IPC identity"}),
        )?;
        Ok(client)
    }

    fn verify_deployment_configuration(&self) -> TestResult {
        let current = DeploymentConfiguration::capture(&self.cluster, &self.ipc_root, &self.nonces)?;
        self.deployment_configuration.verify_unchanged(&current)?;
        self.evidence.record("deployment-preflight", &current.evidence()?)
    }

    pub(crate) async fn create(&self) -> TestResult<Session> {
        let suffix = Uuid::new_v4().simple().to_string();
        let created = self.clients[0]
            .call(json!({"op":"create","public_name":format!("m2-{}", &suffix[..16]),"index_name":"index.v1","dimension":3}))
            .await?;
        let mut ids = vec![field(&created, "session_id")?.to_owned()];
        for client in self.clients.iter().skip(1) {
            let opened = client.call(json!({"op":"open","identity":created["identity"]})).await?;
            ids.push(field(&opened, "session_id")?.to_owned());
        }
        let session = Session {
            identity: created["identity"].clone(),
            ids,
            initial: created["head"].clone(),
        };
        self.evidence
            .record("session", &json!({"identity":session.identity,"initial":session.initial}))?;
        self.session_identities
            .lock()
            .expect("session evidence")
            .push(session.identity.clone());
        Ok(session)
    }

    pub(crate) async fn reopen(&self, session: &mut Session, node: usize) -> TestResult {
        let opened = self.clients[node]
            .call(json!({"op":"open","identity":session.identity}))
            .await?;
        session.ids[node] = field(&opened, "session_id")?.to_owned();
        Ok(())
    }

    pub(crate) async fn prepare(&self, session: &Session, node: usize) -> TestResult<Value> {
        self.clients[node].call(json!({"op":"prepare","session_id":session.ids[node],"commit_id":Uuid::new_v4().to_string(),"idempotency_key":Uuid::new_v4().to_string()})).await
    }

    pub(crate) async fn read(&self, session: &Session, node: usize) -> TestResult<Value> {
        let result = self.clients[node]
            .call(json!({"op":"read","session_id":session.ids[node]}))
            .await?;
        verify_manifest(&result["manifest"])?;
        Ok(result)
    }

    pub(crate) async fn agree(&self, session: &Session) -> TestResult<Value> {
        let reference = self.read(session, 0).await?;
        for node in 1..4 {
            let read = self.read(session, node).await?;
            check(
                read == reference,
                format!("node {node} reads a different authoritative Catalog/manifest: {read}"),
            )?;
        }
        let (head, _) = self.object_census(&reference["head"]["raw"])?;
        let (manifest, _) = self.object_census(&reference["manifest"])?;
        self.evidence
            .record("authoritative-object-census", &json!({"head":head,"manifest":manifest}))?;
        Ok(reference)
    }

    pub(crate) async fn restart(&mut self, node: usize) -> TestResult {
        let previous = self.clients[node].identity.clone();
        self.receipt.verify_async().await?;
        self.verify_deployment_configuration()?;
        self.stop_owned_node(node)?;
        self.evidence
            .record("node-exited", &json!({"identity":previous,"owned_child_reaped":true}))?;
        self.cluster.start_node_from_binary(node, &self.receipt.launcher_path).await?;
        let current = self.connect(node).await?;
        check(
            current.identity.pid != previous.pid && current.identity.boot_id != previous.boot_id,
            "restart did not produce a new process/boot",
        )?;
        self.evidence
            .record("node-restarted", &json!({"previous":previous,"current":current.identity}))?;
        self.clients[node] = current;
        Ok(())
    }

    pub(crate) async fn capture(&self, label: &str) -> TestResult {
        let roots = self
            .cluster
            .nodes
            .iter()
            .flat_map(|n| n.data_dirs.iter())
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        let disks =
            tokio::task::spawn_blocking(move || roots.iter().map(|p| inventory(p)).collect::<TestResult<Vec<_>>>()).await??;
        self.evidence.record(
            label,
            &json!({"nodes":self.clients.iter().map(|c| &c.identity).collect::<Vec<_>>(),"disks":disks}),
        )
    }

    pub(crate) async fn finish(mut self, result: TestResult) -> TestResult {
        let recorded = match &result {
            Err(error) => self.evidence.record("first-failure", &json!({"error":error.to_string()})),
            Ok(()) => Ok(()),
        };
        let diagnostics = self.final_checks().await;
        let cleanup = self.cleanup_storage().await;
        let result = combine_results(combine_results(combine_results(result, recorded), diagnostics), cleanup);
        let recorded = self.evidence.record("result", &json!({"status":if result.is_ok(){"passed"}else{"failed"},"error":result.as_ref().err().map(ToString::to_string),"cluster_root":self.cluster.temp_dir,"evidence":self.evidence.root}));
        self.finished = true;
        combine_results(result, recorded)
    }

    async fn final_checks(&self) -> TestResult {
        let mut diagnostics: TestResult = Ok(());
        let identity = self.session_identities.lock().expect("session evidence").last().cloned();
        if let Some(identity) = identity {
            for client in &self.clients {
                let inspection = tokio::time::timeout(Duration::from_secs(5), async {
                    let opened = client.call(json!({"op":"open","identity":identity})).await?;
                    let session_id = field(&opened, "session_id")?;
                    let read = client.response(json!({"op":"read","session_id":session_id})).await?;
                    let recovery = client.response(json!({"op":"recovery","session_id":session_id})).await?;
                    check(
                        read["status"] == "ok" && recovery["status"] == "ok",
                        "final node inspection returned a worker error",
                    )?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(json!({"read":read,"recovery":recovery}))
                })
                .await;
                self.evidence.record(
                    "final-node-inspection",
                    &match inspection {
                        Ok(Ok(value)) => json!({"node":client.identity,"evidence":value}),
                        Ok(Err(error)) => {
                            let message = error.to_string();
                            if diagnostics.is_ok() {
                                diagnostics = Err(error);
                            }
                            json!({"node":client.identity,"error":message})
                        }
                        Err(error) => {
                            let message = error.to_string();
                            if diagnostics.is_ok() {
                                diagnostics = Err(error.into());
                            }
                            json!({"node":client.identity,"error":message})
                        }
                    },
                )?;
            }
        }
        let capture = self.capture("final-physical-state").await;
        let verification = self.receipt.verify_async().await;
        combine_results(combine_results(diagnostics, capture), verification)
    }

    fn reap_owned_child(process: &mut Option<std::process::Child>, require_running: bool) -> TestResult {
        if let Some(child) = process.as_mut() {
            let exited = child.try_wait()?;
            check(
                !require_running || exited.is_none(),
                format!("owned worker exited before requested stop: {exited:?}"),
            )?;
            if exited.is_none() {
                child.kill()?;
            }
            child.wait()?;
        } else {
            check(!require_running, "requested stop has no owned worker")?;
        }
        *process = None;
        Ok(())
    }

    fn stop_owned_node(&mut self, node: usize) -> TestResult {
        if self.recovery_storage.is_none() {
            return self.cluster.stop_node(node);
        }
        Self::reap_owned_child(&mut self.cluster.nodes.get_mut(node).ok_or("invalid owned node")?.process, true)
    }

    fn stop_owned_workers(&mut self) -> TestResult {
        let mut result = Ok(());
        for node in &mut self.cluster.nodes {
            result = combine_results(result, Self::reap_owned_child(&mut node.process, false));
        }
        result
    }

    async fn cleanup_storage(&mut self) -> TestResult {
        if self.recovery_storage.is_none() {
            return Ok(());
        }
        self.stop_owned_workers()?;
        let result = self.recovery_storage.as_ref().ok_or("missing storage owner")?.cleanup().await;
        let recorded = self.evidence.record(
            "device-cleanup",
            &json!({"status":if result.is_ok(){"complete"}else{"failed"},"error":result.as_ref().err().map(ToString::to_string)}),
        );
        combine_results(result, recorded)
    }

    pub(crate) fn validate_disk(&self, node: usize) -> TestResult<PathBuf> {
        let root = Path::new(&self.cluster.temp_dir).canonicalize()?;
        let node = self.cluster.nodes.get(node).ok_or("invalid node index")?;
        check(node.process.is_none(), "stop the owned process before replacing its directory")?;
        check(node.data_dirs.len() == 1, "directory fault requires one disk per node")?;
        let path = PathBuf::from(&node.data_dirs[0]);
        check(
            path.parent() == Some(root.as_path()) && path.components().all(|c| !matches!(c, Component::ParentDir)),
            "disk path escapes this cluster",
        )?;
        check(!std::fs::symlink_metadata(&path)?.file_type().is_symlink(), "disk path is a symlink")?;
        Ok(path)
    }
}

impl Drop for CatalogCluster {
    fn drop(&mut self) {
        if !self.finished {
            let cleanup = if self.recovery_storage.is_some() {
                self.stop_owned_workers()
                    .and_then(|()| self.recovery_storage.as_ref().ok_or("missing storage owner")?.cleanup_sync())
            } else {
                Ok(())
            };
            if let Err(error) = &cleanup {
                eprintln!("unfinished D07 device cleanup failed: {error}");
            }
            if let Err(error) = self.evidence.record("unfinished", &json!({"status":"failed","reason":"case exited without a completed result","cluster_root":self.cluster.temp_dir,"cleanup_error":cleanup.as_ref().err().map(ToString::to_string)})) {
                eprintln!("unfinished Catalog evidence failed: {error}");
            }
        }
    }
}

impl CatalogCluster {
    pub(crate) fn stop_node(&mut self, node: usize) -> TestResult {
        check(
            self.cluster.nodes.get(node).and_then(|n| n.process.as_ref()).is_some(),
            "fault requires a running owned node",
        )?;
        let identity = self.clients[node].identity.clone();
        self.stop_owned_node(node)?;
        self.evidence
            .record("node-exited", &json!({"identity":identity,"owned_child_reaped":true}))
    }

    pub(crate) async fn start_stopped_node(&mut self, node: usize) -> TestResult {
        check(self.cluster.nodes[node].process.is_none(), "node must be stopped before restart")?;
        let previous = self.clients[node].identity.clone();
        self.receipt.verify_async().await?;
        self.verify_deployment_configuration()?;
        self.cluster.start_node_from_binary(node, &self.receipt.launcher_path).await?;
        let current = self.connect(node).await?;
        check(
            current.identity.pid != previous.pid && current.identity.boot_id != previous.boot_id,
            "restart reused process/boot identity",
        )?;
        self.evidence
            .record("node-restarted", &json!({"previous":previous,"current":current.identity}))?;
        self.clients[node] = current;
        Ok(())
    }

    pub(crate) async fn restart_all(&mut self, session: &mut Session) -> TestResult {
        let previous = self.clients.iter().map(|c| c.identity.clone()).collect::<Vec<_>>();
        for node in 0..4 {
            self.stop_node(node)?;
        }
        self.capture("all-nodes-stopped").await?;
        self.receipt.verify_async().await?;
        self.verify_deployment_configuration()?;
        self.cluster.start_with_binary(&self.receipt.launcher_path).await?;
        for (node, old) in previous.iter().enumerate() {
            let client = self.connect(node).await?;
            check(
                client.identity.pid != old.pid && client.identity.boot_id != old.boot_id,
                "full restart reused process identity",
            )?;
            self.evidence
                .record("node-restarted", &json!({"previous":old,"current":client.identity}))?;
            self.clients[node] = client;
            self.reopen(session, node).await?;
        }
        Ok(())
    }

    pub(crate) async fn snapshot_stopped_disk(&self, node: usize) -> TestResult<PathBuf> {
        let source = self.validate_disk(node)?;
        let target = Path::new(&self.cluster.temp_dir).join(format!("snapshot-{node}-{}", Uuid::new_v4()));
        let target_copy = target.clone();
        let copied = tokio::task::spawn_blocking(move || copy_owned_tree(&source, &target_copy)).await??;
        self.evidence.record("disk-snapshot", &copied)?;
        if let Some(owner) = &self.recovery_storage {
            owner.retain_snapshot(target.clone()).await?;
        }
        Ok(target)
    }

    pub(crate) async fn replace_stopped_disk(&self, node: usize, snapshot: Option<PathBuf>) -> TestResult {
        let original = self.validate_disk(node)?;
        if let Some(owner) = &self.recovery_storage {
            return owner.replace(node, snapshot).await;
        }
        let before = inventory(&original)?;
        let parent = original.parent().ok_or("disk has no parent")?.to_path_buf();
        let retired = parent.join(format!("retired-{node}-{}", Uuid::new_v4()));
        if let Some(ref path) = snapshot {
            check(
                path.parent() == Some(parent.as_path())
                    && path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("snapshot-")),
                "snapshot is outside this case",
            )?;
            check(!std::fs::symlink_metadata(path)?.file_type().is_symlink(), "snapshot is a symlink")?;
            inventory(path)?;
        }
        std::fs::rename(&original, &retired)?;
        if let Some(source) = snapshot {
            let target = original.clone();
            tokio::task::spawn_blocking(move || copy_owned_tree(&source, &target)).await??;
        } else {
            std::fs::create_dir(&original)?;
        }
        let after = inventory(&original)?;
        check(retired.exists() && original.exists(), "replacement did not preserve both directories")?;
        self.evidence.record(
            "directory-replaced",
            &json!({"node":node,"before":before,"retired":retired,"after":after}),
        )
    }
}

fn copy_owned_tree(source: &Path, target: &Path) -> TestResult<Value> {
    check(!target.exists(), "snapshot destination already exists")?;
    let before = inventory(source)?;
    std::fs::create_dir(target)?;
    copy_owned_contents_from_inventory(source, target, before)
}

#[cfg(target_os = "linux")]
fn copy_owned_contents(source: &Path, target: &Path) -> TestResult<Value> {
    let before = inventory(source)?;
    check(std::fs::read_dir(target)?.next().is_none(), "copy target is not empty")?;
    copy_owned_contents_from_inventory(source, target, before)
}

fn copy_owned_contents_from_inventory(source: &Path, target: &Path, before: Value) -> TestResult<Value> {
    for item in walkdir::WalkDir::new(source).follow_links(false).min_depth(1).max_depth(64) {
        let item = item?;
        check(!item.file_type().is_symlink(), "snapshot encountered a symlink")?;
        let destination = target.join(item.path().strip_prefix(source)?);
        if item.file_type().is_dir() {
            std::fs::create_dir(&destination)?;
        } else if item.file_type().is_file() {
            std::fs::copy(item.path(), &destination)?;
        } else {
            return Err(io::Error::other("unexpected special file in disk snapshot").into());
        }
    }
    let after = inventory(target)?;
    let compact = |value: &Value| -> Vec<Value> {
        value["files"]
            .as_array()
            .expect("inventory files")
            .iter()
            .map(|f| json!({"path":f["path"],"length":f["length"],"sha256":f["sha256"]}))
            .collect()
    };
    check(compact(&before) == compact(&after), "copied disk contents differ")?;
    Ok(json!({"source":before,"copy":after}))
}

impl CatalogCluster {
    pub(crate) fn object_census(&self, object: &Value) -> TestResult<(Value, bool)> {
        let bucket = field(object, "bucket")?;
        let key = field(object, "key")?;
        check(
            Path::new(bucket).components().count() == 1
                && Path::new(bucket).components().all(|c| matches!(c, Component::Normal(_)))
                && !key.is_empty()
                && Path::new(key).components().all(|c| matches!(c, Component::Normal(_))),
            "unsafe object census path",
        )?;
        let mut disks = Vec::new();
        let mut complete = true;
        for (node, process) in self.cluster.nodes.iter().enumerate() {
            let directory = Path::new(&process.data_dirs[0]);
            let census = crate::chaos::census_object_version_on_disk(directory, bucket, key, None)?;
            let meta_path = directory.join(bucket).join(key).join("xl.meta");
            let mut identity_matches = false;
            let metadata = if meta_path.is_file() {
                let bytes = std::fs::read(&meta_path)?;
                check(bytes.len() <= MAX_RESPONSE, "object metadata evidence exceeds budget")?;
                let info = rustfs_filemeta::FileMeta::load(&bytes)?.into_fileinfo(bucket, key, "", true, false, true)?;
                info.validate_for_metadata_read()?;
                identity_matches = info.get_etag().as_deref() == object["etag"].as_str()
                    && u64::try_from(info.size).ok() == object["length"].as_u64();
                json!({"sha256":sha256(&bytes),"bytes_base64":base64_simd::STANDARD.encode_to_string(&bytes),"etag":info.get_etag(),"size":info.size})
            } else {
                Value::Null
            };
            let intact = census.is_complete()
                && census.data_blocks == Some(2)
                && census.parity_blocks == Some(2)
                && (census.inline_data_fingerprint.is_some() || !census.expected_part_numbers.is_empty())
                && identity_matches;
            complete &= intact;
            disks.push(json!({"node":node,"directory":directory,"object":object,"census":census,"metadata":metadata,"matches_expected":intact}));
        }
        Ok((json!(disks), complete))
    }

    pub(crate) async fn wait_for_repaired_objects(&self, read: &Value) -> TestResult {
        let url = format!("{}/rustfs/admin/v3/heal/?forceStart=true", self.cluster.nodes[0].url);
        let response=crate::chaos::start_root_heal_when_control_ready(&url,r#"{"recursive":true,"dryRun":false,"remove":false,"recreate":true,"scanMode":2,"updateParity":false,"nolock":false}"#,&self.cluster.access_key,&self.cluster.secret_key).await?;
        self.evidence.record("root-heal-response", &json!({"response":response}))?;
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let (head, head_complete) = self.object_census(&read["head"]["raw"])?;
            let (manifest, manifest_complete) = self.object_census(&read["manifest"])?;
            if head_complete && manifest_complete {
                self.evidence
                    .record("physical-repair-complete", &json!({"head":head,"manifest":manifest}))?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                self.evidence
                    .record("physical-repair-incomplete", &json!({"head":head,"manifest":manifest}))?;
                return Err(io::Error::other(
                    "heal did not restore exact head/manifest physical metadata and shards on every directory",
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

impl CatalogCluster {
    pub(crate) fn dataset_census(&self, session: &Session) -> TestResult<Value> {
        let bucket = field(&session.identity["bucket"], "storage_bucket_id")?;
        let table_id = field(&session.identity, "table_id")?;
        Uuid::parse_str(table_id)?;
        check(
            Path::new(bucket).components().count() == 1
                && Path::new(bucket).components().all(|c| matches!(c, Component::Normal(_))),
            "unsafe dataset bucket",
        )?;
        let prefix = Path::new(bucket).join(".rustfs-table").join(table_id).join("dataset");
        let disks = self
            .cluster
            .nodes
            .iter()
            .map(|node| inventory(&Path::new(&node.data_dirs[0]).join(&prefix)))
            .collect::<TestResult<Vec<_>>>()?;
        Ok(json!(disks))
    }
}

/// Reject a witnessed regression without treating a previously old replica as a rollback.
pub(crate) fn assert_no_head_regression(before: &Value, after: &Value) -> TestResult {
    fn directories(value: &Value) -> TestResult<std::collections::BTreeMap<u64, &Value>> {
        let entries = value.as_array().ok_or("needs_review: directory census is not an array")?;
        check(entries.len() == 4, "needs_review: four directory observations are required")?;
        let mut result = std::collections::BTreeMap::new();
        let mut paths = std::collections::BTreeSet::new();
        for entry in entries {
            let node = entry["node"]
                .as_u64()
                .ok_or("needs_review: directory node identity is absent")?;
            check(
                node < 4 && result.insert(node, entry).is_none(),
                "needs_review: invalid or duplicate directory node identity",
            )?;
            let path = field(entry, "directory")?;
            check(!path.is_empty() && paths.insert(path), "needs_review: duplicate or empty directory path")?;
            check(
                entry["matches_expected"].is_boolean(),
                "needs_review: directory content verdict is absent",
            )?;
        }
        Ok(result)
    }
    let before = directories(before)?;
    let after = directories(after)?;
    let holders = before.values().filter(|entry| entry["matches_expected"] == true).count();
    check(
        holders >= 3,
        "needs_review: the acknowledged head lacks three physically confirmed copies before release",
    )?;
    for (node, previous) in before {
        let current = after
            .get(&node)
            .ok_or("needs_review: directory disappeared from the evidence")?;
        check(
            previous["directory"] == current["directory"] && previous["object"] == current["object"],
            "needs_review: directory or observed object identity changed",
        )?;
        if previous["matches_expected"] != true {
            continue;
        }
        check(
            current["census"]["has_xl_meta"].is_boolean(),
            "needs_review: post-release metadata presence is unknown",
        )?;
        check(
            current["census"]["has_xl_meta"] == true,
            format!("failed: node {node} lost its previously confirmed new head"),
        )?;
        check(current["metadata"].is_object(), "needs_review: post-release head metadata is unavailable")?;
        check(
            current["matches_expected"] == true,
            format!("failed: node {node} no longer holds its previously confirmed new head"),
        )?;
        for key in ["etag", "size"] {
            check(
                previous["metadata"].get(key).is_some() && current["metadata"].get(key).is_some(),
                "needs_review: head identity fields are absent",
            )?;
            check(
                previous["metadata"][key] == current["metadata"][key],
                format!("failed: node {node} head {key} changed after old requests resumed"),
            )?;
        }
        for key in [
            "erasure_index",
            "erasure_distribution",
            "data_blocks",
            "parity_blocks",
            "expected_part_numbers",
        ] {
            check(
                previous["census"].get(key).is_some() && current["census"].get(key).is_some(),
                "needs_review: shard layout evidence is absent",
            )?;
            check(
                previous["census"][key] == current["census"][key],
                format!("needs_review: node {node} changed its shard layout during old-request release"),
            )?;
        }
        for key in ["inline_data_fingerprint", "present_part_fingerprints"] {
            check(
                previous["census"].get(key).is_some() && current["census"].get(key).is_some(),
                "needs_review: physical content fingerprints are absent",
            )?;
            check(
                previous["census"][key] == current["census"][key],
                format!("failed: node {node} changed bytes belonging to its confirmed new head"),
            )?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum LateCommitRole {
    Superseded,
    Committed,
}

pub(crate) struct LateCommitProof<'a> {
    pub identity: &'a Value,
    pub prepared: &'a Value,
    pub current_head: &'a Value,
    pub commit: &'a Value,
    pub idempotency: &'a Value,
    pub candidate: &'a Value,
    pub recovery: &'a Value,
    pub role: LateCommitRole,
}

fn exact_json_record(object: &Value, label: &str) -> TestResult<Value> {
    check(object.is_object(), format!("failed: required {label} object is absent"))?;
    let bytes = verify_manifest(object)
        .map_err(|error| io::Error::other(format!("needs_review: invalid {label} byte evidence: {error}")))?;
    Ok(serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::other(format!("failed: stored {label} JSON is damaged: {error}")))?)
}

pub(crate) fn assert_retained_record(before: &Value, after: &Value) -> TestResult {
    check(after.is_object(), "failed: a previously observed commit/idempotency object is absent")?;
    verify_manifest(before)?;
    verify_manifest(after)?;
    for key in ["bucket", "key", "length", "sha256", "bytes_base64"] {
        check(
            before[key] == after[key],
            format!("failed: retained commit/idempotency {key} changed after release"),
        )?;
    }
    Ok(())
}

/// Recovery summaries are checked against the original typed record payload and candidate.
pub(crate) fn assert_late_commit_proof(proof: LateCommitProof<'_>) -> TestResult {
    let LateCommitProof {
        identity,
        prepared,
        current_head,
        commit,
        idempotency,
        candidate,
        recovery,
        role,
    } = proof;
    let record = exact_json_record(commit, "commit log")?;
    let indexed = exact_json_record(idempotency, "idempotency")?;
    check(record == indexed, "failed: commit log and idempotency payload differ")?;
    let paths = &prepared["paths"];
    check(
        commit["bucket"] == paths["bucket"]
            && commit["key"] == paths["commit_log"]
            && idempotency["bucket"] == paths["bucket"]
            && idempotency["key"] == paths["idempotency"],
        "needs_review: commit evidence came from another object path",
    )?;
    check(candidate.is_object(), "failed: the completed candidate is absent")?;
    verify_manifest(candidate)
        .map_err(|error| io::Error::other(format!("needs_review: invalid candidate byte evidence: {error}")))?;
    check(
        candidate == &prepared["candidate_readback"],
        "failed: completed candidate bytes or exact reference changed",
    )?;
    let (status, state) = match role {
        LateCommitRole::Superseded => ("STAGED", "SUPERSEDED"),
        LateCommitRole::Committed => ("COMMITTED", "COMMITTED"),
    };
    let base = &prepared["expected_head"];
    let metadata = &prepared["candidate"];
    check(
        prepared["replay"]["base"] == *base && prepared["replay"]["commit_id"] == prepared["commit_id"],
        "needs_review: original replay does not identify the exact prepared base and commit",
    )?;
    check(
        candidate["bucket"] == identity["bucket"]["storage_bucket_id"]
            && candidate["key"] == metadata["object_key"]
            && candidate["length"] == metadata["content_length"]
            && candidate["sha256"] == metadata["content_sha256"]
            && candidate["body_revision"] == metadata["body_revision"]
            && candidate["etag"] == metadata["etag"],
        "failed: candidate bytes do not match the exact prepared metadata reference",
    )?;
    let expected_metadata = json!({"object_key":metadata["object_key"],"content_length":metadata["content_length"],"content_sha256":metadata["content_sha256"],"body_revision":metadata["body_revision"],"etag":metadata["etag"]});
    let expected_binding = json!({"version":1,"bucket":identity["bucket"],"table_id":identity["table_id"],"expected_generation":base["generation"],"expected_metadata":base["metadata"],"expected_descriptor":base["descriptor"],"candidate_metadata":expected_metadata,"descriptor":prepared["descriptor"],"request_digest_sha256":prepared["request_digest"]});
    check(
        record["vector_commit_binding"] == expected_binding,
        "failed: commit digest, resource, base, candidate or descriptor binding changed",
    )?;
    for (key, expected) in [
        ("version", json!(2)),
        ("commit_id", prepared["commit_id"].clone()),
        ("idempotency_key", prepared["replay"]["idempotency_key"].clone()),
        ("table_id", identity["table_id"].clone()),
        ("operation", json!("native-test")),
        ("expected_version_token", base["version_token"].clone()),
        ("previous_metadata_location", base["metadata"]["object_key"].clone()),
        ("new_metadata_location", metadata["object_key"].clone()),
        ("status", json!(status)),
    ] {
        check(
            record.get(key) == Some(&expected),
            format!("failed: exact commit {key} differs from its prepared input or observed role"),
        )?;
    }
    check(
        field(&record, "new_version_token")? != field(base, "version_token")?,
        "failed: commit reused its base token",
    )?;
    check(
        prepared["replay"]["idempotency_key"]
            .as_str()
            .is_some_and(|key| !key.is_empty()),
        "needs_review: the test lacks its original idempotency binding",
    )?;
    let entries = recovery["commits"]
        .as_array()
        .ok_or("needs_review: recovery entries are absent")?;
    let matching = entries
        .iter()
        .filter(|entry| entry["commit_id"] == prepared["commit_id"])
        .collect::<Vec<_>>();
    check(!matching.is_empty(), "failed: recovery omitted a required commit_id")?;
    check(
        matching.len() == 1,
        "needs_review: duplicate recovery entries prevent an exact commit decision",
    )?;
    let entry = matching[0];
    check(
        entry["recovery_state"] == state,
        format!(
            "needs_review: expected {state} for the exact commit, received {}",
            entry["recovery_state"]
        ),
    )?;
    check(
        entry["idempotency_index_present"] == true && entry["idempotency_index_status"] == "MATCHES",
        "failed: recovery does not retain the matching idempotency payload",
    )?;
    for key in [
        "commit_id",
        "idempotency_key",
        "operation",
        "status",
        "previous_metadata_location",
        "new_metadata_location",
        "expected_version_token",
        "new_version_token",
    ] {
        check(
            entry.get(key) == record.get(key) && entry.get(key).is_some(),
            format!("failed: recovery {key} differs from the exact stored commit"),
        )?;
    }
    let head = &current_head["entry"];
    check(
        recovery["table_id"] == identity["table_id"]
            && recovery["table_bucket"] == identity["bucket"]["storage_bucket_id"]
            && recovery["namespace"] == "indexes"
            && recovery["table"] == identity["index_name"],
        "needs_review: recovery report belongs to another resource",
    )?;
    check(
        recovery["current_metadata_location"] == head["metadata_location"]
            && recovery["current_version_token"] == head["version_token"]
            && recovery["current_generation"] == head["generation"],
        "failed: recovery report disagrees with the acknowledged current head",
    )?;
    check(
        recovery["manual_review_count"] == 0,
        "needs_review: current recovery history remains uncertain",
    )?;
    match role {
        LateCommitRole::Committed => check(
            head["metadata_location"] == record["new_metadata_location"] && head["version_token"] == record["new_version_token"],
            "failed: winning commit no longer names the current head",
        )?,
        LateCommitRole::Superseded => check(
            head["metadata_location"] != record["new_metadata_location"] && head["version_token"] != record["new_version_token"],
            "failed: superseded old commit became current",
        )?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deployment_fixture() -> (DeploymentConfiguration, BTreeMap<OsString, OsString>) {
        let common = [
            ("RUSTFS_VOLUMES", "http://127.0.0.1:9000/disk"),
            ("RUSTFS_STORAGE_CLASS_STANDARD", "EC:2"),
            ("RUSTFS_TABLE_CATALOG_BACKING", "object"),
            ("RUSTFS_CATALOG_WORKER_RECEIPT", "/private/receipt.json"),
            ("RUSTFS_CATALOG_WORKER_LAUNCHER", "/private/launcher"),
            ("RUSTFS_SECRET_KEY", "private-secret"),
            ("RUSTFS_RPC_SECRET", "private-rpc-secret"),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect();
        let local = [
            ("RUSTFS_ADDRESS", "127.0.0.1:9000"),
            ("RUSTFS_CATALOG_WORKER_SOCKET", "/private/0.sock"),
            ("RUSTFS_CATALOG_WORKER_NODE_ID", "node-0"),
            ("RUSTFS_CATALOG_WORKER_NONCE", "private-nonce"),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect();
        (DeploymentConfiguration { common }, local)
    }

    #[test]
    fn deployment_accepts_exact_local_identity_with_uniform_settings() -> TestResult {
        let (configuration, local) = deployment_fixture();
        let mut actual = configuration.common.clone();
        actual.extend(local.clone());
        DeploymentConfiguration::verify_node(&configuration.common, actual, &local)?;
        configuration.verify_unchanged(&configuration)
    }

    #[test]
    fn deployment_rejects_changed_compatibility_credentials_or_build_receipt() {
        let (configuration, local) = deployment_fixture();
        for key in [
            "RUSTFS_REGION",
            "RUSTFS_STORAGE_CLASS_STANDARD",
            "RUSTFS_TABLE_CATALOG_BACKING",
            "RUSTFS_CATALOG_WORKER_RECEIPT",
            "RUSTFS_CATALOG_WORKER_LAUNCHER",
            "RUSTFS_SECRET_KEY",
            "RUSTFS_RPC_SECRET",
            "RAYON_NUM_THREADS",
            "HTTPS_PROXY",
        ] {
            let mut actual = configuration.common.clone();
            actual.extend(local.clone());
            actual.insert(key.into(), "different-private-value".into());
            let error = DeploymentConfiguration::verify_node(&configuration.common, actual, &local)
                .expect_err("node-specific common settings must be rejected");
            assert!(
                !error.to_string().contains("different-private-value"),
                "configuration values must stay private"
            );
        }
    }

    #[test]
    fn deployment_rejects_wrong_local_identity() {
        let (configuration, local) = deployment_fixture();
        for key in local.keys() {
            let mut actual = configuration.common.clone();
            actual.extend(local.clone());
            actual.insert(key.clone(), "different-private-value".into());
            let error = DeploymentConfiguration::verify_node(&configuration.common, actual, &local)
                .expect_err("local differences require the expected node identity");
            assert!(!error.to_string().contains("different-private-value"), "local values must stay private");
        }
    }

    #[test]
    fn deployment_rejects_common_changes_on_restart_without_persisting_secrets() -> TestResult {
        let (configuration, _) = deployment_fixture();
        let mut changed = DeploymentConfiguration {
            common: configuration.common.clone(),
        };
        changed.common.insert("RUSTFS_SECRET_KEY".into(), "replacement-secret".into());
        assert!(configuration.verify_unchanged(&changed).is_err());
        assert_eq!(
            configuration.evidence()?,
            changed.evidence()?,
            "secret values must not affect a public digest"
        );
        let evidence = serde_json::to_string(&configuration.evidence()?)?;
        for secret in ["private-secret", "private-rpc-secret"] {
            assert!(!evidence.contains(secret));
            assert!(!evidence.contains(&sha256(secret.as_bytes())));
        }
        changed.common.insert("RUSTFS_STORAGE_CLASS_STANDARD".into(), "EC:3".into());
        assert!(configuration.verify_unchanged(&changed).is_err());
        assert_ne!(configuration.evidence()?, changed.evidence()?);
        Ok(())
    }

    #[tokio::test]
    async fn deployment_capture_preserves_node_defaults_and_rejects_global_address_override() -> TestResult {
        let mut cluster = RustFSTestClusterEnvironment::new(2).await?;
        let directory = tempfile::tempdir()?;
        let nonces = vec!["private-a".to_owned(), "private-b".to_owned()];
        for (node, nonce) in nonces.iter().enumerate() {
            cluster.set_node_env(
                node,
                "RUSTFS_CATALOG_WORKER_SOCKET",
                directory.path().join(format!("{node}.sock")).to_string_lossy(),
            )?;
            cluster.set_node_env(node, "RUSTFS_CATALOG_WORKER_NODE_ID", format!("node-{node}"))?;
            cluster.set_node_env(node, "RUSTFS_CATALOG_WORKER_NONCE", nonce)?;
        }
        let configuration = DeploymentConfiguration::capture(&cluster, directory.path(), &nonces)?;
        let current = DeploymentConfiguration::capture(&cluster, directory.path(), &nonces)?;
        configuration.verify_unchanged(&current)?;
        cluster.set_node_env(1, "RUSTFS_STORAGE_CLASS_STANDARD", "EC:3")?;
        assert!(DeploymentConfiguration::capture(&cluster, directory.path(), &nonces).is_err());
        assert_eq!(
            cluster.node_extra_env[1].pop(),
            Some(("RUSTFS_STORAGE_CLASS_STANDARD".into(), "EC:3".into()))
        );
        cluster.set_env("RUSTFS_ADDRESS", "127.0.0.1:1");
        assert!(DeploymentConfiguration::capture(&cluster, directory.path(), &nonces).is_err());
        Ok(())
    }

    #[test]
    fn receipt_rejects_unknown_fields() {
        let mut value = receipt_example();
        value["unverified_override"] = json!(true);
        assert!(serde_json::from_value::<WorkerReceipt>(value).is_err());
    }

    #[test]
    fn receipt_rejects_missing_worker_identity() {
        let mut value = receipt_example();
        value.as_object_mut().expect("receipt object").remove("worker_sha256");
        assert!(serde_json::from_value::<WorkerReceipt>(value).is_err());
    }

    #[test]
    fn response_rejects_another_request_or_boot() {
        let identity = NodeIdentity {
            node_id: "node-0".into(),
            boot_id: "boot-a".into(),
            pid: 42,
        };
        let response = json!({"version":1,"request_id":"other","node_id":"node-0","boot_id":"boot-a","pid":42,"status":"ok","result":{},"error":null});
        assert!(validate_response(&response, "wanted", &identity).is_err());
        let mut response = response;
        response["request_id"] = json!("wanted");
        response["boot_id"] = json!("boot-b");
        assert!(validate_response(&response, "wanted", &identity).is_err());
    }

    #[test]
    fn physical_inventory_rejects_symlink_escape() {
        let dir = tempfile::tempdir().expect("private test directory");
        std::os::unix::fs::symlink("/", dir.path().join("escape")).expect("test symlink");
        assert!(inventory(dir.path()).is_err());
    }

    #[test]
    fn manifest_rejects_wrong_content_digest() {
        let value = json!({"bucket":"fixture","key":"candidate","length":3,"sha256":"wrong","bytes_base64":"YWJj"});
        assert!(verify_manifest(&value).is_err());
    }

    #[tokio::test]
    async fn retained_cluster_data_survives_drop_without_processes() -> TestResult {
        let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
        let root = PathBuf::from(&cluster.temp_dir);
        cluster.retain_data()?;
        let marker = root.join("evidence-marker");
        std::fs::write(&marker, b"retained")?;
        drop(cluster);
        assert_eq!(std::fs::read(&marker)?, b"retained");
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn default_cluster_drop_keeps_cleanup_behavior() -> TestResult {
        let cluster = RustFSTestClusterEnvironment::new(4).await?;
        let root = PathBuf::from(&cluster.temp_dir);
        drop(cluster);
        assert!(!root.exists());
        Ok(())
    }

    fn head_material(holder: bool, node: usize) -> Value {
        let content = if holder { "B" } else { "A" };
        json!({"node":node,"directory":format!("/owned/node{node}"),"object":{"bucket":".rustfs.sys","key":"head","etag":"B"},"matches_expected":holder,
            "metadata":{"etag":content,"size":100},"census":{"has_xl_meta":true,"erasure_index":node+1,"erasure_distribution":[1,2,3,4],"data_blocks":2,"parity_blocks":2,
            "expected_part_numbers":[],"inline_data_fingerprint":{"size":50,"sha256":content},"present_part_fingerprints":{}}})
    }

    #[test]
    fn late_head_rejects_one_new_replica_replaced_by_old_content() {
        let before = json!((0..4).map(|node| head_material(true, node)).collect::<Vec<_>>());
        let mut after = before.clone();
        after[2] = head_material(false, 2);
        assert!(
            assert_no_head_regression(&before, &after)
                .expect_err("one witnessed rollback must fail despite three current replicas")
                .to_string()
                .starts_with("failed:")
        );
    }

    #[test]
    fn late_head_keeps_preexisting_old_replica_distinct_from_rollback() {
        let before = json!((0..4).map(|node| head_material(node != 0, node)).collect::<Vec<_>>());
        assert_no_head_regression(&before, &before).expect("an unchanged old replica has no witnessed regression");
        let mut healed = before.clone();
        healed[0] = head_material(true, 0);
        assert_no_head_regression(&before, &healed).expect("an old replica may acquire the acknowledged content");
    }

    #[test]
    fn late_head_rejects_changed_shard_despite_matching_metadata() {
        let before = json!((0..4).map(|node| head_material(true, node)).collect::<Vec<_>>());
        let mut after = before.clone();
        after[1]["census"]["inline_data_fingerprint"]["sha256"] = json!("old-or-corrupt-bytes");
        assert!(
            assert_no_head_regression(&before, &after)
                .expect_err("physical byte changes must fail")
                .to_string()
                .starts_with("failed:")
        );
    }

    #[test]
    fn late_head_requires_complete_directory_evidence() {
        let before = json!((0..4).map(|node| head_material(true, node)).collect::<Vec<_>>());
        let mut after = before.clone();
        after.as_array_mut().expect("directory array").pop();
        assert!(
            assert_no_head_regression(&before, &after)
                .expect_err("missing directory evidence must fail closed")
                .to_string()
                .starts_with("needs_review:")
        );
    }

    struct LateProofFixture {
        identity: Value,
        prepared: Value,
        current_head: Value,
        commit: Value,
        idempotency: Value,
        candidate: Value,
        recovery: Value,
        role: LateCommitRole,
    }
    impl LateProofFixture {
        fn proof(&self) -> LateCommitProof<'_> {
            LateCommitProof {
                identity: &self.identity,
                prepared: &self.prepared,
                current_head: &self.current_head,
                commit: &self.commit,
                idempotency: &self.idempotency,
                candidate: &self.candidate,
                recovery: &self.recovery,
                role: self.role,
            }
        }
    }
    fn raw_fixture(bucket: &str, key: &str, bytes: &[u8]) -> Value {
        json!({"bucket":bucket,"key":key,"length":bytes.len(),"sha256":sha256(bytes),"bytes_base64":base64_simd::STANDARD.encode_to_string(bytes),"body_revision":"revision","etag":"etag"})
    }
    fn late_proof_fixture(role: LateCommitRole) -> LateProofFixture {
        let (id, status, state, current_key, current_token) = match role {
            LateCommitRole::Superseded => ("A", "STAGED", "SUPERSEDED", "candidate-B", "new-B"),
            LateCommitRole::Committed => ("B", "COMMITTED", "COMMITTED", "candidate-B", "new-B"),
        };
        let identity = json!({"bucket":{"version":1,"storage_bucket_id":"storage","bucket_incarnation":"incarnation","account_namespace_id":"","region":"us-east-1","public_name":"fixture"},"table_id":"table","index_name":"index.v1"});
        let candidate = raw_fixture("storage", &format!("candidate-{id}"), b"exact immutable candidate");
        let metadata = json!({"object_key":candidate["key"],"content_length":candidate["length"],"content_sha256":candidate["sha256"],"body_revision":candidate["body_revision"],"etag":candidate["etag"]});
        let descriptor = json!({"encoding_version":1,"format":"lance","format_version":1,"metadata":metadata,"payload_base64":"ZGVzY3JpcHRvcg=="});
        let base = json!({"generation":1,"version_token":"base-token","metadata":{"object_key":"base-candidate"},"descriptor":{"payload_base64":"YmFzZQ=="}});
        let prepared = json!({"commit_id":id,"replay":{"commit_id":id,"base":base,"idempotency_key":format!("idem-{id}")},"request_digest":"d".repeat(64),"expected_head":base,"candidate":metadata,"candidate_readback":candidate,"descriptor":descriptor,
            "paths":{"bucket":".rustfs.sys","commit_log":format!("commit-{id}"),"idempotency":format!("idem-{id}")}});
        let record = json!({"version":2,"commit_id":id,"idempotency_key":format!("idem-{id}"),"table_id":"table","operation":"native-test","expected_version_token":"base-token","new_version_token":format!("new-{id}"),
            "previous_metadata_location":"base-candidate","new_metadata_location":candidate["key"],"status":status,"requirements":[],"writer":null,"created_at":null,"updated_at":null,
            "vector_commit_binding":{"version":1,"bucket":identity["bucket"],"table_id":"table","expected_generation":1,"expected_metadata":base["metadata"],"expected_descriptor":base["descriptor"],"candidate_metadata":metadata,"descriptor":descriptor,"request_digest_sha256":prepared["request_digest"]}});
        let bytes = serde_json::to_vec(&record).expect("record JSON");
        let commit = raw_fixture(".rustfs.sys", &format!("commit-{id}"), &bytes);
        let idempotency = raw_fixture(".rustfs.sys", &format!("idem-{id}"), &bytes);
        let current_head =
            json!({"entry":{"table_id":"table","metadata_location":current_key,"version_token":current_token,"generation":2}});
        let recovery = json!({"table_id":"table","table_bucket":"storage","namespace":"indexes","table":"index.v1","current_metadata_location":current_key,"current_version_token":current_token,"current_generation":2,"manual_review_count":0,
            "commits":[{"commit_id":id,"idempotency_key":format!("idem-{id}"),"operation":"native-test","status":status,"recovery_state":state,"previous_metadata_location":"base-candidate","new_metadata_location":candidate["key"],"expected_version_token":"base-token","new_version_token":format!("new-{id}"),"idempotency_index_present":true,"idempotency_index_status":"MATCHES"}]});
        LateProofFixture {
            identity,
            prepared,
            current_head,
            commit,
            idempotency,
            candidate,
            recovery,
            role,
        }
    }

    #[test]
    fn late_commit_accepts_exact_superseded_and_committed_proofs() {
        for role in [LateCommitRole::Superseded, LateCommitRole::Committed] {
            let fixture = late_proof_fixture(role);
            assert_late_commit_proof(fixture.proof()).expect("complete exact history and candidate proof");
        }
    }

    #[test]
    fn late_commit_requires_one_exact_recovery_entry() {
        let mut fixture = late_proof_fixture(LateCommitRole::Superseded);
        fixture.recovery["commits"] = json!([]);
        assert!(
            assert_late_commit_proof(fixture.proof())
                .expect_err("zero ManualReview does not prove the missing A entry")
                .to_string()
                .starts_with("failed:")
        );
        let mut fixture = late_proof_fixture(LateCommitRole::Superseded);
        let duplicate = fixture.recovery["commits"][0].clone();
        fixture.recovery["commits"].as_array_mut().expect("commits").push(duplicate);
        assert!(
            assert_late_commit_proof(fixture.proof())
                .expect_err("duplicate commit evidence is ambiguous")
                .to_string()
                .starts_with("needs_review:")
        );
    }

    #[test]
    fn late_commit_rejects_missing_staged_log_or_candidate() {
        let mut fixture = late_proof_fixture(LateCommitRole::Superseded);
        fixture.commit = Value::Null;
        assert!(
            assert_late_commit_proof(fixture.proof())
                .expect_err("missing staged log must fail")
                .to_string()
                .starts_with("failed:")
        );
        let mut fixture = late_proof_fixture(LateCommitRole::Superseded);
        fixture.idempotency = Value::Null;
        assert!(
            assert_late_commit_proof(fixture.proof())
                .expect_err("missing idempotency index must fail")
                .to_string()
                .starts_with("failed:")
        );
        let mut fixture = late_proof_fixture(LateCommitRole::Superseded);
        fixture.candidate = Value::Null;
        assert!(
            assert_late_commit_proof(fixture.proof())
                .expect_err("missing completed candidate must fail")
                .to_string()
                .starts_with("failed:")
        );
    }

    #[test]
    fn late_commit_rejects_changed_digest_base_candidate_and_idempotency() {
        for pointer in [
            "/vector_commit_binding/request_digest_sha256",
            "/vector_commit_binding/expected_metadata/object_key",
            "/vector_commit_binding/candidate_metadata/object_key",
            "/idempotency_key",
        ] {
            let mut fixture = late_proof_fixture(LateCommitRole::Superseded);
            let mut record: Value =
                serde_json::from_slice(&verify_manifest(&fixture.commit).expect("raw record")).expect("record JSON");
            *record.pointer_mut(pointer).expect("existing binding field") = json!("substituted");
            let bytes = serde_json::to_vec(&record).expect("modified record JSON");
            fixture.commit = raw_fixture(".rustfs.sys", "commit-A", &bytes);
            fixture.idempotency = raw_fixture(".rustfs.sys", "idem-A", &bytes);
            assert!(
                assert_late_commit_proof(fixture.proof()).is_err(),
                "changed field {pointer} must fail even when log and index agree"
            );
        }
    }

    #[test]
    fn late_commit_rejects_unresolved_recovery_classification() {
        let mut fixture = late_proof_fixture(LateCommitRole::Superseded);
        fixture.recovery["commits"][0]["recovery_state"] = json!("STAGED_BEFORE_TABLE_UPDATE");
        assert!(
            assert_late_commit_proof(fixture.proof())
                .expect_err("staged state alone does not prove this post-B outcome")
                .to_string()
                .starts_with("needs_review:")
        );
    }

    #[test]
    fn late_record_rejects_changed_bytes_after_checkpoint() {
        let before = raw_fixture(".rustfs.sys", "commit-A", b"original record");
        let after = raw_fixture(".rustfs.sys", "commit-A", b"different record");
        assert!(
            assert_retained_record(&before, &after)
                .expect_err("retained record content changed")
                .to_string()
                .starts_with("failed:")
        );
    }

    #[tokio::test]
    async fn pending_response_keeps_connection_open_until_explicit_discard() -> TestResult {
        use tokio::io::AsyncReadExt;
        let directory = tempfile::Builder::new().prefix("rfc-ipc-").tempdir_in("/tmp")?;
        let path = directory.path().join("worker.sock");
        let listener = tokio::net::UnixListener::bind(&path)?;
        let client = WorkerClient {
            path,
            nonce: "test-nonce".into(),
            identity: NodeIdentity {
                node_id: "node-0".into(),
                boot_id: "boot".into(),
                pid: 1,
            },
            evidence: Arc::new(Evidence::new(directory.path().join("evidence"))?),
        };
        let pending = client
            .send_pending_response(json!({"op":"commit","prepared_token":"fixture"}))
            .await?;
        let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await??;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line)).await??;
        let request: Value = serde_json::from_str(&line)?;
        assert_eq!(request["request_id"], pending.request_id);
        let mut byte = [0_u8];
        assert_eq!(
            reader
                .get_mut()
                .try_read(&mut byte)
                .expect_err("response connection must remain open")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        pending.discard().await?;
        assert_eq!(tokio::time::timeout(Duration::from_secs(5), reader.read(&mut byte)).await??, 0);
        Ok(())
    }

    fn receipt_example() -> Value {
        json!({"version":1,"workspace_root":"/workspace","source":{"head":"abc","sha256":"def"},"worker_path":"/worker","worker_sha256":"abc","worker_test":WORKER_TEST,"launcher_path":"/launcher","launcher_sha256":"abc","build_manifest_path":"/manifest","build_manifest_sha256":"abc","profile":"debug","features":[],"run_id":Uuid::new_v4().to_string()})
    }
}
