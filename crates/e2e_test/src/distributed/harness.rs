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

//! Shared 4-node distributed e2e helpers.
//!
//! Two localhost-expressible layouts cover the suite:
//!
//! * **4×4 single pool** (`four_by_four`) — four processes, four drives each,
//!   one `DistErasure` pool (16 explicit volume endpoints). This is the
//!   default S3 / lock / versioning / chaos topology.
//! * **4×4 four pool** — start one durable single-node pool, then append three
//!   single-node pools one at a time. Required for decommission/rebalance/expand,
//!   which the server rejects on a single pool. Cold-starting multiple empty
//!   pools races their bootstrap identities on localhost DistErasure.
//!
//! Genuine multi-node *striped* pools still need multi-host CI (backlog
//! #1313 / #1314). Site replication uses two 4-node 1-drive clusters so the
//! process count stays at eight rather than sixteen.

use crate::common::{
    ClusterTopology, FAST_DATA_USAGE_SCANNER_ENV, RustFSTestClusterEnvironment, admin_request, build_test_s3_config,
    local_http_client, replication_fast_env, signed_request,
};
use crate::replication_extension_test::LOOPBACK_REPLICATION_TARGET_ENV;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use http::{Method, StatusCode};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) const NODE_COUNT: usize = 4;
pub(crate) const DRIVES_PER_NODE: usize = 4;
/// Retire the seed pool after test data is written there, proving that user
/// objects—not only internal metadata—move to the expansion pools.
pub(crate) const DECOMMISSION_POOL_ID: usize = 0;
const POOL_ROOTS_ENV: &str = "RUSTFS_E2E_POOL_ROOTS";
const POOL_META_V3_ENV: [(&str, &str); 2] = [
    ("RUSTFS_POOL_META_V3_WRITE", "true"),
    ("RUSTFS_POOL_META_V3_FLEET_CONFIRMED", "true"),
];

#[derive(Clone, Copy, Debug)]
pub(crate) enum DistLayout {
    /// 4 nodes × 4 drives, one erasure pool spanning every endpoint.
    FourByFour,
    /// 4 nodes × 1 drive, one erasure pool (minimum 4-node 4-disk layout).
    FourNodeFourDisk,
    /// 1 node × 4 drives, used only as the durable seed for pool expansion.
    SingleNodeFourDrive,
}

pub(crate) struct DistCluster {
    pub cluster: RustFSTestClusterEnvironment,
    pool_storage_roots: Option<Vec<PathBuf>>,
    owned_pool_dirs: Vec<PathBuf>,
}

impl DistCluster {
    pub async fn start(layout: DistLayout) -> TestResult<Self> {
        Self::start_with_env(layout, &[]).await
    }

    pub async fn start_with_env(layout: DistLayout, extra_env: &[(&str, &str)]) -> TestResult<Self> {
        let mut dist = Self::new_stopped_with_env(layout, extra_env).await?;
        dist.cluster.start().await?;
        Ok(dist)
    }

    /// Allocate ports and data dirs without spawning processes.
    ///
    /// Upgrade tests configure capture logs, then start a pinned previous
    /// binary against the same directories.
    pub async fn new_stopped(layout: DistLayout) -> TestResult<Self> {
        Self::new_stopped_with_env(layout, &[]).await
    }

    pub async fn new_stopped_with_env(layout: DistLayout, extra_env: &[(&str, &str)]) -> TestResult<Self> {
        let topology = match layout {
            DistLayout::FourByFour => ClusterTopology::single_pool_multidrive(NODE_COUNT, DRIVES_PER_NODE),
            DistLayout::FourNodeFourDisk => ClusterTopology::single_pool(NODE_COUNT),
            DistLayout::SingleNodeFourDrive => ClusterTopology::per_node_pools(DRIVES_PER_NODE, vec![vec![0]]),
        };
        let mut cluster = RustFSTestClusterEnvironment::with_topology(topology).await?;
        let pool_storage_roots = match layout {
            DistLayout::SingleNodeFourDrive => Some(configured_pool_storage_roots()?),
            DistLayout::FourByFour | DistLayout::FourNodeFourDisk => None,
        };
        let mut owned_pool_dirs = Vec::new();
        if let Some(roots) = pool_storage_roots.as_deref() {
            owned_pool_dirs.push(relocate_pool_storage(&mut cluster, 0, roots)?);
        }
        cluster.set_env("NO_PROXY", "127.0.0.1,localhost");
        cluster.set_env("HTTP_PROXY", "");
        cluster.set_env("HTTPS_PROXY", "");
        if matches!(layout, DistLayout::SingleNodeFourDrive) {
            // This fresh, same-version fleet is safe to initialize at V3. The
            // durable generation protocol is required for concurrent
            // decommission progress updates and crash-recoverable movement.
            for (key, value) in POOL_META_V3_ENV {
                cluster.set_env(key, value);
            }
        }
        for &(key, value) in extra_env {
            cluster.set_env(key, value);
        }
        configure_node_logs(&mut cluster)?;
        Ok(Self {
            cluster,
            pool_storage_roots,
            owned_pool_dirs,
        })
    }

    /// Start every node with a specific `rustfs` binary, keeping the allocated
    /// data directories. Used to seed an old on-disk format before upgrading.
    pub async fn start_from_binary(&mut self, binary: &Path) -> TestResult {
        self.cluster.start_with_binary(binary).await?;
        wait_for_ready(&self.cluster).await?;
        Ok(())
    }

    /// Stop every node and bring the same data directories up on the workspace
    /// binary (direct upgrade).
    pub async fn restart_with_current_binary(&mut self) -> TestResult {
        self.cluster.stop();
        self.cluster.start().await?;
        wait_for_ready(&self.cluster).await?;
        Ok(())
    }

    /// Planned topology changes use graceful shutdown. Crash semantics are
    /// exercised separately by the chaos cases.
    pub async fn restart_current_binary_gracefully(&mut self) -> TestResult {
        self.stop_all_gracefully().await?;
        self.cluster.start().await?;
        wait_for_ready(&self.cluster).await?;
        Ok(())
    }

    /// Replace one running node with the workspace binary (rolling upgrade).
    pub async fn replace_node_with_current_binary(&mut self, node_idx: usize) -> TestResult {
        self.cluster.stop_node(node_idx)?;
        self.cluster.start_node(node_idx).await?;
        wait_for_ready(&self.cluster).await?;
        Ok(())
    }

    pub fn client_with_credentials(&self, node_idx: usize, access_key: &str, secret_key: &str) -> TestResult<Client> {
        if node_idx >= self.cluster.nodes.len() {
            return Err("node_idx is invalid".into());
        }
        Ok(Client::from_conf(build_test_s3_config(
            &self.cluster.nodes[node_idx].url,
            access_key,
            secret_key,
            None,
            "cluster-iam-test",
        )))
    }

    pub async fn append_pool_and_restart(&mut self) -> TestResult<usize> {
        self.stop_all_gracefully().await?;
        let node_idx = self.cluster.append_single_node_pool().await?;
        let roots = self
            .pool_storage_roots
            .as_deref()
            .ok_or("pool expansion requires configured isolated pool filesystems")?;
        self.owned_pool_dirs
            .push(relocate_pool_storage(&mut self.cluster, node_idx, roots)?);
        configure_node_logs(&mut self.cluster)?;
        self.cluster.start().await?;
        wait_for_ready(&self.cluster).await?;
        Ok(node_idx)
    }

    async fn stop_all_gracefully(&mut self) -> TestResult {
        for node_idx in 0..self.cluster.nodes.len() {
            self.cluster.stop_node_gracefully(node_idx).await?;
        }
        Ok(())
    }

    /// Expand the durable seed into four single-node pools through three
    /// serialized additions. Callers can seed objects before this step so a
    /// later pool-0 decommission proves that user data crosses pool boundaries.
    pub async fn expand_to_four_pools(&mut self) -> TestResult {
        for _ in 0..3 {
            self.append_pool_and_restart().await?;
        }
        self.restart_current_binary_gracefully().await?;
        Ok(())
    }

    pub async fn start_replication_pair() -> TestResult<(Self, Self)> {
        let mut extra: Vec<(&str, &str)> = replication_fast_env();
        extra.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
        extra.extend_from_slice(FAST_DATA_USAGE_SCANNER_ENV);
        let source = Self::start_with_env(DistLayout::FourNodeFourDisk, &extra).await?;
        let target = Self::start_with_env(DistLayout::FourNodeFourDisk, &extra).await?;
        Ok((source, target))
    }

    pub fn client(&self, node_idx: usize) -> TestResult<Client> {
        self.cluster.create_s3_client(node_idx)
    }

    pub fn clients(&self) -> TestResult<Vec<Client>> {
        self.cluster.create_all_clients()
    }

    pub async fn create_bucket(&self, bucket: &str) -> TestResult {
        self.cluster.create_test_bucket(bucket).await
    }
}

impl Drop for DistCluster {
    fn drop(&mut self) {
        self.cluster.stop();
        for path in &self.owned_pool_dirs {
            if let Err(error) = std::fs::remove_dir_all(path) {
                eprintln!("failed to clean up isolated distributed E2E pool directory {}: {error}", path.display());
            }
        }
    }
}

fn configured_pool_storage_roots() -> TestResult<Vec<PathBuf>> {
    let raw = std::env::var_os(POOL_ROOTS_ENV).ok_or_else(|| {
        format!(
            "{POOL_ROOTS_ENV} must name {NODE_COUNT} isolated filesystems for pool expansion, decommission, and rebalance tests"
        )
    })?;
    validate_pool_storage_roots(&raw)
}

fn validate_pool_storage_roots(raw: &std::ffi::OsStr) -> TestResult<Vec<PathBuf>> {
    let roots: Vec<PathBuf> = std::env::split_paths(raw).collect();
    if roots.len() != NODE_COUNT {
        return Err(format!("{POOL_ROOTS_ENV} must contain exactly {NODE_COUNT} paths, got {}", roots.len()).into());
    }

    let mut canonical_roots = Vec::with_capacity(roots.len());
    for root in roots {
        if !root.is_absolute() {
            return Err(format!("{POOL_ROOTS_ENV} path must be absolute: {}", root.display()).into());
        }
        let canonical = root
            .canonicalize()
            .map_err(|error| format!("{POOL_ROOTS_ENV} path {} is unavailable: {error}", root.display()))?;
        if !canonical.is_dir() {
            return Err(format!("{POOL_ROOTS_ENV} path is not a directory: {}", canonical.display()).into());
        }
        if canonical_roots.contains(&canonical) {
            return Err(format!("{POOL_ROOTS_ENV} contains a duplicate path: {}", canonical.display()).into());
        }
        canonical_roots.push(canonical);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let devices: std::collections::BTreeSet<u64> = canonical_roots
            .iter()
            .map(|root| std::fs::metadata(root).map(|metadata| metadata.dev()))
            .collect::<Result<_, _>>()?;
        if devices.len() != canonical_roots.len() {
            return Err(format!(
                "{POOL_ROOTS_ENV} paths must be backed by distinct filesystems; found {} devices for {} paths",
                devices.len(),
                canonical_roots.len()
            )
            .into());
        }
    }

    Ok(canonical_roots)
}

fn relocate_pool_storage(cluster: &mut RustFSTestClusterEnvironment, node_idx: usize, roots: &[PathBuf]) -> TestResult<PathBuf> {
    let root = roots
        .get(node_idx)
        .ok_or_else(|| format!("no isolated pool filesystem configured for node {node_idx}"))?;
    let cluster_name = Path::new(&cluster.temp_dir)
        .file_name()
        .ok_or("cluster temp directory omitted a basename")?;
    let pool_run_root = root.join(cluster_name);
    let node_root = pool_run_root.join(format!("node{node_idx}"));
    let data_dirs: Vec<String> = (0..cluster.topology.drives_per_node)
        .map(|drive| node_root.join(format!("drive{drive}")).to_string_lossy().into_owned())
        .collect();
    for data_dir in &data_dirs {
        std::fs::create_dir_all(data_dir)?;
    }
    let node = cluster
        .nodes
        .get_mut(node_idx)
        .ok_or_else(|| format!("cannot relocate missing cluster node {node_idx}"))?;
    node.data_dir = data_dirs[0].clone();
    node.data_dirs = data_dirs;
    Ok(pool_run_root)
}

fn configure_node_logs(cluster: &mut RustFSTestClusterEnvironment) -> TestResult {
    let Some(log_dir) = std::env::var_os("RUSTFS_E2E_LOG_DIR") else {
        return Ok(());
    };
    std::fs::create_dir_all(&log_dir)?;
    let cluster_id = Uuid::new_v4().simple().to_string();
    for node_idx in 0..cluster.nodes.len() {
        let path = Path::new(&log_dir).join(format!("cluster-{cluster_id}-node-{node_idx}.log"));
        cluster.set_node_capture_log_path(node_idx, path.to_string_lossy().into_owned())?;
    }
    Ok(())
}

pub(crate) fn unique_bucket(prefix: &str) -> String {
    let id = Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &id[..12])
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn payload_for(key: &str, size: usize) -> Vec<u8> {
    let seed = key.as_bytes();
    (0..size)
        .map(|idx| seed.get(idx % seed.len()).copied().unwrap_or(0) ^ (idx as u8))
        .collect()
}

pub(crate) async fn put_object(client: &Client, bucket: &str, key: &str, body: Vec<u8>) -> TestResult {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await?;
    Ok(())
}

pub(crate) async fn get_object_bytes(client: &Client, bucket: &str, key: &str) -> TestResult<Vec<u8>> {
    let output = client.get_object().bucket(bucket).key(key).send().await?;
    Ok(output.body.collect().await?.into_bytes().to_vec())
}

pub(crate) async fn assert_object_bytes(client: &Client, bucket: &str, key: &str, expected: &[u8]) -> TestResult {
    let got = get_object_bytes(client, bucket, key).await?;
    if got.as_slice() != expected {
        return Err(format!(
            "object {bucket}/{key} bytes mismatch: expected {} bytes sha256={} got {} bytes sha256={}",
            expected.len(),
            sha256_hex(expected),
            got.len(),
            sha256_hex(&got)
        )
        .into());
    }
    Ok(())
}

pub(crate) async fn put_inventory(
    client: &Client,
    bucket: &str,
    count: usize,
    size: usize,
) -> TestResult<BTreeMap<String, Vec<u8>>> {
    let mut inventory = BTreeMap::new();
    for idx in 0..count {
        let key = format!("obj-{idx:04}");
        let body = payload_for(&key, size);
        put_object(client, bucket, &key, body.clone()).await?;
        inventory.insert(key, body);
    }
    Ok(inventory)
}

/// Retry only transport-level service availability failures while a data
/// movement operation changes the pool map. Generic InternalError responses
/// remain fatal because accepting them would hide server defects.
pub(crate) async fn put_inventory_retrying(
    client: &Client,
    bucket: &str,
    count: usize,
    size: usize,
    timeout: Duration,
) -> TestResult<BTreeMap<String, Vec<u8>>> {
    let mut inventory = BTreeMap::new();
    for idx in 0..count {
        let key = format!("obj-{idx:04}");
        let body = payload_for(&key, size);
        retrying_put(client, bucket, &key, body.clone(), timeout).await?;
        inventory.insert(key, body);
    }
    Ok(inventory)
}

pub(crate) async fn assert_inventory(client: &Client, bucket: &str, inventory: &BTreeMap<String, Vec<u8>>) -> TestResult {
    for (key, expected) in inventory {
        assert_object_bytes(client, bucket, key, expected).await?;
    }
    Ok(())
}

pub(crate) async fn enable_versioning(client: &Client, bucket: &str) -> TestResult {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

pub(crate) async fn wait_until<F, Fut>(timeout: Duration, mut probe: F, label: &str) -> TestResult
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = TestResult<bool>>,
{
    let deadline = Instant::now() + timeout;
    let mut delay = Duration::from_millis(50);
    loop {
        let last_error = match probe().await {
            Ok(true) => return Ok(()),
            Ok(false) => format!("{label} still false"),
            Err(error) => error.to_string(),
        };
        if Instant::now() >= deadline {
            return Err(format!("{label} did not become true within {timeout:?}: {last_error}").into());
        }
        sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(1));
    }
}

pub(crate) async fn cluster_admin(
    cluster: &RustFSTestClusterEnvironment,
    method: Method,
    path_and_query: &str,
    body: Option<String>,
) -> TestResult<(StatusCode, String)> {
    admin_request(
        &cluster.nodes[0].url,
        method,
        path_and_query,
        body,
        &cluster.access_key,
        &cluster.secret_key,
    )
    .await
}

pub(crate) async fn cluster_admin_ok(
    cluster: &RustFSTestClusterEnvironment,
    method: Method,
    path_and_query: &str,
    body: Option<String>,
) -> TestResult<String> {
    let (status, response) = cluster_admin(cluster, method.clone(), path_and_query, body).await?;
    if !status.is_success() {
        return Err(format!("{method} {path_and_query} failed: {status} {response}").into());
    }
    Ok(response)
}

pub(crate) async fn wait_for_ready(cluster: &RustFSTestClusterEnvironment) -> TestResult {
    let client = local_http_client();
    for node in &cluster.nodes {
        let url = format!("{}/health/ready", node.url);
        wait_until(
            Duration::from_secs(30),
            || {
                let client = client.clone();
                let url = url.clone();
                async move {
                    match client.get(&url).send().await {
                        Ok(response) if response.status().is_success() => Ok(true),
                        _ => Ok(false),
                    }
                }
            },
            &format!("node {} ready", node.address),
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn set_remote_target(
    source: &RustFSTestClusterEnvironment,
    source_bucket: &str,
    target: &RustFSTestClusterEnvironment,
    target_bucket: &str,
) -> TestResult<String> {
    let body = serde_json::json!({
        "endpoint": target.nodes[0].address,
        "credentials": {
            "accessKey": target.access_key,
            "secretKey": target.secret_key
        },
        "targetbucket": target_bucket,
        "secure": false,
        "type": "replication"
    });
    let url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}",
        source.nodes[0].url,
        urlencoding::encode(source_bucket)
    );
    let response = signed_request(
        Method::PUT,
        &url,
        &source.access_key,
        &source.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("set remote target failed: {status} {body}").into());
    }
    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

pub(crate) async fn put_bucket_replication(source: &RustFSTestClusterEnvironment, bucket: &str, target_arn: &str) -> TestResult {
    let body = format!(
        r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role></Role>
  <Rule>
    <ID>rule-1</ID>
    <Priority>1</Priority>
    <Status>Enabled</Status>
    <DeleteMarkerReplication>
      <Status>Enabled</Status>
    </DeleteMarkerReplication>
    <ExistingObjectReplication>
      <Status>Enabled</Status>
    </ExistingObjectReplication>
    <Destination>
      <Bucket>{target_arn}</Bucket>
    </Destination>
  </Rule>
</ReplicationConfiguration>"#
    );
    let url = format!("{}/{bucket}?replication", source.nodes[0].url);
    let response = signed_request(
        Method::PUT,
        &url,
        &source.access_key,
        &source.secret_key,
        Some(body.into_bytes()),
        Some("application/xml"),
    )
    .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("put bucket replication failed: {status} {body}").into());
    }
    Ok(())
}

pub(crate) async fn wait_for_replicated_bytes(
    client: &Client,
    bucket: &str,
    key: &str,
    expected: &[u8],
    timeout: Duration,
) -> TestResult {
    wait_until(
        timeout,
        || async {
            match get_object_bytes(client, bucket, key).await {
                Ok(got) if got.as_slice() == expected => Ok(true),
                Ok(_) => Ok(false),
                Err(error) => {
                    let message = error.to_string();
                    if message.contains("NoSuchKey") || message.contains("NotFound") {
                        Ok(false)
                    } else {
                        Err(error)
                    }
                }
            }
        },
        &format!("replicated object {bucket}/{key}"),
    )
    .await
}

pub(crate) async fn set_bucket_quota(cluster: &RustFSTestClusterEnvironment, bucket: &str, quota_bytes: u64) -> TestResult {
    wait_until(
        Duration::from_secs(30),
        || async {
            let (status, _) =
                cluster_admin(cluster, Method::GET, &format!("/rustfs/admin/v3/quota-stats/{bucket}"), None).await?;
            Ok(status.is_success() || status == StatusCode::NOT_FOUND)
        },
        "quota stats ready",
    )
    .await?;
    let body = serde_json::json!({ "quota": quota_bytes, "quota_type": "HARD" }).to_string();
    wait_until(
        Duration::from_secs(30),
        || async {
            let (status, response) =
                cluster_admin(cluster, Method::PUT, &format!("/rustfs/admin/v3/quota/{bucket}"), Some(body.clone())).await?;
            if status.is_success() {
                return Ok(true);
            }
            if status == StatusCode::SERVICE_UNAVAILABLE {
                return Ok(false);
            }
            Err(format!("failed to set quota for {bucket}: {status} {response}").into())
        },
        "set hard quota",
    )
    .await
}

/// Start decommission and fail closed unless the admin API acknowledges it.
pub(crate) async fn start_decommission(cluster: &RustFSTestClusterEnvironment, pool_id: usize) -> TestResult {
    let path = format!("/rustfs/admin/v3/pools/decommission?pool={pool_id}&by-id=true");
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let (status, response) = cluster_admin(cluster, Method::POST, &path, None).await?;
        if status.is_success() {
            return Ok(());
        }
        if status == StatusCode::INTERNAL_SERVER_ERROR
            && response.contains("requires a live fleet capability proof")
            && Instant::now() < deadline
        {
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        return Err(format!("POST {path} did not start decommission: {status} {response}").into());
    }
}

pub(crate) async fn decommission_status_json(cluster: &RustFSTestClusterEnvironment) -> TestResult<serde_json::Value> {
    let body = cluster_admin_ok(cluster, Method::GET, "/rustfs/admin/v3/decommission/status", None).await?;
    Ok(serde_json::from_str(&body)?)
}

fn pool_entry(status: &serde_json::Value, pool_id: usize) -> Option<&serde_json::Value> {
    if let Some(pools) = status.get("pools").and_then(serde_json::Value::as_array) {
        return pools
            .iter()
            .find(|pool| pool.get("id").and_then(serde_json::Value::as_u64) == Some(pool_id as u64));
    }
    if status.get("id").and_then(serde_json::Value::as_u64) == Some(pool_id as u64) {
        Some(status)
    } else {
        None
    }
}

fn nonzero_u64(value: Option<&serde_json::Value>) -> bool {
    value.and_then(serde_json::Value::as_u64).is_some_and(|count| count > 0)
}

fn decommission_failure(pool: &serde_json::Value) -> Option<String> {
    let info = pool.get("decommissionInfo");
    let flagged = |key: &str| info.and_then(|value| value.get(key)).and_then(serde_json::Value::as_bool) == Some(true);
    let terminal_flag = flagged("failed")
        || flagged("canceled")
        || pool
            .get("status")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|status| status.eq_ignore_ascii_case("failed") || status.eq_ignore_ascii_case("canceled"));
    let object_failures = nonzero_u64(info.and_then(|value| value.get("objectsDecommissionedFailed")));
    let byte_failures = nonzero_u64(info.and_then(|value| value.get("bytesDecommissionedFailed")));
    let unresolved = info
        .and_then(|value| value.get("unresolvedEntries"))
        .and_then(serde_json::Value::as_array)
        .is_some_and(|entries| !entries.is_empty());
    terminal_flag
        .then(|| "decommission reported failed or canceled".to_string())
        .or_else(|| object_failures.then(|| "decommission reported failed objects".to_string()))
        .or_else(|| byte_failures.then(|| "decommission reported failed bytes".to_string()))
        .or_else(|| unresolved.then(|| "decommission reported unresolved entries".to_string()))
}

pub(crate) fn decommission_active(status: &serde_json::Value, pool_id: usize) -> TestResult<bool> {
    let pool = pool_entry(status, pool_id).ok_or_else(|| format!("pool {pool_id} missing from decommission status: {status}"))?;
    if let Some(reason) = decommission_failure(pool) {
        return Err(format!("{reason}: {pool}").into());
    }
    let info = pool
        .get("decommissionInfo")
        .ok_or_else(|| format!("pool {pool_id} has no decommissionInfo: {pool}"))?;
    let status_text = pool.get("status").and_then(serde_json::Value::as_str).unwrap_or("");
    let pool_status = pool.get("poolStatus").and_then(serde_json::Value::as_str).unwrap_or("");
    let queued = info.get("queued").and_then(serde_json::Value::as_bool) == Some(true);
    Ok(queued || status_text.eq_ignore_ascii_case("running") || pool_status.eq_ignore_ascii_case("decommissioning"))
}

pub(crate) fn decommission_complete(status: &serde_json::Value, pool_id: usize) -> TestResult<bool> {
    let pool = pool_entry(status, pool_id).ok_or_else(|| format!("pool {pool_id} missing from decommission status: {status}"))?;
    if let Some(reason) = decommission_failure(pool) {
        return Err(format!("{reason}: {pool}").into());
    }
    let info = pool
        .get("decommissionInfo")
        .ok_or_else(|| format!("pool {pool_id} has no decommissionInfo: {pool}"))?;
    let complete = info.get("complete").and_then(serde_json::Value::as_bool) == Some(true);
    let status_text = pool.get("status").and_then(serde_json::Value::as_str).unwrap_or("");
    let pool_status = pool.get("poolStatus").and_then(serde_json::Value::as_str).unwrap_or("");
    let terminal = status_text.eq_ignore_ascii_case("complete") && pool_status.eq_ignore_ascii_case("decommissioned");
    let moved_data = nonzero_u64(info.get("objectsDecommissioned")) || nonzero_u64(info.get("bytesDecommissioned"));
    Ok(complete && terminal && moved_data)
}

pub(crate) async fn wait_for_decommission_active(
    cluster: &RustFSTestClusterEnvironment,
    pool_id: usize,
    timeout: Duration,
) -> TestResult {
    wait_for_decommission_state(cluster, pool_id, timeout, "active", decommission_active).await
}

pub(crate) async fn wait_for_decommission_complete(
    cluster: &RustFSTestClusterEnvironment,
    pool_id: usize,
    timeout: Duration,
) -> TestResult {
    wait_for_decommission_state(cluster, pool_id, timeout, "complete with non-zero progress", decommission_complete).await
}

async fn wait_for_decommission_state(
    cluster: &RustFSTestClusterEnvironment,
    pool_id: usize,
    timeout: Duration,
    expected: &str,
    predicate: fn(&serde_json::Value, usize) -> TestResult<bool>,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let status = decommission_status_json(cluster).await?;
        if predicate(&status, pool_id)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("decommission did not become {expected} within {timeout:?}; last status: {status}").into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

pub(crate) async fn start_rebalance(cluster: &RustFSTestClusterEnvironment) -> TestResult<String> {
    let path = "/rustfs/admin/v3/rebalance/start";
    let deadline = Instant::now() + Duration::from_secs(45);
    let response = loop {
        let (status, response) = cluster_admin(cluster, Method::POST, path, None).await?;
        if status.is_success() {
            break response;
        }
        if status == StatusCode::INTERNAL_SERVER_ERROR
            && response.contains("requires a live fleet capability proof")
            && Instant::now() < deadline
        {
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        return Err(format!("POST {path} did not start rebalance: {status} {response}").into());
    };
    let parsed: serde_json::Value = serde_json::from_str(&response)?;
    parsed
        .get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("rebalance start response omitted id: {response}").into())
}

pub(crate) async fn rebalance_status_json(cluster: &RustFSTestClusterEnvironment) -> TestResult<serde_json::Value> {
    let body = cluster_admin_ok(cluster, Method::GET, "/rustfs/admin/v3/rebalance/status", None).await?;
    Ok(serde_json::from_str(&body)?)
}

fn validate_rebalance_status<'a>(status: &'a serde_json::Value, expected_id: &str) -> TestResult<&'a [serde_json::Value]> {
    let id = status
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or("rebalance status omitted id")?;
    if id != expected_id {
        return Err(format!("rebalance status id changed: expected {expected_id}, got {id}").into());
    }
    let pools = status
        .get("pools")
        .and_then(serde_json::Value::as_array)
        .filter(|pools| !pools.is_empty())
        .ok_or("rebalance status omitted non-empty pools")?;
    for pool in pools {
        if pool.get("status").and_then(serde_json::Value::as_str).is_some_and(|status| {
            ["failed", "stopped", "canceled", "cancelled"]
                .iter()
                .any(|terminal| status.eq_ignore_ascii_case(terminal))
        }) {
            return Err(format!("rebalance entered an unsuccessful terminal state: {status}").into());
        }
        if pool.get("stopping").and_then(serde_json::Value::as_bool) == Some(true) {
            return Err(format!("rebalance entered stopping state: {status}").into());
        }
        if pool
            .get("lastError")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| !error.is_empty())
        {
            return Err(format!("rebalance reported lastError: {status}").into());
        }
        if nonzero_u64(pool.get("cleanupWarnings").and_then(|warnings| warnings.get("count"))) {
            return Err(format!("rebalance reported cleanup warnings: {status}").into());
        }
    }
    let propagation_failed = status.get("stopPropagation").is_some_and(|propagation| {
        propagation
            .get("failedPeers")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|peers| !peers.is_empty())
            || propagation
                .get("terminalReloadFailedPeers")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|peers| !peers.is_empty())
            || propagation.get("pendingTerminalReload").and_then(serde_json::Value::as_bool) == Some(true)
    });
    if propagation_failed {
        return Err(format!("rebalance stop propagation is incomplete: {status}").into());
    }
    Ok(pools)
}

pub(crate) fn rebalance_active(status: &serde_json::Value, expected_id: &str) -> TestResult<bool> {
    Ok(validate_rebalance_status(status, expected_id)?.iter().any(|pool| {
        pool.get("status")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("started"))
    }))
}

pub(crate) fn rebalance_complete(status: &serde_json::Value, expected_id: &str) -> TestResult<bool> {
    let pools = validate_rebalance_status(status, expected_id)?;
    let completed: Vec<&serde_json::Value> = pools
        .iter()
        .filter(|pool| {
            pool.get("status")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case("completed"))
        })
        .collect();
    let all_terminal = pools.iter().all(|pool| {
        pool.get("status")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("completed") || value.eq_ignore_ascii_case("none"))
    });
    let any_progress = completed.iter().any(|pool| {
        let progress = pool.get("progress");
        nonzero_u64(progress.and_then(|value| value.get("objects")))
            || nonzero_u64(progress.and_then(|value| value.get("versions")))
            || nonzero_u64(progress.and_then(|value| value.get("bytes")))
    });
    Ok(all_terminal && !completed.is_empty() && any_progress)
}

pub(crate) async fn wait_for_rebalance_active(
    cluster: &RustFSTestClusterEnvironment,
    expected_id: &str,
    timeout: Duration,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let status = rebalance_status_json(cluster).await?;
        if rebalance_active(&status, expected_id)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("rebalance did not become active within {timeout:?}; last status: {status}").into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

pub(crate) async fn wait_for_rebalance_complete(
    cluster: &RustFSTestClusterEnvironment,
    expected_id: &str,
    timeout: Duration,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let status = rebalance_status_json(cluster).await?;
        if rebalance_complete(&status, expected_id)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("rebalance did not complete with non-zero progress within {timeout:?}; last status: {status}").into(),
            );
        }
        sleep(Duration::from_secs(1)).await;
    }
}

pub(crate) async fn list_pools_json(cluster: &RustFSTestClusterEnvironment) -> TestResult<serde_json::Value> {
    let body = cluster_admin_ok(cluster, Method::GET, "/rustfs/admin/v3/pools/list", None).await?;
    Ok(serde_json::from_str(&body)?)
}

pub(crate) async fn retrying_put(client: &Client, bucket: &str, key: &str, body: Vec<u8>, timeout: Duration) -> TestResult {
    wait_until(
        timeout,
        || {
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let body = body.clone();
            async move {
                match put_object(&client, &bucket, &key, body).await {
                    Ok(()) => Ok(true),
                    Err(error) => {
                        let message = error.to_string();
                        if message.contains("SlowDown") || message.contains("ServiceUnavailable") || message.contains("503") {
                            Ok(false)
                        } else {
                            Err(error)
                        }
                    }
                }
            }
        },
        &format!("put {bucket}/{key} during data movement"),
    )
    .await
}

pub(crate) async fn retrying_get_equals(
    client: &Client,
    bucket: &str,
    key: &str,
    expected: &[u8],
    timeout: Duration,
) -> TestResult {
    wait_until(
        timeout,
        || async {
            match get_object_bytes(client, bucket, key).await {
                Ok(got) if got.as_slice() == expected => Ok(true),
                Ok(_) => Ok(false),
                Err(error) => {
                    let message = error.to_string();
                    if message.contains("NoSuchKey")
                        || message.contains("SlowDown")
                        || message.contains("ServiceUnavailable")
                        || message.contains("503")
                    {
                        Ok(false)
                    } else {
                        Err(error)
                    }
                }
            }
        },
        &format!("get {bucket}/{key} during data movement"),
    )
    .await
}

#[tokio::test]
async fn append_single_node_pool_extends_ellipses_volumes() {
    let mut env = RustFSTestClusterEnvironment::with_topology(ClusterTopology::per_node_pools(2, vec![vec![0], vec![1]]))
        .await
        .expect("two-pool seed topology");
    assert_eq!(env.rustfs_volumes_arg().split(' ').count(), 2);

    let added = env.append_single_node_pool().await.expect("append third pool");
    assert_eq!(added, 2);
    assert_eq!(env.nodes.len(), 3);
    assert_eq!(env.nodes[2].pool_idx, 2);
    assert_eq!(env.nodes[2].data_dirs.len(), 2);
    let volumes = env.rustfs_volumes_arg();
    assert_eq!(volumes.split(' ').count(), 3, "expected three pool arguments, got: {volumes}");
    assert!(volumes.contains("/drive{0...1}"), "expanded layout must keep drive ellipses: {volumes}");
}

#[tokio::test]
async fn append_single_node_pool_rejects_striped_single_pool() {
    let mut env = RustFSTestClusterEnvironment::new(4).await.expect("four-node single pool");
    let err = env
        .append_single_node_pool()
        .await
        .expect_err("a striped single pool cannot gain a localhost pool");
    let message = err.to_string();
    assert!(
        message.contains("drives_per_node") || message.contains("one node per pool"),
        "unexpected error: {message}"
    );
}

#[test]
fn pool_storage_roots_require_exactly_four_unique_paths() {
    let temp_root = std::env::temp_dir();
    let too_few = std::env::join_paths([temp_root.as_path()]).expect("join one path");
    let error = validate_pool_storage_roots(&too_few).expect_err("one pool root must be rejected");
    assert!(error.to_string().contains("exactly 4 paths"));

    let duplicates = std::env::join_paths([
        temp_root.as_path(),
        temp_root.as_path(),
        temp_root.as_path(),
        temp_root.as_path(),
    ])
    .expect("join duplicate paths");
    let error = validate_pool_storage_roots(&duplicates).expect_err("duplicate pool roots must be rejected");
    assert!(error.to_string().contains("duplicate path"));
}

#[cfg(unix)]
#[tokio::test]
async fn pool_storage_roots_reject_distinct_paths_on_the_same_device() {
    let env = RustFSTestClusterEnvironment::new(NODE_COUNT)
        .await
        .expect("create same-device pool root fixture");
    let roots: Vec<PathBuf> = env.nodes.iter().map(|node| PathBuf::from(&node.data_dir)).collect();
    let joined = std::env::join_paths(&roots).expect("join same-device paths");
    let error = validate_pool_storage_roots(&joined).expect_err("same-device pool roots must be rejected");
    assert!(error.to_string().contains("distinct filesystems"), "unexpected error: {error}");
}

#[test]
fn decommission_complete_requires_terminal_status_and_clean_counters() {
    let status = serde_json::json!({
        "pools": [
            {
                "id": 0,
                "status": "complete",
                "poolStatus": "decommissioned",
                "decommissionInfo": {
                    "complete": true,
                    "failed": false,
                    "canceled": false,
                    "objectsDecommissioned": 1,
                    "bytesDecommissioned": 1024
                }
            },
            { "id": 1, "status": "none", "poolStatus": "active" }
        ]
    });
    assert!(decommission_complete(&status, 0).unwrap());
    assert!(decommission_complete(&status, 1).is_err());
}

#[test]
fn rebalance_active_treats_started_as_in_progress() {
    let started = serde_json::json!({ "id": "run-1", "pools": [{ "id": 0, "status": "Started", "stopping": false }] });
    let done = serde_json::json!({ "id": "run-1", "pools": [{ "id": 0, "status": "Completed", "stopping": false }] });
    assert!(rebalance_active(&started, "run-1").unwrap());
    assert!(!rebalance_active(&done, "run-1").unwrap());
}

#[test]
fn rebalance_complete_accepts_non_participating_pools_but_requires_progress() {
    let completed = serde_json::json!({
        "id": "run-1",
        "pools": [
            { "id": 0, "status": "Completed", "stopping": false, "progress": { "objects": 2, "bytes": 1024 } },
            { "id": 1, "status": "None", "stopping": false, "progress": null },
            { "id": 2, "status": "None", "stopping": false, "progress": null },
            { "id": 3, "status": "None", "stopping": false, "progress": null }
        ]
    });
    assert!(rebalance_complete(&completed, "run-1").unwrap());

    let no_movement = serde_json::json!({
        "id": "run-1",
        "pools": [
            { "id": 0, "status": "None", "stopping": false, "progress": null },
            { "id": 1, "status": "None", "stopping": false, "progress": null }
        ]
    });
    assert!(!rebalance_complete(&no_movement, "run-1").unwrap());
}
