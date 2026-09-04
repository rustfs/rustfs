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
//! * **4×4 four pool** (`four_pool_four_drive`) — four single-node pools of
//!   four drives. Required for decommission/rebalance/expand, which the
//!   server rejects on a single pool.
//!
//! Genuine multi-node *striped* pools still need multi-host CI (backlog
//! #1313 / #1314). Site replication uses two 4-node 1-drive clusters so the
//! process count stays at eight rather than sixteen.

use crate::common::{
    ClusterTopology, FAST_DATA_USAGE_SCANNER_ENV, RustFSTestClusterEnvironment, admin_request, local_http_client,
    replication_fast_env, signed_request,
};
use crate::replication_extension_test::LOOPBACK_REPLICATION_TARGET_ENV;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use http::{Method, StatusCode};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) const NODE_COUNT: usize = 4;
pub(crate) const DRIVES_PER_NODE: usize = 4;

#[derive(Clone, Copy, Debug)]
pub(crate) enum DistLayout {
    /// 4 nodes × 4 drives, one erasure pool spanning every endpoint.
    FourByFour,
    /// 4 nodes × 1 drive, one erasure pool (minimum 4-node 4-disk layout).
    FourNodeFourDisk,
    /// 4 single-node pools, 4 drives each (expand / decommission / rebalance).
    FourPoolFourDrive,
    /// 2 single-node pools, 4 drives each (expansion seed).
    TwoPoolFourDrive,
}

pub(crate) struct DistCluster {
    pub cluster: RustFSTestClusterEnvironment,
}

impl DistCluster {
    pub async fn start(layout: DistLayout) -> TestResult<Self> {
        Self::start_with_env(layout, &[]).await
    }

    pub async fn start_with_env(layout: DistLayout, extra_env: &[(&str, &str)]) -> TestResult<Self> {
        let topology = match layout {
            DistLayout::FourByFour => ClusterTopology::single_pool_multidrive(NODE_COUNT, DRIVES_PER_NODE),
            DistLayout::FourNodeFourDisk => ClusterTopology::single_pool(NODE_COUNT),
            DistLayout::FourPoolFourDrive => {
                ClusterTopology::per_node_pools(DRIVES_PER_NODE, (0..NODE_COUNT).map(|idx| vec![idx]).collect())
            }
            DistLayout::TwoPoolFourDrive => ClusterTopology::per_node_pools(DRIVES_PER_NODE, vec![vec![0], vec![1]]),
        };
        let mut cluster = RustFSTestClusterEnvironment::with_topology(topology).await?;
        cluster.set_env("NO_PROXY", "127.0.0.1,localhost");
        cluster.set_env("HTTP_PROXY", "");
        cluster.set_env("HTTPS_PROXY", "");
        for &(key, value) in extra_env {
            cluster.set_env(key, value);
        }
        cluster.start().await?;
        Ok(Self { cluster })
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

/// Four-pool DistErasure on localhost can 500 a PUT while heal_bucket hits a
/// pool-meta write fence. Retry only those transient codes.
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

pub(crate) fn take_drive_offline(
    cluster: &RustFSTestClusterEnvironment,
    node_idx: usize,
    drive_idx: usize,
) -> TestResult<String> {
    let dir = cluster
        .nodes
        .get(node_idx)
        .and_then(|node| node.data_dirs.get(drive_idx))
        .ok_or("invalid node/drive index")?;
    let offline = format!("{dir}.offline");
    if Path::new(&offline).exists() {
        return Err(format!("drive already offline: {offline}").into());
    }
    std::fs::rename(dir, &offline)?;
    Ok(offline)
}

pub(crate) fn bring_drive_online(cluster: &RustFSTestClusterEnvironment, node_idx: usize, drive_idx: usize) -> TestResult {
    let dir = cluster
        .nodes
        .get(node_idx)
        .and_then(|node| node.data_dirs.get(drive_idx))
        .ok_or("invalid node/drive index")?;
    let offline = format!("{dir}.offline");
    if Path::new(dir).exists() {
        std::fs::remove_dir_all(dir)?;
    }
    std::fs::rename(&offline, dir)?;
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

/// Localhost DistErasure multi-pool can boot and serve S3 while still refusing
/// pool.bin mutations (`pool metadata writes remain blocked` / missing fleet
/// capability proof). Decommission and rebalance POST then 500. That is a
/// product gate, not a harness URL mistake; tests must not pretend a move ran.
pub(crate) fn is_pool_meta_write_fence(body: &str) -> bool {
    body.contains("pool metadata writes remain blocked")
        || body.contains("pool metadata recovery required")
        || body.contains("pool activation requires a live fleet capability proof")
        || body.contains("pool activation fleet capability proof expired")
        || body.contains("live fleet capability proof")
}

#[derive(Debug)]
pub(crate) enum DataMovementStart {
    Started,
    RefusedByPoolMetaFence(String),
}

pub(crate) async fn try_start_decommission(
    cluster: &RustFSTestClusterEnvironment,
    pool_id: usize,
) -> TestResult<DataMovementStart> {
    let path = format!("/rustfs/admin/v3/pools/decommission?pool={pool_id}&by-id=true");
    let (status, response) = cluster_admin(cluster, Method::POST, &path, None).await?;
    if status.is_success() {
        return Ok(DataMovementStart::Started);
    }
    if is_pool_meta_write_fence(&response) {
        return Ok(DataMovementStart::RefusedByPoolMetaFence(format!("{status} {response}")));
    }
    Err(format!("POST {path} failed: {status} {response}").into())
}

/// Returns whether decommission actually started. A pool-meta fence is not a
/// test failure: callers still assert object bytes. Any other error fails.
pub(crate) async fn decommission_started_or_fenced(cluster: &RustFSTestClusterEnvironment, pool_id: usize) -> TestResult<bool> {
    match try_start_decommission(cluster, pool_id).await? {
        DataMovementStart::Started => Ok(true),
        DataMovementStart::RefusedByPoolMetaFence(detail) => {
            eprintln!("decommission POST refused by pool-meta write fence on localhost DistErasure: {detail}");
            Ok(false)
        }
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

fn decommission_pool_failed(pool: &serde_json::Value) -> bool {
    let info = pool.get("decommissionInfo");
    let flagged = |key: &str| info.and_then(|value| value.get(key)).and_then(serde_json::Value::as_bool) == Some(true);
    flagged("failed")
        || flagged("canceled")
        || pool
            .get("status")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|status| status.eq_ignore_ascii_case("failed") || status.eq_ignore_ascii_case("canceled"))
}

pub(crate) fn decommission_complete(status: &serde_json::Value, pool_id: usize) -> bool {
    let Some(pool) = pool_entry(status, pool_id) else {
        return false;
    };
    if decommission_pool_failed(pool) {
        return false;
    }
    let info_complete = pool
        .get("decommissionInfo")
        .and_then(|value| value.get("complete"))
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let status_text = pool.get("status").and_then(serde_json::Value::as_str).unwrap_or("");
    let pool_status = pool.get("poolStatus").and_then(serde_json::Value::as_str).unwrap_or("");
    info_complete || status_text.eq_ignore_ascii_case("complete") || pool_status.eq_ignore_ascii_case("decommissioned")
}

pub(crate) fn decommission_failed(status: &serde_json::Value, pool_id: usize) -> bool {
    pool_entry(status, pool_id).is_some_and(decommission_pool_failed)
}

pub(crate) async fn wait_for_decommission_complete(
    cluster: &RustFSTestClusterEnvironment,
    pool_id: usize,
    timeout: Duration,
) -> TestResult {
    wait_until(
        timeout,
        || async {
            let status = decommission_status_json(cluster).await?;
            if decommission_failed(&status, pool_id) {
                return Err(format!("decommission failed for pool {pool_id}: {status}").into());
            }
            Ok(decommission_complete(&status, pool_id))
        },
        "decommission complete",
    )
    .await
}

pub(crate) async fn try_start_rebalance(cluster: &RustFSTestClusterEnvironment) -> TestResult<DataMovementStart> {
    let path = "/rustfs/admin/v3/rebalance/start";
    let (status, response) = cluster_admin(cluster, Method::POST, path, None).await?;
    if status.is_success() {
        return Ok(DataMovementStart::Started);
    }
    if is_pool_meta_write_fence(&response) {
        return Ok(DataMovementStart::RefusedByPoolMetaFence(format!("{status} {response}")));
    }
    Err(format!("POST {path} failed: {status} {response}").into())
}

pub(crate) async fn rebalance_started_or_fenced(cluster: &RustFSTestClusterEnvironment) -> TestResult<bool> {
    match try_start_rebalance(cluster).await? {
        DataMovementStart::Started => Ok(true),
        DataMovementStart::RefusedByPoolMetaFence(detail) => {
            eprintln!("rebalance POST refused by pool-meta write fence on localhost DistErasure: {detail}");
            Ok(false)
        }
    }
}

pub(crate) async fn rebalance_status_json(cluster: &RustFSTestClusterEnvironment) -> TestResult<serde_json::Value> {
    let body = cluster_admin_ok(cluster, Method::GET, "/rustfs/admin/v3/rebalance/status", None).await?;
    Ok(serde_json::from_str(&body)?)
}

pub(crate) fn rebalance_active(status: &serde_json::Value) -> bool {
    status
        .get("pools")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|pools| {
            pools.iter().any(|pool| {
                let stopping = pool.get("stopping").and_then(serde_json::Value::as_bool) == Some(true);
                let value = pool.get("status").and_then(serde_json::Value::as_str).unwrap_or("");
                stopping
                    || value.eq_ignore_ascii_case("started")
                    || value.eq_ignore_ascii_case("active")
                    || value.eq_ignore_ascii_case("running")
                    || value.eq_ignore_ascii_case("stopping")
            })
        })
}

pub(crate) async fn wait_for_rebalance_idle(cluster: &RustFSTestClusterEnvironment, timeout: Duration) -> TestResult {
    wait_until(
        timeout,
        || async {
            match rebalance_status_json(cluster).await {
                Ok(status) => Ok(!rebalance_active(&status)),
                Err(error) => {
                    let message = error.to_string();
                    if message.contains("NoSuchResource") || message.contains("404") || message.contains("not started") {
                        Ok(true)
                    } else {
                        Err(error)
                    }
                }
            }
        },
        "rebalance idle",
    )
    .await
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
                        if message.contains("SlowDown")
                            || message.contains("ServiceUnavailable")
                            || message.contains("InternalError")
                            || message.contains("503")
                            || message.contains("500")
                        {
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
                        || message.contains("InternalError")
                        || message.contains("503")
                        || message.contains("500")
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
fn decommission_complete_reads_pool_status_and_info_flag() {
    let status = serde_json::json!({
        "pools": [
            {
                "id": 0,
                "status": "complete",
                "poolStatus": "decommissioned",
                "decommissionInfo": { "complete": true, "failed": false, "canceled": false }
            },
            { "id": 1, "status": "none", "poolStatus": "active" }
        ]
    });
    assert!(decommission_complete(&status, 0));
    assert!(!decommission_complete(&status, 1));
    assert!(!decommission_failed(&status, 0));
}

#[test]
fn rebalance_active_treats_started_as_in_progress() {
    let started = serde_json::json!({ "pools": [{ "id": 0, "status": "Started", "stopping": false }] });
    let done = serde_json::json!({ "pools": [{ "id": 0, "status": "Completed", "stopping": false }] });
    assert!(rebalance_active(&started));
    assert!(!rebalance_active(&done));
}

#[test]
fn pool_meta_write_fence_matches_known_product_gates() {
    assert!(is_pool_meta_write_fence(
        "heal_bucket: pool metadata writes remain blocked after a recovery-required replica state"
    ));
    assert!(is_pool_meta_write_fence(
        "rebalance meta save failed: pool activation requires a live fleet capability proof"
    ));
    assert!(is_pool_meta_write_fence("pool metadata recovery required: no durable bootstrap identity"));
    assert!(!is_pool_meta_write_fence("NotImplemented: single pool cannot decommission"));
    assert!(!is_pool_meta_write_fence("InternalError"));
}
