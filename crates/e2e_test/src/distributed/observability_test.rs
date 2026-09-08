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
    DistCluster, DistLayout, TestResult, assert_object_bytes, cluster_admin_ok, unique_bucket, wait_for_ready, wait_until,
};
use crate::common::{admin_request, init_logging, local_http_client, signal_process, signed_request};
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use http::Method;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use local_ip_address::local_ip;
use rustfs_madmin::metrics::{HttpMetrics, RealtimeMetrics};
use rustfs_utils::egress::ENV_OUTBOUND_ALLOW_ORIGINS;
use serde_json::Value;
use std::convert::Infallible;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

async fn spawn_audit_collector() -> TestResult<(String, mpsc::UnboundedReceiver<Value>, JoinHandle<()>)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let endpoint = format!("http://{}/audit", std::net::SocketAddr::new(local_ip()?, listener.local_addr()?.port()));
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let tx = tx.clone();
                    async move {
                        let method = request.method().clone();
                        if let Ok(body) = request.into_body().collect().await
                            && method == Method::POST
                            && let Ok(payload) = serde_json::from_slice::<Value>(&body.to_bytes())
                        {
                            if let Some(records) = payload["Records"].as_array() {
                                for entry in records {
                                    let _ = tx.send(entry.clone());
                                }
                            } else {
                                let _ = tx.send(payload);
                            }
                        }
                        Ok::<_, Infallible>(Response::new(Empty::<Bytes>::new()))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Ok((endpoint, rx, handle))
}

async fn wait_for_audit_entry(
    rx: &mut mpsc::UnboundedReceiver<Value>,
    bucket: &str,
    key: &str,
    request_id: &str,
) -> TestResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "audit webhook did not receive PutObject for {bucket}/{key}; received {} other records: {seen:?}",
                seen.len()
            )
            .into());
        }
        let entry = match timeout(remaining, rx.recv()).await {
            Ok(Some(entry)) => entry,
            Ok(None) => return Err("audit collector stopped before the expected entry arrived".into()),
            Err(_) => {
                return Err(format!(
                    "audit webhook did not receive PutObject for {bucket}/{key}; received {} other records: {seen:?}",
                    seen.len()
                )
                .into());
            }
        };
        if entry["api"]["name"].as_str() == Some("s3:PutObject")
            && entry["api"]["bucket"].as_str() == Some(bucket)
            && entry["api"]["object"].as_str() == Some(key)
            && entry["requestID"].as_str() == Some(request_id)
        {
            return Ok(entry);
        }
        if seen.len() < 8 {
            seen.push(format!(
                "api={:?} bucket={:?} object={:?} requestID={:?}",
                entry["api"]["name"].as_str(),
                entry["api"]["bucket"].as_str(),
                entry["api"]["object"].as_str(),
                entry["requestID"].as_str()
            ));
        }
    }
}

#[tokio::test]
async fn four_node_health_inventory_metrics_and_audit_delivery_are_consistent() -> TestResult {
    init_logging();
    let (audit_endpoint, mut audit_entries, collector) = spawn_audit_collector().await?;
    let audit_origin = reqwest::Url::parse(&audit_endpoint)?.origin().ascii_serialization();
    let audit_env = [
        ("RUST_LOG", "warn"),
        ("RUSTFS_AUDIT_ENABLE", "true"),
        ("RUSTFS_AUDIT_WEBHOOK_ENABLE_DISTRIBUTED", "on"),
        ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_DISTRIBUTED", audit_endpoint.as_str()),
        (ENV_OUTBOUND_ALLOW_ORIGINS, audit_origin.as_str()),
    ];
    let mut dist = DistCluster::new_stopped_with_env(DistLayout::FourByFour, &audit_env).await?;
    for node_idx in 0..dist.cluster.nodes.len() {
        let queue_dir = format!("{}/audit-queue-node-{node_idx}", dist.cluster.temp_dir);
        tokio::fs::create_dir_all(&queue_dir).await?;
        dist.cluster
            .set_node_env(node_idx, "RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_DISTRIBUTED", queue_dir)?;
    }
    dist.cluster.start().await?;
    wait_for_ready(&dist.cluster).await?;

    let http = local_http_client();
    for node in &dist.cluster.nodes {
        for probe in ["ready", "live"] {
            let response = http.get(format!("{}/health/{probe}", node.url)).send().await?;
            assert!(
                response.status().is_success(),
                "node {} {probe} probe failed: {}",
                node.address,
                response.status()
            );
        }
    }

    let info_body = cluster_admin_ok(&dist.cluster, Method::GET, "/rustfs/admin/v3/info", None).await?;
    let info: Value = serde_json::from_str(&info_body)?;
    let servers = info["info"]["servers"]
        .as_array()
        .ok_or_else(|| format!("admin info omitted servers: {info}"))?;
    assert_eq!(servers.len(), 4, "admin info did not report all four nodes: {info}");

    let storage_body = cluster_admin_ok(&dist.cluster, Method::GET, "/rustfs/admin/v3/storageinfo", None).await?;
    let storage: Value = serde_json::from_str(&storage_body)?;
    let disks = storage["info"]["disks"]
        .as_array()
        .ok_or_else(|| format!("storageinfo omitted disks: {storage}"))?;
    assert_eq!(disks.len(), 16, "storageinfo did not report all sixteen drives: {storage}");
    assert!(
        disks.iter().all(|disk| {
            disk["state"].as_str().is_some_and(|state| state.eq_ignore_ascii_case("ok"))
                && disk["runtimeState"]
                    .as_str()
                    .is_some_and(|state| state.eq_ignore_ascii_case("online"))
        }),
        "storageinfo reported a drive that was not healthy and online: {storage}"
    );

    for (node_idx, node) in dist.cluster.nodes.iter().enumerate() {
        let (status, metrics_body) = admin_request(
            &node.url,
            Method::GET,
            "/rustfs/admin/v3/metrics?n=1&by-host=true&by-disk=true",
            None,
            &dist.cluster.access_key,
            &dist.cluster.secret_key,
        )
        .await?;
        assert!(status.is_success(), "node {node_idx} metrics failed: {status} {metrics_body}");
        let sample: RealtimeMetrics = serde_json::from_str(
            metrics_body
                .lines()
                .next()
                .ok_or_else(|| format!("node {node_idx} returned empty metrics"))?,
        )?;
        assert!(sample.finally, "node {node_idx} metrics sample was not terminal");
        assert!(sample.errors.is_empty(), "node {node_idx} metrics reported errors: {:?}", sample.errors);
        assert!(!sample.hosts.is_empty(), "node {node_idx} metrics omitted hosts");
    }

    let targets_body = cluster_admin_ok(&dist.cluster, Method::GET, "/rustfs/admin/v3/audit/target/list", None).await?;
    let targets: Value = serde_json::from_str(&targets_body)?;
    let configured = targets["audit_endpoints"]
        .as_array()
        .ok_or_else(|| format!("audit target list omitted audit_endpoints: {targets}"))?
        .iter()
        .any(|target| target["account_id"].as_str() == Some("distributed") && target["service"].as_str() == Some("webhook"));
    assert!(configured, "configured audit webhook was missing: {targets}");

    let bucket = unique_bucket("audit");
    dist.create_bucket(&bucket).await?;
    let key = "correlated/audit-object.bin";
    let put = dist
        .client(2)?
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"distributed audit payload"))
        .send()
        .await?;
    let request_id = put.request_id().ok_or("PutObject response omitted request ID")?;
    let audit = wait_for_audit_entry(&mut audit_entries, &bucket, key, request_id).await?;
    assert_eq!(
        audit["api"]["status_code"].as_i64(),
        Some(200),
        "audit entry did not report success: {audit}"
    );
    assert!(
        !audit.to_string().contains(&dist.cluster.secret_key),
        "audit entry leaked the root secret key"
    );

    let result = verify_write_observations_during_peer_failure(&dist, &bucket).await;
    collector.abort();
    result
}

async fn node_admin_body(dist: &DistCluster, node: usize, path: &str) -> TestResult<String> {
    let (status, body) = timeout(
        Duration::from_secs(30),
        admin_request(
            &dist.cluster.nodes[node].url,
            Method::GET,
            path,
            None,
            &dist.cluster.access_key,
            &dist.cluster.secret_key,
        ),
    )
    .await??;
    assert!(status.is_success(), "node {node} admin request {path}: {status} {body}");
    Ok(body)
}

async fn http_put_counts(dist: &DistCluster, node: usize) -> TestResult<[u64; 2]> {
    let body = node_admin_body(dist, node, "/rustfs/admin/v3/metrics?types=512&by-host=true&n=1").await?;
    let sample: RealtimeMetrics = serde_json::from_str(body.lines().next().ok_or("empty HTTP metrics stream")?)?;
    assert!(sample.errors.is_empty(), "HTTP metrics returned errors: {:?}", sample.errors);
    let http = sample.aggregated.http.ok_or("HTTP metrics missing at WARN log level")?;
    let count = |http: &HttpMetrics, outcome: &str| {
        http.requests
            .iter()
            .filter(|row| row.method == "PUT" && row.outcome == outcome)
            .map(|row| row.total)
            .sum::<u64>()
    };
    assert_eq!(sample.by_host.len(), 1, "HTTP admin metrics must remain node-local");
    let host = sample.by_host.values().next().expect("one reporting host");
    let host = host.http.as_ref().ok_or("by-host HTTP metrics missing")?;
    let totals = [count(&http, "2xx"), count(&http, "5xx")];
    assert_eq!(totals, [count(host, "2xx"), count(host, "5xx")]);
    Ok(totals)
}

async fn observed_put(dist: &DistCluster, node: usize, bucket: &str, key: &str) -> TestResult<http::StatusCode> {
    // One signed HTTP attempt: SDK retries must not change the expected denominator.
    timeout(Duration::from_secs(90), async {
        let response = signed_request(
            Method::PUT,
            &format!("{}/{bucket}/{key}", dist.cluster.nodes[node].url),
            &dist.cluster.access_key,
            &dist.cluster.secret_key,
            Some(b"write-observation".to_vec()),
            Some("application/octet-stream"),
        )
        .await?;
        assert!(response.headers().contains_key("x-amz-request-id"), "PUT omitted correlation ID");
        let status = response.status();
        let body = response.text().await?;
        assert!(!body.contains(&dist.cluster.secret_key), "PUT response leaked credentials");
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(status)
    })
    .await?
}

struct SuspendedPeer<'a> {
    // Borrowing the owned child keeps its PID from being reaped/reused before cleanup.
    child: &'a std::process::Child,
    suspended: bool,
}

impl<'a> SuspendedPeer<'a> {
    fn suspend(dist: &'a DistCluster, node: usize) -> TestResult<Self> {
        let child = dist.cluster.nodes[node].process.as_ref().ok_or("peer process missing")?;
        signal_process(child.id(), "STOP")?;
        Ok(Self { child, suspended: true })
    }

    fn resume(&mut self) -> TestResult {
        signal_process(self.child.id(), "CONT")?;
        self.suspended = false;
        Ok(())
    }
}

impl Drop for SuspendedPeer<'_> {
    fn drop(&mut self) {
        if self.suspended {
            let _ = signal_process(self.child.id(), "CONT");
        }
    }
}

async fn verify_write_observations_during_peer_failure(dist: &DistCluster, bucket: &str) -> TestResult {
    let mut baseline = Vec::new();
    for node in 0..dist.cluster.nodes.len() {
        let before = http_put_counts(dist, node).await?;
        assert!(
            observed_put(dist, node, bucket, &format!("healthy-{node}"))
                .await?
                .is_success()
        );
        let after = http_put_counts(dist, node).await?;
        assert_eq!(after, [before[0] + 1, before[1]], "node {node} lost its successful PUT denominator");
        baseline.push(after);
    }

    // Refresh provenance immediately before the first failed probe, within the cache age budget.
    node_admin_body(dist, 0, "/rustfs/admin/v3/storageinfo").await?;
    let mut suspended = [SuspendedPeer::suspend(dist, 2)?, SuspendedPeer::suspend(dist, 3)?];
    let storage: Value = serde_json::from_str(&node_admin_body(dist, 0, "/rustfs/admin/v3/storageinfo").await?)?;
    let observations = storage["info"]["observations"]
        .as_array()
        .ok_or("storageinfo omitted observations")?;
    let disks = storage["info"]["disks"]
        .as_array()
        .ok_or("storageinfo omitted disks during peer failure")?;
    assert_eq!(disks.len(), 16, "failed peers must not vanish from inventory");
    for node in [2, 3] {
        let endpoint = &dist.cluster.nodes[node].address;
        let observation = observations
            .iter()
            .find(|item| item["endpoint"].as_str().is_some_and(|value| value.contains(endpoint)))
            .ok_or_else(|| format!("missing failed peer observation {endpoint}: {storage}"))?;
        assert_eq!(observation["status"], "failed", "suspension did not affect peer RPC: {observation}");
        assert_eq!(observation["cached"], true, "first failure must identify the warm cache: {observation}");
        assert!(observation["last_success_unix_millis"].as_u64().is_some());
        assert!(observation["snapshot_age_seconds"].as_u64().is_some_and(|age| age < 60));
        let peer_disks: Vec<_> = disks
            .iter()
            .filter(|disk| disk["endpoint"].as_str().is_some_and(|value| value.contains(endpoint)))
            .collect();
        assert_eq!(peer_disks.len(), 4, "failed peer lost its four drive identities: {storage}");
        for disk in peer_disks {
            assert_eq!(disk["state"], "unknown");
            assert_eq!(disk["runtimeState"], "unknown");
        }
    }

    let snapshot: Value = serde_json::from_str(&node_admin_body(dist, 0, "/rustfs/admin/v4/cluster/snapshot").await?)?;
    let metadata = &snapshot["snapshot"]["pool_meta_write_gate"];
    assert_eq!(
        metadata["state"], "writable",
        "peer probe failure must not invent a metadata latch: {snapshot}"
    );
    assert!(metadata.get("sinceUnixSecs").is_none());

    for attempt in 0..2 {
        let status = observed_put(dist, 0, bucket, &format!("unavailable-{attempt}")).await?;
        assert!(status.is_server_error(), "sub-quorum write unexpectedly returned {status}");
    }
    assert_eq!(http_put_counts(dist, 0).await?, [baseline[0][0], baseline[0][1] + 2]);
    assert_eq!(
        http_put_counts(dist, 1).await?,
        baseline[1],
        "internal RPCs must not count as external PUTs"
    );

    for peer in &mut suspended {
        peer.resume()?;
    }
    wait_until(
        Duration::from_secs(90),
        || async {
            let storage: Value = serde_json::from_str(&node_admin_body(dist, 0, "/rustfs/admin/v3/storageinfo").await?)?;
            let observations = storage["info"]["observations"]
                .as_array()
                .ok_or("recovery omitted observations")?;
            Ok(observations.len() == 4
                && observations
                    .iter()
                    .all(|item| item["status"] == "succeeded" && item["cached"] == false))
        },
        "peer probes recover to fresh successful observations",
    )
    .await?;
    wait_for_ready(&dist.cluster).await?;
    assert!(observed_put(dist, 0, bucket, "recovered").await?.is_success());
    assert_eq!(http_put_counts(dist, 0).await?, [baseline[0][0] + 1, baseline[0][1] + 2]);
    for node in 0..dist.cluster.nodes.len() {
        assert_object_bytes(&dist.client(node)?, bucket, "healthy-0", b"write-observation").await?;
        assert_object_bytes(&dist.client(node)?, bucket, "recovered", b"write-observation").await?;
    }
    Ok(())
}
