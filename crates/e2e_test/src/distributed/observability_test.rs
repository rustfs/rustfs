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

use super::harness::{DistCluster, DistLayout, TestResult, cluster_admin_ok, unique_bucket, wait_for_ready};
use crate::common::{admin_request, init_logging, local_http_client};
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
use rustfs_madmin::metrics::RealtimeMetrics;
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

    collector.abort();
    Ok(())
}
