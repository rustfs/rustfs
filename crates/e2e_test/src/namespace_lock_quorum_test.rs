// Copyright 2024 RustFS Team
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

use crate::common::RustFSTestClusterEnvironment;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use tracing::{info, warn};

const BUCKET: &str = "namespace-lock-quorum-bucket";
const KEY: &str = "thumb/79/concurrent-overwrite.jpg";

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn put_object(client: Client, payload: Vec<u8>, writer_id: usize) -> Result<(), String> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(Bytes::from(payload).into())
        .send()
        .await
        .map(|_| ())
        .map_err(|err| format_s3_error(err, writer_id))
}

fn format_s3_error(err: SdkError<aws_sdk_s3::operation::put_object::PutObjectError>, writer_id: usize) -> String {
    match err {
        SdkError::ServiceError(service_err) => {
            let err = service_err.err();
            let code = err.meta().code().unwrap_or("<unknown>");
            let message = err.meta().message().unwrap_or("<empty>");
            format!("writer {writer_id} returned service error {code}: {message}")
        }
        other => format!("writer {writer_id} returned SDK error: {other:?}"),
    }
}

#[tokio::test]
async fn test_concurrent_cluster_overwrites_do_not_fail_namespace_lock_quorum() -> TestResult {
    crate::common::init_logging();
    info!("Starting namespace lock quorum regression test with auto cluster");

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    // Keep the regression focused on false quorum-loss errors, not ordinary lock
    // wait exhaustion under a heavily contended same-key overwrite workload.
    cluster.set_env("RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT", "20");
    cluster.set_env("RUSTFS_STORAGE_CLASS_STANDARD", "EC:2");
    cluster.set_env("RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE", "false");
    cluster.set_env("RUSTFS_HEALTH_MINIMAL_RESPONSE_ENABLE", "false");
    cluster.start().await?;
    cluster.create_test_bucket(BUCKET).await?;

    let clients = cluster.create_all_clients()?;
    let first_payload = b"initial object contents".to_vec();
    put_object(clients[0].clone(), first_payload, 0).await?;

    let writer_count = clients.len() * 2;
    let barrier = Arc::new(Barrier::new(writer_count));
    let mut handles = Vec::with_capacity(writer_count);

    for writer_id in 0..writer_count {
        let client = clients[writer_id % clients.len()].clone();
        let barrier = barrier.clone();
        let payload = format!("replacement payload from writer {writer_id:02}").into_bytes();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            put_object(client, payload, writer_id).await
        }));
    }

    let mut failures = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => failures.push(err),
            Err(err) => failures.push(format!("writer task join failed: {err}")),
        }
    }

    if !failures.is_empty() {
        for failure in &failures {
            warn!("concurrent overwrite failure: {}", failure);
        }
    }

    assert!(
        failures.is_empty(),
        "concurrent overwrites must not surface namespace lock quorum failures: {failures:#?}"
    );

    let body = clients[0]
        .get_object()
        .bucket(BUCKET)
        .key(KEY)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    let body = std::str::from_utf8(&body)?;
    assert!(
        body.starts_with("replacement payload from writer "),
        "final object body should be one of the successful overwrite payloads, got {body:?}"
    );

    clients[0].delete_object().bucket(BUCKET).key(KEY).send().await?;
    assert_node_readiness_tracks_quorum(&mut cluster).await?;
    Ok(())
}

async fn assert_node_readiness_tracks_quorum(cluster: &mut RustFSTestClusterEnvironment) -> TestResult {
    let clients: Vec<_> = cluster
        .create_all_clients()?
        .into_iter()
        .map(|client| {
            Client::from_conf(
                client
                    .config()
                    .to_builder()
                    .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
                    .build(),
            )
        })
        .collect();
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()?;
    let seed_key = "readiness-seed";
    let seed_body = b"readiness quorum regression";
    clients[0]
        .put_object()
        .bucket(BUCKET)
        .key(seed_key)
        .body(Bytes::from_static(seed_body).into())
        .send()
        .await?;

    for (phase, survivors) in [4, 3, 2, 1, 4].into_iter().enumerate() {
        if phase == 4 {
            cluster.stop();
            cluster.start().await?;
        } else if survivors < 4 {
            cluster.stop_node(survivors)?;
        }
        let write_ready = survivors >= 3;
        let read_quorum = survivors >= 2;
        let expected_status = if write_ready { 200 } else { 503 };
        for (idx, client) in clients.iter().enumerate().take(survivors) {
            let url = &cluster.nodes[idx].url;
            let deadline = Instant::now() + Duration::from_secs(30);
            // Poll health before issuing S3 I/O: idle remote disk handles must
            // not remain evidence of quorum after their host becomes unreachable.
            let payload = loop {
                let response = http.get(format!("{url}/health/ready")).send().await?;
                let status = response.status().as_u16();
                let payload: serde_json::Value = response.json().await?;
                if status == expected_status
                    && payload["ready"] == write_ready
                    && payload["details"]["storage"]["ready"] == write_ready
                    && payload["details"]["storage"]["readQuorum"] == read_quorum
                    && payload["details"]["storage"]["writeQuorum"] == write_ready
                    && payload["details"]["poolMetadata"]["ready"] == true
                    && payload["details"]["iam"]["ready"] == true
                    && payload["details"]["lock"]["ready"] == write_ready
                {
                    break payload;
                }
                assert!(Instant::now() < deadline, "node {idx}, survivors={survivors}: HTTP {status}, {payload}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            };
            assert_eq!(payload["details"]["storage"]["readinessScope"], "write_quorum_and_pool_metadata");
            assert_eq!(payload["details"]["storage"]["source"], "local_runtime");
            assert_eq!(
                payload["details"]["storage"]["status"],
                if write_ready { "connected" } else { "disconnected" }
            );
            if !write_ready {
                assert!(
                    payload["degradedReasons"]
                        .as_array()
                        .expect("degraded reasons")
                        .iter()
                        .any(|reason| reason == "storage_and_lock_unavailable")
                );
            }

            for path in ["/health/ready", "/minio/health/ready"] {
                let head = http.head(format!("{url}{path}")).send().await?;
                assert_eq!(head.status().as_u16(), expected_status, "HEAD {path}, survivors={survivors}");
                assert!(head.bytes().await?.is_empty());
                let response = http.get(format!("{url}{path}")).send().await?;
                assert_eq!(response.status().as_u16(), expected_status);
                let body: serde_json::Value = response.json().await?;
                assert_eq!(body["details"]["storage"], payload["details"]["storage"]);
                assert_eq!(body["details"]["poolMetadata"], payload["details"]["poolMetadata"]);
            }
            let live = http.get(format!("{url}/health/live")).send().await?;
            assert_eq!(live.status().as_u16(), 200);
            assert!(live.json::<serde_json::Value>().await?.get("details").is_none());
            for (path, storage_ready, scope) in [
                ("/minio/health/cluster", write_ready, "write_quorum_and_pool_metadata"),
                ("/minio/health/cluster/read", read_quorum, "read_quorum"),
            ] {
                let deadline = Instant::now() + Duration::from_secs(30);
                // Cluster read/write reports have independent caches; allow
                // each observation to expire before comparing stable states.
                let body = loop {
                    let response = http.get(format!("{url}{path}")).send().await?;
                    let status = response.status().as_u16();
                    let body: serde_json::Value = response.json().await?;
                    if status == expected_status
                        && body["details"]["storage"]["ready"] == storage_ready
                        && body["details"]["lock"]["ready"] == write_ready
                    {
                        break body;
                    }
                    assert!(Instant::now() < deadline, "{path}, survivors={survivors}: HTTP {status}, {body}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                };
                assert_eq!(body["details"]["storage"]["readinessScope"], scope);
            }

            let put = client
                .put_object()
                .bucket(BUCKET)
                .key(format!("readiness-phase-{phase}-node-{idx}"))
                .body(Bytes::from_static(seed_body).into())
                .send()
                .await;
            let put_status = if write_ready {
                put.expect("a ready node must accept the PUT");
                200
            } else {
                let error = put.expect_err("subquorum node must reject PUT");
                assert!(
                    error.raw_response().is_some_and(|response| response.status().as_u16() >= 500),
                    "unexpected PUT failure: {error:?}"
                );
                error.raw_response().expect("PUT error response").status().as_u16()
            };
            let get = client.get_object().bucket(BUCKET).key(seed_key).send().await;
            let get_status = match get {
                Ok(object) => {
                    assert_eq!(object.body.collect().await?.into_bytes().as_ref(), seed_body);
                    200
                }
                Err(error) => {
                    assert!(!write_ready, "GET must succeed on a ready cluster: {error:?}");
                    let status = error
                        .raw_response()
                        .expect("GET should have an HTTP response")
                        .status()
                        .as_u16();
                    assert!(status >= 500, "unexpected GET failure: {error:?}");
                    status
                }
            };
            let list = client.list_objects_v2().bucket(BUCKET).send().await;
            let list_status = match list {
                Ok(result) => {
                    assert!(result.contents().iter().any(|object| object.key() == Some(seed_key)));
                    200
                }
                Err(error) => {
                    assert!(!write_ready, "listing must succeed on a ready cluster: {error:?}");
                    let status = error
                        .raw_response()
                        .expect("LIST should have an HTTP response")
                        .status()
                        .as_u16();
                    assert!(status >= 500, "unexpected listing failure: {error:?}");
                    status
                }
            };
            eprintln!(
                "readiness matrix: survivors={survivors}, node={idx}, ready={write_ready}, read_quorum={read_quorum}, PUT={put_status}, GET={get_status}, LIST={list_status}"
            );
        }
    }
    cluster.stop();
    Ok(())
}

/// Regression test: concurrent PUTs to the same key must return 503 (ServiceUnavailable)
/// on lock contention, never 500 (InternalError).
///
/// Before the fix, `map_namespace_lock_error` wrapped lock timeout/conflict errors as
/// `StorageError::other(...)` → `StorageError::Io(...)`, which fell through to
/// `S3ErrorCode::InternalError` (500) in the error mapping.
#[tokio::test]
async fn test_concurrent_put_same_key_never_returns_500() -> TestResult {
    crate::common::init_logging();
    info!("Starting concurrent PUT 500 regression test");

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    // Short lock timeout to trigger contention errors quickly
    cluster.set_env("RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT", "3");
    cluster.start().await?;
    cluster.create_test_bucket(BUCKET).await?;

    let clients = cluster.create_all_clients()?;
    let writer_count = clients.len() * 4; // 16 writers for heavy contention
    let barrier = Arc::new(Barrier::new(writer_count));

    // Seed initial object
    let first_payload = b"initial object for 500 regression".to_vec();
    put_object(clients[0].clone(), first_payload, 0).await?;

    let err_500_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let err_503_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let unexpected_err_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let ok_count = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut handles = Vec::with_capacity(writer_count);
    for writer_id in 0..writer_count {
        let client = clients[writer_id % clients.len()].clone();
        let barrier = barrier.clone();
        let payload = format!("payload from writer {writer_id:02}").into_bytes();
        let err_500 = err_500_count.clone();
        let err_503 = err_503_count.clone();
        let unexpected_err = unexpected_err_count.clone();
        let ok = ok_count.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            match client
                .put_object()
                .bucket(BUCKET)
                .key(KEY)
                .body(Bytes::from(payload).into())
                .send()
                .await
            {
                Ok(_) => {
                    ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(err) => match &err {
                    SdkError::ServiceError(service_err) => {
                        let code = service_err.err().meta().code().unwrap_or("<unknown>");
                        if code == "500" || code == "InternalError" {
                            err_500.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            warn!("writer {writer_id} returned 500: {code}");
                        } else if code == "503" || code == "ServiceUnavailable" {
                            err_503.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            unexpected_err.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            warn!("writer {writer_id} returned unexpected error: {code}");
                        }
                    }
                    other => {
                        unexpected_err.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        warn!("writer {writer_id} returned SDK error: {other:?}");
                    }
                },
            }
        }));
    }

    for handle in handles {
        if let Err(err) = handle.await {
            unexpected_err_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!("writer task join failed: {err}");
        }
    }

    let ok = ok_count.load(std::sync::atomic::Ordering::Relaxed);
    let err_503 = err_503_count.load(std::sync::atomic::Ordering::Relaxed);
    let err_500 = err_500_count.load(std::sync::atomic::Ordering::Relaxed);
    let unexpected_err = unexpected_err_count.load(std::sync::atomic::Ordering::Relaxed);
    let total = ok + err_503 + err_500 + unexpected_err;

    info!("Concurrent PUT 500 regression: total={total}, ok={ok}, 503={err_503}, 500={err_500}, unexpected={unexpected_err}");

    assert_eq!(
        total, writer_count as u64,
        "every concurrent PUT writer must be classified as success, 503, 500, or unexpected error"
    );

    assert_eq!(
        unexpected_err, 0,
        "Concurrent PUTs to the same key must only succeed or return 503. \
         Got {unexpected_err} unexpected errors out of {total} requests. 503 count: {err_503}, ok: {ok}"
    );

    assert_eq!(
        err_500, 0,
        "Concurrent PUTs to the same key must NEVER return 500 InternalError. \
         Got {err_500} out of {total} requests. 503 count: {err_503}, ok: {ok}"
    );

    clients[0].delete_object().bucket(BUCKET).key(KEY).send().await?;
    Ok(())
}
