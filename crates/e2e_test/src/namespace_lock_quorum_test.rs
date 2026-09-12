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

async fn assert_degraded_cluster_publication_guard_errors_are_retryable() -> TestResult {
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.set_env("RUSTFS_STORAGE_CLASS_STANDARD", "EC:2");
    cluster.set_env("RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE", "false");
    cluster.set_env("RUSTFS_OBS_METRICS_EXPORT_ENABLED", "false");
    cluster.set_env("RUST_LOG", "warn");
    cluster.start().await?;
    cluster.create_test_bucket(BUCKET).await?;
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

    for alive in (1..=4).rev() {
        if alive < 4 {
            cluster.stop_node(alive)?;
        }
        for (node, client) in clients.iter().take(alive).enumerate() {
            let key = format!("publication-put-{alive}-{node}");
            let put = client
                .put_object()
                .bucket(BUCKET)
                .key(&key)
                .body(Bytes::from_static(b"publication guard regression").into())
                .send()
                .await;
            if alive >= 3 {
                put?;
            } else {
                let err = put.expect_err("PUT must reject writes without a write quorum");
                assert_eq!(
                    err.raw_response().map(|response| response.status().as_u16()),
                    Some(503),
                    "PUT with {alive} nodes alive, requested through node {node}: {err:?}"
                );
                assert_eq!(
                    err.as_service_error().and_then(|error| error.meta().code()),
                    Some("ServiceUnavailable"),
                    "PUT with {alive} nodes alive, requested through node {node}: {err:?}"
                );
            }

            let multipart_key = format!("publication-multipart-{alive}-{node}");
            let multipart = client
                .create_multipart_upload()
                .bucket(BUCKET)
                .key(&multipart_key)
                .send()
                .await;
            if alive >= 3 {
                let upload = multipart?;
                let upload_id = upload
                    .upload_id()
                    .expect("successful multipart initialization must return an upload ID");
                client
                    .abort_multipart_upload()
                    .bucket(BUCKET)
                    .key(&multipart_key)
                    .upload_id(upload_id)
                    .send()
                    .await?;
            } else {
                let err = multipart.expect_err("multipart initialization must reject writes without a write quorum");
                assert_eq!(
                    err.raw_response().map(|response| response.status().as_u16()),
                    Some(503),
                    "CreateMultipartUpload with {alive} nodes alive, requested through node {node}: {err:?}"
                );
                assert_eq!(
                    err.as_service_error().and_then(|error| error.meta().code()),
                    Some("ServiceUnavailable"),
                    "CreateMultipartUpload with {alive} nodes alive, requested through node {node}: {err:?}"
                );
            }
        }
    }

    cluster.stop();
    cluster.start().await?;
    for client in &clients {
        for alive in [1, 3, 4] {
            for node in 0..alive {
                let key = format!("publication-put-{alive}-{node}");
                let get = client.get_object().bucket(BUCKET).key(key).send().await;
                if alive >= 3 {
                    assert_eq!(
                        get?.body.collect().await?.into_bytes().as_ref(),
                        b"publication guard regression",
                        "acknowledged writes must survive restart"
                    );
                } else {
                    let err = get.expect_err("a rejected publication guard must not publish an object");
                    assert_eq!(err.as_service_error().and_then(|error| error.meta().code()), Some("NoSuchKey"));
                }
            }
        }
        let uploads = client.list_multipart_uploads().bucket(BUCKET).send().await?;
        assert!(
            uploads
                .uploads()
                .iter()
                .all(|upload| upload.key() != Some("publication-multipart-1-0")),
            "a rejected publication guard must not publish a multipart upload"
        );
    }
    Ok(())
}

async fn assert_quorum_object_body(client: &Client, bucket: &str, key: &str, expected: &[u8]) -> TestResult {
    let body = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(body.as_ref(), expected, "quorum read returned incorrect contents for {key}");
    Ok(())
}

async fn wait_for_quorum_read_admission(clients: &[Client], bucket: &str) -> TestResult {
    // SIGKILL can orphan a granted lease. Wait for shared metadata-lock
    // admission before asserting the stable quorum boundary; cold bodies
    // remain unread throughout this readiness probe.
    let deadline =
        tokio::time::Instant::now() + rustfs_lock::fast_lock::DEFAULT_LOCK_TIMEOUT + std::time::Duration::from_secs(15);
    loop {
        let mut ready = true;
        for client in clients {
            for key in ["warm-small", "warm-large"] {
                match client.head_object().bucket(bucket).key(key).send().await {
                    Ok(_) => {}
                    Err(error) if error.raw_response().is_some_and(|response| response.status().as_u16() == 503) => {
                        ready = false;
                        break;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            if !ready {
                break;
            }
        }
        if ready {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("read quorum did not become available after lease convergence for {bucket}").into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn test_degraded_cluster_read_quorum_follows_erasure_layout() -> TestResult {
    crate::common::init_logging();

    for (node_count, parity) in [(4, 2), (6, 3), (6, 2)] {
        let read_quorum = node_count - parity;
        let write_quorum = read_quorum + usize::from(read_quorum == parity);
        let mut cluster = RustFSTestClusterEnvironment::new(node_count).await?;
        cluster.set_env("RUSTFS_STORAGE_CLASS_STANDARD", format!("EC:{parity}"));
        // Wait for every seed fanout before removing any physical shard.
        cluster.set_env("RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE", "false");
        cluster.set_env("RUSTFS_OBS_METRICS_EXPORT_ENABLED", "false");
        cluster.set_env("RUST_LOG", "warn,rustfs_lock=debug");
        cluster.start().await?;

        let clients = cluster
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
            .collect::<Vec<_>>();
        let bucket = format!("read-quorum-{node_count}-{parity}");
        clients[0].create_bucket().bucket(&bucket).send().await?;
        let small = b"read quorum is derived from the erasure layout".to_vec();
        let large = (0..1_048_576)
            .map(|index| u8::try_from(index % 251).expect("bounded payload byte"))
            .collect::<Vec<_>>();
        for (key, body) in [
            ("warm-small", &small),
            ("warm-large", &large),
            ("cold-small", &small),
            ("cold-large", &large),
            ("below-quorum", &large),
        ] {
            clients[node_count - 1]
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(Bytes::copy_from_slice(body).into())
                .send()
                .await?;
        }
        for node in &cluster.nodes {
            for key in ["warm-small", "warm-large", "cold-small", "cold-large", "below-quorum"] {
                let census =
                    crate::chaos::census_object_version_on_disk(std::path::Path::new(&node.data_dir), &bucket, key, None)?;
                assert!(census.is_complete(), "seed shard must be complete before fault injection: {census:?}");
                assert_eq!(census.data_blocks, Some(read_quorum));
                assert_eq!(census.parity_blocks, Some(parity));
            }
        }
        for client in &clients {
            assert_quorum_object_body(client, &bucket, "warm-small", &small).await?;
            assert_quorum_object_body(client, &bucket, "warm-large", &large).await?;
        }

        for offline_node in (read_quorum..node_count).rev() {
            cluster.stop_node(offline_node)?;
            wait_for_quorum_read_admission(&clients[..offline_node], &bucket).await?;
            for client in clients.iter().take(offline_node) {
                client.head_bucket().bucket(&bucket).send().await?;
                assert_quorum_object_body(client, &bucket, "warm-large", &large).await?;
            }
        }

        // Exercise more than the five-second positive bucket-validation TTL.
        // Every sample must succeed; polling must not hide a transient failure.
        let validation_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(6);
        loop {
            for client in clients.iter().take(read_quorum) {
                assert_quorum_object_body(client, &bucket, "warm-small", &small).await?;
                assert_quorum_object_body(client, &bucket, "warm-large", &large).await?;
                let listing = client.list_objects_v2().bucket(&bucket).send().await?;
                for key in ["warm-small", "warm-large", "cold-small", "cold-large", "below-quorum"] {
                    assert!(listing.contents().iter().any(|entry| entry.key() == Some(key)), "listing omitted {key}");
                }
            }
            if tokio::time::Instant::now() >= validation_deadline {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        for client in clients.iter().take(read_quorum) {
            assert_quorum_object_body(client, &bucket, "cold-small", &small).await?;
            assert_quorum_object_body(client, &bucket, "cold-large", &large).await?;
        }

        let write = clients[0]
            .put_object()
            .bucket(&bucket)
            .key("quorum-write")
            .body(Bytes::copy_from_slice(&small).into())
            .send()
            .await;
        if read_quorum >= write_quorum {
            write?;
        } else {
            let error = write.expect_err("a read quorum must not authorize a write that needs more votes");
            assert_eq!(error.as_service_error().and_then(|error| error.meta().code()), Some("ServiceUnavailable"));
        }

        cluster.stop_node(read_quorum - 1)?;
        for client in clients.iter().take(read_quorum - 1) {
            match client.get_object().bucket(&bucket).key("below-quorum").send().await {
                Ok(response) => assert!(
                    response.body.collect().await.is_err(),
                    "fewer than {read_quorum} valid fragments must not reconstruct an uncached object"
                ),
                Err(error) => assert_eq!(
                    error.as_service_error().and_then(|error| error.meta().code()),
                    Some("ServiceUnavailable"),
                    "a quorum loss must not be mistaken for a missing object"
                ),
            }
        }

        for node in 0..read_quorum - 1 {
            cluster.stop_node(node)?;
        }
        cluster.start().await?;
        for client in &clients {
            assert_quorum_object_body(client, &bucket, "warm-large", &large).await?;
            assert_quorum_object_body(client, &bucket, "below-quorum", &large).await?;
        }
    }

    Ok(())
}

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
/// Also checks PUT and multipart initialization when node failures prevent
/// acquiring a table publication guard.
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
    cluster.stop();
    assert_degraded_cluster_publication_guard_errors_are_retryable().await
}
