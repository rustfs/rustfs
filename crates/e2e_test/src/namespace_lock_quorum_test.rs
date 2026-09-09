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
