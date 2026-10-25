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
    TestResult, assert_object_bytes, cluster_admin_ok, configure_node_logs, payload_for, retrying_put, sha256_hex, unique_bucket,
    wait_until,
};
use crate::common::{ClusterTopology, RustFSTestClusterEnvironment};
use crate::rpc_fault_proxy::{RpcFaultProxy, RpcPause};
use aws_sdk_s3::{
    Client,
    config::retry::RetryConfig,
    error::{ProvideErrorMetadata, SdkError},
    operation::create_multipart_upload::{CreateMultipartUploadError, CreateMultipartUploadOutput},
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const DEADLINE: Duration = Duration::from_secs(45);

async fn fixture(
    drives_per_node: usize,
    storage_class: &str,
) -> TestResult<(RustFSTestClusterEnvironment, Vec<RpcFaultProxy>, Vec<Client>, String)> {
    let mut cluster =
        RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(4, drives_per_node)).await?;
    for (name, value) in [
        ("RUSTFS_STORAGE_CLASS_STANDARD", storage_class),
        ("RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT", "5"),
        ("RUSTFS_OBJECT_LOCK_RPC_TIMEOUT_MS", "3000"),
        ("RUSTFS_DRIVE_METADATA_TIMEOUT_SECS", "2"),
        ("RUSTFS_DRIVE_MAX_TIMEOUT_DURATION", "5"),
        // Keep disk-health ejection out of a method-specific quorum test.
        ("RUSTFS_DRIVE_SUSPECT_FAILURE_THRESHOLD", "1000"),
        ("RUSTFS_OBS_METRICS_EXPORT_ENABLED", "false"),
        ("NO_PROXY", "127.0.0.1,::1,localhost"),
        ("HTTP_PROXY", ""),
        ("HTTPS_PROXY", ""),
        ("RUST_LOG", "warn"),
    ] {
        cluster.set_env(name, value);
    }
    let mut proxies = Vec::new();
    for node in 0..4 {
        proxies.push(cluster.start_rpc_proxy_for_node(node).await?);
    }
    configure_node_logs(&mut cluster, None)?;
    cluster.start().await?;
    let clients = cluster
        .create_all_clients()?
        .into_iter()
        .map(|client| {
            Client::from_conf(
                client
                    .config()
                    .to_builder()
                    .retry_config(RetryConfig::standard().with_max_attempts(1))
                    .build(),
            )
        })
        .collect();
    let bucket = unique_bucket("mpu-fence");
    cluster.create_test_bucket(&bucket).await?;
    Ok((cluster, proxies, clients, bucket))
}

fn create(
    client: &Client,
    bucket: &str,
    key: &str,
) -> JoinHandle<Result<CreateMultipartUploadOutput, SdkError<CreateMultipartUploadError>>> {
    let request = client.create_multipart_upload().bucket(bucket).key(key);
    tokio::spawn(async move { request.send().await })
}

async fn wait_for_faults(pauses: &[RpcPause], methods: &[&str]) -> TestResult {
    if methods.iter().any(|method| matches!(*method, "Lock" | "LockBatch")) {
        // Adaptive lock fan-out may not contact an unnecessary voter. All
        // selected peers remain gated; require an actual scoped request hit.
        let (result, _, _) = futures::future::select_all(pauses.iter().map(|pause| Box::pin(pause.wait_for_hits(1)))).await;
        result?;
    } else {
        // Storage mutations fan out eagerly. Every selected peer must really
        // receive this upload's RPC, so an unrelated failure cannot fake quorum.
        for pause in pauses {
            pause.wait_for_hits(1).await?;
        }
    }
    Ok(())
}

fn assert_unavailable<E: std::fmt::Debug + ProvideErrorMetadata>(error: &SdkError<E>, cause: &str, retry: Option<&str>) {
    let response = error.raw_response().expect("an S3 service response");
    assert_eq!(response.status().as_u16(), 503, "{error:?}");
    assert_eq!(error.as_service_error().and_then(|e| e.meta().code()), Some("ServiceUnavailable"));
    assert_eq!(response.headers().get("x-rustfs-error-cause"), Some(cause));
    assert_eq!(response.headers().get("retry-after"), retry);
    assert!(
        response
            .headers()
            .get("content-type")
            .is_some_and(|v| v.starts_with("application/xml"))
    );
    let request_id = response.headers().get("x-amz-request-id").expect("request correlation ID");
    assert!(!request_id.is_empty());
    eprintln!("MPU acceptance: cause={cause}, retry={retry:?}, request_id={request_id}");
}

async fn abort(client: &Client, bucket: &str, key: &str, upload: &str) -> TestResult {
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload)
        .send()
        .await?;
    let missing = client
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(upload)
        .send()
        .await
        .expect_err("an aborted upload must no longer admit ListParts");
    assert_eq!(missing.as_service_error().and_then(|e| e.meta().code()), Some("NoSuchUpload"));
    Ok(())
}

// Fault removal does not synchronously clear existing server leases or drive
// health state. Keep the injected operation single-attempt, then require
// bounded recovery of the exact cleanup API instead of assuming instant heal.
async fn recover_after_fault<T, E, F, Fut>(operation: &str, mut call: F) -> TestResult<Result<T, SdkError<E>>>
where
    E: std::fmt::Debug + ProvideErrorMetadata,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, SdkError<E>>>,
{
    let started = Instant::now();
    let mut attempts = 0;
    let mut last_failure = String::new();
    let outcome = tokio::time::timeout(DEADLINE, async {
        loop {
            attempts += 1;
            let result = call().await;
            let retry = result.as_ref().err().and_then(|error| {
                let response = error.raw_response()?;
                if response.status().as_u16() != 503
                    || error.as_service_error()?.meta().code() != Some("ServiceUnavailable")
                    || !matches!(
                        response.headers().get("x-rustfs-error-cause"),
                        Some("namespace_lock_timeout" | "namespace_lock_unavailable" | "storage_write_quorum")
                    )
                {
                    return None;
                }
                let delay = match response.headers().get("retry-after") {
                    Some("1") => Duration::from_secs(1),
                    None => Duration::from_millis(100),
                    Some(_) => return None,
                };
                last_failure = format!(
                    "cause={:?}, request_id={:?}",
                    response.headers().get("x-rustfs-error-cause"),
                    response.headers().get("x-amz-request-id")
                );
                Some(delay)
            });
            match retry {
                Some(delay) => tokio::time::sleep(delay).await,
                None => return result,
            }
        }
    })
    .await;
    eprintln!(
        "MPU recovery: operation={operation}, attempts={attempts}, elapsed={:?}, last={last_failure}",
        started.elapsed()
    );
    outcome.map_err(|_| format!("{operation} did not recover within {DEADLINE:?}: {last_failure}").into())
}

async fn abort_after_fault(client: &Client, bucket: &str, key: &str, upload: &str, allow_missing: bool) -> TestResult {
    match recover_after_fault(&format!("Abort {key}"), || {
        client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload)
            .send()
    })
    .await?
    {
        Ok(_) => {}
        Err(error) if allow_missing && error.as_service_error().and_then(|e| e.meta().code()) == Some("NoSuchUpload") => {}
        Err(error) => return Err(error.into()),
    }
    let missing = recover_after_fault(&format!("ListParts {key}"), || {
        client.list_parts().bucket(bucket).key(key).upload_id(upload).send()
    })
    .await?
    .expect_err("a recovered Abort must remove the upload");
    assert_eq!(missing.as_service_error().and_then(|e| e.meta().code()), Some("NoSuchUpload"));
    Ok(())
}

async fn round_trip(clients: &[Client], bucket: &str, key: &str, size: usize) -> TestResult {
    let upload = clients[0].create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = upload.upload_id().ok_or("missing upload ID")?;
    let body = payload_for(key, size);
    let mut parts: Vec<_> = stream::iter(body.chunks(8 * 1024 * 1024).enumerate())
        .map(|(part, bytes)| async move {
            let number = i32::try_from(part + 1)?;
            let result = clients[part % clients.len()]
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(number)
                .body(ByteStream::from(bytes.to_vec()))
                .send()
                .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                CompletedPart::builder()
                    .part_number(number)
                    .e_tag(result.e_tag().ok_or("missing part ETag")?)
                    .build(),
            )
        })
        .buffer_unordered(4)
        .try_collect()
        .await?;
    parts.sort_by_key(|part| part.part_number());
    clients[1]
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
        .send()
        .await?;
    for client in clients {
        assert_object_bytes(client, bucket, key, &body).await?;
    }
    clients[0].delete_object().bucket(bucket).key(key).send().await?;
    Ok(())
}

async fn assert_no_uploads(clients: &[Client], bucket: &str) -> TestResult {
    for client in clients {
        let uploads = client.list_multipart_uploads().bucket(bucket).send().await?;
        assert!(uploads.uploads().is_empty(), "unexpected residual uploads: {:?}", uploads.uploads());
    }
    Ok(())
}

async fn assert_no_uploads_after_fault(clients: &[Client], bucket: &str) -> TestResult {
    for client in clients {
        let uploads = recover_after_fault("ListMultipartUploads after fault", || {
            client.list_multipart_uploads().bucket(bucket).send()
        })
        .await??;
        assert!(uploads.uploads().is_empty(), "unexpected residual uploads: {:?}", uploads.uploads());
    }
    Ok(())
}

async fn assert_object_bytes_after_fault(client: &Client, bucket: &str, key: &str, expected: &[u8]) -> TestResult {
    let output = recover_after_fault(&format!("GetObject {key} after fault"), || {
        client.get_object().bucket(bucket).key(key).send()
    })
    .await??;
    // A successful response must have a complete, correct body. Streaming
    // failures are not retried or converted into a successful comparison.
    let bytes = output.body.collect().await?.into_bytes();
    if bytes.as_ref() != expected {
        return Err(format!("object {key} bytes differ: expected {} bytes, got {}", expected.len(), bytes.len()).into());
    }
    Ok(())
}

// This fixture inspects RustFS RPCs, so use the server-generated upload's
// canonical suffix and fail explicitly if its encoding changes.
fn upload_scope(upload_id: &str) -> TestResult<String> {
    let decoded = String::from_utf8(base64_simd::URL_SAFE_NO_PAD.decode_to_vec(upload_id.as_bytes())?)?;
    let (_, suffix) = decoded.split_once('.').ok_or("server upload ID lacks deployment delimiter")?;
    if suffix.is_empty() {
        return Err("server upload ID has an empty suffix".into());
    }
    Ok(suffix.to_owned())
}

fn upload_path_scope(bucket: &str, key: &str, upload_id: &str) -> TestResult<String> {
    Ok(format!(
        "{}/{}",
        sha256_hex(format!("{bucket}/{key}").as_bytes()),
        upload_scope(upload_id)?
    ))
}

async fn with_paused_peers<T: Send + 'static>(
    proxies: &[RpcFaultProxy],
    methods: &[&'static str],
    limits: &[usize],
    scope: &str,
    request: impl std::future::Future<Output = T> + Send + 'static,
) -> TestResult<T> {
    let pauses: Vec<_> = proxies[1..=limits.len()]
        .iter()
        .zip(limits)
        .map(|(proxy, limit)| proxy.pause(methods, *limit, Some(scope)))
        .collect::<Result<_, _>>()?;
    let task = tokio::spawn(request);
    wait_for_faults(&pauses, methods).await?;
    let result = tokio::time::timeout(DEADLINE, task).await??;
    for pause in &pauses {
        pause.discard_pending();
    }
    drop(pauses);
    Ok(result)
}

fn quorum_outcome<T: std::fmt::Debug, E: std::fmt::Debug + ProvideErrorMetadata>(
    result: Result<T, SdkError<E>>,
    at_quorum: bool,
    locks: bool,
) -> TestResult<Option<T>> {
    if at_quorum {
        Ok(Some(result.map_err(|error| format!("exactly quorum must succeed: {error:?}"))?))
    } else {
        assert_unavailable(
            &result.expect_err("quorum-minus-one must fail"),
            if locks {
                "namespace_lock_timeout"
            } else {
                "storage_write_quorum"
            },
            if locks { Some("1") } else { None },
        );
        Ok(None)
    }
}

async fn wait_for_lock_peer_recovery(proxies: &[RpcFaultProxy], client: &Client, bucket: &str) -> TestResult {
    let key = unique_bucket("peer-recovery");
    let observers = proxies[1..]
        .iter()
        .map(|proxy| proxy.pause(&["Lock", "LockBatch"], usize::MAX, Some(&key)))
        .collect::<Result<Vec<_>, _>>()?;
    for observer in &observers {
        observer.release();
    }
    wait_until(
        DEADLINE,
        || async {
            retrying_put(client, bucket, &key, vec![0x42; 1024], DEADLINE).await?;
            // Quorum success alone can leave one peer's request breaker open.
            // Two replies require a half-open peer to finish its first probe
            // before admitting another RPC; every peer must rejoin the matrix.
            Ok(observers.iter().all(|observer| observer.forwarded() >= 2))
        },
        "all lock peers recover after fault retirement",
    )
    .await
}

async fn phase_quorum_matrix(
    proxies: &[RpcFaultProxy],
    clients: &[Client],
    bucket: &str,
    locks: bool,
    drives_per_node: usize,
) -> TestResult {
    for phase in ["create", "part", "complete", "abort"] {
        // Ordinary Create holds only the shared bucket lifecycle fence.
        // Four-node read quorum is two; commit/write quorum is three.
        let unavailable_at_quorum = if locks && phase == "create" { 2 } else { 1 };
        for unavailable in [unavailable_at_quorum, unavailable_at_quorum + 1] {
            let key = format!("{locks}-{phase}-{unavailable}");
            let mut limits = vec![usize::MAX; unavailable];
            // EC12+4: one peer contributes four shards; one packet on the
            // second peer moves exactly twelve available shards to eleven.
            if !locks && drives_per_node == 4 && unavailable > 1 {
                limits[unavailable - 1] = 1;
            }
            let available_votes = if locks {
                4 - unavailable
            } else {
                4 * drives_per_node - limits.iter().map(|limit| drives_per_node.min(*limit)).sum::<usize>()
            };
            let methods: &[&'static str] = if locks {
                &["Lock", "LockBatch"]
            } else {
                match phase {
                    "create" => &["WriteMetadata"],
                    "part" => &["PreparePartTransaction"],
                    "complete" => &["RenameData", "RenameDataAtIncarnation", "RenameDataWithQuotaFence"],
                    "abort" => &["Delete"],
                    _ => unreachable!(),
                }
            };
            eprintln!("MPU quorum acceptance: phase={phase}, lock_fault={locks}, available_votes={available_votes}");
            if phase == "create" {
                let request = clients[0].create_multipart_upload().bucket(bucket).key(&key);
                let result = with_paused_peers(proxies, methods, &limits, bucket, async move { request.send().await }).await?;
                if let Some(upload) = quorum_outcome(result, unavailable == unavailable_at_quorum, locks)? {
                    abort_after_fault(&clients[0], bucket, &key, upload.upload_id().ok_or("missing upload ID")?, false).await?;
                }
            } else {
                let upload = clients[0]
                    .create_multipart_upload()
                    .bucket(bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|error| format!("setup Create failed for {key}: {error:?}"))?;
                let upload_id = upload.upload_id().ok_or("missing upload ID")?;
                let scope = if !locks && phase == "abort" {
                    upload_path_scope(bucket, &key, upload_id)?
                } else {
                    upload_scope(upload_id)?
                };
                let body = payload_for(&key, if drives_per_node == 4 { 9 * 1024 * 1024 } else { 1024 * 1024 });
                let initial = if phase != "part" {
                    Some(
                        clients[0]
                            .upload_part()
                            .bucket(bucket)
                            .key(&key)
                            .upload_id(upload_id)
                            .part_number(1)
                            .body(ByteStream::from(body.clone()))
                            .send()
                            .await?,
                    )
                } else {
                    None
                };
                match phase {
                    "part" => {
                        let request = clients[0]
                            .upload_part()
                            .bucket(bucket)
                            .key(&key)
                            .upload_id(upload_id)
                            .part_number(1)
                            .body(ByteStream::from(body.clone()));
                        let result =
                            with_paused_peers(proxies, methods, &limits, &scope, async move { request.send().await }).await?;
                        if let Some(part) = quorum_outcome(result, unavailable == unavailable_at_quorum, locks)? {
                            let completion = clients[0]
                                .complete_multipart_upload()
                                .bucket(bucket)
                                .key(&key)
                                .upload_id(upload_id)
                                .multipart_upload(
                                    CompletedMultipartUpload::builder()
                                        .parts(
                                            CompletedPart::builder()
                                                .part_number(1)
                                                .e_tag(part.e_tag().ok_or("missing ETag")?)
                                                .build(),
                                        )
                                        .build(),
                                );
                            recover_after_fault(&format!("Complete {key} after part fault"), || completion.clone().send())
                                .await??;
                            for reader in clients {
                                assert_object_bytes_after_fault(reader, bucket, &key, &body).await?;
                            }
                        }
                    }
                    "complete" => {
                        let part = initial.as_ref().expect("complete fixture has one part");
                        let request = clients[0]
                            .complete_multipart_upload()
                            .bucket(bucket)
                            .key(&key)
                            .upload_id(upload_id)
                            .multipart_upload(
                                CompletedMultipartUpload::builder()
                                    .parts(
                                        CompletedPart::builder()
                                            .part_number(1)
                                            .e_tag(part.e_tag().ok_or("missing ETag")?)
                                            .build(),
                                    )
                                    .build(),
                            );
                        let result =
                            with_paused_peers(proxies, methods, &limits, &scope, async move { request.send().await }).await?;
                        if quorum_outcome(result, unavailable == unavailable_at_quorum, locks)?.is_some() {
                            for reader in clients {
                                assert_object_bytes_after_fault(reader, bucket, &key, &body).await?;
                            }
                        }
                    }
                    "abort" => {
                        let request = clients[0]
                            .abort_multipart_upload()
                            .bucket(bucket)
                            .key(&key)
                            .upload_id(upload_id);
                        let result =
                            with_paused_peers(proxies, methods, &limits, &scope, async move { request.send().await }).await?;
                        quorum_outcome(result, unavailable == unavailable_at_quorum, locks)?;
                    }
                    _ => unreachable!(),
                }
                // Cleanup must recover even after a below-quorum mutation;
                // only an already retired session may return NoSuchUpload.
                abort_after_fault(&clients[0], bucket, &key, upload_id, true).await?;
                recover_after_fault(&format!("DeleteObject {key} after {phase}"), || {
                    clients[0].delete_object().bucket(bucket).key(&key).send()
                })
                .await??;
            }
            wait_for_lock_peer_recovery(proxies, &clients[0], bucket).await?;
            round_trip(clients, bucket, "recovered", 9 * 1024 * 1024).await?;
            assert_no_uploads_after_fault(clients, bucket).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn metadata_delay_preserves_write_quorum_and_rejects_quorum_minus_one() -> TestResult {
    let (_cluster, proxies, clients, bucket) = fixture(1, "EC:2").await?;
    phase_quorum_matrix(&proxies, &clients, &bucket, false, 1).await
}

#[tokio::test]
async fn lock_delay_preserves_admission_quorum_and_reports_timeout() -> TestResult {
    let (_cluster, proxies, clients, bucket) = fixture(1, "EC:2").await?;
    phase_quorum_matrix(&proxies, &clients, &bucket, true, 1).await
}

#[tokio::test]
async fn sixteen_shard_multipart_metadata_requires_twelve_votes() -> TestResult {
    let (cluster, proxies, clients, bucket) = fixture(4, "EC:4").await?;
    let storage = cluster_admin_ok(&cluster, http::Method::GET, "/rustfs/admin/v3/storageinfo", None).await?;
    let storage: serde_json::Value = serde_json::from_str(&storage)?;
    let storage: rustfs_madmin::StorageInfo = serde_json::from_value(storage["info"].clone())?;
    assert_eq!(storage.disks.len(), 16);
    assert_eq!(storage.backend.total_sets, vec![1]);
    assert_eq!(storage.backend.drives_per_set, vec![16]);
    assert_eq!(storage.backend.standard_sc_parity, Some(4));
    phase_quorum_matrix(&proxies, &clients, &bucket, false, 4).await
}

#[tokio::test]
async fn cancelled_metadata_wait_releases_fence_for_bucket_recreation() -> TestResult {
    let (_cluster, proxies, clients, bucket) = fixture(1, "EC:2").await?;
    let pauses: Vec<_> = proxies[1..]
        .iter()
        .map(|proxy| proxy.pause(&["WriteMetadata"], usize::MAX, Some(&bucket)))
        .collect::<Result<_, _>>()?;
    let task = create(&clients[0], &bucket, "cancelled");
    wait_for_faults(&pauses, &["WriteMetadata"]).await?;
    task.abort();
    assert!(task.await.expect_err("cancelled S3 future").is_cancelled());
    drop(pauses);
    // Deletion must wait for admitted server work to finish, then erase its
    // generation. Client cancellation alone does not prove server cancellation.
    tokio::time::timeout(DEADLINE, clients[1].delete_bucket().bucket(&bucket).send()).await??;
    clients[2].create_bucket().bucket(&bucket).send().await?;
    assert_no_uploads(&clients, &bucket).await?;
    round_trip(&clients, &bucket, "new-generation", 9 * 1024 * 1024).await?;
    assert_no_uploads(&clients, &bucket).await
}

/// Same logical geometry as the incident: one pool, four 16-shard sets,
/// EC12+4, 45 MiB files, 8 MiB parts, two files and four parts in flight.
#[tokio::test]
async fn eight_node_eight_drive_multipart_load_leaves_no_residual_uploads() -> TestResult {
    let rounds = std::env::var("RUSTFS_E2E_MPU_LOAD_ROUNDS")
        .unwrap_or_else(|_| "3".into())
        .parse::<usize>()?;
    if !(1..=256).contains(&rounds) {
        return Err("RUSTFS_E2E_MPU_LOAD_ROUNDS must be in 1..=256".into());
    }
    let mut cluster = RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(8, 8)).await?;
    for (name, value) in [
        ("RUSTFS_STORAGE_CLASS_STANDARD", "EC:4"),
        ("TOKIO_WORKER_THREADS", "2"),
        ("RAYON_NUM_THREADS", "2"),
        ("RUSTFS_OBS_METRICS_EXPORT_ENABLED", "false"),
        ("NO_PROXY", "127.0.0.1,::1,localhost"),
        ("HTTP_PROXY", ""),
        ("HTTPS_PROXY", ""),
        ("RUST_LOG", "warn"),
    ] {
        cluster.set_env(name, value);
    }
    configure_node_logs(&mut cluster, None)?;
    cluster.start().await?;
    let clients: Vec<_> = cluster
        .create_all_clients()?
        .into_iter()
        .map(|client| {
            Client::from_conf(
                client
                    .config()
                    .to_builder()
                    .retry_config(RetryConfig::standard().with_max_attempts(1))
                    .build(),
            )
        })
        .collect();
    let storage = cluster_admin_ok(&cluster, http::Method::GET, "/rustfs/admin/v3/storageinfo", None).await?;
    let storage: serde_json::Value = serde_json::from_str(&storage)?;
    let storage: rustfs_madmin::StorageInfo = serde_json::from_value(storage["info"].clone())?;
    assert_eq!(storage.disks.len(), 64);
    assert_eq!(storage.backend.total_sets, vec![4]);
    assert_eq!(storage.backend.drives_per_set, vec![16]);
    assert_eq!(storage.backend.standard_sc_parity, Some(4));
    let bucket = unique_bucket("mpu-8x8");
    cluster.create_test_bucket(&bucket).await?;
    let started = Instant::now();
    for round in 0..rounds {
        let first = format!("round-{round}-first");
        let second = format!("round-{round}-second");
        tokio::try_join!(
            round_trip(&clients, &bucket, &first, 45 * 1024 * 1024),
            round_trip(&clients, &bucket, &second, 45 * 1024 * 1024),
        )?;
        let aborted = clients[round % clients.len()]
            .create_multipart_upload()
            .bucket(&bucket)
            .key("aborted")
            .send()
            .await?;
        let upload_id = aborted.upload_id().ok_or("missing upload ID")?;
        clients[(round + 1) % clients.len()]
            .upload_part()
            .bucket(&bucket)
            .key("aborted")
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0xa5; 8 * 1024 * 1024]))
            .send()
            .await?;
        abort(&clients[(round + 2) % clients.len()], &bucket, "aborted", upload_id).await?;
        assert_no_uploads(&clients, &bucket).await?;
        eprintln!(
            "MPU 8x8 acceptance: round={}/{rounds}, completed={}, aborted={}, elapsed={:?}",
            round + 1,
            2 * (round + 1),
            round + 1,
            started.elapsed()
        );
    }
    Ok(())
}

async fn resume_at_rpc<T: Send + 'static>(
    proxies: &[RpcFaultProxy],
    methods: &[&'static str],
    scope: &str,
    request: impl std::future::Future<Output = TestResult<T>> + Send + 'static,
) -> TestResult<T> {
    let pauses: Vec<_> = proxies[1..]
        .iter()
        .map(|proxy| proxy.pause(methods, usize::MAX, Some(scope)))
        .collect::<Result<_, _>>()?;
    eprintln!("MPU delay acceptance: methods={methods:?}, scope={scope}");
    let task = tokio::spawn(request);
    wait_for_faults(&pauses, methods).await?;
    assert!(
        !task.is_finished(),
        "the selected commit must wait while only one shard/voter is available"
    );
    drop(pauses);
    tokio::time::timeout(DEADLINE, task).await??
}

#[tokio::test]
async fn each_multipart_phase_resumes_after_metadata_and_lock_delays() -> TestResult {
    let (_cluster, proxies, clients, bucket) = fixture(1, "EC:2").await?;
    for locks in [false, true] {
        let key = format!("phase-{locks}");
        let client = clients[0].clone();
        let create = client.create_multipart_upload().bucket(&bucket).key(&key);
        let upload = resume_at_rpc(
            &proxies,
            if locks { &["Lock", "LockBatch"] } else { &["WriteMetadata"] },
            bucket.as_str(),
            async move { Ok(create.send().await?) },
        )
        .await?;
        let upload_id = upload.upload_id().ok_or("missing upload ID")?;
        let scope = upload_scope(upload_id)?;
        let body = payload_for(&key, 1024 * 1024);
        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(body.clone()));
        let part = resume_at_rpc(
            &proxies,
            if locks {
                &["Lock", "LockBatch"]
            } else {
                &["PreparePartTransaction"]
            },
            &scope,
            async move { Ok(part.send().await?) },
        )
        .await?;
        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.e_tag().ok_or("missing ETag")?)
                            .build(),
                    )
                    .build(),
            );
        resume_at_rpc(
            &proxies,
            if locks {
                &["Lock", "LockBatch"]
            } else {
                &["RenameData", "RenameDataAtIncarnation", "RenameDataWithQuotaFence"]
            },
            &scope,
            async move { Ok(complete.send().await?) },
        )
        .await?;
        for reader in &clients {
            assert_object_bytes(reader, &bucket, &key, &body).await?;
        }
        client.delete_object().bucket(&bucket).key(&key).send().await?;
        let upload = client.create_multipart_upload().bucket(&bucket).key(&key).send().await?;
        let upload_id = upload.upload_id().ok_or("missing upload ID")?;
        let scope = upload_scope(upload_id)?;
        let scope = if locks {
            scope
        } else {
            upload_path_scope(&bucket, &key, upload_id)?
        };
        let request = client.abort_multipart_upload().bucket(&bucket).key(&key).upload_id(upload_id);
        resume_at_rpc(&proxies, if locks { &["Lock", "LockBatch"] } else { &["Delete"] }, &scope, async move {
            Ok(request.send().await?)
        })
        .await?;
        let missing = client
            .list_parts()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .send()
            .await
            .expect_err("delayed Abort must remove the upload");
        assert_eq!(missing.as_service_error().and_then(|e| e.meta().code()), Some("NoSuchUpload"));
        assert_no_uploads(&clients, &bucket).await?;
    }
    Ok(())
}
