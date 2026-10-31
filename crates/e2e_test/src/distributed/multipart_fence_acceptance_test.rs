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
use crate::storage_api::multipart_fence_acceptance::{LOCK_DIAGNOSTICS_FILTER, RUSTFS_META_MULTIPART_BUCKET};
use aws_sdk_s3::{
    Client,
    config::retry::RetryConfig,
    error::{ProvideErrorMetadata, SdkError},
    operation::create_multipart_upload::{CreateMultipartUploadError, CreateMultipartUploadOutput},
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const DEADLINE: Duration = Duration::from_secs(45);

async fn fixture(
    drives_per_node: usize,
    storage_class: &str,
    lock_diagnostics: bool,
) -> TestResult<(RustFSTestClusterEnvironment, Vec<RpcFaultProxy>, Vec<Client>, String)> {
    fixture_from_binary(drives_per_node, storage_class, None, lock_diagnostics).await
}

async fn fixture_from_binary(
    drives_per_node: usize,
    storage_class: &str,
    binary: Option<&std::path::Path>,
    lock_diagnostics: bool,
) -> TestResult<(RustFSTestClusterEnvironment, Vec<RpcFaultProxy>, Vec<Client>, String)> {
    let mut cluster =
        RustFSTestClusterEnvironment::with_topology(ClusterTopology::single_pool_multidrive(4, drives_per_node)).await?;
    if binary.is_some() {
        cluster.set_env("TOKIO_WORKER_THREADS", "2");
        cluster.set_env("RAYON_NUM_THREADS", "2");
        // The pinned release passes host_str() directly to TCP resolution,
        // which rejects bracketed IPv6. A loopback hostname retains direct
        // IPv4 S3 listeners and the same signed RPC port. Install the IPv6
        // observers only after the genuine previous-release upload is seeded.
        let mut volumes = Vec::new();
        for node in &cluster.nodes {
            let address: std::net::SocketAddr = node.address.parse()?;
            volumes.extend(
                node.data_dirs
                    .iter()
                    .map(|root| format!("http://localhost:{}{root}", address.port())),
            );
        }
        cluster.set_env("RUSTFS_VOLUMES", volumes.join(" "));
    }
    let fault_budgets = [
        ("RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT", "5"),
        ("RUSTFS_OBJECT_LOCK_RPC_TIMEOUT_MS", "3000"),
        ("RUSTFS_DRIVE_METADATA_TIMEOUT_SECS", "2"),
        ("RUSTFS_DRIVE_MAX_TIMEOUT_DURATION", "5"),
    ];
    if binary.is_none() {
        for (name, value) in fault_budgets {
            cluster.set_env(name, value);
        }
    }
    for (name, value) in [
        ("RUSTFS_STORAGE_CLASS_STANDARD", storage_class),
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
    if lock_diagnostics {
        cluster.set_env("RUST_LOG", LOCK_DIAGNOSTICS_FILTER);
        cluster.set_env("RUSTFS_OBJECT_LOCK_DIAG_ENABLE", "true");
    }
    let mut proxies = Vec::new();
    if binary.is_none() {
        for node in 0..4 {
            proxies.push(cluster.start_rpc_proxy_for_node(node).await?);
        }
    }
    configure_node_logs(&mut cluster, None)?;
    if let Some(binary) = binary {
        // Bootstrap the old fleet with its normal budgets. Subsequent current
        // replacements still receive the short method-fault budgets above.
        cluster.start_with_binary(binary).await?;
        for (name, value) in fault_budgets {
            cluster.set_env(name, value);
        }
    } else {
        cluster.start().await?;
    }
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
                        Some(
                            "namespace_lock_timeout"
                                | "namespace_lock_unavailable"
                                | "storage_write_quorum"
                                | "storage_read_quorum"
                        )
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

async fn with_paused_peers<T, E>(
    proxies: &[RpcFaultProxy],
    methods: &[&'static str],
    limits: &[usize],
    scope: &str,
    request: impl std::future::Future<Output = Result<T, SdkError<E>>> + Send + 'static,
) -> TestResult<Result<T, SdkError<E>>>
where
    T: Send + 'static,
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    let deadline = tokio::time::Instant::now() + DEADLINE;
    let pauses: Vec<_> = proxies[1..=limits.len()]
        .iter()
        .zip(limits)
        .map(|(proxy, limit)| proxy.pause(methods, *limit, Some(scope)))
        .collect::<Result<_, _>>()?;
    let mut task = tokio::spawn(request);
    let mut joined = None;
    let mut deadline_expired = false;
    let mut barrier_result = {
        let faults = wait_for_faults(&pauses, methods);
        tokio::pin!(faults);
        match tokio::time::timeout_at(deadline, async {
            tokio::select! {
                result = &mut faults => result,
                result = &mut task => {
                    joined = Some(result);
                    // A ready request is not evidence that every selected fault
                    // was reached. Poll the original barrier once for that proof.
                    match futures::poll!(faults.as_mut()) {
                        std::task::Poll::Ready(result) => result,
                        std::task::Poll::Pending => Err("SDK request completed before the selected RPC fault barrier".into()),
                    }
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(error) => {
                deadline_expired = true;
                Err(std::io::Error::new(std::io::ErrorKind::TimedOut, error).into())
            }
        }
    };
    // Tokio's timeout polls an already-ready inner future before its timer.
    // A late observation cannot certify the original absolute deadline.
    deadline_expired |= tokio::time::Instant::now() >= deadline;
    if deadline_expired {
        barrier_result = Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "selected RPC fault proof observed after the absolute MPU deadline",
        )
        .into());
    }
    if barrier_result.is_err() {
        for pause in &pauses {
            pause.discard_pending();
        }
    }
    let mut client_task_aborted = false;
    let request_result = match joined {
        Some(result) => result,
        None => match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(result) => result,
            Err(error) => {
                deadline_expired = true;
                for pause in &pauses {
                    pause.discard_pending();
                }
                barrier_result = Err(std::io::Error::new(std::io::ErrorKind::TimedOut, error).into());
                // This owns only the test's SDK request, not server/native work.
                // Retire the client task without granting another operation budget.
                client_task_aborted = true;
                task.abort();
                task.await
            }
        },
    };
    deadline_expired |= tokio::time::Instant::now() >= deadline;
    if deadline_expired {
        barrier_result = Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "SDK request result observed after the absolute MPU deadline",
        )
        .into());
    }
    let hits: Vec<_> = pauses.iter().map(RpcPause::hits).collect();
    for pause in &pauses {
        pause.discard_pending();
    }
    drop(pauses);
    if barrier_result.is_err() {
        let sdk_error = request_result.as_ref().ok().and_then(|result| result.as_ref().err());
        let response = sdk_error.and_then(SdkError::raw_response);
        eprintln!(
            "MPU fault barrier failed: methods={methods:?}, hits={hits:?}, client_task_aborted={client_task_aborted}, deadline_expired={deadline_expired}, sdk_code={:?}, cause={:?}, request_id={:?}",
            sdk_error
                .and_then(|error| error.as_service_error())
                .and_then(|error| error.meta().code()),
            response.and_then(|response| response.headers().get("x-rustfs-error-cause")),
            response.and_then(|response| response.headers().get("x-amz-request-id")),
        );
    }
    match (barrier_result, request_result) {
        // Preserve the actual SDK source instead of replacing an early service
        // failure with an unrelated fault-wait timeout. The outer Err stays red.
        (Err(_), Ok(Err(error))) => Err(Box::new(error)),
        (Err(_), Err(error)) if !client_task_aborted => Err(Box::new(error)),
        (Err(error), _) => Err(error),
        (Ok(()), Ok(result)) => Ok(result),
        (Ok(()), Err(error)) => Err(Box::new(error)),
    }
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

async fn wait_for_storage_peer_recovery(
    proxies: &[RpcFaultProxy],
    client: &Client,
    bucket: &str,
    upload: &str,
    drives_per_node: usize,
) -> TestResult {
    let scope = upload_scope(upload)?;
    tokio::time::timeout(
        DEADLINE,
        wait_until(
            DEADLINE,
            || async {
                let observers = proxies[1..]
                    .iter()
                    .map(|proxy| proxy.pause(&["ReadVersion"], usize::MAX, Some(&scope)))
                    .collect::<Result<Vec<_>, _>>()?;
                for observer in &observers {
                    observer.release();
                }
                // A fresh absent key forces a full metadata read without mutating
                // this upload. Count distinct configured disk routes, not retries
                // from the same healthy disk or just lock-RPC reachability.
                let key = format!("storage-ready-{}", uuid::Uuid::new_v4());
                let _ = client.list_parts().bucket(bucket).key(key).upload_id(upload).send().await;
                Ok(observers
                    .iter()
                    .all(|observer| observer.forwarded_routes() >= drives_per_node))
            },
            "all storage disk routes recover after fault retirement",
        ),
    )
    .await
    .map_err(|_| "storage route recovery deadline exceeded".to_string())?
}

#[derive(Eq, PartialEq)]
struct CreateSource {
    participant: (usize, usize),
    directories: BTreeSet<PathBuf>,
    files: BTreeMap<PathBuf, Vec<u8>>,
}

type CreateSources = BTreeMap<PathBuf, CreateSource>;
type ListedUploads = BTreeSet<(String, String)>;

struct RetainedCreate {
    key: String,
    sources: CreateSources,
    listing: Option<ListedUploads>,
}

fn multipart_sources(cluster: &RustFSTestClusterEnvironment, only_hash: Option<&str>) -> TestResult<CreateSources> {
    let mut sources = CreateSources::new();
    let mut volumes = BTreeSet::new();
    for (node, participant) in cluster.nodes.iter().enumerate() {
        for (drive, volume) in participant.data_dirs.iter().enumerate() {
            let volume = std::fs::canonicalize(volume)?;
            assert!(volumes.insert(volume.clone()), "a configured volume alias cannot be another participant");
            let multipart_root = volume.join(RUSTFS_META_MULTIPART_BUCKET);
            let hash_roots = if let Some(hash) = only_hash {
                vec![multipart_root.join(hash)]
            } else {
                match std::fs::read_dir(&multipart_root) {
                    Ok(entries) => entries
                        .map(|entry| {
                            let entry = entry?;
                            assert!(entry.file_type()?.is_dir(), "fixture MPI contains an unexpected non-SHA artifact");
                            let name = entry.file_name();
                            let name = name.to_str().ok_or("fixture SHA directory is not UTF-8")?;
                            assert!(name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit()));
                            Ok(entry.path())
                        })
                        .collect::<TestResult<Vec<_>>>()?,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                    Err(error) => return Err(error.into()),
                }
            };
            for hash_root in hash_roots {
                let entries = match std::fs::read_dir(hash_root) {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error.into()),
                };
                for entry in entries {
                    let entry = entry?;
                    assert!(entry.file_type()?.is_dir(), "fixture SHA contains an unexpected non-upload artifact");
                    let root = entry.path();
                    let mut source = CreateSource {
                        participant: (node, drive),
                        directories: BTreeSet::new(),
                        files: BTreeMap::new(),
                    };
                    for child in walkdir::WalkDir::new(&root).sort_by_file_name() {
                        let child = child?;
                        let relative = child.path().strip_prefix(&root)?.to_owned();
                        if child.file_type().is_dir() {
                            source.directories.insert(relative);
                        } else {
                            assert!(child.file_type().is_file(), "fixture source contains a non-regular artifact");
                            source.files.insert(relative, std::fs::read(child.path())?);
                        }
                    }
                    assert!(sources.insert(root, source).is_none());
                }
            }
        }
    }
    Ok(sources)
}

fn create_sources(cluster: &RustFSTestClusterEnvironment, bucket: &str, key: &str) -> TestResult<CreateSources> {
    multipart_sources(cluster, Some(&sha256_hex(format!("{bucket}/{key}").as_bytes())))
}

fn source_heads(sources: &CreateSources, raw_upload: Option<&str>) -> BTreeSet<(usize, usize)> {
    let mut heads = BTreeSet::new();
    for (root, source) in sources {
        if raw_upload.is_none_or(|raw| root.file_name().and_then(|name| name.to_str()) == Some(raw))
            && source.files.contains_key(Path::new("xl.meta"))
        {
            assert!(
                heads.insert(source.participant),
                "one physical drive cannot provide duplicate source heads"
            );
        }
    }
    heads
}

fn unchanged_create_sources(
    cluster: &RustFSTestClusterEnvironment,
    bucket: &str,
    key: &str,
    expected: &CreateSources,
) -> TestResult {
    assert!(
        create_sources(cluster, bucket, key)? == *expected,
        "native Abort changed original bytes, directories or a missing physical participant"
    );
    Ok(())
}

async fn no_created_target(clients: &[Client], bucket: &str, key: &str) -> TestResult {
    for client in clients {
        let error = recover_after_fault(&format!("GetObject absent Create target {key}"), || {
            client.get_object().bucket(bucket).key(key).send()
        })
        .await?
        .expect_err("Create or a rejected Abort cannot publish a target");
        assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(404));
        assert_eq!(error.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));
    }
    Ok(())
}

async fn create_listing(
    client: &Client,
    bucket: &str,
    prefix: &str,
    evidenced_sub_read_quorum: bool,
) -> TestResult<Option<ListedUploads>> {
    let output = recover_after_fault("ListMultipartUploads with actual source evidence", || async {
        match client.list_multipart_uploads().bucket(bucket).prefix(prefix).send().await {
            Err(error)
                if evidenced_sub_read_quorum
                    && error.raw_response().is_some_and(|response| {
                        response.status().as_u16() == 503
                            && response.headers().get("x-rustfs-error-cause") == Some("storage_read_quorum")
                    }) =>
            {
                assert_unavailable(&error, "storage_read_quorum", None);
                // This is an explicit unavailable observation, never an empty
                // successful listing. Other classified faults keep the old budget.
                Ok(None)
            }
            result => result.map(|output| Some(Box::new(output))),
        }
    })
    .await??;
    let Some(output) = output else { return Ok(None) };
    assert_ne!(output.is_truncated(), Some(true));
    let mut uploads = ListedUploads::new();
    for upload in output.uploads() {
        let key = upload.key().ok_or("actual listed upload omitted key")?;
        let id = upload.upload_id().ok_or("actual listed upload omitted ID")?;
        assert!(uploads.insert((key.to_owned(), id.to_owned())), "listing duplicated an upload");
    }
    Ok(Some(uploads))
}

async fn verify_create_after_fault(
    cluster: &RustFSTestClusterEnvironment,
    clients: &[Client],
    bucket: &str,
    key: &str,
    acknowledged: Option<&CreateMultipartUploadOutput>,
    write_quorum: usize,
) -> TestResult<Option<RetainedCreate>> {
    let participants: usize = cluster.nodes.iter().map(|node| node.data_dirs.len()).sum();
    let sources = create_sources(cluster, bucket, key)?;
    let heads = source_heads(&sources, None);
    no_created_target(clients, bucket, key).await?;
    if let Some(created) = acknowledged {
        let upload = created.upload_id().ok_or("acknowledged Create omitted upload ID")?;
        let raw = upload_scope(upload)?;
        assert!(
            sources
                .keys()
                .all(|root| root.file_name().and_then(|name| name.to_str()) == Some(raw.as_str())),
            "isolated acknowledged Create must bind its one actual upload"
        );
        assert!(
            heads.len() >= write_quorum && heads.len() <= participants,
            "acknowledged Create must have real source write quorum, independent of proxy vote counters"
        );
        if heads.len() == participants {
            abort_after_fault(&clients[0], bucket, key, upload, false).await?;
            no_created_target(clients, bucket, key).await?;
            return Ok(None);
        }
        let error = clients[0]
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload)
            .send()
            .await
            .expect_err("transport recovery cannot create missing all-N Abort witnesses");
        assert_unavailable(&error, "storage_write_quorum", None);
        unchanged_create_sources(cluster, bucket, key, &sources)?;
        no_created_target(clients, bucket, key).await?;
        let body = payload_for(key, if participants == 16 { 9 * 1024 * 1024 } else { 1024 * 1024 });
        let part = recover_after_fault(&format!("UploadPart same admitted Create {key}"), || {
            clients[0]
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload)
                .part_number(1)
                .body(ByteStream::from(body.clone()))
                .send()
        })
        .await??;
        assert_eq!(
            source_heads(&create_sources(cluster, bucket, key)?, Some(&raw)),
            heads,
            "Part must not initialize a missing original participant"
        );
        let complete = clients[0]
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.e_tag().ok_or("same-upload part omitted ETag")?)
                            .build(),
                    )
                    .build(),
            );
        let completed =
            recover_after_fault(&format!("Complete same admitted Create {key}"), || complete.clone().send()).await??;
        let etag = completed
            .e_tag()
            .filter(|etag| !etag.is_empty())
            .ok_or("same-upload completion omitted a nonempty ETag")?;
        for client in clients {
            let output = recover_after_fault(&format!("GetObject settled Create {key}"), || {
                client
                    .get_object()
                    .bucket(bucket)
                    .key(key)
                    .set_version_id(completed.version_id().map(str::to_owned))
                    .send()
            })
            .await??;
            assert_eq!(output.e_tag(), Some(etag));
            if let Some(version) = completed.version_id() {
                assert_eq!(output.version_id(), Some(version));
            }
            assert_eq!(
                output.body.collect().await?.into_bytes().as_ref(),
                body.as_slice(),
                "same admitted upload must retain all acknowledged bytes"
            );
        }
        let missing = clients[0]
            .list_parts()
            .bucket(bucket)
            .key(key)
            .upload_id(upload)
            .send()
            .await
            .expect_err("successful same-upload completion must retire its upload");
        assert_eq!(missing.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchUpload"));
        recover_after_fault(&format!("DeleteObject settled Create {key}"), || {
            clients[0].delete_object().bucket(bucket).key(key).send()
        })
        .await??;
        no_created_target(clients, bucket, key).await?;
        return Ok(None);
    }
    assert!(
        heads.len() < write_quorum,
        "unacknowledged Create cannot be treated as source write quorum"
    );
    let read_quorum = if participants == 16 { 12 } else { 2 };
    let sub_read_quorum = heads.len() >= participants / 2 && heads.len() < read_quorum;
    let listing = create_listing(&clients[0], bucket, key, sub_read_quorum).await?;
    assert!(
        !sub_read_quorum || listing.is_none(),
        "physically qualified sub-read-quorum originals must return typed 503, never a swallowed 200 listing"
    );
    if let Some(uploads) = &listing {
        for (listed_key, upload) in uploads {
            assert_eq!(listed_key, key);
            let raw = upload_scope(upload)?;
            assert!(
                sources
                    .keys()
                    .any(|root| root.file_name().and_then(|name| name.to_str()) == Some(raw.as_str())),
                "only an ID actually listed for real source bytes can be probed"
            );
            let error = clients[0]
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload)
                .send()
                .await
                .expect_err("sub-Q originals cannot authorize native Abort");
            assert_unavailable(&error, "storage_write_quorum", None);
        }
    }
    unchanged_create_sources(cluster, bucket, key, &sources)?;
    // A fresh admission owns a distinct original; it is never a replacement
    // witness or a fabricated ID for the failed producer.
    let fresh = recover_after_fault(&format!("fresh Create beside retained orphan {key}"), || {
        clients[0].create_multipart_upload().bucket(bucket).key(key).send()
    })
    .await??;
    let fresh_id = fresh.upload_id().ok_or("fresh admission omitted upload ID")?;
    let fresh_raw = upload_scope(fresh_id)?;
    assert!(
        !sources
            .keys()
            .any(|root| root.file_name().and_then(|name| name.to_str()) == Some(fresh_raw.as_str()))
    );
    assert_eq!(
        source_heads(&create_sources(cluster, bucket, key)?, Some(&fresh_raw)).len(),
        participants,
        "fresh native Abort must use all actual configured disks, not one vote per node"
    );
    abort_after_fault(&clients[0], bucket, key, fresh_id, false).await?;
    unchanged_create_sources(cluster, bucket, key, &sources)?;
    assert_eq!(
        create_listing(&clients[0], bucket, key, sub_read_quorum).await?,
        listing,
        "fresh cleanup must preserve the exact retained visibility or explicit read-quorum unavailability"
    );
    no_created_target(clients, bucket, key).await?;
    eprintln!(
        "MPU retained failed Create: key={key}, actual_heads={}, configured_disks={participants}, write_quorum={write_quorum}, listing={}",
        heads.len(),
        if listing.is_none() {
            "storage_read_quorum_503"
        } else {
            "exact_actual_ids"
        }
    );
    Ok(Some(RetainedCreate {
        key: key.to_owned(),
        sources,
        listing,
    }))
}

async fn verify_retained_creates(
    cluster: &RustFSTestClusterEnvironment,
    clients: &[Client],
    bucket: &str,
    retained: &[RetainedCreate],
) -> TestResult {
    let mut expected = ListedUploads::new();
    let mut unavailable = false;
    for upload in retained {
        unchanged_create_sources(cluster, bucket, &upload.key, &upload.sources)?;
        match &upload.listing {
            Some(listed) => expected.extend(listed.iter().cloned()),
            None => unavailable = true,
        }
    }
    // A set-wide listing 503 could hide unrelated leaks. Compare the complete
    // fixture MPI (every disk/hash/upload), including headless control roots.
    // No completed/control-root exception is accepted; empty SHA parents alone
    // are harmless. This physical gate is independent of S3 listing success.
    let expected_roots: BTreeSet<_> = retained.iter().flat_map(|upload| upload.sources.keys().cloned()).collect();
    wait_until(
        DEADLINE,
        || async {
            let actual = match multipart_sources(cluster, None) {
                Ok(actual) => actual,
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                        || error
                            .downcast_ref::<walkdir::Error>()
                            .and_then(walkdir::Error::io_error)
                            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };
            let roots: BTreeSet<_> = actual.keys().cloned().collect();
            Ok(roots == expected_roots
                && retained.iter().all(|upload| {
                    upload
                        .sources
                        .iter()
                        .all(|(root, expected)| actual.get(root) == Some(expected))
                }))
        },
        "all configured MPI hashes and roots contain only exact retained failed-Create bytes",
    )
    .await?;
    if retained.is_empty() {
        return assert_no_uploads_after_fault(clients, bucket).await;
    }
    for client in clients {
        let listed = create_listing(client, bucket, "", unavailable).await?;
        if unavailable {
            assert!(
                listed.is_none(),
                "the evidenced sub-read-Q orphan must remain explicitly indeterminate, not empty/successful"
            );
        } else {
            assert_eq!(
                listed,
                Some(expected.clone()),
                "only byte-verified failed-Create uploads may remain listed"
            );
        }
    }
    Ok(())
}

async fn phase_quorum_matrix(
    cluster: &RustFSTestClusterEnvironment,
    proxies: &[RpcFaultProxy],
    clients: &[Client],
    bucket: &str,
    locks: bool,
    drives_per_node: usize,
    write_quorum: usize,
) -> TestResult {
    let mut retained_creates = Vec::new();
    let mut route_probe_upload = None;
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
                    "complete" => &[
                        "RenameData",
                        "RenameMultipartData",
                        "RenameDataAtIncarnation",
                        "RenameDataWithQuotaFence",
                    ],
                    // The proxy strictly decodes RecoverMultipartSource and
                    // matches only DeleteAborted at the exact root below.
                    // Inspect/Prepare/Commit pass before this all-N-closed cut.
                    "abort" => &["Delete", "RecoverMultipartSource"],
                    _ => unreachable!(),
                }
            };
            eprintln!("MPU quorum acceptance: phase={phase}, lock_fault={locks}, available_votes={available_votes}");
            if phase == "create" {
                let request = clients[0].create_multipart_upload().bucket(bucket).key(&key);
                let result = with_paused_peers(proxies, methods, &limits, bucket, async move { request.send().await }).await?;
                let acknowledged = quorum_outcome(result, unavailable == unavailable_at_quorum, locks)?;
                if let Some(upload) = &acknowledged {
                    route_probe_upload = Some(upload.upload_id().ok_or("missing upload ID")?.to_owned());
                }
                // The positive row always precedes its failed row. Reuse its
                // genuine ID only for the existing read-only absent-key probe,
                // never to authorize an unknown orphan or supply a witness.
                wait_for_storage_peer_recovery(
                    proxies,
                    &clients[0],
                    bucket,
                    route_probe_upload
                        .as_deref()
                        .ok_or("matrix lacks its genuine route-probe upload ID")?,
                    drives_per_node,
                )
                .await?;
                if let Some(retained) = Box::pin(verify_create_after_fault(
                    cluster,
                    clients,
                    bucket,
                    &key,
                    acknowledged.as_ref(),
                    write_quorum,
                ))
                .await?
                {
                    retained_creates.push(retained);
                }
            } else {
                let committed = if !locks && phase == "abort" {
                    let previous = payload_for(&format!("{key}-committed"), 4096);
                    clients[0]
                        .put_object()
                        .bucket(bucket)
                        .key(&key)
                        .body(ByteStream::from(previous.clone()))
                        .send()
                        .await?;
                    Some(previous)
                } else {
                    None
                };
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
                let mut abort_roots = Vec::new();
                if committed.is_some() {
                    for directory in cluster.nodes.iter().flat_map(|node| &node.data_dirs) {
                        let root = Path::new(directory).join(RUSTFS_META_MULTIPART_BUCKET).join(&scope);
                        assert!(root.join("xl.meta").try_exists()?, "each native participant must have its original head");
                        abort_roots.push(root);
                    }
                    assert_eq!(abort_roots.len(), 4 * drives_per_node);
                }
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
                wait_for_storage_peer_recovery(proxies, &clients[0], bucket, upload_id, drives_per_node).await?;
                // A rejected Complete must retain authority for ordinary
                // Abort. NoSuchUpload must not hide partial staging loss.
                let allow_missing = phase != "complete" || unavailable == unavailable_at_quorum;
                if !abort_roots.is_empty() {
                    let survivors = abort_roots.iter().filter(|root| root.exists()).count();
                    assert_eq!(
                        survivors,
                        abort_roots.len() - available_votes,
                        "the scoped DeleteAborted fault must be real"
                    );
                    // Every native participant closed and persisted the full-N
                    // certificate before this exact DeleteAborted fault. The
                    // surviving roots have retired their ACTIVE heads; their
                    // certificate authorizes retry below metadata read quorum.
                    // Uncertified legacy authority has a separate regression.
                    for root in abort_roots.iter().filter(|root| root.exists()) {
                        assert!(!root.join("xl.meta").try_exists()?, "certified Abort must retire each surviving head");
                    }
                    abort_after_fault(&clients[0], bucket, &key, upload_id, false).await?;
                    assert!(abort_roots.iter().all(|root| !root.exists()));
                    for reader in clients {
                        assert_object_bytes(reader, bucket, &key, committed.as_ref().expect("Abort fixture has an old object"))
                            .await?;
                    }
                    eprintln!(
                        "MPU native Abort boundary: closure_members={}, deleted={available_votes}, survivors={survivors}, retry_cleanup=true",
                        abort_roots.len()
                    );
                } else {
                    abort_after_fault(&clients[0], bucket, &key, upload_id, allow_missing).await?;
                }
                recover_after_fault(&format!("DeleteObject {key} after {phase}"), || {
                    clients[0].delete_object().bucket(bucket).key(&key).send()
                })
                .await??;
            }
            wait_for_lock_peer_recovery(proxies, &clients[0], bucket).await?;
            round_trip(clients, bucket, "recovered", 9 * 1024 * 1024).await?;
            verify_retained_creates(cluster, clients, bucket, &retained_creates).await?;
        }
    }
    Ok(())
}

async fn list_parts_disk_loss_after_metadata_read(
    cluster: &RustFSTestClusterEnvironment,
    proxies: &[RpcFaultProxy],
    clients: &[Client],
    bucket: &str,
) -> TestResult {
    let key = "list-parts-after-metadata-loss";
    let body = payload_for("list-parts-acknowledged", 4096);
    let upload = clients[0].create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = upload.upload_id().ok_or("missing upload ID")?.to_owned();
    let part = clients[0]
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    let etag = part.e_tag().ok_or("missing part ETag")?.to_owned();
    let path = upload_path_scope(bucket, key, &upload_id)?;
    let root = Path::new(&cluster.nodes[0].data_dirs[0])
        .join(RUSTFS_META_MULTIPART_BUCKET)
        .join(&path);
    let data_dirs = std::fs::read_dir(&root)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|entry| entry.join("part.1").is_file() && entry.join("part.1.meta").is_file())
        .collect::<Vec<_>>();
    assert_eq!(data_dirs.len(), 1, "the coordinator's actual part directory must be identified");
    let data_dir = &data_dirs[0];
    let dir_name = data_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("non-UTF8 data directory")?;
    let scoped_path = format!("{path}/{dir_name}");
    let held_dir = data_dir.with_extension("e2e-held");
    std::fs::rename(data_dir, &held_dir)?;
    let pauses = proxies[1..]
        .iter()
        .map(|proxy| proxy.pause(&["ListDir"], usize::MAX, Some(&scoped_path)))
        .collect::<Result<Vec<_>, _>>()?;
    let request = clients[0].list_parts().bucket(bucket).key(key).upload_id(&upload_id);
    let task = tokio::spawn(async move { request.send().await });
    // ListDir is dispatched only after the upload metadata read succeeded.
    // Require all three scoped remote requests; unrelated failures do not count.
    wait_for_faults(&pauses, &["ListDir"]).await?;
    let result = tokio::time::timeout(DEADLINE, task).await??;
    for pause in &pauses {
        pause.discard_pending();
    }
    drop(pauses);
    std::fs::rename(&held_dir, data_dir)?;
    let error = result.expect_err("one missing directory and three unavailable participants cannot prove an empty upload");
    let response = error.raw_response().expect("ListParts must return an S3 service response");
    assert_eq!(response.status().as_u16(), 503);
    assert_eq!(error.as_service_error().and_then(|err| err.meta().code()), Some("SlowDownRead"));
    assert!(response.headers().get("x-rustfs-error-cause").is_none());
    assert!(response.headers().get("retry-after").is_none());
    assert!(
        response
            .headers()
            .get("content-type")
            .is_some_and(|value| value.starts_with("application/xml"))
    );
    assert!(
        response
            .headers()
            .get("x-amz-request-id")
            .is_some_and(|value| !value.is_empty())
    );
    let listed = recover_after_fault("ListParts after directory restoration", || {
        clients[0].list_parts().bucket(bucket).key(key).upload_id(&upload_id).send()
    })
    .await??;
    assert_eq!(listed.parts().len(), 1);
    assert_eq!(listed.parts()[0].e_tag(), Some(etag.as_str()));
    clients[0]
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(CompletedPart::builder().part_number(1).e_tag(&etag).build())
                .build(),
        )
        .send()
        .await?;
    for client in clients {
        assert_object_bytes_after_fault(client, bucket, key, &body).await?;
    }
    eprintln!(
        "MPU list proof: metadata read completed, three scoped ListDir RPCs held, missing local directory rejected; restored part and all readers verified"
    );
    Ok(())
}

async fn concurrent_capped_parts_respect_limit_on_distinct_nodes(clients: &[Client], bucket: &str) -> TestResult {
    let key = "concurrent-capped-parts";
    let upload = clients[0]
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .customize()
        .mutate_request(|request| {
            let separator = if request.uri().contains('?') { "&" } else { "?" };
            let uri = format!("{}{separator}x-rustfs-max-total-object-size=10", request.uri());
            request
                .set_uri(uri)
                .expect("the signed multipart size query must be a valid URI");
        })
        .send()
        .await?;
    let upload_id = upload.upload_id().ok_or("missing capped upload ID")?.to_owned();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for (index, number, byte) in [(0_usize, 1_i32, 0x41_u8), (1, 2, 0x42)] {
        let client = clients[index].clone();
        let bucket = bucket.to_owned();
        let upload_id = upload_id.clone();
        let barrier = std::sync::Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let result = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(number)
                .body(ByteStream::from(vec![byte; 6]))
                .send()
                .await;
            (number, byte, result)
        }));
    }
    barrier.wait().await;
    let mut winner = None;
    let mut rejected = 0;
    for task in tasks {
        let (number, byte, result) = tokio::time::timeout(DEADLINE, task).await??;
        match result {
            Ok(part) => {
                assert!(winner.is_none(), "both six-byte parts cannot fit the ten-byte limit");
                winner = Some((number, byte, part.e_tag().ok_or("missing accepted ETag")?.to_owned()));
            }
            Err(error) => {
                assert_eq!(
                    error.as_service_error().and_then(|err| err.meta().code()),
                    Some("EntityTooLarge"),
                    "{error:?}"
                );
                rejected += 1;
            }
        }
    }
    let (number, byte, etag) = winner.ok_or("no part was admitted")?;
    assert_eq!(rejected, 1);
    let listed = clients[0]
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await?;
    assert_eq!(listed.parts().len(), 1);
    assert_eq!(listed.parts()[0].part_number(), Some(number));
    assert_eq!(listed.parts()[0].e_tag(), Some(etag.as_str()));
    clients[0]
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(CompletedPart::builder().part_number(number).e_tag(&etag).build())
                .build(),
        )
        .send()
        .await?;
    for client in clients {
        assert_object_bytes_after_fault(client, bucket, key, &[byte; 6]).await?;
    }
    eprintln!("MPU size limit: two coordinators raced; exactly one six-byte part admitted under ten-byte limit");
    Ok(())
}

#[tokio::test]
async fn metadata_delay_preserves_write_quorum_and_rejects_quorum_minus_one() -> TestResult {
    let (cluster, proxies, clients, bucket) = fixture(1, "EC:2", true).await?;
    list_parts_disk_loss_after_metadata_read(&cluster, &proxies, &clients, &bucket).await?;
    concurrent_capped_parts_respect_limit_on_distinct_nodes(&clients, &bucket).await?;
    phase_quorum_matrix(&cluster, &proxies, &clients, &bucket, false, 1, 3).await
}

#[tokio::test]
async fn lock_delay_preserves_admission_quorum_and_reports_timeout() -> TestResult {
    let (cluster, proxies, clients, bucket) = fixture(1, "EC:2", false).await?;
    phase_quorum_matrix(&cluster, &proxies, &clients, &bucket, true, 1, 3).await
}

#[tokio::test]
async fn sixteen_shard_multipart_metadata_requires_twelve_votes() -> TestResult {
    let (cluster, proxies, clients, bucket) = fixture(4, "EC:4", false).await?;
    let storage = cluster_admin_ok(&cluster, http::Method::GET, "/rustfs/admin/v3/storageinfo", None).await?;
    let storage: serde_json::Value = serde_json::from_str(&storage)?;
    let storage: rustfs_madmin::StorageInfo = serde_json::from_value(storage["info"].clone())?;
    assert_eq!(storage.disks.len(), 16);
    assert_eq!(storage.backend.total_sets, vec![1]);
    assert_eq!(storage.backend.drives_per_set, vec![16]);
    assert_eq!(storage.backend.standard_sc_parity, Some(4));
    phase_quorum_matrix(&cluster, &proxies, &clients, &bucket, false, 4, 12).await
}

#[tokio::test]
async fn mixed_version_failed_complete_requires_current_coordinator_for_abort() -> TestResult {
    let previous = super::upgrade_test::source_binary()?;
    let (mut cluster, mut proxies, clients, bucket) = fixture_from_binary(4, "EC:4", Some(&previous), false).await?;
    assert!(
        proxies.is_empty(),
        "the genuine previous fleet boots and seeds through its normal direct transport"
    );
    let key = "mixed-failed-complete";
    let unrelated = payload_for("historical-unrelated", 4096);
    clients[0]
        .put_object()
        .bucket(&bucket)
        .key("unrelated")
        .body(ByteStream::from(unrelated.clone()))
        .send()
        .await?;
    let upload = clients[0].create_multipart_upload().bucket(&bucket).key(key).send().await?;
    let upload_id = upload.upload_id().ok_or("missing legacy upload ID")?;
    let part = clients[0]
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(vec![0x62; 9 * 1024 * 1024]))
        .send()
        .await?;
    let path = upload_path_scope(&bucket, key, upload_id)?;
    let roots = cluster
        .nodes
        .iter()
        .flat_map(|node| &node.data_dirs)
        .map(|root| Path::new(root).join(RUSTFS_META_MULTIPART_BUCKET).join(&path))
        .collect::<Vec<_>>();
    assert_eq!(roots.len(), 16);
    wait_until(
        DEADLINE,
        || async {
            let sources = create_sources(&cluster, &bucket, key)?;
            Ok(sources.len() == 16
                && sources.values().all(|source| {
                    source.files.contains_key(Path::new("xl.meta"))
                        && source.files.keys().any(|path| path.ends_with("part.1"))
                        && source.files.keys().any(|path| path.ends_with("part.1.meta"))
                }))
        },
        "all sixteen previous-release source heads, part metadata and shards finish before rollout",
    )
    .await?;
    let original = create_sources(&cluster, &bucket, key)?;
    // Keep every old source byte and the advertised localhost authority.
    // Only the new coordinator's fresh channels enter the method observers.
    for node in 0..cluster.nodes.len() {
        proxies.push(cluster.start_rpc_proxy_for_node(node).await?);
    }
    cluster.stop_node(0)?;
    cluster.start_node(0).await?;
    crate::upgrade_compatibility_test::wait_for_upgrade_write_readiness(&clients, &bucket, "mixed-mpu-current-writer", DEADLINE)
        .await?;
    // Read-only ListParts remains available during the documented mutation pause.
    // Compare the actual old and current coordinators on the same legacy upload.
    for (version, client) in [("current", &clients[0]), ("previous", &clients[1])] {
        let listed = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await?;
        assert_eq!(listed.parts().len(), 1, "{version} coordinator must list the legacy part");
        assert_eq!(listed.parts()[0].part_number(), Some(1));
        assert_eq!(listed.parts()[0].size(), Some(9 * 1024 * 1024));
        assert_eq!(listed.parts()[0].e_tag(), part.e_tag());
    }
    eprintln!("MPU mixed version: old and current coordinators agree on legacy ListParts identity and size");
    let scope = upload_scope(upload_id)?;
    let observers = proxies[1..]
        .iter()
        .map(|proxy| {
            proxy.observe_source_inspections(
                &[
                    "RenameMultipartData",
                    "RenameData",
                    "RenameDataAtIncarnation",
                    "RenameDataWithQuotaFence",
                ],
                &scope,
                &path,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for observer in &observers {
        observer.release();
    }
    let failure = tokio::time::timeout(
        DEADLINE,
        clients[0]
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.e_tag().ok_or("missing legacy ETag")?)
                            .build(),
                    )
                    .build(),
            )
            .send(),
    )
    .await?
    .expect_err("legacy source publication must wait for a fleet supporting native recovery");
    assert_unavailable(&failure, "storage_write_quorum", None);
    assert!(
        observers.iter().all(|observer| observer.hits() == 0),
        "unsupported legacy inspection must stop before any old or new remote publication RPC"
    );
    for (observer, peer) in observers.iter().zip(&cluster.nodes[1..]) {
        let address: std::net::SocketAddr = peer.address.parse()?;
        let expected: std::collections::HashSet<_> = peer
            .data_dirs
            .iter()
            .map(|root| format!("http://localhost:{}{root}", address.port()))
            .collect();
        assert_eq!(
            observer.unsupported_source_inspection_routes(),
            expected,
            "the new coordinator must observe native Inspect rejection from this old peer's exact configured disk routes"
        );
    }
    drop(observers);
    // Legacy upgrade validates every physical participant before writing any
    // marker. An old peer's Unsupported inspection leaves all authority intact.
    unchanged_create_sources(&cluster, &bucket, key, &original)?;
    let listed = clients[0]
        .list_parts()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await?;
    assert_eq!(listed.parts().len(), 1);
    assert_eq!(listed.parts()[0].part_number(), Some(1));
    assert_eq!(listed.parts()[0].e_tag(), part.e_tag());
    no_created_target(&clients, &bucket, key).await?;
    assert_object_bytes_after_fault(&clients[1], &bucket, "unrelated", &unrelated).await?;

    // Do not send MPU mutations through an old coordinator after rollout
    // starts. Resume this exact upload only after every peer is current.
    for node in 1..cluster.nodes.len() {
        cluster.stop_node(node)?;
        cluster.start_node(node).await?;
        crate::upgrade_compatibility_test::wait_for_upgrade_write_readiness(
            &clients,
            &bucket,
            &format!("mixed-mpu-current-node-{node}"),
            DEADLINE,
        )
        .await?;
    }
    wait_for_storage_peer_recovery(&proxies, &clients[0], &bucket, upload_id, 4).await?;
    unchanged_create_sources(&cluster, &bucket, key, &original)?;
    abort_after_fault(&clients[0], &bucket, key, upload_id, false).await?;
    assert!(
        roots.iter().all(|root| !root.exists()),
        "the current fleet must reclaim every original staging root without recreating the upload"
    );
    no_created_target(&clients, &bucket, key).await?;
    for client in &clients {
        assert_object_bytes_after_fault(client, &bucket, "unrelated", &unrelated).await?;
    }
    assert_no_uploads_after_fault(&clients, &bucket).await?;
    Ok(())
}

#[tokio::test]
async fn native_certified_partial_abort_retry_reclaims_survivors_below_metadata_quorum() -> TestResult {
    use crate::storage_api::multipart_source_rpc::{
        MultipartAbortCertificate, MultipartAbortProofKind, MultipartAbortRound, MultipartSourcePhase,
    };
    // Read-only views of actual native records. They issue no authority; the
    // successful server-side retry below authenticates the owned certificate.
    #[derive(serde::Deserialize)]
    struct CertificateRef {
        round: MultipartAbortRound,
        sha256: [u8; 32],
        slot: u8,
    }
    #[derive(serde::Deserialize)]
    struct AbortRef {
        round: MultipartAbortRound,
        committed: Option<CertificateRef>,
    }
    #[derive(serde::Deserialize)]
    struct SourceRef {
        disk_id: uuid::Uuid,
        physical_slot: usize,
        backup_owner: uuid::Uuid,
        phase: MultipartSourcePhase,
        capability_sha256: Option<[u8; 32]>,
        abort: Option<AbortRef>,
    }
    let (mut cluster, proxies, clients, bucket) = fixture(4, "EC:4", false).await?;
    let key = "partial-abort";
    let committed = payload_for("committed-before-abort", 4096);
    clients[0]
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(committed.clone()))
        .send()
        .await?;
    let upload = clients[0].create_multipart_upload().bucket(&bucket).key(key).send().await?;
    let upload_id = upload.upload_id().ok_or("missing upload ID")?;
    clients[0]
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(vec![0x62; 8 * 1024 * 1024]))
        .send()
        .await?;
    let path = upload_path_scope(&bucket, key, upload_id)?;
    // Strict proxy decoding matches only DeleteAborted at this exact root.
    // All sixteen sources close and bind the complete certificate first; only
    // then hold five deletes, leaving eleven responses below write quorum twelve.
    let pauses = vec![
        proxies[1].pause(&["RecoverMultipartSource"], 1, Some(&path))?,
        proxies[2].pause(&["RecoverMultipartSource"], usize::MAX, Some(&path))?,
    ];
    let request = clients[0]
        .abort_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id);
    let caller = tokio::spawn(async move { request.send().await });
    pauses[0].wait_for_hits(1).await?;
    pauses[1].wait_for_hits(4).await?;
    let first = tokio::time::timeout(DEADLINE, caller)
        .await??
        .expect_err("eleven deletes cannot acknowledge Abort");
    assert_unavailable(&first, "storage_write_quorum", None);
    for pause in &pauses {
        pause.discard_pending();
    }
    drop(pauses);
    wait_for_storage_peer_recovery(&proxies, &clients[0], &bucket, upload_id, 4).await?;
    let roots = cluster
        .nodes
        .iter()
        .flat_map(|node| &node.data_dirs)
        .map(|root| Path::new(root).join(RUSTFS_META_MULTIPART_BUCKET).join(&path))
        .collect::<Vec<_>>();
    assert_eq!(
        roots.iter().filter(|root| root.exists()).count(),
        5,
        "the partial delete must be physically observed"
    );
    let hash = sha256_hex(format!("{bucket}/{key}").as_bytes());
    let retained = multipart_sources(&cluster, Some(&hash))?;
    assert_eq!(retained.len(), 5, "five actual physical sources retain their closure evidence");
    let mut common_certificate = None;
    let mut physical_owners = BTreeSet::new();
    for source in retained.values() {
        assert!(!source.files.contains_key(Path::new("xl.meta")), "Aborting never leaves an ACTIVE head");
        let state: SourceRef = rmp_serde::from_slice(
            source
                .files
                .get(Path::new(".mpu-source-state"))
                .ok_or("surviving native source has no fixed state")?,
        )?;
        assert_eq!(state.phase, MultipartSourcePhase::Aborting);
        assert!(state.capability_sha256.is_none());
        assert!(
            physical_owners.insert((state.physical_slot, state.disk_id)),
            "each survivor is a distinct physical owner"
        );
        let abort = state.abort.ok_or("surviving native state lacks closure intent")?;
        let committed = abort
            .committed
            .ok_or("all-N closure must bind a durable certificate before deleting any root")?;
        assert!(committed.slot <= 1);
        let certificate_path = PathBuf::from(".mpu-completions")
            .join(state.backup_owner.to_string())
            .join(format!("abort-{}.msgpack", committed.slot));
        let certificate: MultipartAbortCertificate = rmp_serde::from_slice(
            source
                .files
                .get(&certificate_path)
                .ok_or("surviving closure certificate is not present under its actual owner")?,
        )?;
        assert_eq!(certificate.round, abort.round);
        assert_eq!(certificate.round, committed.round);
        assert_eq!(certificate.round.lineage.path, path);
        assert_eq!(certificate.round.lineage.bucket, bucket);
        assert_eq!(certificate.round.lineage.object, key);
        assert_eq!(certificate.disk_ids.len(), 16);
        assert_eq!(certificate.proofs.len(), 16);
        assert_eq!(certificate.disk_ids.iter().copied().collect::<BTreeSet<_>>().len(), 16);
        assert!(certificate.disk_ids.iter().all(|id| !id.is_nil()));
        assert_eq!(certificate.disk_ids.get(state.physical_slot), Some(&state.disk_id));
        for (slot, proof) in certificate.proofs.iter().enumerate() {
            assert_eq!(proof.kind, MultipartAbortProofKind::Prepared);
            assert_eq!(proof.challenge, certificate.round.round);
            assert_eq!(proof.round, certificate.round);
            assert_eq!(proof.snapshot.physical_slot, slot);
            assert_eq!(proof.snapshot.disk_id, certificate.disk_ids[slot]);
            assert_eq!(proof.snapshot.lineage, certificate.round.lineage);
            assert_eq!(proof.snapshot.generation, certificate.round.generation);
            assert_eq!(proof.snapshot.phase, MultipartSourcePhase::Aborting);
            assert!(proof.snapshot.head_sha256.is_none() && proof.snapshot.capability_sha256.is_none());
        }
        let digest = certificate.digest()?;
        assert_eq!(
            digest, committed.sha256,
            "the actual fixed state binds exactly this immutable full-N certificate"
        );
        if let Some(expected) = common_certificate {
            assert_eq!(digest, expected);
        } else {
            common_certificate = Some(digest);
        }
    }
    for client in &clients {
        assert_object_bytes_after_fault(client, &bucket, key, &committed).await?;
    }
    eprintln!(
        "MPU partial Abort: stage=first_delete, expected_write_quorum=12, deleted=11, survivors=5, request_id={:?}",
        first
            .raw_response()
            .and_then(|response| response.headers().get("x-amz-request-id"))
    );
    assert!(
        retained.values().all(|source| source.participant.0 != 3),
        "the selected offline node must have no surviving root"
    );
    cluster.stop_node(3)?;
    let offline = tokio::time::timeout(
        DEADLINE,
        clients[0]
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send(),
    )
    .await?
    .expect_err("an unavailable original participant cannot be replaced by the certificate");
    assert_unavailable(&offline, "storage_write_quorum", None);
    assert!(
        multipart_sources(&cluster, Some(&hash))? == retained,
        "an unavailable participant must not authorize deletion or change retained evidence"
    );
    cluster.start_node(3).await?;
    wait_for_storage_peer_recovery(&proxies, &clients[0], &bucket, upload_id, 4).await?;
    eprintln!("MPU certified residual: unavailable original returned storage_write_quorum; all surviving bytes retained");

    // Kill the coordinator and a node owning a real surviving source. The
    // retained bytes and full-N certificate must survive both process restarts
    // before a fresh coordinator can authorize the retry.
    let survivor_node = retained.values().next().ok_or("missing surviving source")?.participant.0;
    let mut restarted = vec![0, survivor_node];
    restarted.sort_unstable();
    restarted.dedup();
    for node in restarted {
        cluster.stop_node(node)?;
        cluster.start_node(node).await?;
        wait_for_storage_peer_recovery(&proxies, &clients[0], &bucket, upload_id, 4).await?;
        assert!(
            multipart_sources(&cluster, Some(&hash))? == retained,
            "restart must preserve every surviving source and certificate byte"
        );
    }
    eprintln!("MPU certified residual: coordinator and surviving owner restarted; all retained bytes unchanged");
    let delete_observers = proxies
        .iter()
        .map(|proxy| proxy.pause(&["RecoverMultipartSource"], usize::MAX, Some(&path)))
        .collect::<Result<Vec<_>, _>>()?;
    for observer in &delete_observers {
        observer.release();
    }
    assert!(
        multipart_sources(&cluster, Some(&hash))? == retained,
        "every retained shard and owned closure record must stay byte-for-byte intact before retry"
    );
    clients[0]
        .abort_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .expect("five surviving owners reuse the authenticated full-N closure; absent roots are not votes");
    let delete_calls = delete_observers.iter().map(RpcPause::hits).sum::<usize>();
    assert_eq!(delete_calls, 5, "retry must dispatch only the five actual native owned deletions");
    assert!(
        roots.iter().all(|root| !root.exists()),
        "certified retry must reclaim every surviving source"
    );
    assert!(
        multipart_sources(&cluster, None)?.is_empty(),
        "no hidden native root or control record may remain"
    );
    eprintln!(
        "MPU native partial Abort: stage=certified_retry, closure_members=16, previous_deletes=11, survivors=5, native_delete_calls={delete_calls}, all_sources_absent=true"
    );
    drop(delete_observers);
    assert_no_uploads_after_fault(&clients, &bucket).await?;
    for client in &clients {
        assert_object_bytes_after_fault(client, &bucket, key, &committed).await?;
    }
    Ok(())
}

#[tokio::test]
async fn late_complete_packets_cannot_publish_after_abort_and_bucket_recreation() -> TestResult {
    for size in [0, 8 * 1024 * 1024] {
        let (cluster, proxies, clients, bucket) = fixture(1, "EC:2", false).await?;
        let key = "late-complete";
        let upload = clients[0].create_multipart_upload().bucket(&bucket).key(key).send().await?;
        let upload_id = upload.upload_id().ok_or("missing upload ID")?;
        let part = clients[0]
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0x62; size]))
            .send()
            .await?;
        let path = upload_path_scope(&bucket, key, upload_id)?;
        let staging_roots = cluster
            .nodes
            .iter()
            .flat_map(|node| &node.data_dirs)
            .map(|root| std::path::Path::new(root).join(".rustfs.sys/multipart").join(&path))
            .collect::<Vec<_>>();
        assert_eq!(staging_roots.len(), 4);
        assert!(
            staging_roots.iter().all(|root| root.exists()),
            "staging paths must be physically witnessed before retirement"
        );
        let scope = upload_scope(upload_id)?;
        let pauses = proxies[1..3]
            .iter()
            .map(|proxy| {
                proxy.pause_late(
                    &[
                        "RenameData",
                        "RenameMultipartData",
                        "RenameDataAtIncarnation",
                        "RenameDataWithQuotaFence",
                    ],
                    64,
                    &scope,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let complete = clients[0]
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
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
        let caller = tokio::spawn(async move { complete.send().await });
        wait_for_faults(&pauses, &["RenameMultipartData"]).await?;
        let failure = tokio::time::timeout(DEADLINE, caller)
            .await??
            .expect_err("two retained peers must reject Complete below write quorum");
        assert_unavailable(&failure, "storage_write_quorum", None);
        assert!(pauses.iter().all(|pause| pause.responses() == 0));

        // Retire the upload and old bucket while the exact signed packets are
        // still owned by the proxy, beyond their callers' operation budget.
        abort_after_fault(&clients[0], &bucket, key, upload_id, false).await?;
        assert!(
            staging_roots.iter().all(|root| !root.exists()),
            "Abort must reclaim every staging root before delivery"
        );
        recover_after_fault("retire the old target before bucket recreation", || {
            clients[0].delete_object().bucket(&bucket).key(key).send()
        })
        .await??;
        clients[0].delete_bucket().bucket(&bucket).send().await?;
        clients[1].create_bucket().bucket(&bucket).send().await?;
        let fresh = payload_for("new-bucket-incarnation", 4096);
        recover_after_fault("publish in the recreated bucket", || {
            clients[1]
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(fresh.clone()))
                .send()
        })
        .await??;
        for client in &clients {
            assert_object_bytes_after_fault(client, &bucket, key, &fresh).await?;
        }
        for pause in &pauses {
            pause.release();
        }
        wait_until(
            DEADLINE,
            || async {
                Ok(pauses
                    .iter()
                    .all(|pause| pause.refused_mutations() == pause.hits() && pause.responses() == pause.hits()))
            },
            "every retained RenameMultipartData packet reaches the peer and returns an authenticated mutation refusal",
        )
        .await?;
        eprintln!(
            "MPU late delivery: bytes={size}, stage=after_abort_gc_and_bucket_recreation, delivered={}, refused={}, request_id={:?}",
            pauses.iter().map(RpcPause::responses).sum::<usize>(),
            pauses.iter().map(RpcPause::refused_mutations).sum::<usize>(),
            failure
                .raw_response()
                .and_then(|response| response.headers().get("x-amz-request-id"))
        );
        for client in &clients {
            assert_object_bytes_after_fault(client, &bucket, key, &fresh).await?;
        }
        assert_no_uploads_after_fault(&clients, &bucket).await?;
        assert!(
            staging_roots.iter().all(|root| !root.exists()),
            "refused late packets must not recreate staging"
        );
    }
    late_part_packets_preserve_retired_and_fresh_uploads().await?;
    Ok(())
}

async fn late_part_packets_preserve_retired_and_fresh_uploads() -> TestResult {
    for (method, settles) in [("PreparePartTransaction", false), ("SettlePartTransaction", true)] {
        for recreate_bucket in [false, true] {
            let (cluster, proxies, clients, bucket) = fixture(1, "EC:2", false).await?;
            let key = "late-part-transaction";
            let previous = payload_for("committed-before-late-part", 4096);
            clients[0]
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(previous.clone()))
                .send()
                .await?;
            let upload = clients[0].create_multipart_upload().bucket(&bucket).key(key).send().await?;
            let upload_id = upload.upload_id().ok_or("missing original upload ID")?;
            clients[0]
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(1)
                .body(ByteStream::from(vec![0x41; 9 * 1024 * 1024]))
                .send()
                .await?;
            let path = upload_path_scope(&bucket, key, upload_id)?;
            let hash = sha256_hex(format!("{bucket}/{key}").as_bytes());
            let roots = cluster
                .nodes
                .iter()
                .flat_map(|node| &node.data_dirs)
                .map(|root| Path::new(root).join(RUSTFS_META_MULTIPART_BUCKET).join(&path))
                .collect::<Vec<_>>();
            assert_eq!(roots.len(), 4);
            let original = multipart_sources(&cluster, Some(&hash))?;
            assert_eq!(original.len(), 4);
            assert!(
                original.values().all(|source| source
                    .files
                    .keys()
                    .any(|path| path.file_name().and_then(|name| name.to_str()) == Some("part.1"))),
                "all selected peers originally own the acknowledged part"
            );
            let scope = upload_scope(upload_id)?;
            // Retain only the original attempt; recovery of the same upload
            // must be able to issue fresh settlement requests through the gate.
            let pauses = proxies[1..3]
                .iter()
                .map(|proxy| proxy.pause_late(&[method], 1, &scope))
                .collect::<Result<Vec<_>, _>>()?;
            let replacement = clients[0]
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(1)
                .body(ByteStream::from(vec![0x62; 8 * 1024 * 1024]));
            let caller = tokio::spawn(async move { replacement.send().await });
            wait_for_faults(&pauses, &[method]).await?;
            let failure = tokio::time::timeout(DEADLINE, caller)
                .await??
                .expect_err("two retained peers cannot acknowledge part replacement");
            assert_unavailable(&failure, "storage_write_quorum", None);
            assert!(pauses.iter().all(|pause| pause.hits() == 1 && pause.responses() == 0));
            let expected_actions = if settles { (1, 0) } else { (0, 0) };
            assert!(
                pauses.iter().all(|pause| pause.retained_settle_actions() == expected_actions),
                "settlement capture must be a real Commit, not Rollback"
            );
            abort_after_fault(&clients[0], &bucket, key, upload_id, false).await?;
            assert!(
                roots.iter().all(|root| !root.exists()),
                "ordinary Abort physically retires all original roots"
            );
            assert!(multipart_sources(&cluster, Some(&hash))?.is_empty());
            for client in &clients {
                assert_object_bytes_after_fault(client, &bucket, key, &previous).await?;
            }
            let protected = if recreate_bucket {
                clients[0].delete_object().bucket(&bucket).key(key).send().await?;
                clients[0].delete_bucket().bucket(&bucket).send().await?;
                clients[1].create_bucket().bucket(&bucket).send().await?;
                let fresh = payload_for("recreated-before-late-part", 4096);
                clients[1]
                    .put_object()
                    .bucket(&bucket)
                    .key(key)
                    .body(ByteStream::from(fresh.clone()))
                    .send()
                    .await?;
                fresh
            } else {
                previous
            };
            let fresh = clients[2].create_multipart_upload().bucket(&bucket).key(key).send().await?;
            let fresh_id = fresh.upload_id().ok_or("missing fresh upload ID")?;
            let fresh_body = payload_for("fresh-active-part", 4096);
            let fresh_part = clients[2]
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(fresh_id)
                .part_number(1)
                .body(ByteStream::from(fresh_body.clone()))
                .send()
                .await?;
            let before = multipart_sources(&cluster, Some(&hash))?;
            assert_eq!(before.len(), 4, "the new upload exists on every original participant");
            for pause in &pauses {
                pause.release();
            }
            wait_until(
                DEADLINE,
                || async {
                    Ok(pauses.iter().all(|pause| {
                        pause.responses() == pause.hits()
                            && if settles {
                                pause.successful_mutation_replies() == pause.hits() && pause.refused_mutations() == 0
                            } else {
                                pause.refused_mutations() == pause.hits() && pause.successful_mutation_replies() == 0
                            }
                    }))
                },
                "every original part packet returns its typed authenticated terminal outcome",
            )
            .await?;
            assert!(roots.iter().all(|root| !root.exists()), "late part packets cannot recreate retired roots");
            assert!(
                multipart_sources(&cluster, Some(&hash))? == before,
                "late packets cannot change a fresh upload's shard or control bytes"
            );
            for client in &clients {
                assert_object_bytes_after_fault(client, &bucket, key, &protected).await?;
                let listed = client
                    .list_parts()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(fresh_id)
                    .send()
                    .await?;
                assert_eq!(listed.parts().len(), 1);
                assert_eq!(listed.parts()[0].e_tag(), fresh_part.e_tag());
                let missing = client
                    .list_parts()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .send()
                    .await
                    .expect_err("old upload remains retired");
                assert_eq!(missing.raw_response().expect("old upload service response").status().as_u16(), 404);
                assert_eq!(missing.as_service_error().and_then(|error| error.meta().code()), Some("NoSuchUpload"));
            }
            clients[2]
                .complete_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(fresh_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .parts(
                            CompletedPart::builder()
                                .part_number(1)
                                .e_tag(fresh_part.e_tag().ok_or("missing fresh part ETag")?)
                                .build(),
                        )
                        .build(),
                )
                .send()
                .await?;
            for client in &clients {
                assert_object_bytes_after_fault(client, &bucket, key, &fresh_body).await?;
            }
            assert!(multipart_sources(&cluster, Some(&hash))?.is_empty());
            assert_no_uploads_after_fault(&clients, &bucket).await?;
            eprintln!(
                "MPU late part: method={method}, recreate_bucket={recreate_bucket}, retired_roots=4, delivered=2, refused={}, successful_replies={}, fresh_sources_unchanged=4, verified_readers=4",
                pauses.iter().map(RpcPause::refused_mutations).sum::<usize>(),
                pauses.iter().map(RpcPause::successful_mutation_replies).sum::<usize>()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_metadata_wait_releases_fence_for_bucket_recreation() -> TestResult {
    let (_cluster, proxies, clients, bucket) = fixture(1, "EC:2", false).await?;
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

#[tokio::test]
async fn part_resend_owner_serializes_complete_and_abort_after_client_cancellation() -> TestResult {
    use md5::{Digest as _, Md5};

    let (cluster, proxies, clients, bucket) = fixture(1, "EC:2", false).await?;
    for cancel_waiter in [false, true] {
        for complete in [false, true] {
            let key = format!("resend-{cancel_waiter}-{complete}");
            let previous = payload_for(&key, 4096);
            clients[0]
                .put_object()
                .bucket(&bucket)
                .key(&key)
                .body(ByteStream::from(previous.clone()))
                .send()
                .await?;
            let upload = clients[0].create_multipart_upload().bucket(&bucket).key(&key).send().await?;
            let upload_id = upload.upload_id().ok_or("missing upload ID")?;
            let original = clients[0]
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(upload_id)
                .part_number(1)
                .body(ByteStream::from(vec![0x41; 9 * 1024 * 1024]))
                .send()
                .await?;
            let replacement = vec![0x62; 9 * 1024 * 1024];
            let replacement_etag = format!(
                "\"{}\"",
                hex_simd::encode_to_string(Md5::digest(&replacement), hex_simd::AsciiCase::Lower)
            );
            assert_ne!(original.e_tag(), Some(replacement_etag.as_str()));
            let scope = upload_scope(upload_id)?;
            let path = upload_path_scope(&bucket, &key, upload_id)?;
            let roots = cluster
                .nodes
                .iter()
                .flat_map(|node| &node.data_dirs)
                .map(|root| std::path::Path::new(root).join(".rustfs.sys/multipart").join(&path))
                .collect::<Vec<_>>();
            let pauses = proxies[1..]
                .iter()
                .map(|proxy| proxy.pause(&["PreparePartTransaction"], usize::MAX, Some(&scope)))
                .collect::<Result<Vec<_>, _>>()?;
            let resend = clients[0]
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(upload_id)
                .part_number(1)
                .body(ByteStream::from(replacement.clone()));
            let caller = tokio::spawn(async move { resend.send().await });
            wait_for_faults(&pauses, &["PreparePartTransaction"]).await?;
            let caller = if cancel_waiter {
                caller.abort();
                assert!(caller.await.expect_err("cancelled part response waiter").is_cancelled());
                None
            } else {
                Some(caller)
            };

            // Observe a real upload-lock attempt on another coordinator, then
            // release this observer while the part transaction remains paused.
            let admission = proxies[0].pause(&["Lock", "LockBatch"], usize::MAX, Some(&scope))?;
            let client = clients[1].clone();
            let terminal_bucket = bucket.clone();
            let terminal_key = key.clone();
            let terminal_upload = upload_id.to_owned();
            let terminal_etag = replacement_etag.clone();
            let mut terminal = tokio::spawn(async move {
                let result: TestResult = if complete {
                    client
                        .complete_multipart_upload()
                        .bucket(terminal_bucket)
                        .key(terminal_key)
                        .upload_id(terminal_upload)
                        .multipart_upload(
                            CompletedMultipartUpload::builder()
                                .parts(CompletedPart::builder().part_number(1).e_tag(terminal_etag).build())
                                .build(),
                        )
                        .send()
                        .await
                        .map(|_| ())
                        .map_err(Into::into)
                } else {
                    client
                        .abort_multipart_upload()
                        .bucket(terminal_bucket)
                        .key(terminal_key)
                        .upload_id(terminal_upload)
                        .send()
                        .await
                        .map(|_| ())
                        .map_err(Into::into)
                };
                result
            });
            admission.wait_for_hits(1).await?;
            admission.release();
            wait_until(
                DEADLINE,
                || async { Ok(admission.forwarded() > 0) },
                "terminal upload-lock request forwarded",
            )
            .await?;
            assert!(
                tokio::time::timeout(Duration::from_millis(150), &mut terminal).await.is_err(),
                "finalization must wait for the admitted part owner: cancel_waiter={cancel_waiter}, complete={complete}"
            );
            drop(admission);
            for pause in &pauses {
                pause.release();
            }
            if let Some(caller) = caller {
                let part = tokio::time::timeout(DEADLINE, caller).await???;
                assert_eq!(part.e_tag(), Some(replacement_etag.as_str()));
            }
            tokio::time::timeout(DEADLINE, terminal).await???;
            wait_until(
                DEADLINE,
                || async { Ok(roots.iter().all(|root| !root.exists())) },
                "retired resend upload staging",
            )
            .await?;
            let expected = if complete { &replacement } else { &previous };
            for client in &clients {
                assert_object_bytes(client, &bucket, &key, expected).await?;
                let missing = client
                    .list_parts()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(upload_id)
                    .send()
                    .await
                    .expect_err("a terminal upload must reject ListParts on every coordinator");
                assert_eq!(missing.as_service_error().and_then(|error| error.meta().code()), Some("NoSuchUpload"));
            }
            assert_no_uploads(&clients, &bucket).await?;
            eprintln!(
                "MPU resend owner: cancel_waiter={cancel_waiter}, complete={complete}, prepared_peers=3, retired_roots=4, verified_readers=4"
            );
        }
    }
    simultaneous_resend_complete_and_abort(&cluster, &proxies, &clients, &bucket).await
}

async fn simultaneous_resend_complete_and_abort(
    cluster: &RustFSTestClusterEnvironment,
    proxies: &[RpcFaultProxy],
    clients: &[Client],
    bucket: &str,
) -> TestResult {
    use md5::{Digest as _, Md5};

    for (cancel_waiter, preferred_complete) in [
        (false, None),
        (true, None),
        (false, Some(false)),
        (true, Some(false)),
        (false, Some(true)),
        (true, Some(true)),
    ] {
        let schedule = match preferred_complete {
            None => "race",
            Some(false) => "abort",
            Some(true) => "complete",
        };
        let key = format!("three-way-resend-{cancel_waiter}-{schedule}");
        let previous = payload_for(&key, 4096);
        clients[0]
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(previous.clone()))
            .send()
            .await?;
        let upload = clients[0].create_multipart_upload().bucket(bucket).key(&key).send().await?;
        let upload_id = upload.upload_id().ok_or("missing three-way upload ID")?;
        let original = clients[0]
            .upload_part()
            .bucket(bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0x41; 9 * 1024 * 1024]))
            .send()
            .await?;
        let replacement = vec![0x63; 9 * 1024 * 1024];
        let replacement_etag = format!(
            "\"{}\"",
            hex_simd::encode_to_string(Md5::digest(&replacement), hex_simd::AsciiCase::Lower)
        );
        assert_ne!(original.e_tag(), Some(replacement_etag.as_str()));
        let scope = upload_scope(upload_id)?;
        let path = upload_path_scope(bucket, &key, upload_id)?;
        let roots = cluster
            .nodes
            .iter()
            .flat_map(|node| &node.data_dirs)
            .map(|root| std::path::Path::new(root).join(".rustfs.sys/multipart").join(&path))
            .collect::<Vec<_>>();
        let pauses = proxies[1..]
            .iter()
            .map(|proxy| proxy.pause(&["PreparePartTransaction"], usize::MAX, Some(&scope)))
            .collect::<Result<Vec<_>, _>>()?;
        let resend = clients[0]
            .upload_part()
            .bucket(bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(replacement.clone()));
        let caller = tokio::spawn(async move { resend.send().await });
        wait_for_faults(&pauses, &["PreparePartTransaction"]).await?;
        let caller = if cancel_waiter {
            caller.abort();
            assert!(
                caller
                    .await
                    .expect_err("cancelled three-way part response waiter")
                    .is_cancelled()
            );
            None
        } else {
            Some(caller)
        };

        // The part owns the upload lock before either terminal call starts.
        // Ordered rows hold the loser's remote votes so partial lock grants
        // cannot prevent the winner from reaching its quorum.
        let held_locks = if let Some(complete_wins) = preferred_complete {
            let loser = if complete_wins { 2 } else { 1 };
            let owner = proxies[loser].local_addr().to_string();
            (0..4)
                .filter(|node| *node != loser)
                .map(|node| proxies[node].pause_upload_lock_owner(&scope, &owner))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        let admission = if preferred_complete.is_none() {
            let admission = proxies[0].pause_upload_locks(&scope)?;
            admission.release();
            Some(admission)
        } else {
            None
        };
        let observer = admission
            .as_ref()
            .or_else(|| held_locks.first())
            .expect("peer-zero lock observer");
        let start = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let complete_request = clients[1]
            .complete_multipart_upload()
            .bucket(bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(1).e_tag(&replacement_etag).build())
                    .build(),
            );
        let complete_start = start.clone();
        let mut complete = tokio::spawn(async move {
            complete_start.wait().await;
            complete_request.send().await
        });
        let abort_request = clients[2]
            .abort_multipart_upload()
            .bucket(bucket)
            .key(&key)
            .upload_id(upload_id);
        let abort_start = start.clone();
        let mut abort = tokio::spawn(async move {
            abort_start.wait().await;
            abort_request.send().await
        });
        start.wait().await;
        if let Some(complete_wins) = preferred_complete {
            let winner = if complete_wins { 1 } else { 2 };
            let owner = proxies[winner].local_addr().to_string();
            wait_for_faults(&held_locks, &["Lock", "LockBatch"]).await?;
            wait_until(
                DEADLINE,
                || async { Ok(observer.has_forwarded_lock_owner(&owner)) },
                "preferred terminal coordinator reaches the peer while its competitor is held",
            )
            .await?;
        } else {
            wait_until(
                DEADLINE,
                || async { Ok(observer.forwarded_lock_owners() == 2) },
                "Complete and Abort from distinct upload-lock owners reach the peer",
            )
            .await?;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut complete).await.is_err(),
            "Complete must wait while the resent part is prepared: cancel_waiter={cancel_waiter}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut abort).await.is_err(),
            "Abort must wait while the resent part is prepared: cancel_waiter={cancel_waiter}"
        );
        let terminal_owners = observer.forwarded_lock_owners();
        if preferred_complete.is_none() {
            assert_eq!(terminal_owners, 2, "both ungated terminal coordinators reach the peer");
        } else {
            assert_eq!(terminal_owners, 1, "only the preferred coordinator may pass the owner gate");
        }
        let held_lock_calls = held_locks.iter().map(RpcPause::hits).sum::<usize>();
        assert_eq!(held_lock_calls > 0, preferred_complete.is_some());
        drop(admission);
        for pause in &pauses {
            pause.release();
        }
        if let Some(caller) = caller {
            let part = tokio::time::timeout(DEADLINE, caller).await???;
            assert_eq!(part.e_tag(), Some(replacement_etag.as_str()));
        }
        // Keep remote peers unavailable only to the losing coordinator until
        // the winner finishes. The part owner and winner remain unaffected.
        let (completion, abortion) = if preferred_complete == Some(false) {
            let abortion = tokio::time::timeout(DEADLINE, &mut abort).await??;
            assert!(abortion.is_ok(), "preferred Abort must succeed: {abortion:?}");
            for gate in &held_locks {
                gate.release();
            }
            (tokio::time::timeout(DEADLINE, &mut complete).await??, abortion)
        } else {
            let completion = tokio::time::timeout(DEADLINE, &mut complete).await??;
            if preferred_complete == Some(true) {
                assert!(completion.is_ok(), "preferred Complete must succeed: {completion:?}");
            }
            for gate in &held_locks {
                gate.release();
            }
            (completion, tokio::time::timeout(DEADLINE, &mut abort).await??)
        };
        let completed = match completion {
            Ok(_) => true,
            Err(error) => {
                assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(404), "{error:?}");
                assert_eq!(error.as_service_error().and_then(|error| error.meta().code()), Some("NoSuchUpload"));
                false
            }
        };
        if let Some(expected) = preferred_complete {
            assert_eq!(completed, expected, "the scoped lock gate must establish the requested terminal order");
        }
        if !completed {
            abortion?;
        } else if let Err(error) = abortion {
            assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(404), "{error:?}");
            assert_eq!(error.as_service_error().and_then(|error| error.meta().code()), Some("NoSuchUpload"));
        }
        wait_until(
            DEADLINE,
            || async { Ok(roots.iter().all(|root| !root.exists())) },
            "three-way terminal operation retires every staging root",
        )
        .await?;
        let expected = if completed { &replacement } else { &previous };
        for client in clients {
            assert_object_bytes(client, bucket, &key, expected).await?;
            let missing = client
                .list_parts()
                .bucket(bucket)
                .key(&key)
                .upload_id(upload_id)
                .send()
                .await
                .expect_err("three-way terminal upload must reject ListParts");
            assert_eq!(missing.as_service_error().and_then(|error| error.meta().code()), Some("NoSuchUpload"));
        }
        assert_no_uploads(clients, bucket).await?;
        eprintln!(
            "MPU three-way owner: cancel_waiter={cancel_waiter}, complete_won={completed}, forwarded_terminal_owners={terminal_owners}, prepared_peers=3, retired_roots=4, verified_readers=4, schedule={schedule}, held_lock_calls={held_lock_calls}"
        );
    }
    Ok(())
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
    let (_cluster, proxies, clients, bucket) = fixture(1, "EC:2", false).await?;
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
                &[
                    "RenameData",
                    "RenameMultipartData",
                    "RenameDataAtIncarnation",
                    "RenameDataWithQuotaFence",
                ]
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
        resume_at_rpc(
            &proxies,
            if locks {
                &["Lock", "LockBatch"]
            } else {
                &["Delete", "RecoverMultipartSource"]
            },
            &scope,
            async move { Ok(request.send().await?) },
        )
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
