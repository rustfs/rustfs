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
use crate::storage_api::multipart_fence_acceptance::RUSTFS_META_MULTIPART_BUCKET;
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
                wait_for_storage_peer_recovery(proxies, &clients[0], bucket, upload_id, drives_per_node).await?;
                // A rejected Complete must retain authority for ordinary
                // Abort. NoSuchUpload must not hide partial staging loss.
                let allow_missing = phase != "complete" || unavailable == unavailable_at_quorum;
                abort_after_fault(&clients[0], bucket, &key, upload_id, allow_missing).await?;
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

#[tokio::test]
async fn metadata_delay_preserves_write_quorum_and_rejects_quorum_minus_one() -> TestResult {
    let (cluster, proxies, clients, bucket) = fixture(1, "EC:2").await?;
    phase_quorum_matrix(&cluster, &proxies, &clients, &bucket, false, 1, 3).await
}

#[tokio::test]
async fn lock_delay_preserves_admission_quorum_and_reports_timeout() -> TestResult {
    let (cluster, proxies, clients, bucket) = fixture(1, "EC:2").await?;
    phase_quorum_matrix(&cluster, &proxies, &clients, &bucket, true, 1, 3).await
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
    phase_quorum_matrix(&cluster, &proxies, &clients, &bucket, false, 4, 12).await
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
