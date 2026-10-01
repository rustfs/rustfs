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

//! Multi-process coverage for the distributed object-metadata cache mutation
//! fence. The cache remains bypassed by default; this test opts into mutation
//! fencing to exercise peer RPC, unavailable-peer fail-closed behavior, and
//! restart recovery across independent RustFS processes.

use crate::common::{RustFSTestClusterEnvironment, init_logging};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier, Tag, Tagging,
    VersioningConfiguration,
};
use rustfs_ecstore::api::rpc::PeerRestClient;
use rustfs_utils::XHost;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

const BUCKET: &str = "metadata-cache-mutation-fence";
const TEST_RPC_SECRET: &str = "rustfs-internode-signature-e2e-secret";

fn batch_delete(keys: &[String]) -> Delete {
    let objects = keys
        .iter()
        .map(|key| {
            ObjectIdentifier::builder()
                .key(key)
                .build()
                .expect("object identifier should be valid")
        })
        .collect();
    Delete::builder()
        .set_objects(Some(objects))
        .build()
        .expect("batch delete request should be valid")
}

async fn get_object_bytes(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let output = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|err| format!("GetObject {bucket}/{key}: {err:?}"))?;
    Ok(output
        .body
        .collect()
        .await
        .map_err(|err| format!("GetObject body {bucket}/{key}: {err:?}"))?
        .into_bytes()
        .to_vec())
}

async fn expect_object_error_code(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected_code: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let error = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err("GetObject should fail for a deleted current object");
    let actual_code = error.as_service_error().and_then(|service_error| service_error.code());
    if actual_code != Some(expected_code) {
        return Err(format!("GetObject error code mismatch for {bucket}/{key}: expected {expected_code}, got {error:?}").into());
    }
    Ok(())
}

fn assert_mutation_timeout(phase: &str, error: impl std::fmt::Display) {
    let message = error.to_string();
    let normalized = message.to_ascii_lowercase();
    assert!(
        normalized.contains("timeout") || normalized.contains("timed out") || normalized.contains("expired"),
        "unexpected {phase} timeout error: {message}"
    );
}

#[tokio::test]
async fn mutation_fence_fails_closed_when_peer_is_unavailable_and_recovers_after_restart()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.set_env("RUSTFS_GET_OBJECT_METADATA_CACHE_DISTRIBUTED_ENABLE", "true");
    cluster.set_env("RUSTFS_RPC_SECRET", TEST_RPC_SECRET);
    cluster.set_node_env(2, "RUSTFS_E2E_TEST_DROP_METADATA_CACHE_BEGIN_RESPONSE_ONCE", "true")?;
    cluster.set_node_env(2, "RUSTFS_E2E_TEST_DROP_METADATA_CACHE_COMMIT_RESPONSE_ONCE", "true")?;
    cluster.set_node_env(2, "RUSTFS_E2E_TEST_DROP_METADATA_CACHE_ABORT_RESPONSE_ONCE", "true")?;
    let response_loss_marker = format!("{}/mutation-response-loss-node2.txt", cluster.temp_dir);
    cluster.set_node_env(2, "RUSTFS_E2E_TEST_METADATA_CACHE_MUTATION_RESPONSE_LOSS_MARKER", &response_loss_marker)?;
    for node_idx in 0..4 {
        cluster.set_node_capture_log_path(node_idx, format!("{}/node-{node_idx}.log", cluster.temp_dir))?;
    }
    cluster.start().await?;
    let _ = rustfs_credentials::set_global_rpc_secret(TEST_RPC_SECRET.to_string());
    let remote_host = cluster.nodes[1].address.clone();
    let remote = PeerRestClient::new(XHost::try_from(remote_host.clone())?, cluster.nodes[1].url.clone());
    remote
        .mutate_object_metadata_cache_scoped(
            rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Abort,
            Uuid::new_v4(),
            "protocol-probe",
            "abort-without-begin",
            rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
            true,
        )
        .await
        .map_err(|err| format!("body-bound mutation RPC protocol probe to {remote_host}: {err}"))?;

    cluster
        .create_test_bucket(BUCKET)
        .await
        .map_err(|err| format!("create test bucket: {err:?}"))?;

    let clients = cluster.create_all_clients()?;
    let key = format!("mutation-fence/{}", Uuid::new_v4().simple());
    clients[0]
        .put_object()
        .bucket(BUCKET)
        .key(&key)
        .body(ByteStream::from_static(b"before"))
        .send()
        .await
        .map_err(|err| format!("initial fenced PutObject: {err:?}"))?;
    for client in &clients {
        assert_eq!(get_object_bytes(client, BUCKET, &key).await?, b"before");
    }

    let abort_peer_host = cluster.nodes[2].address.clone();
    let abort_peer = PeerRestClient::new(XHost::try_from(abort_peer_host.clone())?, cluster.nodes[2].url.clone());
    abort_peer
        .mutate_object_metadata_cache_scoped(
            rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Abort,
            Uuid::new_v4(),
            BUCKET,
            &key,
            rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
            true,
        )
        .await
        .map_err(|err| format!("Abort response-loss retry to {abort_peer_host}: {err}"))?;

    let response_loss_phases = std::fs::read_to_string(&response_loss_marker)?;
    for phase in ["phase=1", "phase=2", "phase=3"] {
        assert!(
            response_loss_phases.lines().any(|line| line == phase),
            "response loss hook did not fire for {phase}"
        );
    }

    let peer_host = cluster.nodes[1].address.clone();
    let peer = PeerRestClient::new(XHost::try_from(peer_host.clone())?, cluster.nodes[1].url.clone());
    let replay_id = Uuid::new_v4();
    for phase in [
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Abort,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Begin,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Begin,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Commit,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Commit,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Abort,
    ] {
        peer.mutate_object_metadata_cache_scoped(
            phase,
            replay_id,
            BUCKET,
            &key,
            rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
            true,
        )
        .await
        .map_err(|err| format!("idempotent/reordered phase {phase:?} to {peer_host}: {err}"))?;
    }
    assert_eq!(get_object_bytes(&clients[1], BUCKET, &key).await?, b"before");

    let global_replay_id = Uuid::new_v4();
    for phase in [
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Begin,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Begin,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Commit,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Commit,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Abort,
    ] {
        peer.mutate_object_metadata_cache_scoped(
            phase,
            global_replay_id,
            BUCKET,
            "recursive-prefix/",
            rustfs_protos::ObjectMetadataCacheMutationRpcScope::All,
            true,
        )
        .await
        .map_err(|err| format!("all-cache-scope replay {phase:?} to {peer_host}: {err}"))?;
    }
    let bucket_scope_id = Uuid::new_v4();
    for phase in [
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Begin,
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Commit,
    ] {
        peer.mutate_object_metadata_cache_scoped(
            phase,
            bucket_scope_id,
            BUCKET,
            "",
            rustfs_protos::ObjectMetadataCacheMutationRpcScope::All,
            true,
        )
        .await
        .map_err(|err| format!("empty-prefix all-cache {phase:?} to {peer_host}: {err}"))?;
    }
    let encoded_max_key = format!("{}__XLDIR__", "a".repeat(1023));
    assert_eq!(encoded_max_key.len(), 1032);
    let long_key_id = Uuid::new_v4();
    peer.mutate_object_metadata_cache_scoped(
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Begin,
        long_key_id,
        BUCKET,
        &encoded_max_key,
        rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
        true,
    )
    .await
    .map_err(|err| format!("maximum encoded directory key Begin to {peer_host}: {err}"))?;
    peer.mutate_object_metadata_cache_scoped(
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Abort,
        long_key_id,
        BUCKET,
        &encoded_max_key,
        rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
        true,
    )
    .await
    .map_err(|err| format!("maximum encoded directory key Abort to {peer_host}: {err}"))?;

    // Three nodes still satisfy the four-node EC read/write quorum, but Begin
    // must fail closed because every topology peer must acknowledge the fence.
    cluster.stop_node_gracefully(3).await?;
    let failed_overwrite = clients[0]
        .put_object()
        .bucket(BUCKET)
        .key(&key)
        .body(ByteStream::from_static(b"must-not-commit"))
        .send()
        .await;
    assert!(
        failed_overwrite.is_err(),
        "a mutation must not commit when one serving peer cannot acknowledge its metadata-cache fence"
    );
    cluster.start_node(3).await?;
    sleep(Duration::from_secs(2)).await;
    let clients = cluster.create_all_clients()?;
    for client in &clients {
        assert_eq!(
            get_object_bytes(client, BUCKET, &key).await?,
            b"before",
            "failed fenced overwrite must leave the prior committed generation readable"
        );
    }

    clients[1]
        .put_object()
        .bucket(BUCKET)
        .key(&key)
        .body(ByteStream::from_static(b"after"))
        .send()
        .await
        .map_err(|err| format!("post-recovery PutObject: {err:?}"))?;

    clients[0]
        .put_object_tagging()
        .bucket(BUCKET)
        .key(&key)
        .tagging(
            Tagging::builder()
                .tag_set(Tag::builder().key("generation").value("after-restart").build()?)
                .build()?,
        )
        .send()
        .await
        .map_err(|err| format!("PutObjectTagging: {err:?}"))?;
    let tags = clients[2]
        .get_object_tagging()
        .bucket(BUCKET)
        .key(&key)
        .send()
        .await
        .map_err(|err| format!("GetObjectTagging after metadata mutation: {err:?}"))?;
    assert!(
        tags.tag_set()
            .iter()
            .any(|tag| tag.key() == "generation" && tag.value() == "after-restart"),
        "metadata-only mutation must be visible on another process"
    );

    let multipart_key = format!("mutation-fence/multipart-{}", Uuid::new_v4().simple());
    let upload = clients[1]
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(&multipart_key)
        .send()
        .await
        .map_err(|err| format!("CreateMultipartUpload: {err:?}"))?;
    let upload_id = upload.upload_id().ok_or("multipart response omitted upload id")?.to_owned();
    let part_body = vec![0x5a; 5 * 1024 * 1024];
    let uploaded_part = clients[1]
        .upload_part()
        .bucket(BUCKET)
        .key(&multipart_key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(part_body.clone()))
        .send()
        .await
        .map_err(|err| format!("UploadPart: {err:?}"))?;
    let etag = uploaded_part.e_tag().ok_or("uploaded part omitted ETag")?.to_owned();
    clients[1]
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(&multipart_key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(vec![CompletedPart::builder().part_number(1).e_tag(etag).build()]))
                .build(),
        )
        .send()
        .await
        .map_err(|err| format!("CompleteMultipartUpload: {err:?}"))?;
    assert_eq!(
        get_object_bytes(&clients[3], BUCKET, &multipart_key)
            .await
            .map_err(|err| format!("GET after multipart completion: {err:?}"))?,
        part_body
    );

    let copied_key = format!("mutation-fence/copy-{}", Uuid::new_v4().simple());
    clients[0]
        .copy_object()
        .bucket(BUCKET)
        .key(&copied_key)
        .copy_source(format!("{BUCKET}/{key}"))
        .send()
        .await
        .map_err(|err| format!("CopyObject: {err:?}"))?;
    assert_eq!(
        get_object_bytes(&clients[2], BUCKET, &copied_key)
            .await
            .map_err(|err| format!("GET after CopyObject: {err:?}"))?,
        b"after"
    );

    let listed = clients[3]
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("mutation-fence/")
        .send()
        .await
        .map_err(|err| format!("ListObjects after mutation sequence: {err:?}"))?;
    assert!(
        listed.contents().iter().any(|entry| entry.key() == Some(copied_key.as_str())),
        "LIST must observe the same committed generation as GET"
    );

    clients[2]
        .delete_object()
        .bucket(BUCKET)
        .key(&copied_key)
        .send()
        .await
        .map_err(|err| format!("DeleteObject after copy: {err:?}"))?;
    expect_object_error_code(&clients[1], BUCKET, &copied_key, "NoSuchKey").await?;

    cluster.stop_node_gracefully(2).await?;
    cluster.start_node(2).await?;
    sleep(Duration::from_secs(2)).await;

    let restarted_clients = cluster.create_all_clients()?;
    for (client_index, client) in restarted_clients.iter().enumerate() {
        assert_eq!(
            get_object_bytes(client, BUCKET, &key)
                .await
                .map_err(|err| format!("current GET after restart from client {client_index}: {err:?}"))?,
            b"after",
            "a restarted process with an empty in-memory cache must resolve the committed generation"
        );
        assert_eq!(
            get_object_bytes(client, BUCKET, &multipart_key)
                .await
                .map_err(|err| format!("GET after restart for multipart object from client {client_index}: {err:?}"))?,
            part_body
        );
        expect_object_error_code(client, BUCKET, &copied_key, "NoSuchKey").await?;
    }

    Ok(())
}

#[tokio::test]
async fn versioned_delete_marker_mutation_is_visible_across_processes() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.set_env("RUSTFS_GET_OBJECT_METADATA_CACHE_DISTRIBUTED_ENABLE", "true");
    cluster.set_env("RUSTFS_RPC_SECRET", TEST_RPC_SECRET);
    for node_idx in 0..4 {
        cluster.set_node_capture_log_path(node_idx, format!("{}/node-{node_idx}.log", cluster.temp_dir))?;
    }
    cluster.start().await?;
    let _ = rustfs_credentials::set_global_rpc_secret(TEST_RPC_SECRET.to_string());
    cluster
        .create_test_bucket(BUCKET)
        .await
        .map_err(|err| format!("create test bucket: {err:?}"))?;
    let clients = cluster.create_all_clients()?;

    clients[0]
        .put_bucket_versioning()
        .bucket(BUCKET)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .map_err(|err| format!("Enable bucket versioning: {err:?}"))?;
    let versioned_key = format!("mutation-fence/versioned-{}", Uuid::new_v4().simple());
    let version_id = clients[0]
        .put_object()
        .bucket(BUCKET)
        .key(&versioned_key)
        .body(ByteStream::from_static(b"version-before-delete"))
        .send()
        .await
        .map_err(|err| format!("Versioned PutObject: {err:?}"))?
        .version_id()
        .ok_or("versioned PUT omitted version ID")?
        .to_owned();
    let delete_marker = clients[1]
        .delete_object()
        .bucket(BUCKET)
        .key(&versioned_key)
        .send()
        .await
        .map_err(|err| format!("versioned DeleteObject: {err:?}"))?;
    assert_eq!(delete_marker.delete_marker(), Some(true));
    expect_object_error_code(&clients[3], BUCKET, &versioned_key, "NoSuchKey").await?;
    let historical = clients[2]
        .get_object()
        .bucket(BUCKET)
        .key(&versioned_key)
        .version_id(&version_id)
        .send()
        .await
        .map_err(|err| format!("GET historical version after delete marker: {err:?}"))?;
    assert_eq!(
        historical
            .body
            .collect()
            .await
            .map_err(|err| format!("historical version body after delete marker: {err:?}"))?
            .into_bytes()
            .as_ref(),
        b"version-before-delete"
    );
    let versions = clients[3]
        .list_object_versions()
        .bucket(BUCKET)
        .prefix(&versioned_key)
        .send()
        .await
        .map_err(|err| format!("LIST object versions after delete marker: {err:?}"))?;
    assert!(
        versions
            .delete_markers()
            .iter()
            .any(|marker| marker.key() == Some(versioned_key.as_str())),
        "LIST versions must expose the delete marker written under the mutation fence"
    );

    cluster.stop_node_gracefully(2).await?;
    cluster.start_node(2).await?;
    sleep(Duration::from_secs(2)).await;
    let restarted_clients = cluster.create_all_clients()?;
    for (client_index, client) in restarted_clients.iter().enumerate() {
        expect_object_error_code(client, BUCKET, &versioned_key, "NoSuchKey").await?;
        let historical = client
            .get_object()
            .bucket(BUCKET)
            .key(&versioned_key)
            .version_id(&version_id)
            .send()
            .await
            .map_err(|err| format!("historical version GET after restart from client {client_index}: {err:?}"))?;
        assert_eq!(
            historical
                .body
                .collect()
                .await
                .map_err(|err| format!("historical version body after restart: {err:?}"))?
                .into_bytes()
                .as_ref(),
            b"version-before-delete"
        );
    }

    Ok(())
}

#[tokio::test]
async fn mutation_fence_phase_timeouts_are_safe_to_replay() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.set_env("RUSTFS_GET_OBJECT_METADATA_CACHE_DISTRIBUTED_ENABLE", "true");
    cluster.set_env("RUSTFS_RPC_SECRET", TEST_RPC_SECRET);
    cluster.set_node_env(2, "RUSTFS_E2E_TEST_DELAY_METADATA_CACHE_BEGIN_RESPONSE_MS", "6000")?;
    cluster.set_node_env(2, "RUSTFS_E2E_TEST_DELAY_METADATA_CACHE_COMMIT_RESPONSE_MS", "6000")?;
    cluster.set_node_env(2, "RUSTFS_E2E_TEST_DELAY_METADATA_CACHE_ABORT_RESPONSE_MS", "6000")?;
    for node_idx in 0..4 {
        cluster.set_node_capture_log_path(node_idx, format!("{}/node-{node_idx}.log", cluster.temp_dir))?;
    }
    cluster.start().await?;
    let _ = rustfs_credentials::set_global_rpc_secret(TEST_RPC_SECRET.to_string());

    let remote_host = cluster.nodes[2].address.clone();
    let peer = PeerRestClient::new(XHost::try_from(remote_host.clone())?, cluster.nodes[2].url.clone());
    let begin_id = Uuid::new_v4();
    let begin_error = peer
        .mutate_object_metadata_cache_scoped(
            rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Begin,
            begin_id,
            BUCKET,
            "timeout/begin",
            rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
            true,
        )
        .await
        .expect_err("delayed Begin must time out at the caller");
    assert_mutation_timeout("Begin", begin_error);
    let abort_after_begin_timeout = peer
        .mutate_object_metadata_cache_scoped(
            rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Abort,
            begin_id,
            BUCKET,
            "timeout/begin",
            rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
            true,
        )
        .await
        .expect_err("delayed Abort must time out at the caller");
    assert_mutation_timeout("Abort", abort_after_begin_timeout);
    peer.mutate_object_metadata_cache_scoped(
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Abort,
        begin_id,
        BUCKET,
        "timeout/begin",
        rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
        true,
    )
    .await
    .map_err(|err| format!("Abort replay after timeout to {remote_host}: {err}"))?;

    let commit_id = Uuid::new_v4();
    peer.mutate_object_metadata_cache_scoped(
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Begin,
        commit_id,
        BUCKET,
        "timeout/commit",
        rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
        true,
    )
    .await
    .map_err(|err| format!("Begin before Commit timeout to {remote_host}: {err}"))?;
    let commit_error = peer
        .mutate_object_metadata_cache_scoped(
            rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Commit,
            commit_id,
            BUCKET,
            "timeout/commit",
            rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
            true,
        )
        .await
        .expect_err("delayed Commit must time out at the caller");
    assert_mutation_timeout("Commit", commit_error);
    peer.mutate_object_metadata_cache_scoped(
        rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Commit,
        commit_id,
        BUCKET,
        "timeout/commit",
        rustfs_protos::ObjectMetadataCacheMutationRpcScope::Object,
        true,
    )
    .await
    .map_err(|err| format!("duplicate Commit after timeout to {remote_host}: {err}"))?;

    Ok(())
}

#[tokio::test]
async fn mixed_cache_configuration_fails_closed_before_object_mutation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.set_env("RUSTFS_GET_OBJECT_METADATA_CACHE_DISTRIBUTED_ENABLE", "true");
    cluster.set_env("RUSTFS_RPC_SECRET", TEST_RPC_SECRET);
    cluster.set_node_env(3, "RUSTFS_GET_OBJECT_METADATA_CACHE_DISTRIBUTED_ENABLE", "false")?;
    cluster.start().await?;
    let _ = rustfs_credentials::set_global_rpc_secret(TEST_RPC_SECRET.to_string());
    cluster
        .create_test_bucket(BUCKET)
        .await
        .map_err(|err| format!("create test bucket: {err:?}"))?;

    let clients = cluster.create_all_clients()?;
    let enabled_key = format!("mixed-config/enabled-writer/{}", Uuid::new_v4().simple());
    let disabled_key = format!("mixed-config/disabled-writer/{}", Uuid::new_v4().simple());
    for (client, key) in [(&clients[0], &enabled_key), (&clients[3], &disabled_key)] {
        let result = client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from_static(b"must-not-commit"))
            .send()
            .await;
        assert!(result.is_err(), "mixed cache configuration must reject a mutation before commit");
    }
    expect_object_error_code(&clients[0], BUCKET, &enabled_key, "NoSuchKey").await?;
    expect_object_error_code(&clients[3], BUCKET, &disabled_key, "NoSuchKey").await?;

    let remote_host = cluster.nodes[3].address.clone();
    let remote = PeerRestClient::new(XHost::try_from(remote_host.clone())?, cluster.nodes[3].url.clone());
    assert_eq!(remote.probe_object_metadata_cache_configuration(true).await?, Some(false));
    let mismatch = remote
        .mutate_object_metadata_cache_scoped(
            rustfs_protos::ObjectMetadataCacheMutationRpcPhase::Begin,
            Uuid::new_v4(),
            BUCKET,
            "mixed-config/probe",
            rustfs_protos::ObjectMetadataCacheMutationRpcScope::All,
            true,
        )
        .await;
    assert!(
        mismatch.is_err(),
        "a peer must reject a mutation request with a different cache configuration"
    );

    Ok(())
}

#[tokio::test]
async fn all_cache_scope_bypasses_warmed_peer_metadata_during_multi_object_delete()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.set_env("RUSTFS_GET_OBJECT_METADATA_CACHE_DISTRIBUTED_ENABLE", "true");
    cluster.set_env("RUSTFS_RPC_SECRET", TEST_RPC_SECRET);
    let marker_paths = (0..4)
        .map(|node| format!("{}/node-{node}-metadata-cache-hit.txt", cluster.temp_dir))
        .collect::<Vec<_>>();
    for (node, path) in marker_paths.iter().enumerate() {
        cluster.set_node_env(node, "RUSTFS_E2E_TEST_METADATA_CACHE_HIT_MARKER", path)?;
    }
    cluster.start().await?;
    let _ = rustfs_credentials::set_global_rpc_secret(TEST_RPC_SECRET.to_string());
    cluster
        .create_test_bucket(BUCKET)
        .await
        .map_err(|err| format!("create test bucket: {err:?}"))?;

    let clients = cluster.create_all_clients()?;
    let keys = [
        format!("cache-prefix/{}", Uuid::new_v4().simple()),
        format!("cache-prefix/nested/{}", Uuid::new_v4().simple()),
    ];
    for key in &keys {
        clients[0]
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from_static(b"warm-cache-before-delete"))
            .send()
            .await
            .map_err(|err| format!("PutObject {key}: {err:?}"))?;
    }
    for client in &clients {
        for key in &keys {
            assert_eq!(get_object_bytes(client, BUCKET, key).await?, b"warm-cache-before-delete");
        }
        assert_eq!(get_object_bytes(client, BUCKET, &keys[0]).await?, b"warm-cache-before-delete");
    }
    for path in &marker_paths {
        let marker = std::fs::read_to_string(path)?;
        assert!(
            marker.contains(&keys[0]),
            "the workload must prove a real metadata-cache hit on every peer"
        );
    }

    cluster.stop_node_gracefully(3).await?;
    let failed_delete = clients[0]
        .delete_objects()
        .bucket(BUCKET)
        .delete(batch_delete(&keys))
        .send()
        .await;
    if let Ok(output) = failed_delete {
        assert!(output.deleted().is_empty(), "no batch delete may commit without every cache-fence peer");
        assert_eq!(output.errors().len(), keys.len(), "every requested key should report the failed fence");
    }
    for client in &clients[..3] {
        for key in &keys {
            assert_eq!(get_object_bytes(client, BUCKET, key).await?, b"warm-cache-before-delete");
        }
    }

    cluster.start_node(3).await?;
    sleep(Duration::from_secs(2)).await;
    cluster.stop_node_gracefully(0).await?;
    cluster.start_node(0).await?;
    sleep(Duration::from_secs(2)).await;
    let clients = cluster.create_all_clients()?;
    let deleted = clients[0]
        .delete_objects()
        .bucket(BUCKET)
        .delete(batch_delete(&keys))
        .send()
        .await
        .map_err(|err| format!("batch DeleteObjects with the all-cache fence: {err:?}"))?;
    assert_eq!(deleted.deleted().len(), keys.len());
    assert!(deleted.errors().is_empty(), "successful batch delete should not report per-key errors");
    for client in &clients {
        for key in &keys {
            expect_object_error_code(client, BUCKET, key, "NoSuchKey").await?;
        }
    }

    Ok(())
}
