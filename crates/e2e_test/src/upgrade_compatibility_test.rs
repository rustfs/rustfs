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

use crate::common::{RustFSTestClusterEnvironment, RustFSTestEnvironment, init_logging, rustfs_binary_path};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, ServerSideEncryption, VersioningConfiguration,
};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const SOURCE_BINARY_ENV: &str = "RUSTFS_UPGRADE_SOURCE_BINARY";
const SSE_MASTER_KEY_ENV: &str = "RUSTFS_SSE_S3_MASTER_KEY";
const SSE_MASTER_KEY: &str = "QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI=";
const PLAIN_BUCKET: &str = "upgrade-plain-data";
const VERSIONED_BUCKET: &str = "upgrade-versioned-data";
const MIXED_BUCKET: &str = "upgrade-mixed-version-data";
const MIXED_NODE_COUNT: usize = 4;
const MULTIPART_WORKERS: usize = 16;
const MULTIPART_UPLOADS_PER_WORKER: usize = 16;
// Peers keep a restarted node's drive in Suspect/Returning for roughly
// probe_interval (2s) x success_threshold (3) after it comes back; 30s
// comfortably covers that window plus CI scheduling jitter.
const LISTING_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

fn source_binary() -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let path = std::env::var_os(SOURCE_BINARY_ENV)
        .map(PathBuf::from)
        .ok_or("RUSTFS_UPGRADE_SOURCE_BINARY must point to the pinned previous release binary")?;
    if !path.is_file() {
        return Err(format!("upgrade source binary does not exist: {}", path.display()).into());
    }
    Ok(path)
}

async fn enable_versioning(client: &Client, bucket: &str) -> TestResult {
    let configuration = VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .build();
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(configuration)
        .send()
        .await?;
    Ok(())
}

async fn read_object(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(Option<ServerSideEncryption>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let mut request = client.get_object().bucket(bucket).key(key);
    if let Some(version_id) = version_id {
        request = request.version_id(version_id);
    }
    let response = request.send().await?;
    let encryption = response.server_side_encryption().cloned();
    let body = response.body.collect().await?.into_bytes().to_vec();
    Ok((encryption, body))
}

async fn write_multipart(client: &Client, bucket: &str, key: &str, parts: &[Vec<u8>]) -> TestResult {
    let created = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = created.upload_id().ok_or("CreateMultipartUpload omitted upload ID")?;
    let mut completed_parts = Vec::with_capacity(parts.len());

    for (index, part) in parts.iter().enumerate() {
        let part_number = i32::try_from(index + 1)?;
        let uploaded = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(part.clone()))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().ok_or("UploadPart omitted ETag")?)
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;
    Ok(())
}

fn configure_cluster_logs(cluster: &mut RustFSTestClusterEnvironment) -> TestResult {
    let Some(log_dir) = std::env::var_os("RUSTFS_E2E_LOG_DIR") else {
        return Ok(());
    };
    std::fs::create_dir_all(&log_dir)?;
    for node_idx in 0..cluster.nodes.len() {
        let path = Path::new(&log_dir).join(format!("mixed-upgrade-node-{node_idx}.log"));
        cluster.set_node_capture_log_path(node_idx, path.to_string_lossy().into_owned())?;
    }
    Ok(())
}

async fn write_multipart_load(clients: &[Client], phase: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut tasks = JoinSet::new();
    for worker in 0..MULTIPART_WORKERS {
        let client = clients[worker % clients.len()].clone();
        let phase = phase.to_string();
        tasks.spawn(async move {
            let mut keys = Vec::with_capacity(MULTIPART_UPLOADS_PER_WORKER);
            for upload in 0..MULTIPART_UPLOADS_PER_WORKER {
                let key = format!("{phase}/multipart/{worker:02}/{upload:02}");
                let part = vec![u8::try_from(worker)?; 64 * 1024];
                write_multipart(&client, MIXED_BUCKET, &key, &[part]).await?;
                keys.push(key);
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(keys)
        });
    }

    let mut keys = Vec::with_capacity(MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER);
    while let Some(result) = tasks.join_next().await {
        keys.extend(result??);
    }
    Ok(keys)
}

/// Assert that `client` eventually lists exactly `expected` objects under
/// `{phase}/`, polling until [`LISTING_CONVERGENCE_TIMEOUT`].
///
/// A single-snapshot assertion here is racy by construction: each phase both
/// writes and lists within seconds of a node restart. While a peer still holds
/// the restarted node's drive in Suspect/Returning, strict-quorum listing
/// consults only the remaining three drives and drops any object that was
/// itself legally written at write quorum (3/4 drives) during an earlier
/// node's identical post-restart window — its xl.meta is then visible on only
/// two of the three consulted drives, below the required object quorum of
/// three. GET still succeeds for such objects; only the listing under-counts
/// until drive health converges. A genuine upgrade data-loss regression still
/// fails after the deadline.
async fn wait_for_phase_listing(client: &Client, phase: &str, expected: usize, context: &str) -> TestResult {
    let deadline = Instant::now() + LISTING_CONVERGENCE_TIMEOUT;
    loop {
        let listed = client
            .list_objects_v2()
            .bucket(MIXED_BUCKET)
            .prefix(format!("{phase}/"))
            .send()
            .await?;
        let count = listed.contents().len();
        if count == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{context}: listing under {phase}/ returned {count} of {expected} objects even after {}s of post-restart convergence",
                LISTING_CONVERGENCE_TIMEOUT.as_secs()
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn exercise_mixed_cluster(
    cluster: &RustFSTestClusterEnvironment,
    phase: &str,
    current_node: usize,
    previous_node: usize,
) -> TestResult {
    let clients = cluster.create_all_clients()?;
    let current_client = &clients[current_node];
    let previous_client = &clients[previous_node];

    let current_key = format!("{phase}/written-by-current");
    let current_body = format!("{phase}: current RustFS build").into_bytes();
    current_client
        .put_object()
        .bucket(MIXED_BUCKET)
        .key(&current_key)
        .body(ByteStream::from(current_body.clone()))
        .send()
        .await?;
    assert_eq!(read_object(previous_client, MIXED_BUCKET, &current_key, None).await?.1, current_body);

    let previous_key = format!("{phase}/written-by-previous");
    let previous_body = format!("{phase}: previous RustFS release").into_bytes();
    previous_client
        .put_object()
        .bucket(MIXED_BUCKET)
        .key(&previous_key)
        .body(ByteStream::from(previous_body.clone()))
        .send()
        .await?;
    assert_eq!(read_object(current_client, MIXED_BUCKET, &previous_key, None).await?.1, previous_body);

    let multipart_keys = write_multipart_load(&clients, phase).await?;
    let expected_count = multipart_keys.len() + 2;
    for (label, client) in [("current", current_client), ("previous", previous_client)] {
        wait_for_phase_listing(
            client,
            phase,
            expected_count,
            &format!("the {label} RustFS version must stream the complete mixed-version listing"),
        )
        .await?;
    }

    let last_multipart_key = format!("{phase}/multipart/{:02}/{:02}", MULTIPART_WORKERS - 1, MULTIPART_UPLOADS_PER_WORKER - 1);
    assert_eq!(
        read_object(previous_client, MIXED_BUCKET, &last_multipart_key, None).await?.1,
        vec![u8::try_from(MULTIPART_WORKERS - 1)?; 64 * 1024]
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn direct_upgrade_from_rc2_preserves_object_contracts() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let mut env = RustFSTestEnvironment::new().await?;
    let server_env = [(SSE_MASTER_KEY_ENV, SSE_MASTER_KEY)];
    env.start_rustfs_server_from_binary(&previous_binary, vec![], &server_env)
        .await?;

    let old_client = env.create_s3_client();
    env.create_test_bucket(PLAIN_BUCKET).await?;
    env.create_test_bucket(VERSIONED_BUCKET).await?;
    enable_versioning(&old_client, VERSIONED_BUCKET).await?;

    let plain_key = "plain-object";
    let plain_bytes = b"written by the previous RustFS release";
    old_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(plain_key)
        .body(ByteStream::from_static(plain_bytes))
        .send()
        .await?;

    let encrypted_key = "sse-s3-object";
    let encrypted_bytes = b"encrypted by the previous RustFS release";
    old_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(encrypted_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(encrypted_bytes))
        .send()
        .await?;

    let multipart_key = "multipart-object";
    let multipart_parts = vec![vec![b'a'; 5 * 1024 * 1024], b"final multipart bytes".to_vec()];
    let multipart_bytes = multipart_parts.concat();
    write_multipart(&old_client, PLAIN_BUCKET, multipart_key, &multipart_parts).await?;

    let versioned_key = "versioned-object";
    let version1_bytes = b"version one from the previous release";
    let version1 = old_client
        .put_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(version1_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("first versioned PUT omitted version ID")?
        .to_string();
    let version2_bytes = b"version two from the previous release";
    let version2 = old_client
        .put_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(version2_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("second versioned PUT omitted version ID")?
        .to_string();
    let deleted = old_client
        .delete_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .send()
        .await?;
    assert_eq!(deleted.delete_marker(), Some(true));
    let delete_marker = deleted
        .version_id()
        .ok_or("versioned DELETE omitted delete marker version ID")?
        .to_string();

    env.restart_server_preserving_data(vec![], &server_env).await?;
    let current_client = env.create_s3_client();

    assert_eq!(read_object(&current_client, PLAIN_BUCKET, plain_key, None).await?.1, plain_bytes);

    let (encryption, upgraded_encrypted_bytes) = read_object(&current_client, PLAIN_BUCKET, encrypted_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(upgraded_encrypted_bytes, encrypted_bytes);

    assert_eq!(read_object(&current_client, PLAIN_BUCKET, multipart_key, None).await?.1, multipart_bytes);

    assert_eq!(
        read_object(&current_client, VERSIONED_BUCKET, versioned_key, Some(&version1))
            .await?
            .1,
        version1_bytes
    );
    assert_eq!(
        read_object(&current_client, VERSIONED_BUCKET, versioned_key, Some(&version2))
            .await?
            .1,
        version2_bytes
    );

    let current_read = current_client
        .get_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .send()
        .await
        .expect_err("the previous release's delete marker must remain current after upgrade");
    assert_eq!(current_read.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(current_read.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));

    let listed = current_client
        .list_object_versions()
        .bucket(VERSIONED_BUCKET)
        .prefix(versioned_key)
        .send()
        .await?;
    assert_eq!(listed.versions().len(), 2);
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(version1.as_str()))
    );
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(version2.as_str()))
    );
    assert_eq!(listed.delete_markers().len(), 1);
    assert_eq!(listed.delete_markers()[0].version_id(), Some(delete_marker.as_str()));
    assert_eq!(listed.delete_markers()[0].is_latest(), Some(true));

    let post_upgrade_key = "written-after-upgrade";
    let post_upgrade_bytes = b"written by the current RustFS build";
    current_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(post_upgrade_key)
        .body(ByteStream::from_static(post_upgrade_bytes))
        .send()
        .await?;
    assert_eq!(
        read_object(&current_client, PLAIN_BUCKET, post_upgrade_key, None).await?.1,
        post_upgrade_bytes
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn rolling_upgrade_from_rc2_preserves_mixed_version_contracts() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let current_binary = rustfs_binary_path();
    let mut cluster = RustFSTestClusterEnvironment::new(MIXED_NODE_COUNT).await?;
    cluster.set_env("RUST_LOG", "rustfs=warn,rustfs_notify=warn");
    configure_cluster_logs(&mut cluster)?;
    cluster.start_with_binary(&previous_binary).await?;
    cluster.create_test_bucket(MIXED_BUCKET).await?;

    cluster.stop_node(0)?;
    cluster.start_node_from_binary(0, &current_binary).await?;
    exercise_mixed_cluster(&cluster, "one-current-node", 0, 1).await?;

    for node_idx in [1, 2] {
        cluster.stop_node(node_idx)?;
        cluster.start_node_from_binary(node_idx, &current_binary).await?;
    }
    exercise_mixed_cluster(&cluster, "one-previous-node", 0, 3).await?;

    cluster.stop_node(3)?;
    cluster.start_node_from_binary(3, &current_binary).await?;

    for (node_idx, client) in cluster.create_all_clients()?.iter().enumerate() {
        for phase in ["one-current-node", "one-previous-node"] {
            wait_for_phase_listing(
                client,
                phase,
                MULTIPART_WORKERS * MULTIPART_UPLOADS_PER_WORKER + 2,
                &format!("node {node_idx}: the homogeneous current cluster must preserve every object"),
            )
            .await?;
        }
    }

    Ok(())
}
