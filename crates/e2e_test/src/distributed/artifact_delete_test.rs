// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

#![cfg(unix)]

use super::artifact_seal_test::{assert_exact_version, confirm_seal, create_artifact_attempt};
use super::harness::{
    DistCluster, DistLayout, TestResult, enable_versioning, payload_for, s3_probe_client, unique_bucket, wait_for_ready,
};
use crate::chaos::{VersionShardCensus, census_object_version_on_disk};
use crate::common::init_logging;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;

const PUBLIC_INTERNAL_ERROR: &str = "We encountered an internal error, please try again.";

#[derive(Debug, PartialEq, Eq)]
struct PhysicalEntry {
    identity: (u64, u64),
    changed: (i64, i64),
    modified: (i64, i64),
    bytes: Option<Vec<u8>>,
}

// Include directory identity/ctime as well as file bytes. Creating and removing
// a temporary marker/rollback file cannot hide behind a restored final xl.meta.
// Reads change atime, which is deliberately excluded.
fn physical_snapshot(dist: &DistCluster, bucket: &str) -> TestResult<BTreeMap<(usize, PathBuf), PhysicalEntry>> {
    let mut snapshot = BTreeMap::new();
    for (node, instance) in dist.cluster.nodes.iter().enumerate() {
        let root = Path::new(&instance.data_dirs[0]).join(bucket);
        for entry in walkdir::WalkDir::new(&root) {
            let entry = entry?;
            let metadata = std::fs::symlink_metadata(entry.path())?;
            assert!(!metadata.file_type().is_symlink(), "unexpected fixture symlink");
            snapshot.insert(
                (node, entry.path().strip_prefix(&root)?.to_path_buf()),
                PhysicalEntry {
                    identity: (metadata.dev(), metadata.ino()),
                    changed: (metadata.ctime(), metadata.ctime_nsec()),
                    modified: (metadata.mtime(), metadata.mtime_nsec()),
                    bytes: if metadata.is_dir() {
                        None
                    } else {
                        Some(std::fs::read(entry.path())?)
                    },
                },
            );
        }
    }
    Ok(snapshot)
}

struct Version {
    key: String,
    id: String,
    body: Vec<u8>,
    manifests: Vec<VersionShardCensus>,
}

impl Version {
    async fn put(dist: &DistCluster, bucket: &str, key: &str, seed: &str) -> TestResult<Self> {
        let body = payload_for(seed, 2 * 1024 * 1024);
        let output = timeout(
            Duration::from_secs(30),
            dist.client(1)?
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body.clone().into())
                .send(),
        )
        .await??;
        let id = output.version_id().ok_or("fixture PUT omitted version ID")?.to_owned();
        let manifests = dist
            .cluster
            .nodes
            .iter()
            .map(|node| census_object_version_on_disk(Path::new(&node.data_dirs[0]), bucket, key, Some(&id)))
            .collect::<TestResult<Vec<_>>>()?;
        assert!(
            manifests
                .iter()
                .all(|manifest| manifest.is_complete() && !manifest.expected_part_numbers.is_empty())
        );
        Ok(Self {
            key: key.to_owned(),
            id,
            body,
            manifests,
        })
    }
}

fn target(key: &str, version: Option<&str>) -> TestResult<ObjectIdentifier> {
    Ok(ObjectIdentifier::builder()
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .build()?)
}

async fn assert_fixture(
    dist: &DistCluster,
    bucket: &str,
    prefix: &str,
    versions: &[Version],
    marker: (&str, &str),
) -> TestResult {
    let expected: BTreeSet<_> = versions
        .iter()
        .map(|version| (version.key.clone(), version.id.clone()))
        .collect();
    for node in 0..dist.cluster.nodes.len() {
        let client = dist.client(node)?;
        let listed = timeout(
            Duration::from_secs(30),
            client.list_object_versions().bucket(bucket).prefix(prefix).send(),
        )
        .await??;
        let actual: BTreeSet<_> = listed
            .versions()
            .iter()
            .map(|version| {
                (
                    version.key().unwrap_or_default().to_owned(),
                    version.version_id().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        assert_eq!(listed.versions().len(), versions.len());
        assert_eq!(actual, expected, "delete changed exact data versions on node {node}");
        assert_eq!(listed.delete_markers().len(), 1);
        assert_eq!(listed.delete_markers()[0].key(), Some(marker.0));
        assert_eq!(listed.delete_markers()[0].version_id(), Some(marker.1));
        assert_eq!(listed.delete_markers()[0].is_latest(), Some(true));
        for version in versions {
            assert_exact_version(&client, bucket, &version.key, &version.id, &version.body).await?;
            let disk = Path::new(&dist.cluster.nodes[node].data_dirs[0]);
            assert!(
                census_object_version_on_disk(disk, bucket, &version.key, Some(&version.id))?
                    .matches_manifest(&version.manifests[node])
            );
        }
    }
    Ok(())
}

async fn assert_single_denied(dist: &DistCluster, node: usize, bucket: &str, key: &str, version: Option<&str>) -> TestResult {
    let before = physical_snapshot(dist, bucket)?;
    // One attempt: a later retry must not conceal a first local mutation.
    let client = s3_probe_client(&dist.client(node)?);
    let error = timeout(
        Duration::from_secs(30),
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version.map(str::to_owned))
            .send(),
    )
    .await?
    .expect_err("ordinary deletion succeeded in a managed bucket");
    let service = error
        .as_service_error()
        .ok_or("delete failed without an S3 service rejection")?;
    assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(500));
    assert_eq!(service.code(), Some("InternalError"));
    assert_eq!(service.message(), Some(PUBLIC_INTERNAL_ERROR));
    assert!(
        physical_snapshot(dist, bucket)? == before,
        "first delete through node {node} wrote or rolled back managed data"
    );
    Ok(())
}

async fn assert_batch_denied(dist: &DistCluster, node: usize, bucket: &str, objects: &[ObjectIdentifier]) -> TestResult {
    let before = physical_snapshot(dist, bucket)?;
    let client = s3_probe_client(&dist.client(node)?);
    let response = timeout(
        Duration::from_secs(30),
        client
            .delete_objects()
            .bucket(bucket)
            .delete(Delete::builder().set_objects(Some(objects.to_vec())).quiet(false).build()?)
            .send(),
    )
    .await??;
    assert!(response.deleted().is_empty(), "managed batch reported a successful delete");
    assert_eq!(response.errors().len(), objects.len(), "batch must reject every requested occurrence");
    for (error, requested) in response.errors().iter().zip(objects) {
        assert_eq!(error.key(), Some(requested.key()));
        assert_eq!(error.version_id(), requested.version_id());
        assert_eq!(error.code(), Some("InternalError"));
        assert_eq!(error.message(), Some(PUBLIC_INTERNAL_ERROR));
    }
    assert!(
        physical_snapshot(dist, bucket)? == before,
        "first batch through node {node} wrote or rolled back managed data"
    );
    Ok(())
}

fn copy_existing_object(source: &Path, destination: &Path) -> TestResult {
    assert!(!destination.exists());
    for entry in walkdir::WalkDir::new(source) {
        let entry = entry?;
        let target = destination.join(entry.path().strip_prefix(source)?);
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&target)?;
        } else {
            assert!(entry.file_type().is_file());
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

async fn ordinary_delete_control(dist: &DistCluster, bucket: &str) -> TestResult {
    let client = s3_probe_client(&dist.client(0)?);
    let key = "ordinary-observer-control.bin";
    let version = Version::put(dist, bucket, key, key).await?;
    let before = physical_snapshot(dist, bucket)?;
    let deleted = client.delete_object().bucket(bucket).key(key).send().await?;
    assert_eq!(deleted.delete_marker(), Some(true));
    let marker = deleted.version_id().ok_or("ordinary delete omitted marker ID")?;
    assert!(
        physical_snapshot(dist, bucket)? != before,
        "physical observer missed real marker creation"
    );
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .version_id(marker)
        .send()
        .await?;
    // The logical version inventory is restored, but ctime/identity still
    // expose the two writes. This checks the observer used by managed negatives.
    let listed = client.list_object_versions().bucket(bucket).prefix(key).send().await?;
    assert_eq!(listed.versions().len(), 1);
    assert_eq!(listed.versions()[0].version_id(), Some(version.id.as_str()));
    assert!(listed.delete_markers().is_empty());
    assert!(
        physical_snapshot(dist, bucket)? != before,
        "marker round-trip hid transient physical writes"
    );
    assert_exact_version(&client, bucket, key, &version.id, &version.body).await?;
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .version_id(&version.id)
        .send()
        .await?;
    assert!(
        client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .send()
            .await?
            .versions()
            .is_empty()
    );

    let current = Version::put(dist, bucket, "ordinary-batch-current.bin", "current").await?;
    let exact = Version::put(dist, bucket, "ordinary-batch-exact.bin", "exact").await?;
    let response = client
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .objects(target(&current.key, None)?)
                .objects(target(&exact.key, Some(&exact.id))?)
                .objects(target(&exact.key, Some(&exact.id))?)
                .build()?,
        )
        .send()
        .await?;
    assert!(response.errors().is_empty());
    assert_eq!(response.deleted().len(), 3);
    let listed = client
        .list_object_versions()
        .bucket(bucket)
        .prefix(&current.key)
        .send()
        .await?;
    assert_eq!(listed.versions().len(), 1);
    assert_eq!(listed.delete_markers().len(), 1);
    assert!(
        client
            .list_object_versions()
            .bucket(bucket)
            .prefix(&exact.key)
            .send()
            .await?
            .versions()
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn four_node_artifact_delete_entrypoints_preserve_versions_and_physical_state() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourNodeFourDisk).await?;
    let (identity, admin) = create_artifact_attempt(&dist).await?;
    let bucket = identity["bucket"]
        .as_str()
        .ok_or("managed receipt omitted bucket")?
        .to_owned();
    let prefix = identity["attempt_prefix"]
        .as_str()
        .ok_or("managed receipt omitted prefix")?
        .to_owned();
    let key = format!("{prefix}native.bin");
    let mut versions = vec![
        Version::put(&dist, &bucket, &key, "native-old").await?,
        Version::put(&dist, &bucket, &key, "native-new").await?,
    ];
    let ordinary = unique_bucket("delete-control");
    dist.create_bucket(&ordinary).await?;
    enable_versioning(&dist.client(0)?, &ordinary).await?;

    // A new managed bucket cannot legitimately create a marker. Model existing
    // persisted state from before adoption/upgrade using a real S3-generated
    // ordinary object+marker, copied disk-for-disk in the same erasure set.
    // Only this object's files move; bucket authority/configuration is untouched.
    let marker_key = format!("{prefix}existing-marker.bin");
    versions.push(Version::put(&dist, &ordinary, &marker_key, "existing-data").await?);
    let output = dist
        .client(2)?
        .delete_object()
        .bucket(&ordinary)
        .key(&marker_key)
        .send()
        .await?;
    assert_eq!(output.delete_marker(), Some(true));
    let marker = output.version_id().ok_or("ordinary fixture omitted marker ID")?.to_owned();
    dist.cluster.stop();
    for node in &dist.cluster.nodes {
        let disk = Path::new(&node.data_dirs[0]);
        copy_existing_object(&disk.join(&ordinary).join(&marker_key), &disk.join(&bucket).join(&marker_key))?;
    }
    dist.cluster.start().await?;
    wait_for_ready(&dist.cluster).await?;
    assert_fixture(&dist, &bucket, &prefix, &versions, (&marker_key, &marker)).await?;
    ordinary_delete_control(&dist, &ordinary).await?;
    let batch = vec![
        target(&key, None)?,
        target(&key, Some(&versions[0].id))?,
        target(&key, Some(&versions[1].id))?,
        target(&marker_key, Some(&marker))?,
        target(&key, Some(&versions[0].id))?,
        target(&marker_key, Some(&versions[2].id))?,
    ];
    for phase in ["open", "sealed", "restarted"] {
        if phase == "sealed" {
            confirm_seal(&dist, &admin, &json!({"version": 1, "target": identity}).to_string(), &identity).await?;
        } else if phase == "restarted" {
            dist.restart_with_current_binary().await?;
        }
        for node in 0..dist.cluster.nodes.len() {
            assert_single_denied(&dist, node, &bucket, &key, None).await?;
            assert_single_denied(&dist, node, &bucket, &key, Some(&versions[0].id)).await?;
            assert_single_denied(&dist, node, &bucket, &marker_key, Some(&marker)).await?;
            assert_batch_denied(&dist, node, &bucket, &batch).await?;
        }
        assert_fixture(&dist, &bucket, &prefix, &versions, (&marker_key, &marker)).await?;
    }
    Ok(())
}
