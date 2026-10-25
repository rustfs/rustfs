// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::artifact_repair_tail_test::PhysicalBarrier;
use super::artifact_seal_test::{assert_exact_version, confirm_seal, create_artifact_attempt};
use super::harness::{DistCluster, DistLayout, TestResult, payload_for, wait_until};
use crate::chaos::census_object_version_on_disk;
use crate::common::init_logging;
use crate::storage_api::artifact_rpc::{
    DiskError, TONIC_RPC_PREFIX, gen_tonic_signature_headers, set_tonic_canonical_body_digest,
};
use http::StatusCode;
use rustfs_filemeta::FileMeta;
use rustfs_protos::proto_gen::node_service::{RenameDataRequest, node_service_client::NodeServiceClient};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;
use tonic::Request;
use uuid::Uuid;

const PUT_SEAL_ROUTE: &str = "/rustfs/admin/v3/plugin-artifacts/seal-attempt";

fn signed_put_request(body: RenameDataRequest, audience: &str) -> TestResult<Request<RenameDataRequest>> {
    let canonical = rustfs_protos::canonical_artifact_rename_data_request_body(&body)?;
    let mut request = Request::new(body);
    request.set_timeout(Duration::from_secs(15));
    set_tonic_canonical_body_digest(&mut request, &canonical)?;
    let digest = request
        .metadata()
        .get("x-rustfs-content-sha256")
        .ok_or("missing canonical digest")?
        .to_str()?
        .to_owned();
    request.metadata_mut().as_mut().extend(gen_tonic_signature_headers(
        audience,
        TONIC_RPC_PREFIX.trim_start_matches('/'),
        "ArtifactRenameData",
        Some(&digest),
    )?);
    Ok(request)
}

fn publication_snapshot(paths: &[&Path]) -> TestResult<BTreeMap<PathBuf, Option<Vec<u8>>>> {
    let mut snapshot = BTreeMap::new();
    for root in paths {
        for entry in walkdir::WalkDir::new(root) {
            let entry = entry?;
            let contents = if entry.file_type().is_dir() {
                None
            } else {
                Some(std::fs::read(entry.path())?)
            };
            snapshot.insert(entry.path().to_path_buf(), contents);
        }
    }
    Ok(snapshot)
}

async fn assert_raw_put_closed(body: &RenameDataRequest, url: &str, audience: &str, paths: &[&Path]) -> TestResult {
    let before = publication_snapshot(paths)?;
    // Re-sign the exact original request; replay protection is not the rejection.
    let request = signed_put_request(body.clone(), audience)?;
    let response = timeout(Duration::from_secs(20), async {
        let mut client = NodeServiceClient::connect(url.to_owned()).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(client.artifact_rename_data(request).await?.into_inner())
    })
    .await??;
    assert!(!response.success, "sealed attempt admitted another ordinary PUT");
    let error = response.error.ok_or("rejected raw PUT omitted error")?;
    assert_eq!(error.code, DiskError::Io(std::io::ErrorKind::PermissionDenied.into()).to_u32());
    assert_eq!(error.error_info, "io error artifact attempt mutation domain is closed");
    assert_eq!(publication_snapshot(paths)?, before, "rejected raw PUT mutated staged or published files");
    Ok(())
}

#[tokio::test]
async fn four_node_artifact_put_seal_waits_for_disconnected_physical_tail() -> TestResult {
    crate::common::rustfs_binary_path_with_features(Some("e2e-test-hooks"));
    init_logging();
    let _ = rustfs_credentials::set_global_rpc_secret("rustfs-internode-signature-e2e-secret".to_owned());
    let rpc_secret = rustfs_credentials::try_get_rpc_token()?;
    let dist = DistCluster::start_with_env(DistLayout::FourNodeFourDisk, &[("RUSTFS_RPC_SECRET", &rpc_secret)]).await?;
    let (target, user) = create_artifact_attempt(&dist).await?;
    let bucket = target["bucket"].as_str().ok_or("target omitted bucket")?;
    let prefix = target["attempt_prefix"].as_str().ok_or("target omitted prefix")?;
    let key = format!("{prefix}detached-put.bin");
    let payload = payload_for(&key, 2 * 1024 * 1024);
    let output = timeout(
        Duration::from_secs(30),
        dist.client(1)?
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(payload.clone().into())
            .send(),
    )
    .await??;
    let version = output.version_id().ok_or("managed PUT omitted version ID")?;
    let request_body = json!({"version": 1, "target": target}).to_string();
    let baselines = dist
        .cluster
        .nodes
        .iter()
        .map(|node| census_object_version_on_disk(Path::new(&node.data_dirs[0]), bucket, &key, Some(version)))
        .collect::<TestResult<Vec<_>>>()?;
    assert!(
        baselines
            .iter()
            .all(|baseline| baseline.is_complete() && !baseline.expected_part_numbers.is_empty())
    );

    let node = &dist.cluster.nodes[3];
    let disk = PathBuf::from(&node.data_dirs[0]).canonicalize()?;
    let object = disk.join(bucket).join(&key);
    let metadata_bytes = std::fs::read(object.join("xl.meta"))?;
    let fi = FileMeta::load(&metadata_bytes)?.into_fileinfo(bucket, &key, version, true, false, true)?;
    let admitted_file_info = serde_json::to_value(&fi)?;
    assert!(!fi.is_healing(), "ordinary publication must not carry the repair marker");
    let data_dir = fi
        .data_dir
        .ok_or("physical-tail fixture requires external shards")?
        .to_string();
    let source = Uuid::new_v4().to_string();
    let staging = disk.join(".rustfs.sys/tmp").join(&source);
    std::fs::create_dir_all(&staging)?;
    let destination = object.join(&data_dir);
    // Replay the receiver publication of a real ordinary PUT while its attempt
    // remains open. Preserve its referenced version metadata. The admitted
    // owned operation may still finish metadata publication after RPC detach.
    std::fs::rename(&destination, staging.join(&data_dir))?;
    let barrier = PhysicalBarrier::arm(&disk, &destination)?;
    let format: Value = serde_json::from_slice(&std::fs::read(disk.join(".rustfs.sys/format.json"))?)?;
    let disk_id = Uuid::parse_str(format["xl"]["this"].as_str().ok_or("disk format omitted xl.this")?)?;
    let incarnation = Uuid::parse_str(target["bucket_incarnation"].as_str().ok_or("target omitted incarnation")?)?;
    let put = RenameDataRequest {
        disk: disk_id.to_string(),
        src_volume: ".rustfs.sys/tmp".to_owned(),
        src_path: source,
        dst_volume: bucket.to_owned(),
        dst_path: key.clone(),
        file_info: serde_json::to_string(&fi)?,
        file_info_bin: Default::default(),
        scanner_publication_lease_token: Default::default(),
        bucket_incarnation_id: incarnation.as_bytes().to_vec().into(),
    };
    let request = signed_put_request(put.clone(), &node.address)?;
    // Direct RPC takes no coordinator attempt lock and owns its own connection.
    let url = node.url.clone();
    let mut rpc = tokio::spawn(async move {
        timeout(Duration::from_secs(30), async move {
            let mut client = NodeServiceClient::connect(url).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(client.artifact_rename_data(request).await?)
        })
        .await
    });
    tokio::select! {
        result = &mut rpc => {
            let response = result???.into_inner();
            panic!(
                "ordinary PUT receiver completed before the physical barrier: success={}, error={:?}",
                response.success, response.error
            );
        }
        arrived = barrier.observed("arrived") => arrived?,
    }
    assert!(!destination.exists(), "physical PUT escaped its barrier");
    rpc.abort();
    assert!(rpc.await.expect_err("disconnect the ordinary PUT waiter").is_cancelled());
    // The ordinary PUT wrapper retains an owned task. Observe its RPC-facing
    // waiter dropping, rather than cancellation of the continuing disk task.
    barrier.observed("rpc-waiter-dropped").await?;

    let seal = user.request(&dist, 0, PUT_SEAL_ROUTE, request_body.clone());
    tokio::pin!(seal);
    let closed = wait_until(
        Duration::from_secs(15),
        || async {
            Ok(walkdir::WalkDir::new(disk.join(".rustfs-artifact-put-seals"))
                .min_depth(2)
                .max_depth(2)
                .into_iter()
                .filter_map(Result::ok)
                .any(|entry| entry.file_name() == "closed"))
        },
        "PUT receiver persisted closing before physical drain",
    );
    tokio::pin!(closed);
    tokio::select! {
        result = &mut seal => {
            let (status, response) = result?;
            panic!("PUT seal returned before its blocked receiver closed admission: {status} {response}");
        }
        result = &mut closed => result?,
    }
    let (status, response) = timeout(Duration::from_secs(40), &mut seal).await??;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "PUT seal confirmed an unfinished physical tail: {response}"
    );
    assert!(response.contains("artifact operation unconfirmed"));
    assert!(!destination.exists());
    assert_raw_put_closed(&put, &node.url, &node.address, &[&staging, &object]).await?;
    assert!(!destination.exists(), "a second admission published while the worker was paused");

    barrier.release()?;
    wait_until(
        Duration::from_secs(15),
        || async { Ok(destination.join("part.1").is_file()) },
        "detached ordinary PUT worker finished",
    )
    .await?;
    confirm_seal(&dist, &user, &request_body, &target).await?;
    // Seal confirmation is the boundary: already admitted work may re-encode
    // metadata before this point, but the complete FileInfo must be unchanged.
    let published_objects: Vec<_> = dist
        .cluster
        .nodes
        .iter()
        .map(|node| PathBuf::from(&node.data_dirs[0]).join(bucket).join(&key))
        .collect();
    let published_paths: Vec<_> = published_objects.iter().map(PathBuf::as_path).collect();
    let sealed_snapshot = publication_snapshot(&published_paths)?;
    let sealed_file_info =
        FileMeta::load(&std::fs::read(object.join("xl.meta"))?)?.into_fileinfo(bucket, &key, version, true, false, true)?;
    assert_eq!(
        serde_json::to_value(sealed_file_info)?,
        admitted_file_info,
        "admitted PUT changed FileInfo semantics"
    );
    for (index, baseline) in baselines.iter().enumerate() {
        assert!(
            census_object_version_on_disk(Path::new(&dist.cluster.nodes[index].data_dirs[0]), bucket, &key, Some(version))?
                .matches_manifest(baseline)
        );
        assert_exact_version(&dist.client(index)?, bucket, &key, version, &payload).await?;
    }

    // Stage the exact request again. A source metadata sentinel makes a missing
    // admission check observable even if the existing destination later causes
    // ENOTEMPTY: ordinary rename rewrites source metadata before that syscall.
    let staged_data = staging.join(&data_dir);
    std::fs::create_dir_all(&staged_data)?;
    for entry in std::fs::read_dir(&destination)? {
        let entry = entry?;
        assert!(entry.file_type()?.is_file(), "unexpected nested shard fixture");
        std::fs::copy(entry.path(), staged_data.join(entry.file_name()))?;
    }
    std::fs::write(staging.join("xl.meta"), b"closed-admission-must-preserve-this-staged-metadata")?;
    assert_raw_put_closed(&put, &node.url, &node.address, &[&staging, &object]).await?;
    let listed = dist
        .client(0)?
        .list_object_versions()
        .bucket(bucket)
        .prefix(&key)
        .send()
        .await?;
    assert_eq!(listed.versions().len(), 1, "raw PUT retries created another version");
    assert_eq!(listed.versions()[0].version_id(), Some(version));
    assert!(listed.delete_markers().is_empty());
    confirm_seal(&dist, &user, &request_body, &target).await?;
    assert!(
        publication_snapshot(&published_paths)? == sealed_snapshot,
        "published files changed after the first successful physical-drain confirmation"
    );
    Ok(())
}
