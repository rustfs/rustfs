// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::artifact_seal_test::{assert_exact_version, confirm_seal, create_artifact_attempt};
use super::harness::{DistCluster, DistLayout, TestResult, payload_for, wait_until};
use crate::chaos::census_object_version_on_disk;
use crate::common::init_logging;
use http::StatusCode;
use rustfs_ecstore::api::rpc::{TONIC_RPC_PREFIX, gen_tonic_signature_headers, set_tonic_canonical_body_digest};
use rustfs_filemeta::FileMeta;
use rustfs_protos::proto_gen::node_service::{
    ArtifactRepairRenameDataRequest, RenameDataRequest, node_service_client::NodeServiceClient,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;
use tonic::Request;
use uuid::Uuid;

const REPAIR_SEAL_ROUTE: &str = "/rustfs/admin/v3/plugin-artifacts/seal-repairs";

struct PhysicalBarrier(PathBuf);

impl PhysicalBarrier {
    fn arm(disk: &Path, destination: &Path) -> TestResult<Self> {
        let path = disk.join(".rustfs.sys/e2e-physical-rename-barrier");
        std::fs::write(&path, destination.to_str().ok_or("non-UTF8 fixture destination")?)?;
        Ok(Self(path))
    }

    async fn observed(&self, stage: &str) -> TestResult {
        wait_until(Duration::from_secs(15), || async { Ok(self.0.with_extension(stage).is_file()) }, stage).await
    }

    fn release(&self) -> TestResult {
        std::fs::write(self.0.with_extension("release"), b"release physical worker")?;
        Ok(())
    }
}

impl Drop for PhysicalBarrier {
    fn drop(&mut self) {
        // Release even on assertion failure; the worker also has its own deadline.
        let _ = self.release();
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn four_node_artifact_repair_seal_waits_for_disconnected_physical_tail() -> TestResult {
    crate::common::rustfs_binary_path_with_features(Some("e2e-test-hooks"));
    init_logging();
    // Match the other raw internode signature tests sharing the OnceLock.
    let _ = rustfs_credentials::set_global_rpc_secret("rustfs-internode-signature-e2e-secret".to_owned());
    let rpc_secret = rustfs_credentials::try_get_rpc_token()?;
    let dist = DistCluster::start_with_env(DistLayout::FourNodeFourDisk, &[("RUSTFS_RPC_SECRET", &rpc_secret)]).await?;
    let (target, user) = create_artifact_attempt(&dist).await?;
    let bucket = target["bucket"].as_str().ok_or("target omitted bucket")?;
    let prefix = target["attempt_prefix"].as_str().ok_or("target omitted prefix")?;
    let key = format!("{prefix}detached-rename.bin");
    let body = payload_for(&key, 2 * 1024 * 1024);
    let output = timeout(
        Duration::from_secs(30),
        dist.client(1)?
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(body.clone().into())
            .send(),
    )
    .await??;
    let version = output.version_id().ok_or("managed PUT omitted version ID")?;
    let request_body = json!({"version": 1, "target": target}).to_string();
    confirm_seal(&dist, &user, &request_body, &target).await?;

    let node = &dist.cluster.nodes[3];
    let disk = PathBuf::from(&node.data_dirs[0]).canonicalize()?;
    let object = disk.join(bucket).join(&key);
    let metadata = FileMeta::load(&std::fs::read(object.join("xl.meta"))?)?;
    let mut fi = metadata.into_fileinfo(bucket, &key, version, true, false, true)?;
    fi.set_healing();
    let data_dir = fi
        .data_dir
        .ok_or("physical-tail fixture requires external shard files")?
        .to_string();
    let baseline = census_object_version_on_disk(&disk, bucket, &key, Some(version))?;
    assert!(baseline.is_complete() && !baseline.expected_part_numbers.is_empty());
    let source = Uuid::new_v4().to_string();
    let staging = disk.join(".rustfs.sys/tmp").join(&source);
    std::fs::create_dir_all(&staging)?;
    let destination = object.join(&data_dir);
    std::fs::rename(&destination, staging.join(&data_dir))?;
    let barrier = PhysicalBarrier::arm(&disk, &destination)?;
    let format: Value = serde_json::from_slice(&std::fs::read(disk.join(".rustfs.sys/format.json"))?)?;
    let disk_id = Uuid::parse_str(format["xl"]["this"].as_str().ok_or("disk format omitted xl.this")?)?;
    let incarnation = Uuid::parse_str(target["bucket_incarnation"].as_str().ok_or("target omitted incarnation")?)?;
    let repair = ArtifactRepairRenameDataRequest {
        rename: Some(RenameDataRequest {
            disk: disk_id.to_string(),
            src_volume: ".rustfs.sys/tmp".to_owned(),
            src_path: source,
            dst_volume: bucket.to_owned(),
            dst_path: key.clone(),
            file_info: serde_json::to_string(&fi)?,
            file_info_bin: Default::default(),
            scanner_publication_lease_token: Default::default(),
            bucket_incarnation_id: incarnation.as_bytes().to_vec().into(),
        }),
        expected_disk_id: disk_id.as_bytes().to_vec().into(),
    };
    let canonical = rustfs_protos::canonical_artifact_repair_rename_data_request_body(&repair)?;
    let mut request = Request::new(repair);
    request.set_timeout(Duration::from_secs(15));
    set_tonic_canonical_body_digest(&mut request, &canonical)?;
    let digest = request
        .metadata()
        .get("x-rustfs-content-sha256")
        .ok_or("missing canonical digest")?
        .to_str()?
        .to_owned();
    let headers = gen_tonic_signature_headers(
        &node.address,
        TONIC_RPC_PREFIX.trim_start_matches('/'),
        "ArtifactRepairRenameData",
        Some(&digest),
    )?;
    request.metadata_mut().as_mut().extend(headers);
    // An independent channel and direct receiver RPC take no coordinator attempt
    // lock. Dropping this task closes the client stream while rename is paused.
    let url = node.url.clone();
    let rpc = tokio::spawn(async move {
        timeout(Duration::from_secs(30), async move {
            let mut client = NodeServiceClient::connect(url).await?;
            client
                .artifact_repair_rename_data(request)
                .await
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })
        })
        .await
    });
    barrier.observed("arrived").await?;
    assert!(!destination.exists(), "physical publication ran before its barrier");
    rpc.abort();
    assert!(rpc.await.expect_err("cancel the disconnected RPC waiter").is_cancelled());
    // This acknowledgement is emitted by the receiver's dropped async waiter,
    // not by the client and not by a delayed response.
    barrier.observed("waiter-dropped").await?;

    let seal = user.request(&dist, 0, REPAIR_SEAL_ROUTE, request_body.clone());
    tokio::pin!(seal);
    let closed = wait_until(
        Duration::from_secs(15),
        || async {
            Ok(walkdir::WalkDir::new(disk.join(".rustfs-artifact-repair-seals"))
                .min_depth(2)
                .max_depth(2)
                .into_iter()
                .filter_map(Result::ok)
                .any(|entry| entry.file_name() == "closed"))
        },
        "receiver persisted repair closure while its physical worker remains paused",
    );
    tokio::pin!(closed);
    tokio::select! {
        result = &mut seal => {
            let (status, response) = result?;
            panic!("repair seal returned before the blocked receiver closed admission: {status} {response}");
        }
        result = &mut closed => result?,
    }
    // Cover a complete receiver drain timeout, so early physical-owner release
    // causes a successful response here and fails this runtime assertion.
    let (status, response) = timeout(Duration::from_secs(40), &mut seal).await??;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "seal confirmed an unfinished physical tail: {response}"
    );
    assert!(response.contains("artifact operation unconfirmed"));
    assert!(!destination.exists(), "worker escaped the physical barrier");
    barrier.release()?;
    wait_until(
        Duration::from_secs(15),
        || async { Ok(destination.join("part.1").is_file()) },
        "detached physical rename completes after release",
    )
    .await?;
    wait_until(
        Duration::from_secs(45),
        || async {
            let (status, response) = user.request(&dist, 0, REPAIR_SEAL_ROUTE, request_body.clone()).await?;
            if status == StatusCode::SERVICE_UNAVAILABLE && response.contains("artifact operation unconfirmed") {
                return Ok(false);
            }
            assert_eq!(status, StatusCode::OK, "repair seal did not confirm after physical drain: {response}");
            assert_eq!(
                serde_json::from_str::<Value>(&response)?,
                json!({"version": 1, "target": target, "attempt_repairs_sealed": true})
            );
            Ok(true)
        },
        "exact retry confirms all four receivers after physical drain",
    )
    .await?;
    assert!(census_object_version_on_disk(&disk, bucket, &key, Some(version))?.matches_manifest(&baseline));
    assert_exact_version(&dist.client(0)?, bucket, &key, version, &body).await?;
    Ok(())
}
