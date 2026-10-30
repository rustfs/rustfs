// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! 4-node upgrade coverage for historical objects and IAM AK/SK.
//!
//! Complements `upgrade_compatibility_test` (single-node SSE/multipart and
//! mixed-version listing). This module pins the distributed contract the
//! hardware upgrade chain is meant to catch: after a 4-node upgrade, objects
//! written on the previous release still read back, and IAM user credentials
//! created before the upgrade still authenticate.
//!
//! Requires `RUSTFS_UPGRADE_SOURCE_BINARY` pointing at the pinned previous
//! release. The `e2e-distributed` workflow downloads that binary; a local run
//! without it fails closed rather than skipping.

use super::harness::{
    DistCluster, DistLayout, TestResult, assert_object_bytes, cluster_admin_ok, enable_versioning, get_object_bytes, put_object,
    s3_probe_client, s3_probe_error_code, sha256_hex, unique_bucket, wait_until,
};
use crate::common::{
    AdminTransport, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user_via, init_logging,
};
use crate::upgrade_compatibility_test::wait_for_upgrade_write_readiness;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

const SOURCE_BINARY_ENV: &str = "RUSTFS_UPGRADE_SOURCE_BINARY";
const IAM_SECRET: &str = "UpgradeTestSecretKey1";
const WRONG_SECRET: &str = "WrongSecretKey000000";
const CREDENTIAL_TIMEOUT: Duration = Duration::from_secs(30);

struct UpgradeSeed {
    history_bucket: String,
    history_key: &'static str,
    history_body: Vec<u8>,
    multipart_upload: String,
    multipart_etag: String,
    multipart_body: Vec<u8>,
    versioned_bucket: String,
    versioned_key: &'static str,
    version1: String,
    version1_body: Vec<u8>,
    version2: String,
    version2_body: Vec<u8>,
    iam_bucket: String,
    iam_key: &'static str,
    iam_body: Vec<u8>,
    iam_user: String,
    iam_secret: &'static str,
}

pub(super) fn source_binary() -> TestResult<PathBuf> {
    let path = std::env::var_os(SOURCE_BINARY_ENV).map(PathBuf::from).ok_or_else(|| {
        format!(
            "{SOURCE_BINARY_ENV} must point to the pinned previous release binary (the e2e-distributed workflow downloads it)"
        )
    })?;
    if !path.is_file() {
        return Err(format!("upgrade source binary does not exist: {}", path.display()).into());
    }
    Ok(path)
}

fn capture_upgrade_logs(cluster: &mut DistCluster, label: &str) -> TestResult {
    let Some(log_dir) = std::env::var_os("RUSTFS_E2E_LOG_DIR") else {
        return Ok(());
    };
    std::fs::create_dir_all(&log_dir)?;
    for node_idx in 0..cluster.cluster.nodes.len() {
        let path = Path::new(&log_dir).join(format!("{label}-node-{node_idx}.log"));
        cluster
            .cluster
            .set_node_capture_log_path(node_idx, path.to_string_lossy().into_owned())?;
    }
    Ok(())
}

fn iam_rw_policy(bucket: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": [
                format!("arn:aws:s3:::{bucket}"),
                format!("arn:aws:s3:::{bucket}/*")
            ]
        }]
    })
    .to_string()
}

async fn create_iam_user(dist: &DistCluster, user: &str, secret: &str, policy_name: &str, bucket: &str) -> TestResult {
    let url = &dist.cluster.nodes[0].url;
    let access = &dist.cluster.access_key;
    let admin_secret = &dist.cluster.secret_key;
    admin_create_user_via(AdminTransport::Signed, url, access, admin_secret, user, secret).await?;
    admin_add_canned_policy_via(AdminTransport::Signed, url, access, admin_secret, policy_name, &iam_rw_policy(bucket)).await?;
    admin_attach_user_policy_via(AdminTransport::Signed, url, access, admin_secret, policy_name, user).await?;
    Ok(())
}

async fn wait_for_put(client: &Client, bucket: &str, key: &str, body: Vec<u8>, label: &str) -> TestResult {
    let client = s3_probe_client(client);
    wait_until(
        CREDENTIAL_TIMEOUT,
        || {
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let body = body.clone();
            async move {
                match put_object(&client, &bucket, &key, body).await {
                    Ok(()) => Ok(true),
                    Err(error)
                        if matches!(
                            s3_probe_error_code(error.as_ref()),
                            Some("AccessDenied" | "InvalidAccessKeyId" | "SlowDown" | "ServiceUnavailable")
                        ) =>
                    {
                        Ok(false)
                    }
                    Err(error) => Err(error),
                }
            }
        },
        label,
    )
    .await
}

async fn wait_for_bytes(client: &Client, bucket: &str, key: &str, expected: &[u8], label: &str) -> TestResult {
    let client = s3_probe_client(client);
    wait_until(
        CREDENTIAL_TIMEOUT,
        || {
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let expected = expected.to_vec();
            async move {
                match get_object_bytes(&client, &bucket, &key).await {
                    Ok(got) if got == expected => Ok(true),
                    Ok(got) => Err(format!(
                        "{label}: object {bucket}/{key} bytes mismatch: expected sha256={} got sha256={}",
                        sha256_hex(&expected),
                        sha256_hex(&got)
                    )
                    .into()),
                    Err(error)
                        if matches!(
                            s3_probe_error_code(error.as_ref()),
                            Some("AccessDenied" | "InvalidAccessKeyId" | "SlowDown" | "ServiceUnavailable")
                        ) =>
                    {
                        Ok(false)
                    }
                    Err(error) => Err(error),
                }
            }
        },
        label,
    )
    .await
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use crate::common::build_test_s3_config;
    use crate::fake_s3_target::{FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3Target, FaultAction, Operation};

    #[tokio::test]
    async fn iam_polls_only_retry_auth_propagation_and_availability() -> TestResult {
        let target = FakeS3Target::start().await?;
        target.create_bucket("upgrade-retry-probe");
        let client = Client::from_conf(build_test_s3_config(
            target.endpoint(),
            FAKE_ACCESS_KEY,
            FAKE_SECRET_KEY,
            None,
            "upgrade-retry-test",
        ));

        for status in [403, 503] {
            target.inject(Operation::PutObject, FaultAction::ResponseStatus(status), 1);
            wait_for_put(&client, "upgrade-retry-probe", "key", b"body".to_vec(), "IAM PUT").await?;
            assert_eq!(target.take_requests().len(), 2);

            target.inject(Operation::GetObject, FaultAction::ResponseStatus(status), 1);
            wait_for_bytes(&client, "upgrade-retry-probe", "key", b"body", "IAM GET").await?;
            assert_eq!(target.take_requests().len(), 2);
        }

        target.inject(Operation::PutObject, FaultAction::ResponseStatus(500), 1);
        let error = wait_for_put(&client, "upgrade-retry-probe", "key", b"body".to_vec(), "IAM PUT")
            .await
            .expect_err("IAM convergence must not hide InternalError on PUT");
        assert_eq!(s3_probe_error_code(error.as_ref()), Some("InternalError"));
        assert_eq!(target.take_requests().len(), 1);

        target.inject(Operation::GetObject, FaultAction::ResponseStatus(500), 1);
        let error = wait_for_bytes(&client, "upgrade-retry-probe", "key", b"body", "IAM GET")
            .await
            .expect_err("IAM convergence must not hide InternalError on GET");
        assert_eq!(s3_probe_error_code(error.as_ref()), Some("InternalError"));
        assert_eq!(target.take_requests().len(), 1);

        let error = wait_for_bytes(&client, "upgrade-retry-probe", "key", b"wrong bytes", "IAM GET")
            .await
            .expect_err("IAM convergence must not hide a payload mismatch");
        assert!(error.to_string().contains("bytes mismatch"));
        assert_eq!(target.requests().len(), 1);
        Ok(())
    }
}

async fn seed_history_and_iam(dist: &DistCluster) -> TestResult<UpgradeSeed> {
    let history_bucket = unique_bucket("upg-hist");
    let versioned_bucket = unique_bucket("upg-ver");
    let iam_bucket = unique_bucket("upg-iam");
    dist.create_bucket(&history_bucket).await?;
    dist.create_bucket(&versioned_bucket).await?;
    dist.create_bucket(&iam_bucket).await?;

    let root = dist.client(0)?;
    enable_versioning(&root, &versioned_bucket).await?;

    let history_key = "plain-history.bin";
    let history_body = b"written by the previous 4-node release".to_vec();
    put_object(&root, &history_bucket, history_key, history_body.clone()).await?;

    let multipart_body = vec![0x5a; 4096];
    let multipart = root
        .create_multipart_upload()
        .bucket(&history_bucket)
        .key("legacy-multipart.bin")
        .send()
        .await?;
    let multipart_upload = multipart.upload_id().ok_or("legacy multipart upload ID missing")?.to_owned();
    let part = root
        .upload_part()
        .bucket(&history_bucket)
        .key("legacy-multipart.bin")
        .upload_id(&multipart_upload)
        .part_number(1)
        .body(aws_sdk_s3::primitives::ByteStream::from(multipart_body.clone()))
        .send()
        .await?;
    let multipart_etag = part.e_tag().ok_or("legacy multipart part ETag missing")?.to_owned();

    let versioned_key = "versioned-history.txt";
    let version1_body = b"version-one-before-upgrade".to_vec();
    let version1 = root
        .put_object()
        .bucket(&versioned_bucket)
        .key(versioned_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(version1_body.clone()))
        .send()
        .await?
        .version_id()
        .ok_or("first versioned PUT omitted version ID")?
        .to_string();
    let version2_body = b"version-two-before-upgrade".to_vec();
    let version2 = root
        .put_object()
        .bucket(&versioned_bucket)
        .key(versioned_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(version2_body.clone()))
        .send()
        .await?
        .version_id()
        .ok_or("second versioned PUT omitted version ID")?
        .to_string();

    let iam_user = format!("upg{}", &Uuid::new_v4().simple().to_string()[..8]);
    let policy_name = format!("upgpol{}", &Uuid::new_v4().simple().to_string()[..8]);
    create_iam_user(dist, &iam_user, IAM_SECRET, &policy_name, &iam_bucket).await?;

    let iam_key = "iam-history.bin";
    let iam_body = b"written with pre-upgrade IAM AK/SK".to_vec();
    let iam_client = dist.client_with_credentials(1, &iam_user, IAM_SECRET)?;
    wait_for_put(&iam_client, &iam_bucket, iam_key, iam_body.clone(), "IAM user PUT before upgrade").await?;

    Ok(UpgradeSeed {
        history_bucket,
        history_key,
        history_body,
        multipart_upload,
        multipart_etag,
        multipart_body,
        versioned_bucket,
        versioned_key,
        version1,
        version1_body,
        version2,
        version2_body,
        iam_bucket,
        iam_key,
        iam_body,
        iam_user,
        iam_secret: IAM_SECRET,
    })
}

async fn assert_history_and_iam(dist: &DistCluster, seed: &UpgradeSeed, context: &str) -> TestResult {
    let root_a = dist.client(0)?;
    let root_b = dist.client(3)?;
    wait_for_bytes(
        &root_b,
        &seed.history_bucket,
        seed.history_key,
        &seed.history_body,
        &format!("{context}: root GET historical object"),
    )
    .await?;
    assert_object_bytes(&root_a, &seed.history_bucket, seed.history_key, &seed.history_body).await?;
    // During mixed-version operation this is read-only; mutation resumes only
    // after the whole fleet is upgraded.
    for client in [&root_a, &root_b] {
        let listed = client
            .list_parts()
            .bucket(&seed.history_bucket)
            .key("legacy-multipart.bin")
            .upload_id(&seed.multipart_upload)
            .send()
            .await?;
        assert_eq!(listed.parts().len(), 1, "{context}: legacy part listing");
        assert_eq!(listed.parts()[0].part_number(), Some(1));
        assert_eq!(listed.parts()[0].size(), Some(4096));
        assert_eq!(listed.parts()[0].e_tag(), Some(seed.multipart_etag.as_str()));
    }

    let v1 = root_b
        .get_object()
        .bucket(&seed.versioned_bucket)
        .key(seed.versioned_key)
        .version_id(&seed.version1)
        .send()
        .await?;
    let v1_body = v1.body.collect().await?.into_bytes();
    if v1_body.as_ref() != seed.version1_body.as_slice() {
        return Err(format!("{context}: version 1 bytes changed after upgrade").into());
    }
    let v2 = root_a
        .get_object()
        .bucket(&seed.versioned_bucket)
        .key(seed.versioned_key)
        .version_id(&seed.version2)
        .send()
        .await?;
    let v2_body = v2.body.collect().await?.into_bytes();
    if v2_body.as_ref() != seed.version2_body.as_slice() {
        return Err(format!("{context}: version 2 bytes changed after upgrade").into());
    }

    let users = cluster_admin_ok(&dist.cluster, http::Method::GET, "/rustfs/admin/v3/list-users", None).await?;
    if !users.contains(&seed.iam_user) {
        return Err(format!("{context}: list-users lost IAM user {}: {users}", seed.iam_user).into());
    }

    let iam_on_upgraded = dist.client_with_credentials(0, &seed.iam_user, seed.iam_secret)?;
    let iam_on_peer = dist.client_with_credentials(3, &seed.iam_user, seed.iam_secret)?;
    wait_for_bytes(
        &iam_on_upgraded,
        &seed.iam_bucket,
        seed.iam_key,
        &seed.iam_body,
        &format!("{context}: IAM GET historical object on node 0"),
    )
    .await?;
    wait_for_bytes(
        &iam_on_peer,
        &seed.iam_bucket,
        seed.iam_key,
        &seed.iam_body,
        &format!("{context}: IAM GET historical object on node 3"),
    )
    .await?;

    let post_key = format!("after-upgrade-{context}.txt");
    let post_body = format!("{context}: written with the same IAM AK/SK after upgrade").into_bytes();
    wait_for_put(
        &iam_on_peer,
        &seed.iam_bucket,
        &post_key,
        post_body.clone(),
        &format!("{context}: IAM PUT after upgrade"),
    )
    .await?;
    assert_object_bytes(&iam_on_upgraded, &seed.iam_bucket, &post_key, &post_body).await?;

    let bad = dist.client_with_credentials(1, &seed.iam_user, WRONG_SECRET)?;
    match bad.get_object().bucket(&seed.iam_bucket).key(seed.iam_key).send().await {
        Ok(_) => return Err(format!("{context}: wrong secret must not read the IAM object").into()),
        Err(error) => {
            let code = error.as_service_error().and_then(ProvideErrorMetadata::code);
            let rejected = code == Some("SignatureDoesNotMatch")
                || code == Some("InvalidAccessKeyId")
                || code == Some("AccessDenied")
                || code == Some("InvalidArgument")
                || error.raw_response().is_some_and(|response| response.status().as_u16() == 403);
            if !rejected {
                return Err(format!("{context}: wrong secret failed with unexpected error {error:?}").into());
            }
        }
    }

    let post_root_key = format!("root-after-{context}.bin");
    let post_root_body = format!("{context}: root write after upgrade").into_bytes();
    put_object(&root_a, &seed.history_bucket, &post_root_key, post_root_body.clone()).await?;
    assert_object_bytes(&root_b, &seed.history_bucket, &post_root_key, &post_root_body).await?;
    Ok(())
}

async fn compare_degraded_legacy_list_parts(dist: &mut DistCluster, seed: &UpgradeSeed, previous: &Path) -> TestResult {
    let decoded = String::from_utf8(base64_simd::URL_SAFE_NO_PAD.decode_to_vec(seed.multipart_upload.as_bytes())?)?;
    let (_, upload_dir) = decoded.split_once('.').ok_or("legacy upload ID lacks deployment delimiter")?;
    let hash = sha256_hex(format!("{}/legacy-multipart.bin", seed.history_bucket).as_bytes());
    let root = format!("{hash}/{upload_dir}");
    let mut held = Vec::new();
    let mut original_files = Vec::new();
    for (index, node) in dist.cluster.nodes.iter().enumerate() {
        let path = Path::new(&node.data_dirs[0])
            .join(crate::storage_api::multipart_fence_acceptance::RUSTFS_META_MULTIPART_BUCKET)
            .join(&root);
        let entries = std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        let dirs = entries
            .into_iter()
            .map(|entry| entry.path())
            .filter(|entry| entry.join("part.1").is_file() && entry.join("part.1.meta").is_file())
            .collect::<Vec<_>>();
        assert_eq!(dirs.len(), 1, "each configured disk must hold the original part before injection");
        let source_root = dirs[0].parent().ok_or("part directory has no upload root")?;
        for path in [
            source_root.join("xl.meta"),
            dirs[0].join("part.1"),
            dirs[0].join("part.1.meta"),
        ] {
            original_files.push((path.clone(), std::fs::read(path)?));
        }
        if index < 3 {
            held.push((dirs[0].clone(), dirs[0].with_extension("held-for-mixed-test")));
        }
    }
    dist.cluster.stop_node(3)?;
    for (original, temporary) in &held {
        std::fs::rename(original, temporary)?;
    }

    let clients = dist.clients()?;
    let current = tokio::time::timeout(
        CREDENTIAL_TIMEOUT,
        clients[0]
            .list_parts()
            .bucket(&seed.history_bucket)
            .key("legacy-multipart.bin")
            .upload_id(&seed.multipart_upload)
            .send(),
    )
    .await?;
    let legacy = tokio::time::timeout(
        CREDENTIAL_TIMEOUT,
        clients[1]
            .list_parts()
            .bucket(&seed.history_bucket)
            .key("legacy-multipart.bin")
            .upload_id(&seed.multipart_upload)
            .send(),
    )
    .await?;
    for (original, temporary) in &held {
        std::fs::rename(temporary, original)?;
    }
    // Three online disks retain metadata quorum but have no part directory;
    // the fourth disk is offline. A legacy empty response is not a proof that
    // the unavailable participant has no part.
    let legacy = legacy.expect("previous coordinator should expose its legacy empty-list behavior");
    assert!(legacy.parts().is_empty());
    let current = current.expect_err("current coordinator must not infer empty from missing directories and offline disks");
    assert_eq!(current.raw_response().expect("current S3 response").status().as_u16(), 503);
    assert_eq!(current.as_service_error().and_then(|error| error.meta().code()), Some("SlowDownRead"));
    dist.cluster.start_node_from_binary(3, previous).await?;
    wait_for_upgrade_write_readiness(&clients, &seed.history_bucket, "mixed-list-restored", CREDENTIAL_TIMEOUT).await?;
    for (path, before) in original_files {
        assert_eq!(
            std::fs::read(&path)?,
            before,
            "the fault must preserve every original source file: {path:?}"
        );
    }
    for client in [&clients[0], &clients[1]] {
        let listed = client
            .list_parts()
            .bucket(&seed.history_bucket)
            .key("legacy-multipart.bin")
            .upload_id(&seed.multipart_upload)
            .send()
            .await?;
        assert_eq!(listed.parts().len(), 1);
        assert_eq!(listed.parts()[0].e_tag(), Some(seed.multipart_etag.as_str()));
    }
    eprintln!(
        "MPU mixed degraded: one offline disk, three missing part directories; previous returned empty, current returned 503 SlowDownRead; original part restored on both"
    );
    Ok(())
}

async fn complete_legacy_multipart(dist: &DistCluster, seed: &UpgradeSeed) -> TestResult {
    // Native legacy-source upgrade needs every original participant, not just
    // the ordinary three-of-four write quorum proven by the earlier probes.
    let clients = dist.clients()?;
    crate::upgrade_compatibility_test::wait_for_drive_readmission(
        &clients,
        &seed.history_bucket,
        "legacy-multipart-all-drives",
        CREDENTIAL_TIMEOUT,
    )
    .await?;
    let root = &clients[0];
    root.complete_multipart_upload()
        .bucket(&seed.history_bucket)
        .key("legacy-multipart.bin")
        .upload_id(&seed.multipart_upload)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(&seed.multipart_etag)
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;
    for client in &clients {
        assert_object_bytes(client, &seed.history_bucket, "legacy-multipart.bin", &seed.multipart_body).await?;
    }
    Ok(())
}

#[tokio::test]
async fn four_node_direct_upgrade_preserves_history_and_iam_credentials() -> TestResult {
    init_logging();
    let previous = source_binary()?;
    let mut dist = DistCluster::new_stopped(DistLayout::FourNodeFourDisk).await?;
    dist.cluster.set_env("RUSTFS_STORAGE_CLASS_RRS", "EC:0");
    capture_upgrade_logs(&mut dist, "direct-upgrade")?;
    dist.start_from_binary(&previous).await?;

    let seed = seed_history_and_iam(&dist).await?;
    dist.restart_with_current_binary().await?;
    assert_history_and_iam(&dist, &seed, "direct").await?;
    complete_legacy_multipart(&dist, &seed).await?;
    Ok(())
}

async fn replace_node_and_wait_for_write_readiness(dist: &mut DistCluster, seed: &UpgradeSeed, node_idx: usize) -> TestResult {
    dist.replace_node_with_current_binary(node_idx).await?;
    // Node readiness proves reads can succeed; a rolling writer must also
    // recover before replacing the next peer or checking upgrade contracts.
    wait_for_upgrade_write_readiness(
        &dist.clients()?,
        &seed.history_bucket,
        &format!("replaced-node-{node_idx}"),
        CREDENTIAL_TIMEOUT,
    )
    .await
}

#[tokio::test]
async fn four_node_rolling_upgrade_preserves_history_and_iam_credentials() -> TestResult {
    init_logging();
    let previous = source_binary()?;
    let mut dist = DistCluster::new_stopped(DistLayout::FourNodeFourDisk).await?;
    dist.cluster.set_env("RUSTFS_STORAGE_CLASS_RRS", "EC:0");
    capture_upgrade_logs(&mut dist, "rolling-upgrade")?;
    dist.start_from_binary(&previous).await?;

    let seed = seed_history_and_iam(&dist).await?;

    replace_node_and_wait_for_write_readiness(&mut dist, &seed, 0).await?;
    assert_history_and_iam(&dist, &seed, "one-current-node").await?;
    compare_degraded_legacy_list_parts(&mut dist, &seed, &previous).await?;

    for node_idx in [1, 2] {
        replace_node_and_wait_for_write_readiness(&mut dist, &seed, node_idx).await?;
    }
    assert_history_and_iam(&dist, &seed, "one-previous-node").await?;

    replace_node_and_wait_for_write_readiness(&mut dist, &seed, 3).await?;
    assert_history_and_iam(&dist, &seed, "homogeneous-current").await?;
    complete_legacy_multipart(&dist, &seed).await?;
    Ok(())
}
