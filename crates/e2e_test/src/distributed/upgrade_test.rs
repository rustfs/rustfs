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
    unique_bucket, wait_until,
};
use crate::common::{
    AdminTransport, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user_via, init_logging,
};
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

fn source_binary() -> TestResult<PathBuf> {
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
    wait_until(
        CREDENTIAL_TIMEOUT,
        || {
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let body = body.clone();
            async move {
                put_object(&client, &bucket, &key, body).await?;
                Ok(true)
            }
        },
        label,
    )
    .await
}

async fn wait_for_bytes(client: &Client, bucket: &str, key: &str, expected: &[u8], label: &str) -> TestResult {
    wait_until(
        CREDENTIAL_TIMEOUT,
        || {
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let expected = expected.to_vec();
            async move {
                let got = get_object_bytes(&client, &bucket, &key).await?;
                Ok(got == expected)
            }
        },
        label,
    )
    .await
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
            if code == Some("SignatureDoesNotMatch")
                || code == Some("InvalidAccessKeyId")
                || code == Some("AccessDenied")
                || code == Some("InvalidArgument")
            {
            } else if error.raw_response().is_some_and(|response| response.status().as_u16() == 403) {
            } else {
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

#[tokio::test]
async fn four_node_direct_upgrade_preserves_history_and_iam_credentials() -> TestResult {
    init_logging();
    let previous = source_binary()?;
    let mut dist = DistCluster::new_stopped(DistLayout::FourNodeFourDisk).await?;
    capture_upgrade_logs(&mut dist, "direct-upgrade")?;
    dist.start_from_binary(&previous).await?;

    let seed = seed_history_and_iam(&dist).await?;
    dist.restart_with_current_binary().await?;
    assert_history_and_iam(&dist, &seed, "direct").await?;
    Ok(())
}

#[tokio::test]
async fn four_node_rolling_upgrade_preserves_history_and_iam_credentials() -> TestResult {
    init_logging();
    let previous = source_binary()?;
    let mut dist = DistCluster::new_stopped(DistLayout::FourNodeFourDisk).await?;
    capture_upgrade_logs(&mut dist, "rolling-upgrade")?;
    dist.start_from_binary(&previous).await?;

    let seed = seed_history_and_iam(&dist).await?;

    dist.replace_node_with_current_binary(0).await?;
    assert_history_and_iam(&dist, &seed, "one-current-node").await?;

    for node_idx in [1, 2] {
        dist.replace_node_with_current_binary(node_idx).await?;
    }
    assert_history_and_iam(&dist, &seed, "one-previous-node").await?;

    dist.replace_node_with_current_binary(3).await?;
    assert_history_and_iam(&dist, &seed, "homogeneous-current").await?;
    Ok(())
}
