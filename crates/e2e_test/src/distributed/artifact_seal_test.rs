// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::harness::{
    DistCluster, DistLayout, TestResult, assert_object_bytes, put_object, s3_probe_client, unique_bucket, wait_for_ready,
    wait_until,
};
use crate::common::{
    AdminTransport, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user_via, admin_request, init_logging,
};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use http::{Method, StatusCode};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

const BUCKET_ROUTE: &str = "/rustfs/admin/v3/plugin-artifacts/buckets";
const INSPECT_ROUTE: &str = "/rustfs/admin/v3/plugin-artifacts/buckets/inspect";
const SEAL_ROUTE: &str = "/rustfs/admin/v3/plugin-artifacts/seal-attempt";
const ADMIN_BUDGET: Duration = Duration::from_secs(45);

pub(super) struct ArtifactAdmin {
    access_key: String,
    secret_key: String,
}

impl ArtifactAdmin {
    pub(super) async fn request(
        &self,
        dist: &DistCluster,
        node: usize,
        route: &str,
        body: String,
    ) -> TestResult<(StatusCode, String)> {
        timeout(
            ADMIN_BUDGET,
            admin_request(
                &dist.cluster.nodes[node].url,
                Method::POST,
                route,
                Some(body),
                &self.access_key,
                &self.secret_key,
            ),
        )
        .await?
    }
}

async fn create_artifact_admin(dist: &DistCluster, bucket: &str, prefix: &str) -> TestResult<ArtifactAdmin> {
    let user = ArtifactAdmin {
        access_key: format!("artifact{}", &Uuid::new_v4().simple().to_string()[..12]),
        secret_key: Uuid::new_v4().simple().to_string(),
    };
    let policy_name = format!("artifact{}", &Uuid::new_v4().simple().to_string()[..12]);
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["admin:CreatePluginArtifactBucket", "admin:GetPluginArtifactBucket"],
                "Resource": format!("arn:aws:s3:::{bucket}"),
            },
            {
                "Effect": "Allow",
                "Action": ["admin:SealPluginArtifactAttempt", "admin:SealPluginArtifactRepairs", "admin:SealPluginArtifactWrites"],
                "Resource": format!("arn:aws:s3:::{bucket}/{}", prefix.trim_end_matches('/')),
            },
        ],
    })
    .to_string();
    let url = &dist.cluster.nodes[0].url;
    let access = &dist.cluster.access_key;
    let secret = &dist.cluster.secret_key;
    admin_create_user_via(AdminTransport::Signed, url, access, secret, &user.access_key, &user.secret_key).await?;
    admin_add_canned_policy_via(AdminTransport::Signed, url, access, secret, &policy_name, &policy).await?;
    admin_attach_user_policy_via(AdminTransport::Signed, url, access, secret, &policy_name, &user.access_key).await?;
    Ok(user)
}

pub(super) async fn create_artifact_attempt(dist: &DistCluster) -> TestResult<(Value, ArtifactAdmin)> {
    let bucket = unique_bucket("artifact");
    let prefix = format!("outputs/6169/{}/{}/", Uuid::new_v4(), Uuid::new_v4());
    let user = create_artifact_admin(dist, &bucket, &prefix).await?;
    let body = json!({"version": 1, "bucket": bucket, "base_prefix": "outputs/"}).to_string();
    let (status, response) = user.request(dist, 0, BUCKET_ROUTE, body).await?;
    assert_eq!(status, StatusCode::OK, "artifact bucket creation failed: {response}");
    let receipt: Value = serde_json::from_str(&response)?;
    assert_eq!(receipt["version"], 1);
    assert_eq!(receipt["bucket"], bucket);
    assert_eq!(receipt["base_prefix"], "outputs/");
    assert_eq!(receipt["versioning_enabled"], true);
    let incarnation = receipt["bucket_incarnation"]
        .as_str()
        .ok_or("artifact receipt omitted incarnation")?;
    assert!(!Uuid::parse_str(incarnation)?.is_nil());
    let target = json!({
        "bucket": bucket,
        "bucket_incarnation": incarnation,
        "attempt_prefix": prefix,
    });
    assert_bucket_identity(dist, &user, 1, &target).await?;
    Ok((target, user))
}

async fn assert_bucket_identity(dist: &DistCluster, user: &ArtifactAdmin, node: usize, target: &Value) -> TestResult {
    let body = json!({"version": 1, "bucket": target["bucket"], "base_prefix": "outputs/"}).to_string();
    wait_until(
        ADMIN_BUDGET,
        || async {
            let (status, response) = user.request(dist, node, INSPECT_ROUTE, body.clone()).await?;
            // IAM peers may not have received the new user and policy yet.
            // Storage failures must fail immediately, including the old gate's 503.
            if matches!(status, StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED)
                && (response.contains("<Code>AccessDenied</Code>") || response.contains("<Code>InvalidAccessKeyId</Code>"))
            {
                return Ok(false);
            }
            assert_eq!(status, StatusCode::OK, "cross-node artifact inspect failed: {response}");
            assert_eq!(
                serde_json::from_str::<Value>(&response)?,
                json!({
                    "version": 1,
                    "bucket": target["bucket"],
                    "base_prefix": "outputs/",
                    "bucket_incarnation": target["bucket_incarnation"],
                    "versioning_enabled": true,
                }),
                "inspect must return the created managed bucket identity"
            );
            Ok(true)
        },
        "artifact IAM user and policy propagation",
    )
    .await
}

pub(super) async fn confirm_seal(dist: &DistCluster, user: &ArtifactAdmin, request: &str, target: &Value) -> TestResult {
    wait_until(
        ADMIN_BUDGET,
        || async {
            let (status, response) = user.request(dist, 0, SEAL_ROUTE, request.to_owned()).await?;
            // HTTP readiness does not confirm that remote disk handles have
            // recovered. Retry only the API's explicit same-identity response.
            if status == StatusCode::SERVICE_UNAVAILABLE
                && response.contains("<Code>ServiceUnavailable</Code>")
                && response.contains("<Message>artifact operation unconfirmed; retry the same identity</Message>")
            {
                return Ok(false);
            }
            assert_eq!(status, StatusCode::OK, "artifact seal failed: {response}");
            let receipt: Value = serde_json::from_str(&response)?;
            assert_eq!(receipt, json!({"version": 1, "target": target, "ordinary_put_sealed": true}));
            Ok(true)
        },
        "exact artifact seal confirmation after receiver recovery",
    )
    .await
}

async fn assert_seal_unconfirmed(dist: &DistCluster, user: &ArtifactAdmin, request: &str) -> TestResult {
    let (status, body) = user.request(dist, 0, SEAL_ROUTE, request.to_owned()).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "offline receiver seal response: {body}");
    assert!(
        body.contains("<Code>ServiceUnavailable</Code>"),
        "seal failure lost its service code: {body}"
    );
    assert!(
        body.contains("artifact operation unconfirmed"),
        "seal failure lost its retry contract: {body}"
    );
    Ok(())
}

pub(super) async fn assert_exact_version(client: &Client, bucket: &str, key: &str, version: &str, body: &[u8]) -> TestResult {
    let output = timeout(
        Duration::from_secs(30),
        client.get_object().bucket(bucket).key(key).version_id(version).send(),
    )
    .await??;
    assert_eq!(output.version_id(), Some(version));
    let actual = timeout(Duration::from_secs(30), output.body.collect()).await??.into_bytes();
    assert_eq!(actual.as_ref(), body);
    Ok(())
}

async fn assert_put_denied(client: &Client, ordinary_bucket: &str, bucket: &str, key: &str) -> TestResult {
    // The same receiver must still accept a normal write before its sealed probe.
    put_object(client, ordinary_bucket, key, b"ordinary-control".to_vec()).await?;
    assert_object_bytes(client, ordinary_bucket, key, b"ordinary-control").await?;
    let client = s3_probe_client(client);
    let result = timeout(
        Duration::from_secs(30),
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(b"must-not-publish".to_vec().into())
            .send(),
    )
    .await?;
    let error = result.expect_err("sealed artifact attempt accepted a new PUT");
    let service = error
        .as_service_error()
        .ok_or("sealed PUT failed without a service rejection")?;
    assert_eq!(
        error.raw_response().map(|response| response.status().as_u16()),
        Some(500),
        "sealed PUT must return its current HTTP status"
    );
    assert_eq!(service.code(), Some("InternalError"), "sealed PUT must return its current service code");
    // StorageError::Io is redacted at the API boundary; the sealed reason is
    // not exposed. Success controls and exact inventory checks prove the effect.
    assert_eq!(
        service.message(),
        Some("We encountered an internal error, please try again."),
        "sealed PUT must retain the public redacted message"
    );
    Ok(())
}

#[tokio::test]
async fn four_node_artifact_put_seal_survives_full_restart() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourNodeFourDisk).await?;
    let (target, user) = create_artifact_attempt(&dist).await?;
    let bucket = target["bucket"].as_str().ok_or("target omitted bucket")?;
    let prefix = target["attempt_prefix"].as_str().ok_or("target omitted attempt prefix")?;
    let key = format!("{prefix}result.bin");
    let mut versions = Vec::new();
    for (node, body) in [(1, b"first-version".as_slice()), (2, b"second-version".as_slice())] {
        let client = dist.client(node)?;
        let output = timeout(
            Duration::from_secs(30),
            client.put_object().bucket(bucket).key(&key).body(body.to_vec().into()).send(),
        )
        .await??;
        let version = output.version_id().ok_or("artifact PUT omitted version ID")?.to_owned();
        assert!(!Uuid::parse_str(&version)?.is_nil());
        assert_exact_version(&dist.client(3)?, bucket, &key, &version, body).await?;
        versions.push((version, body.to_vec()));
    }
    assert_ne!(versions[0].0, versions[1].0);
    let ordinary = unique_bucket("ordinary");
    dist.create_bucket(&ordinary).await?;
    put_object(&dist.client(2)?, &ordinary, "normal.bin", b"before-seal".to_vec()).await?;
    assert_object_bytes(&dist.client(3)?, &ordinary, "normal.bin", b"before-seal").await?;

    let request = json!({"version": 1, "target": target}).to_string();
    confirm_seal(&dist, &user, &request, &target).await?;
    let other_prefix = format!("outputs/6169/{}/{}/", Uuid::new_v4(), Uuid::new_v4());
    for phase in ["sealed", "restarted"] {
        if phase == "restarted" {
            dist.cluster.stop();
            dist.cluster.start().await?;
            wait_for_ready(&dist.cluster).await?;
        }
        assert_bucket_identity(&dist, &user, 2, &target).await?;
        for node in 0..dist.cluster.nodes.len() {
            // A second attempt in the managed bucket must remain writable;
            // a broken bucket-wide write path cannot stand in for sealing.
            let client = dist.client(node)?;
            let other_key = format!("{other_prefix}{phase}-{node}.bin");
            let output = timeout(
                Duration::from_secs(30),
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&other_key)
                    .body(b"other-attempt".to_vec().into())
                    .send(),
            )
            .await??;
            let version = output.version_id().ok_or("other attempt PUT omitted version ID")?;
            assert!(!Uuid::parse_str(version)?.is_nil());
            assert_exact_version(
                &dist.client((node + 1) % dist.cluster.nodes.len())?,
                bucket,
                &other_key,
                version,
                b"other-attempt",
            )
            .await?;
            assert_put_denied(&dist.client(node)?, &ordinary, bucket, &format!("{prefix}{phase}-{node}.bin")).await?;
            for (version, body) in &versions {
                assert_exact_version(&dist.client(node)?, bucket, &key, version, body).await?;
            }
        }
        let listed = dist
            .client(0)?
            .list_object_versions()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await?;
        assert_eq!(listed.versions().len(), 2, "denied PUT left an artifact version: {listed:?}");
        assert!(listed.delete_markers().is_empty(), "artifact seal created a delete marker");
        put_object(&dist.client(1)?, &ordinary, "normal.bin", phase.as_bytes().to_vec()).await?;
        assert_object_bytes(&dist.client(2)?, &ordinary, "normal.bin", phase.as_bytes()).await?;
    }
    Ok(())
}

#[tokio::test]
async fn four_node_artifact_seal_requires_every_configured_receiver_on_exact_retry() -> TestResult {
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourNodeFourDisk).await?;
    let (target, user) = create_artifact_attempt(&dist).await?;
    let bucket = target["bucket"].as_str().ok_or("target omitted bucket")?;
    let prefix = target["attempt_prefix"].as_str().ok_or("target omitted attempt prefix")?;
    let key = format!("{prefix}existing.bin");
    let output = dist
        .client(1)?
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(b"existing".to_vec().into())
        .send()
        .await?;
    let version = output.version_id().ok_or("artifact PUT omitted version ID")?.to_owned();
    assert_exact_version(&dist.client(3)?, bucket, &key, &version, b"existing").await?;
    let ordinary = unique_bucket("ordinary");
    dist.create_bucket(&ordinary).await?;
    let request = json!({"version": 1, "target": target}).to_string();

    // The second iteration starts from a fully acknowledged global seal. Its
    // persisted record cannot replace a fresh acknowledgement from every disk.
    for _ in 0..2 {
        dist.cluster.stop_node(3)?;
        assert_seal_unconfirmed(&dist, &user, &request).await?;
        dist.cluster.start_node(3).await?;
        wait_for_ready(&dist.cluster).await?;
        confirm_seal(&dist, &user, &request, &target).await?;
    }
    assert_put_denied(&dist.client(3)?, &ordinary, bucket, &format!("{prefix}late.bin")).await?;
    assert_exact_version(&dist.client(2)?, bucket, &key, &version, b"existing").await?;
    let listed = timeout(
        Duration::from_secs(30),
        dist.client(0)?.list_object_versions().bucket(bucket).prefix(prefix).send(),
    )
    .await??;
    assert_eq!(listed.versions().len(), 1, "rejected PUT added a version");
    assert!(listed.delete_markers().is_empty(), "seal added a delete marker");
    Ok(())
}
