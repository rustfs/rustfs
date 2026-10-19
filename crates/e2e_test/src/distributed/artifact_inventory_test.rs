// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

#![cfg(unix)]

use super::artifact_delete_test::physical_snapshot;
use super::artifact_seal_test::{ArtifactAdmin, create_artifact_attempt};
use super::harness::{DistCluster, DistLayout, TestResult, wait_for_ready};
use crate::common::{
    AdminTransport, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user_via, init_logging,
    signed_s3_request,
};
use futures::{StreamExt as _, TryStreamExt as _};
use http::{Method, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};
use tokio::time::timeout;
use uuid::Uuid;

const ROUTE: &str = "/rustfs/admin/v3/plugin-artifacts/inventory";
const BUDGET: Duration = Duration::from_secs(45);

struct Reader {
    access: String,
    secret: String,
}

impl Reader {
    async fn create(dist: &DistCluster, target: &Value, action: &str) -> TestResult<Self> {
        let reader = Self {
            access: format!("inventory{}", &Uuid::new_v4().simple().to_string()[..12]),
            secret: Uuid::new_v4().simple().to_string(),
        };
        let policy_name = format!("inventory{}", &Uuid::new_v4().simple().to_string()[..12]);
        let bucket = target["bucket"].as_str().ok_or("target bucket")?;
        let prefix = target["attempt_prefix"]
            .as_str()
            .ok_or("target prefix")?
            .trim_end_matches('/');
        let resource = if action == "s3:ListBucket" {
            format!("arn:aws:s3:::{bucket}")
        } else {
            format!("arn:aws:s3:::{bucket}/{prefix}")
        };
        let policy =
            json!({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":action,"Resource":resource}]}).to_string();
        let url = &dist.cluster.nodes[0].url;
        let access = &dist.cluster.access_key;
        let secret = &dist.cluster.secret_key;
        admin_create_user_via(AdminTransport::Signed, url, access, secret, &reader.access, &reader.secret).await?;
        admin_add_canned_policy_via(AdminTransport::Signed, url, access, secret, &policy_name, &policy).await?;
        admin_attach_user_policy_via(AdminTransport::Signed, url, access, secret, &policy_name, &reader.access).await?;
        Ok(reader)
    }

    async fn request(&self, dist: &DistCluster, target: &Value, invocation: Uuid) -> TestResult<reqwest::Response> {
        Ok(timeout(
            BUDGET,
            signed_s3_request(
                Method::POST,
                &format!("{}{ROUTE}", dist.cluster.nodes[0].url),
                Some(json!({"version":1,"invocation_id":invocation,"target":target}).to_string()),
                Some("application/json"),
                &self.access,
                &self.secret,
            ),
        )
        .await??)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    version: u32,
    invocation_id: Uuid,
    target: Value,
    sequence: u64,
    payload: Payload,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Payload {
    Observation {
        disk_id: Uuid,
        key: String,
        version_id: Uuid,
        delete_marker: bool,
    },
    Complete {
        members: usize,
        observations: u64,
    },
}

async fn read_frames(mut response: reqwest::Response) -> TestResult<(Vec<Frame>, bool)> {
    let mut pending = Vec::new();
    let mut frames = Vec::new();
    let mut clean = true;
    loop {
        match timeout(BUDGET, response.chunk()).await? {
            Ok(Some(chunk)) => {
                pending.extend_from_slice(&chunk);
                while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                    assert!(end < 1024 * 1024, "HTTP inventory line exceeds encoded bound");
                    let line: Vec<_> = pending.drain(..=end).collect();
                    frames.push(serde_json::from_slice(&line[..end])?);
                }
                assert!(pending.len() < 1024 * 1024, "unbounded incomplete HTTP line");
            }
            Ok(None) => break,
            Err(_) => {
                clean = false;
                break;
            }
        }
    }
    Ok((frames, clean && pending.is_empty()))
}

async fn seal(dist: &DistCluster, user: &ArtifactAdmin, target: &Value) -> TestResult {
    let (status, body) = user
        .request(
            dist,
            0,
            "/rustfs/admin/v3/plugin-artifacts/seal-writes",
            json!({"version":1,"target":target}).to_string(),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "actual all-member closure: {body}");
    assert_eq!(serde_json::from_str::<Value>(&body)?["all_writes_sealed"], true);
    Ok(())
}

fn exact_inventory(frames: &[Frame], target: &Value, invocation: Uuid, versions: &BTreeSet<(String, Uuid, bool)>) {
    let mut observed = BTreeSet::new();
    let mut disks = BTreeSet::new();
    let mut count = 0u64;
    for (sequence, frame) in frames.iter().enumerate() {
        assert_eq!(frame.version, 1);
        assert_eq!(frame.invocation_id, invocation);
        assert_eq!(&frame.target, target);
        assert_eq!(frame.sequence, u64::try_from(sequence).expect("bounded fixture sequence"));
        match &frame.payload {
            Payload::Observation {
                disk_id,
                key,
                version_id,
                delete_marker,
            } => {
                assert!(!disk_id.is_nil() && !version_id.is_nil());
                assert!(
                    observed.insert((*disk_id, key.clone(), *version_id, *delete_marker)),
                    "duplicate physical observation"
                );
                disks.insert(*disk_id);
                count += 1;
            }
            Payload::Complete { members, observations } => {
                assert_eq!(sequence + 1, frames.len(), "Complete must be the last HTTP frame");
                assert_eq!(*members, 4);
                assert_eq!(*observations, count);
            }
        }
    }
    assert!(
        matches!(frames.last().map(|frame| &frame.payload), Some(Payload::Complete { .. })),
        "A03I3A_HTTP_COMPLETE"
    );
    if !versions.is_empty() {
        assert_eq!(disks.len(), 4);
    }
    for disk in disks {
        let actual: BTreeSet<_> = observed
            .iter()
            .filter(|(id, ..)| *id == disk)
            .map(|(_, key, version, marker)| (key.clone(), *version, *marker))
            .collect();
        assert_eq!(&actual, versions, "A03I3A_EXACT_VERSIONS");
    }
    assert_eq!(observed.len(), versions.len() * 4);
}

#[tokio::test]
async fn artifact_inventory_http_exact_versions() -> TestResult {
    crate::common::rustfs_binary_path_with_features(Some("e2e-test-hooks"));
    init_logging();
    let dist = DistCluster::start(DistLayout::FourNodeFourDisk).await?;
    let (target, admin) = create_artifact_attempt(&dist).await?;
    let bucket = target["bucket"].as_str().ok_or("bucket")?;
    let prefix = target["attempt_prefix"].as_str().ok_or("prefix")?;
    let mut versions = BTreeSet::new();
    for (name, body) in [
        ("same", b"first".to_vec()),
        ("same", b"second".to_vec()),
        ("zero", Vec::new()),
    ] {
        let key = format!("{prefix}{name}");
        let output = timeout(BUDGET, dist.client(1)?.put_object().bucket(bucket).key(&key).body(body.into()).send()).await??;
        let version = Uuid::parse_str(output.version_id().ok_or("actual PUT version")?)?;
        versions.insert((key, version, false));
    }
    seal(&dist, &admin, &target).await?;
    // A wildcard action is still restricted to this exact attempt resource.
    // The separate authority case exercises the dedicated action alone.
    let reader = Reader::create(&dist, &target, "admin:*").await?;
    let before = physical_snapshot(&dist, bucket)?;
    let invocation = Uuid::new_v4();
    let response = reader.request(&dist, &target, invocation).await?;
    assert_eq!(response.status(), StatusCode::OK, "A03I3A_HTTP_ROUTE");
    assert_eq!(
        response.headers().get("content-type").and_then(|value| value.to_str().ok()),
        Some("application/x-ndjson")
    );
    let (frames, clean) = read_frames(response).await?;
    assert!(clean, "Complete requires clean HTTP EOF");
    assert!(frames.len() > 2, "real multi-frame HTTP body");
    exact_inventory(&frames, &target, invocation, &versions);
    assert!(
        before == physical_snapshot(&dist, bucket)?,
        "inventory mutated physical artifact namespace"
    );
    let (empty, empty_admin) = create_artifact_attempt(&dist).await?;
    seal(&dist, &empty_admin, &empty).await?;
    let empty_reader = Reader::create(&dist, &empty, "admin:*").await?;
    let invocation = Uuid::new_v4();
    let response = empty_reader.request(&dist, &empty, invocation).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let (frames, clean) = read_frames(response).await?;
    assert!(clean);
    exact_inventory(&frames, &empty, invocation, &BTreeSet::new());
    Ok(())
}

#[tokio::test]
async fn artifact_inventory_http_scoped_authority() -> TestResult {
    crate::common::rustfs_binary_path_with_features(Some("e2e-test-hooks"));
    init_logging();
    let dist = DistCluster::start(DistLayout::FourNodeFourDisk).await?;
    let (target, admin) = create_artifact_attempt(&dist).await?;
    seal(&dist, &admin, &target).await?;
    let reader = Reader::create(&dist, &target, "admin:InventoryPluginArtifactAttempt").await?;
    let invocation = Uuid::new_v4();
    let response = reader.request(&dist, &target, invocation).await?;
    assert_eq!(response.status(), StatusCode::OK, "dedicated inventory authority");
    let (frames, clean) = read_frames(response).await?;
    assert!(clean);
    exact_inventory(&frames, &target, invocation, &BTreeSet::new());
    let mut other_prefix = target.clone();
    other_prefix["attempt_prefix"] = json!(format!("outputs/6169/{}/{}/", Uuid::new_v4(), Uuid::new_v4()));
    let mut other_bucket = target.clone();
    other_bucket["bucket"] = json!("another-artifact-bucket");
    for denied in [&other_prefix, &other_bucket] {
        assert_eq!(
            reader.request(&dist, denied, Uuid::new_v4()).await?.status(),
            StatusCode::FORBIDDEN,
            "A03I3A_SCOPED_AUTHORITY"
        );
    }
    let (status, _) = admin
        .request(
            &dist,
            0,
            ROUTE,
            json!({"version":1,"invocation_id":Uuid::new_v4(),"target":target}).to_string(),
        )
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "A03I3A_SCOPED_AUTHORITY seal-only user");
    let listed = Reader::create(&dist, &target, "s3:ListBucket").await?;
    dist.client_with_credentials(0, &listed.access, &listed.secret)?
        .list_objects_v2()
        .bucket(target["bucket"].as_str().ok_or("bucket")?)
        .send()
        .await?;
    assert_eq!(
        listed.request(&dist, &target, Uuid::new_v4()).await?.status(),
        StatusCode::FORBIDDEN,
        "A03I3A_SCOPED_AUTHORITY S3 list-only user"
    );
    let root_response = signed_s3_request(
        Method::POST,
        &format!("{}{ROUTE}", dist.cluster.nodes[0].url),
        Some(json!({"version":1,"invocation_id":Uuid::new_v4(),"target":target}).to_string()),
        Some("application/json"),
        &dist.cluster.access_key,
        &dist.cluster.secret_key,
    )
    .await?;
    assert_eq!(root_response.status(), StatusCode::FORBIDDEN, "original scoped user required");
    for body in [
        json!({"version":1,"invocation_id":Uuid::nil(),"target":target}),
        json!({"version":1,"invocation_id":Uuid::new_v4(),"target":target,"extra":true}),
    ] {
        let response = signed_s3_request(
            Method::POST,
            &format!("{}{ROUTE}", dist.cluster.nodes[0].url),
            Some(body.to_string()),
            Some("application/json"),
            &reader.access,
            &reader.secret,
        )
        .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "strict inventory request");
    }
    Ok(())
}

async fn assert_incomplete(response: reqwest::Response) -> TestResult<Vec<Frame>> {
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "valid request must exercise the streaming failure boundary"
    );
    let (frames, _) = read_frames(response).await?;
    assert!(
        !frames.iter().any(|frame| matches!(frame.payload, Payload::Complete { .. })),
        "A03I3A_NO_FALSE_COMPLETE"
    );
    Ok(frames)
}

#[tokio::test]
async fn artifact_inventory_http_incomplete_has_no_terminal() -> TestResult {
    crate::common::rustfs_binary_path_with_features(Some("e2e-test-hooks"));
    init_logging();
    let mut dist = DistCluster::start(DistLayout::FourNodeFourDisk).await?;
    let (target, admin) = create_artifact_attempt(&dist).await?;
    let bucket = target["bucket"].as_str().ok_or("bucket")?;
    let prefix = target["attempt_prefix"].as_str().ok_or("prefix")?;
    // Real PUTs produce more data than a stalled HTTP/2 flow-control window.
    futures::stream::iter(0..256)
        .map(|index| {
            let client = dist.client(1);
            let key = format!("{prefix}slow/{index:04}/{}/{}", "a".repeat(190), "b".repeat(190));
            async move {
                let output = client?
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(vec![1].into())
                    .send()
                    .await?;
                assert!(!Uuid::parse_str(output.version_id().ok_or("actual PUT version")?)?.is_nil());
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            }
        })
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;
    seal(&dist, &admin, &target).await?;
    let reader = Reader::create(&dist, &target, "admin:InventoryPluginArtifactAttempt").await?;
    dist.cluster.stop_node(3)?;
    assert_incomplete(reader.request(&dist, &target, Uuid::new_v4()).await?).await?;
    dist.cluster.start_node(3).await?;
    wait_for_ready(&dist.cluster).await?;
    let mut wrong = target.clone();
    wrong["bucket_incarnation"] = json!(Uuid::new_v4());
    assert_incomplete(reader.request(&dist, &wrong, Uuid::new_v4()).await?).await?;
    // The configured endpoint map is ordered. Damage its final receiver, so
    // earlier real observations are emitted before the physical scan fails.
    let node = dist
        .cluster
        .nodes
        .iter()
        .max_by_key(|node| format!("{}{}", node.url, node.data_dirs[0]))
        .ok_or("configured members")?;
    let metadata = std::path::Path::new(&node.data_dirs[0])
        .join(bucket)
        .join(format!("{prefix}slow/0000/{}/{}", "a".repeat(190), "b".repeat(190)))
        .join("xl.meta");
    let original = std::fs::read(&metadata)?;
    std::fs::write(&metadata, b"corrupt inventory metadata")?;
    let before = physical_snapshot(&dist, bucket)?;
    let outcome = assert_incomplete(reader.request(&dist, &target, Uuid::new_v4()).await?).await;
    let after = physical_snapshot(&dist, bucket)?;
    std::fs::write(&metadata, original)?;
    let frames = outcome?;
    assert!(before == after, "failed inventory repaired or mutated a physical object");
    assert!(
        frames
            .iter()
            .any(|frame| matches!(frame.payload, Payload::Observation { .. })),
        "late physical failure must follow real HTTP observations"
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .http2_prior_knowledge()
        .http2_initial_stream_window_size(1)
        .http2_initial_connection_window_size(1)
        .http2_adaptive_window(false)
        .build()?;
    let url = format!("{}{ROUTE}", dist.cluster.nodes[0].url);
    let request = |invocation| {
        json!({"version":1,"invocation_id":invocation,"target":target})
            .to_string()
            .into_bytes()
    };
    let response = crate::common::signed_request_with_client(
        &client,
        Method::POST,
        &url,
        &reader.access,
        &reader.secret,
        Some(request(Uuid::new_v4())),
        Some("application/json"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    // Do not poll the body: its one-byte HTTP/2 receive window stalls transport.
    // This waits beyond the fixed30s service budget, not for readiness.
    tokio::time::sleep(Duration::from_secs(35)).await;
    assert_incomplete(response).await?;
    let response = crate::common::signed_request_with_client(
        &client,
        Method::POST,
        &url,
        &reader.access,
        &reader.secret,
        Some(request(Uuid::new_v4())),
        Some("application/json"),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    drop(response);
    let response = timeout(Duration::from_secs(10), reader.request(&dist, &target, Uuid::new_v4())).await??;
    let (frames, clean) = timeout(Duration::from_secs(10), read_frames(response)).await??;
    assert!(
        clean && matches!(frames.last().map(|frame| &frame.payload), Some(Payload::Complete { .. })),
        "fresh scan must succeed promptly after an actual disconnected HTTP body"
    );
    Ok(())
}
