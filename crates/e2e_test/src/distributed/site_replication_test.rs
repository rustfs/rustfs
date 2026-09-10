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

use super::harness::{
    DistCluster, TestResult, cluster_admin_ok, enable_versioning, put_object, unique_bucket, wait_for_replicated_bytes,
    wait_until,
};
use crate::common::{init_logging, signed_request};
use http::{Method, StatusCode};
use rustfs_madmin::{PeerSite, ReplicateAddStatus, SiteReplicationInfo, SyncStatus};
use std::time::Duration;

async fn site_replication_add(
    cluster: &crate::common::RustFSTestClusterEnvironment,
    sites: &[PeerSite],
) -> TestResult<ReplicateAddStatus> {
    let url = format!("{}/rustfs/admin/v3/site-replication/add?replicateILMExpiry=false", cluster.nodes[0].url);
    let response = signed_request(
        Method::PUT,
        &url,
        &cluster.access_key,
        &cluster.secret_key,
        Some(serde_json::to_vec(sites)?),
        Some("application/json"),
    )
    .await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication add failed: {status} {body}").into());
    }
    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_info(cluster: &crate::common::RustFSTestClusterEnvironment) -> TestResult<SiteReplicationInfo> {
    let body = cluster_admin_ok(cluster, Method::GET, "/rustfs/admin/v3/site-replication/info", None).await?;
    Ok(serde_json::from_str(&body)?)
}

async fn wait_for_site_replication_enabled(cluster: &crate::common::RustFSTestClusterEnvironment) -> TestResult {
    wait_until(
        Duration::from_secs(30),
        || async {
            let info = site_replication_info(cluster).await?;
            Ok(info.enabled && info.sites.len() == 2 && info.sites.iter().all(|site| site.sync_state == SyncStatus::Enable))
        },
        "site replication enabled with two synchronized sites",
    )
    .await
}

#[tokio::test]
async fn four_node_site_replication_replicates_object_to_peer_site() -> TestResult {
    init_logging();
    let (site_a, site_b) = DistCluster::start_replication_pair().await?;
    let bucket = unique_bucket("siterepl");
    site_a.create_bucket(&bucket).await?;
    site_b.create_bucket(&bucket).await?;

    let client_a = site_a.client(0)?;
    let client_b = site_b.client(0)?;
    enable_versioning(&client_a, &bucket).await?;
    enable_versioning(&client_b, &bucket).await?;

    let sites = vec![
        PeerSite {
            name: "site-a".to_string(),
            endpoint: site_a.cluster.nodes[0].url.clone(),
            access_key: site_a.cluster.access_key.clone(),
            secret_key: site_a.cluster.secret_key.clone(),
            ..Default::default()
        },
        PeerSite {
            name: "site-b".to_string(),
            endpoint: site_b.cluster.nodes[0].url.clone(),
            access_key: site_b.cluster.access_key.clone(),
            secret_key: site_b.cluster.secret_key.clone(),
            ..Default::default()
        },
    ];
    let add_status = site_replication_add(&site_a.cluster, &sites).await?;
    assert!(
        add_status.success && add_status.err_detail.is_empty() && add_status.initial_sync_error_message.is_empty(),
        "site replication add reported failure: {add_status:?}"
    );
    wait_for_site_replication_enabled(&site_a.cluster).await?;
    wait_for_site_replication_enabled(&site_b.cluster).await?;

    let info_a = site_replication_info(&site_a.cluster).await?;
    let remote = info_a
        .sites
        .iter()
        .find(|site| site.name == "site-b")
        .ok_or_else(|| format!("site A info omitted the configured site-b peer: {info_a:?}"))?;
    assert_eq!(remote.endpoint, site_b.cluster.nodes[0].url);
    let deployment_ids: std::collections::BTreeSet<_> = info_a.sites.iter().map(|site| site.deployment_id.as_str()).collect();
    assert!(
        deployment_ids.iter().all(|deployment_id| !deployment_id.is_empty()) && deployment_ids.len() == 2,
        "site peers must have two distinct non-empty deployment IDs: {info_a:?}"
    );
    assert!(info_a.retry_stats.is_none(), "site A has pending replication retries: {info_a:?}");
    assert!(info_a.pending_operation.is_none(), "site A has a pending operation: {info_a:?}");

    let key = "site-object.bin";
    let body = b"four-node-site-replication".to_vec();
    put_object(&client_a, &bucket, key, body.clone()).await?;
    wait_for_replicated_bytes(&client_b, &bucket, key, &body, Duration::from_secs(60)).await?;

    let peer_b = site_b.client(3)?;
    wait_for_replicated_bytes(&peer_b, &bucket, key, &body, Duration::from_secs(20)).await?;

    let reverse_key = "reverse/site-object.bin";
    let reverse_body = b"site-b-to-site-a".to_vec();
    put_object(&site_b.client(2)?, &bucket, reverse_key, reverse_body.clone()).await?;
    wait_for_replicated_bytes(&site_a.client(3)?, &bucket, reverse_key, &reverse_body, Duration::from_secs(60)).await?;
    Ok(())
}

async fn node_admin(
    cluster: &crate::common::RustFSTestClusterEnvironment,
    node_idx: usize,
    method: Method,
    path_and_query: &str,
    body: Option<String>,
) -> TestResult<(StatusCode, String)> {
    crate::common::admin_request(
        &cluster.nodes[node_idx].url,
        method,
        path_and_query,
        body,
        &cluster.access_key,
        &cluster.secret_key,
    )
    .await
}

/// Pair two clusters through site A's first node and wait until both report
/// the two-site topology as enabled.
async fn pair_sites(site_a: &DistCluster, site_b: &DistCluster) -> TestResult {
    let sites = vec![
        PeerSite {
            name: "site-a".to_string(),
            endpoint: site_a.cluster.nodes[0].url.clone(),
            access_key: site_a.cluster.access_key.clone(),
            secret_key: site_a.cluster.secret_key.clone(),
            ..Default::default()
        },
        PeerSite {
            name: "site-b".to_string(),
            endpoint: site_b.cluster.nodes[0].url.clone(),
            access_key: site_b.cluster.access_key.clone(),
            secret_key: site_b.cluster.secret_key.clone(),
            ..Default::default()
        },
    ];
    let add_status = site_replication_add(&site_a.cluster, &sites).await?;
    assert!(
        add_status.success && add_status.err_detail.is_empty() && add_status.initial_sync_error_message.is_empty(),
        "site replication add reported failure: {add_status:?}"
    );
    wait_for_site_replication_enabled(&site_a.cluster).await?;
    wait_for_site_replication_enabled(&site_b.cluster).await?;
    Ok(())
}

async fn list_users_contains(
    cluster: &crate::common::RustFSTestClusterEnvironment,
    node_idx: usize,
    access_key: &str,
) -> TestResult<bool> {
    let (status, body) = node_admin(cluster, node_idx, Method::GET, "/rustfs/admin/v3/list-users", None).await?;
    if !status.is_success() {
        return Err(format!("list-users on node {node_idx} failed: {status} {body}").into());
    }
    let users: serde_json::Value = serde_json::from_str(&body)?;
    Ok(users.get(access_key).is_some())
}

/// backlog#2367 A-7 / functional SITE-102: an IAM change handled by a node
/// other than the one that ran `site-replication/add` must still reach the
/// peer site. Behind a load balancer every admin call may land on a
/// different node, so the coordinator node is not special.
#[tokio::test]
async fn four_node_site_replication_converges_iam_user_created_on_a_non_coordinator_node() -> TestResult {
    init_logging();
    let (site_a, site_b) = DistCluster::start_replication_pair().await?;
    pair_sites(&site_a, &site_b).await?;

    let user = format!("siteuser-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let body = serde_json::json!({ "secretKey": "siteuser-secret-key-1234", "status": "enabled" }).to_string();
    let (status, response) = node_admin(
        &site_a.cluster,
        1,
        Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={user}"),
        Some(body),
    )
    .await?;
    assert!(status.is_success(), "add-user on site A node 1 failed: {status} {response}");

    let site_b_cluster = &site_b.cluster;
    let user_ref = user.as_str();
    wait_until(
        Duration::from_secs(90),
        || async move { list_users_contains(site_b_cluster, 0, user_ref).await },
        "user created on site A node 1 visible on site B",
    )
    .await?;
    assert!(
        list_users_contains(&site_a.cluster, 2, &user).await?,
        "the user must be visible on every site A node"
    );
    Ok(())
}

/// backlog#2367 A-5 / functional SITE-105: a resync started right after
/// pairing must not report buckets as failed. The bucket carrying an
/// operator-configured bucket-replication target to the peer (the shape the
/// functional suite leaves behind) and a plain versioned bucket are both
/// wired by the pairing itself.
#[tokio::test]
async fn four_node_site_replication_resync_start_right_after_pairing_reports_no_failed_bucket() -> TestResult {
    init_logging();
    let (site_a, site_b) = DistCluster::start_replication_pair().await?;

    let pre_src = unique_bucket("pre-src");
    let pre_dst = unique_bucket("pre-dst");
    let plain = unique_bucket("plain");
    site_a.create_bucket(&pre_src).await?;
    site_b.create_bucket(&pre_dst).await?;
    site_a.create_bucket(&plain).await?;
    enable_versioning(&site_a.client(0)?, &pre_src).await?;
    enable_versioning(&site_b.client(0)?, &pre_dst).await?;
    enable_versioning(&site_a.client(0)?, &plain).await?;
    let arn = super::harness::set_remote_target(&site_a.cluster, &pre_src, &site_b.cluster, &pre_dst).await?;
    super::harness::put_bucket_replication(&site_a.cluster, &pre_src, &arn).await?;

    pair_sites(&site_a, &site_b).await?;

    let (status, info) = node_admin(&site_a.cluster, 1, Method::GET, "/rustfs/admin/v3/site-replication/info", None).await?;
    assert!(status.is_success(), "site-replication/info failed: {status} {info}");
    let info: serde_json::Value = serde_json::from_str(&info)?;
    let peer = info["sites"]
        .as_array()
        .and_then(|sites| sites.iter().find(|site| site["name"] == "site-b"))
        .cloned()
        .ok_or_else(|| format!("site-b peer missing from info: {info}"))?;

    // Through a non-coordinator node, like a load-balanced admin call.
    let (status, response) = node_admin(
        &site_a.cluster,
        1,
        Method::PUT,
        "/rustfs/admin/v3/site-replication/resync/op?operation=start",
        Some(peer.to_string()),
    )
    .await?;
    assert!(status.is_success(), "resync start failed: {status} {response}");
    let resync: rustfs_madmin::SRResyncOpStatus = serde_json::from_str(&response)?;
    let failed: Vec<String> = resync
        .buckets
        .iter()
        .filter(|bucket| bucket.status == "failed")
        .map(|bucket| format!("{}: {}", bucket.bucket, bucket.err_detail))
        .collect();
    assert!(
        failed.is_empty(),
        "resync right after pairing reported failed buckets: {failed:?} (status={}, detail={})",
        resync.status,
        resync.err_detail
    );
    assert!(
        resync.buckets.iter().any(|bucket| bucket.bucket == pre_src)
            && resync.buckets.iter().any(|bucket| bucket.bucket == plain),
        "both buckets must be part of the resync: {:?}",
        resync.buckets
    );
    Ok(())
}
