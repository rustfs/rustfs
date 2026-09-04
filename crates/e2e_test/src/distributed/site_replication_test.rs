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
