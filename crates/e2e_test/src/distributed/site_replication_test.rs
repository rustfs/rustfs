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
};
use crate::common::{init_logging, signed_request};
use http::{Method, StatusCode};
use rustfs_madmin::PeerSite;
use std::time::Duration;

async fn site_replication_add(cluster: &crate::common::RustFSTestClusterEnvironment, sites: &[PeerSite]) -> TestResult<String> {
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
    Ok(response.text().await?)
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
    site_replication_add(&site_a.cluster, &sites).await?;

    let info = cluster_admin_ok(&site_a.cluster, Method::GET, "/rustfs/admin/v3/site-replication/info", None).await?;
    assert!(
        info.contains("site-a") || info.contains("enabled") || info.contains("true"),
        "site replication info did not show a configured peer: {info}"
    );

    let key = "site-object.bin";
    let body = b"four-node-site-replication".to_vec();
    put_object(&client_a, &bucket, key, body.clone()).await?;
    wait_for_replicated_bytes(&client_b, &bucket, key, &body, Duration::from_secs(60)).await?;

    let peer_b = site_b.client(3)?;
    wait_for_replicated_bytes(&peer_b, &bucket, key, &body, Duration::from_secs(20)).await?;
    Ok(())
}
