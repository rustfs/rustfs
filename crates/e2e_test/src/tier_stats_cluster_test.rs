// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Two-node gate for the tier stats wire contract (rustfs/backlog#2207).
//!
//! Before this contract, `GET /v3/tier-stats` answered from the process that
//! happened to receive the request, so the same query returned different
//! numbers depending on which node a client reached, with nothing in the body
//! saying so. These tests pin the two properties that fix costs: the answer is
//! node-independent, and it states how much of the cluster it covers.

use crate::common::{RustFSTestClusterEnvironment, admin_request, init_logging};
use http::Method;
use http::StatusCode;
use serde_json::Value;
use std::time::Duration;
use tokio::time::{Instant, sleep};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const TIER_STATS_PATH: &str = "/rustfs/admin/v3/tier-stats";
const PEER_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

async fn tier_stats(cluster: &RustFSTestClusterEnvironment, node: usize, query: &str) -> TestResult<(StatusCode, String)> {
    admin_request(
        &cluster.nodes[node].url,
        Method::GET,
        &format!("{TIER_STATS_PATH}{query}"),
        None,
        &cluster.access_key,
        &cluster.secret_key,
    )
    .await
}

async fn tier_stats_json(cluster: &RustFSTestClusterEnvironment, node: usize) -> TestResult<Value> {
    let (status, body) = tier_stats(cluster, node, "").await?;
    assert_eq!(status, StatusCode::OK, "node {node} must answer tier-stats: {body}");
    Ok(serde_json::from_str(&body)?)
}

/// Wait until every node reports the whole cluster.
///
/// Peer clients are established during startup, so a query issued in the first
/// moments can legitimately see a peer as unavailable. Polling separates that
/// startup window from the failure this test is about: an answer that stays
/// partial because the peer never reports at all.
async fn wait_for_complete_activity(cluster: &RustFSTestClusterEnvironment) -> TestResult<Vec<Value>> {
    let deadline = Instant::now() + PEER_CONVERGENCE_TIMEOUT;
    loop {
        let mut bodies = Vec::with_capacity(cluster.nodes.len());
        for node in 0..cluster.nodes.len() {
            bodies.push(tier_stats_json(cluster, node).await?);
        }

        if bodies.iter().all(|body| body["activity"]["status"] == "complete") {
            return Ok(bodies);
        }
        if Instant::now() >= deadline {
            return Err(format!("tier-stats activity never became complete on every node: {bodies:?}").into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn tier_stats_answers_the_same_cluster_result_from_either_node() -> TestResult {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(2).await?;
    cluster.start().await?;

    let bodies = wait_for_complete_activity(&cluster).await?;
    let (first, second) = (&bodies[0], &bodies[1]);

    for (node, body) in bodies.iter().enumerate() {
        assert_eq!(body["contractVersion"], 2, "node {node} must name the current contract version");
        assert_eq!(
            body["activity"]["nodesExpected"], 2,
            "node {node} must expect both cluster members, not only itself"
        );
        assert_eq!(
            body["activity"]["nodesReporting"], 2,
            "node {node} must include its peer's rolling window in the sum"
        );
        assert_eq!(
            body["activity"]["unavailableNodes"],
            serde_json::json!([]),
            "node {node} reported a complete result while naming an unavailable member"
        );
    }

    // A fixture cluster has no remote tier, so this pair is equal over an
    // empty list; `nodesReporting` above is what proves the peer answered.
    // The per-tier summing itself is pinned by the aggregator unit tests.
    assert_eq!(
        first["tiers"], second["tiers"],
        "the same query must not depend on which node received it"
    );
    assert_eq!(
        first["inventory"]["status"], second["inventory"]["status"],
        "both nodes read the same persisted usage snapshot"
    );

    cluster.stop();
    Ok(())
}

#[tokio::test]
async fn tier_stats_keeps_the_legacy_body_reachable_and_rejects_unknown_formats() -> TestResult {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(2).await?;
    cluster.start().await?;

    let (status, body) = tier_stats(&cluster, 0, "?format=legacy").await?;
    assert_eq!(status, StatusCode::OK, "the pinned legacy body must stay reachable: {body}");
    let legacy: Value = serde_json::from_str(&body)?;
    assert!(
        legacy.is_object() && legacy.get("contractVersion").is_none(),
        "the legacy body is the bare tier map, not the current envelope: {legacy}"
    );

    let (status, body) = tier_stats(&cluster, 0, "?format=v3").await?;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an unknown format must be rejected rather than answered in another shape: {body}"
    );

    cluster.stop();
    Ok(())
}
