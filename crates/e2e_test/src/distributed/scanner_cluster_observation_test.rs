// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::harness::{NODE_COUNT, TestResult, wait_until};
use super::scanner_peer_bucket_fixture::PeerBucketRun;
use crate::common::init_logging;
use futures::future::try_join_all;
use serde_json::Value;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const CLUSTER_SCANNER: &str = "/rustfs/admin/v3/realtime?n=1&types=1&scope=cluster";
const OBSERVATION_TIMEOUT: Duration = Duration::from_secs(35);

async fn scanner_frames(run: &PeerBucketRun) -> TestResult<Vec<Value>> {
    try_join_all((0..NODE_COUNT).map(|node| run.admin_json(node, CLUSTER_SCANNER))).await
}

fn full_sample(frame: &Value) -> bool {
    frame["final"] == true
        && frame["cluster_scanner"]["nodes_total"] == NODE_COUNT
        && frame["cluster_scanner"]["nodes_responded"] == NODE_COUNT
}

#[tokio::test]
async fn followers_report_the_coordinators_active_progress_and_committed_completion() -> TestResult {
    init_logging();
    let mut run = PeerBucketRun::start("cluster-scanner-observation").await?;
    let baseline = run.quiet_baseline().await?;
    let leader = run.leader().await?;
    let pid = run.pid(leader)?;
    let hot = run.buckets[2].clone();
    run.barrier.arm("before_start_snapshot", pid, None)?;
    run.barrier.start_session(&run.buckets)?;
    // All four child processes and the harness use the same host clock.
    let written_after = OffsetDateTime::now_utc();
    run.put(leader, &hot, "wake-cluster-observation", 149).await?;

    let start = run.barrier.wait_stage("before_start_snapshot", pid, None, true).await?;
    let epoch = start["detail"]["epoch"].as_u64().ok_or("start gate omitted epoch")?;
    let cycle = start["detail"]["cycle"].as_u64().ok_or("start gate omitted cycle")?;
    if baseline.scanner_epoch != Some(epoch) || baseline.scanner_cycle.is_none_or(|previous| previous >= cycle) {
        return Err("controlled cycle did not follow the quiet baseline under the same coordinator".into());
    }
    run.barrier.arm("root_durable", pid, Some((epoch, cycle)))?;
    run.barrier.release("before_start_snapshot", pid)?;
    run.barrier
        .wait_stage("root_durable", pid, Some((epoch, cycle)), true)
        .await?;

    // The leader is held after the real walk and before committing its cycle completion.
    // Each ingress node maintains its own short-lived cluster sample cache.
    let expected_leader = &run.dist.cluster.nodes[leader].url;
    let active = wait_until(
        OBSERVATION_TIMEOUT,
        || async {
            let frames = scanner_frames(&run).await?;
            let started = frames[leader]["cluster_scanner"]["current"]["started_at"].as_str();
            let consistent = started.is_some()
                && frames.iter().all(|frame| {
                    let summary = &frame["cluster_scanner"];
                    let current = &summary["current"];
                    full_sample(frame)
                        && summary["state"] == "scanning"
                        && current["cycle"] == cycle
                        && current["leader"].as_str() == Some(expected_leader.as_str())
                        && current["started_at"].as_str() == started
                        && current["objects_scanned"].as_u64().is_some_and(|objects| objects > 0)
                });
            if !consistent {
                tracing::info!(
                    ?frames,
                    cycle,
                    expected_leader,
                    "waiting for active scanner progress on every ingress node"
                );
            }
            Ok(consistent)
        },
        "leader and all three followers report the identified active cycle with nonzero work",
    )
    .await;
    run.barrier.disarm()?;
    run.barrier.release("root_durable", pid)?;
    active?;

    wait_until(
        OBSERVATION_TIMEOUT,
        || async {
            let frames = scanner_frames(&run).await?;
            let settled = frames.iter().all(|frame| {
                let summary = &frame["cluster_scanner"];
                full_sample(frame)
                    && summary["state"] == "idle"
                    && summary.get("current").is_none()
                    && summary["last_attempt"]["cycle"] == cycle
                    && summary["last_attempt"]["result"] == "success"
                    && summary["last_success_at"].as_str().is_some_and(|value| {
                        OffsetDateTime::parse(value, &Rfc3339).is_ok_and(|completed| completed >= written_after)
                    })
            });
            if !settled {
                tracing::info!(?frames, cycle, "waiting for the controlled scanner cycle to finish");
            }
            Ok(settled)
        },
        "every ingress node reports the completed cycle and an idle cluster scanner",
    )
    .await
}
