// Copyright 2026 RustFS Team
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

//! `scanner_heal_topology_` protection matrix (backlog#2873, S00).
//!
//! Every case seeds the deterministic inventory from the fixture into one
//! pool, then drives expansion, rebalance or decommission while re-verifying
//! the recorded versions through every node and the physical pool placement
//! from `xl.meta`. Fault cases hold the mover at a proven window through the
//! `e2e-test-hooks` barriers before injecting a kill or a commit failure. The
//! gate mapping is documented in `docs/testing/scanner-heal-topology-gates.md`.

use super::harness::unique_bucket;
use super::harness::{
    DECOMMISSION_POOL_ID, TestResult, decommission_status_json, rebalance_complete, rebalance_status_json, start_decommission,
    start_rebalance, wait_for_decommission_active, wait_for_decommission_complete, wait_for_decommission_running_with_progress,
    wait_for_rebalance_active, wait_for_rebalance_complete,
};
use super::scanner_heal_topology_fixture::{
    BARRIER_REACH_TIMEOUT, LiveWorkload, MOVEMENT_ACTIVE_TIMEOUT, MOVEMENT_COMPLETE_TIMEOUT, SEED_POOL,
    STAGE_AFTER_RESPONSIBILITY_PERSISTED, STAGE_BEFORE_SOURCE_CLEANUP, STAGE_BEFORE_TARGET_COMMIT, TopologyManifest, TopologyRun,
    assert_bucket_durability_strict, assert_every_node_serves, assert_probe_objects, assert_process_durability_strict,
    assert_seed_pool_census_unchanged, assert_single_pool_placement, cancel_decommission, capture_seed_pool_census,
    clear_decommission, expect_decommission_rejected, expected_live_state, incarnation_sidecar_census, pool_count, pools_holding,
    put_probe_objects, rebalance_stopped, require_test_hooks, run_live_workload, seed_manifest, start_decommission_targets,
    stop_rebalance, verify_from_every_node, verify_live_state, verify_manifest, wait_for_decommission_flag,
};
use crate::common::{admin_request, init_logging};
use http::Method;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const ALL_POOLS: [usize; 4] = [0, 1, 2, 3];
const TARGET_POOLS: [usize; 3] = [1, 2, 3];
const LIVE_WRITERS: usize = 6;
const LIVE_ROUNDS: usize = 6;
/// Locally the decommission start reaches its durable save in about a second
/// (POST to response ~0.8 s); 60 s separates a stalled start path from slow I/O.
const PRE_ACK_REACH_TIMEOUT: Duration = Duration::from_secs(60);
/// Cancel and stop are acknowledged before every in-flight entry finishes its
/// cleanup; terminal state and placement are polled within this budget.
const TERMINAL_TRANSITION_TIMEOUT: Duration = Duration::from_secs(90);

fn pools(ids: &[usize]) -> BTreeSet<usize> {
    ids.iter().copied().collect()
}

fn seed_pool_sidecars(
    census: &std::collections::BTreeMap<String, crate::chaos::VersionShardCensus>,
) -> Vec<(&String, &crate::chaos::VersionShardCensus)> {
    census
        .iter()
        .filter(|(location, _)| location.starts_with(&format!("{SEED_POOL}/")))
        .collect()
}

/// Expansion and restart must leave the seed pool's sidecar census unchanged.
fn assert_seed_sidecars_unchanged(run: &TopologyRun, manifest: &TopologyManifest, label: &str) -> TestResult {
    for bucket in manifest.buckets() {
        let now = incarnation_sidecar_census(&run.dist, bucket)?;
        let before = &manifest.incarnation_sidecars[bucket];
        if seed_pool_sidecars(&now) != seed_pool_sidecars(before) {
            return Err(format!(
                "{label}: bucket incarnation sidecar of {bucket} changed on the seed pool: before {before:?} now {now:?}"
            )
            .into());
        }
    }
    Ok(())
}

/// After the seed pool is retired every bucket must keep its incarnation
/// sidecar on at least one target pool.
fn assert_sidecars_survive_on_targets(
    run: &TopologyRun,
    manifest: &TopologyManifest,
    targets: &BTreeSet<usize>,
    label: &str,
) -> TestResult<Value> {
    let mut summary = serde_json::Map::new();
    for bucket in manifest.buckets() {
        let now = incarnation_sidecar_census(&run.dist, bucket)?;
        let on_targets: Vec<&String> = now
            .keys()
            .filter(|location| {
                location
                    .split('/')
                    .next()
                    .and_then(|pool| pool.parse::<usize>().ok())
                    .is_some_and(|pool| targets.contains(&pool))
            })
            .collect();
        if on_targets.is_empty() {
            return Err(
                format!("{label}: bucket {bucket} has no incarnation sidecar on target pools {targets:?}: {now:?}").into(),
            );
        }
        summary.insert(bucket.to_string(), json!(on_targets));
    }
    Ok(Value::Object(summary))
}

fn manifest_summary(manifest: &TopologyManifest) -> Value {
    let mut shapes = std::collections::BTreeMap::new();
    for entry in &manifest.entries {
        *shapes.entry(entry.shape).or_insert(0usize) += 1;
    }
    json!({
        "buckets": manifest.buckets(),
        "entries": manifest.entries.len(),
        "keys": manifest.entries.iter().map(|entry| format!("{}/{}", entry.bucket, entry.key)).collect::<BTreeSet<_>>().len(),
        "shapes": shapes,
        "bytes": manifest.entries.iter().map(|entry| entry.length).sum::<u64>(),
    })
}

fn timeouts() -> Value {
    json!({
        "movement_active_secs": MOVEMENT_ACTIVE_TIMEOUT.as_secs(),
        "movement_complete_secs": MOVEMENT_COMPLETE_TIMEOUT.as_secs(),
        "barrier_reach_secs": BARRIER_REACH_TIMEOUT.as_secs(),
        "terminal_transition_secs": TERMINAL_TRANSITION_TIMEOUT.as_secs(),
    })
}

/// Cancel and stop acknowledge before every in-flight entry has finished its
/// cleanup, so placement is polled until it settles within the terminal budget.
async fn wait_for_single_pool_placement(
    run: &TopologyRun,
    manifest: &TopologyManifest,
    allowed: &BTreeSet<usize>,
    label: &str,
) -> TestResult<std::collections::BTreeMap<String, usize>> {
    let deadline = Instant::now() + TERMINAL_TRANSITION_TIMEOUT;
    loop {
        match assert_single_pool_placement(&run.dist, manifest, allowed, label) {
            Ok(placement) => return Ok(placement),
            Err(error) if Instant::now() < deadline => {
                tracing::info!("{label}: placement not settled yet: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// A stopped run may leave a committed target beside its retained source.
/// Every version must still be held by at least one pool; returns the
/// versions held by more than one.
fn assert_held_by_some_pool(run: &TopologyRun, manifest: &TopologyManifest, label: &str) -> TestResult<Vec<String>> {
    let mut duplicated = Vec::new();
    for entry in &manifest.entries {
        let held = pools_holding(&run.dist, &entry.bucket, &entry.key, entry.version_id.as_deref())?;
        if held.is_empty() {
            return Err(format!(
                "{label}: {}/{} version {:?} is held by no pool",
                entry.bucket, entry.key, entry.version_id
            )
            .into());
        }
        if held.len() > 1 {
            duplicated.push(format!("{}/{}@{} {held:?}", entry.bucket, entry.key, entry.listed_version_id()));
        }
    }
    Ok(duplicated)
}

async fn expand_and_verify(run: &mut TopologyRun, manifest: &TopologyManifest) -> TestResult {
    run.dist.expand_to_four_pools().await?;
    assert_eq!(pool_count(&run.dist.cluster).await?, 4, "expected four pools after expansion");
    verify_from_every_node(&run.dist, manifest, "after expansion to four pools").await?;
    assert_single_pool_placement(&run.dist, manifest, &pools(&[SEED_POOL]), "before data movement")?;
    Ok(())
}

/// G-EXP: 1→2→4 pools with scanner and heal enabled; old versions intact on
/// every node, seed-pool shards and set placement untouched, new writes land
/// in exactly one pool, and the pool map survives a full restart.
#[tokio::test]
async fn scanner_heal_topology_expand_one_to_four_pools_preserves_versions_and_placement() -> TestResult {
    init_logging();
    let mut run = TopologyRun::start("expand").await?;
    let manifest = seed_manifest(&run).await?;
    let bucket_durability = assert_bucket_durability_strict(&run.dist.cluster, &manifest.buckets()).await?;
    verify_from_every_node(&run.dist, &manifest, "seeded").await?;
    let seed_census = capture_seed_pool_census(&run.dist, &manifest)?;
    let scratch = unique_bucket("sht-scratch");
    run.dist.create_bucket(&scratch).await?;

    let mut steps = Vec::new();
    for expected_pools in 2..=4 {
        let new_node = run.dist.append_pool_and_restart().await?;
        if new_node + 1 != expected_pools {
            return Err(format!("appended node index {new_node} does not match pool count {expected_pools}").into());
        }
        let label = format!("after expanding to {expected_pools} pools");
        assert_eq!(pool_count(&run.dist.cluster).await?, expected_pools, "{label}: pool list");
        verify_from_every_node(&run.dist, &manifest, &label).await?;
        assert_seed_pool_census_unchanged(&run.dist, &manifest, &seed_census, &label)?;
        assert_seed_sidecars_unchanged(&run, &manifest, &label)?;
        steps.push(json!({ "pools": expected_pools, "verified_nodes": run.dist.cluster.nodes.len() }));
    }

    let probes = put_probe_objects(&run.dist, &scratch, "post-expand", 8).await?;
    let probe_placement = assert_probe_objects(&run.dist, &scratch, &probes, &pools(&ALL_POOLS), "post-expand probes").await?;

    run.dist.restart_current_binary_gracefully().await?;
    assert_eq!(pool_count(&run.dist.cluster).await?, 4, "pool map after restart");
    verify_from_every_node(&run.dist, &manifest, "after full restart").await?;
    assert_seed_pool_census_unchanged(&run.dist, &manifest, &seed_census, "after full restart")?;
    assert_seed_sidecars_unchanged(&run, &manifest, "after full restart")?;
    assert_probe_objects(&run.dist, &scratch, &probes, &pools(&ALL_POOLS), "post-expand probes after restart").await?;
    assert_bucket_durability_strict(&run.dist.cluster, &manifest.buckets()).await?;
    let strict_nodes = assert_process_durability_strict(&run.node_log_paths())?;

    run.write_evidence(
        "g-exp-expand",
        &json!({
            "gate": "G-EXP",
            "manifest": manifest_summary(&manifest),
            "bucket_durability": bucket_durability,
            "process_durability_strict_nodes": strict_nodes,
            "expansion_steps": steps,
            "probe_placement": probe_placement,
            "timeouts": timeouts(),
        }),
    )?;
    Ok(())
}

/// Wait for the first rebalance run to report either `stopped` (the expected
/// outcome) or `completed` (the inventory moved before the stop arrived).
async fn wait_for_rebalance_stopped_or_completed(run: &TopologyRun, rebalance_id: &str) -> TestResult<(&'static str, Value)> {
    let deadline = Instant::now() + TERMINAL_TRANSITION_TIMEOUT;
    loop {
        let status = rebalance_status_json(&run.dist.cluster).await?;
        if rebalance_stopped(&status, rebalance_id)? {
            return Ok(("stopped", status));
        }
        if rebalance_complete(&status, rebalance_id).unwrap_or(false) {
            return Ok(("completed_before_stop", status));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "rebalance {rebalance_id} reached neither stopped nor completed within {TERMINAL_TRANSITION_TIMEOUT:?}: {status}"
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// G-REB: rebalance with concurrent PUT/GET/DELETE and the scanner running;
/// stop mid-run, verify no content error and no lost acknowledged version,
/// restart the cluster, then rebalance to completion.
#[tokio::test]
async fn scanner_heal_topology_rebalance_survives_live_writes_stop_and_restart() -> TestResult {
    init_logging();
    let mut run = TopologyRun::start("rebalance").await?;
    let manifest = seed_manifest(&run).await?;
    assert_bucket_durability_strict(&run.dist.cluster, &manifest.buckets()).await?;
    verify_from_every_node(&run.dist, &manifest, "seeded").await?;
    let scratch = unique_bucket("sht-scratch");
    run.dist.create_bucket(&scratch).await?;
    expand_and_verify(&mut run, &manifest).await?;

    let baseline: Vec<(String, String)> = manifest
        .plain_entries()
        .filter(|entry| matches!(entry.shape, "inline" | "external" | "special"))
        .map(|entry| (entry.key.clone(), entry.sha256.clone()))
        .collect();

    let first_id = start_rebalance(&run.dist.cluster).await?;
    wait_for_rebalance_active(&run.dist.cluster, &first_id, MOVEMENT_ACTIVE_TIMEOUT).await?;
    let during_rebalance = put_probe_objects(&run.dist, &scratch, "during-rebalance", 4).await?;
    let workload = tokio::spawn(run_live_workload(LiveWorkload {
        clients: run.dist.clients()?,
        bucket: scratch.clone(),
        prefix: "live".to_string(),
        writers: LIVE_WRITERS,
        rounds: LIVE_ROUNDS,
        baseline_bucket: manifest.plain_bucket.clone(),
        baseline: baseline.clone(),
        pause: Duration::from_millis(100),
    }));
    sleep(Duration::from_secs(2)).await;
    stop_rebalance(&run.dist.cluster).await?;
    let (first_outcome, first_status) = wait_for_rebalance_stopped_or_completed(&run, &first_id).await?;
    let ops = workload.await??;
    let live_state = expected_live_state(&ops);

    for node_idx in 0..run.dist.cluster.nodes.len() {
        let client = run.dist.client(node_idx)?;
        verify_manifest(&client, &manifest, &format!("after rebalance {first_outcome} via node {node_idx}")).await?;
        verify_live_state(
            &client,
            &scratch,
            &live_state,
            &format!("after rebalance {first_outcome} via node {node_idx}"),
        )
        .await?;
    }
    let retained_after_stop = assert_held_by_some_pool(&run, &manifest, &format!("after rebalance {first_outcome}"))?;
    let during_placement = assert_probe_objects(
        &run.dist,
        &scratch,
        &during_rebalance,
        &pools(&TARGET_POOLS),
        "writes issued while the seed pool was rebalancing must avoid it",
    )
    .await?;

    run.dist.restart_current_binary_gracefully().await?;
    assert_every_node_serves(&run.dist, &scratch, "after-stop-restart").await?;
    verify_from_every_node(&run.dist, &manifest, "after restart following the stop").await?;
    verify_live_state(&run.dist.client(0)?, &scratch, &live_state, "after restart following the stop").await?;

    let second_id = start_rebalance(&run.dist.cluster).await?;
    wait_for_rebalance_active(&run.dist.cluster, &second_id, MOVEMENT_ACTIVE_TIMEOUT).await?;
    wait_for_rebalance_complete(&run.dist.cluster, &second_id, MOVEMENT_COMPLETE_TIMEOUT).await?;
    verify_from_every_node(&run.dist, &manifest, "after the second rebalance completed").await?;
    verify_live_state(&run.dist.client(3)?, &scratch, &live_state, "after the second rebalance completed").await?;
    let final_placement =
        assert_single_pool_placement(&run.dist, &manifest, &pools(&ALL_POOLS), "after the second rebalance completed")?;
    let moved = final_placement.values().filter(|pool| **pool != SEED_POOL).count();
    if moved == 0 {
        return Err("rebalance completed without moving any manifest version off the seed pool".into());
    }
    let strict_nodes = assert_process_durability_strict(&run.node_log_paths())?;

    run.write_evidence(
        "g-reb-rebalance",
        &json!({
            "gate": "G-REB",
            "manifest": manifest_summary(&manifest),
            "first_run": { "id": first_id, "outcome": first_outcome, "status": first_status, "versions_held_by_two_pools_after_stop": retained_after_stop },
            "second_run": { "id": second_id, "versions_off_seed_pool": moved },
            "live_ops": ops,
            "probe_placement_during_rebalance": during_placement,
            "process_durability_strict_nodes": strict_nodes,
            "timeouts": timeouts(),
        }),
    )?;
    Ok(())
}

/// G-DEC: cancel an active decommission and keep every version; queue two
/// pools, retire them in order, reject retiring the last active pools, and
/// keep listings, history, markers and locks correct across a restart.
#[tokio::test]
async fn scanner_heal_topology_decommission_queue_cancel_resume_and_last_pool_guard() -> TestResult {
    init_logging();
    let mut run = TopologyRun::start("decommission").await?;
    let manifest = seed_manifest(&run).await?;
    assert_bucket_durability_strict(&run.dist.cluster, &manifest.buckets()).await?;
    verify_from_every_node(&run.dist, &manifest, "seeded").await?;
    let scratch = unique_bucket("sht-scratch");
    run.dist.create_bucket(&scratch).await?;
    expand_and_verify(&mut run, &manifest).await?;

    start_decommission(&run.dist.cluster, DECOMMISSION_POOL_ID).await?;
    wait_for_decommission_running_with_progress(&run.dist.cluster, DECOMMISSION_POOL_ID, MOVEMENT_ACTIVE_TIMEOUT).await?;
    cancel_decommission(&run.dist.cluster, DECOMMISSION_POOL_ID).await?;
    let canceled =
        wait_for_decommission_flag(&run.dist.cluster, DECOMMISSION_POOL_ID, "canceled", TERMINAL_TRANSITION_TIMEOUT).await?;
    let after_cancel = wait_for_single_pool_placement(&run, &manifest, &pools(&ALL_POOLS), "after cancel").await?;
    verify_from_every_node(&run.dist, &manifest, "after cancel").await?;
    clear_decommission(&run.dist.cluster, DECOMMISSION_POOL_ID).await?;

    start_decommission_targets(&run.dist.cluster, "0,1").await?;
    let queued = wait_for_decommission_flag(&run.dist.cluster, 1, "queued", Duration::from_secs(5)).await?;
    wait_for_decommission_active(&run.dist.cluster, 0, MOVEMENT_ACTIVE_TIMEOUT).await?;
    let during = put_probe_objects(&run.dist, &scratch, "during-decommission", 4).await?;
    wait_for_decommission_complete(&run.dist.cluster, 0, MOVEMENT_COMPLETE_TIMEOUT).await?;
    wait_for_decommission_complete(&run.dist.cluster, 1, MOVEMENT_COMPLETE_TIMEOUT).await?;
    let (rejected_status, rejected_body) = expect_decommission_rejected(&run.dist.cluster, "2,3").await?;
    verify_from_every_node(&run.dist, &manifest, "after retiring pools 0 and 1").await?;
    let final_placement = assert_single_pool_placement(&run.dist, &manifest, &pools(&[2, 3]), "after retiring pools 0 and 1")?;
    assert_probe_objects(&run.dist, &scratch, &during, &pools(&[2, 3]), "writes issued during decommission").await?;
    let sidecars = assert_sidecars_survive_on_targets(&run, &manifest, &pools(&[2, 3]), "after retiring pools 0 and 1")?;

    // A decommissioned pool must leave the command line; the server refuses to
    // start while it is still listed. Pools 2 and 3 are renumbered to 0 and 1.
    run.dist.remove_retired_pools_and_restart(&[0, 1]).await?;
    let pools_after_restart = pool_count(&run.dist.cluster).await?;
    if pools_after_restart != 2 {
        return Err(format!("expected two pools after removing the retired pools, found {pools_after_restart}").into());
    }
    verify_from_every_node(&run.dist, &manifest, "after restart without the retired pools").await?;
    assert_single_pool_placement(&run.dist, &manifest, &pools(&[0, 1]), "after restart without the retired pools")?;
    let status_after_restart = decommission_status_json(&run.dist.cluster).await?;
    let strict_nodes = assert_process_durability_strict(&run.node_log_paths())?;

    run.write_evidence(
        "g-dec-decommission",
        &json!({
            "gate": "G-DEC",
            "manifest": manifest_summary(&manifest),
            "cancel": canceled,
            "placement_after_cancel_pools": after_cancel.values().collect::<BTreeSet<_>>(),
            "queued_pool_1": queued,
            "final_placement_pools": final_placement.values().collect::<BTreeSet<_>>(),
            "sidecars_on_targets": sidecars,
            "last_pool_rejection": { "status": rejected_status.as_u16(), "body": rejected_body },
            "status_after_restart": status_after_restart,
            "process_durability_strict_nodes": strict_nodes,
            "timeouts": timeouts(),
        }),
    )?;
    Ok(())
}

/// Outcome of a fault case: the worker result and the data verdict are
/// reported separately so a fail-closed worker and a lost version never hide
/// each other.
async fn settle_decommission(run: &TopologyRun, manifest: &TopologyManifest, label: &str) -> TestResult<Value> {
    let completion = wait_for_decommission_complete(&run.dist.cluster, DECOMMISSION_POOL_ID, MOVEMENT_COMPLETE_TIMEOUT).await;
    let allowed = if completion.is_ok() {
        pools(&TARGET_POOLS)
    } else {
        pools(&ALL_POOLS)
    };
    let integrity = async {
        verify_from_every_node(&run.dist, manifest, label).await?;
        wait_for_single_pool_placement(run, manifest, &allowed, label).await
    }
    .await;
    let status = decommission_status_json(&run.dist.cluster).await.unwrap_or(Value::Null);
    settle(label, completion, integrity, status)
}

async fn settle_rebalance(run: &TopologyRun, manifest: &TopologyManifest, rebalance_id: &str, label: &str) -> TestResult<Value> {
    let completion = wait_for_rebalance_complete(&run.dist.cluster, rebalance_id, MOVEMENT_COMPLETE_TIMEOUT).await;
    let integrity = async {
        verify_from_every_node(&run.dist, manifest, label).await?;
        wait_for_single_pool_placement(run, manifest, &pools(&ALL_POOLS), label).await
    }
    .await;
    let status = rebalance_status_json(&run.dist.cluster).await.unwrap_or(Value::Null);
    settle(label, completion, integrity, status)
}

fn settle(
    label: &str,
    completion: TestResult,
    integrity: TestResult<std::collections::BTreeMap<String, usize>>,
    status: Value,
) -> TestResult<Value> {
    match (completion, integrity) {
        (Ok(()), Ok(placement)) => {
            Ok(json!({ "worker": "completed", "placement_pools": placement.values().collect::<BTreeSet<_>>(), "status": status }))
        }
        (Err(worker), Ok(_)) => {
            Err(format!("{label}: data stayed intact but the worker did not complete: {worker}; status {status}").into())
        }
        (Ok(()), Err(data)) => Err(format!("{label}: worker completed but data diverged: {data}").into()),
        (Err(worker), Err(data)) => {
            Err(format!("{label}: worker did not complete: {worker}; and data diverged: {data}; status {status}").into())
        }
    }
}

fn record(run: &TopologyRun, case: &str, outcome: &TestResult<Value>, mut evidence: Value) -> TestResult {
    evidence["outcome"] = match outcome {
        Ok(detail) => detail.clone(),
        Err(error) => json!({ "error": error.to_string() }),
    };
    run.write_evidence(case, &evidence)?;
    Ok(())
}

/// G-DEC fault: SIGKILL the decommission leader while an entry is held after
/// its target commit and before source cleanup; the resumed worker must
/// finish without losing or duplicating the version.
#[tokio::test]
async fn scanner_heal_topology_decommission_kill_before_source_cleanup_resumes_without_loss() -> TestResult {
    init_logging();
    require_test_hooks();
    let mut run = TopologyRun::start("decom-kill").await?;
    let manifest = seed_manifest(&run).await?;
    verify_from_every_node(&run.dist, &manifest, "seeded").await?;
    expand_and_verify(&mut run, &manifest).await?;

    let key = manifest.plain_key("external", 3)?;
    run.movement_barrier
        .arm_movement("decommission", &manifest.plain_bucket, &key, STAGE_BEFORE_SOURCE_CLEANUP)?;
    start_decommission(&run.dist.cluster, DECOMMISSION_POOL_ID).await?;
    let reached = run.movement_barrier.wait_reached(BARRIER_REACH_TIMEOUT).await?;
    let window = pools_holding(&run.dist, &manifest.plain_bucket, &key, None)?;
    if !window.contains(&SEED_POOL) || window.len() < 2 {
        return Err(format!(
            "barrier window did not show a committed target beside the retained source for {key}: pools {window:?}"
        )
        .into());
    }

    let killed_pid = run.kill_node(SEED_POOL)?;
    run.movement_barrier.disarm()?;
    let restarted_pid = run.restart_killed_node(SEED_POOL).await?;
    let label = "after the killed leader resumed decommission";
    let outcome = settle_decommission(&run, &manifest, label).await;
    let strict_nodes = assert_process_durability_strict(&run.node_log_paths());
    record(
        &run,
        "g-dec-kill-before-source-cleanup",
        &outcome,
        json!({
            "gate": "G-DEC",
            "window": STAGE_BEFORE_SOURCE_CLEANUP,
            "nonce": run.movement_barrier.nonce(),
            "object": key,
            "reached": reached,
            "pools_holding_at_window": window,
            "killed_pid": killed_pid,
            "restarted_pid": restarted_pid,
            "process_durability_strict_nodes": strict_nodes.as_ref().ok(),
            "timeouts": timeouts(),
        }),
    )?;
    outcome?;
    strict_nodes?;
    Ok(())
}

/// G-DEC fault: inject a target commit failure after the multipart copy is
/// staged; the source must stay authoritative and a later attempt completes.
#[tokio::test]
async fn scanner_heal_topology_decommission_target_commit_failure_retains_source() -> TestResult {
    init_logging();
    require_test_hooks();
    let mut run = TopologyRun::start("decom-target-fail").await?;
    let manifest = seed_manifest(&run).await?;
    verify_from_every_node(&run.dist, &manifest, "seeded").await?;
    expand_and_verify(&mut run, &manifest).await?;

    let key = manifest.plain_key("multipart", 0)?;
    run.movement_barrier
        .arm_movement("decommission", &manifest.plain_bucket, &key, STAGE_BEFORE_TARGET_COMMIT)?;
    start_decommission(&run.dist.cluster, DECOMMISSION_POOL_ID).await?;
    let reached = run.movement_barrier.wait_reached(BARRIER_REACH_TIMEOUT).await?;
    let window = pools_holding(&run.dist, &manifest.plain_bucket, &key, None)?;
    if window != pools(&[SEED_POOL]) {
        return Err(format!("the target must not be published before its commit; {key} is held by {window:?}").into());
    }
    run.movement_barrier.disarm()?;
    run.movement_barrier.release_movement_with_io_error()?;

    let label = "after the failed target commit was retried";
    let outcome = settle_decommission(&run, &manifest, label).await;
    let strict_nodes = assert_process_durability_strict(&run.node_log_paths());
    record(
        &run,
        "g-dec-target-commit-failure",
        &outcome,
        json!({
            "gate": "G-DEC",
            "window": STAGE_BEFORE_TARGET_COMMIT,
            "nonce": run.movement_barrier.nonce(),
            "object": key,
            "reached": reached,
            "injection": "io_error",
            "process_durability_strict_nodes": strict_nodes.as_ref().ok(),
            "timeouts": timeouts(),
        }),
    )?;
    outcome?;
    strict_nodes?;
    Ok(())
}

/// G-DEC fault: kill the leader after the decommission start transaction is
/// durable in `pool.bin` but before the admin request is acknowledged and
/// before any worker starts; the persisted responsibility must resume on restart.
#[tokio::test]
async fn scanner_heal_topology_decommission_kill_after_responsibility_persisted_before_ack_resumes() -> TestResult {
    init_logging();
    require_test_hooks();
    let mut run = TopologyRun::start("decom-ack").await?;
    let manifest = seed_manifest(&run).await?;
    verify_from_every_node(&run.dist, &manifest, "seeded").await?;
    expand_and_verify(&mut run, &manifest).await?;

    run.movement_barrier
        .arm_movement("decommission_start", ".rustfs.sys", "pool.bin", STAGE_AFTER_RESPONSIBILITY_PERSISTED)?;
    let leader_url = run.dist.cluster.nodes[SEED_POOL].url.clone();
    let access_key = run.dist.cluster.access_key.clone();
    let secret_key = run.dist.cluster.secret_key.clone();
    let start_request = tokio::spawn(async move {
        // Mirror start_decommission: the init stage answers 500 with the fleet
        // proof marker until the fleet has proven its capability.
        let path = format!("/rustfs/admin/v3/pools/decommission?pool={DECOMMISSION_POOL_ID}&by-id=true");
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            match admin_request(&leader_url, Method::POST, &path, None, &access_key, &secret_key).await {
                Ok((status, body))
                    if status.as_u16() == 500
                        && body.contains("requires a live fleet capability proof")
                        && Instant::now() < deadline =>
                {
                    sleep(Duration::from_secs(1)).await;
                }
                Ok((status, body)) => return Ok(format!("{status} {body}")),
                Err(error) => return Err(error.to_string()),
            }
        }
    });
    // The start request normally reaches the durable save within about a
    // second. If it returns first the window was never entered; if it stays in
    // flight past the budget, the start path itself stalled before the save.
    let requested_at = Instant::now();
    let mut start_request = start_request;
    let reached = tokio::select! {
        reached = run.movement_barrier.wait_reached(PRE_ACK_REACH_TIMEOUT) => match reached {
            Ok(reached) => reached,
            Err(error) => {
                return Err(format!(
                    "the decommission start stayed in flight for {PRE_ACK_REACH_TIMEOUT:?} without reaching the durable start save: {error}"
                )
                .into());
            }
        },
        finished = &mut start_request => {
            return Err(format!("the decommission start returned before the pre-ACK window was reached: {:?}", finished?).into());
        }
    };
    let reach_elapsed = requested_at.elapsed();
    let killed_pid = run.kill_node(SEED_POOL)?;
    let start_outcome = start_request.await?;
    if let Ok(response) = &start_outcome {
        return Err(format!(
            "the decommission start was acknowledged ({response}) although the leader was killed in the pre-ACK window"
        )
        .into());
    }
    run.movement_barrier.disarm()?;
    let restarted_pid = run.restart_killed_node(SEED_POOL).await?;

    let label = "after the persisted decommission resumed";
    let resumed = wait_for_decommission_active(&run.dist.cluster, DECOMMISSION_POOL_ID, Duration::from_secs(60)).await;
    let outcome = match resumed {
        Ok(()) => settle_decommission(&run, &manifest, label).await,
        Err(error) => Err(format!("{label}: the persisted decommission did not become active after the restart: {error}").into()),
    };
    let strict_nodes = assert_process_durability_strict(&run.node_log_paths());
    record(
        &run,
        "g-dec-kill-before-ack",
        &outcome,
        json!({
            "gate": "G-DEC",
            "window": STAGE_AFTER_RESPONSIBILITY_PERSISTED,
            "nonce": run.movement_barrier.nonce(),
            "reached": reached,
            "reach_elapsed_ms": reach_elapsed.as_millis() as u64,
            "start_request": { "outcome": "uncertain", "error": start_outcome.err() },
            "killed_pid": killed_pid,
            "restarted_pid": restarted_pid,
            "process_durability_strict_nodes": strict_nodes.as_ref().ok(),
            "timeouts": timeouts(),
        }),
    )?;
    outcome?;
    strict_nodes?;
    Ok(())
}

/// G-REB fault: SIGKILL the rebalancing node while an entry is held after its
/// target commit and before source cleanup; the resumed run must complete
/// without losing or duplicating the version.
#[tokio::test]
async fn scanner_heal_topology_rebalance_kill_before_source_cleanup_resumes_without_loss() -> TestResult {
    init_logging();
    require_test_hooks();
    let mut run = TopologyRun::start("rebal-kill").await?;
    let manifest = seed_manifest(&run).await?;
    verify_from_every_node(&run.dist, &manifest, "seeded").await?;
    expand_and_verify(&mut run, &manifest).await?;

    let key = manifest.plain_key("empty", 0)?;
    run.movement_barrier
        .arm_movement("rebalance", &manifest.plain_bucket, &key, STAGE_BEFORE_SOURCE_CLEANUP)?;
    let rebalance_id = start_rebalance(&run.dist.cluster).await?;
    let reached = run.movement_barrier.wait_reached(BARRIER_REACH_TIMEOUT).await?;
    let window = pools_holding(&run.dist, &manifest.plain_bucket, &key, None)?;
    if !window.contains(&SEED_POOL) || window.len() < 2 {
        return Err(format!(
            "barrier window did not show a committed target beside the retained source for {key}: pools {window:?}"
        )
        .into());
    }

    let killed_pid = run.kill_node(SEED_POOL)?;
    run.movement_barrier.disarm()?;
    let restarted_pid = run.restart_killed_node(SEED_POOL).await?;
    let label = "after the killed node resumed rebalance";
    let outcome = settle_rebalance(&run, &manifest, &rebalance_id, label).await;
    let strict_nodes = assert_process_durability_strict(&run.node_log_paths());
    record(
        &run,
        "g-reb-kill-before-source-cleanup",
        &outcome,
        json!({
            "gate": "G-REB",
            "window": STAGE_BEFORE_SOURCE_CLEANUP,
            "nonce": run.movement_barrier.nonce(),
            "object": key,
            "reached": reached,
            "pools_holding_at_window": window,
            "rebalance_id": rebalance_id,
            "killed_pid": killed_pid,
            "restarted_pid": restarted_pid,
            "process_durability_strict_nodes": strict_nodes.as_ref().ok(),
            "timeouts": timeouts(),
        }),
    )?;
    outcome?;
    strict_nodes?;
    Ok(())
}
