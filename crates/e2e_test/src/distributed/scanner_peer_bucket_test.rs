// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::harness::{TestResult, wait_until};
use super::scanner_peer_bucket_fixture::{CYCLE_TIMEOUT, PeerBucketRun};
use crate::common::init_logging;
use crate::rpc_fault_proxy::ScopedAckFault;
use rustfs_data_usage::DataUsageInfo;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

fn identity(observation: &Value) -> TestResult<(u64, u64)> {
    Ok((
        observation["detail"]["epoch"]
            .as_u64()
            .ok_or("scanner observation omitted epoch")?,
        observation["detail"]["cycle"]
            .as_u64()
            .ok_or("scanner observation omitted cycle")?,
    ))
}

fn sequence(observation: &Value) -> TestResult<u64> {
    observation["sequence"]
        .as_u64()
        .ok_or_else(|| "scanner observation omitted sequence".into())
}

fn names(value: &Value) -> TestResult<BTreeSet<String>> {
    value
        .as_array()
        .ok_or_else(|| "scanner bucket observation is not an array".into())
        .and_then(|items| {
            items
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_string)
                        .ok_or_else(|| "scanner bucket name is not a string".into())
                })
                .collect()
        })
}

fn peer_for_address<'a>(observation: &'a Value, address: &str) -> TestResult<&'a Value> {
    let peers = observation["detail"]["peers"]
        .as_array()
        .ok_or("scanner snapshot omitted actual peer maps")?;
    let matching = peers
        .iter()
        .filter(|peer| {
            peer["host"].as_str().is_some_and(|host| {
                host.trim_start_matches("http://")
                    .trim_start_matches("https://")
                    .trim_end_matches('/')
                    == address
            })
        })
        .collect::<Vec<_>>();
    if matching.len() != 1 {
        return Err(format!("actual snapshot must identify the remote ingress process once: {address}, {peers:?}").into());
    }
    Ok(matching[0])
}

fn tuples(entries: &Value) -> TestResult<BTreeMap<String, (String, u64)>> {
    let mut result = BTreeMap::new();
    for entry in entries.as_array().ok_or("scoped observation omitted entries")? {
        let bucket = entry["bucket"].as_str().ok_or("scoped tuple omitted bucket")?;
        let incarnation = entry["bucket_incarnation"]
            .as_str()
            .ok_or("scoped tuple omitted bucket incarnation")?;
        if uuid::Uuid::parse_str(incarnation)?.is_nil() {
            return Err("scoped tuple contains a nil incarnation".into());
        }
        let generation = entry["generation"]
            .as_u64()
            .filter(|value| *value > 0)
            .ok_or("scoped tuple omitted generation")?;
        if result
            .insert(bucket.to_string(), (incarnation.to_string(), generation))
            .is_some()
        {
            return Err("scoped observation contains duplicate buckets".into());
        }
    }
    Ok(result)
}

fn assert_snapshot_window(observation: &Value, peer: &Value) -> TestResult {
    let host = peer["host"].as_str().ok_or("snapshot peer omitted host")?;
    let before = &observation["detail"]["activity_before"][host];
    let after = &observation["detail"]["activity_after"][host];
    let generation = peer["generation"].as_u64().ok_or("snapshot peer omitted generation")?;
    let lower = before["dirty_usage_generation"]
        .as_u64()
        .ok_or("leading peer activity omitted generation")?;
    let upper = after["dirty_usage_generation"]
        .as_u64()
        .ok_or("trailing peer activity omitted generation")?;
    if peer["complete"].as_bool() != Some(true)
        || peer["instance_id"] != before["instance_id"]
        || peer["instance_id"] != after["instance_id"]
        || lower >= generation
        || generation > upper
    {
        return Err(
            format!("real remote write did not advance a complete snapshot inside one process/activity window: {peer}").into(),
        );
    }
    Ok(())
}

fn peer_identities(peers: &Value) -> TestResult<BTreeSet<(String, String, String)>> {
    let mut identities = BTreeSet::new();
    for peer in peers.as_array().ok_or("peer identities are not an array")? {
        let host = peer["host"].as_str().ok_or("peer identity omitted host")?;
        let owner = peer["owner_id"].as_str().ok_or("peer identity omitted owner")?;
        let instance = peer["instance_id"].as_str().ok_or("peer identity omitted instance")?;
        if uuid::Uuid::parse_str(owner)?.is_nil() || instance.is_empty() {
            return Err("peer identity contains an empty owner or process instance".into());
        }
        if !identities.insert((host.to_string(), owner.to_string(), instance.to_string())) {
            return Err("peer identities contain a duplicate".into());
        }
    }
    if identities.len() != 3
        || identities
            .iter()
            .map(|(_, _, instance)| instance)
            .collect::<BTreeSet<_>>()
            .len()
            != 3
    {
        return Err("the four-process fixture must observe exactly three distinct remote process instances".into());
    }
    Ok(identities)
}

fn assert_scope(candidate: &Value, expected: &BTreeSet<String>, incarnations: &BTreeMap<String, (String, u64)>) -> TestResult {
    assert_completed_scope(candidate, expected, incarnations, false)
}

fn assert_completed_scope(
    candidate: &Value,
    expected: &BTreeSet<String>,
    incarnations: &BTreeMap<String, (String, u64)>,
    allow_full_scan: bool,
) -> TestResult {
    let detail = &candidate["detail"];
    let selected = &detail["selected_scope"];
    let expected_scope = (allow_full_scan && selected["kind"] == "all")
        || (selected["kind"] == "buckets" && names(&selected["buckets"])? == *expected);
    if detail["scan_mode"].as_str() != Some("normal")
        || detail["status"].as_str() != Some("complete")
        || !expected_scope
        || names(&detail["scanned_buckets"])? != *expected
    {
        return Err(format!("cycle did not complete the exact Normal bucket scope: {detail}").into());
    }
    let coverage = detail["set_coverage"]
        .as_array()
        .ok_or("candidate omitted actual set completion evidence")?;
    let mut sets = BTreeSet::new();
    for set in coverage {
        if set["pool"].as_u64() != Some(0) || names(&set["buckets"])? != *expected {
            return Err(format!("set completed a different bucket scope: {set}").into());
        }
        for bucket in expected {
            if set["bucket_incarnations"][bucket].as_str() != incarnations.get(bucket).map(|entry| entry.0.as_str()) {
                return Err(format!("actual set completion did not bind the same bucket incarnation for {bucket}: {set}").into());
            }
        }
        if !sets.insert(set["set"].as_u64().ok_or("set completion omitted set index")?) {
            return Err("candidate repeated a set completion".into());
        }
    }
    if sets != BTreeSet::from([0, 1, 2, 3]) {
        return Err(format!("candidate lacks actual completion from all four sets: {sets:?}").into());
    }
    Ok(())
}

fn receiver_observation(run: &PeerBucketRun, stage: &str, pid: u32, expected_ack: &Value) -> TestResult<Option<Value>> {
    receiver_observation_after(run, stage, pid, expected_ack, None)
}

fn receiver_observation_after(
    run: &PeerBucketRun,
    stage: &str,
    pid: u32,
    expected_ack: &Value,
    after_sequence: Option<u64>,
) -> TestResult<Option<Value>> {
    select_receiver_observation(run.barrier.observations(false)?, stage, pid, expected_ack, after_sequence)
}

fn select_receiver_observation(
    observations: Vec<Value>,
    stage: &str,
    pid: u32,
    expected_ack: &Value,
    after_sequence: Option<u64>,
) -> TestResult<Option<Value>> {
    let expected_entries = tuples(&expected_ack["entries"])?;
    for observation in observations {
        let detail = &observation["detail"];
        if observation["stage"].as_str() == Some(stage)
            && observation["pid"].as_u64() == Some(u64::from(pid))
            && match after_sequence {
                Some(previous) => sequence(&observation)? > previous,
                None => true,
            }
            && detail["owner_id"] == expected_ack["owner_id"]
            && detail["instance_id"] == expected_ack["instance_id"]
            && tuples(&detail["entries"])? == expected_entries
        {
            return Ok(Some(observation));
        }
    }
    Ok(None)
}

#[test]
fn scanner_peer_bucket_receiver_observation_keeps_zero_and_excludes_prior_sequences() -> TestResult {
    let ack = json!({
        "owner_id": "00000000-0000-0000-0000-000000000001",
        "instance_id": "a".repeat(32),
        "entries": [{"bucket": "cold-a", "bucket_incarnation": "00000000-0000-0000-0000-000000000002", "generation": 7}],
    });
    let observation = |stage: &str, sequence: u64| {
        json!({"stage": stage, "pid": 17, "sequence": sequence, "detail": {
            "owner_id": ack["owner_id"], "instance_id": ack["instance_id"], "entries": ack["entries"],
        }})
    };
    let before = observation("ack_before_apply", 0);
    let after = observation("ack_after_apply", 1);
    let later_before = observation("ack_before_apply", 2);
    let later_after = observation("ack_after_apply", 3);
    let observations = vec![before.clone(), after.clone(), later_before.clone(), later_after.clone()];
    assert_eq!(
        select_receiver_observation(observations.clone(), "ack_before_apply", 17, &ack, None)?,
        Some(before),
        "the first receiver event may have sequence zero"
    );
    assert_eq!(
        select_receiver_observation(observations.clone(), "ack_after_apply", 17, &ack, None)?,
        Some(after),
        "the initial apply pair must retain its real 0/1 events"
    );
    assert_eq!(
        select_receiver_observation(observations.clone(), "ack_before_apply", 17, &ack, Some(0))?,
        Some(later_before),
        "an actual sequence-zero cursor must exclude the earlier matching event"
    );
    assert_eq!(
        select_receiver_observation(observations.clone(), "ack_after_apply", 17, &ack, Some(1))?,
        Some(later_after),
        "a later logical duplicate must not reuse the first apply observation"
    );
    assert_eq!(
        select_receiver_observation(observations, "ack_after_apply", 17, &ack, Some(3))?,
        None,
        "an exclusive cursor must not select its own observation"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_hot_window_certifies_cold_and_scopes_next_normal_cycle() -> TestResult {
    hot_window(None).await
}

#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_alternating_producers_retry_cold_after_scoped_ack_request_loss() -> TestResult {
    hot_window(Some(ScopedAckFault::RequestLoss)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_alternating_producers_preserve_hot_after_scoped_ack_response_loss() -> TestResult {
    hot_window(Some(ScopedAckFault::ResponseLoss)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_logical_duplicate_scoped_acks_clear_cold_once() -> TestResult {
    reordered_scoped_acks(false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_reordered_scoped_ack_preserves_newest_cold_generation() -> TestResult {
    reordered_scoped_acks(true).await
}

#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_root_before_publish_sigkill_preserves_old_root_and_dirty_scope() -> TestResult {
    crash_window("root_before_publish").await
}

#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_root_durable_sigkill_preserves_committed_root_without_ack() -> TestResult {
    crash_window("root_durable").await
}

#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_ack_before_apply_sigkill_recovers_unapplied_scope() -> TestResult {
    crash_window("ack_before_apply").await
}

#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_ack_after_apply_sigkill_recovers_without_response() -> TestResult {
    crash_window("ack_after_apply").await
}

fn assert_root_binding(candidate: &Value, root: &Value) -> TestResult {
    for field in ["candidate_sha256", "coverage_sha256"] {
        let digest = candidate["detail"][field].as_str().filter(|value| value.len() == 64);
        if digest.is_none() || root["detail"][field].as_str() != digest {
            return Err(format!("durable root did not bind the actual candidate {field}").into());
        }
    }
    if root["detail"]["root_sha256"].as_str().is_none_or(|value| value.len() != 64)
        || root["detail"]["root_version"].is_null()
        || !matches!(root["detail"]["outcome"].as_str(), Some("saved" | "already_durable"))
    {
        return Err(format!("root omitted its verified durable publication receipt: {root}").into());
    }
    Ok(())
}

fn scoped_ack_for_peer<'a>(root: &Value, send: &'a Value, peer: &Value) -> TestResult<&'a Value> {
    if sequence(root)? >= sequence(send)?
        || identity(root)? != identity(send)?
        || send["detail"]["candidate_sha256"] != root["detail"]["candidate_sha256"]
        || send["detail"]["coverage_sha256"] != root["detail"]["coverage_sha256"]
    {
        return Err("scoped ACK did not follow the same candidate's verified durable root".into());
    }
    send["detail"]["acks"]
        .as_array()
        .ok_or("sender omitted actual acknowledgements")?
        .iter()
        .find(|ack| ack["kind"] == "scoped" && ack["owner_id"] == peer["owner_id"] && ack["instance_id"] == peer["instance_id"])
        .ok_or_else(|| "sender omitted the actual producer's scoped ACK".into())
}

fn assert_retained_ack(receipt: &Value, ack: &Value, receiver: &str) -> TestResult {
    if receipt["request"]["owner_id"] != ack["owner_id"]
        || receipt["request"]["instance_id"] != ack["instance_id"]
        || receipt["request"]["probe_only"] != false
        || tuples(&receipt["request"]["entries"])? != tuples(&ack["entries"])?
        || receipt["receiver"].as_str() != Some(receiver)
        || receipt["forwarded"] != false
        || receipt["completed"] != false
        || receipt["outcome"] != "held"
        || receipt["request_sha256"].as_str().is_none_or(|value| value.len() != 64)
        || receipt["request_headers_sha256"]
            .as_str()
            .is_none_or(|value| value.len() != 64)
        || (receipt["auth_nonce"].as_str().is_none_or(str::is_empty)
            && receipt["replay_nonce"].as_str().is_none_or(str::is_empty))
        || (receipt["signature_sha256"].as_str().is_none_or(|value| value.len() != 64)
            && receipt["replay_signature_sha256"]
                .as_str()
                .is_none_or(|value| value.len() != 64))
    {
        return Err(format!("proxy did not retain the original signed production ACK unsent: {receipt}").into());
    }
    Ok(())
}

fn assert_forwarded_ack(receipt: &Value, ack: &Value, cleared: u64) -> TestResult {
    if receipt["outcome"] != "forwarded"
        || receipt["forwarded"] != true
        || receipt["backend_complete"] != true
        || receipt["response"]["status"] != 200
        || receipt["response"]["grpc_status"] != "0"
        || receipt["response"]["ack"]["owner_id"] != ack["owner_id"]
        || receipt["response"]["ack"]["instance_id"] != ack["instance_id"]
        || receipt["response"]["ack"]["supported"] != true
        || receipt["response"]["ack"]["cleared"].as_u64() != Some(cleared)
        || receipt["response"]["ack"]["response_proof_bytes"]
            .as_u64()
            .is_none_or(|count| count == 0)
        || receipt["response"]["sha256"].as_str().is_none_or(|value| value.len() != 64)
        || receipt["response"]["headers_sha256"]
            .as_str()
            .is_none_or(|value| value.len() != 64)
    {
        return Err(format!("retained ACK lacks an authenticated backend success clearing {cleared}: {receipt}").into());
    }
    Ok(())
}

fn dirty_generations(observation: &Value) -> TestResult<BTreeMap<String, u64>> {
    let snapshot = &observation["detail"]["dirty_snapshot"];
    let entries = snapshot["buckets"]
        .as_array()
        .ok_or("receiver omitted remaining dirty buckets")?;
    if snapshot["complete"] != true || snapshot["pending_bucket_count"].as_u64() != Some(u64::try_from(entries.len())?) {
        return Err(format!("receiver dirty snapshot is incomplete or inconsistent: {snapshot}").into());
    }
    let mut result = BTreeMap::new();
    for entry in entries {
        let bucket = entry["bucket"].as_str().ok_or("receiver dirty entry omitted bucket")?;
        let generation = entry["generation"]
            .as_u64()
            .filter(|value| *value > 0)
            .ok_or("receiver omitted dirty generation")?;
        if result.insert(bucket.to_string(), generation).is_some() {
            return Err("receiver repeated a dirty bucket".into());
        }
    }
    Ok(result)
}

async fn applied_ack_after(
    run: &PeerBucketRun,
    pid: u32,
    ack: &Value,
    after_sequence: Option<u64>,
) -> TestResult<(Value, Value)> {
    wait_until(
        CYCLE_TIMEOUT,
        || async { Ok(receiver_observation_after(run, "ack_after_apply", pid, ack, after_sequence)?.is_some()) },
        "actual retained scoped ACK compare-and-clear",
    )
    .await?;
    let before = receiver_observation_after(run, "ack_before_apply", pid, ack, after_sequence)?
        .ok_or("retained ACK omitted its receiver pre-apply observation")?;
    let after = receiver_observation_after(run, "ack_after_apply", pid, ack, after_sequence)?
        .ok_or("retained ACK omitted its receiver post-apply observation")?;
    if sequence(&before)? >= sequence(&after)? {
        return Err("retained ACK receiver observations are out of order".into());
    }
    Ok((before, after))
}

fn ack_deadline_log(path: &str, offset: u64) -> TestResult<Option<String>> {
    use std::io::{BufRead, Seek};

    let mut file = std::fs::File::open(path)?;
    file.seek(std::io::SeekFrom::Start(offset))?;
    for line in std::io::BufReader::new(file).lines() {
        let line = line?;
        if line.contains("scoped dirty usage acknowledgement deadline elapsed")
            && line.contains("remote_dirty_usage_acknowledgement_pending")
        {
            return Ok(Some(line));
        }
    }
    Ok(None)
}

fn converged_shards(run: &PeerBucketRun) -> TestResult<Vec<Value>> {
    let mut physical = Vec::new();
    for object in &run.objects {
        for (node_index, node) in run.dist.cluster.nodes.iter().enumerate() {
            let mut complete = Vec::new();
            for drive in &node.data_dirs {
                let census = crate::chaos::census_object_version_on_disk(
                    std::path::Path::new(drive),
                    &object.bucket,
                    &object.key,
                    Some(&object.version_id),
                )?;
                if census.has_xl_meta {
                    if !census.is_complete() || (census.data_blocks, census.parity_blocks) != (Some(2), Some(2)) {
                        return Err(format!("converged version lost an acknowledged EC:2 shard on {drive}: {census:?}").into());
                    }
                    complete.push(json!({"drive": drive, "census": census}));
                }
            }
            if complete.len() != 1 {
                return Err(format!("converged version must retain one complete shard on node {node_index}: {object:?}").into());
            }
            physical.push(json!({"object": object, "node": node_index, "shards": complete}));
        }
    }
    Ok(physical)
}

// The caller retains each real ACK only after this exact hot-window candidate
// has completed every set and published its verified root.
async fn queued_hot_cycle(run: &mut PeerBucketRun, leader: usize, producer: usize, start_gate: &Value) -> TestResult<Value> {
    queued_hot_cycle_until(run, leader, producer, start_gate, "root_durable").await
}

async fn queued_hot_cycle_until(
    run: &mut PeerBucketRun,
    leader: usize,
    producer: usize,
    start_gate: &Value,
    root_stage: &str,
) -> TestResult<Value> {
    let cycle = identity(start_gate)?;
    let leader_pid = run.pid(leader)?;
    let [cold_a, cold_b, hot] = run.buckets.clone();
    let all = run.buckets.iter().cloned().collect::<BTreeSet<_>>();
    let cold = BTreeSet::from([cold_a, cold_b]);
    let address = run.peer_address(producer);
    run.put(producer, &hot, &format!("queued-start-{}", cycle.1), 157).await?;
    run.barrier.arm("before_post_walk_snapshot", leader_pid, Some(cycle))?;
    run.barrier.release("before_start_snapshot", leader_pid)?;
    let start = run
        .barrier
        .wait_stage("start_snapshot", leader_pid, Some(cycle), false)
        .await?;
    let start_peer = peer_for_address(&start, &address)?;
    assert_snapshot_window(&start, start_peer)?;
    let start_tuples = tuples(&start_peer["buckets"])?;
    if start_tuples.keys().cloned().collect::<BTreeSet<_>>() != all {
        return Err("held ACK did not leave all three producer buckets dirty for the next actual scan".into());
    }
    run.barrier
        .wait_stage("before_post_walk_snapshot", leader_pid, Some(cycle), true)
        .await?;
    run.put(producer, &hot, &format!("queued-post-{}", cycle.1), 197).await?;
    run.barrier.arm(root_stage, leader_pid, Some(cycle))?;
    run.barrier.release("before_post_walk_snapshot", leader_pid)?;
    let post = run
        .barrier
        .wait_stage("post_walk_snapshot", leader_pid, Some(cycle), false)
        .await?;
    let post_peer = peer_for_address(&post, &address)?;
    assert_snapshot_window(&post, post_peer)?;
    let post_tuples = tuples(&post_peer["buckets"])?;
    let identities = peer_identities(&start["detail"]["peers"])?;
    if peer_identities(&post["detail"]["peers"])? != identities
        || cold.iter().any(|bucket| start_tuples.get(bucket) != post_tuples.get(bucket))
        || post_tuples
            .get(&hot)
            .zip(start_tuples.get(&hot))
            .is_none_or(|(after, before)| after.0 != before.0 || after.1 <= before.1)
    {
        return Err("queued ACK cycle did not preserve cold tuples while real hot writes advanced".into());
    }
    let candidate = run
        .barrier
        .wait_stage("cycle_candidate", leader_pid, Some(cycle), false)
        .await?;
    assert_scope(&candidate, &all, &start_tuples)?;
    if candidate["detail"]["peer_certification"] != true
        || peer_identities(&candidate["detail"]["certified_peers"])? != identities
        || names(&candidate["detail"]["pending_buckets"])? != BTreeSet::from([hot])
        || candidate["detail"]["converged"] != false
    {
        return Err(format!("queued ACK candidate did not certify only cold with hot pending: {candidate}").into());
    }
    let root = run.barrier.wait_stage(root_stage, leader_pid, Some(cycle), true).await?;
    if root_stage == "root_durable" {
        assert_root_binding(&candidate, &root)?;
    } else if root_stage != "root_before_publish"
        || root["detail"]["candidate_sha256"] != candidate["detail"]["candidate_sha256"]
        || root["detail"]["coverage_sha256"] != candidate["detail"]["coverage_sha256"]
    {
        return Err("pre-publication barrier does not identify the sealed candidate".into());
    }
    Ok(json!({"start_gate": start_gate, "start": start, "post": post, "candidate": candidate, "root": root}))
}

fn complete_dirty_tuples(snapshot: &Value) -> TestResult<BTreeMap<String, (String, u64)>> {
    let entries = tuples(&snapshot["buckets"])?;
    if snapshot["complete"] != true
        || snapshot["pending_bucket_count"].as_u64() != Some(u64::try_from(entries.len())?)
        || snapshot["instance_id"].as_str().is_none_or(str::is_empty)
        || snapshot["owner_id"].as_str().is_none_or(str::is_empty)
    {
        return Err(format!("crash window requires a complete authenticated dirty snapshot: {snapshot}").into());
    }
    Ok(entries)
}

fn assert_physical_root(root: &Value, physical: &Value) -> TestResult {
    let usage: DataUsageInfo = serde_json::from_value(physical["usage"].clone())?;
    if physical["root_version"] != root["detail"]["root_version"]
        || physical["root_sha256"] != root["detail"]["root_sha256"]
        || (usage.scanner_epoch.zip(usage.scanner_cycle)) != Some(identity(root)?)
    {
        return Err("physical authoritative root differs from the actual committed version, digest or cycle".into());
    }
    Ok(())
}

fn persisted_usage_fields(usage: &Value) -> TestResult<Value> {
    let mut fields = usage.as_object().ok_or("usage comparison requires an object")?.clone();
    // Admin fills these from live storage statistics; all other fields must
    // still match the persisted scanner snapshot. Leave both raw receipts intact.
    for field in ["total_capacity", "total_free_capacity", "total_used_capacity"] {
        fields.remove(field);
    }
    Ok(Value::Object(fields))
}

fn assert_walked_buckets(run: &PeerBucketRun, cycle: (u64, u64), expected: &BTreeSet<String>) -> TestResult {
    let actual = run
        .barrier
        .observations(false)?
        .iter()
        .filter(|event| event["stage"] == "bucket_walk" && identity(event).ok() == Some(cycle))
        .map(|event| {
            event["detail"]["bucket"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| "crash recovery bucket walk omitted its bucket".into())
        })
        .collect::<TestResult<BTreeSet<_>>>()?;
    if actual != *expected {
        return Err(format!("crash recovery physically walked {actual:?}, expected {expected:?}").into());
    }
    Ok(())
}

async fn crash_window(stage: &str) -> TestResult {
    init_logging();
    let receiver_crash = matches!(stage, "ack_before_apply" | "ack_after_apply");
    if !receiver_crash && !matches!(stage, "root_before_publish" | "root_durable") {
        return Err("unknown scanner crash window".into());
    }
    let mut run = PeerBucketRun::start_for_crash(stage).await?;
    let executable = run.executable_identity()?;
    let scheduler_config = run.assert_default_cadence().await?;
    let baseline = run.quiet_baseline().await?;
    let leader = run.leader().await?;
    let leader_pid = run.pid(leader)?;
    let producer = (leader + 1) % 4;
    let producer_pid = run.pid(producer)?;
    let target = if receiver_crash { producer } else { leader };
    let old_process = run.dirty_snapshot(target).await?;
    let [cold_a, cold_b, hot] = run.buckets.clone();
    let cold = BTreeSet::from([cold_a.clone(), cold_b.clone()]);
    let all = run.buckets.iter().cloned().collect::<BTreeSet<_>>();
    run.barrier.arm("before_start_snapshot", leader_pid, None)?;
    run.barrier.start_session(&run.buckets)?;
    run.put(leader, &hot, "wake-crash-window", 149).await?;
    let start_gate = run
        .barrier
        .wait_stage("before_start_snapshot", leader_pid, None, true)
        .await?;
    let cycle = identity(&start_gate)?;
    if baseline.scanner_epoch != Some(cycle.0) || baseline.scanner_cycle.is_none_or(|previous| previous >= cycle.1) {
        return Err("crash window did not start under the observed baseline coordinator".into());
    }
    let old_root = run.authoritative_root()?;
    if persisted_usage_fields(&old_root["usage"])? != persisted_usage_fields(&serde_json::to_value(&baseline)?)? {
        return Err("physical pre-crash root does not match the authoritative baseline".into());
    }
    for bucket in &run.buckets.clone() {
        run.put(producer, bucket, "crash-start-window", 137).await?;
    }
    let root_stage = if stage == "root_before_publish" {
        stage
    } else {
        "root_durable"
    };
    let window = queued_hot_cycle_until(&mut run, leader, producer, &start_gate, root_stage).await?;
    let candidate = &window["candidate"];
    let root = &window["root"];
    let start_peer = peer_for_address(&window["start"], &run.peer_address(producer))?;
    let post_tuples = tuples(&peer_for_address(&window["post"], &run.peer_address(producer))?["buckets"])?;
    let before_dirty = run.dirty_snapshot(producer).await?;
    if complete_dirty_tuples(&before_dirty)? != post_tuples
        || before_dirty["owner_id"] != start_peer["owner_id"]
        || before_dirty["instance_id"] != start_peer["instance_id"]
        || run.leader().await? != leader
    {
        return Err("actual producer state or coordinator changed before the crash window".into());
    }
    let planned_ack = candidate["detail"]["acks"]
        .as_array()
        .ok_or("candidate omitted ACK plan")?
        .iter()
        .find(|ack| {
            ack["kind"] == "scoped"
                && ack["owner_id"] == start_peer["owner_id"]
                && ack["instance_id"] == start_peer["instance_id"]
        })
        .ok_or("candidate omitted the actual producer's scoped ACK")?
        .clone();
    let expected_cold = post_tuples
        .iter()
        .filter(|(bucket, _)| cold.contains(*bucket))
        .map(|(bucket, tuple)| (bucket.clone(), tuple.clone()))
        .collect::<BTreeMap<_, _>>();
    if tuples(&planned_ack["entries"])? != expected_cold || sequence(candidate)? >= sequence(root)? {
        return Err("crash window did not seal the exact cold tuples before root publication".into());
    }
    let root_at_gate = run.authoritative_root()?;
    if stage == "root_before_publish" {
        if root_at_gate != old_root {
            return Err("uncommitted candidate already replaced the authoritative root".into());
        }
    } else {
        assert_physical_root(root, &root_at_gate)?;
        let usage: DataUsageInfo = serde_json::from_value(root_at_gate["usage"].clone())?;
        if usage.usage_snapshot_converged != Some(false)
            || usage.usage_snapshot_pending_buckets != vec![hot.clone()]
            || !run.usage_matches(&usage, &[&cold_a, &cold_b])
        {
            return Err("committed crash-window root lost cold usage or the hot pending obligation".into());
        }
    }
    let before_shards = converged_shards(&run)?;
    // Start before either queue retention or the production sender can begin;
    // forwarding cannot reset the age of a previously held request.
    let ack_started_at = std::time::Instant::now();
    let queue = run.scoped_ack_queue(producer, start_peer, cold, 1)?;
    let mut ack_send = Value::Null;
    let mut retained = Value::Null;
    let mut before_apply = Value::Null;
    let mut after_apply = Value::Null;
    let mut crash;
    if receiver_crash {
        run.barrier.arm_receiver(stage, producer_pid, start_peer)?;
        run.barrier.release("root_durable", leader_pid)?;
        ack_send = run.barrier.wait_stage("ack_send", leader_pid, Some(cycle), false).await?;
        let ack = scoped_ack_for_peer(root, &ack_send, start_peer)?;
        if tuples(&ack["entries"])? != expected_cold {
            return Err("production sender changed the sealed cold ACK tuples".into());
        }
        queue.wait_for_packets(1).await?;
        retained = queue.receipts().into_iter().next().ok_or("crash ACK packet disappeared")?;
        assert_retained_ack(&retained, ack, &run.dist.cluster.nodes[producer].address)?;
        if ack_started_at.elapsed() >= std::time::Duration::from_secs(5) {
            return Err("receiver crash window expired while the original ACK was held".into());
        }
        let forwarded_at = std::time::Instant::now();
        queue.settle(0, ScopedAckFault::Forward)?;
        let receiver_gate = run.barrier.wait_stage(stage, producer_pid, None, true).await?;
        if receiver_observation(&run, stage, producer_pid, ack)?.as_ref() != Some(&receiver_gate) {
            return Err("armed receiver barrier does not identify the original signed ACK".into());
        }
        if ack_started_at.elapsed() >= std::time::Duration::from_secs(5) {
            return Err("receiver crash window expired before SIGKILL".into());
        }
        // Kill while the unchanged five-second receiver operation is still
        // paused. A timed-out hook is not the requested crash window.
        crash = run.kill_at(producer, &receiver_gate)?;
        let kill_elapsed = forwarded_at.elapsed();
        let total_kill_elapsed = ack_started_at.elapsed();
        if kill_elapsed >= std::time::Duration::from_secs(5) || total_kill_elapsed >= std::time::Duration::from_secs(5) {
            return Err("receiver kill exceeded the real scoped ACK operation deadline".into());
        }
        crash["forward_to_sigkill_ms"] = json!(u64::try_from(kill_elapsed.as_millis())?);
        crash["total_to_sigkill_ms"] = json!(u64::try_from(total_kill_elapsed.as_millis())?);
        before_apply = receiver_observation(&run, "ack_before_apply", producer_pid, ack)?
            .ok_or("crashed receiver omitted its pre-apply state")?;
        let expected_before = post_tuples
            .iter()
            .map(|(bucket, (_, generation))| (bucket.clone(), *generation))
            .collect::<BTreeMap<_, _>>();
        if dirty_generations(&before_apply)? != expected_before {
            return Err("receiver did not retain the exact original dirty map before apply".into());
        }
        let observed_after = receiver_observation(&run, "ack_after_apply", producer_pid, ack)?;
        if stage == "ack_before_apply" {
            if observed_after.is_some() {
                return Err("ACK applied despite killing the receiver at its pre-apply barrier".into());
            }
        } else {
            after_apply = observed_after.ok_or("receiver was killed without observed compare-and-remove")?;
            if sequence(&before_apply)? >= sequence(&after_apply)?
                || after_apply["detail"]["cleared"] != 2
                || dirty_generations(&after_apply)? != BTreeMap::from([(hot.clone(), post_tuples[&hot].1)])
            {
                return Err("post-apply crash did not clear exactly cold while conserving the hot generation".into());
            }
        }
    } else {
        crash = run.kill_at(leader, root)?;
        if run.barrier.observations(false)?.iter().any(|event| {
            event["pid"].as_u64() == Some(u64::from(leader_pid))
                && identity(event).ok() == Some(cycle)
                && (event["stage"] == "ack_send" || (stage == "root_before_publish" && event["stage"] == "root_durable"))
        }) || receiver_observation(&run, "ack_before_apply", producer_pid, &planned_ack)?.is_some()
            || receiver_observation(&run, "ack_after_apply", producer_pid, &planned_ack)?.is_some()
            || !queue.receipts().is_empty()
        {
            return Err("killed coordinator published or acknowledged past its held crash boundary".into());
        }
    }
    // Read before any restart or recovery work. No root version is selected
    // from disagreeing replicas, and an unexpected successor publication fails
    // the controlled window instead of being mistaken for preservation.
    let root_after_kill = run.authoritative_root()?;
    if root_after_kill != root_at_gate {
        return Err("authoritative root changed outside the controlled crash boundary".into());
    }
    let transport = if receiver_crash {
        let receipt = queue.wait_for_completion(0).await?;
        let total_transport_elapsed = ack_started_at.elapsed();
        let transport_elapsed = receipt["completed_at_ms"]
            .as_u64()
            .zip(receipt["forwarded_at_ms"].as_u64())
            .and_then(|(completed, forwarded)| completed.checked_sub(forwarded));
        if receipt["completed"] != true
            || receipt["forwarded"] != true
            || receipt["backend_complete"] != false
            || receipt["outcome"] != "transport_failed_or_expired"
            || !receipt["response"].is_null()
            || receipt["request_frame_hex"] != retained["request_frame_hex"]
            || receipt["request_sha256"] != retained["request_sha256"]
            || receipt["request_headers_sha256"] != retained["request_headers_sha256"]
            || receipt["auth_nonce"] != retained["auth_nonce"]
            || receipt["replay_nonce"] != retained["replay_nonce"]
            || transport_elapsed.is_none_or(|millis| millis >= 5_000)
            || total_transport_elapsed >= std::time::Duration::from_secs(5)
        {
            return Err("receiver crash did not lose the original signed ACK response in transport".into());
        }
        crash["total_to_transport_completion_ms"] = json!(u64::try_from(total_transport_elapsed.as_millis())?);
        receipt
    } else {
        let after = run.dirty_snapshot(producer).await?;
        if after != before_dirty {
            return Err("coordinator crash changed the surviving producer's exact dirty responsibility".into());
        }
        after
    };
    drop(queue);
    run.write_evidence(
        "crash-boundary",
        &json!({"stage": stage, "executable": executable, "scheduler_config": scheduler_config,
        "baseline": baseline, "topology": run.topology, "old_process": old_process, "old_root": old_root,
        "window": window, "before_dirty": before_dirty, "planned_ack": planned_ack, "ack_send": ack_send,
        "retained_packet": retained, "before_apply": before_apply, "after_apply": after_apply, "transport": transport,
        "root_at_gate": root_at_gate, "root_after_kill": root_after_kill, "crash": crash,
        "objects": run.objects, "before_shards": before_shards}),
    )?;
    run.barrier.disarm()?;
    let before_restart = run.barrier.observations(false)?;
    let restarted = run.restart_after_crash(&crash, &old_process).await?;
    run.write_evidence(
        "restarted-process",
        &json!({"crash": crash, "previous": old_process, "restarted": restarted,
        "drives": run.drive_identity(target)?, "before_restart_observations": before_restart}),
    )?;

    // No S3 mutation is allowed between the crash and this convergence check.
    // An empty volatile map in the new process cannot account for old versions.
    let recovered = run.quiet_baseline().await?;
    let recovery_cycle = recovered
        .scanner_epoch
        .zip(recovered.scanner_cycle)
        .ok_or("recovery root omitted cycle")?;
    if recovery_cycle == cycle || recovered == baseline {
        return Err("restart did not produce a fresh authoritative recovery root".into());
    }
    let recovery_root = run.authoritative_root()?;
    let observations = run.barrier.observations(false)?;
    run.write_evidence(
        "automatic-crash-recovery-observed",
        &json!({"restarted": restarted, "usage": recovered, "root": recovery_root,
            "objects": run.objects, "observations": observations}),
    )?;
    if persisted_usage_fields(&recovery_root["usage"])? != persisted_usage_fields(&serde_json::to_value(&recovered)?)? {
        return Err("automatic recovery admin state differs from the physical authoritative root".into());
    }
    let recovery_durable = observations
        .iter()
        .find(|event| {
            event["stage"] == "root_durable"
                && identity(event).ok() == Some(recovery_cycle)
                && event["detail"]["root_version"] == recovery_root["root_version"]
                && event["detail"]["root_sha256"] == recovery_root["root_sha256"]
        })
        .ok_or("automatic recovery lacks an actual durable root receipt")?;
    let recovery_candidate = observations
        .iter()
        .find(|event| {
            event["stage"] == "cycle_candidate"
                && event["pid"] == recovery_durable["pid"]
                && identity(event).ok() == Some(recovery_cycle)
        })
        .ok_or("automatic recovery lacks actual per-set completion")?;
    let recovery_sequence = sequence(recovery_candidate)?;
    if before_restart.iter().any(|event| {
        event["pid"] == recovery_candidate["pid"]
            && event["sequence"]
                .as_u64()
                .is_some_and(|sequence| sequence >= recovery_sequence)
    }) {
        return Err("automatic recovery reused a candidate observed before the replacement process started".into());
    }
    assert_completed_scope(recovery_candidate, &all, &post_tuples, true)?;
    assert_root_binding(recovery_candidate, recovery_durable)?;
    assert_physical_root(recovery_durable, &recovery_root)?;
    assert_walked_buckets(&run, recovery_cycle, &all)?;
    if sequence(recovery_candidate)? >= sequence(recovery_durable)?
        || recovery_candidate["detail"]["converged"] != true
        || !names(&recovery_candidate["detail"]["pending_buckets"])?.is_empty()
    {
        return Err("automatic restart recovery did not conservatively complete all bucket obligations".into());
    }
    run.verify_objects().await?;
    let recovered_shards = converged_shards(&run)?;
    if recovered_shards != before_shards {
        return Err("restart changed an acknowledged physical version shard".into());
    }
    run.write_evidence(
        "automatic-crash-recovery",
        &json!({"restarted": restarted, "usage": recovered,
        "root": recovery_root, "candidate": recovery_candidate, "durable": recovery_durable,
        "objects": run.objects, "physical": recovered_shards, "observations": observations}),
    )?;

    let new_leader = run.leader().await?;
    // Recovery may elect another coordinator. Keep the ACK receiver remote,
    // and bind the replacement through the role it actually holds now.
    let fresh_receiver = if target == new_leader { (new_leader + 1) % 4 } else { target };
    run.write_evidence(
        "fresh-process-roles",
        &json!({"leader_before_crash": leader, "leader_after_recovery": new_leader,
            "coordinator_pid": run.pid(new_leader)?, "receiver": fresh_receiver,
            "receiver_pid": run.pid(fresh_receiver)?, "replacement": target,
            "replacement_pid": restarted["pid"],
            "replacement_role": if target == new_leader { "coordinator" } else { "receiver" }}),
    )?;
    let fresh_ack = fresh_ack_after_restart(&mut run, new_leader, fresh_receiver, &post_tuples).await?;
    if (fresh_receiver == target && fresh_ack["peer"]["instance_id"] != restarted["instance_id"])
        || (new_leader == target && fresh_ack["coordinator_instance"] != restarted["instance_id"])
    {
        return Err("fresh scoped ACK did not bind the actual replacement process instance".into());
    }
    run.barrier.stop_session()?;
    run.verify_objects().await?;
    let physical = converged_shards(&run)?;
    if run.executable_identity()? != executable {
        return Err("crash recovery restarted a different executable".into());
    }
    run.write_evidence(
        "crash-result",
        &json!({"stage": stage, "crash": crash, "restarted": restarted,
        "automatic_recovery_root": recovery_root, "fresh_ack": fresh_ack, "executable": executable,
        "objects": run.objects, "physical": physical, "observations": run.barrier.observations(false)?}),
    )?;
    Ok(())
}

async fn fresh_ack_after_restart(
    run: &mut PeerBucketRun,
    leader: usize,
    receiver: usize,
    incarnations: &BTreeMap<String, (String, u64)>,
) -> TestResult<Value> {
    let leader_pid = run.pid(leader)?;
    let receiver_pid = run.pid(receiver)?;
    let hot = run.buckets[2].clone();
    let expected = BTreeSet::from([hot.clone()]);
    let previous = run.dirty_snapshot(receiver).await?;
    if !complete_dirty_tuples(&previous)?.is_empty() {
        return Err("automatic recovery left receiver dirty state before fresh-write proof".into());
    }
    let previous_sequence = run
        .barrier
        .observations(false)?
        .iter()
        .filter(|event| event["pid"].as_u64() == Some(u64::from(leader_pid)))
        .map(sequence)
        .collect::<TestResult<Vec<_>>>()?
        .into_iter()
        .max();
    run.barrier.arm("before_start_snapshot", leader_pid, None)?;
    run.put(leader, &hot, "wake-fresh-process-proof", 151).await?;
    let gate = match previous_sequence {
        Some(previous) => {
            run.barrier
                .wait_stage_after("before_start_snapshot", leader_pid, None, true, previous)
                .await?
        }
        None => {
            run.barrier
                .wait_stage("before_start_snapshot", leader_pid, None, true)
                .await?
        }
    };
    let cycle = identity(&gate)?;
    let write = run.put(receiver, &hot, "fresh-process-proof", 167).await?;
    run.barrier.arm("root_durable", leader_pid, Some(cycle))?;
    run.barrier.release("before_start_snapshot", leader_pid)?;
    let start = run
        .barrier
        .wait_stage("start_snapshot", leader_pid, Some(cycle), false)
        .await?;
    let peer = peer_for_address(&start, &run.peer_address(receiver))?;
    assert_snapshot_window(&start, peer)?;
    let entries = complete_dirty_tuples(peer)?;
    if peer["owner_id"] != previous["owner_id"]
        || peer["instance_id"] != previous["instance_id"]
        || entries.keys().cloned().collect::<BTreeSet<_>>() != expected
        || entries[&hot].0 != incarnations[&hot].0
    {
        return Err("fresh write did not establish new evidence under the surviving bucket incarnation and process".into());
    }
    let candidate = run
        .barrier
        .wait_stage("cycle_candidate", leader_pid, Some(cycle), false)
        .await?;
    assert_scope(&candidate, &expected, &entries)?;
    if candidate["detail"]["converged"] != true || !names(&candidate["detail"]["pending_buckets"])?.is_empty() {
        return Err("fresh-process candidate did not converge its new write".into());
    }
    let root = run.barrier.wait_stage("root_durable", leader_pid, Some(cycle), true).await?;
    assert_root_binding(&candidate, &root)?;
    assert_walked_buckets(run, cycle, &expected)?;
    let physical_root = run.authoritative_root()?;
    assert_physical_root(&root, &physical_root)?;
    let usage: DataUsageInfo = serde_json::from_value(physical_root["usage"].clone())?;
    if !run.usage_matches(&usage, &run.buckets.iter().map(String::as_str).collect::<Vec<_>>()) {
        return Err("fresh process publication lost an acknowledged object/version total".into());
    }
    let queue = run.scoped_ack_queue(receiver, peer, expected, 1)?;
    run.barrier.disarm()?;
    run.barrier.release("root_durable", leader_pid)?;
    let send = run.barrier.wait_stage("ack_send", leader_pid, Some(cycle), false).await?;
    let ack = scoped_ack_for_peer(&root, &send, peer)?;
    if tuples(&ack["entries"])? != entries {
        return Err("fresh process ACK did not carry its own newly certified dirty tuple".into());
    }
    queue.wait_for_packets(1).await?;
    let retained = queue
        .receipts()
        .into_iter()
        .next()
        .ok_or("fresh process ACK packet disappeared")?;
    assert_retained_ack(&retained, ack, &run.dist.cluster.nodes[receiver].address)?;
    queue.settle(0, ScopedAckFault::Forward)?;
    let receipt = queue.wait_for_completion(0).await?;
    assert_forwarded_ack(&receipt, ack, 1)?;
    let (before, after) = applied_ack_after(run, receiver_pid, ack, None).await?;
    if dirty_generations(&before)? != BTreeMap::from([(hot, entries[&run.buckets[2]].1)])
        || !dirty_generations(&after)?.is_empty()
        || !complete_dirty_tuples(&run.dirty_snapshot(receiver).await?)?.is_empty()
    {
        return Err("fresh process clearing lacked the matching pre-apply tuple and actual successful removal".into());
    }
    Ok(json!({"write": write, "peer": peer, "start": start,
        "coordinator_instance": start["detail"]["activity_before"]["<local>"]["instance_id"], "candidate": candidate, "root": root,
        "physical_root": physical_root, "send": send, "retained": retained, "transport": receipt,
        "before_apply": before, "after_apply": after}))
}

async fn reordered_scoped_acks(newer_generation: bool) -> TestResult {
    init_logging();
    let label = if newer_generation {
        "ack-generation-reorder"
    } else {
        "ack-logical-duplicate"
    };
    let mut run = PeerBucketRun::start_with_ack_faults(label).await?;
    let scheduler_config = run.assert_default_cadence().await?;
    let baseline = run.quiet_baseline().await?;
    let leader = run.leader().await?;
    let leader_pid = run.pid(leader)?;
    let producer = (leader + 1) % 4;
    let producer_pid = run.pid(producer)?;
    let address = run.peer_address(producer);
    let [cold_a, cold_b, hot] = run.buckets.clone();
    let cold = BTreeSet::from([cold_a.clone(), cold_b.clone()]);
    let all = run.buckets.iter().cloned().collect::<BTreeSet<_>>();
    let log_path = run.dist.cluster.node_capture_log_paths[leader]
        .clone()
        .ok_or("coordinator has no captured log")?;
    run.write_evidence(
        "baseline",
        &json!({"topology": run.topology, "baseline": baseline,
        "scheduler_config": scheduler_config, "objects": run.objects, "leader_node": leader, "producer_node": producer}),
    )?;
    run.barrier.arm("before_start_snapshot", leader_pid, None)?;
    run.barrier.start_session(&run.buckets)?;
    run.put(leader, &hot, "queued-wake-start", 149).await?;
    let mut start_gate = run
        .barrier
        .wait_stage("before_start_snapshot", leader_pid, None, true)
        .await?;
    let first_cycle = identity(&start_gate)?;
    if baseline.scanner_epoch != Some(first_cycle.0) || baseline.scanner_cycle.is_none_or(|cycle| cycle >= first_cycle.1) {
        return Err("retained ACK window did not follow the quiet baseline under the same leader epoch".into());
    }
    for bucket in &all {
        run.put(producer, bucket, "queued-initial-generation", 137).await?;
    }
    let mut writer = Some(run.start_hot_writer(producer, &hot)?);
    let mut hot_stream = Vec::new();
    let mut queue = None;
    let mut cycles = Vec::new();
    let mut deadline_log = None;
    for index in 0..2 {
        let cycle = identity(&start_gate)?;
        if cycle.0 != first_cycle.0 || run.leader().await? != leader || (index == 1 && cycle.1 <= first_cycle.1) {
            return Err("retained ACK cycles changed coordinator or epoch".into());
        }
        let mut observed = queued_hot_cycle(&mut run, leader, producer, &start_gate).await?;
        let peer = peer_for_address(&observed["start"], &address)?.clone();
        if index == 0 {
            queue = Some(run.scoped_ack_queue(producer, &peer, cold.clone(), 2)?);
        }
        let hot_writer = writer.as_mut().ok_or("hot writer ended before the second candidate")?;
        hot_writer.wait_for_completed(hot_writer.completed() + 1).await?;
        if index == 1 {
            hot_stream = writer.take().ok_or("hot writer already joined")?.finish().await?;
            run.objects.extend(hot_stream.clone());
        }
        run.put(producer, &hot, &format!("queued-after-root-{}", cycle.1), 257)
            .await?;
        run.barrier.arm("before_start_snapshot", leader_pid, None)?;
        run.put(leader, &hot, &format!("queued-wake-next-{}", cycle.1), 151).await?;
        let log_offset = std::fs::metadata(&log_path)?.len();
        run.barrier.release("root_durable", leader_pid)?;
        let send = run.barrier.wait_stage("ack_send", leader_pid, Some(cycle), false).await?;
        let ack = scoped_ack_for_peer(&observed["root"], &send, &peer)?.clone();
        let entries = tuples(&ack["entries"])?;
        let snapshot = tuples(&peer["buckets"])?;
        if entries.keys().cloned().collect::<BTreeSet<_>>() != cold
            || entries.iter().any(|(bucket, entry)| snapshot.get(bucket) != Some(entry))
        {
            return Err("retained production ACK did not bind exactly the certified cold tuples".into());
        }
        let ack_queue = queue.as_ref().ok_or("ACK queue was not armed")?;
        ack_queue.wait_for_packets(index + 1).await?;
        let captured = ack_queue.receipts()[index].clone();
        assert_retained_ack(&captured, &ack, &run.dist.cluster.nodes[producer].address)?;
        observed["send"] = send;
        observed["ack"] = ack;
        observed["captured"] = captured;
        run.write_evidence(&format!("queued-cycle-{index}"), &observed)?;
        cycles.push(observed);
        if index == 0 {
            // No proxy response or request loss is injected. Reaching the next
            // cycle requires the genuine caller deadline to end this attempt.
            start_gate = run
                .barrier
                .wait_stage_after("before_start_snapshot", leader_pid, None, true, sequence(&start_gate)?)
                .await?;
            wait_until(
                CYCLE_TIMEOUT,
                || async { Ok(ack_deadline_log(&log_path, log_offset)?.is_some()) },
                "actual production ACK deadline",
            )
            .await?;
            deadline_log = ack_deadline_log(&log_path, log_offset)?;
            assert_retained_ack(&ack_queue.receipts()[0], &cycles[0]["ack"], &run.dist.cluster.nodes[producer].address)?;
            if receiver_observation(&run, "ack_before_apply", producer_pid, &cycles[0]["ack"])?.is_some()
                || receiver_observation(&run, "ack_after_apply", producer_pid, &cycles[0]["ack"])?.is_some()
            {
                return Err("held first ACK reached the receiver before its caller expired".into());
            }
            if newer_generation {
                for bucket in &cold {
                    run.put(producer, bucket, "queued-newer-generation", 211).await?;
                }
            }
        }
    }
    let queue = queue.ok_or("two production ACKs were not retained")?;
    let first_ack = &cycles[0]["ack"];
    let second_ack = &cycles[1]["ack"];
    let first_entries = tuples(&first_ack["entries"])?;
    let second_entries = tuples(&second_ack["entries"])?;
    if first_ack["owner_id"] != second_ack["owner_id"]
        || first_ack["instance_id"] != second_ack["instance_id"]
        || cold.iter().any(|bucket| {
            let before = &first_entries[bucket];
            let after = &second_entries[bucket];
            before.0 != after.0
                || if newer_generation {
                    after.1 <= before.1
                } else {
                    after.1 != before.1
                }
        })
    {
        return Err("second real cycle did not produce the required same-owner cold generation relation".into());
    }
    let first_packet = &cycles[0]["captured"];
    let second_packet = &cycles[1]["captured"];
    for field in ["auth_nonce", "replay_nonce", "signature_sha256", "replay_signature_sha256"] {
        if !first_packet[field].is_null() && (second_packet[field].is_null() || first_packet[field] == second_packet[field]) {
            return Err(format!("separate production ACKs reused authentication field {field}").into());
        }
    }
    if first_packet["request_sha256"] == second_packet["request_sha256"]
        || first_packet["request_headers_sha256"] == second_packet["request_headers_sha256"]
    {
        return Err("logical ACK duplication reused a raw signed request instead of a fresh production request".into());
    }
    queue.settle(1, ScopedAckFault::Forward)?;
    let second_receipt = queue.wait_for_completion(1).await?;
    assert_forwarded_ack(&second_receipt, second_ack, 2)?;
    let (second_before, second_after) = applied_ack_after(&run, producer_pid, second_ack, None).await?;
    let after_second = dirty_generations(&second_after)?;
    let second_post = tuples(&peer_for_address(&cycles[1]["post"], &address)?["buckets"])?;
    if second_after["detail"]["cleared"] != 2
        || after_second.keys().cloned().collect::<BTreeSet<_>>() != BTreeSet::from([hot.clone()])
        || after_second
            .get(&hot)
            .is_none_or(|generation| *generation <= second_post[&hot].1)
    {
        return Err("newer delivery did not clear exactly cold while retaining the later hot generation".into());
    }
    let recovery_gate = run
        .barrier
        .wait_stage_after("before_start_snapshot", leader_pid, None, true, sequence(&start_gate)?)
        .await?;
    let recovery_cycle = identity(&recovery_gate)?;
    if recovery_cycle.0 != first_cycle.0 || recovery_cycle.1 <= identity(&cycles[1]["root"])?.1 || run.leader().await? != leader {
        return Err("recovery cycle changed coordinator, epoch, or order".into());
    }
    if newer_generation {
        for bucket in &cold {
            run.put(producer, bucket, "queued-newest-generation", 307).await?;
        }
    }
    assert_retained_ack(&queue.receipts()[0], first_ack, &run.dist.cluster.nodes[producer].address)?;
    queue.settle(0, ScopedAckFault::Forward)?;
    let first_receipt = queue.wait_for_completion(0).await?;
    assert_forwarded_ack(&first_receipt, first_ack, 0)?;
    let (first_before, first_after) = applied_ack_after(&run, producer_pid, first_ack, Some(sequence(&second_after)?)).await?;
    let remaining = dirty_generations(&first_after)?;
    let recovery_scope = if newer_generation {
        all.clone()
    } else {
        BTreeSet::from([hot.clone()])
    };
    if first_after["detail"]["cleared"] != 0
        || first_before["detail"]["dirty_snapshot"] != first_after["detail"]["dirty_snapshot"]
        || remaining.keys().cloned().collect::<BTreeSet<_>>() != recovery_scope
        || remaining.get(&hot) != after_second.get(&hot)
        || (newer_generation && cold.iter().any(|bucket| remaining[bucket] <= second_entries[bucket].1))
        || first_receipt["forwarded_at_ms"]
            .as_u64()
            .zip(second_receipt["backend_completed_at_ms"].as_u64())
            .is_none_or(|(first, second)| first < second)
    {
        return Err(format!("late old ACK changed responsibility or lost a newer generation: {first_after}").into());
    }
    run.barrier.arm("root_durable", leader_pid, Some(recovery_cycle))?;
    run.barrier.release("before_start_snapshot", leader_pid)?;
    let recovery_start = run
        .barrier
        .wait_stage("start_snapshot", leader_pid, Some(recovery_cycle), false)
        .await?;
    let recovery_peer = peer_for_address(&recovery_start, &address)?;
    let recovery_entries = tuples(&recovery_peer["buckets"])?;
    if recovery_entries
        .iter()
        .map(|(bucket, entry)| (bucket.clone(), entry.1))
        .collect::<BTreeMap<_, _>>()
        != remaining
    {
        return Err("next real snapshot did not retain the exact generations surviving the old ACK".into());
    }
    let recovery_candidate = run
        .barrier
        .wait_stage("cycle_candidate", leader_pid, Some(recovery_cycle), false)
        .await?;
    assert_scope(&recovery_candidate, &recovery_scope, &recovery_entries)?;
    if !names(&recovery_candidate["detail"]["pending_buckets"])?.is_empty() || recovery_candidate["detail"]["converged"] != true {
        return Err("healthy recovery failed to certify the remaining actual generations".into());
    }
    let recovery_root = run
        .barrier
        .wait_stage("root_durable", leader_pid, Some(recovery_cycle), true)
        .await?;
    assert_root_binding(&recovery_candidate, &recovery_root)?;
    let queue_receipts = queue.receipts();
    drop(queue);
    run.barrier.disarm()?;
    run.barrier.release("root_durable", leader_pid)?;
    let recovery_send = run
        .barrier
        .wait_stage("ack_send", leader_pid, Some(recovery_cycle), false)
        .await?;
    let recovery_ack = scoped_ack_for_peer(&recovery_root, &recovery_send, recovery_peer)?;
    if tuples(&recovery_ack["entries"])? != recovery_entries {
        return Err("recovery ACK did not preserve the surviving next-snapshot tuples".into());
    }
    let (recovery_before, recovery_after) =
        applied_ack_after(&run, producer_pid, recovery_ack, Some(sequence(&first_after)?)).await?;
    if recovery_after["detail"]["cleared"].as_u64() != Some(u64::try_from(recovery_scope.len())?)
        || !dirty_generations(&recovery_after)?.is_empty()
    {
        return Err("healthy production ACK did not clear precisely the final retained responsibility".into());
    }
    let recovered_usage = run.quiet_baseline().await?;
    run.barrier.stop_session()?;
    run.verify_objects().await?;
    let physical_after_convergence = converged_shards(&run)?;
    run.write_evidence(
        label,
        &json!({"topology": run.topology, "objects": run.objects, "baseline": baseline,
        "scheduler_config": scheduler_config, "leader_node": leader, "leader_pid": leader_pid,
        "producer_node": producer, "producer_pid": producer_pid, "newer_generation": newer_generation,
        "hot_stream": hot_stream, "deadline_log": deadline_log, "cycles": cycles, "queue_receipts": queue_receipts,
        "second_before": second_before, "second_after": second_after, "first_before": first_before, "first_after": first_after,
        "recovery_scope": recovery_scope, "recovery_gate": recovery_gate, "recovery_start": recovery_start,
        "recovery_candidate": recovery_candidate, "recovery_root": recovery_root, "recovery_send": recovery_send,
        "recovery_before": recovery_before, "recovery_after": recovery_after, "recovered_usage": recovered_usage,
        "physical_after_convergence": physical_after_convergence,
        "observations": run.barrier.observations(false)?}),
    )
}

fn assert_worker_cycle(
    evidence: &Value,
    candidate: &Value,
    root: &Value,
    scope: &BTreeSet<String>,
    fleet: &Value,
    require_walks: bool,
) -> TestResult<Value> {
    let cycle = identity(root)?;
    let manifests = evidence["manifests"].as_array().ok_or("worker evidence omitted manifests")?;
    let matching = manifests
        .iter()
        .filter(|manifest| manifest["epoch"].as_u64() == Some(cycle.0) && manifest["cycle"].as_u64() == Some(cycle.1))
        .collect::<Vec<_>>();
    if matching.len() != 1 {
        return Err("worker cycle manifest is not unique".into());
    }
    let manifest = matching[0];
    if manifest["candidate"] != candidate["detail"]
        || manifest["root"] != root["detail"]
        || manifest["pid"] != root["pid"]
        || manifest["nonce"] != root["nonce"]
    {
        return Err("worker manifest is not bound to the actual candidate and durable root".into());
    }
    let leader = usize::try_from(fleet["leader"].as_u64().ok_or("fleet omitted leader")?)?;
    let events = evidence["events"].as_array().ok_or("worker evidence omitted raw events")?;
    let event_at = |sequence: &Value| -> TestResult<&Value> {
        let sequence = usize::try_from(sequence.as_u64().ok_or("worker event sequence is absent")?)?;
        let event = events.get(sequence).ok_or("worker raw event was omitted")?;
        if event["epoch"].as_u64() != Some(cycle.0) || event["cycle"].as_u64() != Some(cycle.1) {
            return Err("worker event crossed cycle identity".into());
        }
        Ok(event)
    };
    let endpoint = |value: &str| -> TestResult<(String, u16)> {
        let value = if value.contains("://") {
            value.to_string()
        } else {
            format!("http://{value}")
        };
        let url = reqwest::Url::parse(&value)?;
        Ok((
            url.host_str().ok_or("worker endpoint omitted host")?.to_string(),
            url.port_or_known_default().ok_or("worker endpoint omitted port")?,
        ))
    };
    let mut tuples_seen = BTreeSet::new();
    let mut physical = BTreeSet::new();
    let mut old_workers = Vec::new();
    let mut validate_context = |context: &Value, remote: bool, walked: bool| -> TestResult<Option<Value>> {
        let bucket = context["bucket"].as_str().ok_or("worker context omitted bucket")?;
        let pool = context["pool"].as_u64().ok_or("worker omitted pool")?;
        let set = context["set"].as_u64().ok_or("worker omitted set")?;
        if !scope.contains(bucket) || pool != 0 || set >= 4 || !tuples_seen.insert((pool, set, bucket.to_string())) {
            return Err("worker inventory has a cold/unknown/extra or duplicate request".into());
        }
        let coverage = candidate["detail"]["set_coverage"]
            .as_array()
            .ok_or("candidate omitted actual completions")?;
        let incarnation = context["bucket_incarnation"]
            .as_str()
            .ok_or("worker omitted captured incarnation")?;
        if uuid::Uuid::parse_str(incarnation)?.is_nil()
            || !coverage.iter().any(|row| {
                row["pool"].as_u64() == Some(pool)
                    && row["set"].as_u64() == Some(set)
                    && row["bucket_incarnations"][bucket].as_str() == Some(incarnation)
            })
        {
            return Err("worker was not in the candidate's actual completion set".into());
        }
        let path = context["worker_path"].as_str().ok_or("worker omitted actual disk path")?;
        let disk_uuid = context["disk_uuid"].as_str().ok_or("worker omitted cached disk UUID")?;
        let disk_index = usize::try_from(context["disk_index"].as_u64().ok_or("worker omitted disk index")?)?;
        if uuid::Uuid::parse_str(disk_uuid)?.is_nil() {
            return Err("worker disk UUID is nil".into());
        }
        let drives = fleet["drives_after"]
            .as_array()
            .filter(|nodes| nodes.len() == 4)
            .ok_or("fleet omitted drives")?;
        let mut owners = Vec::new();
        for (node, disks) in drives.iter().enumerate() {
            for disk in disks.as_array().ok_or("fleet drive inventory is malformed")? {
                if disk["drive"].as_str() == Some(path)
                    && disk["format"]["xl"]["this"].as_str() == Some(disk_uuid)
                    && disk["set"].as_u64() == Some(set)
                    && disk["format"]["xl"]["sets"]
                        .get(usize::try_from(set)?)
                        .and_then(|group| group.get(disk_index))
                        .and_then(Value::as_str)
                        == Some(disk_uuid)
                {
                    owners.push(node);
                }
            }
        }
        if owners.len() != 1 || remote == (owners[0] == leader) {
            return Err("worker endpoint/path/UUID does not map to the measured local/remote process".into());
        }
        let node = owners[0];
        if remote {
            let host = fleet["peers"][node]["host"].as_str().ok_or("worker peer omitted host")?;
            if endpoint(context["worker_endpoint"].as_str().ok_or("worker omitted endpoint")?)? != endpoint(host)?
                || fleet["processes"][node]["sha256"] != fleet["old_sha256"]
            {
                return Err("remote worker is not the measured official O endpoint and executable".into());
            }
        }
        if walked {
            physical.insert((pool, set, bucket.to_string()));
        }
        Ok((remote && walked).then(|| {
            json!({"node": node, "pid": fleet["processes"][node]["pid"],
            "executable_sha256": fleet["processes"][node]["sha256"], "owner_id": fleet["peers"][node]["owner_id"],
            "instance_id": fleet["peers"][node]["instance_id"], "context": context})
        }))
    };
    let requests = manifest["requests"]
        .as_object()
        .ok_or("worker manifest omitted complete request map")?;
    for (id, request) in requests {
        let context = &request["context"];
        if context["request_id"].as_str() != Some(id.as_str()) {
            return Err("worker request map changed request ID".into());
        }
        let opening = event_at(&request["opening_sequence"])?;
        let complete = event_at(&request["complete_sequence"])?;
        let progress = request["progress"]
            .as_array()
            .filter(|values| values.len() == 3)
            .ok_or("worker completion omitted progress")?;
        let positive = progress
            .iter()
            .map(|value| value.as_u64().ok_or("worker progress is malformed"))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|value| *value > 0);
        if opening["kind"] != "remote_scan_opening"
            || opening["detail"] != *context
            || complete["kind"] != "remote_scan_complete"
            || complete["detail"]["context"] != *context
            || complete["detail"]["request_id"].as_str() != Some(id.as_str())
            || complete["detail"]["progress"] != request["progress"]
            || complete["detail"]["eof_verified"] != true
            || complete["detail"]["mac_source_plan_verified"] != true
            || request["opening_sequence"].as_u64() >= request["complete_sequence"].as_u64()
        {
            return Err("remote worker is unpaired or lacks verified terminal EOF and source/plan proof".into());
        }
        if require_walks && !positive {
            return Err("verified remote Complete made zero physical progress".into());
        }
        if let Some(worker) = validate_context(context, true, positive)? {
            old_workers.push(worker);
        }
    }
    for walk in manifest["local_walks"]
        .as_array()
        .ok_or("worker manifest omitted local walk inventory")?
    {
        let event = event_at(&walk["sequence"])?;
        if event["kind"] != "local_walk" || event["detail"] != walk["context"] {
            return Err("local walk is absent from the raw worker inventory".into());
        }
        validate_context(&walk["context"], false, true)?;
    }
    let expected = (0..4_u64)
        .flat_map(|set| scope.iter().map(move |bucket| (0_u64, set, bucket.clone())))
        .collect::<BTreeSet<_>>();
    if require_walks && (physical != expected || tuples_seen != expected || old_workers.is_empty()) {
        return Err(
            "hot-only cycle lacks exact four-set physical execution or a verified positive-progress official O worker".into(),
        );
    }
    Ok(json!({"epoch": cycle.0, "cycle": cycle.1, "request_count": requests.len(),
        "physical_walks": physical, "old_workers": old_workers, "cold_requests": 0}))
}

#[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
#[test]
fn worker_cycle_rejects_cold_zero_progress_and_incomplete_sets() {
    let candidate = json!({"detail":{"epoch":1,"cycle":2,"set_coverage":[]}});
    let root = json!({"pid":1,"nonce":"unit","detail":{"epoch":1,"cycle":2}});
    let fleet = json!({"leader":0});
    let scope = BTreeSet::from(["hot".to_string()]);
    let evidence = |bucket: &str, progress: [u64; 3]| {
        let context = json!({"request_id":"request","bucket":bucket,"pool":0,"set":0});
        let request = json!({"context":context,"opening_sequence":0,"complete_sequence":1,"progress":progress});
        json!({"manifests":[{"epoch":1,"cycle":2,"pid":1,"nonce":"unit", "candidate":candidate["detail"],
            "root":root["detail"],"requests":{"request":request},"local_walks":[]}],
            "events":[{"epoch":1,"cycle":2,"kind":"remote_scan_opening","detail":context},
                {"epoch":1,"cycle":2,"kind":"remote_scan_complete","detail":{"context":context,"request_id":"request",
                    "progress":progress,"eof_verified":true,"mac_source_plan_verified":true}}]})
    };
    let cold = assert_worker_cycle(&evidence("cold", [1, 0, 0]), &candidate, &root, &scope, &fleet, true)
        .expect_err("complete cold request must not be hidden by filtering");
    assert!(cold.to_string().contains("cold/unknown/extra"));
    let zero = assert_worker_cycle(&evidence("hot", [0, 0, 0]), &candidate, &root, &scope, &fleet, true)
        .expect_err("Current Complete with zero progress cannot prove a physical walk");
    assert!(zero.to_string().contains("zero physical progress"));
    let incarnation = uuid::Uuid::new_v4();
    let disk_ids = (0..4)
        .map(|_| (0..4).map(|_| uuid::Uuid::new_v4()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let sets = (0..4)
        .map(|set| disk_ids.iter().map(|disks| disks[set]).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let drives = disk_ids
        .iter()
        .enumerate()
        .map(|(node, disks)| {
            disks
                .iter()
                .enumerate()
                .map(|(set, disk)| {
                    json!({"drive":format!("/node-{node}/set-{set}"),"set":set,
            "format":{"xl":{"this":disk,"sets":sets}}})
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let old_sha256 = "a7fd2f93862ff7b6a7e1e6603e69f31a506abfafcc64c5067ded0516f24a0b6d";
    let candidate = json!({"detail":{"epoch":1,"cycle":2,"set_coverage":(0..4).map(|set| json!({"pool":0,"set":set,
        "bucket_incarnations":{"hot":incarnation}})).collect::<Vec<_>>()}});
    let fleet = json!({"leader":0,"old_sha256":old_sha256,"drives_after":drives,
        "processes":[{"pid":1,"sha256":"b".repeat(64)},{"pid":2,"sha256":old_sha256},
            {"pid":3,"sha256":old_sha256},{"pid":4,"sha256":old_sha256}],
        "peers":[{}, {"host":"127.0.0.1:29001","owner_id":uuid::Uuid::new_v4(),"instance_id":uuid::Uuid::new_v4()}, {}, {}]});
    let valid = |set_count: usize| {
        let mut requests = serde_json::Map::new();
        let mut events = Vec::new();
        for set in 0..set_count {
            let id = uuid::Uuid::new_v4().to_string();
            let context = json!({"epoch":1,"cycle":2,"request_id":id,"bucket":"hot","pool":0,"set":set,
                "bucket_incarnation":incarnation,"worker_path":format!("/node-1/set-{set}"),"disk_uuid":disk_ids[1][set],
                "disk_index":1,"worker_endpoint":"http://127.0.0.1:29001","server_epoch":uuid::Uuid::new_v4(),
                "wire_session_id":uuid::Uuid::new_v4(),"wire_session_sequence":set,
                "reuse_plan_sha256":"c".repeat(64),"coverage_plan_sha256":"d".repeat(64)});
            let opening = events.len();
            events.push(json!({"epoch":1,"cycle":2,"kind":"remote_scan_opening","detail":context}));
            events.push(json!({"epoch":1,"cycle":2,"kind":"remote_scan_complete","detail":{"context":context,
                "request_id":id,"progress":[1,0,0],"eof_verified":true,"mac_source_plan_verified":true}}));
            requests.insert(
                id,
                json!({"context":context,"opening_sequence":opening,"complete_sequence":opening+1,"progress":[1,0,0]}),
            );
        }
        json!({"manifests":[{"epoch":1,"cycle":2,"pid":1,"nonce":"unit","candidate":candidate["detail"],
            "root":root["detail"],"requests":requests,"local_walks":[]}],"events":events})
    };
    let healthy = assert_worker_cycle(&valid(4), &candidate, &root, &scope, &fleet, true)
        .expect("four completed positive-progress official O sets must satisfy the worker contract");
    assert_eq!(healthy["old_workers"].as_array().expect("mapped O workers").len(), 4);
    let partial = valid(3);
    let otherwise_valid = assert_worker_cycle(&partial, &candidate, &root, &scope, &fleet, false)
        .expect("the missing-set example must pass pairing, progress, incarnation and official O mapping");
    assert_eq!(otherwise_valid["old_workers"].as_array().expect("mapped O workers").len(), 3);
    let missing = assert_worker_cycle(&partial, &candidate, &root, &scope, &fleet, true)
        .expect_err("three valid positive O sets cannot substitute for all four sets");
    assert!(missing.to_string().contains("exact four-set"));
}

async fn hot_window(fault: Option<ScopedAckFault>) -> TestResult {
    hot_window_with_old_followers(fault, None).await
}

#[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_new_coordinator_certifies_old_peers_and_scopes_next_cycle() -> TestResult {
    mixed_version_n_over_o(true).await
}

#[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
#[tokio::test(flavor = "multi_thread")]
async fn scanner_peer_bucket_new_coordinator_certifies_old_worker_peers_and_scopes_next_cycle() -> TestResult {
    mixed_version_n_over_o(false).await
}

#[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
async fn mixed_version_n_over_o(old_scanner_enabled: bool) -> TestResult {
    let old_binary = std::env::var_os("RUSTFS_SCANNER_COMPAT_OLD_BINARY")
        .filter(|path| !path.is_empty())
        .map(std::path::PathBuf::from)
        .ok_or("RUSTFS_SCANNER_COMPAT_OLD_BINARY must identify the qualified previous executable")?;
    let old_sha256 = std::env::var("RUSTFS_SCANNER_COMPAT_OLD_SHA256")
        .map_err(|error| format!("RUSTFS_SCANNER_COMPAT_OLD_SHA256 must identify that executable: {error}"))?;
    hot_window_with_old_followers(None, Some((&old_binary, &old_sha256, old_scanner_enabled))).await
}

async fn hot_window_with_old_followers(
    fault: Option<ScopedAckFault>,
    old_followers: Option<(&std::path::Path, &str, bool)>,
) -> TestResult {
    init_logging();
    let mixed = old_followers.is_some();
    let observe_workers = old_followers.is_some_and(|(_, _, enabled)| !enabled);
    if mixed && fault.is_some() {
        return Err("the mixed-version gate requires successful original ACK forwarding".into());
    }
    let label = match fault {
        None if old_followers.is_some_and(|(_, _, enabled)| !enabled) => "new-coordinator-old-worker-peers",
        None if mixed => "new-coordinator-old-peers",
        None => "hot-window",
        Some(ScopedAckFault::RequestLoss) => "ack-request-loss",
        Some(ScopedAckFault::ResponseLoss) => "ack-response-loss",
        Some(ScopedAckFault::Forward) => {
            return Err("fault scenario needs actual request or response loss".into());
        }
    };
    let mut run = if mixed {
        PeerBucketRun::start_for_crash(label).await?
    } else if fault.is_some() {
        PeerBucketRun::start_with_ack_faults(label).await?
    } else {
        PeerBucketRun::start(label).await?
    };
    let mixed_fleet = if let Some((old_binary, expected_old_sha256, old_scanner_enabled)) = old_followers {
        #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
        {
            run.replace_followers_with_old_binary(old_binary, expected_old_sha256, old_scanner_enabled)
                .await?
        }
        #[cfg(not(all(target_os = "linux", feature = "scanner-peer-mixed-version")))]
        {
            let _ = (old_binary, expected_old_sha256, old_scanner_enabled);
            return Err("mixed-version process evidence requires the dedicated Linux feature".into());
        }
    } else {
        Value::Null
    };
    let mixed_initial_physical = if mixed { json!(converged_shards(&run)?) } else { Value::Null };
    let scheduler_config = run.assert_default_cadence().await?;
    let baseline = run.quiet_baseline().await?;
    let leader = run.leader().await?;
    let leader_pid = run.pid(leader)?;
    let producer = (leader + 1) % 4;
    let producers = if fault.is_some() {
        vec![producer, (leader + 2) % 4]
    } else {
        vec![producer]
    };
    if mixed
        && (mixed_fleet["leader"].as_u64() != Some(u64::try_from(leader)?)
            || mixed_fleet["leader_pid"].as_u64() != Some(u64::from(leader_pid)))
    {
        return Err("the hot window did not retain its measured new coordinator".into());
    }
    let producer_pid = run.pid(producer)?;
    let producer_address = run.peer_address(producer);
    let [cold_a, cold_b, hot] = run.buckets.clone();
    let cold = BTreeSet::from([cold_a.clone(), cold_b.clone()]);
    let all = run.buckets.iter().cloned().collect::<BTreeSet<_>>();
    run.write_evidence(
        "baseline",
        &json!({"topology": run.topology, "baseline": baseline, "objects": run.objects, "scheduler_config": scheduler_config,
            "leader_node": leader, "leader_pid": leader_pid, "producer_node": producer, "producer_pid": producer_pid}),
    )?;

    run.barrier.arm("before_start_snapshot", leader_pid, None)?;
    run.barrier.start_session(&run.buckets)?;
    // A quiet baseline has no pending wake. A local S3 mutation reaches the
    // real DirtyUsage path; waiting for the timer would require a full scan.
    let start_cycle_wake = run.put(leader, &hot, "wake-start-cycle", 149).await?;
    run.write_evidence("start-cycle-wake", &json!({"leader_pid": leader_pid, "put": start_cycle_wake}))?;
    let start_gate = run
        .barrier
        .wait_stage("before_start_snapshot", leader_pid, None, true)
        .await?;
    let cycle = identity(&start_gate)?;
    if baseline.scanner_epoch != Some(cycle.0) || baseline.scanner_cycle.is_none_or(|value| value >= cycle.1) {
        return Err("test window did not follow the quiet baseline under the observed leader epoch".into());
    }
    if observe_workers {
        run.barrier.enable_worker_observation(leader_pid, &start_gate)?;
    }
    // These are actual mutations after leading activity and before the start
    // map RPCs. All three buckets must appear on the remote ingress peer.
    for node in &producers {
        for bucket in [&cold_a, &cold_b, &hot] {
            run.put(*node, bucket, "start-window", 137).await?;
        }
    }
    run.barrier.arm("before_post_walk_snapshot", leader_pid, Some(cycle))?;
    run.barrier.release("before_start_snapshot", leader_pid)?;
    let start = run
        .barrier
        .wait_stage("start_snapshot", leader_pid, Some(cycle), false)
        .await?;
    let start_peer = peer_for_address(&start, &producer_address)?;
    assert_snapshot_window(&start, start_peer)?;
    let start_producers = producers
        .iter()
        .map(|node| peer_for_address(&start, &run.peer_address(*node)).cloned())
        .collect::<TestResult<Vec<_>>>()?;
    for peer in &start_producers {
        assert_snapshot_window(&start, peer)?;
    }
    let start_identities = peer_identities(&start["detail"]["peers"])?;
    for node in (0..4).filter(|node| *node != leader) {
        let peer = peer_for_address(&start, &run.peer_address(node))?;
        if mixed
            && (peer["owner_id"] != mixed_fleet["peers"][node]["owner_id"]
                || peer["instance_id"] != mixed_fleet["peers"][node]["instance_id"])
        {
            return Err("the coordinator snapshot does not name the measured old peer instance".into());
        }
    }
    let start_tuples = tuples(&start_peer["buckets"])?;
    if start_tuples.keys().cloned().collect::<BTreeSet<_>>() != all {
        return Err(format!("remote producer did not own all three actual start tuples: {start_peer}").into());
    }

    run.barrier
        .wait_stage("before_post_walk_snapshot", leader_pid, Some(cycle), true)
        .await?;
    let mut hot_in_post_snapshot_window = Vec::new();
    for node in &producers {
        hot_in_post_snapshot_window.push(run.put(*node, &hot, "post-walk-window", 193).await?);
    }
    let mut hot_writer = if producers.len() == 1 {
        run.start_hot_writer(producer, &hot)?
    } else {
        run.start_alternating_hot_writer(&producers, &hot)?
    };
    hot_writer.wait_for_completed(producers.len()).await?;
    run.barrier.arm("root_durable", leader_pid, Some(cycle))?;
    run.barrier.release("before_post_walk_snapshot", leader_pid)?;
    let post = run
        .barrier
        .wait_stage("post_walk_snapshot", leader_pid, Some(cycle), false)
        .await?;
    let post_peer = peer_for_address(&post, &producer_address)?;
    assert_snapshot_window(&post, post_peer)?;
    let post_tuples = tuples(&post_peer["buckets"])?;
    if peer_identities(&post["detail"]["peers"])? != start_identities {
        return Err("a remote peer identity changed inside the certification window".into());
    }
    for bucket in &cold {
        if start_tuples.get(bucket) != post_tuples.get(bucket) {
            return Err(format!("cold bucket {bucket} changed inside the certification window").into());
        }
    }
    let hot_start = start_tuples.get(&hot).ok_or("hot bucket absent from actual start map")?;
    let hot_post = post_tuples.get(&hot).ok_or("hot bucket absent from actual post-walk map")?;
    if hot_start.0 != hot_post.0 || hot_post.1 <= hot_start.1 {
        return Err("hot S3 operation did not advance the same-incarnation remote dirty tuple".into());
    }
    for (node, start_peer) in producers.iter().zip(&start_producers) {
        let post_peer = peer_for_address(&post, &run.peer_address(*node))?;
        assert_snapshot_window(&post, post_peer)?;
        let previous = tuples(&start_peer["buckets"])?;
        let current = tuples(&post_peer["buckets"])?;
        if previous.keys().cloned().collect::<BTreeSet<_>>() != all
            || cold.iter().any(|bucket| previous.get(bucket) != current.get(bucket))
            || current
                .get(&hot)
                .zip(previous.get(&hot))
                .is_none_or(|(after, before)| after.0 != before.0 || after.1 <= before.1)
        {
            return Err(format!("alternating producer {node} did not preserve cold and advance hot: {post_peer}").into());
        }
    }

    let candidate = run
        .barrier
        .wait_stage("cycle_candidate", leader_pid, Some(cycle), false)
        .await?;
    assert_scope(&candidate, &all, &start_tuples)?;
    if candidate["detail"]["peer_certification"].as_bool() != Some(true)
        || peer_identities(&candidate["detail"]["certified_peers"])? != start_identities
        || names(&candidate["detail"]["pending_buckets"])? != BTreeSet::from([hot.clone()])
        || candidate["detail"]["converged"].as_bool() != Some(false)
    {
        return Err(format!("hot-window candidate is not complete with only hot pending: {candidate}").into());
    }
    let root = run.barrier.wait_stage("root_durable", leader_pid, Some(cycle), true).await?;
    for field in ["candidate_sha256", "coverage_sha256"] {
        let digest = candidate["detail"][field].as_str().filter(|value| value.len() == 64);
        if digest.is_none() || root["detail"][field].as_str() != digest {
            return Err(format!("durable root is not bound to the actual candidate {field}").into());
        }
    }
    if root["detail"]["root_sha256"].as_str().is_none_or(|value| value.len() != 64)
        || root["detail"]["root_version"].is_null()
        || !matches!(root["detail"]["outcome"].as_str(), Some("saved" | "already_durable"))
    {
        return Err(format!("root observation lacks a verified durable publication receipt: {root}").into());
    }
    let mixed_physical_root = if mixed {
        let physical = run.authoritative_root()?;
        assert_physical_root(&root, &physical)?;
        let usage: DataUsageInfo = serde_json::from_value(physical["usage"].clone())?;
        if usage.usage_snapshot_converged != Some(false)
            || usage.usage_snapshot_pending_buckets != vec![hot.clone()]
            || !run.usage_matches(&usage, &[&cold_a, &cold_b])
        {
            return Err("mixed-version physical root did not certify cold and retain only hot".into());
        }
        physical
    } else {
        Value::Null
    };
    let completed_at_root = hot_writer.completed();
    hot_writer.wait_for_completed(completed_at_root + producers.len()).await?;
    let hot_stream = hot_writer.finish().await?;
    run.objects.extend(hot_stream.clone());
    // The sealed pending set is immutable. A later write must survive the
    // actual receiver's generation check without resealing that candidate.
    if hot_stream.iter().map(|receipt| receipt.ingress_node).collect::<BTreeSet<_>>() != producers.iter().copied().collect()
        || hot_stream
            .iter()
            .enumerate()
            .any(|(index, receipt)| receipt.ingress_node != producers[index % producers.len()])
    {
        return Err("acknowledged hot stream did not alternate the actual S3 ingress processes".into());
    }
    let mut hot_after_root = Vec::new();
    for node in &producers {
        hot_after_root.push(run.put(*node, &hot, "after-root-before-ack", 257).await?);
    }
    let mixed_before_ack = if mixed {
        #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
        run.assert_mixed_roles(&mixed_fleet).await?;
        let snapshot = run.dirty_snapshot(producer).await?;
        let entries = complete_dirty_tuples(&snapshot)?;
        if snapshot["owner_id"] != start_peer["owner_id"]
            || snapshot["instance_id"] != start_peer["instance_id"]
            || entries.keys().cloned().collect::<BTreeSet<_>>() != all
            || cold.iter().any(|bucket| entries.get(bucket) != start_tuples.get(bucket))
            || entries
                .get(&hot)
                .is_none_or(|entry| entry.0 != hot_post.0 || entry.1 <= hot_post.1)
        {
            return Err("old receiver did not retain the original cold tuples and later hot write before ACK".into());
        }
        snapshot
    } else {
        Value::Null
    };
    let mixed_ack_started = mixed.then(std::time::Instant::now);
    let ack_queue = if mixed || fault.is_some() {
        Some(run.scoped_ack_queue(producer, start_peer, cold.clone(), 1)?)
    } else {
        None
    };
    run.barrier.arm("before_start_snapshot", leader_pid, None)?;
    // Advance the coordinator's local generation after this cycle's start.
    // The pending hot bucket then wakes the next scoped cycle immediately.
    let next_cycle_wake = run.put(leader, &hot, "wake-next-cycle", 151).await?;
    run.write_evidence("next-cycle-wake", &json!({"leader_pid": leader_pid, "put": next_cycle_wake}))?;
    run.barrier.release("root_durable", leader_pid)?;
    let ack_send = run.barrier.wait_stage("ack_send", leader_pid, Some(cycle), false).await?;
    if sequence(&root)? >= sequence(&ack_send)?
        || ack_send["detail"]["candidate_sha256"] != root["detail"]["candidate_sha256"]
        || ack_send["detail"]["coverage_sha256"] != root["detail"]["coverage_sha256"]
    {
        return Err("scoped ACK was not ordered after the same candidate's verified root".into());
    }
    let expected_ack = ack_send["detail"]["acks"]
        .as_array()
        .ok_or("sender omitted actual acknowledgements")?
        .iter()
        .find(|ack| ack["owner_id"] == start_peer["owner_id"] && ack["instance_id"] == start_peer["instance_id"])
        .ok_or("sender did not acknowledge the actual remote producer")?;
    let expected_entries = tuples(&expected_ack["entries"])?;
    if expected_ack["kind"].as_str() != Some("scoped")
        || expected_entries.keys().cloned().collect::<BTreeSet<_>>() != cold
        || expected_entries
            .iter()
            .any(|(bucket, entry)| start_tuples.get(bucket) != Some(entry))
    {
        return Err(format!("sender did not preserve the certified remote cold tuples: {expected_ack}").into());
    }
    let mut mixed_retained_ack = Value::Null;
    let fault_receipt = if mixed {
        let queue = ack_queue.as_ref().ok_or("mixed-version ACK queue is absent")?;
        queue.wait_for_packets(1).await?;
        let captured = queue
            .receipts()
            .into_iter()
            .next()
            .ok_or("old receiver ACK packet is absent")?;
        assert_retained_ack(&captured, expected_ack, &run.dist.cluster.nodes[producer].address)?;
        let started = mixed_ack_started.ok_or("mixed-version ACK clock is absent")?;
        if started.elapsed() >= std::time::Duration::from_secs(5) {
            return Err("old receiver ACK exceeded the unchanged sender budget before forwarding".into());
        }
        queue.settle(0, ScopedAckFault::Forward)?;
        let receipt = queue.wait_for_completion(0).await?;
        let ack_elapsed = started.elapsed();
        if ack_elapsed >= std::time::Duration::from_secs(5) {
            return Err("old receiver ACK exceeded the unchanged sender budget before completion".into());
        }
        assert_forwarded_ack(&receipt, expected_ack, 2)?;
        for field in [
            "request_sha256",
            "request_headers_sha256",
            "request_frame_hex",
            "request_bytes",
            "auth_nonce",
            "replay_nonce",
            "signature_sha256",
            "replay_signature_sha256",
            "receiver",
            "session_nonce",
            "request",
        ] {
            if captured[field] != receipt[field] {
                return Err(format!("old receiver ACK changed original packet field {field}").into());
            }
        }
        let frame = hex_simd::decode_to_vec(
            captured["request_frame_hex"]
                .as_str()
                .ok_or("ACK frame is absent")?
                .as_bytes(),
        )?;
        if captured["request_bytes"].as_u64() != Some(u64::try_from(frame.len())?)
            || captured["request_sha256"].as_str() != Some(super::harness::sha256_hex(&frame).as_str())
        {
            return Err("old receiver ACK frame failed its original byte digest".into());
        }
        mixed_retained_ack = captured;
        run.write_evidence(
            "mixed-original-ack",
            &json!({"retained": mixed_retained_ack, "transport": receipt,
            "elapsed_millis": ack_elapsed.as_millis()}),
        )?;
        receipt
    } else if let (Some(queue), Some(fault)) = (&ack_queue, fault) {
        queue.wait_for_packets(1).await?;
        let captured = queue
            .receipts()
            .into_iter()
            .next()
            .ok_or("scoped ACK queue omitted its retained packet")?;
        run.write_evidence("ack-fault-captured", &captured)?;
        if captured["request"]["owner_id"] != expected_ack["owner_id"]
            || captured["request"]["instance_id"] != expected_ack["instance_id"]
            || tuples(&captured["request"]["entries"])? != expected_entries
            || captured["receiver"].as_str() != Some(run.dist.cluster.nodes[producer].address.as_str())
            || captured["forwarded"].as_bool() != Some(false)
            || (captured["auth_nonce"].as_str().is_none_or(str::is_empty)
                && captured["replay_nonce"].as_str().is_none_or(str::is_empty))
            || (captured["signature_sha256"].as_str().is_none_or(|value| value.len() != 64)
                && captured["replay_signature_sha256"]
                    .as_str()
                    .is_none_or(|value| value.len() != 64))
        {
            return Err(format!("proxy did not retain the original signed cold ACK before forwarding: {captured}").into());
        }
        queue.settle(0, fault)?;
        let receipt = queue.wait_for_completion(0).await?;
        run.write_evidence("ack-fault", &receipt)?;
        match fault {
            ScopedAckFault::RequestLoss
                if receipt["outcome"] == "request_lost"
                    && receipt["forwarded"] == false
                    && receipt["backend_complete"] == false => {}
            ScopedAckFault::ResponseLoss
                if receipt["outcome"] == "response_lost"
                    && receipt["forwarded"] == true
                    && receipt["backend_complete"] == true
                    && receipt["response"]["status"] == 200
                    && receipt["response"]["grpc_status"] == "0"
                    && receipt["response"]["ack"]["owner_id"] == expected_ack["owner_id"]
                    && receipt["response"]["ack"]["instance_id"] == expected_ack["instance_id"]
                    && receipt["response"]["ack"]["cleared"] == 2
                    && receipt["response"]["ack"]["response_proof_bytes"]
                        .as_u64()
                        .is_some_and(|count| count > 0) => {}
            _ => {
                return Err(format!("scoped ACK fault lacks its actual transport/backend outcome: {receipt}").into());
            }
        }
        receipt
    } else {
        Value::Null
    };
    let find_receiver = |stage: &str| receiver_observation(&run, stage, producer_pid, expected_ack);
    let mut mixed_after_ack = Value::Null;
    let (before_ack, after_ack) = if mixed {
        let snapshot = run.dirty_snapshot(producer).await?;
        let entries = complete_dirty_tuples(&snapshot)?;
        let before = complete_dirty_tuples(&mixed_before_ack)?;
        if snapshot["owner_id"] != start_peer["owner_id"]
            || snapshot["instance_id"] != start_peer["instance_id"]
            || entries.keys().cloned().collect::<BTreeSet<_>>() != BTreeSet::from([hot.clone()])
            || entries.get(&hot) != before.get(&hot)
        {
            return Err("the authenticated old receiver did not clear precisely cold and preserve its later hot tuple".into());
        }
        mixed_after_ack = snapshot;
        (Value::Null, Value::Null)
    } else if fault == Some(ScopedAckFault::RequestLoss) {
        (Value::Null, Value::Null)
    } else {
        wait_until(
            CYCLE_TIMEOUT,
            || async { Ok(find_receiver("ack_after_apply")?.is_some()) },
            "real remote scoped ACK apply",
        )
        .await?;
        let before_ack = find_receiver("ack_before_apply")?.ok_or("receiver omitted pre-apply observation")?;
        let after_ack = find_receiver("ack_after_apply")?.ok_or("receiver omitted post-apply observation")?;
        let remaining = &after_ack["detail"]["dirty_snapshot"];
        let remaining_buckets = remaining["buckets"]
            .as_array()
            .ok_or("receiver omitted actual remaining dirty map")?;
        if sequence(&before_ack)? >= sequence(&after_ack)?
            || after_ack["detail"]["cleared"].as_u64() != Some(2)
            || remaining["complete"].as_bool() != Some(true)
            || remaining["pending_bucket_count"].as_u64() != Some(1)
            || remaining_buckets.len() != 1
            || remaining_buckets[0]["bucket"].as_str() != Some(hot.as_str())
            || remaining_buckets[0]["generation"]
                .as_u64()
                .is_none_or(|generation| generation <= hot_post.1)
        {
            return Err(
                format!("receiver did not clear precisely cold while preserving the later hot generation: {after_ack}").into(),
            );
        }
        (before_ack, after_ack)
    };

    let next_start_gate = run
        .barrier
        .wait_stage_after("before_start_snapshot", leader_pid, None, true, sequence(&start_gate)?)
        .await?;
    let next_cycle = identity(&next_start_gate)?;
    if next_cycle.0 != cycle.0 || next_cycle.1 <= cycle.1 || run.leader().await? != leader {
        return Err("next-cycle assertion lost the actual coordinator or epoch".into());
    }
    if fault == Some(ScopedAckFault::RequestLoss)
        && (find_receiver("ack_before_apply")?.is_some() || find_receiver("ack_after_apply")?.is_some())
    {
        return Err("lost scoped ACK request reached the receiver before the next cycle".into());
    }
    let mut secondary_acks = Vec::new();
    for (node, peer) in producers.iter().zip(&start_producers).skip(1) {
        let ack = ack_send["detail"]["acks"]
            .as_array()
            .ok_or("sender omitted acknowledgements")?
            .iter()
            .find(|ack| ack["owner_id"] == peer["owner_id"] && ack["instance_id"] == peer["instance_id"])
            .ok_or("second producer did not receive a scoped ACK")?;
        let entries = tuples(&ack["entries"])?;
        let original = tuples(&peer["buckets"])?;
        if ack["kind"] != "scoped"
            || entries.keys().cloned().collect::<BTreeSet<_>>() != cold
            || entries.iter().any(|(bucket, tuple)| original.get(bucket) != Some(tuple))
        {
            return Err("second producer ACK does not bind its own original cold tuples".into());
        }
        let pid = run.pid(*node)?;
        wait_until(
            CYCLE_TIMEOUT,
            || async { Ok(receiver_observation(&run, "ack_after_apply", pid, ack)?.is_some()) },
            "second real producer scoped ACK",
        )
        .await?;
        let applied = receiver_observation(&run, "ack_after_apply", pid, ack)?.ok_or("second producer omitted ACK apply")?;
        let remaining = &applied["detail"]["dirty_snapshot"];
        let buckets = remaining["buckets"]
            .as_array()
            .ok_or("second producer omitted remaining buckets")?;
        let hot_generation = tuples(&peer_for_address(&post, &run.peer_address(*node))?["buckets"])?
            .get(&hot)
            .ok_or("second producer omitted post-walk hot generation")?
            .1;
        if applied["detail"]["cleared"] != 2
            || remaining["complete"] != true
            || remaining["pending_bucket_count"] != 1
            || buckets.len() != 1
            || buckets[0]["bucket"] != hot
            || buckets[0]["generation"]
                .as_u64()
                .is_none_or(|generation| generation <= hot_generation)
        {
            return Err(format!("second producer did not clear cold and preserve its later hot tuple: {applied}").into());
        }
        secondary_acks.push(applied);
    }
    // Hold the next cycle before its start map so admin reads cannot race a
    // newer publication. The candidate and root evidence above are separate.
    wait_until(
        CYCLE_TIMEOUT,
        || async {
            let usage: DataUsageInfo = serde_json::from_value(run.admin_json(leader, "/rustfs/admin/v3/datausageinfo").await?)?;
            Ok(usage.scanner_epoch == Some(cycle.0) && usage.scanner_cycle == Some(cycle.1))
        },
        "admin read-back of the exact complete-pending root",
    )
    .await?;
    let usage: DataUsageInfo = serde_json::from_value(run.admin_json(leader, "/rustfs/admin/v3/datausageinfo").await?)?;
    if !usage.is_complete_bucket_usage_snapshot()
        || usage.usage_snapshot_converged != Some(false)
        || usage.usage_snapshot_observational
        || usage.usage_snapshot_partial
        || usage.usage_snapshot_bootstrap_pending
        || usage.usage_snapshot_pending_buckets != vec![hot.clone()]
        || usage.usage_snapshot_pending_bucket_count != 1
        || !run.usage_matches(&usage, &[&cold_a, &cold_b])
    {
        return Err(format!("admin root did not retain exact cold usage and only-hot pending: {usage:?}").into());
    }
    run.barrier.arm("root_durable", leader_pid, Some(next_cycle))?;
    run.barrier.release("before_start_snapshot", leader_pid)?;
    let next_start = run
        .barrier
        .wait_stage("start_snapshot", leader_pid, Some(next_cycle), false)
        .await?;
    let next_scope = if fault == Some(ScopedAckFault::RequestLoss) {
        all.clone()
    } else {
        BTreeSet::from([hot.clone()])
    };
    if mixed || fault.is_some() {
        let actual_peer = peer_for_address(&next_start, &producer_address)?;
        let actual = tuples(&actual_peer["buckets"])?;
        if mixed
            && (actual_peer["owner_id"] != start_peer["owner_id"]
                || actual_peer["instance_id"] != start_peer["instance_id"]
                || actual != complete_dirty_tuples(&mixed_after_ack)?)
        {
            return Err("the next Normal cycle did not observe the actual old receiver's retained map".into());
        }
        if actual.keys().cloned().collect::<BTreeSet<_>>() != next_scope
            || (fault == Some(ScopedAckFault::RequestLoss)
                && cold.iter().any(|bucket| actual.get(bucket) != start_tuples.get(bucket)))
        {
            return Err(
                format!("next actual peer map did not retain exactly the unapplied scoped responsibility: {actual:?}").into(),
            );
        }
    }
    let next_candidate = run
        .barrier
        .wait_stage("cycle_candidate", leader_pid, Some(next_cycle), false)
        .await?;
    assert_scope(&next_candidate, &next_scope, &start_tuples)?;
    let next_root = run
        .barrier
        .wait_stage("root_durable", leader_pid, Some(next_cycle), true)
        .await?;
    let mixed_next_root = if mixed {
        assert_root_binding(&next_candidate, &next_root)?;
        let physical = run.authoritative_root()?;
        assert_physical_root(&next_root, &physical)?;
        let usage: DataUsageInfo = serde_json::from_value(physical["usage"].clone())?;
        let buckets = run.buckets.iter().map(String::as_str).collect::<Vec<_>>();
        if next_candidate["detail"]["converged"] != true
            || !names(&next_candidate["detail"]["pending_buckets"])?.is_empty()
            || usage.usage_snapshot_converged != Some(true)
            || !usage.usage_snapshot_pending_buckets.is_empty()
            || !run.usage_matches(&usage, &buckets)
        {
            return Err("mixed-version hot-only recovery did not converge every acknowledged version".into());
        }
        #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
        run.assert_mixed_roles(&mixed_fleet).await?;
        physical
    } else {
        Value::Null
    };
    let observations = run.barrier.observations(false)?;
    let next_walks = observations
        .iter()
        .filter(|event| event["stage"].as_str() == Some("bucket_walk") && identity(event).ok() == Some(next_cycle))
        .collect::<Vec<_>>();
    let walked_buckets = next_walks
        .iter()
        .map(|event| {
            event["detail"]["bucket"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| "actual bucket walk omitted bucket".into())
        })
        .collect::<TestResult<BTreeSet<_>>>()?;
    let worker_observations = if observe_workers {
        let evidence = run.barrier.worker_observations(leader_pid, &[cycle, next_cycle])?;
        let initial = assert_worker_cycle(&evidence, &candidate, &root, &all, &mixed_fleet, false)?;
        let next = assert_worker_cycle(&evidence, &next_candidate, &next_root, &next_scope, &mixed_fleet, true)?;
        run.write_evidence("worker-execution", &json!({"raw": evidence, "initial": initial, "next": next}))?;
        evidence
    } else {
        if walked_buckets != next_scope {
            return Err(format!(
                "next Normal cycle must physically walk exactly the retained bucket scope {next_scope:?}: {next_walks:?}"
            )
            .into());
        }
        Value::Null
    };
    run.barrier.disarm()?;
    run.barrier.release("root_durable", leader_pid)?;
    let mut recovered_ack = Value::Null;
    let mut recovered_usage = Value::Null;
    if fault.is_some() {
        let next_send = run
            .barrier
            .wait_stage("ack_send", leader_pid, Some(next_cycle), false)
            .await?;
        if sequence(&next_root)? >= sequence(&next_send)?
            || next_send["detail"]["candidate_sha256"] != next_root["detail"]["candidate_sha256"]
            || next_send["detail"]["coverage_sha256"] != next_root["detail"]["coverage_sha256"]
        {
            return Err("recovery ACK did not follow the next verified durable root".into());
        }
        let retry = next_send["detail"]["acks"]
            .as_array()
            .ok_or("recovery cycle omitted ACKs")?
            .iter()
            .find(|ack| ack["owner_id"] == start_peer["owner_id"] && ack["instance_id"] == start_peer["instance_id"])
            .ok_or("recovery cycle did not acknowledge the same producer process")?;
        if retry["kind"] != "scoped"
            || tuples(&retry["entries"])? != tuples(&peer_for_address(&next_start, &producer_address)?["buckets"])?
        {
            return Err("recovery ACK did not preserve the next actual producer snapshot tuples".into());
        }
        wait_until(
            CYCLE_TIMEOUT,
            || async { Ok(receiver_observation(&run, "ack_after_apply", producer_pid, retry)?.is_some()) },
            "actual scoped ACK recovery after transport loss",
        )
        .await?;
        recovered_ack =
            receiver_observation(&run, "ack_after_apply", producer_pid, retry)?.ok_or("recovery receiver omitted apply")?;
        let remaining = &recovered_ack["detail"]["dirty_snapshot"];
        if recovered_ack["detail"]["cleared"].as_u64() != Some(u64::try_from(next_scope.len())?)
            || remaining["complete"] != true
            || remaining["pending_bucket_count"] != 0
            || remaining["buckets"].as_array().is_none_or(|entries| !entries.is_empty())
        {
            return Err(format!("healthy recovery did not clear precisely the retained responsibility: {recovered_ack}").into());
        }
        recovered_usage = serde_json::to_value(run.quiet_baseline().await?)?;
    }
    run.barrier.stop_session()?;
    run.verify_objects().await?;
    let mixed_final_processes = if let Some((old_binary, expected_old_sha256, _)) = old_followers {
        #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
        {
            run.assert_mixed_roles(&mixed_fleet).await?;
            let processes = run.mixed_process_identities(leader, old_binary, expected_old_sha256)?;
            if json!(processes) != mixed_fleet["processes"] {
                return Err("mixed-version processes or executable bytes changed during the gate".into());
            }
            json!(processes)
        }
        #[cfg(not(all(target_os = "linux", feature = "scanner-peer-mixed-version")))]
        {
            let _ = (old_binary, expected_old_sha256);
            return Err("mixed-version process evidence requires the dedicated Linux feature".into());
        }
    } else {
        Value::Null
    };
    let mixed_final_drives = if mixed {
        let drives = (0..4).map(|node| run.drive_identity(node)).collect::<TestResult<Vec<_>>>()?;
        if json!(drives) != mixed_fleet["drives_after"] {
            return Err("mixed-version measurement changed a retained drive identity".into());
        }
        json!(drives)
    } else {
        Value::Null
    };
    let mixed_final_physical = if mixed {
        let physical = converged_shards(&run)?;
        let initial = mixed_initial_physical
            .as_array()
            .ok_or("initial mixed-version shard oracle is absent")?;
        if initial.iter().any(|row| !physical.contains(row)) {
            return Err("mixed-version gate changed a previously acknowledged physical shard".into());
        }
        json!(physical)
    } else {
        Value::Null
    };
    let mixed_final_roles = if mixed {
        #[cfg(all(target_os = "linux", feature = "scanner-peer-mixed-version"))]
        {
            let roles = run.assert_mixed_roles(&mixed_fleet).await?;
            run.write_evidence("mixed-final-roles", &roles)?;
            roles
        }
        #[cfg(not(all(target_os = "linux", feature = "scanner-peer-mixed-version")))]
        {
            return Err("mixed-version process evidence requires the dedicated Linux feature".into());
        }
    } else {
        Value::Null
    };
    let mut evidence = json!({"topology": run.topology, "objects": run.objects, "baseline": baseline, "scheduler_config": scheduler_config,
            "leader_node": leader, "leader_pid": leader_pid, "producer_node": producer, "producer_pid": producer_pid,
            "producers": producers, "start_producers": start_producers, "secondary_acks": secondary_acks,
            "start_cycle_wake": start_cycle_wake, "next_cycle_wake": next_cycle_wake,
            "hot_in_post_snapshot_window": hot_in_post_snapshot_window[0], "hot_after_root": hot_after_root[0],
            "hot_in_post_snapshot_window_by_producer": hot_in_post_snapshot_window, "hot_after_root_by_producer": hot_after_root,
            "hot_stream": hot_stream, "hot_stream_completed_at_root": completed_at_root,
            "cycle": cycle, "next_cycle": next_cycle, "usage": usage,
            "candidate": candidate, "root": root, "ack_send": ack_send, "ack_before_apply": before_ack,
            "ack_after_apply": after_ack, "fault_receipt": fault_receipt, "next_scope": next_scope,
            "next_start": next_start, "next_candidate": next_candidate, "next_root": next_root,
            "recovered_ack": recovered_ack, "recovered_usage": recovered_usage,
            "observations": run.barrier.observations(false)?});
    evidence["mixed_fleet"] = mixed_fleet;
    evidence["mixed_initial_physical"] = mixed_initial_physical;
    evidence["mixed_physical_root"] = mixed_physical_root;
    evidence["mixed_before_ack"] = mixed_before_ack;
    evidence["mixed_retained_ack"] = mixed_retained_ack;
    evidence["mixed_after_ack"] = mixed_after_ack;
    evidence["mixed_next_root"] = mixed_next_root;
    evidence["mixed_final_processes"] = mixed_final_processes;
    evidence["mixed_final_physical"] = mixed_final_physical;
    evidence["mixed_final_drives"] = mixed_final_drives;
    evidence["mixed_final_roles"] = mixed_final_roles;
    if observe_workers {
        let final_workers = run.barrier.worker_observations(leader_pid, &[cycle, next_cycle])?;
        if final_workers != worker_observations {
            return Err("worker inventory changed or failed after root closure".into());
        }
        run.write_evidence("worker-final-health", &final_workers)?;
    }
    evidence["worker_observations"] = worker_observations;
    run.write_evidence(label, &evidence)
}
