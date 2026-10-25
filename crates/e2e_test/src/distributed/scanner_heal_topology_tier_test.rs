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

//! G-TIER: real mover processes contend with a held remote DELETE while a
//! free version owns the old remote tuple beside a live transitioned version.
//! The remote is the HTTP fake S3 target, not a provider interoperability test.

use super::harness::{
    DECOMMISSION_POOL_ID, DRIVES_PER_NODE, TestResult, cluster_admin_ok, decommission_status_json, enable_versioning,
    rebalance_status_json, sha256_hex, start_decommission, start_rebalance, unique_bucket, wait_for_decommission_complete,
    wait_for_rebalance_complete,
};
use super::scanner_heal_topology_fixture::{
    MOVEMENT_COMPLETE_TIMEOUT, MovementOperation, SEED_POOL, STAGE_BEFORE_SOURCE_CLEANUP, TopologyRun,
    assert_bucket_durability_strict, assert_process_durability_strict, require_test_hooks,
};
use crate::common::init_logging;
use crate::fake_s3_target::{FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3Target, Operation, RequestRecord};
use crate::storage_api::OBJECT_LOCK_TRACE_FILTER;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleRule, LifecycleRuleFilter, Transition, TransitionStorageClass,
};
use http::Method;
use rustfs_filemeta::{FileMeta, TransitionVersionState};
use rustfs_utils::http::{SUFFIX_TRANSITION_TIER_DESTINATION_ID, get_consistent_str};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::time::{Instant, sleep, timeout, timeout_at};

const TIER: &str = "TOPOLOGYCOLD";
const KEY: &str = "000-tier-owner.bin";
const TRANSITION_TIMEOUT: Duration = Duration::from_secs(90);
// Free-version cleanup holds its locks for at most 30 s during remote DELETE.
// Fail before that deadline instead of confusing a retry with the held attempt.
const DELETE_OVERLAP_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RemoteTuple {
    bucket: String,
    key: String,
    version: String,
    tier: String,
    destination: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Owner {
    pool: usize,
    drive: usize,
    version: String,
    free: bool,
    remote: RemoteTuple,
}

fn owners(run: &TopologyRun, bucket: &str, remote_bucket: &str) -> TestResult<Vec<Owner>> {
    let mut owners = Vec::new();
    for node in &run.dist.cluster.nodes {
        for (drive, root) in node.data_dirs.iter().enumerate() {
            let bytes = match std::fs::read(Path::new(root).join(bucket).join(KEY).join("xl.meta")) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            for version in FileMeta::load(&bytes)?.get_file_info_versions(bucket, KEY, true)?.versions {
                if version.transition_status != "complete" && !version.tier_free_version() {
                    continue;
                }
                assert_eq!(version.transition_version_state, TransitionVersionState::Exact);
                owners.push(Owner {
                    pool: node.pool_idx,
                    drive,
                    version: version.version_id.ok_or("tier owner omitted its local version")?.to_string(),
                    free: version.tier_free_version(),
                    remote: RemoteTuple {
                        bucket: remote_bucket.to_string(),
                        key: version.transitioned_objname.clone(),
                        version: version
                            .transition_version
                            .clone()
                            .ok_or("tier owner omitted its remote version")?,
                        tier: version.transition_tier.clone(),
                        destination: get_consistent_str(&version.metadata, SUFFIX_TRANSITION_TIER_DESTINATION_ID)
                            .ok_or("tier owner omitted its destination identity")?
                            .to_string(),
                    },
                });
            }
        }
    }
    Ok(owners)
}

fn assert_remote(target: &FakeS3Target, remote: &RemoteTuple, expected: &[u8]) -> TestResult {
    assert_eq!(target.stored_versions(&remote.bucket, &remote.key), vec![(remote.version.clone(), false)]);
    let (body, metadata) = target
        .stored_object(&remote.bucket, &remote.key)
        .ok_or("remote bytes are missing")?;
    assert_eq!(body.as_ref(), expected, "remote bytes changed for {remote:?}");
    assert_eq!(
        get_consistent_str(&metadata, SUFFIX_TRANSITION_TIER_DESTINATION_ID),
        Some(remote.destination.as_str())
    );
    assert_eq!(remote.tier, TIER);
    Ok(())
}

async fn transition_version(
    run: &TopologyRun,
    target: &FakeS3Target,
    bucket: &str,
    remote_bucket: &str,
    body: &[u8],
) -> TestResult<(String, RemoteTuple)> {
    let client = run.dist.client(0)?;
    let put = client
        .put_object()
        .bucket(bucket)
        .key(KEY)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await?;
    let version = put.version_id().ok_or("versioned PUT omitted its version")?.to_string();
    cluster_admin_ok(
        &run.dist.cluster,
        Method::POST,
        &format!("/rustfs/admin/v3/ilm/transition/run?bucket={bucket}&prefix={KEY}&tier={TIER}&dryRun=false"),
        None,
    )
    .await?;
    let deadline = Instant::now() + TRANSITION_TIMEOUT;
    loop {
        let current = owners(run, bucket, remote_bucket)?;
        let shards: Vec<_> = current
            .iter()
            .filter(|owner| owner.version == version && !owner.free)
            .collect();
        if let Some(first) = shards.first()
            && shards.len() == DRIVES_PER_NODE
            && shards
                .iter()
                .all(|owner| owner.pool == SEED_POOL && owner.remote == first.remote)
        {
            let head = client
                .head_object()
                .bucket(bucket)
                .key(KEY)
                .version_id(&version)
                .send()
                .await?;
            assert_eq!(head.storage_class().map(|class| class.as_str()), Some(TIER));
            assert_remote(target, &first.remote, body)?;
            return Ok((version, first.remote.clone()));
        }
        if Instant::now() >= deadline {
            return Err(format!("transition of {bucket}/{KEY}@{version} did not reach every seed drive: {current:?}").into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

fn lock_events(run: &TopologyRun, bucket: &str) -> TestResult<Vec<Value>> {
    let mut events = Vec::new();
    for path in run.node_log_paths() {
        let log = std::fs::read_to_string(path)?;
        for line in log.lines() {
            let Ok(row) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let fields = row.get("fields").unwrap_or(&row);
            if fields.get("event").and_then(Value::as_str) == Some("object_namespace_lock")
                && fields.get("op").and_then(Value::as_str) == Some("data_movement_publication")
                && fields.get("bucket").and_then(Value::as_str) == Some(bucket)
                && fields.get("object").and_then(Value::as_str) == Some(KEY)
            {
                let mut event = fields.clone();
                event["timestamp"] = row["timestamp"].clone();
                events.push(event);
            }
        }
    }
    Ok(events)
}

fn event_time(event: &Value) -> TestResult<OffsetDateTime> {
    Ok(OffsetDateTime::parse(
        event["timestamp"].as_str().ok_or("lock event omitted its timestamp")?,
        &Rfc3339,
    )?)
}

async fn pending_publication(run: &TopologyRun, bucket: &str, started: OffsetDateTime) -> TestResult<Value> {
    loop {
        let events = lock_events(run, bucket)?;
        for event in &events {
            if event_time(event)? < started {
                continue;
            }
            assert_ne!(
                event["state"], "acquired",
                "mover acquired its publication lock while remote DELETE was held"
            );
            if event["state"] == "acquiring"
                && !events
                    .iter()
                    .any(|later| later["lock_attempt_id"] == event["lock_attempt_id"] && later["state"] != "acquiring")
            {
                assert!(event["lock_attempt_id"].as_str().is_some_and(|id| !id.is_empty()));
                return Ok(event.clone());
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn acquired_publication(run: &TopologyRun, bucket: &str, pending: &Value) -> TestResult<Value> {
    loop {
        for event in lock_events(run, bucket)? {
            if event["lock_attempt_id"] != pending["lock_attempt_id"] {
                continue;
            }
            match event["state"].as_str() {
                Some("acquired") => return Ok(event),
                Some("failed" | "cancelled") => return Err(format!("held publication attempt did not acquire: {event}").into()),
                _ => {}
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
}

fn deletes(target: &FakeS3Target, remote: &RemoteTuple) -> Vec<RequestRecord> {
    target
        .requests()
        .into_iter()
        .filter(|request| {
            request.operation == Operation::DeleteObject
                && request.bucket == remote.bucket
                && request.key.as_deref() == Some(remote.key.as_str())
        })
        .collect()
}

async fn verify_live(run: &TopologyRun, bucket: &str, live_version: &str, old_version: &str, body: &[u8]) -> TestResult {
    for client in run.dist.clients()? {
        let got = client
            .get_object()
            .bucket(bucket)
            .key(KEY)
            .version_id(live_version)
            .send()
            .await?;
        assert_eq!(got.body.collect().await?.into_bytes().as_ref(), body);
        let latest = client.head_object().bucket(bucket).key(KEY).send().await?;
        assert_eq!(latest.version_id(), Some(live_version));
        assert_eq!(latest.storage_class().map(|class| class.as_str()), Some(TIER));
        let deleted = client
            .get_object()
            .bucket(bucket)
            .key(KEY)
            .version_id(old_version)
            .send()
            .await;
        let error = deleted.expect_err("deleted source version must not be visible");
        assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(404));
        let listed = client.list_object_versions().bucket(bucket).prefix(KEY).send().await?;
        assert_eq!(listed.versions().len(), 1, "free owners must not appear as object versions");
        assert_eq!(listed.versions()[0].version_id(), Some(live_version));
        assert!(
            listed.delete_markers().is_empty(),
            "moving the free owner must not create a delete marker"
        );
    }
    Ok(())
}

async fn tier_delete_during_movement(decommission: bool) -> TestResult {
    init_logging();
    require_test_hooks();
    let operation = if decommission { "decommission" } else { "rebalance" };
    let target = FakeS3Target::start().await?;
    let remote_bucket = unique_bucket("sht-cold");
    target.create_bucket(&remote_bucket);
    let mut run = TopologyRun::start_with_env(
        &format!("tier-{operation}"),
        &[
            ("RUSTFS_TIER_RUSTFS_ALLOW_LOOPBACK_ENDPOINT", "true"),
            ("RUSTFS_OBJECT_LOCK_DIAG_ENABLE", "true"),
            ("RUST_LOG", OBJECT_LOCK_TRACE_FILTER),
        ],
    )
    .await?;
    let tier = json!({
        "type": "rustfs",
        "rustfs": {
            "name": TIER, "endpoint": target.endpoint(), "accessKey": FAKE_ACCESS_KEY,
            "secretKey": FAKE_SECRET_KEY, "bucket": remote_bucket, "prefix": "",
            "region": "us-east-1", "storageClass": ""
        }
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match cluster_admin_ok(&run.dist.cluster, Method::PUT, "/rustfs/admin/v3/tier", Some(tier.to_string())).await {
            Ok(_) => break,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(250)).await,
            Err(error) => return Err(error),
        }
    }
    let bucket = unique_bucket("sht-tier");
    run.dist.create_bucket(&bucket).await?;
    let client = run.dist.client(0)?;
    enable_versioning(&client, &bucket).await?;
    let rule = LifecycleRule::builder()
        .id("tier-owner")
        .status(ExpirationStatus::Enabled)
        .filter(LifecycleRuleFilter::builder().prefix(KEY).build())
        .transitions(
            Transition::builder()
                .days(0)
                .storage_class(TransitionStorageClass::from(TIER))
                .build(),
        )
        .build()?;
    client
        .put_bucket_lifecycle_configuration()
        .bucket(&bucket)
        .lifecycle_configuration(BucketLifecycleConfiguration::builder().rules(rule).build()?)
        .send()
        .await?;
    let old_body = vec![0x31; 4096];
    let live_body = vec![0x92; 8192];
    let (old_version, old_remote) = transition_version(&run, &target, &bucket, &remote_bucket, &old_body).await?;
    let (live_version, live_remote) = transition_version(&run, &target, &bucket, &remote_bucket, &live_body).await?;
    assert_ne!(
        old_remote.key, live_remote.key,
        "the obsolete and live versions need distinct remote tuples"
    );
    // Keep the seed pool above the rebalance goal until the first key is visited.
    // These local objects are outside the transition rule's prefix.
    for index in 0..16 {
        client
            .put_object()
            .bucket(&bucket)
            .key(format!("zz-load/{index:02}.bin"))
            .body(ByteStream::from(vec![index as u8; 1024 * 1024]))
            .send()
            .await?;
    }
    let bucket_durability = assert_bucket_durability_strict(&run.dist.cluster, &[&bucket]).await?;
    run.dist.expand_to_four_pools().await?;
    assert_bucket_durability_strict(&run.dist.cluster, &[&bucket]).await?;
    let seeded = owners(&run, &bucket, &remote_bucket)?;
    assert_eq!(seeded.len(), 2 * DRIVES_PER_NODE);
    assert!(seeded.iter().all(|owner| owner.pool == SEED_POOL && !owner.free));

    run.movement_barrier
        .arm_movement(operation, &bucket, KEY, STAGE_BEFORE_SOURCE_CLEANUP)?;
    let mut delete_gate = target.hold_delete_object(&remote_bucket, &old_remote.key);
    // The public DELETE persists a free owner and enqueues its remote cleanup.
    let client = run.dist.client(0)?;
    client
        .delete_object()
        .bucket(&bucket)
        .key(KEY)
        .version_id(&old_version)
        .send()
        .await?;
    timeout(TRANSITION_TIMEOUT, delete_gate.wait_until_entered()).await??;
    let overlap_deadline = Instant::now() + DELETE_OVERLAP_TIMEOUT;
    let held = owners(&run, &bucket, &remote_bucket)?;
    assert_eq!(held.len(), 2 * DRIVES_PER_NODE);
    let old_owners: Vec<_> = held.iter().filter(|owner| owner.remote == old_remote).collect();
    assert_eq!(old_owners.len(), DRIVES_PER_NODE);
    assert!(
        old_owners
            .iter()
            .all(|owner| owner.pool == SEED_POOL && owner.free && owner.version != old_version)
    );
    assert_eq!(old_owners.iter().map(|owner| &owner.version).collect::<BTreeSet<_>>().len(), 1);
    assert_eq!(
        held.iter()
            .filter(|owner| {
                owner.pool == SEED_POOL && !owner.free && owner.version == live_version && owner.remote == live_remote
            })
            .count(),
        DRIVES_PER_NODE
    );
    assert_remote(&target, &old_remote, &old_body)?;
    assert_remote(&target, &live_remote, &live_body)?;
    let held_deletes = deletes(&target, &old_remote);
    assert!(!held_deletes.is_empty());
    assert!(held_deletes.iter().all(|request| {
        request.prepared_response.is_none() && request.version_id.as_deref() == Some(old_remote.version.as_str())
    }));
    assert!(deletes(&target, &live_remote).is_empty());

    let movement_started = OffsetDateTime::now_utc();
    let rebalance_id = timeout_at(overlap_deadline, async {
        if decommission {
            start_decommission(&run.dist.cluster, DECOMMISSION_POOL_ID).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(None)
        } else {
            Ok(Some(start_rebalance(&run.dist.cluster).await?))
        }
    })
    .await??;
    let pending = timeout_at(overlap_deadline, pending_publication(&run, &bucket, movement_started)).await??;
    // Positive gate entry, exact physical ownership and the pending lock attempt
    // establish the overlap; an absent log alone is not an ordering assertion.
    assert_eq!(owners(&run, &bucket, &remote_bucket)?, held);
    assert_remote(&target, &old_remote, &old_body)?;
    let released_at = OffsetDateTime::now_utc();
    drop(delete_gate);
    let acquired = timeout(DELETE_OVERLAP_TIMEOUT, acquired_publication(&run, &bucket, &pending)).await??;
    assert!(event_time(&pending)? >= movement_started && event_time(&pending)? < released_at);
    assert!(
        event_time(&acquired)? >= released_at,
        "the same publication attempt must acquire after DELETE is released"
    );
    let reached = run
        .movement_barrier
        .wait_reached_during(
            &run.dist.cluster,
            match rebalance_id.as_deref() {
                Some(id) => MovementOperation::Rebalance(id),
                None => MovementOperation::Decommission(DECOMMISSION_POOL_ID),
            },
            MOVEMENT_COMPLETE_TIMEOUT,
        )
        .await?;
    let published = owners(&run, &bucket, &remote_bucket)?;
    assert!(
        published
            .iter()
            .all(|owner| !owner.free && owner.version == live_version && owner.remote == live_remote)
    );
    let publication_pools: BTreeSet<_> = published.iter().map(|owner| owner.pool).collect();
    assert!(publication_pools.contains(&SEED_POOL) && publication_pools.iter().any(|pool| *pool != SEED_POOL));
    assert_eq!(published.len(), publication_pools.len() * DRIVES_PER_NODE);
    assert!(target.stored_versions(&remote_bucket, &old_remote.key).is_empty());
    assert_remote(&target, &live_remote, &live_body)?;
    run.movement_barrier.release_movement()?;
    run.movement_barrier.disarm()?;
    let completion = match rebalance_id.as_deref() {
        Some(id) => {
            wait_for_rebalance_complete(&run.dist.cluster, id, MOVEMENT_COMPLETE_TIMEOUT).await?;
            rebalance_status_json(&run.dist.cluster).await?
        }
        None => {
            wait_for_decommission_complete(&run.dist.cluster, DECOMMISSION_POOL_ID, MOVEMENT_COMPLETE_TIMEOUT).await?;
            decommission_status_json(&run.dist.cluster).await?
        }
    };
    let settled = owners(&run, &bucket, &remote_bucket)?;
    assert_eq!(settled.len(), DRIVES_PER_NODE);
    assert_eq!(settled.iter().map(|owner| owner.pool).collect::<BTreeSet<_>>().len(), 1);
    assert!(
        settled
            .iter()
            .all(|owner| owner.pool != SEED_POOL && !owner.free && owner.remote == live_remote)
    );
    verify_live(&run, &bucket, &live_version, &old_version, &live_body).await?;
    if decommission {
        run.dist.remove_retired_pools_and_restart(&[SEED_POOL]).await?;
    } else {
        run.dist.restart_current_binary_gracefully().await?;
    }
    verify_live(&run, &bucket, &live_version, &old_version, &live_body).await?;
    let restarted = owners(&run, &bucket, &remote_bucket)?;
    assert_eq!(restarted.len(), DRIVES_PER_NODE);
    assert_eq!(restarted.iter().map(|owner| owner.pool).collect::<BTreeSet<_>>().len(), 1);
    assert!(
        restarted
            .iter()
            .all(|owner| !owner.free && owner.version == live_version && owner.remote == live_remote)
    );
    assert_remote(&target, &live_remote, &live_body)?;
    assert!(target.stored_versions(&remote_bucket, &old_remote.key).is_empty());
    assert!(
        deletes(&target, &live_remote).is_empty(),
        "the live transitioned tuple must never receive DELETE"
    );
    let remote_deletes = deletes(&target, &old_remote);
    assert!(!remote_deletes.is_empty());
    assert!(
        remote_deletes
            .iter()
            .all(|request| request.version_id.as_deref() == Some(old_remote.version.as_str()))
    );
    let successful = remote_deletes
        .iter()
        .filter_map(|request| request.prepared_response.as_ref())
        .find(|response| response.status == 204)
        .ok_or("old remote tuple never completed DELETE")?;
    let deleted_at_ms = i128::from(
        successful
            .prepared_at_unix_millis
            .ok_or("DELETE response omitted its timestamp")?,
    );
    assert!(deleted_at_ms >= released_at.unix_timestamp_nanos() / 1_000_000);
    assert!(event_time(&acquired)?.unix_timestamp_nanos() / 1_000_000 >= deleted_at_ms);
    assert_bucket_durability_strict(&run.dist.cluster, &[&bucket]).await?;
    let strict_nodes = assert_process_durability_strict(&run.node_log_paths())?;
    run.write_evidence(
        &format!("g-tier-{operation}"),
        &json!({
            "gate": "G-TIER", "operation": operation, "rebalance_id": rebalance_id,
            "remote_backend": "HTTP fake_s3_target with versioning enabled",
            "ordering": "remote DELETE held, mover publication waiting, DELETE released, same publication acquired",
            "bucket": bucket, "key": KEY, "bucket_durability": bucket_durability,
            "process_durability_strict_nodes": strict_nodes,
            "old_version": old_version, "old_remote": old_remote, "old_sha256": sha256_hex(&old_body),
            "live_version": live_version, "live_remote": live_remote, "live_sha256": sha256_hex(&live_body),
            "owners_while_delete_held": held, "publication_waiting": pending, "publication_acquired": acquired,
            "held_delete_sequences": held_deletes.iter().map(|request| request.sequence).collect::<Vec<_>>(),
            "delete_released_at": released_at.format(&Rfc3339)?, "remote_delete_completed_at_unix_millis": deleted_at_ms,
            "barrier": reached, "owners_before_source_cleanup": published, "owners_after_movement": settled,
            "owners_after_restart": restarted, "completion": completion,
            "old_remote_delete_attempts": remote_deletes.len(), "live_remote_delete_attempts": 0,
            "source_readback_nodes": run.dist.cluster.nodes.len(),
        }),
    )?;
    Ok(())
}

#[tokio::test]
async fn scanner_heal_topology_tier_free_owner_delete_overlaps_rebalance() -> TestResult {
    tier_delete_during_movement(false).await
}

#[tokio::test]
async fn scanner_heal_topology_tier_free_owner_delete_overlaps_decommission() -> TestResult {
    tier_delete_during_movement(true).await
}
