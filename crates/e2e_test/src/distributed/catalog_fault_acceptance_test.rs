// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Catalog acceptance with four real startup processes and retained evidence.

mod catalog_m4_acceptance_test;

use super::catalog_fault_worker::{
    CatalogCluster, LateCommitProof, LateCommitRole, Session, TestResult, assert_late_commit_proof, assert_no_head_regression,
    assert_retained_record, field, sha256, verify_manifest,
};
use serde_json::{Value, json};
use std::io;
use std::time::{Duration, Instant};

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message.into()).into())
    }
}

fn commit_action(prepared: &Value) -> Value {
    json!({"op":"commit","prepared_token":prepared["prepared_token"]})
}

async fn publish(cluster: &CatalogCluster, session: &Session, node: usize) -> TestResult<Value> {
    let prepared = cluster.prepare(session, node).await?;
    verify_manifest(&prepared["candidate_readback"])?;
    cluster.clients[node].call(commit_action(&prepared)).await?;
    let current = cluster.agree(session).await?;
    assert_candidate(&current, &prepared)?;
    Ok(current)
}

fn assert_candidate(read: &Value, prepared: &Value) -> TestResult {
    require(
        read["manifest"]["key"] == prepared["candidate"]["object_key"],
        "published manifest key differs from exact candidate",
    )?;
    for key in ["length", "sha256", "body_revision", "etag"] {
        require(
            read["manifest"][key] == prepared["candidate_readback"][key],
            format!("published candidate {key} differs"),
        )?;
    }
    require(
        read["manifest"]["bytes_base64"] == prepared["candidate_readback"]["bytes_base64"],
        "published manifest bytes differ",
    )
}

#[tokio::test]
async fn catalog_commit_reads_on_every_node_and_after_full_restart() -> TestResult {
    let mut cluster = CatalogCluster::start("normal").await?;
    let result = async {
        let mut session = cluster.create().await?;
        let initial = cluster.agree(&session).await?;
        let prepared = cluster.prepare(&session, 0).await?;
        verify_manifest(&prepared["candidate_readback"])?;
        require(cluster.agree(&session).await? == initial, "unpublished candidate became authoritative")?;
        cluster.clients[0].call(commit_action(&prepared)).await?;
        let committed = cluster.agree(&session).await?;
        assert_candidate(&committed, &prepared)?;
        cluster.capture("acknowledged-physical-state").await?;
        cluster.restart_all(&mut session).await?;
        require(
            cluster.agree(&session).await? == committed,
            "full restart changed acknowledged Catalog/manifest",
        )
    }
    .await;
    cluster.finish(result).await
}

async fn arm(cluster: &CatalogCluster, prepared: &Value, target: &str, phase: &str) -> TestResult<Value> {
    let baseline = cluster.clients[0]
        .call(json!({"op":"events","session_id":prepared["replay"]["session_id"]}))
        .await?;
    let after_sequence = baseline["events"]
        .as_array()
        .ok_or("missing baseline events")?
        .iter()
        .filter_map(|v| v["sequence"].as_u64())
        .max()
        .unwrap_or(0);
    let mut fault=cluster.clients[0].call(json!({"op":"fault_arm","prepared_token":prepared["prepared_token"],"target":target,"phase":phase,"effect":"pause","pause_timeout_ms":115000})).await?;
    fault["after_sequence"] = json!(after_sequence);
    Ok(fault)
}

async fn events(cluster: &CatalogCluster, session: &Session) -> TestResult<Value> {
    cluster.clients[0]
        .call(json!({"op":"events","session_id":session.ids[0]}))
        .await
}

fn event_count(value: &Value, fault: &Value, phase: &str) -> usize {
    value["events"].as_array().map_or(0, |entries| {
        entries
            .iter()
            .filter(|entry| {
                entry["sequence"].as_u64() > fault["after_sequence"].as_u64()
                    && entry["bucket"] == fault["bucket"]
                    && entry["object"] == fault["key"]
                    && entry["phase"] == phase
            })
            .count()
    })
}

async fn wait_arrived(cluster: &CatalogCluster, session: &Session, fault: &Value) -> TestResult<Value> {
    wait_arrived_for_request(cluster, session, fault, None).await
}

async fn wait_arrived_for_request(
    cluster: &CatalogCluster,
    session: &Session,
    fault: &Value,
    request_id: Option<&str>,
) -> TestResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let value = events(cluster, session).await?;
        if let Some(request_id) = request_id
            && let Err(error) = discarded_request_receipt(&value, request_id)
        {
            cluster.evidence.record(
                "response-loss-operation-rejected",
                &json!({"request_id":request_id,"fault":fault,"worker":value,"error":error.to_string()}),
            )?;
            return Err(error);
        }
        let matching = value["observations"]["rules"]
            .as_array()
            .ok_or("missing fault rule evidence")?
            .iter()
            .filter(|rule| {
                rule["bucket"] == fault["bucket"] && rule["key"] == fault["key"] && rule["attempt"] == fault["attempt"]
            })
            .collect::<Vec<_>>();
        require(matching.len() == 1, "exact phase rule is missing or ambiguous")?;
        if matching[0]["hits"] == 1 && event_count(&value, fault, "arrived") == 1 {
            if let Some(request_id) = request_id {
                require_discarded_request_arrived(&value, request_id)?;
            }
            let completed = event_count(&value, fault, "inner-completed");
            let attempt = usize::try_from(fault["attempt"].as_u64().ok_or("missing attempt")?)?;
            let expected = if fault["phase"] == "after" { attempt } else { attempt - 1 };
            require(completed == expected, "barrier did not reach the requested Before/After boundary")?;
            cluster
                .evidence
                .record("fault-arrived", &json!({"fault":fault,"evidence":value}))?;
            return Ok(value);
        }
        require(Instant::now() < deadline, "precise fault did not reach its boundary")?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn release(cluster: &CatalogCluster, fault: &Value) -> TestResult {
    let result = cluster.clients[0]
        .call(json!({"op":"fault_release","fault_id":fault["fault_id"]}))
        .await?;
    require(result["released"] == true, "fault release was not acknowledged")
}

async fn raw(cluster: &CatalogCluster, session: &Session, node: usize, bucket: &Value, key: &Value) -> TestResult<Value> {
    cluster.clients[node]
        .call(json!({"op":"raw_read","session_id":session.ids[node],"bucket":bucket,"key":key}))
        .await
}

async fn verify_after_boundary(cluster: &CatalogCluster, session: &Session, fault: &Value, observation: &Value) -> TestResult {
    if fault["phase"] == "after" {
        let value = raw(cluster, session, 0, &fault["bucket"], &fault["key"]).await?;
        verify_manifest(&value["object"])?;
        let completed = observation["events"]
            .as_array()
            .ok_or("missing events")?
            .iter()
            .rev()
            .find(|event| {
                event["bucket"] == fault["bucket"] && event["object"] == fault["key"] && event["phase"] == "inner-completed"
            })
            .ok_or("missing completed backend write")?;
        require(
            value["object"]["sha256"] == completed["sha256"],
            "After boundary bytes differ from completed backend request",
        )?;
    }
    Ok(())
}

fn recovery_state(report: &Value, commit_id: &Value) -> TestResult<Option<String>> {
    let matches = report["commits"]
        .as_array()
        .ok_or("missing recovery entries")?
        .iter()
        .filter(|entry| &entry["commit_id"] == commit_id)
        .collect::<Vec<_>>();
    require(matches.len() <= 1, "duplicate recovery entry for commit")?;
    matches
        .first()
        .map(|entry| field(entry, "recovery_state").map(str::to_owned))
        .transpose()
}

fn restart_replay_operation_outcome(op: &str, response: &Value, elapsed: Duration, budget: Duration) -> TestResult<bool> {
    require(elapsed < budget, "restart replay recovery deadline exhausted")?;
    require(matches!(op, "recover" | "commit"), "unknown restart replay operation")?;
    let success_payload = match op {
        "commit" => response["result"]["table"].is_object() && response["result"]["commit_log"].is_object(),
        "recover" => {
            response["result"]["commits"].is_array() && response["result"]["table_id"].as_str().is_some_and(|id| !id.is_empty())
        }
        _ => false,
    };
    match response["status"].as_str() {
        Some("ok") if response["error"].is_null() && success_payload => Ok(true),
        Some("error") if response["result"].is_null() && response["error"]["class"] == "LockUnavailable" => Ok(false),
        _ => Err(io::Error::other(format!("restart replay returned a terminal or unknown response: {response}")).into()),
    }
}

async fn replay_after_lease_release(
    cluster: &CatalogCluster,
    session: &Session,
    prepared: &Value,
    published: bool,
) -> TestResult {
    // Two existing 30-second lease periods bound recover and commit together.
    // Production TTL, acquire timeout, resource identity and prepared token remain unchanged.
    let budget = Duration::from_secs(60);
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + budget;
    let actions = if published {
        vec![json!({"op":"recover","session_id":session.ids[0]}), commit_action(prepared)]
    } else {
        vec![commit_action(prepared)]
    };
    for action in actions {
        let op = field(&action, "op")?;
        let mut attempt = 0_u64;
        loop {
            require(started.elapsed() < budget, "restart replay recovery deadline exhausted before dispatch")?;
            attempt += 1;
            let response = tokio::time::timeout_at(deadline, cluster.clients[0].response(action.clone()))
                .await
                .map_err(|_| io::Error::other(format!("restart replay recovery deadline exhausted during {op}")))??;
            cluster.evidence.record("restart-replay-attempt", &json!({"operation":op,"attempt":attempt,"elapsed_ms":started.elapsed().as_millis(),
                "recovery_budget_ms":budget.as_millis(),"identity":session.identity,"action":action,"prepared":prepared,"response":response}))?;
            if restart_replay_operation_outcome(op, &response, started.elapsed(), budget)? {
                if op == "recover" {
                    let report = &response["result"];
                    require(
                        report["table_id"] == session.identity["table_id"]
                            && report["table_bucket"] == session.identity["bucket"]["storage_bucket_id"]
                            && report["namespace"] == "indexes"
                            && report["table"] == session.identity["index_name"],
                        "recovery response belongs to another resource",
                    )?;
                    cluster.evidence.record("recovery-applied", report)?;
                }
                break;
            }
            tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(250)))
                .await
                .map_err(|_| io::Error::other("restart replay recovery deadline exhausted before retry"))?;
        }
    }
    Ok(())
}

async fn replay_after_restart(
    cluster: &CatalogCluster,
    session: &Session,
    prepared: &Value,
    expected_state: Option<&str>,
    published: bool,
) -> TestResult {
    let report = cluster.clients[0]
        .call(json!({"op":"recovery","session_id":session.ids[0]}))
        .await?;
    require(
        recovery_state(&report, &prepared["commit_id"])?.as_deref() == expected_state,
        format!("unexpected recovery state: {report}"),
    )?;
    let before = cluster.agree(session).await?;
    if published {
        assert_candidate(&before, prepared)?;
    } else {
        require(before["head"] == session.initial, "unpublished operation changed head")?;
    }
    let mut replay = prepared["replay"].clone();
    replay["session_id"] = json!(session.ids[0]);
    let retry = cluster.clients[0].call(replay).await?;
    for key in [
        "candidate",
        "candidate_readback",
        "expected_head",
        "descriptor",
        "commit_id",
        "request_digest",
    ] {
        require(retry[key] == prepared[key], format!("restart replay changed {key}"))?;
    }
    replay_after_lease_release(cluster, session, &retry, published).await?;
    let current = cluster.agree(session).await?;
    assert_candidate(&current, prepared)?;
    let report = cluster.clients[0]
        .call(json!({"op":"recovery","session_id":session.ids[0]}))
        .await?;
    require(
        recovery_state(&report, &prepared["commit_id"])?.as_deref() == Some("COMMITTED"),
        "replayed commit did not reach Committed",
    )
}

#[tokio::test]
async fn catalog_completed_candidate_survives_process_restart() -> TestResult {
    let mut cluster = CatalogCluster::start("candidate-restart").await?;
    let result = async {
        let mut session = cluster.create().await?;
        let prepared = cluster.prepare(&session, 0).await?;
        verify_manifest(&prepared["candidate_readback"])?;
        require(
            cluster.agree(&session).await?["head"] == session.initial,
            "candidate already visible before publication",
        )?;
        cluster.capture("candidate-uploaded").await?;
        cluster.restart(0).await?;
        cluster.reopen(&mut session, 0).await?;
        replay_after_restart(&cluster, &session, &prepared, None, false).await
    }
    .await;
    cluster.finish(result).await
}

fn expected_crash_state(target: &str, phase: &str) -> (Option<&'static str>, bool) {
    match (target, phase) {
        ("staged", "before") => (None, false),
        ("staged", _) | ("idempotency", _) | ("head", "before") => (Some("STAGED_BEFORE_TABLE_UPDATE"), false),
        ("head", "after") | ("finalization", "before") => (Some("FINALIZATION_REQUIRED"), true),
        ("finalization", "after") | ("idempotency_finalization", "before") => (Some("IDEMPOTENCY_INDEX_REPAIR_REQUIRED"), true),
        ("idempotency_finalization", "after") => (Some("COMMITTED"), true),
        _ => unreachable!("fixed phase matrix"),
    }
}

#[tokio::test]
async fn catalog_commit_phase_process_exit_matrix() -> TestResult {
    let mut cluster = CatalogCluster::start("phase-restart").await?;
    let result = async {
        for target in ["staged", "idempotency", "head", "finalization", "idempotency_finalization"] {
            for phase in ["before", "after"] {
                let mut session = cluster.create().await?;
                let prepared = cluster.prepare(&session, 0).await?;
                let fault = arm(&cluster, &prepared, target, phase).await?;
                let client = cluster.clients[0].clone();
                let action = commit_action(&prepared);
                let commit = tokio::spawn(async move { client.response(action).await });
                let observation = wait_arrived(&cluster, &session, &fault).await?;
                verify_after_boundary(&cluster, &session, &fault, &observation).await?;
                cluster.capture("before-process-exit").await?;
                cluster.stop_node(0)?;
                let outcome = tokio::time::timeout(Duration::from_secs(5), commit).await??;
                require(outcome.is_err(), "paused commit returned a complete response after process termination")?;
                cluster.start_stopped_node(0).await?;
                cluster.reopen(&mut session, 0).await?;
                let (state, published) = expected_crash_state(target, phase);
                replay_after_restart(&cluster, &session, &prepared, state, published).await?;
                cluster.capture("phase-restart-completed").await?;
            }
        }
        Ok(())
    }
    .await;
    cluster.finish(result).await
}

fn discarded_request_receipt<'a>(observation: &'a Value, request_id: &str) -> TestResult<Option<&'a Value>> {
    let matching = observation["operations"]
        .as_array()
        .ok_or("missing server operation receipt")?
        .iter()
        .filter(|operation| operation["request_id"] == request_id)
        .collect::<Vec<_>>();
    require(matching.len() <= 1, "the exact response-loss request has duplicate server receipts")?;
    let Some(operation) = matching.first() else {
        return Ok(None);
    };
    require(
        operation["action"] == "commit" && operation["status"] == "running" && operation["completed_micros"].is_null(),
        format!("response-loss request failed or completed before its selected pause: {operation}"),
    )?;
    Ok(Some(*operation))
}

fn require_discarded_request_arrived(observation: &Value, request_id: &str) -> TestResult {
    require(
        discarded_request_receipt(observation, request_id)?.is_some(),
        "the exact response-loss request has no server receipt at the selected pause",
    )
}

async fn wait_operation(cluster: &CatalogCluster, session: &Session, request_id: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(110);
    loop {
        let value = events(cluster, session).await?;
        let operation = value["operations"]
            .as_array()
            .ok_or("missing worker operations")?
            .iter()
            .find(|item| item["request_id"] == request_id);
        if let Some(operation) = operation.filter(|item| !item["completed_micros"].is_null()) {
            require(operation["status"] == "ok", format!("discarded-response operation failed: {operation}"))?;
            cluster.evidence.record("discarded-operation-completed", operation)?;
            return Ok(());
        }
        require(Instant::now() < deadline, "discarded-response operation did not complete")?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn catalog_commit_response_loss_and_restart_matrix() -> TestResult {
    let mut cluster = CatalogCluster::start("response-loss").await?;
    let result = async {
        for target in ["staged", "idempotency", "head", "finalization", "idempotency_finalization"] {
            for phase in ["before", "after"] {
                let mut session = cluster.create().await?;
                let prepared = cluster.prepare(&session, 0).await?;
                let fault = arm(&cluster, &prepared, target, phase).await?;
                let pending = cluster.clients[0].send_pending_response(commit_action(&prepared)).await?;
                let request_id = pending.request_id.clone();
                let observation = wait_arrived_for_request(&cluster, &session, &fault, Some(&request_id)).await?;
                require_discarded_request_arrived(&observation, &request_id)?;
                cluster.evidence.record(
                    "response-discard-boundary",
                    &json!({"request_id":request_id,"fault":fault,"observation":observation}),
                )?;
                verify_after_boundary(&cluster, &session, &fault, &observation).await?;
                pending.discard().await?;
                release(&cluster, &fault).await?;
                wait_operation(&cluster, &session, &request_id).await?;
                cluster.capture("response-lost-before-restart").await?;
                cluster.restart(0).await?;
                cluster.reopen(&mut session, 0).await?;
                replay_after_restart(&cluster, &session, &prepared, Some("COMMITTED"), true).await?;
            }
        }
        Ok(())
    }
    .await;
    cluster.finish(result).await
}

async fn directory_case(kind: &str) -> TestResult {
    let mut cluster = CatalogCluster::start(kind).await?;
    let result = async {
        let mut session = cluster.create().await?;
        let first = publish(&cluster, &session, 0).await?;
        cluster.stop_node(3)?;
        let snapshot = if kind == "stale-directory" {
            Some(cluster.snapshot_stopped_disk(3).await?)
        } else {
            None
        };
        let expected = if kind == "empty-directory" {
            first
        } else {
            let prepared = cluster.prepare(&session, 1).await?;
            cluster.clients[1].call(commit_action(&prepared)).await?;
            let current = cluster.read(&session, 1).await?;
            assert_candidate(&current, &prepared)?;
            require(current != first, "later commit did not change the authoritative head")?;
            current
        };
        cluster.replace_stopped_disk(3, snapshot).await?;
        cluster.start_stopped_node(3).await?;
        cluster.reopen(&mut session, 3).await?;
        require(
            cluster.agree(&session).await? == expected,
            "directory recovery changed committed Catalog/manifest",
        )?;
        cluster.wait_for_repaired_objects(&expected).await?;
        cluster.capture("directory-recovery-state").await?;
        cluster.restart_all(&mut session).await?;
        require(cluster.agree(&session).await? == expected, "whole cluster restart lost recovered data")
    }
    .await;
    cluster.finish(result).await
}

#[tokio::test]
async fn catalog_empty_directory_recovers_from_quorum() -> TestResult {
    directory_case("empty-directory").await
}
#[tokio::test]
async fn catalog_stale_directory_cannot_replace_later_head() -> TestResult {
    directory_case("stale-directory").await
}
#[tokio::test]
async fn catalog_replacement_directory_preserves_acknowledged_data() -> TestResult {
    directory_case("replacement-directory").await
}

#[tokio::test]
async fn catalog_corrupt_records_are_errors_and_preserve_preimages() -> TestResult {
    let cluster = CatalogCluster::start("corrupt-catalog").await?;
    let result=async {
        for (target,identity_mismatch) in [("head",false),("head",true),("commit_log",false),("idempotency",false)] {
            let session=cluster.create().await?;
            let prepared=cluster.prepare(&session,0).await?;
            cluster.clients[0].call(commit_action(&prepared)).await?;
            let committed=cluster.agree(&session).await?;
            let paths=&prepared["paths"];
            let before=raw(&cluster,&session,0,&paths["bucket"],&paths[target]).await?["object"].clone();
            verify_manifest(&before)?;
            let bytes=if identity_mismatch {
                let mut entry:Value=serde_json::from_slice(&verify_manifest(&before)?)?;
                entry["table_id"]=json!(uuid::Uuid::new_v4().to_string());
                serde_json::to_vec(&entry)?
            } else { b"{invalid-catalog-json".to_vec() };
            let corrupted=cluster.clients[0].call(json!({"op":"raw_write","session_id":session.ids[0],"bucket":paths["bucket"],"key":paths[target],"expected_etag":before["etag"],"bytes_base64":base64_simd::STANDARD.encode_to_string(&bytes)})).await?;
            require(corrupted["before"]==before,"conditional corruption preimage changed")?;
            verify_manifest(&corrupted["after"])?;
            let op=if target=="head" {"head"} else {"recovery"};
            for node in 0..4 {
                let response=cluster.clients[node].response(json!({"op":op,"session_id":session.ids[node]})).await?;
                require(response["status"]=="error",format!("node {node} accepted corrupt {target}: {response}"))?;
                require(matches!(response["error"]["class"].as_str(),Some("Invalid"|"Conflict")),format!("unexpected corruption error: {response}"))?;
            }
            let retained=raw(&cluster,&session,0,&paths["bucket"],&paths[target]).await?;
            require(retained["object"]==corrupted["after"],"corrupt record was reinitialized or changed")?;
            cluster.capture("corrupt-record-physical-state").await?;
            let restored=cluster.clients[0].call(json!({"op":"raw_write","session_id":session.ids[0],"bucket":paths["bucket"],"key":paths[target],"expected_etag":corrupted["after"]["etag"],"bytes_base64":before["bytes_base64"]})).await?;
            require(restored["after"]["bytes_base64"]==before["bytes_base64"],"restored bytes differ from retained preimage")?;
            let current=cluster.agree(&session).await?;
            require(current["head"]["entry"]==committed["head"]["entry"] && current["manifest"]==committed["manifest"],"restoration changed logical Catalog/manifest")?;
        }
        Ok(())
    }.await;
    cluster.finish(result).await
}

use super::catalog_fault_worker::Evidence;
use crate::rpc_fault_proxy::{CatalogRenameScope, CatalogRpcGate};
use rustfs_lock::{LockId, ObjectKey};
use std::sync::Arc;

struct FaultGates {
    locks: Vec<CatalogRpcGate>,
    refresh: Vec<CatalogRpcGate>,
    rename: Vec<CatalogRpcGate>,
    evidence: Arc<Evidence>,
    refresh_targets: Vec<RefreshLossTarget>,
}

impl FaultGates {
    fn observe(cluster: &CatalogCluster, prepared: &Value) -> TestResult<Self> {
        let resources: Vec<ObjectKey> = serde_json::from_value(prepared["paths"]["lock_resources"].clone())?;
        require(!resources.is_empty(), "publication lock resources are absent")?;
        let locks = cluster
            .proxies
            .iter()
            .map(|proxy| proxy.observe_catalog_locks(resources.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let rename = cluster
            .proxies
            .iter()
            .skip(1)
            .map(|proxy| {
                proxy.pause_catalog_rename(
                    CatalogRenameScope {
                        dst_volume: prepared["paths"]["bucket"].as_str().unwrap_or_default().to_owned(),
                        dst_path: prepared["paths"]["head"].as_str().unwrap_or_default().to_owned(),
                        src_path: None,
                    },
                    1,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            locks,
            refresh: Vec::new(),
            refresh_targets: Vec::new(),
            rename,
            evidence: cluster.evidence.clone(),
        })
    }

    fn record(&self, label: &str) -> TestResult {
        self.evidence.record(label,&json!({"locks":self.locks.iter().map(CatalogRpcGate::snapshot).collect::<Vec<_>>(),"refresh":self.refresh.iter().map(CatalogRpcGate::snapshot).collect::<Vec<_>>(),"rename":self.rename.iter().map(CatalogRpcGate::snapshot).collect::<Vec<_>>()}))
    }

    fn block_refresh(&mut self, cluster: &CatalogCluster, prepared: &Value, barrier: &Value) -> TestResult {
        let resources: Vec<ObjectKey> = serde_json::from_value(prepared["paths"]["lock_resources"].clone())?;
        let locks = self
            .locks
            .iter()
            .map(|gate| serde_json::to_value(gate.snapshot()))
            .collect::<Result<Vec<_>, _>>()?;
        self.refresh_targets = capture_refresh_targets(&resources, barrier, &locks)?;
        self.evidence.record(
            "refresh-loss-targets",
            &json!({"targets":self.refresh_targets,"peer_count":cluster.proxies.len(),"write_quorum":cluster.proxies.len()/2+1}),
        )?;
        for node in 1..4 {
            let evidence = self.locks[node].snapshot();
            require(evidence.overflow == 0, "lock observation overflow")?;
            let mut ids: Vec<LockId> = Vec::new();
            for packet in &evidence.packets {
                for lock in &packet.locks {
                    if resources.contains(&lock.resource) && !ids.contains(&lock.lock_id) {
                        ids.push(lock.lock_id.clone());
                    }
                }
            }
            require(!ids.is_empty(), format!("node {node} has no actual publication Lock request"))?;
            self.refresh.push(cluster.proxies[node].pause_catalog_refresh(ids)?);
        }
        self.record("refresh-rules-installed")
    }
}

impl Drop for FaultGates {
    fn drop(&mut self) {
        let _ = self.record("rpc-fault-final-state");
    }
}

#[derive(Clone, serde::Serialize)]
struct RefreshLossTarget {
    lock_id: LockId,
    observation_id: u64,
    nodes: Vec<usize>,
    barrier_sequence: u64,
    barrier_elapsed_micros: u64,
}

fn capture_refresh_targets(resources: &[ObjectKey], barrier: &Value, locks: &[Value]) -> TestResult<Vec<RefreshLossTarget>> {
    let observations = barrier["observations"]["locks"]
        .as_array()
        .ok_or("missing barrier lock observations")?;
    let boundary = barrier["events"]
        .as_array()
        .and_then(|events| events.last())
        .ok_or("missing barrier backend event")?;
    let mut targets: Vec<RefreshLossTarget> = Vec::new();
    for resource in resources {
        let active = observations
            .iter()
            .filter(|lock| {
                lock["bucket"] == resource.bucket.as_ref()
                    && lock["key"] == resource.object.as_ref()
                    && lock["has_loss_signal"] == true
                    && lock["signal_alive"] == true
            })
            .collect::<Vec<_>>();
        require(active.len() == 1, "publication resource has no unique live guard at the barrier")?;
        require(
            active[0]["is_lost"] == false && active[0]["lost_observed"] == false,
            "publication guard was already lost before Refresh injection",
        )?;
        let observation_id = active[0]["observation_id"].as_u64().ok_or("missing guard observation id")?;
        for (node, snapshot) in locks.iter().enumerate().skip(1) {
            let packets = snapshot["packets"].as_array().ok_or("missing captured Lock packets")?;
            for packet in packets {
                require(
                    matches!(packet["method"].as_str(), Some("Lock" | "LockBatch")),
                    "unexpected acquisition method",
                )?;
                for observed in packet["locks"].as_array().ok_or("missing captured Lock identities")? {
                    let observed_resource: ObjectKey = serde_json::from_value(observed["resource"].clone())?;
                    if &observed_resource != resource {
                        continue;
                    }
                    let lock_id: LockId = serde_json::from_value(observed["lock_id"].clone())?;
                    require(lock_id.resource == *resource, "captured lock id and resource disagree")?;
                    uuid::Uuid::parse_str(&lock_id.uuid)?;
                    if let Some(target) = targets.iter_mut().find(|target| target.lock_id == lock_id) {
                        if !target.nodes.contains(&node) {
                            target.nodes.push(node);
                        }
                    } else {
                        targets.push(RefreshLossTarget {
                            lock_id,
                            observation_id,
                            nodes: vec![node],
                            barrier_sequence: boundary["sequence"].as_u64().ok_or("missing barrier sequence")?,
                            barrier_elapsed_micros: boundary["elapsed_micros"].as_u64().ok_or("missing barrier time")?,
                        });
                    }
                }
            }
        }
    }
    require(!targets.is_empty(), "no exact lock identities were captured for Refresh injection")?;
    Ok(targets)
}

fn refresh_quorum_loss_proof(
    worker: &Value,
    prepared: &Value,
    targets: &[RefreshLossTarget],
    refresh: &[(usize, Value)],
    peer_count: usize,
) -> TestResult<Option<Value>> {
    require(peer_count > 1 && refresh.len() == peer_count - 1, "incomplete remote peer evidence")?;
    // SetDisks::namespace_lock_for_key uses this majority over its actual locker domain.
    let write_quorum = peer_count / 2 + 1;
    let required_blocked = peer_count - write_quorum + 1;
    let mut peers = std::collections::BTreeSet::new();
    let mut blocked = vec![std::collections::BTreeSet::new(); targets.len()];
    for (node, snapshot) in refresh {
        require(*node > 0 && *node < peer_count && peers.insert(*node), "duplicate or invalid remote peer")?;
        for field in ["overflow", "forwarded", "discarded", "responses", "refused"] {
            require(snapshot[field] == 0, format!("Refresh evidence has nonzero or missing {field}"))?;
        }
        let packets = snapshot["packets"].as_array().ok_or("missing Refresh packets")?;
        require(
            snapshot["hits"].as_u64() == Some(u64::try_from(packets.len())?) && snapshot["blocked"] == snapshot["hits"],
            "Refresh hit/blocked evidence is incomplete",
        )?;
        for packet in packets {
            require(
                packet["method"] == "Refresh"
                    && packet["blocked"] == true
                    && packet["forwarded"] == false
                    && packet["discarded"] == false
                    && packet["response"].is_null()
                    && packet["transport_error"].is_null(),
                "Refresh packet was not retained without forwarding",
            )?;
            let identities = packet["locks"].as_array().ok_or("missing Refresh identity")?;
            require(identities.len() == 1, "Refresh must identify exactly one lock")?;
            let lock_id: LockId = serde_json::from_value(identities[0]["lock_id"].clone())?;
            let resource: ObjectKey = serde_json::from_value(identities[0]["resource"].clone())?;
            require(resource == lock_id.resource, "Refresh resource differs from its lock id")?;
            let index = targets
                .iter()
                .position(|target| target.lock_id == lock_id && target.nodes.contains(node))
                .ok_or("Refresh does not match the captured acquisition on this peer")?;
            blocked[index].insert(*node);
        }
    }
    let observations = worker["observations"]["locks"]
        .as_array()
        .ok_or("missing current lock observations")?;
    for (target, nodes) in targets.iter().zip(blocked) {
        if nodes.len() < required_blocked {
            continue;
        }
        let matching = observations
            .iter()
            .filter(|lock| lock["observation_id"].as_u64() == Some(target.observation_id))
            .collect::<Vec<_>>();
        require(matching.len() == 1, "current guard identity is missing or duplicated")?;
        let lock = matching[0];
        require(
            lock["bucket"] == target.lock_id.resource.bucket.as_ref()
                && lock["key"] == target.lock_id.resource.object.as_ref()
                && lock["has_loss_signal"] == true,
            "current loss signal belongs to another resource",
        )?;
        if lock["lost_observed"] != true {
            continue;
        }
        let first_lost = lock["first_lost_elapsed_micros"]
            .as_u64()
            .ok_or("missing actual loss observation time")?;
        require(
            first_lost >= target.barrier_elapsed_micros,
            "loss predates this guard's injection boundary",
        )?;
        let live_loss = lock["signal_alive"] == true && lock["is_lost"] == true;
        // The backend records these terminal events before returning to the guard owner.
        // A historical observation after that return cannot establish live-guard loss.
        let before_backend_return = lock["signal_alive"] == false
            && lock["is_lost"].is_null()
            && worker["events"].as_array().is_some_and(|events| {
                events.iter().any(|event| {
                    event["sequence"]
                        .as_u64()
                        .is_some_and(|sequence| sequence > target.barrier_sequence)
                        && event["elapsed_micros"].as_u64().is_some_and(|time| time >= first_lost)
                        && event["method"] == "put_object_unlocked_fenced"
                        && event["bucket"] == prepared["paths"]["bucket"]
                        && event["object"] == prepared["paths"]["head"]
                        && matches!(event["phase"].as_str(), Some("inner-error" | "inner-completed"))
                })
            });
        if live_loss || before_backend_return {
            return Ok(Some(json!({"target":target,"blocked_nodes":nodes,"peer_count":peer_count,
                "write_quorum":write_quorum,"required_blocked":required_blocked,"remaining_votes_upper_bound":peer_count-nodes.len(),
                "loss":lock,"observed_live":live_loss,"recorded_before_backend_return":before_backend_return})));
        }
    }
    Ok(None)
}

async fn wait_lost(cluster: &CatalogCluster, session: &Session, gates: &FaultGates, prepared: &Value) -> TestResult<Value> {
    let peer_count = cluster.proxies.len();
    require(
        peer_count == 4 && cluster.clients.len() == peer_count && cluster.cluster.nodes.len() == peer_count,
        "lock proof does not match the verified four-node locker domain",
    )?;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let value = events(cluster, session).await?;
        let refresh = gates
            .refresh
            .iter()
            .enumerate()
            .map(|(index, gate)| serde_json::to_value(gate.snapshot()).map(|value| (index + 1, value)))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(proof) = refresh_quorum_loss_proof(&value, prepared, &gates.refresh_targets, &refresh, peer_count)? {
            cluster
                .evidence
                .record("actual-lock-loss", &json!({"worker":value,"refresh":refresh,"proof":proof}))?;
            return Ok(value);
        }
        require(
            Instant::now() < deadline,
            "actual Refresh quorum loss and LockLostSignal were not both observed",
        )?;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
async fn catalog_head_before_write_rejects_actual_refresh_quorum_loss() -> TestResult {
    let cluster = CatalogCluster::start("lock-loss-before-head").await?;
    let result = async {
        let session = cluster.create().await?;
        let initial = cluster.agree(&session).await?;
        let prepared = cluster.prepare(&session, 0).await?;
        let fault = arm(&cluster, &prepared, "head", "before").await?;
        let mut gates = FaultGates::observe(&cluster, &prepared)?;
        let client = cluster.clients[0].clone();
        let action = commit_action(&prepared);
        let commit = tokio::spawn(async move { client.response(action).await });
        let barrier = wait_arrived(&cluster, &session, &fault).await?;
        gates.block_refresh(&cluster, &prepared, &barrier)?;
        wait_lost(&cluster, &session, &gates, &prepared).await?;
        release(&cluster, &fault).await?;
        let response = tokio::time::timeout(Duration::from_secs(20), commit).await???;
        require(
            response["status"] == "error" && response["error"]["class"] == "Unavailable",
            format!("lock loss did not return the actual fence error: {response}"),
        )?;
        let observed = events(&cluster, &session).await?;
        require(
            event_count(&observed, &fault, "inner-error") == 1 && event_count(&observed, &fault, "inner-completed") == 0,
            "head backend did not reject before completing storage mutation",
        )?;
        require(
            gates.rename.iter().all(|gate| {
                let snapshot = gate.snapshot();
                snapshot.hits == 0 && snapshot.forwarded == 0 && snapshot.overflow == 0
            }),
            "head RenameData was issued after the observed protection loss",
        )?;
        gates.record("before-head-rejected")?;
        require(
            cluster.agree(&session).await? == initial,
            "head or manifest changed after rejected publication",
        )?;
        let candidate = raw(
            &cluster,
            &session,
            0,
            &prepared["candidate_readback"]["bucket"],
            &prepared["candidate_readback"]["key"],
        )
        .await?;
        require(
            candidate["object"] == prepared["candidate_readback"],
            "rejected publication lost the prepared candidate",
        )?;
        cluster.capture("lock-loss-before-head-physical-state").await
    }
    .await;
    cluster.finish(result).await
}

async fn wait_renames_settled(gates: &FaultGates) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let states = gates.rename.iter().map(CatalogRpcGate::snapshot).collect::<Vec<_>>();
        require(states.iter().all(|state| state.overflow == 0), "old RenameData evidence overflow")?;
        if states
            .iter()
            .all(|state| state.packets.len() == 1 && state.packets[0].response.is_some())
        {
            for state in &states {
                let packet = &state.packets[0];
                require(packet.forwarded && !packet.discarded, "old signed RenameData was not actually forwarded")?;
                let response = packet.response.as_ref().ok_or("missing old RenameData response")?;
                require(
                    sha256(&response.body) == response.body_sha256,
                    "stored raw RPC response bytes differ from digest",
                )?;
                require_storage_reply(
                    response.http_status,
                    response.grpc_status.as_deref(),
                    response.success,
                    response.error.as_deref(),
                    response.decode_error.as_deref(),
                )?;
            }
            return Ok(());
        }
        require(
            Instant::now() < deadline,
            "needs_review: original RenameData requests lack complete backend responses",
        )?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct LateRecords {
    commit: Value,
    idempotency: Value,
}

async fn late_raw(cluster: &CatalogCluster, session: &Session, node: usize, bucket: &Value, key: &Value) -> TestResult<Value> {
    let response = cluster.clients[node]
        .response(json!({"op":"raw_read","session_id":session.ids[node],"bucket":bucket,"key":key}))
        .await?;
    if response["status"] == "ok" {
        return Ok(response["result"]["object"].clone());
    }
    if response["error"]["class"] == "NotFound" {
        return Ok(Value::Null);
    }
    Err(io::Error::other(format!("needs_review: exact object evidence could not be read: {response}")).into())
}

async fn late_records(cluster: &CatalogCluster, session: &Session, node: usize, prepared: &Value) -> TestResult<LateRecords> {
    let paths = &prepared["paths"];
    Ok(LateRecords {
        commit: late_raw(cluster, session, node, &paths["bucket"], &paths["commit_log"]).await?,
        idempotency: late_raw(cluster, session, node, &paths["bucket"], &paths["idempotency"]).await?,
    })
}

async fn inspect_late_commit_proofs(
    cluster: &CatalogCluster,
    session: &Session,
    old: &Value,
    new: &Value,
    current_head: &Value,
    prior: Option<&[LateRecords; 2]>,
) -> TestResult<[LateRecords; 2]> {
    // Each producer session registered its own exact log/index paths during prepare.
    // Do not prepare again: doing so could recreate the very candidate being inspected.
    let records = [
        late_records(cluster, session, 0, old).await?,
        late_records(cluster, session, 1, new).await?,
    ];
    if let Some(prior) = prior {
        for (before, after) in prior.iter().zip(&records) {
            assert_retained_record(&before.commit, &after.commit)?;
            assert_retained_record(&before.idempotency, &after.idempotency)?;
        }
    }
    for node in 0..4 {
        let recovery = cluster.clients[node]
            .call(json!({"op":"recovery","session_id":session.ids[node]}))
            .await?;
        for (index, prepared) in [old, new].into_iter().enumerate() {
            let candidate = late_raw(
                cluster,
                session,
                node,
                &prepared["candidate_readback"]["bucket"],
                &prepared["candidate_readback"]["key"],
            )
            .await?;
            cluster.evidence.record("late-commit-material",&json!({"phase":if prior.is_some(){"after-release"}else{"before-release"},"node":node,"prepared":prepared,"commit":records[index].commit,"idempotency":records[index].idempotency,"candidate":candidate,"recovery":recovery}))?;
            assert_late_commit_proof(LateCommitProof {
                identity: &session.identity,
                prepared,
                current_head,
                commit: &records[index].commit,
                idempotency: &records[index].idempotency,
                candidate: &candidate,
                recovery: &recovery,
                role: if index == 0 {
                    LateCommitRole::Superseded
                } else {
                    LateCommitRole::Committed
                },
            })?;
        }
    }
    Ok(records)
}

#[tokio::test]
async fn catalog_old_rename_packets_after_later_commit_preserve_observed_outcome() -> TestResult {
    let cluster = CatalogCluster::start("late-head-packets").await?;
    let result = async {
        let session = cluster.create().await?;
        let prepared = cluster.prepare(&session, 0).await?;
        let fault = arm(&cluster, &prepared, "head", "before").await?;
        let mut gates = FaultGates::observe(&cluster, &prepared)?;
        let client = cluster.clients[0].clone();
        let action = commit_action(&prepared);
        let commit = tokio::spawn(async move { client.response(action).await });
        let barrier = wait_arrived(&cluster, &session, &fault).await?;
        gates.block_refresh(&cluster, &prepared, &barrier)?;
        release(&cluster, &fault).await?;
        for gate in gates.rename.iter() {
            gate.wait_for_hits(1).await?;
        }
        let retained = gates.rename.iter().map(CatalogRpcGate::snapshot).collect::<Vec<_>>();
        require(
            retained
                .iter()
                .all(|state| state.hits == 1 && state.blocked == 1 && state.forwarded == 0 && state.overflow == 0),
            "old head RenameData was not held at every remote disk",
        )?;
        for state in &retained {
            let packet = &state.packets[0];
            let bytes = base64_simd::STANDARD.decode_to_vec(packet.body_base64.as_bytes())?;
            require(
                sha256(&bytes) == packet.body_sha256,
                "retained original request bytes differ from their digest",
            )?;
        }
        gates.record("old-head-requests-retained")?;
        wait_lost(&cluster, &session, &gates, &prepared).await?;
        let next = cluster.prepare(&session, 1).await?;
        require(
            prepared["expected_head"] == next["expected_head"]
                && prepared["commit_id"] != next["commit_id"]
                && prepared["replay"]["idempotency_key"] != next["replay"]["idempotency_key"],
            "needs_review: old and later publishers do not form the intended distinct same-base attempts",
        )?;
        cluster.clients[1].call(commit_action(&next)).await?;
        let committed = cluster.agree(&session).await?;
        assert_candidate(&committed, &next)?;
        let retained_records = inspect_late_commit_proofs(&cluster, &session, &prepared, &next, &committed["head"], None).await?;
        let (before_head, before_complete) = cluster.object_census(&committed["head"]["raw"])?;
        cluster.evidence.record(
            "head-directories-before-old-release",
            &json!({"directories":before_head,"all_complete":before_complete}),
        )?;
        assert_no_head_regression(&before_head, &before_head)?;
        cluster.capture("later-commit-before-old-release").await?;
        for gate in &gates.rename {
            gate.release_all()?;
        }
        wait_renames_settled(&gates).await?;
        // Capture disk materials before any subsequent quorum read can initiate healing.
        let (after_head, after_complete) = cluster
            .object_census(&committed["head"]["raw"])
            .map_err(|error| io::Error::other(format!("needs_review: post-release directory evidence is unreadable: {error}")))?;
        cluster.evidence.record(
            "head-directories-after-old-release",
            &json!({"directories":after_head,"all_complete":after_complete}),
        )?;
        let directory_verdict = assert_no_head_regression(&before_head, &after_head);
        let publisher_verdict = async {
            for (gate, before) in gates.rename.iter().zip(&retained) {
                let after = gate.snapshot();
                require(
                    after.packets[0].body_sha256 == before.packets[0].body_sha256
                        && after.packets[0].body_base64 == before.packets[0].body_base64
                        && after.packets[0].src_path == before.packets[0].src_path
                        && after.packets[0].dst_path == before.packets[0].dst_path,
                    "released request differs from the retained signed request",
                )?;
            }
            let old = tokio::time::timeout(Duration::from_secs(20), commit).await???;
            gates.record("old-head-requests-completed")?;
            cluster.evidence.record("old-publisher-result", &old)?;
            require(
                old["status"] == "error",
                format!("failed: expired old publisher reported success after later publication: {old}"),
            )
        }
        .await;
        directory_verdict.and(publisher_verdict)?;
        let final_read = cluster.agree(&session).await?;
        require(
            final_read == committed,
            "failed: restored old requests overwrote or lost the later committed head/manifest",
        )?;
        inspect_late_commit_proofs(&cluster, &session, &prepared, &next, &committed["head"], Some(&retained_records)).await?;
        cluster.capture("after-old-request-physical-state").await
    }
    .await;
    cluster.finish(result).await
}

#[tokio::test]
async fn catalog_candidate_before_upload_process_exit_preserves_exact_replay() -> TestResult {
    let mut cluster = CatalogCluster::start("candidate-before-restart").await?;
    let result=async {
        let mut session=cluster.create().await?;
        let initial=cluster.agree(&session).await?;
        let before_dataset=cluster.dataset_census(&session)?;
        cluster.evidence.record("candidate-before-dataset",&before_dataset)?;
        let commit_id=uuid::Uuid::new_v4().to_string();
        let armed=cluster.clients[0].call(json!({"op":"candidate_before_arm","session_id":session.ids[0],"commit_id":commit_id,"idempotency_key":uuid::Uuid::new_v4().to_string(),"pause_timeout_ms":115000})).await?;
        require(armed["expected_head"]==initial["head"]["expected_head"],"candidate barrier captured another base")?;
        require(armed["replay"]["commit_id"]==commit_id,"candidate barrier changed commit_id")?;
        let client=cluster.clients[0].clone();
        let replay=armed["replay"].clone();
        let prepare=tokio::spawn(async move {client.response(replay).await});
        let deadline=Instant::now()+Duration::from_secs(60);
        loop {
            let value=events(&cluster,&session).await?;
            let reached=value["candidate_events"].as_array().ok_or("missing candidate barrier events")?.iter().filter(|event| event["fault_id"]==armed["fault_id"] && event["commit_id"]==commit_id && event["phase"]=="before-arrived").collect::<Vec<_>>();
            if !reached.is_empty() {
                require(reached.len()==1,"candidate Before barrier was reached more than once")?;
                cluster.evidence.record("candidate-before-arrived",&json!({"armed":armed,"events":value}))?;
                break;
            }
            require(Instant::now()<deadline,"candidate Before barrier did not execute")?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        require(cluster.agree(&session).await?==initial,"candidate Before changed the authoritative head/manifest")?;
        let paused_dataset=cluster.dataset_census(&session)?;
        cluster.evidence.record("candidate-paused-dataset",&paused_dataset)?;
        require(paused_dataset==before_dataset,"candidate Before wrote dataset bytes before the selected boundary")?;
        cluster.capture("candidate-before-process-exit").await?;
        cluster.stop_node(0)?;
        let outcome=tokio::time::timeout(Duration::from_secs(5),prepare).await??;
        require(outcome.is_err(),"paused candidate preparation returned after termination")?;
        cluster.start_stopped_node(0).await?;
        cluster.reopen(&mut session,0).await?;
        require(cluster.agree(&session).await?==initial,"candidate Before restart changed authoritative contents")?;
        let report=cluster.clients[0].call(json!({"op":"recovery","session_id":session.ids[0]})).await?;
        require(recovery_state(&report,&json!(commit_id))?.is_none(),"candidate Before created a commit log")?;
        let mut replay=armed["replay"].clone();
        replay["session_id"]=json!(session.ids[0]);
        let prepared=cluster.clients[0].call(replay).await?;
        require(prepared["expected_head"]==armed["expected_head"] && prepared["commit_id"]==commit_id,"restarted candidate preparation changed its immutable input")?;
        verify_manifest(&prepared["candidate_readback"])?;
        cluster.clients[0].call(commit_action(&prepared)).await?;
        assert_candidate(&cluster.agree(&session).await?,&prepared)
    }.await;
    cluster.finish(result).await
}

#[tokio::test]
async fn catalog_same_base_publishers_have_one_success_and_one_conflict() -> TestResult {
    let cluster = CatalogCluster::start("same-base-conflict").await?;
    let result = async {
        let session = cluster.create().await?;
        let a = cluster.prepare(&session, 0).await?;
        let b = cluster.prepare(&session, 1).await?;
        require(
            a["expected_head"] == b["expected_head"],
            "concurrent publishers did not use the same base",
        )?;
        let (ar, br) = tokio::join!(
            cluster.clients[0].response(commit_action(&a)),
            cluster.clients[1].response(commit_action(&b))
        );
        let ar = ar?;
        let br = br?;
        require(
            (ar["status"] == "ok") != (br["status"] == "ok"),
            format!("same-base publication must have exactly one success: A={ar}; B={br}"),
        )?;
        let (winner, loser, error) = if ar["status"] == "ok" { (&a, &b, &br) } else { (&b, &a, &ar) };
        require(
            error["status"] == "error" && error["error"]["class"] == "Conflict",
            format!("losing publisher did not return Conflict: {error}"),
        )?;
        assert_candidate(&cluster.agree(&session).await?, winner)?;
        let retained = raw(
            &cluster,
            &session,
            0,
            &loser["candidate_readback"]["bucket"],
            &loser["candidate_readback"]["key"],
        )
        .await?;
        require(
            retained["object"] == loser["candidate_readback"],
            "conflict removed a candidate without recovery authority",
        )?;
        cluster.capture("same-base-conflict-physical-state").await
    }
    .await;
    cluster.finish(result).await
}

fn require_storage_reply(
    http_status: u16,
    grpc_status: Option<&str>,
    success: Option<bool>,
    error: Option<&str>,
    decode_error: Option<&str>,
) -> TestResult {
    require(
        http_status == 200 && grpc_status == Some("0") && decode_error.is_none(),
        format!(
            "needs_review: old request has no decoded storage RPC reply: HTTP={http_status}, gRPC={grpc_status:?}, decode={decode_error:?}"
        ),
    )?;
    require(
        matches!((success, error), (Some(true), None) | (Some(false), Some(_))),
        format!("needs_review: storage mutation outcome is unknown or contradictory: success={success:?}, error={error:?}"),
    )
}

#[test]
fn old_request_transport_rejection_is_not_a_storage_result() {
    assert!(require_storage_reply(200, Some("7"), None, None, None).is_err());
    assert!(require_storage_reply(403, None, None, None, None).is_err());
    assert!(require_storage_reply(200, Some("0"), None, None, None).is_err());
    assert!(require_storage_reply(200, Some("0"), Some(true), Some("contradiction"), None).is_err());
}

#[test]
fn old_request_requires_an_explicit_storage_success_or_refusal() {
    assert!(require_storage_reply(200, Some("0"), Some(true), None, None).is_ok());
    assert!(require_storage_reply(200, Some("0"), Some(false), Some("generation changed"), None).is_ok());
    assert!(require_storage_reply(200, Some("0"), Some(false), None, None).is_err());
}

#[cfg(test)]
mod fault_control_tests {
    use super::*;

    struct RefreshFixture {
        prepared: Value,
        worker: Value,
        targets: Vec<RefreshLossTarget>,
        refresh: Vec<(usize, Value)>,
    }

    fn refresh_fixture() -> RefreshFixture {
        let resource: ObjectKey =
            serde_json::from_value(json!({"bucket":".rustfs.sys","object":"fixture/head","version":null})).expect("resource");
        let id = LockId {
            resource: resource.clone(),
            uuid: "00000000-0000-4000-8000-000000000001".into(),
        };
        let target = RefreshLossTarget {
            lock_id: id.clone(),
            observation_id: 7,
            nodes: vec![1, 2, 3, 4],
            barrier_sequence: 10,
            barrier_elapsed_micros: 100,
        };
        let packet = json!({"method":"Refresh","blocked":true,"forwarded":false,"discarded":false,"response":null,"transport_error":null,
            "locks":[{"resource":resource,"lock_id":id}]});
        let empty = json!({"hits":0,"blocked":0,"forwarded":0,"discarded":0,"responses":0,"refused":0,"overflow":0,"packets":[]});
        let mut blocked = empty.clone();
        blocked["hits"] = json!(1);
        blocked["blocked"] = json!(1);
        blocked["packets"] = json!([packet]);
        RefreshFixture {
            prepared: json!({"paths":{"bucket":".rustfs.sys","head":"fixture/head"}}),
            worker: json!({"observations":{"locks":[{"observation_id":7,"bucket":".rustfs.sys","key":"fixture/head","has_loss_signal":true,
                "signal_alive":true,"is_lost":true,"lost_observed":true,"first_lost_elapsed_micros":300}]},"events":[]}),
            targets: vec![target],
            refresh: vec![(1, empty), (2, blocked.clone()), (3, blocked)],
        }
    }
    impl RefreshFixture {
        fn proof(&self, peers: usize) -> TestResult<Option<Value>> {
            refresh_quorum_loss_proof(&self.worker, &self.prepared, &self.targets, &self.refresh, peers)
        }
    }

    #[test]
    fn refresh_quorum_accepts_two_exact_peers_and_live_loss() {
        let fixture = refresh_fixture();
        let proof = fixture.proof(4).expect("valid evidence").expect("actual lost quorum");
        assert_eq!(proof["blocked_nodes"], json!([2, 3]));
        assert_eq!(proof["write_quorum"], 3);
        assert_eq!(proof["remaining_votes_upper_bound"], 2);
    }
    #[test]
    fn refresh_quorum_rejects_one_peer_and_duplicate_peer_votes() {
        let mut fixture = refresh_fixture();
        fixture.refresh[2].1 = fixture.refresh[0].1.clone();
        assert!(fixture.proof(4).expect("complete snapshots").is_none());
        let mut fixture = refresh_fixture();
        fixture.refresh[2].0 = 2;
        assert!(fixture.proof(4).is_err());
    }
    #[test]
    fn refresh_quorum_rejects_different_resources_or_lock_ids() {
        for pointer in ["/packets/0/locks/0/resource/object", "/packets/0/locks/0/lock_id/uuid"] {
            let mut fixture = refresh_fixture();
            *fixture.refresh[2].1.pointer_mut(pointer).expect("identity field") = json!("other");
            assert!(fixture.proof(4).is_err(), "{pointer} cannot join another lock's votes");
        }
    }
    #[test]
    fn refresh_quorum_requires_actual_loss_from_the_same_guard() {
        let mut fixture = refresh_fixture();
        fixture.worker["observations"]["locks"][0]["lost_observed"] = json!(false);
        assert!(fixture.proof(4).expect("pending genuine loss").is_none());
        fixture.worker["observations"]["locks"][0]["observation_id"] = json!(8);
        assert!(fixture.proof(4).is_err());
    }
    #[test]
    fn refresh_quorum_rejects_forwarded_or_overflowed_packets() {
        for field in ["forwarded", "overflow"] {
            let mut fixture = refresh_fixture();
            fixture.refresh[1].1[field] = json!(1);
            assert!(fixture.proof(4).is_err());
        }
        let mut fixture = refresh_fixture();
        fixture.refresh[1].1["packets"][0]["forwarded"] = json!(true);
        assert!(fixture.proof(4).is_err());
    }
    #[test]
    fn refresh_quorum_uses_the_verified_peer_domain() {
        let mut fixture = refresh_fixture();
        fixture.refresh.push((4, fixture.refresh[0].1.clone()));
        assert!(fixture.proof(5).expect("five peer topology").is_none());
        fixture.refresh[3].1 = fixture.refresh[1].1.clone();
        let proof = fixture
            .proof(5)
            .expect("three blocked peers")
            .expect("remaining votes below quorum");
        assert_eq!(proof["required_blocked"], 3);
    }
    #[test]
    fn refresh_quorum_accepts_only_loss_recorded_before_guard_return() {
        let mut fixture = refresh_fixture();
        let lock = &mut fixture.worker["observations"]["locks"][0];
        lock["signal_alive"] = json!(false);
        lock["is_lost"] = Value::Null;
        assert!(fixture.proof(4).expect("historical signal needs a terminal event").is_none());
        fixture.worker["events"] = json!([{"sequence":11,"elapsed_micros":301,"method":"put_object_unlocked_fenced",
            "bucket":".rustfs.sys","object":"fixture/head","phase":"inner-error"}]);
        assert!(fixture.proof(4).expect("backend sampled before returning").is_some());
        fixture.worker["observations"]["locks"][0]["first_lost_elapsed_micros"] = json!(302);
        assert!(
            fixture
                .proof(4)
                .expect("loss after return cannot prove live-guard loss")
                .is_none()
        );
    }
    #[test]
    fn refresh_quorum_captures_a_unique_live_guard_and_exact_acquisition() {
        let fixture = refresh_fixture();
        let resource = fixture.targets[0].lock_id.resource.clone();
        let id = fixture.targets[0].lock_id.clone();
        let mut barrier = fixture.worker;
        let lock = &mut barrier["observations"]["locks"][0];
        lock["is_lost"] = json!(false);
        lock["lost_observed"] = json!(false);
        barrier["events"] = json!([{"sequence":10,"elapsed_micros":100}]);
        let packet = json!({"method":"Lock","locks":[{"resource":resource,"lock_id":id}]});
        let snapshots = vec![
            json!({"packets":[]}),
            json!({"packets":[packet]}),
            json!({"packets":[packet]}),
            json!({"packets":[packet]}),
        ];
        let targets = capture_refresh_targets(std::slice::from_ref(&resource), &barrier, &snapshots).expect("exact acquisition");
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].observation_id, 7);
        assert_eq!(targets[0].nodes, vec![1, 2, 3]);
        barrier["observations"]["locks"][0]["signal_alive"] = json!(false);
        assert!(capture_refresh_targets(&[resource], &barrier, &snapshots).is_err());
    }
    #[test]
    fn discarded_response_requires_the_exact_running_commit_receipt() {
        let valid = json!({"operations":[{"request_id":"wanted","action":"commit","status":"running","completed_micros":null}]});
        require_discarded_request_arrived(&valid, "wanted").expect("registered paused request");
        assert!(require_discarded_request_arrived(&valid, "other").is_err());
        assert!(require_discarded_request_arrived(&json!({"operations":[]}), "wanted").is_err());
        let duplicate = json!({"operations":[valid["operations"][0].clone(),valid["operations"][0].clone()]});
        assert!(require_discarded_request_arrived(&duplicate, "wanted").is_err());
        let failed = json!({"operations":[{"request_id":"wanted","action":"commit","status":"error","completed_micros":123,
            "error":{"class":"Internal","message":"original storage failure"}}]});
        let error = discarded_request_receipt(&failed, "wanted")
            .expect_err("explicit worker failure")
            .to_string();
        assert!(error.contains("original storage failure") && error.contains("Internal"));
        for (field, value) in [
            ("action", json!("prepare")),
            ("status", json!("ok")),
            ("completed_micros", json!(123)),
        ] {
            let mut invalid = valid.clone();
            invalid["operations"][0][field] = value;
            assert!(require_discarded_request_arrived(&invalid, "wanted").is_err());
        }
    }
    #[test]
    fn restart_replay_accepts_success_and_retries_exact_lock_unavailable() {
        let budget = Duration::from_secs(60);
        let elapsed = Duration::from_secs(5);
        for (op, result) in [
            ("commit", json!({"table":{},"commit_log":{}})),
            ("recover", json!({"table_id":"fixture","commits":[]})),
        ] {
            assert!(
                restart_replay_operation_outcome(op, &json!({"status":"ok","error":null,"result":result}), elapsed, budget)
                    .expect("success")
            );
            assert!(
                !restart_replay_operation_outcome(
                    op,
                    &json!({"status":"error","result":null,"error":{"class":"LockUnavailable"}}),
                    elapsed,
                    budget
                )
                .expect("exact retryable class")
            );
        }
    }
    #[test]
    fn restart_replay_rejects_other_classes_and_unknown_responses() {
        let budget = Duration::from_secs(60);
        let elapsed = Duration::from_secs(5);
        for op in ["recover", "commit"] {
            for class in ["Unavailable", "Internal", "Conflict", "Timeout", "Cancelled", ""] {
                assert!(
                    restart_replay_operation_outcome(
                        op,
                        &json!({"status":"error","result":null,"error":{"class":class}}),
                        elapsed,
                        budget
                    )
                    .is_err()
                );
            }
            for response in [
                json!({}),
                json!({"status":"unknown"}),
                json!({"status":"ok","result":null}),
                json!({"status":"error","result":{},"error":{"class":"LockUnavailable"}}),
            ] {
                assert!(restart_replay_operation_outcome(op, &response, elapsed, budget).is_err());
            }
        }
        assert!(
            restart_replay_operation_outcome(
                "prepare",
                &json!({"status":"error","result":null,"error":{"class":"LockUnavailable"}}),
                elapsed,
                budget
            )
            .is_err()
        );
    }
    #[test]
    fn restart_replay_rejects_deadline_exhaustion_even_with_success() {
        for (op, result) in [
            ("commit", json!({"table":{},"commit_log":{}})),
            ("recover", json!({"table_id":"fixture","commits":[]})),
        ] {
            for response in [
                json!({"status":"ok","error":null,"result":result}),
                json!({"status":"error","result":null,"error":{"class":"LockUnavailable"}}),
            ] {
                assert!(
                    restart_replay_operation_outcome(op, &response, Duration::from_secs(60), Duration::from_secs(60)).is_err()
                );
            }
        }
    }
}
