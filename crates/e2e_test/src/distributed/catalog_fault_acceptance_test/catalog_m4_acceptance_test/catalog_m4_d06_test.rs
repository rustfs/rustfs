// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Separate initial-lock and native head-write partitions in four real processes.

use super::super::{CatalogCluster, Session, TestResult, assert_candidate, commit_action, events, field, raw, require, sha256};
use crate::rpc_fault_proxy::{CatalogRenameScope, CatalogRpcGate};
use futures::{StreamExt as _, stream::FuturesUnordered};
use prost::Message as _;
use rustfs_lock::{LockRequest, LockType, ObjectKey};
use rustfs_protos::proto_gen::node_service::{
    BatchGenerallyLockRequest, BatchGenerallyLockResponse, GenerallyLockRequest, GenerallyLockResponse,
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};

#[derive(Clone, Copy)]
enum Plane {
    Coordination,
    Data,
}

impl Plane {
    fn label(self) -> &'static str {
        match self {
            Self::Coordination => "coordination",
            Self::Data => "data",
        }
    }
}

struct Participant {
    node: usize,
    partitioned: bool,
    gate: CatalogRpcGate,
}

fn publication_resource(prepared: &Value) -> TestResult<ObjectKey> {
    let resources: Vec<ObjectKey> = serde_json::from_value(prepared["paths"]["lock_resources"].clone())?;
    let key = format!("{}.publication.lock", field(&prepared["paths"], "head")?);
    let selected = resources
        .into_iter()
        .filter(|resource| resource.object.as_ref() == key)
        .collect::<Vec<_>>();
    require(selected.len() == 1, "prepared commit omitted its exact table publication lock")?;
    Ok(selected[0].clone())
}

fn record_participants(cluster: &CatalogCluster, label: &str, gates: &[Participant]) -> TestResult {
    cluster.evidence.record(
        label,
        &json!(
            gates
                .iter()
                .map(|participant| json!({
                    "node":participant.node, "partitioned":participant.partitioned,
                    "rpc":participant.gate.snapshot(),
                }))
                .collect::<Vec<_>>()
        ),
    )
}

fn frame_payload(bytes: &[u8]) -> TestResult<&[u8]> {
    require(bytes.len() >= 5 && bytes[0] == 0, "RPC evidence is not an uncompressed unary frame")?;
    let length = u32::from_be_bytes(bytes[1..5].try_into()?);
    require(
        usize::try_from(length)? == bytes.len() - 5,
        "RPC frame length differs from retained bytes",
    )?;
    Ok(&bytes[5..])
}

fn acquisition_requests(method: &str, bytes: &[u8]) -> TestResult<Vec<LockRequest>> {
    let payload = frame_payload(bytes)?;
    let args = match method {
        "Lock" => vec![GenerallyLockRequest::decode(payload)?.args],
        "LockBatch" => BatchGenerallyLockRequest::decode(payload)?.args,
        _ => return Err("unexpected coordination method".into()),
    };
    args.iter()
        .map(|args| serde_json::from_str(args).map_err(Into::into))
        .collect()
}

fn validate_packets(gates: &[Participant], prepared: &Value, plane: Plane, finished: bool) -> TestResult {
    let resource = publication_resource(prepared)?;
    let mut nodes = BTreeSet::new();
    for participant in gates {
        require(
            (1..4).contains(&participant.node) && nodes.insert(participant.node),
            "duplicate or invalid remote participant",
        )?;
        let observed = participant.gate.snapshot();
        require(observed.overflow == 0, "partition RPC evidence overflow")?;
        require(
            observed.hits > 0 && (!matches!(plane, Plane::Data) || observed.hits < 64),
            "partition missed its participant or exhausted the Rename limit",
        )?;
        for packet in &observed.packets {
            let bytes = base64_simd::STANDARD.decode_to_vec(&packet.body_base64)?;
            require(sha256(&bytes) == packet.body_sha256, "retained request frame digest differs")?;
            match plane {
                Plane::Coordination => {
                    let requests = acquisition_requests(&packet.method, &bytes)?;
                    let selected = requests
                        .iter()
                        .filter(|request| request.resource == resource)
                        .collect::<Vec<_>>();
                    require(selected.len() == 1, "acquisition packet lacks one exact publication resource")?;
                    require(
                        selected[0].lock_type == LockType::Exclusive,
                        "selected publication request is not exclusive",
                    )?;
                    require(
                        selected[0].lock_id.resource == resource && !selected[0].owner.is_empty(),
                        "acquisition identity is incomplete",
                    )?;
                    require(
                        packet.locks.len() == 1
                            && packet.locks[0].lock_id == selected[0].lock_id
                            && packet.locks[0].resource == resource
                            && packet.locks[0].owner == selected[0].owner,
                        "decoded acquisition evidence differs from original request",
                    )?;
                }
                Plane::Data => {
                    require(
                        matches!(
                            packet.method.as_str(),
                            "RenameData" | "RenameDataAtIncarnation" | "RenameDataWithQuotaFence"
                        ),
                        "unexpected data method",
                    )?;
                    require(
                        packet.dst_volume.as_deref() == prepared["paths"]["bucket"].as_str()
                            && packet.dst_path.as_deref() == prepared["paths"]["head"].as_str()
                            && packet.src_volume.as_deref() == Some(".rustfs.sys/tmp")
                            && packet.disk.as_ref().is_some_and(|disk| !disk.is_empty()),
                        "data packet selected another native object",
                    )?;
                }
            }
            if !finished {
                require(
                    packet.blocked && !packet.forwarded && !packet.discarded,
                    "initial participant request escaped the gate",
                )?;
            } else if participant.partitioned {
                require(
                    packet.discarded && !packet.forwarded && !packet.blocked && packet.response.is_none(),
                    "partitioned request reached storage",
                )?;
            } else {
                require(
                    packet.forwarded && !packet.discarded && !packet.blocked,
                    "available participant was not restored",
                )?;
                let reply = packet
                    .response
                    .as_ref()
                    .ok_or("available participant has no complete response")?;
                require(
                    reply.http_status == 200
                        && reply.grpc_status.as_deref() == Some("0")
                        && reply.decode_error.is_none()
                        && packet.transport_error.is_none()
                        && sha256(&reply.body) == reply.body_sha256,
                    "available participant returned an invalid RPC response",
                )?;
                match plane {
                    Plane::Data => require(
                        reply.success == Some(true) && reply.error.is_none(),
                        "available disk refused the native rename",
                    )?,
                    Plane::Coordination => {
                        let requests = acquisition_requests(&packet.method, &bytes)?;
                        let success = if packet.method == "LockBatch" {
                            let response = BatchGenerallyLockResponse::decode(frame_payload(&reply.body)?)?;
                            require(response.results.len() == requests.len(), "batch response cardinality differs")?;
                            requests
                                .iter()
                                .zip(response.results.iter())
                                .filter(|(request, _)| request.resource == resource)
                                .all(|(_, result)| result.success && result.error_info.is_none())
                        } else {
                            let response = GenerallyLockResponse::decode(frame_payload(&reply.body)?)?;
                            response.success && response.error_info.is_none()
                        };
                        require(success, "available voter refused the selected acquisition")?;
                    }
                }
            }
        }
    }
    require(nodes.len() == 3, "partition evidence omitted a remote participant")
}

async fn run_partition(
    cluster: &CatalogCluster,
    session: &Session,
    prepared: &Value,
    plane: Plane,
    blocked: &[usize],
) -> TestResult<Value> {
    let resource = publication_resource(prepared)?;
    let gates = cluster
        .proxies
        .iter()
        .enumerate()
        .skip(1)
        .map(|(node, proxy)| {
            let gate = match plane {
                Plane::Coordination => proxy.pause_catalog_locks(vec![resource.clone()])?,
                Plane::Data => proxy.pause_catalog_rename(
                    CatalogRenameScope {
                        dst_volume: field(&prepared["paths"], "bucket")?.to_owned(),
                        dst_path: field(&prepared["paths"], "head")?.to_owned(),
                        src_path: None,
                    },
                    64,
                )?,
            };
            Ok(Participant {
                node,
                partitioned: blocked.contains(&node),
                gate,
            })
        })
        .collect::<TestResult<Vec<_>>>()?;
    let action = commit_action(prepared);
    let request = cluster.clients[0].response(action);
    tokio::pin!(request);
    let result = tokio::time::timeout(Duration::from_secs(110), async {
        for participant in &gates {
            tokio::select! {
                response = &mut request => return Err(format!("commit completed before all selected requests were blocked: {:?}", response?).into()),
                reached = participant.gate.wait_for_hits(1) => reached?,
            }
        }
        record_participants(cluster, "d06-all-remote-requests-blocked", &gates)?;
        validate_packets(&gates, prepared, plane, false)?;
        let guard_before = events(cluster, session).await?;
        cluster.evidence.record("d06-guard-before-partition", &guard_before)?;
        if matches!(plane, Plane::Data) {
            let observed = guard_before["observations"]["locks"].as_array().ok_or("missing native guard observations")?;
            let live = observed.iter().filter(|guard| guard["bucket"] == resource.bucket.as_ref()
                && guard["key"] == resource.object.as_ref() && guard["signal_alive"] == true).collect::<Vec<_>>();
            require(live.len() == 1 && live[0]["is_lost"] == false && live[0]["lost_observed"] == false,
                "data partition began without one live publication guard")?;
        }
        if matches!(plane, Plane::Coordination) {
            for node in 0..cluster.clients.len() {
                let candidate = raw(cluster, session, node, &prepared["candidate_readback"]["bucket"], &prepared["candidate_readback"]["key"]).await?;
                require(candidate["object"] == prepared["candidate_readback"], "coordination partition changed the uploaded data")?;
            }
        }
        loop {
            let counts = gates.iter().map(|participant| participant.gate.snapshot().hits).collect::<Vec<_>>();
            for participant in &gates {
                let snapshot = participant.gate.snapshot();
                require(snapshot.overflow == 0 && (!matches!(plane, Plane::Data) || snapshot.hits < 64), "partition evidence budget exhausted")?;
                if participant.partitioned { participant.gate.discard_all()?; } else { participant.gate.release_all()?; }
            }
            let mut arrivals = gates.iter().zip(counts).map(|(participant, count)| participant.gate.wait_for_hits(count + 1))
                .collect::<FuturesUnordered<_>>();
            tokio::select! {
                response = &mut request => break response,
                arrived = arrivals.next() => arrived.ok_or("missing partition observers")??,
            }
        }
    }).await;
    for participant in &gates {
        participant.gate.discard_all()?;
    }
    record_participants(cluster, "d06-terminal-rpc-state", &gates)?;
    let response = result??;
    cluster.evidence.record("d06-original-commit-response", &response)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut observation = tokio::time::interval(Duration::from_millis(20));
    loop {
        if gates
            .iter()
            .filter(|participant| !participant.partitioned)
            .all(|participant| {
                let snapshot = participant.gate.snapshot();
                snapshot.responses == snapshot.hits
            })
        {
            break;
        }
        require(
            tokio::time::Instant::now() < deadline,
            "available participant response evidence did not complete",
        )?;
        observation.tick().await;
    }
    record_participants(cluster, "d06-complete-rpc-state", &gates)?;
    validate_packets(&gates, prepared, plane, true)?;
    let guard_after = events(cluster, session).await?;
    cluster.evidence.record("d06-guard-after-partition", &guard_after)?;
    if matches!(plane, Plane::Data) {
        let observed = guard_after["observations"]["locks"]
            .as_array()
            .ok_or("missing final guard observations")?;
        require(
            observed
                .iter()
                .filter(|guard| guard["bucket"] == resource.bucket.as_ref() && guard["key"] == resource.object.as_ref())
                .all(|guard| guard["lost_observed"] == false),
            "data partition also observed publication lock loss",
        )?;
    }
    Ok(response)
}

async fn partition_case(plane: Plane) -> TestResult {
    let mut cluster = CatalogCluster::start(&format!("m4-d06-{}", plane.label())).await?;
    let result = async {
        require(
            cluster.clients.len() == 4
                && cluster.proxies.len() == 4
                && cluster.cluster.nodes.len() == 4
                && cluster.cluster.nodes.iter().all(|node| node.data_dirs.len() == 1),
            "D06 requires four actual single-disk participants",
        )?;
        for blocked in [&[3_usize][..], &[2_usize, 3][..]] {
            let mut session = cluster.create().await?;
            let before = cluster.agree(&session).await?;
            let prepared = cluster.prepare(&session, 0).await?;
            cluster.evidence.record(
                "d06-partition-plan",
                &json!({"plane":plane.label(), "resource":session.identity,
                "paths":prepared["paths"], "blocked_nodes":blocked, "voters":4,
                "available_participants":4-blocked.len(), "exclusive_lock_quorum":3, "ec_write_quorum":3}),
            )?;
            let response = run_partition(&cluster, &session, &prepared, plane, blocked).await?;
            if blocked.len() == 1 {
                require(
                    response["status"] == "ok",
                    format!("three available participants did not commit: {response}"),
                )?;
            } else {
                let expected = match plane {
                    Plane::Coordination => "LockUnavailable",
                    Plane::Data => "Unavailable",
                };
                require(
                    response["status"] == "error" && response["error"]["class"] == expected,
                    format!("two available participants did not return {expected}: {response}"),
                )?;
                require(
                    cluster.agree(&session).await? == before,
                    "failed quorum changed authoritative head or data",
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
                    "failed quorum removed the exact candidate",
                )?;
                let recovery = cluster.clients[0]
                    .call(json!({"op":"recovery", "session_id":session.ids[0]}))
                    .await?;
                cluster.evidence.record("d06-restored-recovery-observation", &recovery)?;
                let replay = cluster.clients[0].response(commit_action(&prepared)).await?;
                cluster.evidence.record("d06-restored-same-commit-response", &replay)?;
                require(
                    replay["status"] == "ok",
                    format!("exact commit did not recover after partition removal: {replay}"),
                )?;
            }
            let restored = cluster.agree(&session).await?;
            assert_candidate(&restored, &prepared)?;
            cluster.evidence.record("d06-restored-authoritative-state", &restored)?;
            cluster.capture("d06-restored-native-files").await?;
            cluster.restart_all(&mut session).await?;
            require(
                cluster.agree(&session).await? == restored,
                "full restart changed the restored exact head or data",
            )?;
        }
        Ok(())
    }
    .await;
    cluster.finish(result).await
}

#[tokio::test]
async fn catalog_m4_d06_coordination_partition_preserves_quorum_and_recovers() -> TestResult {
    partition_case(Plane::Coordination).await
}

#[tokio::test]
async fn catalog_m4_d06_data_partition_preserves_quorum_and_recovers() -> TestResult {
    partition_case(Plane::Data).await
}
