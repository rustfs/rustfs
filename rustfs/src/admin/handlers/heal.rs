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

use crate::admin::auth::{authenticate_request, validate_admin_request};
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::app_context_from_req;
use crate::admin::storage_api::bucket::is_reserved_or_invalid_bucket;
use crate::admin::storage_api::bucket::utils::is_valid_object_prefix;
use crate::server::ADMIN_PREFIX;
use crate::server::RemoteAddr;
use crate::storage::rpc::node_service::heal::{
    HealControlCoordinator, NodeHealStatusSnapshot, capture_node_heal_status, decode_node_heal_status,
    decode_node_replacement_recovery_status, heal_control_coordinator, heal_topology_fingerprint,
};
use bytes::Bytes;
use futures_util::future::join_all;
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_common::heal_channel::{
    HealAdmissionReceipt, HealChannelPriority, HealChannelRequest, HealOpts, HealRequestSource, HealScanMode,
};
use rustfs_config::MAX_HEAL_REQUEST_SIZE;
use rustfs_heal::heal::utils::format_set_disk_id;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_scanner::scanner::{BackgroundHealInfo, read_background_heal_info};
use rustfs_utils::path::path_join;
use s3s::header::{CONTENT_LENGTH, CONTENT_TYPE};
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::time::{Duration, timeout};
use tracing::{info, warn};

const LOG_COMPONENT_ADMIN_API: &str = "admin_api";
const LOG_SUBSYSTEM_HEAL_ADMIN: &str = "heal_admin";
const EVENT_ADMIN_REQUEST_REJECTED: &str = "admin_request_rejected";
const EVENT_ADMIN_REQUEST_FAILED: &str = "admin_request_failed";
const EVENT_ADMIN_RESPONSE_EMITTED: &str = "admin_response_emitted";
const LEGACY_ROOT_HEAL_RESPONSE_ID: &str = ".";
const PEER_HEAL_STATUS_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const REPLACEMENT_RECOVERY_STATUS_ROUTE_SUFFIX: &str = "/v4/heal/replacement-recovery";
const REPLACEMENT_RECOVERY_STATUS_CONTRACT_VERSION: u32 = 2;

#[derive(Debug, Default, Serialize, Deserialize)]
struct HealInitParams {
    bucket: String,
    obj_prefix: String,
    hs: HealOpts,
    client_token: String,
    force_start: bool,
    force_stop: bool,
    /// Incremental result cursor (HS-06): only result items with a sequence
    /// greater than this are returned; absent means full snapshot.
    since_seq: Option<u64>,
}

fn extract_heal_init_params(body: &Bytes, uri: &Uri, params: Params<'_, '_>) -> S3Result<HealInitParams> {
    let mut hip = HealInitParams {
        bucket: params.get("bucket").map(|s| s.to_string()).unwrap_or_default(),
        obj_prefix: params.get("prefix").map(|s| s.to_string()).unwrap_or_default(),
        ..Default::default()
    };
    validate_heal_target(&hip.bucket, &hip.obj_prefix)?;

    if let Some(query) = uri.query() {
        let mut seen = HashSet::with_capacity(3);
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "clientToken" => {
                    if !seen.insert("clientToken") {
                        return Err(s3_error!(InvalidArgument, "duplicate heal query parameter"));
                    }
                    hip.client_token = value.into_owned();
                }
                "forceStart" => {
                    if !seen.insert("forceStart") {
                        return Err(s3_error!(InvalidArgument, "duplicate heal query parameter"));
                    }
                    hip.force_start = parse_heal_query_bool(value.as_ref())?;
                }
                "forceStop" => {
                    if !seen.insert("forceStop") {
                        return Err(s3_error!(InvalidArgument, "duplicate heal query parameter"));
                    }
                    hip.force_stop = parse_heal_query_bool(value.as_ref())?;
                }
                "sinceSeq" => {
                    if !seen.insert("sinceSeq") {
                        return Err(s3_error!(InvalidArgument, "duplicate heal query parameter"));
                    }
                    hip.since_seq = Some(
                        value
                            .parse::<u64>()
                            .map_err(|_| s3_error!(InvalidArgument, "sinceSeq must be a non-negative integer"))?,
                    );
                }
                _ => return Err(s3_error!(InvalidArgument, "unknown heal query parameter")),
            }
        }
    }

    if hip.force_start && (hip.force_stop || !hip.client_token.is_empty()) {
        return Err(s3_error!(
            InvalidRequest,
            "invalid combination of clientToken, forceStart, and forceStop parameters"
        ));
    }

    if hip.client_token.is_empty() {
        hip.hs = serde_json::from_slice(body).map_err(|e| {
            warn!(
                event = EVENT_ADMIN_REQUEST_REJECTED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = "heal_request_init",
                result = "rejected",
                reason = "request_body_parse_failed",
                error = ?e,
                "admin request rejected"
            );
            s3_error!(InvalidRequest, "err request body parse")
        })?;
    }

    Ok(hip)
}

fn parse_heal_query_bool(value: &str) -> S3Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(s3_error!(InvalidArgument, "invalid heal query boolean")),
    }
}

fn validate_heal_target(bucket: &str, obj_prefix: &str) -> S3Result<()> {
    if bucket.is_empty() && !obj_prefix.is_empty() {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if !bucket.is_empty() && is_reserved_or_invalid_bucket(bucket, false) {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if !is_valid_object_prefix(obj_prefix) {
        return Err(s3_error!(InvalidRequest, "invalid object name"));
    }

    Ok(())
}

fn encode_heal_control_path(bucket: &str, obj_prefix: &str) -> String {
    if bucket.is_empty() && obj_prefix.is_empty() {
        return String::new();
    }

    path_join(&[PathBuf::from(bucket), PathBuf::from(obj_prefix)])
        .to_string_lossy()
        .into_owned()
}

fn heal_control_response_id(heal_path: &str, client_token: &str) -> String {
    if !client_token.is_empty() {
        return client_token.to_string();
    }
    if heal_path.is_empty() {
        return LEGACY_ROOT_HEAL_RESPONSE_ID.to_string();
    }
    heal_path.to_string()
}

pub fn register_heal_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // Some APIs are only available in EC mode
    // if is_dist_erasure().await || is_erasure().await {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/").as_str(),
        AdminOperation(&HealHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/{bucket}").as_str(),
        AdminOperation(&HealHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/{bucket}/{prefix}").as_str(),
        AdminOperation(&HealHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/background-heal/status").as_str(),
        AdminOperation(&BackgroundHealStatusHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, REPLACEMENT_RECOVERY_STATUS_ROUTE_SUFFIX).as_str(),
        AdminOperation(&ReplacementRecoveryStatusHandler {}),
    )?;

    Ok(())
}

#[cfg(test)]
#[derive(Default)]
struct HealResp {
    resp_bytes: Vec<u8>,
    api_err: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealStartSuccess {
    client_token: String,
    client_address: String,
    start_time: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealTaskStatus {
    summary: String,
    #[serde(rename = "detail")]
    failure_detail: String,
    start_time: String,
    #[serde(rename = "settings")]
    heal_settings: HealOpts,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    items: Vec<rustfs_madmin::heal_commands::HealResultItem>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BackgroundHealStatus<'a> {
    #[serde(flatten)]
    info: &'a BackgroundHealInfo,
    state: HealRuntimeState,
    heal_queue_length: u64,
    heal_active_tasks: u64,
    heal_operations: rustfs_heal::HealOperationsSnapshot,
    cluster_status_complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<BackgroundHealProgress>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum HealRuntimeState {
    Disabled,
    Uninitialized,
    Idle,
    Active,
    /// One or more nodes were unreachable and the reachable ones report no
    /// active work, so no definitive cluster verdict exists. Distinct from
    /// `Idle` on purpose: an unreachable peer might be healing, and a partial
    /// answer must not read as "nothing is running".
    Degraded,
}

#[cfg(test)]
fn background_heal_runtime_state(
    services_enabled: bool,
    initialized: bool,
    operations: &rustfs_heal::HealOperationsSnapshot,
) -> HealRuntimeState {
    if !services_enabled {
        return HealRuntimeState::Disabled;
    }
    if !initialized {
        return HealRuntimeState::Uninitialized;
    }
    if operations.queue_length > 0 || operations.active_tasks > 0 || operations.retrying_tasks > 0 {
        HealRuntimeState::Active
    } else {
        HealRuntimeState::Idle
    }
}

type BackgroundHealProgress = rustfs_heal::HealProgress;

#[derive(Debug)]
struct ClusterHealStatusSnapshot {
    info: BackgroundHealInfo,
    state: HealRuntimeState,
    operations: rustfs_heal::HealOperationsSnapshot,
    progress: Option<BackgroundHealProgress>,
    complete: bool,
}

fn add_priority_counts(total: &mut rustfs_heal::HealPriorityCounts, next: rustfs_heal::HealPriorityCounts) {
    total.low = total.low.saturating_add(next.low);
    total.normal = total.normal.saturating_add(next.normal);
    total.high = total.high.saturating_add(next.high);
    total.urgent = total.urgent.saturating_add(next.urgent);
}

fn add_source_counts(total: &mut rustfs_heal::HealSourceCounts, next: rustfs_heal::HealSourceCounts) {
    total.scanner = total.scanner.saturating_add(next.scanner);
    total.admin = total.admin.saturating_add(next.admin);
    total.auto_heal = total.auto_heal.saturating_add(next.auto_heal);
    total.internal = total.internal.saturating_add(next.internal);
    total.read_repair = total.read_repair.saturating_add(next.read_repair);
    total.mrf = total.mrf.saturating_add(next.mrf);
}

fn add_operations(total: &mut rustfs_heal::HealOperationsSnapshot, next: rustfs_heal::HealOperationsSnapshot) {
    total.queue_length = total.queue_length.saturating_add(next.queue_length);
    total.active_tasks = total.active_tasks.saturating_add(next.active_tasks);
    total.retrying_tasks = total.retrying_tasks.saturating_add(next.retrying_tasks);
    add_priority_counts(&mut total.queued_by_priority, next.queued_by_priority);
    add_priority_counts(&mut total.active_by_priority, next.active_by_priority);
    add_priority_counts(&mut total.retrying_by_priority, next.retrying_by_priority);
    add_source_counts(&mut total.queued_by_source, next.queued_by_source);
    add_source_counts(&mut total.active_by_source, next.active_by_source);
    add_source_counts(&mut total.retrying_by_source, next.retrying_by_source);
}

fn aggregate_cluster_heal_status(snapshots: Vec<NodeHealStatusSnapshot>) -> ClusterHealStatusSnapshot {
    let mut info = BackgroundHealInfo::default();
    let mut operations = rustfs_heal::HealOperationsSnapshot::default();
    let mut progress = Vec::new();
    let mut any_services_enabled = false;
    let mut any_initialized = false;

    for snapshot in snapshots {
        let snapshot_info = snapshot.info();
        any_services_enabled |= snapshot.services_enabled;
        any_initialized |= snapshot.initialized;
        info.bitrot_start_cycle = info.bitrot_start_cycle.max(snapshot_info.bitrot_start_cycle);
        info.bitrot_start_time = info.bitrot_start_time.max(snapshot_info.bitrot_start_time);
        if snapshot_info.current_scan_mode == HealScanMode::Deep
            || (snapshot_info.current_scan_mode == HealScanMode::Normal && info.current_scan_mode == HealScanMode::Unknown)
        {
            info.current_scan_mode = snapshot_info.current_scan_mode;
        }
        add_operations(&mut operations, snapshot.operations);
        if let Some(next) = snapshot.progress {
            progress.push(next);
        }
    }

    let progress = rustfs_heal::aggregate_heal_progress(progress);

    let state = if operations.queue_length > 0 || operations.active_tasks > 0 || operations.retrying_tasks > 0 {
        HealRuntimeState::Active
    } else if any_initialized {
        HealRuntimeState::Idle
    } else if any_services_enabled {
        HealRuntimeState::Uninitialized
    } else {
        HealRuntimeState::Disabled
    };

    ClusterHealStatusSnapshot {
        info,
        state,
        operations,
        progress,
        complete: true,
    }
}

fn cluster_heal_status_unavailable(reason: &str) -> s3s::S3Error {
    warn!(
        event = EVENT_ADMIN_REQUEST_FAILED,
        component = LOG_COMPONENT_ADMIN_API,
        subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
        operation = "background_heal_status",
        result = "failed",
        reason = reason,
        "cluster heal status unavailable"
    );
    s3_error!(InternalError, "cluster heal status unavailable")
}

fn peer_topology_complete(
    expected_nodes: usize,
    remote_slots: usize,
    unavailable_remote_slots: usize,
    all_slots: usize,
    available_all_slots: usize,
) -> bool {
    let expected_peers = expected_nodes.saturating_sub(1);
    remote_slots == expected_peers
        && unavailable_remote_slots == 0
        && all_slots == expected_nodes
        && available_all_slots == expected_peers
}

fn merge_peer_heal_statuses(
    mut snapshots: Vec<NodeHealStatusSnapshot>,
    peer_statuses: Vec<Result<Option<NodeHealStatusSnapshot>, String>>,
    expected_nodes: usize,
    topology_complete: bool,
) -> S3Result<ClusterHealStatusSnapshot> {
    for peer_status in peer_statuses {
        match peer_status {
            Ok(Some(snapshot)) => snapshots.push(snapshot),
            Ok(None) => {
                warn!(
                    event = EVENT_ADMIN_REQUEST_FAILED,
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                    operation = "background_heal_status",
                    result = "degraded",
                    reason = "peer_status_unsupported",
                    "cluster heal status is partial during rolling upgrade"
                );
            }
            Err(err) => {
                warn!(
                    event = EVENT_ADMIN_REQUEST_FAILED,
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                    operation = "background_heal_status",
                    result = "degraded",
                    reason = "peer_status_unavailable",
                    error = %err,
                    "cluster heal peer status unavailable"
                );
            }
        }
    }
    if snapshots.is_empty() {
        return Err(cluster_heal_status_unavailable("no_node_status_available"));
    }
    // Completeness requires every condition, not just the snapshot count: the
    // peer queries iterate `peer_clients` while the topology check also
    // examines `all_peer_clients`, and the two are populated independently —
    // so during a reconfiguration the count can equal `expected_nodes` while
    // the topology is known-incomplete. Counting alone would report a
    // definitive answer precisely when the membership itself is in doubt.
    let complete = topology_complete && snapshots.len() == expected_nodes;
    let mut status = aggregate_cluster_heal_status(snapshots);
    status.complete = complete;
    // A partial answer must never be mistakable for a definitive verdict: an
    // unreachable peer might be mid-heal, so reporting the reachable nodes'
    // "idle" (or disabled/uninitialized) as the cluster state would falsely
    // reassure. Known-active work is safe to report as-is — it cannot
    // reassure. Everything else degrades to an explicit Degraded state so the
    // caller still gets the reachable nodes' data (queue depths, progress,
    // bitrot cycle) with an honest label, instead of the previous cluster-wide
    // 500 that carried no information at all (issue #5850).
    if !complete && status.state != HealRuntimeState::Active {
        status.state = HealRuntimeState::Degraded;
    }
    Ok(status)
}

async fn query_peer_heal_status<E>(
    host: &str,
    request: impl std::future::Future<Output = Result<Option<Vec<u8>>, E>>,
    request_timeout: Duration,
) -> Result<Option<NodeHealStatusSnapshot>, String>
where
    E: std::fmt::Display,
{
    match timeout(request_timeout, request).await {
        Ok(Ok(Some(status))) => decode_node_heal_status(&status)
            .map(Some)
            .map_err(|err| format!("peer {host}: {err}")),
        Ok(Ok(None)) => Ok(None),
        Ok(Err(err)) => Err(format!("peer {host}: {err}")),
        Err(_) => Err(format!("peer {host}: heal status timed out")),
    }
}

async fn read_cluster_heal_status(
    local_info: BackgroundHealInfo,
    notification_system: Option<&crate::admin::storage_api::runtime_sources::NotificationSys>,
    expected_nodes: usize,
) -> S3Result<ClusterHealStatusSnapshot> {
    let snapshots = vec![capture_node_heal_status(local_info).await];
    if expected_nodes == 1 {
        return Ok(aggregate_cluster_heal_status(snapshots));
    }
    let Some(notification_system) = notification_system else {
        return Err(cluster_heal_status_unavailable("notification_system_unavailable"));
    };
    // An incomplete peer topology (a down member's client slot, a rolling
    // upgrade) previously failed the whole endpoint here, before any peer was
    // queried. Proceed instead: the per-peer query below reports unavailable
    // slots as errors, and merge_peer_heal_statuses folds them into an
    // explicit partial answer (issue #5850).
    let topology_complete = peer_topology_complete(
        expected_nodes,
        notification_system.peer_clients.len(),
        notification_system
            .peer_clients
            .iter()
            .filter(|client| client.is_none())
            .count(),
        notification_system.all_peer_clients.len(),
        notification_system
            .all_peer_clients
            .iter()
            .filter(|client| client.is_some())
            .count(),
    );
    if !topology_complete {
        warn!(
            event = EVENT_ADMIN_REQUEST_FAILED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = "background_heal_status",
            result = "degraded",
            reason = "peer_topology_incomplete",
            "cluster heal status will be partial"
        );
    }

    let peer_statuses = join_all(notification_system.peer_clients.iter().map(|client| async move {
        let Some(client) = client else {
            return Err("configured peer is unavailable".to_string());
        };
        let host = client.host.to_string();
        query_peer_heal_status(&host, client.background_heal_status(), PEER_HEAL_STATUS_TIMEOUT).await
    }))
    .await;

    merge_peer_heal_statuses(snapshots, peer_statuses, expected_nodes, topology_complete)
}

async fn query_peer_replacement_recovery_status<E>(
    host: &str,
    request: impl std::future::Future<Output = Result<Option<Vec<u8>>, E>>,
    request_timeout: Duration,
) -> Result<Option<rustfs_heal::ReplacementRecoverySnapshot>, String>
where
    E: std::fmt::Display,
{
    match timeout(request_timeout, request).await {
        Ok(Ok(Some(status))) => decode_node_replacement_recovery_status(&status)
            .map(|snapshot| Some(snapshot.snapshot))
            .map_err(|err| format!("peer {host}: {err}")),
        Ok(Ok(None)) => Ok(None),
        Ok(Err(err)) => Err(format!("peer {host}: {err}")),
        Err(_) => Err(format!("peer {host}: replacement recovery status timed out")),
    }
}

fn canonical_replacement_records(
    records: &[rustfs_heal::ReplacementRecoveryRecord],
) -> Vec<rustfs_heal::ReplacementRecoveryRecord> {
    let mut records = records.to_vec();
    for record in &mut records {
        record.target_slots.sort();
    }
    records.sort_by(|left, right| {
        (
            &left.task_id,
            &left.generation,
            &left.set_disk_id,
            left.state,
            &left.target_slots,
            &left.verified_at,
            &left.reason,
        )
            .cmp(&(
                &right.task_id,
                &right.generation,
                &right.set_disk_id,
                right.state,
                &right.target_slots,
                &right.verified_at,
                &right.reason,
            ))
    });
    records
}

fn merge_replacement_recovery_records(
    snapshots: &[rustfs_heal::ReplacementRecoverySnapshot],
) -> Vec<rustfs_heal::ReplacementRecoveryRecord> {
    let mut records = BTreeMap::<String, rustfs_heal::ReplacementRecoveryRecord>::new();
    for snapshot in snapshots {
        for record in canonical_replacement_records(&snapshot.records) {
            match records.entry(record.task_id.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(record);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &record => {}
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let task_id = entry.key().clone();
                    entry.insert(rustfs_heal::ReplacementRecoveryRecord {
                        task_id,
                        state: rustfs_heal::ReplacementRecoveryState::Unknown,
                        generation: None,
                        set_disk_id: None,
                        target_slots: Vec::new(),
                        reason: Some("peer replacement recovery records disagree".to_string()),
                        verified_at: None,
                    });
                }
            }
        }
    }
    records.into_values().collect()
}

fn aggregate_replacement_recovery_cluster_status(
    mut snapshots: Vec<rustfs_heal::ReplacementRecoverySnapshot>,
    peer_statuses: Vec<Result<Option<rustfs_heal::ReplacementRecoverySnapshot>, String>>,
    expected_nodes: usize,
    topology_complete: bool,
) -> ReplacementRecoveryClusterStatus {
    let mut reason = None::<&'static str>;
    if !topology_complete {
        reason = Some("peer_topology_incomplete");
    }

    for status in peer_statuses {
        match status {
            Ok(Some(snapshot)) => snapshots.push(snapshot),
            Ok(None) => {
                reason.get_or_insert("peer_replacement_recovery_status_unsupported");
            }
            Err(_) => {
                reason.get_or_insert("peer_replacement_recovery_status_unavailable");
            }
        }
    }

    if snapshots.len() != expected_nodes {
        reason.get_or_insert("peer_replacement_recovery_status_incomplete");
    }
    if snapshots.iter().any(|snapshot| !snapshot.definitive) {
        reason.get_or_insert("peer_replacement_recovery_status_not_definitive");
    }

    let first_records = snapshots
        .first()
        .map(|snapshot| canonical_replacement_records(&snapshot.records));
    if let Some(first_records) = &first_records
        && snapshots
            .iter()
            .skip(1)
            .any(|snapshot| canonical_replacement_records(&snapshot.records) != *first_records)
    {
        reason.get_or_insert("peer_replacement_recovery_status_conflict");
    }

    let records = merge_replacement_recovery_records(&snapshots);
    if records
        .iter()
        .any(|record| matches!(record.state, rustfs_heal::ReplacementRecoveryState::Unknown))
    {
        reason.get_or_insert("peer_replacement_recovery_status_conflict");
    }

    ReplacementRecoveryClusterStatus {
        definitive: reason.is_none(),
        reason: reason.map(str::to_string),
        records,
    }
}

async fn read_cluster_replacement_recovery_status(
    local: rustfs_heal::ReplacementRecoverySnapshot,
    notification_system: Option<&crate::admin::storage_api::runtime_sources::NotificationSys>,
    expected_nodes: usize,
) -> S3Result<ReplacementRecoveryClusterStatus> {
    if expected_nodes == 1 {
        return Ok(aggregate_replacement_recovery_cluster_status(
            vec![local],
            Vec::new(),
            expected_nodes,
            true,
        ));
    }
    let Some(notification_system) = notification_system else {
        return Ok(ReplacementRecoveryClusterStatus {
            definitive: false,
            reason: Some("notification_system_unavailable".to_string()),
            records: local.records,
        });
    };
    let topology_complete = peer_topology_complete(
        expected_nodes,
        notification_system.peer_clients.len(),
        notification_system
            .peer_clients
            .iter()
            .filter(|client| client.is_none())
            .count(),
        notification_system.all_peer_clients.len(),
        notification_system
            .all_peer_clients
            .iter()
            .filter(|client| client.is_some())
            .count(),
    );
    if !topology_complete {
        warn!(
            event = EVENT_ADMIN_REQUEST_FAILED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = "replacement_recovery_status",
            result = "degraded",
            reason = "peer_topology_incomplete",
            "cluster replacement recovery status will be partial"
        );
    }

    let peer_statuses = join_all(notification_system.peer_clients.iter().map(|client| async move {
        let Some(client) = client else {
            return Err("configured peer is unavailable".to_string());
        };
        let host = client.host.to_string();
        query_peer_replacement_recovery_status(&host, client.replacement_recovery_status(), PEER_HEAL_STATUS_TIMEOUT).await
    }))
    .await;

    Ok(aggregate_replacement_recovery_cluster_status(
        vec![local],
        peer_statuses,
        expected_nodes,
        topology_complete,
    ))
}

fn cluster_heal_control_unavailable(reason: &str) -> s3s::S3Error {
    warn!(
        event = EVENT_ADMIN_REQUEST_FAILED,
        component = LOG_COMPONENT_ADMIN_API,
        subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
        operation = "heal_control",
        result = "failed",
        reason,
        "cluster heal coordination unavailable"
    );
    s3_error!(InternalError, "cluster heal coordination unavailable")
}

struct PreparedHealControlRoute {
    remote_grid_hosts: Vec<String>,
    fingerprint: String,
    coordinator_epoch: u64,
    coordinator: HealControlCoordinator,
}

fn prepare_heal_control_route(context: &crate::admin::runtime_sources::AppContext) -> S3Result<PreparedHealControlRoute> {
    let endpoints = context
        .endpoints()
        .handle()
        .ok_or_else(|| cluster_heal_control_unavailable("endpoint_topology_unavailable"))?;
    let fingerprint = heal_topology_fingerprint(&endpoints)
        .map_err(|_| cluster_heal_control_unavailable("topology_fingerprint_unavailable"))?;
    let coordinator_epoch = rustfs_protos::heal_control_coordinator_epoch(&fingerprint)
        .map_err(|_| cluster_heal_control_unavailable("topology_epoch_unavailable"))?;
    let coordinator =
        heal_control_coordinator(&endpoints).map_err(|_| cluster_heal_control_unavailable("coordinator_unavailable"))?;
    let remote_grid_hosts = endpoints
        .get_nodes()
        .into_iter()
        .filter(|node| !node.is_local)
        .map(|node| node.grid_host)
        .collect();
    Ok(PreparedHealControlRoute {
        remote_grid_hosts,
        fingerprint,
        coordinator_epoch,
        coordinator,
    })
}

fn new_heal_control_metadata(route: &PreparedHealControlRoute) -> S3Result<rustfs_protos::heal_control::RequestMetadata> {
    let now = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    let now = i64::try_from(now).map_err(|_| cluster_heal_control_unavailable("clock_out_of_range"))?;
    let lifetime_ms = i64::try_from(rustfs_protos::heal_control_execution_timeout().as_millis())
        .map_err(|_| cluster_heal_control_unavailable("execution_timeout_out_of_range"))?;
    Ok(rustfs_protos::heal_control::RequestMetadata::new(
        *uuid::Uuid::new_v4().as_bytes(),
        now,
        now.saturating_add(lifetime_ms),
        route.coordinator_epoch,
    ))
}

async fn require_cluster_heal_control_capability(
    context: &crate::admin::runtime_sources::AppContext,
    route: &PreparedHealControlRoute,
) -> S3Result<()> {
    if route.remote_grid_hosts.is_empty() {
        return Ok(());
    }
    let notification_system = context
        .notification_system()
        .handle()
        .ok_or_else(|| cluster_heal_control_unavailable("notification_system_unavailable"))?;
    let clients = route
        .remote_grid_hosts
        .iter()
        .map(|grid_host| {
            notification_system
                .peer_client_for_grid_host(grid_host)
                .ok_or_else(|| cluster_heal_control_unavailable("peer_capability_client_unavailable"))
        })
        .collect::<S3Result<Vec<_>>>()?;
    let results = join_all(clients.into_iter().map(|client| {
        let fingerprint = route.fingerprint.clone();
        async move { client.probe_heal_control(fingerprint).await }
    }))
    .await;
    if let Some(err) = results.into_iter().find_map(Result::err) {
        warn!(
            event = EVENT_ADMIN_REQUEST_FAILED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = "heal_control",
            result = "failed",
            reason = "cluster_capability_unavailable",
            error = %err,
            "cluster heal coordination failed"
        );
        return Err(cluster_heal_control_unavailable("cluster_capability_unavailable"));
    }
    Ok(())
}

async fn route_cluster_heal_control(
    context: &crate::admin::runtime_sources::AppContext,
    route: &PreparedHealControlRoute,
    envelope: rustfs_protos::heal_control::Envelope,
    request_id: &str,
    coordinator_capability_verified: bool,
) -> S3Result<rustfs_protos::heal_control::Outcome> {
    let response = if route.coordinator.is_local {
        crate::storage::rpc::node_service::execute_heal_control_envelope(envelope, route.coordinator_epoch)
            .await
            .map_err(|err| {
                warn!(
                    event = EVENT_ADMIN_REQUEST_FAILED,
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                    operation = "heal_control",
                    result = "failed",
                    reason = "local_coordinator_failed",
                    error = %err,
                    "cluster heal coordination failed"
                );
                cluster_heal_control_unavailable("local_coordinator_failed")
            })?
    } else {
        let notification_system = context
            .notification_system()
            .handle()
            .ok_or_else(|| cluster_heal_control_unavailable("notification_system_unavailable"))?;
        let client = notification_system
            .peer_client_for_grid_host(&route.coordinator.grid_host)
            .ok_or_else(|| cluster_heal_control_unavailable("coordinator_client_unavailable"))?;
        if !coordinator_capability_verified {
            client.probe_heal_control(route.fingerprint.clone()).await.map_err(|err| {
                warn!(
                    event = EVENT_ADMIN_REQUEST_FAILED,
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                    operation = "heal_control",
                    result = "failed",
                    reason = "coordinator_capability_unavailable",
                    error = %err,
                    "cluster heal coordination failed"
                );
                cluster_heal_control_unavailable("coordinator_capability_unavailable")
            })?;
        }
        let command = rustfs_protos::heal_control::encode_envelope(&envelope)
            .map_err(|err| s3_error!(InternalError, "encode heal control request failed: {err}"))?;
        client
            .heal_control(rustfs_protos::HEAL_CONTROL_PROTOCOL_VERSION, route.fingerprint.clone(), command)
            .await
            .map_err(|err| {
                warn!(
                    event = EVENT_ADMIN_REQUEST_FAILED,
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                    operation = "heal_control",
                    result = "failed",
                    reason = "coordinator_request_failed",
                    error = %err,
                    "cluster heal coordination failed"
                );
                cluster_heal_control_unavailable("coordinator_request_failed")
            })?
    };
    rustfs_protos::heal_control::decode_result(&response)
        .and_then(|result| result.into_outcome(request_id, route.coordinator_epoch))
        .map_err(|err| {
            warn!(
                event = EVENT_ADMIN_REQUEST_FAILED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = "heal_control",
                result = "failed",
                reason = "coordinator_response_invalid",
                error = %err,
                "cluster heal coordination failed"
            );
            cluster_heal_control_unavailable("coordinator_response_invalid")
        })
}

async fn execute_after_heal_control_capability<P, PF, E, EF, T>(probe: P, execute: E) -> S3Result<T>
where
    P: FnOnce() -> PF,
    PF: Future<Output = S3Result<()>>,
    E: FnOnce() -> EF,
    EF: Future<Output = S3Result<T>>,
{
    probe().await?;
    execute().await
}

async fn submit_cluster_heal_start(
    context: Arc<crate::admin::runtime_sources::AppContext>,
    hip: &HealInitParams,
) -> S3Result<HealAdmissionReceipt> {
    let route = prepare_heal_control_route(&context)?;
    let result = execute_after_heal_control_capability(
        || require_cluster_heal_control_capability(&context, &route),
        || async {
            let heal_request = build_heal_channel_request(hip);
            let request_id = heal_request.id.clone();
            let envelope = rustfs_protos::heal_control::Envelope::start(heal_request, new_heal_control_metadata(&route)?)
                .map_err(|err| s3_error!(InternalError, "encode heal control request failed: {err}"))?;
            route_cluster_heal_control(&context, &route, envelope, &request_id, true).await
        },
    )
    .await?;
    match result {
        rustfs_protos::heal_control::Outcome::Start { task_id, admission } => Ok(HealAdmissionReceipt {
            result: admission.into_heal_admission_result(),
            task_id,
        }),
        rustfs_protos::heal_control::Outcome::Channel { .. } => {
            Err(s3_error!(InternalError, "coordinator returned an unexpected heal result"))
        }
    }
}

fn reject_heal_admission(result: rustfs_common::heal_channel::HealAdmissionResult) -> s3s::S3Error {
    use rustfs_common::heal_channel::{HealAdmissionDropReason, HealAdmissionResult};

    match result {
        HealAdmissionResult::Full | HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull) => s3_error!(
            SlowDown,
            "heal request not admitted: admission={}, reason={}",
            result.result_label(),
            result.reason_label()
        ),
        // Overlap rejections (HS-06) share this arm: the s3s footprint
        // ratchet forbids new s3_error! sites, and the typed reason is
        // preserved through reason_label() ("already_running" /
        // "overlapping_paths") so madmin-style clients can distinguish.
        HealAdmissionResult::Dropped(
            HealAdmissionDropReason::PolicyDropped
            | HealAdmissionDropReason::AlreadyRunning
            | HealAdmissionDropReason::OverlappingPaths,
        ) => s3_error!(
            OperationAborted,
            "heal request not admitted: admission={}, reason={}",
            result.result_label(),
            result.reason_label()
        ),
        HealAdmissionResult::Accepted | HealAdmissionResult::Merged => {
            s3_error!(InternalError, "admitted heal request was rejected unexpectedly")
        }
    }
}

async fn submit_cluster_heal_channel_command(
    context: Arc<crate::admin::runtime_sources::AppContext>,
    route: PreparedHealControlRoute,
    envelope: rustfs_protos::heal_control::Envelope,
    request_id: &str,
    response_id: String,
) -> S3Result<rustfs_common::heal_channel::HealChannelResponse> {
    match route_cluster_heal_control(&context, &route, envelope, request_id, false).await? {
        rustfs_protos::heal_control::Outcome::Channel { success, data, error } => {
            Ok(rustfs_common::heal_channel::HealChannelResponse {
                request_id: response_id,
                success,
                data,
                error,
            })
        }
        rustfs_protos::heal_control::Outcome::Start { .. } => {
            Err(s3_error!(InternalError, "coordinator returned an unexpected heal result"))
        }
    }
}

#[derive(Debug, Deserialize)]
struct HealTaskStatusPayload {
    summary: String,
    #[serde(default)]
    items: Vec<rustfs_madmin::heal_commands::HealResultItem>,
    #[serde(default)]
    truncated: bool,
    #[serde(default)]
    progress: Option<serde_json::Value>,
}

#[cfg(test)]
fn map_heal_response(result: Option<HealResp>) -> S3Result<(StatusCode, Vec<u8>)> {
    match result {
        Some(result) => {
            if let Some(err) = result.api_err {
                return Err(s3_error!(InternalError, "{err}"));
            }

            if result.resp_bytes.is_empty() {
                return Err(s3_error!(InternalError, "heal response body is empty"));
            }

            Ok((StatusCode::OK, result.resp_bytes))
        }
        None => Err(s3_error!(InternalError, "heal channel closed unexpectedly")),
    }
}

fn current_rfc3339_time() -> S3Result<String> {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|e| s3_error!(InternalError, "failed to format heal timestamp: {e}"))
}

fn encode_json<T: Serialize>(value: &T) -> S3Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| s3_error!(InternalError, "failed to serialize heal response: {e}"))
}

fn encode_heal_start_success(client_token: String, client_address: String) -> S3Result<Vec<u8>> {
    encode_json(&HealStartSuccess {
        client_token,
        client_address,
        start_time: current_rfc3339_time()?,
    })
}

fn encode_heal_task_status(
    summary: String,
    failure_detail: String,
    heal_settings: HealOpts,
    items: Vec<rustfs_madmin::heal_commands::HealResultItem>,
    truncated: bool,
    progress: Option<serde_json::Value>,
) -> S3Result<Vec<u8>> {
    encode_json(&HealTaskStatus {
        summary,
        failure_detail,
        start_time: current_rfc3339_time()?,
        heal_settings,
        items,
        truncated,
        progress,
    })
}

fn build_heal_channel_request(hip: &HealInitParams) -> HealChannelRequest {
    let root_erasure_set_target =
        hip.bucket.is_empty() && hip.obj_prefix.is_empty() && matches!((hip.hs.pool, hip.hs.set), (Some(_), Some(_)));
    let root_cluster_target = hip.bucket.is_empty() && hip.obj_prefix.is_empty() && hip.hs.pool.is_none() && hip.hs.set.is_none();
    let recursive = if (!hip.bucket.is_empty() && hip.obj_prefix.is_empty()) || root_erasure_set_target {
        true
    } else {
        hip.hs.recursive
    };
    let mut heal_request = rustfs_common::heal_channel::create_heal_request(
        hip.bucket.clone(),
        if hip.obj_prefix.is_empty() {
            None
        } else {
            Some(hip.obj_prefix.clone())
        },
        hip.force_start,
        Some(HealChannelPriority::High),
    );

    heal_request.pool_index = hip.hs.pool;
    heal_request.set_index = hip.hs.set;
    if root_erasure_set_target && let (Some(pool), Some(set)) = (hip.hs.pool, hip.hs.set) {
        heal_request.disk = Some(format_set_disk_id(pool, set));
    }
    heal_request.scan_mode = Some(hip.hs.scan_mode);
    heal_request.remove_corrupted = Some(hip.hs.remove);
    heal_request.recreate_missing = Some(hip.hs.recreate);
    heal_request.update_parity = Some(hip.hs.update_parity);
    heal_request.recursive = Some(recursive || root_cluster_target);
    heal_request.dry_run = Some(hip.hs.dry_run);
    heal_request.no_lock = Some(hip.hs.no_lock);
    heal_request.source = HealRequestSource::Admin;
    heal_request
}

fn heal_channel_response_status(
    response: &rustfs_common::heal_channel::HealChannelResponse,
) -> (String, Vec<rustfs_madmin::heal_commands::HealResultItem>, bool, Option<serde_json::Value>) {
    let Some(data) = response.data.as_deref() else {
        return ("running".to_string(), Vec::new(), false, None);
    };

    if let Ok(payload) = serde_json::from_slice::<HealTaskStatusPayload>(data)
        && !payload.summary.is_empty()
    {
        return (payload.summary, payload.items, payload.truncated, payload.progress);
    }

    let summary = std::str::from_utf8(data)
        .ok()
        .filter(|summary| !summary.is_empty())
        .unwrap_or("running")
        .to_string();
    (summary, Vec::new(), false, None)
}

#[cfg(test)]
fn heal_channel_response_summary(response: &rustfs_common::heal_channel::HealChannelResponse) -> String {
    heal_channel_response_status(response).0
}

#[cfg(test)]
fn heal_channel_response_items(
    response: &rustfs_common::heal_channel::HealChannelResponse,
) -> Vec<rustfs_madmin::heal_commands::HealResultItem> {
    heal_channel_response_status(response).1
}

#[cfg(test)]
fn heal_channel_response_progress(response: &rustfs_common::heal_channel::HealChannelResponse) -> Option<serde_json::Value> {
    heal_channel_response_status(response).3
}

fn encode_background_heal_status(
    info: &BackgroundHealInfo,
    state: HealRuntimeState,
    heal_operations: rustfs_heal::HealOperationsSnapshot,
    progress: Option<BackgroundHealProgress>,
    cluster_status_complete: bool,
) -> S3Result<Vec<u8>> {
    let status = BackgroundHealStatus {
        info,
        state,
        heal_queue_length: heal_operations.queue_length,
        heal_active_tasks: heal_operations.active_tasks,
        heal_operations,
        cluster_status_complete,
        progress,
    };
    serde_json::to_vec(&status).map_err(|e| {
        warn!(
            event = EVENT_ADMIN_REQUEST_FAILED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = "background_heal_status",
            result = "failed",
            reason = "serialize_background_heal_status_failed",
            error = %e,
            "admin request failed"
        );
        s3_error!(InternalError, "failed to serialize background heal status: {e}")
    })
}

fn validate_heal_request_mode(hip: &HealInitParams) -> S3Result<()> {
    if hip.bucket.is_empty() && hip.client_token.is_empty() && !hip.force_stop {
        return match (hip.hs.pool, hip.hs.set) {
            (Some(_), Some(_)) => Ok(()),
            (Some(_), None) | (None, Some(_)) => {
                Err(s3_error!(InvalidRequest, "root heal erasure-set target requires both pool and set"))
            }
            (None, None) if hip.hs.recursive => Ok(()),
            (None, None) => Err(s3_error!(InvalidRequest, "root heal requires recursive=true or a bucket target")),
        };
    }

    Ok(())
}

fn json_response(status: StatusCode, body: Vec<u8>) -> S3Response<(StatusCode, Body)> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Ok(value) = HeaderValue::from_str(&body.len().to_string()) {
        headers.insert(CONTENT_LENGTH, value);
    }
    S3Response::with_headers((status, Body::from(body)), headers)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReplacementRecoveryStatusResponse {
    pub contract_version: u32,
    pub path: String,
    pub scope: &'static str,
    pub local: rustfs_heal::ReplacementRecoverySnapshot,
    pub cluster: ReplacementRecoveryClusterStatus,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReplacementRecoveryClusterStatus {
    pub definitive: bool,
    pub reason: Option<String>,
    pub records: Vec<rustfs_heal::ReplacementRecoveryRecord>,
}

fn build_replacement_recovery_status_response(
    local: rustfs_heal::ReplacementRecoverySnapshot,
    cluster: ReplacementRecoveryClusterStatus,
) -> ReplacementRecoveryStatusResponse {
    ReplacementRecoveryStatusResponse {
        contract_version: REPLACEMENT_RECOVERY_STATUS_CONTRACT_VERSION,
        path: format!("{}{}", ADMIN_PREFIX, REPLACEMENT_RECOVERY_STATUS_ROUTE_SUFFIX),
        scope: "cluster_survivor_disks",
        local,
        cluster,
    }
}

fn encode_replacement_recovery_status(response: &ReplacementRecoveryStatusResponse) -> S3Result<Vec<u8>> {
    serde_json::to_vec(response).map_err(|e| {
        warn!(
            event = EVENT_ADMIN_REQUEST_FAILED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = "replacement_recovery_status",
            result = "failed",
            reason = "serialize_replacement_recovery_status_failed",
            error = %e,
            "admin request failed"
        );
        s3_error!(InternalError, "failed to serialize replacement recovery status: {e}")
    })
}

async fn validate_heal_admin_request(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) = authenticate_request(&req.headers, &req.uri, input_cred).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::HealAdminAction)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

pub struct HealHandler {}

#[async_trait::async_trait]
impl Operation for HealHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_heal_admin_request(&req).await?;
        let client_address = req
            .extensions
            .get::<Option<RemoteAddr>>()
            .and_then(|opt| opt.map(|addr| addr.0.to_string()))
            .unwrap_or_default();
        let app_context = app_context_from_req(&req);
        let mut input = req.input;
        let bytes = match input.store_all_limited(MAX_HEAL_REQUEST_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    event = EVENT_ADMIN_REQUEST_REJECTED,
                    component = LOG_COMPONENT_ADMIN_API,
                    subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                    operation = "heal_request_body",
                    result = "rejected",
                    reason = "request_body_read_failed",
                    error = ?e,
                    "admin request rejected"
                );
                return Err(s3_error!(InvalidRequest, "heal request body too large or failed to read"));
            }
        };
        let hip = extract_heal_init_params(&bytes, &req.uri, params)?;
        validate_heal_request_mode(&hip)?;
        let response_operation = if hip.force_stop {
            "cancel_heal"
        } else if !hip.client_token.is_empty() && !hip.force_start {
            "query_heal_status"
        } else {
            "start_heal"
        };

        let heal_path = encode_heal_control_path(&hip.bucket, &hip.obj_prefix);
        if !hip.client_token.is_empty() && !hip.force_start && !hip.force_stop {
            let client_token = hip.client_token.clone();
            let request_id = uuid::Uuid::new_v4().to_string();
            let context = app_context
                .clone()
                .ok_or_else(|| cluster_heal_control_unavailable("app_context_unavailable"))?;
            let route = prepare_heal_control_route(&context)?;
            let envelope = rustfs_protos::heal_control::Envelope::query(
                request_id.clone(),
                new_heal_control_metadata(&route)?,
                heal_path,
                client_token.clone(),
                hip.since_seq,
            )
            .map_err(|err| s3_error!(InternalError, "encode heal control query failed: {err}"))?;
            let response = submit_cluster_heal_channel_command(context, route, envelope, &request_id, client_token).await?;
            if !response.success {
                return Err(s3_error!(
                    InternalError,
                    "{}",
                    response.error.unwrap_or_else(|| "query heal status failed".to_string())
                ));
            }
            let (summary, items, truncated, progress) = heal_channel_response_status(&response);
            let body = encode_heal_task_status(
                summary,
                response.error.unwrap_or_default(),
                HealOpts::default(),
                items,
                truncated,
                progress,
            )?;
            info!(
                event = EVENT_ADMIN_RESPONSE_EMITTED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = response_operation,
                result = "success",
                status_code = StatusCode::OK.as_u16(),
                "admin response emitted"
            );
            return Ok(json_response(StatusCode::OK, body));
        } else if hip.force_stop {
            let client_token = hip.client_token.clone();
            let request_id = uuid::Uuid::new_v4().to_string();
            let context = app_context
                .clone()
                .ok_or_else(|| cluster_heal_control_unavailable("app_context_unavailable"))?;
            let route = prepare_heal_control_route(&context)?;
            let envelope = rustfs_protos::heal_control::Envelope::cancel(
                request_id.clone(),
                new_heal_control_metadata(&route)?,
                heal_path.clone(),
                client_token.clone(),
            )
            .map_err(|err| s3_error!(InternalError, "encode heal control cancel failed: {err}"))?;
            let response_id = heal_control_response_id(&heal_path, &client_token);
            let response = submit_cluster_heal_channel_command(context, route, envelope, &request_id, response_id).await?;
            if !response.success {
                return Err(s3_error!(
                    InternalError,
                    "{}",
                    response.error.unwrap_or_else(|| "cancel heal task failed".to_string())
                ));
            }
            let body = if client_token.is_empty() {
                encode_heal_start_success(response.request_id, client_address)?
            } else {
                let (summary, items, truncated, progress) = heal_channel_response_status(&response);
                encode_heal_task_status(summary, response.error.unwrap_or_default(), hip.hs, items, truncated, progress)?
            };
            info!(
                event = EVENT_ADMIN_RESPONSE_EMITTED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = response_operation,
                result = "success",
                status_code = StatusCode::OK.as_u16(),
                "admin response emitted"
            );
            return Ok(json_response(StatusCode::OK, body));
        } else if hip.client_token.is_empty() {
            let Some(context) = app_context else {
                return Err(cluster_heal_control_unavailable("app_context_unavailable"));
            };
            let receipt = submit_cluster_heal_start(context, &hip).await?;
            if !receipt.result.is_admitted() {
                return Err(reject_heal_admission(receipt.result));
            }
            let body = encode_heal_start_success(receipt.task_id, client_address)?;
            info!(
                event = EVENT_ADMIN_RESPONSE_EMITTED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = response_operation,
                result = "success",
                status_code = StatusCode::OK.as_u16(),
                "admin response emitted"
            );
            return Ok(json_response(StatusCode::OK, body));
        }

        Err(s3_error!(InvalidRequest, "invalid heal control request"))
    }
}

pub struct BackgroundHealStatusHandler {}

#[async_trait::async_trait]
impl Operation for BackgroundHealStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_heal_admin_request(&req).await?;

        let Some(context) = app_context_from_req(&req) else {
            warn!(
                event = EVENT_ADMIN_REQUEST_FAILED,
                component = LOG_COMPONENT_ADMIN_API,
                subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
                operation = "background_heal_status",
                result = "failed",
                reason = "server_not_initialized",
                "admin request failed"
            );
            return Err(s3_error!(InternalError, "server not initialized"));
        };
        let store = context.object_store();
        let Some(endpoints) = context.endpoints().handle() else {
            return Err(cluster_heal_status_unavailable("endpoint_topology_unavailable"));
        };
        let expected_nodes = endpoints.get_nodes().len();
        if expected_nodes == 0 {
            return Err(cluster_heal_status_unavailable("endpoint_topology_empty"));
        }
        let notification_system = context.notification_system().handle();

        let local_info = read_background_heal_info(store).await;
        let cluster_status = read_cluster_heal_status(local_info, notification_system.as_deref(), expected_nodes).await?;
        let body = encode_background_heal_status(
            &cluster_status.info,
            cluster_status.state,
            cluster_status.operations,
            cluster_status.progress,
            cluster_status.complete,
        )?;
        info!(
            event = EVENT_ADMIN_RESPONSE_EMITTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = "background_heal_status",
            result = "success",
            "admin response emitted"
        );

        Ok(json_response(StatusCode::OK, body))
    }
}

pub struct ReplacementRecoveryStatusHandler {}

#[async_trait::async_trait]
impl Operation for ReplacementRecoveryStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_heal_admin_request(&req).await?;

        let Some(context) = app_context_from_req(&req) else {
            return Err(cluster_heal_status_unavailable("server_not_initialized"));
        };
        let Some(endpoints) = context.endpoints().handle() else {
            return Err(cluster_heal_status_unavailable("endpoint_topology_unavailable"));
        };
        let expected_nodes = endpoints.get_nodes().len();
        if expected_nodes == 0 {
            return Err(cluster_heal_status_unavailable("endpoint_topology_empty"));
        }
        let notification_system = context.notification_system().handle();
        let local = rustfs_heal::current_replacement_recovery_snapshot().await;
        let cluster =
            read_cluster_replacement_recovery_status(local.clone(), notification_system.as_deref(), expected_nodes).await?;
        let response = build_replacement_recovery_status_response(local, cluster);
        let body = encode_replacement_recovery_status(&response)?;
        info!(
            event = EVENT_ADMIN_RESPONSE_EMITTED,
            component = LOG_COMPONENT_ADMIN_API,
            subsystem = LOG_SUBSYSTEM_HEAL_ADMIN,
            operation = "replacement_recovery_status",
            result = "success",
            "admin response emitted"
        );

        Ok(json_response(StatusCode::OK, body))
    }
}

#[cfg(test)]
mod tests {
    use super::extract_heal_init_params;
    use super::{
        BackgroundHealProgress, HealInitParams, HealResp, HealRuntimeState, aggregate_cluster_heal_status,
        aggregate_replacement_recovery_cluster_status, background_heal_runtime_state, build_heal_channel_request,
        build_replacement_recovery_status_response, encode_background_heal_status, encode_heal_control_path,
        encode_heal_start_success, encode_heal_task_status, execute_after_heal_control_capability, heal_channel_response_items,
        heal_channel_response_progress, heal_channel_response_summary, heal_control_response_id, json_response,
        map_heal_response, merge_peer_heal_statuses, peer_topology_complete, query_peer_heal_status,
        query_peer_replacement_recovery_status, reject_heal_admission, validate_heal_request_mode, validate_heal_target,
    };
    use crate::storage::rpc::node_service::heal::{
        NodeHealProgress, NodeHealStatusSnapshot, NodeReplacementRecoveryStatusSnapshot, encode_node_replacement_recovery_status,
    };
    use bytes::Bytes;
    use http::StatusCode;
    use http::Uri;
    use matchit::Router;
    use rustfs_common::heal_channel::{
        HealAdmissionDropReason, HealAdmissionResult, HealChannelPriority, HealOpts, HealRequestSource, HealScanMode,
    };
    use rustfs_scanner::scanner::BackgroundHealInfo;
    use s3s::{
        S3ErrorCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};
    use tokio::sync::mpsc;
    use tokio::time::Duration;

    fn replacement_record(task_id: &str) -> rustfs_heal::ReplacementRecoveryRecord {
        rustfs_heal::ReplacementRecoveryRecord {
            task_id: task_id.to_string(),
            state: rustfs_heal::ReplacementRecoveryState::Completed,
            generation: Some(task_id.to_string()),
            set_disk_id: Some("pool_0_set_0".to_string()),
            target_slots: vec!["http://node-a:9000/mnt/disk1".to_string()],
            reason: None,
            verified_at: Some(42),
        }
    }

    fn replacement_snapshot(task_id: &str) -> rustfs_heal::ReplacementRecoverySnapshot {
        rustfs_heal::ReplacementRecoverySnapshot {
            records: vec![replacement_record(task_id)],
            definitive: true,
            reason: None,
        }
    }

    #[tokio::test]
    async fn cluster_capability_gate_runs_before_execution() {
        let executed = AtomicBool::new(false);
        let rejected = execute_after_heal_control_capability(
            || async { Err(super::cluster_heal_control_unavailable("test_capability_failure")) },
            || async {
                executed.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
        assert!(rejected.is_err());
        assert!(!executed.load(Ordering::SeqCst));

        execute_after_heal_control_capability(
            || async { Ok(()) },
            || async {
                executed.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert!(executed.load(Ordering::SeqCst));
    }

    #[test]
    fn replacement_recovery_status_response_reports_cluster_proof() {
        let local = replacement_snapshot("11111111-1111-4111-8111-111111111111");
        let cluster = aggregate_replacement_recovery_cluster_status(vec![local.clone()], Vec::new(), 1, true);
        let response = build_replacement_recovery_status_response(local, cluster);
        let value = serde_json::to_value(response).expect("replacement status response should serialize");

        assert_eq!(value["contractVersion"], 2);
        assert_eq!(value["path"], "/rustfs/admin/v4/heal/replacement-recovery");
        assert_eq!(value["scope"], "cluster_survivor_disks");
        assert_eq!(value["local"]["definitive"], true);
        assert_eq!(value["local"]["records"][0]["state"], "completed");
        assert_eq!(value["cluster"]["definitive"], true);
        assert!(value["cluster"]["reason"].is_null());
        assert_eq!(value["cluster"]["records"][0]["state"], "completed");
    }

    #[test]
    fn replacement_recovery_cluster_requires_peer_capability() {
        let local = replacement_snapshot("11111111-1111-4111-8111-111111111111");
        let cluster = aggregate_replacement_recovery_cluster_status(vec![local], vec![Ok(None)], 2, true);

        assert!(!cluster.definitive);
        assert_eq!(cluster.reason.as_deref(), Some("peer_replacement_recovery_status_unsupported"));
        assert_eq!(cluster.records[0].state, rustfs_heal::ReplacementRecoveryState::Completed);
    }

    #[test]
    fn replacement_recovery_cluster_requires_matching_peer_records() {
        let local = replacement_snapshot("11111111-1111-4111-8111-111111111111");
        let peer = replacement_snapshot("22222222-2222-4222-8222-222222222222");
        let cluster = aggregate_replacement_recovery_cluster_status(vec![local], vec![Ok(Some(peer))], 2, true);

        assert!(!cluster.definitive);
        assert_eq!(cluster.reason.as_deref(), Some("peer_replacement_recovery_status_conflict"));
        assert_eq!(cluster.records.len(), 2);
    }

    #[test]
    fn replacement_recovery_cluster_rejects_conflicting_records_inside_a_snapshot() {
        let mut local = replacement_snapshot("11111111-1111-4111-8111-111111111111");
        let mut conflicting = replacement_record("11111111-1111-4111-8111-111111111111");
        conflicting.set_disk_id = Some("pool_0_set_1".to_string());
        local.records.push(conflicting);
        let cluster = aggregate_replacement_recovery_cluster_status(vec![local], Vec::new(), 1, true);

        assert!(!cluster.definitive);
        assert_eq!(cluster.reason.as_deref(), Some("peer_replacement_recovery_status_conflict"));
        assert_eq!(cluster.records[0].state, rustfs_heal::ReplacementRecoveryState::Unknown);
    }

    #[test]
    fn replacement_recovery_cluster_is_definitive_when_peers_match() {
        let local = replacement_snapshot("11111111-1111-4111-8111-111111111111");
        let peer = replacement_snapshot("11111111-1111-4111-8111-111111111111");
        let cluster = aggregate_replacement_recovery_cluster_status(vec![local], vec![Ok(Some(peer))], 2, true);

        assert!(cluster.definitive);
        assert!(cluster.reason.is_none());
        assert_eq!(cluster.records.len(), 1);
        assert_eq!(cluster.records[0].state, rustfs_heal::ReplacementRecoveryState::Completed);
    }

    #[tokio::test]
    async fn peer_replacement_recovery_status_decodes_supported_peers() {
        let payload = encode_node_replacement_recovery_status(&NodeReplacementRecoveryStatusSnapshot::new(replacement_snapshot(
            "11111111-1111-4111-8111-111111111111",
        )))
        .expect("peer status should encode");
        let decoded =
            query_peer_replacement_recovery_status("node-a", async { Ok::<_, String>(Some(payload)) }, Duration::from_secs(1))
                .await
                .expect("peer query should succeed")
                .expect("peer should support status");

        assert_eq!(decoded.records[0].state, rustfs_heal::ReplacementRecoveryState::Completed);
    }

    #[tokio::test]
    async fn peer_replacement_recovery_status_treats_missing_rpc_as_unknown() {
        let decoded = query_peer_replacement_recovery_status(
            "node-a",
            async { Ok::<Option<Vec<u8>>, String>(None) },
            Duration::from_secs(1),
        )
        .await
        .expect("missing rpc should not be a transport failure");

        assert!(decoded.is_none());
    }

    #[test]
    fn test_reject_heal_admission_preserves_retry_semantics() {
        for admission in [
            HealAdmissionResult::Full,
            HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull),
        ] {
            assert_eq!(reject_heal_admission(admission).code(), &S3ErrorCode::SlowDown);
        }
        assert_eq!(
            reject_heal_admission(HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped)).code(),
            &S3ErrorCode::OperationAborted
        );
        assert_eq!(reject_heal_admission(HealAdmissionResult::Accepted).code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn test_heal_opts_serialization() {
        // Test that HealOpts can be properly deserialized
        let heal_opts_json = json!({
            "recursive": true,
            "dryRun": false,
            "remove": true,
            "recreate": false,
            "scanMode": 2,
            "updateParity": true,
            "nolock": false
        });

        let json_str = serde_json::to_string(&heal_opts_json).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed["recursive"], true);
        assert_eq!(parsed["scanMode"], 2);
    }

    #[test]
    fn test_heal_opts_url_encoding() {
        // Test URL encoding/decoding of HealOpts
        let opts = HealOpts {
            recursive: true,
            dry_run: false,
            remove: true,
            recreate: false,
            scan_mode: rustfs_common::heal_channel::HealScanMode::Normal,
            update_parity: false,
            no_lock: true,
            pool: Some(1),
            set: Some(0),
        };

        let encoded = serde_urlencoded::to_string(opts).unwrap();
        assert!(encoded.contains("recursive=true"));
        assert!(encoded.contains("remove=true"));

        // Test round-trip
        let decoded: HealOpts = serde_urlencoded::from_str(&encoded).unwrap();
        assert_eq!(decoded.recursive, opts.recursive);
        assert_eq!(decoded.scan_mode, opts.scan_mode);
    }

    #[test]
    fn test_extract_heal_init_params_invalid_control_combination_returns_descriptive_error() {
        let uri: Uri = "/rustfs/admin/v3/heal/test-bucket?clientToken=token&forceStart=true"
            .parse()
            .expect("uri should parse");

        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/heal/{bucket}", ())
            .expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/heal/test-bucket").expect("route should match");

        let err = extract_heal_init_params(&Bytes::new(), &uri, matched.params).expect_err("must reject invalid combo");
        assert!(
            err.to_string()
                .contains("invalid combination of clientToken, forceStart, and forceStop parameters")
        );
    }

    #[test]
    fn test_extract_heal_init_params_allows_client_token_force_stop() {
        let uri: Uri = "/rustfs/admin/v3/heal/test-bucket?clientToken=token&forceStop=true"
            .parse()
            .expect("uri should parse");

        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/heal/{bucket}", ())
            .expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/heal/test-bucket").expect("route should match");

        let parsed = extract_heal_init_params(&Bytes::new(), &uri, matched.params).expect("client-token stop should be accepted");
        assert_eq!(parsed.client_token, "token");
        assert!(parsed.force_stop);
    }

    #[test]
    fn test_extract_heal_init_params_rejects_unknown_duplicate_and_invalid_bool() {
        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/heal/{bucket}", ())
            .expect("route should insert");

        for query in [
            "forceStart=yes",
            "forceStart=true&forceStart=false",
            "clientToken=token&unexpected=true",
        ] {
            let uri: Uri = format!("/rustfs/admin/v3/heal/test-bucket?{query}")
                .parse()
                .expect("uri should parse");
            let matched = router.at("/rustfs/admin/v3/heal/test-bucket").expect("route should match");
            let err = extract_heal_init_params(&Bytes::new(), &uri, matched.params)
                .expect_err("strict heal query should reject malformed input");
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        }
    }

    #[test]
    fn test_heal_channel_request_preserves_admin_heal_options() {
        let hip = HealInitParams {
            bucket: "bucket".to_string(),
            obj_prefix: "prefix".to_string(),
            hs: HealOpts {
                recursive: true,
                dry_run: true,
                remove: true,
                recreate: true,
                scan_mode: HealScanMode::Deep,
                update_parity: true,
                pool: Some(1),
                set: Some(2),
                ..Default::default()
            },
            force_start: true,
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "bucket");
        assert_eq!(request.object_prefix.as_deref(), Some("prefix"));
        assert_eq!(request.priority, HealChannelPriority::High);
        assert!(request.force_start);
        assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
        assert_eq!(request.recursive, Some(true));
        assert_eq!(request.dry_run, Some(true));
        assert_eq!(request.remove_corrupted, Some(true));
        assert_eq!(request.recreate_missing, Some(true));
        assert_eq!(request.update_parity, Some(true));
        assert_eq!(request.pool_index, Some(1));
        assert_eq!(request.set_index, Some(2));
    }

    #[test]
    fn test_root_heal_channel_request_with_pool_set_targets_erasure_set() {
        let hip = HealInitParams {
            hs: HealOpts {
                scan_mode: HealScanMode::Deep,
                recreate: true,
                pool: Some(1),
                set: Some(2),
                ..Default::default()
            },
            force_start: true,
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "");
        assert_eq!(request.disk.as_deref(), Some("pool_1_set_2"));
        assert_eq!(request.priority, HealChannelPriority::High);
        assert_eq!(request.source, HealRequestSource::Admin);
        assert_eq!(request.pool_index, Some(1));
        assert_eq!(request.set_index, Some(2));
        assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
        assert_eq!(request.recursive, Some(true));
        assert_eq!(request.recreate_missing, Some(true));
    }

    #[test]
    fn test_root_recursive_heal_channel_request_targets_cluster() {
        let hip = HealInitParams {
            hs: HealOpts {
                recursive: true,
                scan_mode: HealScanMode::Deep,
                recreate: true,
                ..Default::default()
            },
            force_start: true,
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "");
        assert_eq!(request.disk, None);
        assert_eq!(request.object_prefix, None);
        assert_eq!(request.priority, HealChannelPriority::High);
        assert_eq!(request.source, HealRequestSource::Admin);
        assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
        assert_eq!(request.recursive, Some(true));
        assert_eq!(request.recreate_missing, Some(true));
    }

    #[test]
    fn test_bucket_heal_channel_request_defaults_to_recursive() {
        let hip = HealInitParams {
            bucket: "bucket".to_string(),
            obj_prefix: String::new(),
            hs: HealOpts {
                recursive: false,
                scan_mode: HealScanMode::Deep,
                recreate: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "bucket");
        assert_eq!(request.object_prefix, None);
        assert_eq!(request.recursive, Some(true));
    }

    #[test]
    fn test_extract_heal_init_params_allows_root_heal_target() {
        let uri: Uri = "/rustfs/admin/v3/heal/".parse().expect("uri should parse");
        let heal_opts = json!({
            "recursive": false,
            "dryRun": false,
            "remove": false,
            "recreate": false,
            "scanMode": 1,
            "updateParity": false,
            "nolock": false
        });

        let mut router = Router::new();
        router.insert("/rustfs/admin/v3/heal/", ()).expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/heal/").expect("route should match");

        let parsed = extract_heal_init_params(
            &Bytes::from(serde_json::to_vec(&heal_opts).expect("json should serialize")),
            &uri,
            matched.params,
        )
        .expect("root heal target should be accepted");

        assert!(parsed.bucket.is_empty());
        assert!(parsed.obj_prefix.is_empty());
    }

    #[test]
    fn test_validate_heal_request_mode_rejects_root_heal_start() {
        let err = validate_heal_request_mode(&HealInitParams::default()).expect_err("must reject root heal start");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(
            err.to_string()
                .contains("root heal requires recursive=true or a bucket target")
        );
    }

    #[test]
    fn test_validate_heal_request_mode_allows_root_recursive_heal_start() {
        validate_heal_request_mode(&HealInitParams {
            hs: HealOpts {
                recursive: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .expect("root recursive heal should start tracked cluster heal");
    }

    #[test]
    fn test_validate_heal_request_mode_allows_root_erasure_set_target() {
        validate_heal_request_mode(&HealInitParams {
            hs: HealOpts {
                pool: Some(1),
                set: Some(2),
                ..Default::default()
            },
            ..Default::default()
        })
        .expect("root heal with pool and set should start tracked erasure-set rebuild");
    }

    #[test]
    fn test_validate_heal_request_mode_rejects_partial_root_erasure_set_target() {
        let err = validate_heal_request_mode(&HealInitParams {
            hs: HealOpts {
                pool: Some(1),
                ..Default::default()
            },
            ..Default::default()
        })
        .expect_err("root heal must provide both pool and set");

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(
            err.to_string()
                .contains("root heal erasure-set target requires both pool and set")
        );
    }

    #[test]
    fn test_root_heal_shapes_route_through_cluster_coordination() {
        // Root heal has no direct local store path: every start shape is either
        // rejected by validate_heal_request_mode or submitted to the cluster
        // heal channel as an Admin-sourced request (see HealHandler::call).
        let accepted_root_starts = [
            HealInitParams {
                hs: HealOpts {
                    recursive: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            HealInitParams {
                force_start: true,
                hs: HealOpts {
                    recursive: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            HealInitParams {
                hs: HealOpts {
                    pool: Some(1),
                    set: Some(2),
                    ..Default::default()
                },
                ..Default::default()
            },
        ];
        for hip in accepted_root_starts {
            validate_heal_request_mode(&hip).expect("accepted root heal start must reach cluster coordination");
            let request = build_heal_channel_request(&hip);
            assert_eq!(request.bucket, "", "root heal must stay cluster-scoped");
            assert_eq!(request.source, HealRequestSource::Admin);
            assert!(!request.id.is_empty(), "cluster heal requests carry a dedup id");
        }

        // Shapes that cannot start a tracked heal (plain start, bare force_start
        // without recursive, bare pool) are rejected instead of falling back to
        // a direct local path.
        for hip in [
            HealInitParams::default(),
            HealInitParams {
                force_start: true,
                ..Default::default()
            },
            HealInitParams {
                hs: HealOpts {
                    pool: Some(1),
                    ..Default::default()
                },
                ..Default::default()
            },
        ] {
            let err = validate_heal_request_mode(&hip).expect_err("unscoped root heal start must be rejected");
            assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        }
    }

    #[test]
    fn test_validate_heal_request_mode_allows_root_query_and_cancel() {
        validate_heal_request_mode(&HealInitParams {
            client_token: "heal-token".to_string(),
            ..Default::default()
        })
        .expect("root heal status query should be accepted");

        validate_heal_request_mode(&HealInitParams {
            force_stop: true,
            ..Default::default()
        })
        .expect("root heal cancel should be accepted");
    }

    #[test]
    fn test_encode_heal_control_path_keeps_root_empty() {
        assert_eq!(encode_heal_control_path("", ""), "");
        assert_eq!(encode_heal_control_path("bucket", ""), "bucket");
        assert_eq!(encode_heal_control_path("bucket", "prefix"), "bucket/prefix");
    }

    #[test]
    fn test_heal_control_response_id_preserves_existing_contract() {
        assert_eq!(heal_control_response_id("", ""), ".");
        assert_eq!(heal_control_response_id("bucket", ""), "bucket");
        assert_eq!(heal_control_response_id("", "task-id"), "task-id");
    }

    #[test]
    fn test_extract_heal_init_params_rejects_prefix_without_bucket() {
        let err = validate_heal_target("", "prefix").expect_err("must reject empty bucket");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.to_string().contains("invalid bucket name"));
    }

    #[test]
    fn test_decode() {
        // Mirror the production decode path (handler decodes the request body via
        // serde_json::from_slice), not urlencoded.
        let b = b"{\"recursive\":false,\"dryRun\":false,\"remove\":false,\"recreate\":false,\"scanMode\":1,\"updateParity\":false,\"nolock\":false}";
        let s: HealOpts = serde_json::from_slice(b).expect("HealOpts JSON body must decode");
        assert!(!s.recursive);
        assert!(!s.dry_run);
        assert!(!s.remove);
        assert!(!s.recreate);
        assert_eq!(s.scan_mode, HealScanMode::Normal);
        assert!(!s.update_parity);
        assert!(!s.no_lock);
        assert_eq!(s.pool, None);
        assert_eq!(s.set, None);
    }

    #[tokio::test]
    async fn test_map_heal_response_propagates_errors() {
        let (tx, mut rx) = mpsc::channel(1);
        drop(tx);
        let result = map_heal_response(rx.recv().await);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), &S3ErrorCode::InternalError);

        let (tx, mut rx) = mpsc::channel(1);
        tx.send(HealResp {
            resp_bytes: vec![],
            api_err: Some("channel failed".to_string()),
        })
        .await
        .unwrap();
        let result = map_heal_response(rx.recv().await);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), &S3ErrorCode::InternalError);

        let (tx, mut rx) = mpsc::channel(1);
        tx.send(HealResp {
            resp_bytes: vec![],
            api_err: None,
        })
        .await
        .unwrap();
        let result = map_heal_response(rx.recv().await);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), &S3ErrorCode::InternalError);

        let (tx, mut rx) = mpsc::channel(1);
        let _ = tx
            .send(HealResp {
                resp_bytes: vec![1, 2, 3],
                api_err: None,
            })
            .await;
        let result = map_heal_response(rx.recv().await).expect("heal response should be successful");
        assert_eq!(result.0, StatusCode::OK);
        assert_eq!(result.1, vec![1, 2, 3]);
    }

    #[test]
    fn test_encode_background_heal_status_uses_expected_shape() {
        let info = BackgroundHealInfo {
            bitrot_start_time: None,
            bitrot_start_cycle: 42,
            current_scan_mode: HealScanMode::Deep,
        };

        let operations = rustfs_heal::HealOperationsSnapshot {
            queue_length: 3,
            active_tasks: 2,
            ..Default::default()
        };

        let encoded = encode_background_heal_status(&info, HealRuntimeState::Active, operations, None, true)
            .expect("background heal info should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["bitrotStartCycle"], 42);
        assert_eq!(json["currentScanMode"], 2);
        assert!(json["bitrotStartTime"].is_null());
        assert!(json["healQueueLength"].is_u64());
        assert!(json["healActiveTasks"].is_u64());
        assert_eq!(json["healOperations"]["queueLength"], json["healQueueLength"]);
        assert_eq!(json["healOperations"]["activeTasks"], json["healActiveTasks"]);
        assert!(json["healOperations"]["queuedBySource"]["scanner"].is_u64());
        assert!(json["healOperations"]["queuedBySource"]["admin"].is_u64());
        assert!(json["healOperations"]["queuedByPriority"]["low"].is_u64());
        assert!(json["healOperations"]["queuedByPriority"]["high"].is_u64());
        assert_eq!(json["state"], "active");
        assert_eq!(json["clusterStatusComplete"], true);
        assert!(json["progress"].is_null());
    }

    #[test]
    fn test_encode_background_heal_status_includes_progress() {
        let info = BackgroundHealInfo {
            bitrot_start_time: None,
            bitrot_start_cycle: 42,
            current_scan_mode: HealScanMode::Deep,
        };

        let progress = BackgroundHealProgress {
            kind: rustfs_heal::heal::progress::HealProgressKind::ObjectSweep,
            objects_scanned: 7,
            objects_healed: 3,
            objects_failed: 1,
            skipped_objects: 3,
            objects_total_count: 10,
            objects_total_size: 8192,
            bytes_processed: 4096,
            progress_percentage: 50.0,
            progress_state: rustfs_heal::heal::progress::HealProgressState::Running,
            baseline_generation: Some(42),
            baseline_known: true,
            ..Default::default()
        };

        let encoded = encode_background_heal_status(
            &info,
            HealRuntimeState::Idle,
            rustfs_heal::HealOperationsSnapshot::default(),
            Some(progress),
            true,
        )
        .expect("background heal info should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["progress"]["objectsScanned"], 7);
        assert_eq!(json["progress"]["objectsHealed"], 3);
        assert_eq!(json["progress"]["objectsFailed"], 1);
        assert_eq!(json["progress"]["skippedObjects"], 3);
        assert_eq!(json["progress"]["objectsTotalCount"], 10);
        assert_eq!(json["progress"]["objectsTotalSize"], 8192);
        assert_eq!(json["progress"]["bytesProcessed"], 4096);
        assert_eq!(json["progress"]["progressState"], "running");
        assert_eq!(json["progress"]["baselineGeneration"], 42);
        assert_eq!(json["progress"]["baselineKnown"], true);
        assert_eq!(json["progress"]["counterUnknown"], false);
    }

    #[test]
    fn test_cluster_heal_status_aggregates_active_peer_independent_of_request_node() {
        let local = NodeHealStatusSnapshot::for_test(
            true,
            true,
            BackgroundHealInfo {
                bitrot_start_time: None,
                bitrot_start_cycle: 7,
                current_scan_mode: HealScanMode::Deep,
            },
            rustfs_heal::HealOperationsSnapshot {
                queue_length: 2,
                queued_by_source: rustfs_heal::HealSourceCounts {
                    admin: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
            Some(NodeHealProgress {
                kind: rustfs_heal::heal::progress::HealProgressKind::ObjectSweep,
                objects_scanned: 3,
                objects_healed: 1,
                objects_failed: 0,
                skipped_objects: 2,
                objects_total_count: 6,
                objects_total_size: 400,
                bytes_processed: 100,
                progress_state: rustfs_heal::heal::progress::HealProgressState::Running,
                baseline_generation: Some(9),
                baseline_known: true,
                ..Default::default()
            }),
        );
        let peer = NodeHealStatusSnapshot::for_test(
            true,
            true,
            BackgroundHealInfo::default(),
            rustfs_heal::HealOperationsSnapshot {
                active_tasks: 1,
                active_by_priority: rustfs_heal::HealPriorityCounts {
                    high: 1,
                    ..Default::default()
                },
                active_by_source: rustfs_heal::HealSourceCounts {
                    admin: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
            Some(NodeHealProgress {
                kind: rustfs_heal::heal::progress::HealProgressKind::ObjectSweep,
                objects_scanned: 5,
                objects_healed: 4,
                objects_failed: 1,
                objects_total_count: 4,
                objects_total_size: 1600,
                bytes_processed: 900,
                progress_state: rustfs_heal::heal::progress::HealProgressState::Running,
                baseline_generation: Some(9),
                baseline_known: true,
                ..Default::default()
            }),
        );

        let local_first = aggregate_cluster_heal_status(vec![local.clone(), peer.clone()]);
        let peer_first = aggregate_cluster_heal_status(vec![peer, local]);

        assert_eq!(local_first.state, HealRuntimeState::Active);
        assert_eq!(local_first.operations.queue_length, 2);
        assert_eq!(local_first.operations.active_tasks, 1);
        assert_eq!(local_first.operations.queued_by_source.admin, 2);
        assert_eq!(local_first.operations.active_by_source.admin, 1);
        assert_eq!(local_first.operations.active_by_priority.high, 1);
        assert_eq!(local_first.info.bitrot_start_cycle, 7);
        assert_eq!(local_first.info.current_scan_mode, HealScanMode::Deep);
        let progress = local_first.progress.expect("cluster progress should be present");
        assert_eq!(progress.objects_scanned, 8);
        assert_eq!(progress.objects_healed, 5);
        assert_eq!(progress.objects_failed, 1);
        assert_eq!(progress.skipped_objects, 2);
        assert_eq!(progress.objects_total_count, 10);
        assert_eq!(progress.objects_total_size, 2000);
        assert_eq!(progress.bytes_processed, 1000);
        assert_eq!(progress.progress_percentage, 50.0);
        assert_eq!(progress.progress_state, rustfs_heal::heal::progress::HealProgressState::Running);
        assert_eq!(progress.baseline_generation, Some(9));
        assert!(progress.baseline_known);
        assert!(!progress.counter_unknown);

        assert_eq!(peer_first.state, HealRuntimeState::Active);
        assert_eq!(peer_first.operations, local_first.operations);
        assert_eq!(peer_first.info.bitrot_start_cycle, local_first.info.bitrot_start_cycle);
        assert_eq!(peer_first.info.current_scan_mode, local_first.info.current_scan_mode);
    }

    #[test]
    fn test_cluster_heal_status_saturates_all_counter_groups() {
        let priorities = |value| rustfs_heal::HealPriorityCounts {
            low: value,
            normal: value,
            high: value,
            urgent: value,
        };
        let sources = |value| rustfs_heal::HealSourceCounts {
            scanner: value,
            admin: value,
            auto_heal: value,
            internal: value,
            read_repair: value,
            mrf: value,
        };
        let operations = |value| rustfs_heal::HealOperationsSnapshot {
            queue_length: value,
            active_tasks: value,
            retrying_tasks: value,
            queued_by_priority: priorities(value),
            active_by_priority: priorities(value),
            retrying_by_priority: priorities(value),
            queued_by_source: sources(value),
            active_by_source: sources(value),
            retrying_by_source: sources(value),
        };
        let progress = |value| NodeHealProgress {
            objects_scanned: value,
            objects_healed: value,
            objects_failed: value,
            bytes_processed: value,
            ..Default::default()
        };
        let saturated = NodeHealStatusSnapshot::for_test(
            true,
            true,
            BackgroundHealInfo::default(),
            operations(u64::MAX),
            Some(progress(u64::MAX)),
        );
        let one = NodeHealStatusSnapshot::for_test(true, true, BackgroundHealInfo::default(), operations(1), Some(progress(1)));

        let status = aggregate_cluster_heal_status(vec![saturated, one]);
        assert_eq!(status.operations, operations(u64::MAX));
        let progress = status.progress.expect("progress");
        assert_eq!(progress.objects_scanned, u64::MAX);
        assert_eq!(progress.objects_healed, u64::MAX);
        assert_eq!(progress.objects_failed, u64::MAX);
        assert_eq!(progress.bytes_processed, u64::MAX);
    }

    #[test]
    fn test_peer_topology_validation_fails_closed_on_missing_slots() {
        assert!(peer_topology_complete(4, 3, 0, 4, 3));
        assert!(!peer_topology_complete(4, 2, 0, 4, 3));
        assert!(!peer_topology_complete(4, 3, 1, 4, 2));
        assert!(!peer_topology_complete(4, 3, 0, 3, 2));
        assert!(peer_topology_complete(1, 0, 0, 1, 0));
    }

    #[test]
    fn test_peer_status_merge_degrades_explicitly_and_never_claims_idle() {
        let local = || {
            NodeHealStatusSnapshot::for_test(
                true,
                true,
                BackgroundHealInfo::default(),
                rustfs_heal::HealOperationsSnapshot::default(),
                None,
            )
        };
        // An unreachable peer must not fail the whole endpoint (issue #5850) —
        // but the safety property of the previous fail-closed behaviour is
        // preserved: the partial answer is labelled Degraded, never Idle, so
        // unknown peer work cannot be mistaken for "nothing is running".
        let partial = merge_peer_heal_statuses(vec![local()], vec![Err("peer timeout".to_string())], 2, true)
            .expect("an unreachable peer degrades the answer instead of destroying it");
        assert!(!partial.complete);
        assert_eq!(partial.state, HealRuntimeState::Degraded);

        let older_peer = merge_peer_heal_statuses(vec![local()], vec![Ok(None)], 2, true)
            .expect("an older peer degrades the answer instead of destroying it");
        assert!(!older_peer.complete);
        assert_eq!(older_peer.state, HealRuntimeState::Degraded);

        let known_active = NodeHealStatusSnapshot::for_test(
            true,
            true,
            BackgroundHealInfo::default(),
            rustfs_heal::HealOperationsSnapshot {
                active_tasks: 1,
                ..Default::default()
            },
            None,
        );
        let partial_active = merge_peer_heal_statuses(vec![known_active], vec![Ok(None)], 2, true)
            .expect("known active work may be reported as an explicit partial status");
        assert!(!partial_active.complete);
        assert_eq!(partial_active.state, HealRuntimeState::Active);

        merge_peer_heal_statuses(Vec::new(), vec![Err("peer timeout".to_string())], 2, true)
            .expect_err("no snapshot at all still fails closed");
    }

    #[test]
    fn test_incomplete_topology_forces_partial_even_with_full_snapshot_count() {
        // peer_clients and all_peer_clients are populated independently, so
        // during a reconfiguration every queried peer can answer — snapshot
        // count == expected_nodes — while the topology check knows the
        // membership itself is in doubt. Completeness must AND in the
        // topology verdict or the answer looks definitive exactly when it
        // is not.
        let snapshot = || {
            NodeHealStatusSnapshot::for_test(
                true,
                true,
                BackgroundHealInfo::default(),
                rustfs_heal::HealOperationsSnapshot::default(),
                None,
            )
        };
        let full_count_incomplete_topology = merge_peer_heal_statuses(vec![snapshot()], vec![Ok(Some(snapshot()))], 2, false)
            .expect("incomplete topology degrades the answer instead of destroying it");
        assert!(!full_count_incomplete_topology.complete);
        assert_eq!(full_count_incomplete_topology.state, HealRuntimeState::Degraded);

        let full_count_complete_topology = merge_peer_heal_statuses(vec![snapshot()], vec![Ok(Some(snapshot()))], 2, true)
            .expect("complete topology and full count is a definitive answer");
        assert!(full_count_complete_topology.complete);
        assert_eq!(full_count_complete_topology.state, HealRuntimeState::Idle);
    }

    #[test]
    fn test_degraded_state_serializes_distinctly_from_idle() {
        let encoded = encode_background_heal_status(
            &BackgroundHealInfo::default(),
            HealRuntimeState::Degraded,
            rustfs_heal::HealOperationsSnapshot::default(),
            None,
            false,
        )
        .expect("degraded status must serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("valid json");
        assert_eq!(json["state"], "degraded");
        assert_eq!(json["clusterStatusComplete"], false);
    }

    #[tokio::test]
    async fn test_peer_status_query_rejects_failure_decode_error_and_timeout() {
        let failure = query_peer_heal_status(
            "node-a",
            std::future::ready(Err::<Option<Vec<u8>>, _>(std::io::Error::other("rpc failed"))),
            Duration::from_secs(1),
        )
        .await
        .expect_err("RPC failure must fail closed");
        assert!(failure.contains("rpc failed"));

        let decode = query_peer_heal_status(
            "node-a",
            std::future::ready(Ok::<_, std::io::Error>(Some(vec![0xc1]))),
            Duration::from_secs(1),
        )
        .await
        .expect_err("invalid payload must fail closed");
        assert!(decode.contains("decode"));

        let timed_out = query_peer_heal_status(
            "node-a",
            std::future::pending::<Result<Option<Vec<u8>>, std::io::Error>>(),
            Duration::from_millis(1),
        )
        .await
        .expect_err("timeout must fail closed");
        assert!(timed_out.contains("timed out"));
    }

    #[test]
    fn test_background_heal_runtime_state_distinguishes_disabled_and_uninitialized() {
        let operations = rustfs_heal::HealOperationsSnapshot::default();

        assert_eq!(background_heal_runtime_state(false, false, &operations), HealRuntimeState::Disabled);
        assert_eq!(background_heal_runtime_state(true, false, &operations), HealRuntimeState::Uninitialized);
        assert_eq!(background_heal_runtime_state(true, true, &operations), HealRuntimeState::Idle);

        let active = rustfs_heal::HealOperationsSnapshot {
            active_tasks: 1,
            ..operations
        };
        assert_eq!(background_heal_runtime_state(true, true, &active), HealRuntimeState::Active);

        let retrying = rustfs_heal::HealOperationsSnapshot {
            retrying_tasks: 1,
            ..Default::default()
        };
        assert_eq!(background_heal_runtime_state(true, true, &retrying), HealRuntimeState::Active);
    }

    #[test]
    fn test_encode_heal_start_success_uses_client_wire_shape() {
        let encoded = encode_heal_start_success("token-1".to_string(), "127.0.0.1:9000".to_string())
            .expect("start response should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["clientToken"], "token-1");
        assert_eq!(json["clientAddress"], "127.0.0.1:9000");
        let start_time = json["startTime"].as_str().expect("startTime should be a string");
        OffsetDateTime::parse(start_time, &Rfc3339).expect("startTime should be RFC3339");
    }

    #[test]
    fn test_encode_heal_task_status_uses_client_wire_shape() {
        let encoded = encode_heal_task_status(
            "Heal status query accepted".to_string(),
            String::new(),
            HealOpts::default(),
            Vec::new(),
            false,
            None,
        )
        .expect("status response should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["summary"], "Heal status query accepted");
        assert_eq!(json["detail"], "");
        assert!(json["items"].is_null());
        assert!(json["truncated"].is_null());
        assert!(json["settings"].is_object());
        let start_time = json["startTime"].as_str().expect("startTime should be a string");
        OffsetDateTime::parse(start_time, &Rfc3339).expect("startTime should be RFC3339");
    }

    #[test]
    fn test_encode_heal_task_status_reports_truncated_items() {
        let encoded = encode_heal_task_status(
            "running".to_string(),
            "heal result items were truncated".to_string(),
            HealOpts::default(),
            Vec::new(),
            true,
            None,
        )
        .expect("truncated status response should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["truncated"], true);
    }

    #[test]
    fn test_encode_heal_task_status_preserves_progress() {
        let progress = json!({
            "objectsScanned": 7,
            "objectsHealed": 3,
            "currentObject": "bucket-a/object-a"
        });
        let encoded = encode_heal_task_status(
            "running".to_string(),
            String::new(),
            HealOpts::default(),
            Vec::new(),
            false,
            Some(progress.clone()),
        )
        .expect("status response should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["progress"], progress);
    }

    #[test]
    fn test_build_heal_channel_request_preserves_client_options() {
        let hip = HealInitParams {
            bucket: "bucket-a".to_string(),
            obj_prefix: "prefix-a".to_string(),
            force_start: true,
            hs: HealOpts {
                recursive: true,
                dry_run: true,
                remove: true,
                recreate: false,
                scan_mode: HealScanMode::Deep,
                update_parity: false,
                no_lock: true,
                pool: Some(1),
                set: Some(2),
            },
            ..Default::default()
        };

        let request = build_heal_channel_request(&hip);

        assert_eq!(request.bucket, "bucket-a");
        assert_eq!(request.object_prefix.as_deref(), Some("prefix-a"));
        assert_eq!(request.priority, HealChannelPriority::High);
        assert_eq!(request.source, HealRequestSource::Admin);
        assert!(request.force_start);
        assert_eq!(request.pool_index, Some(1));
        assert_eq!(request.set_index, Some(2));
        assert_eq!(request.scan_mode, Some(HealScanMode::Deep));
        assert_eq!(request.remove_corrupted, Some(true));
        assert_eq!(request.recreate_missing, Some(false));
        assert_eq!(request.update_parity, Some(false));
        assert_eq!(request.recursive, Some(true));
        assert_eq!(request.dry_run, Some(true));
        assert_eq!(request.no_lock, Some(true));

        let envelope = rustfs_protos::heal_control::Envelope::start(
            request,
            rustfs_protos::heal_control::RequestMetadata::new([1; 16], 1_000, 2_000, 1),
        )
        .expect("admin heal request should encode through heal-control v2");
        let encoded =
            rustfs_protos::heal_control::encode_envelope(&envelope).expect("admin heal-control envelope should serialize");
        let decoded =
            rustfs_protos::heal_control::decode_envelope(&encoded).expect("admin heal-control envelope should deserialize");
        let (_, _, rustfs_protos::heal_control::ExecutableCommand::Start { request }) =
            decoded.into_execution().expect("admin heal start should decode")
        else {
            panic!("expected admin heal start command");
        };
        assert_eq!(request.no_lock, Some(true));
    }

    #[test]
    fn test_heal_channel_response_summary_defaults_to_running() {
        let response = rustfs_common::heal_channel::create_heal_response("token".to_string(), true, None, None);
        assert_eq!(heal_channel_response_summary(&response), "running");

        let response =
            rustfs_common::heal_channel::create_heal_response("token".to_string(), true, Some(b"finished".to_vec()), None);
        assert_eq!(heal_channel_response_summary(&response), "finished");
    }

    #[test]
    fn test_heal_channel_response_status_preserves_items() {
        let payload = serde_json::json!({
            "summary": "finished",
            "items": [{
                "resultId": 0,
                "type": "object",
                "bucket": "bucket-a",
                "object": "object-a",
                "versionId": "",
                "detail": "",
                "parityBlocks": 2,
                "dataBlocks": 2,
                "diskCount": 4,
                "setCount": 1,
                "before": { "drives": [] },
                "after": { "drives": [] },
                "objectSize": 1024
            }]
        });
        let response = rustfs_common::heal_channel::create_heal_response(
            "token".to_string(),
            true,
            Some(serde_json::to_vec(&payload).expect("payload should serialize")),
            None,
        );

        assert_eq!(heal_channel_response_summary(&response), "finished");
        let items = heal_channel_response_items(&response);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].bucket, "bucket-a");
        assert_eq!(items[0].object, "object-a");
        assert_eq!(items[0].object_size, 1024);
    }

    #[test]
    fn test_heal_channel_response_status_preserves_progress() {
        let progress = serde_json::json!({
            "objectsScanned": 4,
            "objectsHealed": 2,
            "currentObject": "bucket-a/object-a"
        });
        let payload = serde_json::json!({
            "summary": "running",
            "items": [],
            "progress": progress
        });
        let response = rustfs_common::heal_channel::create_heal_response(
            "token".to_string(),
            true,
            Some(serde_json::to_vec(&payload).expect("payload should serialize")),
            None,
        );

        assert_eq!(heal_channel_response_summary(&response), "running");
        assert_eq!(heal_channel_response_progress(&response), Some(progress));
    }

    #[test]
    fn test_json_response_sets_application_json_content_type() {
        let response = json_response(StatusCode::OK, b"{}".to_vec());
        let content_type = response.headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok());
        assert_eq!(content_type, Some("application/json"),);
        let content_length = response.headers.get(CONTENT_LENGTH).and_then(|value| value.to_str().ok());
        assert_eq!(content_length, Some("2"),);
    }
}
